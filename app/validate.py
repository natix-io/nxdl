#!/usr/bin/env python3

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path

from app.trip_downloader import get_dataset_request, get_ds_manifest
from app.validation import (
    DATASET_REQUEST_FILENAME,
    TripReport,
    ValidationIssue,
    ValidationReport,
    _discover_trip_dirs,
    _load_saved_dataset_request,
    _should_omit_missing_file_report,
    collect_missing_files,
    parse_trip_dir_name,
    validate_dataset,
)
from app.manifest_coverage import ManifestCoverageResult, collect_manifest_missing_files
from schemas.camera_selection import CAMERA_SELECTION
from utils.path_helpers import resolve_camera_selection
from utils.s3_dataset import (
    build_snapshot,
    dataset_root,
    exclude_dirs,
    overlay_local_file,
)
from utils.settings import settings
from utils.trip_list import TripKey, format_trip_key, parse_trip_line, trip_matches

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)


@dataclass(slots=True)
class DurationSummary:
    countries: int = 0
    trips: int = 0
    minutes: int = 0
    avg_trip_duration_minutes: float = 0.0
    min_trip_duration_minutes: int = 0
    max_trip_duration_minutes: int = 0


@dataclass(slots=True)
class S3ReportContext:
    """Extra report context for S3 mode: URI label, request source, trip coverage."""

    uri: str
    request_note: str | None = None
    # Trip identity is (trip_id, sub_trip, health_grade): one dataset can contain the
    # same pair at several grades, each its own directory with its own files. Collapsing
    # to the pair would report a dataset with one grade uploaded as fully covered.
    expected_trips: set[tuple[str, int, int]] | None = None
    coverage_note: str | None = None
    uploaded_trips: set[tuple[str, int, int]] = field(default_factory=set)
    skipped_trip_count: int = 0

    @property
    def missing_trips(self) -> list[tuple[str, int, int]]:
        if self.expected_trips is None:
            return []
        return sorted(self.expected_trips - self.uploaded_trips)

    @property
    def unexpected_trips(self) -> list[tuple[str, int, int]]:
        if self.expected_trips is None:
            return []
        return sorted(self.uploaded_trips - self.expected_trips)


def _apply_camera_selection(
    flag_value: CAMERA_SELECTION | None,
) -> CAMERA_SELECTION:
    """Resolve the effective camera selection and make it the process-wide setting.

    ``--camera-selection`` is an override of the CAMERA_SELECTION setting, so it
    has to reach every consumer — not just validate_dataset. Manifest coverage
    builds its expected-file set through
    ``manifest_coverage.build_expected_trip_files``, which resolves the selection
    from settings; without this, a front-only dataset validated via the flag
    passes the camera checks but reports every unselected camera's file as
    manifest-missing.
    """
    selection = resolve_camera_selection(flag_value)
    settings.camera_selection = selection
    return selection


def main() -> None:
    args = _parse_args()

    camera_selection = _apply_camera_selection(args.camera_selection)
    source = "--camera-selection" if args.camera_selection is not None else "settings"
    print(f"Camera selection: {camera_selection} (from {source})")

    s3_context: S3ReportContext | None = None
    manifest_coverage: ManifestCoverageResult | None = None
    if args.s3 is not None:
        report, s3_context, manifest_coverage = _validate_on_s3(args.s3, args=args)
    else:
        allowed_root_files = (
            frozenset({args.output.resolve()})
            if args.output is not None
            else frozenset()
        )
        report = validate_dataset(
            dataset_dir=args.dataset_dir,
            allowed_root_files=allowed_root_files,
            camera_selection=camera_selection,
        )
        manifest_coverage = _collect_manifest_coverage(
            report,
            trip_filter=None,
            max_trips=args.manifest_max_trips,
            uploaded_only=args.manifest_uploaded_only,
        )
    _print_report(report, s3_context, manifest_coverage)

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            _render_report(
                report,
                verbose_trips=True,
                s3_context=s3_context,
                manifest_coverage=manifest_coverage,
            ),
            encoding="utf-8",
        )
        print(f"Report written to {args.output.resolve()}")

    sys.exit(_validation_exit_code(report, manifest_coverage))


def _parse_trip_key(trip: str) -> TripKey:
    """``--trip`` value → a requested key; an omitted grade means "any grade"."""
    parsed = parse_trip_line(trip.strip())
    if parsed is None:
        print(
            f"Invalid --trip value {trip!r}; expected "
            "'<trip_id>_<sub_trip>[_<health_grade>]'.",
            file=sys.stderr,
        )
        sys.exit(2)
    return parsed


def _validate_on_s3(
    s3_arg: str,
    *,
    args: argparse.Namespace,
) -> tuple[ValidationReport, S3ReportContext, ManifestCoverageResult | None]:
    """Validate the dataset uploaded to S3 (read-only: LIST + GET only)."""
    uri = s3_arg or settings.s3_upload
    if not uri:
        print(
            "--s3 requires an S3 URI argument or S3_UPLOAD set in the active .env file.",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        snapshot = build_snapshot(uri)
    except Exception as exc:
        print(f"Failed to list {uri}: {exc}", file=sys.stderr)
        sys.exit(2)

    context = S3ReportContext(uri=uri)
    root = dataset_root(snapshot)

    # dataset_request.json is uploaded last, so mid-upload it is not on S3 yet.
    # Fall back to a local copy, or download it from the API to the local disk.
    if root.is_dir() and not (root / DATASET_REQUEST_FILENAME).is_file():
        local_request, context.request_note = _resolve_local_dataset_request()
        snapshot = overlay_local_file(snapshot, DATASET_REQUEST_FILENAME, local_request)
        root = dataset_root(snapshot)

    context.expected_trips, context.coverage_note = _fetch_expected_trips()

    # Coverage is judged on the raw listing; validation is then restricted to
    # the manifest's trips, so trip folders from other datasets sharing the
    # prefix don't pollute the report (they'd be judged against the wrong
    # dataset request anyway).
    parsed_trip_dirs: list[tuple[Path, tuple[str, int, int]]] = []
    if root.is_dir():
        for trip_dir in _discover_trip_dirs(root):
            try:
                trip_key = parse_trip_dir_name(trip_dir.name)
            except ValueError:
                continue
            parsed_trip_dirs.append((trip_dir, trip_key))
    context.uploaded_trips = {trip_key for _, trip_key in parsed_trip_dirs}

    trip: str | None = args.trip
    selected_trip: TripKey | None = _parse_trip_key(trip) if trip is not None else None
    if selected_trip is not None:
        selection = {selected_trip}
        matching = [
            (trip_dir, trip_key)
            for trip_dir, trip_key in parsed_trip_dirs
            if trip_matches(selection, *trip_key)
        ]
        if not matching:
            print(
                f"Trip {trip} was not found under {uri}.",
                file=sys.stderr,
            )
            sys.exit(2)
        # A --trip without a grade selects every grade of that pair, so the
        # expected/uploaded sets are the matched directories, not one key.
        matched_keys = {trip_key for _, trip_key in matching}
        context.expected_trips = matched_keys
        context.uploaded_trips = matched_keys
        context.coverage_note = f"validation limited to trip {trip}"
        trip_dirs_to_drop = [
            trip_dir
            for trip_dir, trip_key in parsed_trip_dirs
            if not trip_matches(selection, *trip_key)
        ]
        if trip_dirs_to_drop:
            context.skipped_trip_count = len(trip_dirs_to_drop)
            snapshot = exclude_dirs(
                snapshot, [trip_dir.as_posix() for trip_dir in trip_dirs_to_drop]
            )
            root = dataset_root(snapshot)
    elif context.expected_trips is not None:
        foreign_trip_dirs = [
            trip_dir
            for trip_dir, trip_key in parsed_trip_dirs
            if trip_key not in context.expected_trips
        ]
        if foreign_trip_dirs:
            context.skipped_trip_count = len(foreign_trip_dirs)
            snapshot = exclude_dirs(
                snapshot, [trip_dir.as_posix() for trip_dir in foreign_trip_dirs]
            )
            root = dataset_root(snapshot)

    report = validate_dataset(
        dataset_dir=root,
        allowed_root_files=frozenset(),
        camera_selection=_apply_camera_selection(args.camera_selection),
    )
    manifest_coverage = _collect_manifest_coverage(
        report,
        trip_filter=context.expected_trips,
        max_trips=args.manifest_max_trips,
        uploaded_only=args.manifest_uploaded_only,
    )
    return report, context, manifest_coverage


def _collect_manifest_coverage(
    report: ValidationReport,
    *,
    trip_filter: set[tuple[str, int, int]] | None,
    max_trips: int | None,
    uploaded_only: bool,
) -> ManifestCoverageResult | None:
    if report.unusable:
        return None
    try:
        saved_request = _load_saved_dataset_request(report.dataset_dir)
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return ManifestCoverageResult(
            note="manifest file coverage skipped: dataset_request.json unavailable",
        )
    return collect_manifest_missing_files(
        report.dataset_dir,
        saved_request,
        trip_filter=trip_filter,
        max_trips=max_trips,
        uploaded_only=uploaded_only,
    )


def _resolve_local_dataset_request() -> tuple[Path, str]:
    """Locate dataset_request.json locally, downloading it from the API if needed."""
    local_path = settings.output_dir / DATASET_REQUEST_FILENAME
    if local_path.is_file():
        return local_path, f"local copy at {local_path} (not found on S3)"

    api_key = settings.api_key.strip()
    dataset_id = settings.dataset_id.strip()
    if not api_key or not dataset_id:
        print(
            f"{DATASET_REQUEST_FILENAME} is not on S3 and no local copy exists at "
            f"{local_path}; set API_KEY and DATASET_ID in the active .env to download it.",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        dataset_request = get_dataset_request(api_key, dataset_id)
    except Exception as exc:
        print(
            f"Failed to download the dataset request from the API: {exc}",
            file=sys.stderr,
        )
        sys.exit(2)

    payload = {
        "request_id": dataset_request.request_id,
        "request": dataset_request.request,
    }
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return local_path, f"downloaded from the API to {local_path} (not found on S3)"


def _fetch_expected_trips() -> tuple[set[tuple[str, int, int]] | None, str | None]:
    """The dataset's full trip set from the API manifest, or a note on why not."""
    api_key = settings.api_key.strip()
    dataset_id = settings.dataset_id.strip()
    if not api_key or not dataset_id:
        return (
            None,
            "trip coverage skipped: API_KEY/DATASET_ID not set in the active .env",
        )
    try:
        ds_manifest = get_ds_manifest(api_key, dataset_id)
    except Exception as exc:
        return (
            None,
            f"trip coverage skipped: failed to fetch the dataset manifest: {exc}",
        )
    return {
        (trip.trip_id, trip.sub_trip, trip.health_grade) for trip in ds_manifest.trips
    }, None


def _print_report(
    report: ValidationReport,
    s3_context: S3ReportContext | None = None,
    manifest_coverage: ManifestCoverageResult | None = None,
) -> None:
    print(
        _render_report(
            report,
            verbose_trips=False,
            s3_context=s3_context,
            manifest_coverage=manifest_coverage,
        ),
        end="",
    )


def _render_report(
    report: ValidationReport,
    *,
    verbose_trips: bool,
    s3_context: S3ReportContext | None = None,
    manifest_coverage: ManifestCoverageResult | None = None,
) -> str:
    buffer = StringIO()
    write = buffer.write

    write("Dataset QA report\n")
    write(f"Dataset path: {s3_context.uri if s3_context else report.dataset_dir}\n")
    if s3_context is not None:
        if s3_context.request_note:
            write(f"Dataset request source: {s3_context.request_note}\n")
        if s3_context.expected_trips is not None:
            uploaded = len(s3_context.expected_trips & s3_context.uploaded_trips)
            write(f"Trips uploaded: {uploaded} / {len(s3_context.expected_trips)}\n")
            if s3_context.skipped_trip_count:
                write(
                    "Skipped trip folders from other datasets under the prefix: "
                    f"{s3_context.skipped_trip_count}\n"
                )
        elif s3_context.coverage_note:
            write(f"Note: {s3_context.coverage_note}\n")
    if manifest_coverage is not None:
        if manifest_coverage.note:
            write(f"Note: {manifest_coverage.note}\n")
        elif manifest_coverage.trip_summaries:
            incomplete = len(manifest_coverage.trip_summaries)
            write(f"Manifest file coverage: {incomplete} incomplete trip(s)\n")
    write("\n")

    _write_base_stats(write, report)
    _write_country_stats(write, report)
    if verbose_trips:
        _write_trip_coverage(write, s3_context)
        _write_manifest_trip_coverage(write, manifest_coverage)
    _write_trip_statuses(write, report, verbose_trips=verbose_trips)
    _write_missing_files(write, report, manifest_coverage)
    _write_issues(write, "Failures", report.all_failures, report.dataset_dir)
    _write_issues(write, "Warnings", report.all_warnings, report.dataset_dir)

    write("Overall\n")
    write(f"- status: {_overall_status(report, manifest_coverage)}\n")
    write(f"- warnings: {len(report.all_warnings)}\n")
    write(f"- failures: {len(report.all_failures)}\n")
    if manifest_coverage is not None:
        write(f"- manifest_missing_files: {len(manifest_coverage.missing_files)}\n")
        write(f"- manifest_missing_minutes: {len(manifest_coverage.missing_minutes)}\n")
        write(f"- manifest_missing_trips: {len(manifest_coverage.missing_trips)}\n")
        write(f"- manifest_archived_trips: {len(manifest_coverage.archived_trips)}\n")
        write(f"- manifest_restoring_trips: {len(manifest_coverage.restoring_trips)}\n")
    write(f"- exit_code: {_validation_exit_code(report, manifest_coverage)}\n")
    return buffer.getvalue()


def _write_base_stats(write, report: ValidationReport) -> None:
    summary = _build_duration_summary(report.trip_reports, report.dataset_dir)
    write("Base stats\n")
    write(f"- countries: {summary.countries}\n")
    write(f"- trips: {summary.trips}\n")
    write(f"- minutes: {summary.minutes}\n")
    write(
        f"- avg_trip_duration_minutes: {_format_duration(summary.avg_trip_duration_minutes)}\n"
    )
    write(f"- min_trip_duration_minutes: {summary.min_trip_duration_minutes}\n")
    write(f"- max_trip_duration_minutes: {summary.max_trip_duration_minutes}\n")
    write("\n")


def _write_country_stats(write, report: ValidationReport) -> None:
    country_groups = _group_trip_reports_by_country(report)
    if not country_groups:
        return

    write("Country stats\n")
    for country, trip_reports in sorted(country_groups.items()):
        summary = _build_duration_summary(trip_reports, report.dataset_dir)
        write(f"- {country}\n")
        write(f"  countries: {summary.countries}\n")
        write(f"  trips: {summary.trips}\n")
        write(f"  minutes: {summary.minutes}\n")
        write(
            f"  avg_trip_duration_minutes: {_format_duration(summary.avg_trip_duration_minutes)}\n"
        )
        write(f"  min_trip_duration_minutes: {summary.min_trip_duration_minutes}\n")
        write(f"  max_trip_duration_minutes: {summary.max_trip_duration_minutes}\n")
    write("\n")


def _write_trip_coverage(write, s3_context: S3ReportContext | None) -> None:
    if s3_context is None or s3_context.expected_trips is None:
        return

    missing = s3_context.missing_trips
    if missing:
        write("Missing trips (in the dataset manifest, not on S3)\n")
        for trip_key in missing:
            write(f"- {format_trip_key(trip_key)}\n")
        write("\n")

    unexpected = s3_context.unexpected_trips
    if unexpected:
        write("Unexpected trips (on S3, not in the dataset manifest)\n")
        for trip_key in unexpected:
            write(f"- {format_trip_key(trip_key)}\n")
        write("\n")


def _write_trip_statuses(
    write, report: ValidationReport, *, verbose_trips: bool
) -> None:
    trip_reports = report.trip_reports
    if verbose_trips:
        trip_reports = sorted(
            trip_reports, key=lambda trip_report: trip_report.trip_dir.as_posix()
        )
    else:
        trip_reports = [
            trip_report
            for trip_report in trip_reports
            if trip_report.failures or trip_report.warnings
        ]

    if not trip_reports:
        return

    write("Trip status\n")
    for trip_report in trip_reports:
        write(
            f"- {_display_path(report.dataset_dir, trip_report.trip_dir)}: "
            f"{trip_report.status} "
            f"(failures={len(trip_report.failures)}, warnings={len(trip_report.warnings)})\n"
        )
    write("\n")


def _write_manifest_trip_coverage(
    write,
    manifest_coverage: ManifestCoverageResult | None,
) -> None:
    if manifest_coverage is None:
        return

    if manifest_coverage.trip_summaries:
        write("Manifest trip coverage\n")
        for summary in manifest_coverage.trip_summaries:
            write(f"- {summary}\n")
        write("\n")

    if manifest_coverage.archived_trips:
        write(
            "Archived trips (not available in this dataset — contact the NATIX "
            "team if you need them, then re-run the download)\n"
        )
        for trip in manifest_coverage.archived_trips:
            write(f"- {trip}\n")
        write("\n")

    if manifest_coverage.restoring_trips:
        write(
            "Trips not ready yet (still being prepared — re-run the download "
            "later to complete them)\n"
        )
        for trip in manifest_coverage.restoring_trips:
            write(f"- {trip}\n")
        write("\n")

    if manifest_coverage.missing_trips:
        write("Missing trips (expected in the dataset, not downloaded)\n")
        for trip in manifest_coverage.missing_trips:
            write(f"- {trip}\n")
        write("\n")

    if manifest_coverage.missing_minutes:
        write("Missing minutes (in the trip manifest, not on disk)\n")
        for minute_path in manifest_coverage.missing_minutes:
            write(f"- {minute_path}\n")
        write("\n")

    if manifest_coverage.fetch_errors:
        write("Trip manifest fetch errors\n")
        for error in manifest_coverage.fetch_errors:
            write(f"- {error}\n")
        write("\n")


def _write_missing_files(
    write,
    report: ValidationReport,
    manifest_coverage: ManifestCoverageResult | None = None,
) -> None:
    missing_files = collect_missing_files(report.all_failures, report.dataset_dir)
    if manifest_coverage is not None:
        manifest_missing = [
            path
            for path in manifest_coverage.missing_files
            if not _should_omit_missing_file_report(path)
        ]
        missing_files = sorted(set(missing_files) | set(manifest_missing))
    if not missing_files:
        return

    write("Missing files\n")
    for path in missing_files:
        write(f"- {path}\n")
    write("\n")


def _write_issues(
    write,
    title: str,
    issues: list[ValidationIssue],
    dataset_dir: Path,
) -> None:
    if not issues:
        return

    write(f"{title}\n")
    for issue in sorted(
        issues, key=lambda current: _issue_sort_key(dataset_dir, current)
    ):
        location = _display_path(dataset_dir, issue.path)
        write(f"- {location}: {issue.message}\n")
    write("\n")


def _issue_sort_key(dataset_dir: Path, issue: ValidationIssue) -> tuple[str, str]:
    path_text = _display_path(dataset_dir, issue.path)
    return path_text, issue.message


def _display_path(dataset_dir: Path, path: Path | None) -> str:
    if path is None:
        return "."
    try:
        return str(path.relative_to(dataset_dir))
    except ValueError:
        return str(path)


def _build_duration_summary(
    trip_reports: list[TripReport], dataset_dir: Path
) -> DurationSummary:
    if not trip_reports:
        return DurationSummary()

    durations = [trip_report.minute_count for trip_report in trip_reports]
    countries = {
        _country_name(dataset_dir, trip_report) for trip_report in trip_reports
    }
    return DurationSummary(
        countries=len(countries),
        trips=len(trip_reports),
        minutes=sum(durations),
        avg_trip_duration_minutes=sum(durations) / len(durations),
        min_trip_duration_minutes=min(durations),
        max_trip_duration_minutes=max(durations),
    )


def _group_trip_reports_by_country(
    report: ValidationReport,
) -> dict[str, list[TripReport]]:
    country_groups: dict[str, list[TripReport]] = {}
    for trip_report in report.trip_reports:
        country = _country_name(report.dataset_dir, trip_report)
        country_groups.setdefault(country, []).append(trip_report)
    return country_groups


def _country_name(dataset_dir: Path, trip_report: TripReport) -> str:
    relative_parts = trip_report.trip_dir.relative_to(dataset_dir).parts
    if not relative_parts:
        return "unknown"
    return relative_parts[0]


def _format_duration(value: float) -> str:
    return f"{value:.2f}"


def _overall_status(
    report: ValidationReport,
    manifest_coverage: ManifestCoverageResult | None,
) -> str:
    if report.unusable:
        return "UNUSABLE"
    if report.all_failures or (manifest_coverage and manifest_coverage.missing_files):
        return "FAIL"
    if report.all_warnings:
        return "WARN"
    return "PASS"


def _validation_exit_code(
    report: ValidationReport,
    manifest_coverage: ManifestCoverageResult | None,
) -> int:
    if report.unusable:
        return 2
    if report.all_failures or (manifest_coverage and manifest_coverage.missing_files):
        return 1
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a downloaded dataset folder.",
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=None,
        help="Dataset directory to validate (defaults to output_dir from settings).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Write the full QA report to this file (includes every trip status).",
    )
    parser.add_argument(
        "--s3",
        nargs="?",
        const="",
        default=None,
        metavar="S3_URI",
        help=(
            "Validate the dataset uploaded to S3 instead of a local directory "
            "(read-only: LIST + GET only). Bare --s3 uses S3_UPLOAD from the "
            "active .env; an explicit s3://bucket/prefix overrides it. "
            "Takes precedence over --dataset-dir."
        ),
    )
    parser.add_argument(
        "--trip",
        type=str,
        default=None,
        metavar="TRIP",
        help=(
            "Limit --s3 validation to a single trip '<trip_id>_<sub_trip>' under the "
            "prefix (other trip folders are excluded from the report)."
        ),
    )
    parser.add_argument(
        "--manifest-max-trips",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Limit manifest file coverage to the first N trips (uploaded trips "
            "first when combined with --manifest-uploaded-only)."
        ),
    )
    parser.add_argument(
        "--camera-selection",
        type=CAMERA_SELECTION,
        choices=list(CAMERA_SELECTION),
        default=None,
        metavar="{front,front_rear,all}",
        help=(
            "Which cameras each minute is required to have. Omit to use "
            "CAMERA_SELECTION from the active .env (default 'all'). Pass the "
            "same value that was used for the download."
        ),
    )
    parser.add_argument(
        "--manifest-uploaded-only",
        action="store_true",
        help=(
            "Limit manifest file coverage to trips that exist on disk/S3, "
            "instead of every trip in the dataset manifest."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
