from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from typing import Any

from schemas.dataset_request import (
    DatasetOutput,
    DatasetRequest,
    SavedDatasetRequest,
    SEIDataOptions,
)
from schemas.trip_metadata import CAMERA_TYPE
from utils.settings import settings

TRIP_DIR_RE = re.compile(r"(?P<trip_id>.+)_(?P<sub_trip>\d+)$")
MINUTE_DIR_RE = re.compile(r"\d{2}-\d{2}-\d{2}$")
VIDEO_LINK_SUFFIX = ".mp4.link"
EXPECTED_CAMERA_DIR_NAMES = {
    f"{camera_type.value.upper()}_FOLDER" for camera_type in CAMERA_TYPE
}
SeiPolicy = Literal["each-minute", "any-minute", "none"]
DATASET_REQUEST_FILENAME = "dataset_request.json"
DEFAULT_QA_REPORT_FILENAME = "qa_report.txt"
ALLOWED_DATASET_ROOT_FILENAMES = frozenset(
    {DATASET_REQUEST_FILENAME, DEFAULT_QA_REPORT_FILENAME}
)


@dataclass(slots=True)
class ValidationIssue:
    message: str
    path: Path | None = None


@dataclass(slots=True)
class ValidationStats:
    country_dirs: int = 0
    state_dirs: int = 0
    trip_dirs: int = 0
    minute_dirs: int = 0
    camera_dirs: int = 0
    video_files: int = 0
    link_files: int = 0
    telemetry_csv_files: int = 0
    telemetry_mcap_files: int = 0
    sei_files: int = 0
    fused_sei_files: int = 0
    fixed_metadata_csv_files: int = 0
    fixed_metadata_json_files: int = 0
    fixed_metadata_mcap_files: int = 0
    trip_insight_files: int = 0
    mapping_files: int = 0


@dataclass(slots=True)
class ValidationPolicies:
    raw_sei_policy: SeiPolicy = "none"
    fused_sei_policy: SeiPolicy = "none"


def _load_saved_dataset_request(dataset_dir: Path) -> SavedDatasetRequest:
    request_path = dataset_dir / DATASET_REQUEST_FILENAME
    if not request_path.is_file():
        raise FileNotFoundError(DATASET_REQUEST_FILENAME)
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    return SavedDatasetRequest.model_validate(payload)


def validation_policies_from_request(
    dataset_request: DatasetRequest,
) -> ValidationPolicies:
    trips = dataset_request.filters.trips
    footage = dataset_request.output.footage

    # AND the output actually includes that SEI stream. A filter never forces
    # output on its own — output.footage.sei / .fused_sei gate the requirement.
    raw_policy: SeiPolicy = "none"
    if trips is not None and footage.sei is not None:
        if trips.seiData == SEIDataOptions.all_data:
            raw_policy = "each-minute"
        elif trips.seiData == SEIDataOptions.with_data:
            raw_policy = "any-minute"

    fused_policy: SeiPolicy = "none"
    if trips is not None and footage.fused_sei is not None:
        if trips.fusedSeiData == SEIDataOptions.all_data:
            fused_policy = "each-minute"
        elif trips.fusedSeiData == SEIDataOptions.with_data:
            fused_policy = "any-minute"

    return ValidationPolicies(raw_sei_policy=raw_policy, fused_sei_policy=fused_policy)


def _is_populated(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, dict, str)):
        return len(value) > 0
    return True


def _validate_request_features(
    trip_report: TripReport,
    trip_dir: Path,
    dataset_request: DatasetRequest,
) -> None:
    output = dataset_request.output

    wants_fixed_metadata = output.fixed_metadata is not None
    trip_insight = output.trip_insight
    wants_base_insights = trip_insight.base_insights if trip_insight else False
    wants_safety_critical = (
        trip_insight.safety_critical_events if trip_insight else False
    )
    events_filter = (
        dataset_request.filters.trips.safety_critical_events
        if dataset_request.filters.trips
        else None
    )
    wants_ego_actions = trip_insight.ego_actions if trip_insight else False
    ego_filter = (
        dataset_request.filters.trips.ego_actions
        if dataset_request.filters.trips
        else None
    )

    if not wants_fixed_metadata:
        for filename in (
            "fixed_metadata.csv",
            "fixed_metadata.json",
            "fixed_metadata.mcap",
        ):
            path = trip_dir / filename
            if path.exists():
                _add_failure(
                    trip_report,
                    "fixed_metadata output must not exist when output.fixed_metadata is None.",
                    path,
                )

    trip_insight_path = trip_dir / "trip_insight.json"
    if not trip_insight_path.exists():
        if wants_base_insights or wants_safety_critical or wants_ego_actions:
            _add_failure(
                trip_report,
                "Missing trip_insight.json required for requested trip insight features.",
                trip_insight_path,
            )
        return

    try:
        payload = json.loads(trip_insight_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
        _add_failure(
            trip_report,
            f"trip_insight.json could not be parsed: {exc}",
            trip_insight_path,
        )
        return

    if not isinstance(payload, dict):
        _add_failure(
            trip_report,
            "trip_insight.json top-level payload must be an object.",
            trip_insight_path,
        )
        return

    if not wants_base_insights and _is_populated(payload.get("base_insights")):
        _add_failure(
            trip_report,
            "base_insights must not be populated when output.trip_insight.base_insights is false.",
            trip_insight_path,
        )
    elif wants_base_insights and not _is_populated(payload.get("base_insights")):
        _add_failure(
            trip_report,
            "base_insights must be populated when output.trip_insight.base_insights is true.",
            trip_insight_path,
        )

    _validate_event_features(
        trip_report,
        trip_insight_path,
        payload,
        key="safety_critical_events",
        wants=wants_safety_critical,
        events_filter=events_filter,
        label="safety-critical",
    )
    _validate_event_features(
        trip_report,
        trip_insight_path,
        payload,
        key="ego_actions",
        wants=wants_ego_actions,
        events_filter=ego_filter,
        label="ego-action",
    )


def _validate_event_features(
    trip_report: TripReport,
    trip_insight_path: Path,
    payload: dict,
    *,
    key: str,
    wants: bool,
    events_filter: Any,
    label: str,
) -> None:
    events = payload.get(key)
    if not wants:
        if _is_populated(events):
            _add_failure(
                trip_report,
                f"{key} must not be populated when "
                f"output.trip_insight.{key} is not set.",
                trip_insight_path,
            )
        return

    if not isinstance(events, list):
        _add_failure(
            trip_report,
            f"{key} must be a list when output.trip_insight.{key} is set.",
            trip_insight_path,
        )
        return

    if events_filter is None:
        return

    filter_names = events_filter.names
    if filter_names is None:
        if not events:
            _add_failure(
                trip_report,
                f"{key} must be non-empty when output.trip_insight.{key} is set.",
                trip_insight_path,
            )
        return

    # trip_insight.json holds the request-facing event names (safety-critical is
    # projected internal -> public by the formatter; ego-action names pass through
    # unchanged), so we match against the requested names directly.
    allowed_names = set(filter_names)
    matching_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("name") in allowed_names
    ]
    if len(matching_events) < events_filter.minEventCount:
        _add_failure(
            trip_report,
            (
                "Expected at least "
                f"{events_filter.minEventCount} {label} events matching the request filter, "
                f"found {len(matching_events)}."
            ),
            trip_insight_path,
        )

    for event in events:
        if not isinstance(event, dict):
            continue
        event_name = event.get("name")
        if event_name is None or event_name in allowed_names:
            continue
        _add_warning(
            trip_report,
            (
                f"{label.capitalize()} event "
                f"'{event_name}'"
                f" is outside the requested output.trip_insight.{key} filter."
            ),
            trip_insight_path,
        )


@dataclass(slots=True)
class MinuteSeiStatus:
    camera_dirs: list[Path]
    has_fused: bool
    raw_sei_camera_count: int

    @property
    def camera_count(self) -> int:
        return len(self.camera_dirs)

    @property
    def has_complete_raw_sei(self) -> bool:
        return self.camera_count > 0 and self.raw_sei_camera_count == self.camera_count


@dataclass(slots=True)
class TripReport:
    trip_dir: Path
    minute_count: int = 0
    minutes_with_complete_raw_sei: int = 0
    minutes_with_fused_sei: int = 0
    failures: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.failures:
            return "FAIL"
        if self.warnings:
            return "WARN"
        return "PASS"


@dataclass(slots=True)
class ValidationReport:
    dataset_dir: Path
    stats: ValidationStats = field(default_factory=ValidationStats)
    failures: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)
    trip_reports: list[TripReport] = field(default_factory=list)
    policies: ValidationPolicies = field(default_factory=ValidationPolicies)
    unusable: bool = False

    @property
    def all_failures(self) -> list[ValidationIssue]:
        issues = list(self.failures)
        for trip_report in self.trip_reports:
            issues.extend(trip_report.failures)
        return issues

    @property
    def all_warnings(self) -> list[ValidationIssue]:
        issues = list(self.warnings)
        for trip_report in self.trip_reports:
            issues.extend(trip_report.warnings)
        return issues

    @property
    def status(self) -> str:
        if self.unusable or self.all_failures:
            return "FAIL"
        if self.all_warnings:
            return "WARN"
        return "PASS"


def validate_dataset(
    dataset_dir: Path | None = None,
    allowed_root_files: frozenset[Path] | None = None,
) -> ValidationReport:
    target_dir = (dataset_dir or settings.output_dir).resolve()
    report = ValidationReport(dataset_dir=target_dir)

    if not target_dir.exists():
        report.unusable = True
        _add_failure(
            report, f"Dataset directory does not exist: {target_dir}", target_dir
        )
        return report
    if not target_dir.is_dir():
        report.unusable = True
        _add_failure(
            report, f"Dataset path is not a directory: {target_dir}", target_dir
        )
        return report

    top_level_entries = list(target_dir.iterdir())
    if not top_level_entries:
        report.unusable = True
        _add_failure(report, "Dataset directory is empty.", target_dir)
        return report

    try:
        saved_request = _load_saved_dataset_request(target_dir)
    except FileNotFoundError:
        report.unusable = True
        _add_failure(
            report,
            f"{DATASET_REQUEST_FILENAME} not found; validation cannot run without dataset request criteria.",
            target_dir,
        )
        return report
    except (json.JSONDecodeError, ValueError) as exc:
        report.unusable = True
        _add_failure(
            report,
            f"{DATASET_REQUEST_FILENAME} is invalid: {exc}",
            target_dir / DATASET_REQUEST_FILENAME,
        )
        return report

    active_policies = validation_policies_from_request(saved_request.request)
    report.policies = active_policies

    trip_dirs = _discover_trip_dirs(target_dir)
    if not trip_dirs:
        report.unusable = True
        _add_failure(
            report,
            "No trip folders matching '<trip_id>_<sub_trip>' were found.",
            target_dir,
        )
        return report

    region_dirs = _collect_region_dirs(target_dir, trip_dirs)
    _validate_region_dirs(report, region_dirs, trip_dirs, allowed_root_files)
    _populate_region_stats(report, trip_dirs)

    for trip_dir in trip_dirs:
        trip_report = _validate_trip_dir(
            report, trip_dir, active_policies, saved_request.request
        )
        report.trip_reports.append(trip_report)

    report.trip_reports.sort(key=lambda trip_report: trip_report.trip_dir.as_posix())
    report.stats.trip_dirs = len(report.trip_reports)
    return report


def validation_exit_code(report: ValidationReport) -> int:
    if report.unusable:
        return 2
    if report.all_failures:
        return 1
    return 0


def validate_trip(
    dataset_dir: Path, trip_dir: Path, *, check_layout: bool = True
) -> TripReport:
    """Validate a single trip against the saved dataset_request.json.

    Reuses the same request loading + policy derivation + per-trip checks as
    ``validate_dataset``. ``dataset_dir`` must contain ``dataset_request.json``
    at its root (written by ``app.main`` before trips are processed). Raises
    ``FileNotFoundError`` if that file is missing.

    ``check_layout`` guards the country/state nesting checks, which require
    ``trip_dir`` to sit under ``dataset_dir``. Pass ``check_layout=False`` to
    validate a trip that is still in the flat cache dir (pre-move); the layout is
    re-checked by ``validate_dataset`` on the finished tree.
    """
    target = dataset_dir.resolve()
    saved_request = _load_saved_dataset_request(target)
    policies = validation_policies_from_request(saved_request.request)
    report = ValidationReport(dataset_dir=target)
    return _validate_trip_dir(
        report,
        trip_dir.resolve(),
        policies,
        saved_request.request,
        check_layout=check_layout,
    )


def _discover_trip_dirs(dataset_dir: Path) -> list[Path]:
    """Find trip folders via country/state layout without walking minute/camera files."""
    discovered: list[Path] = []
    for country_dir in sorted(p for p in dataset_dir.iterdir() if p.is_dir()):
        for entry in sorted(country_dir.iterdir()):
            if not entry.is_dir():
                continue
            if TRIP_DIR_RE.fullmatch(entry.name):
                discovered.append(entry)
                continue
            for trip_dir in sorted(entry.iterdir()):
                if trip_dir.is_dir() and TRIP_DIR_RE.fullmatch(trip_dir.name):
                    discovered.append(trip_dir)
    return discovered


def _collect_region_dirs(dataset_dir: Path, trip_dirs: list[Path]) -> set[Path]:
    region_dirs: set[Path] = {dataset_dir}
    for trip_dir in trip_dirs:
        relative_parts = trip_dir.relative_to(dataset_dir).parts
        parent = dataset_dir
        for part in relative_parts[:-1]:
            parent = parent / part
            region_dirs.add(parent)
    return region_dirs


def _populate_region_stats(report: ValidationReport, trip_dirs: list[Path]) -> None:
    countries: set[str] = set()
    states: set[tuple[str, str]] = set()

    for trip_dir in trip_dirs:
        relative_parts = trip_dir.relative_to(report.dataset_dir).parts
        if relative_parts:
            countries.add(relative_parts[0])
        if len(relative_parts) >= 3 and relative_parts[0] == "United States":
            states.add((relative_parts[0], relative_parts[1]))

    report.stats.country_dirs = len(countries)
    report.stats.state_dirs = len(states)


def _is_allowed_dataset_root_file(
    file_path: Path,
    dataset_dir: Path,
    allowed_root_files: frozenset[Path] | None,
) -> bool:
    if file_path.parent.resolve() != dataset_dir.resolve():
        return False
    if file_path.name in ALLOWED_DATASET_ROOT_FILENAMES:
        return True
    if not allowed_root_files:
        return False
    return file_path.resolve() in {path.resolve() for path in allowed_root_files}


def _validate_region_dirs(
    report: ValidationReport,
    region_dirs: set[Path],
    trip_dirs: list[Path],
    allowed_root_files: frozenset[Path] | None = None,
) -> None:
    trip_dir_set = set(trip_dirs)
    for region_dir in sorted(region_dirs):
        for child in sorted(region_dir.iterdir()):
            if child.is_file():
                if (
                    region_dir.resolve() == report.dataset_dir.resolve()
                    and _is_allowed_dataset_root_file(
                        child,
                        report.dataset_dir,
                        allowed_root_files,
                    )
                ):
                    continue
                _add_warning(
                    report,
                    "Unexpected file outside a trip folder.",
                    child,
                )
                continue
            if child in trip_dir_set or child in region_dirs:
                continue
            if child.is_dir():
                _add_warning(
                    report,
                    "Unexpected directory outside the canonical country/state/trip layout.",
                    child,
                )


def _validate_trip_dir(
    report: ValidationReport,
    trip_dir: Path,
    policies: ValidationPolicies,
    dataset_request: DatasetRequest,
    *,
    check_layout: bool = True,
) -> TripReport:
    trip_report = TripReport(trip_dir=trip_dir)

    # Country/state nesting checks only make sense once the trip sits under the
    # dataset root. When validating a trip still in the (flat) cache dir before the
    # move, skip them — relative_to would raise there anyway, and validate_dataset
    # re-checks the layout on the finished tree.
    if check_layout:
        relative_parts = trip_dir.relative_to(report.dataset_dir).parts
        if len(relative_parts) not in {2, 3}:
            _add_warning(
                trip_report,
                "Trip folder nesting is unexpected; expected country/trip or country/state/trip.",
                trip_dir,
            )
        elif len(relative_parts) == 3 and relative_parts[0] != "United States":
            _add_warning(
                trip_report,
                "State-level nesting is only expected for 'United States' trips.",
                trip_dir,
            )

    minute_dirs = sorted(
        child
        for child in trip_dir.iterdir()
        if child.is_dir() and MINUTE_DIR_RE.fullmatch(child.name)
    )
    other_dirs = sorted(
        child
        for child in trip_dir.iterdir()
        if child.is_dir() and not MINUTE_DIR_RE.fullmatch(child.name)
    )

    if not minute_dirs:
        _add_failure(trip_report, "Trip does not contain any minute folders.", trip_dir)

    for unexpected_dir in other_dirs:
        if unexpected_dir.name == "features":
            _add_warning(
                trip_report,
                "Unexpected features directory found in final output.",
                unexpected_dir,
            )
        else:
            _add_warning(
                trip_report,
                "Unexpected directory inside trip folder.",
                unexpected_dir,
            )

    report.stats.minute_dirs += len(minute_dirs)
    trip_report.minute_count = len(minute_dirs)
    _validate_trip_metadata_files(report, trip_report, trip_dir, dataset_request)

    for minute_dir in minute_dirs:
        minute_status = _validate_minute_dir(
            report, trip_report, minute_dir, policies, dataset_request.output
        )
        if minute_status.has_complete_raw_sei:
            trip_report.minutes_with_complete_raw_sei += 1
        if minute_status.has_fused:
            trip_report.minutes_with_fused_sei += 1

    _validate_trip_sei_policies(trip_report, trip_dir, policies)
    return trip_report


def _validate_trip_sei_policies(
    trip_report: TripReport,
    trip_dir: Path,
    policies: ValidationPolicies,
) -> None:
    if trip_report.minute_count == 0:
        return

    if (
        policies.raw_sei_policy == "any-minute"
        and trip_report.minutes_with_complete_raw_sei == 0
    ):
        _add_failure(
            trip_report,
            "Trip must contain raw SEI for at least one fully covered minute, but none were found.",
            trip_dir,
        )

    if (
        policies.fused_sei_policy == "any-minute"
        and trip_report.minutes_with_fused_sei == 0
    ):
        _add_failure(
            trip_report,
            "Trip must contain fused SEI for at least one minute, but none were found.",
            trip_dir,
        )


def _validate_configured_trip_file(
    report: ValidationReport,
    trip_report: TripReport,
    path: Path,
    *,
    enabled: bool,
    stat_attr: str,
    missing_message: str,
    disabled_message: str,
) -> None:
    if enabled:
        if not path.exists():
            _add_failure(trip_report, missing_message, path)
            return
        setattr(report.stats, stat_attr, getattr(report.stats, stat_attr) + 1)
        _validate_non_empty_file(trip_report, path)
        return

    if path.exists():
        setattr(report.stats, stat_attr, getattr(report.stats, stat_attr) + 1)
        _add_warning(trip_report, disabled_message, path)


def _validate_trip_metadata_files(
    report: ValidationReport,
    trip_report: TripReport,
    trip_dir: Path,
    dataset_request: DatasetRequest,
) -> None:
    allowed_files = {
        "fixed_metadata.csv",
        "fixed_metadata.json",
        "fixed_metadata.mcap",
        "trip_insight.json",
        "minute_metadata.proto",
        "trip_metadata.proto",
    }

    output = dataset_request.output
    _validate_configured_trip_file(
        report,
        trip_report,
        trip_dir / "fixed_metadata.csv",
        enabled=output.fixed_metadata is not None
        and output.fixed_metadata.csv is not None,
        stat_attr="fixed_metadata_csv_files",
        missing_message="Missing fixed_metadata.csv.",
        disabled_message="fixed_metadata.csv exists even though CSV output is disabled in the dataset request.",
    )
    _validate_configured_trip_file(
        report,
        trip_report,
        trip_dir / "fixed_metadata.json",
        enabled=output.fixed_metadata is not None
        and output.fixed_metadata.json_format is not None,
        stat_attr="fixed_metadata_json_files",
        missing_message="Missing fixed_metadata.json.",
        disabled_message="fixed_metadata.json exists even though JSON output is disabled in the dataset request.",
    )
    _validate_configured_trip_file(
        report,
        trip_report,
        trip_dir / "fixed_metadata.mcap",
        enabled=output.fixed_metadata is not None
        and output.fixed_metadata.mcap is not None,
        stat_attr="fixed_metadata_mcap_files",
        missing_message="Missing fixed_metadata.mcap.",
        disabled_message="fixed_metadata.mcap exists even though MCAP output is disabled in the dataset request.",
    )
    _validate_configured_trip_file(
        report,
        trip_report,
        trip_dir / "trip_insight.json",
        enabled=output.trip_insight is not None,
        stat_attr="trip_insight_files",
        missing_message="Missing trip_insight.json.",
        disabled_message="trip_insight.json exists even though trip insight output is disabled in the dataset request.",
    )

    _validate_request_features(trip_report, trip_dir, dataset_request)

    for child in sorted(trip_dir.iterdir()):
        if not child.is_file():
            continue
        if child.name in allowed_files:
            continue
        _add_warning(trip_report, "Unexpected file inside trip folder.", child)


def _validate_minute_dir(
    report: ValidationReport,
    trip_report: TripReport,
    minute_dir: Path,
    policies: ValidationPolicies,
    output: DatasetOutput,
) -> MinuteSeiStatus:
    camera_dirs = sorted(
        child
        for child in minute_dir.iterdir()
        if child.is_dir() and child.name in EXPECTED_CAMERA_DIR_NAMES
    )
    unknown_dirs = sorted(
        child
        for child in minute_dir.iterdir()
        if child.is_dir() and child.name not in EXPECTED_CAMERA_DIR_NAMES
    )

    for unknown_dir in unknown_dirs:
        _add_warning(
            trip_report,
            "Unexpected directory inside minute folder.",
            unknown_dir,
        )

    report.stats.camera_dirs += len(camera_dirs)

    mapping_path = minute_dir / "mapping.txt"
    camera_count = len(camera_dirs)
    if camera_count == 0:
        _add_failure(
            trip_report,
            "Minute folder does not contain any camera folders.",
            minute_dir,
        )
    elif camera_count not in {4, 6}:
        _add_failure(
            trip_report,
            f"Minute folder must contain exactly 4 or 6 camera folders, found {camera_count}.",
            minute_dir,
        )

    if output.minute.mapping_file:
        if not mapping_path.exists():
            _add_failure(trip_report, "Missing mapping.txt.", mapping_path)
        else:
            report.stats.mapping_files += 1
            _validate_non_empty_file(trip_report, mapping_path)
    elif mapping_path.exists():
        report.stats.mapping_files += 1
        _add_warning(
            trip_report,
            "mapping.txt exists even though minute.mapping_file is disabled in the dataset request.",
            mapping_path,
        )

    fused_files = sorted(
        child
        for child in minute_dir.iterdir()
        if child.is_file()
        and child.name.startswith("FUSED_")
        and child.name.endswith(".sei.csv")
    )
    if len(fused_files) > 1:
        _add_warning(
            trip_report,
            "Multiple fused SEI files found in one minute folder.",
            minute_dir,
        )
    has_valid_fused = False
    for fused_file in fused_files:
        report.stats.fused_sei_files += 1
        if _validate_non_empty_file(
            trip_report, fused_file, message="Fused SEI CSV is empty."
        ):
            has_valid_fused = True

    raw_sei_camera_count = 0
    camera_contexts: list[tuple[Path, bool]] = []
    for camera_dir in camera_dirs:
        has_sei = _validate_camera_dir(report, trip_report, camera_dir, output)
        camera_contexts.append((camera_dir, has_sei))
        raw_sei_camera_count += int(has_sei)

    if policies.raw_sei_policy == "each-minute":
        if raw_sei_camera_count != camera_count:
            _add_failure(
                trip_report,
                f"Minute folder must contain raw SEI for all {camera_count} camera folders, found {raw_sei_camera_count}.",
                minute_dir,
            )
    elif raw_sei_camera_count and raw_sei_camera_count != camera_count:
        for camera_dir, has_sei in camera_contexts:
            if has_sei:
                continue
            _add_warning(
                trip_report,
                "Missing per-camera SEI file in a minute with partial raw SEI coverage.",
                camera_dir,
            )

    if policies.fused_sei_policy == "each-minute" and not has_valid_fused:
        _add_failure(
            trip_report,
            "Minute folder is missing a valid fused SEI file.",
            minute_dir,
        )

    allowed_files = {"mapping.txt", *(path.name for path in fused_files)}
    for child in sorted(minute_dir.iterdir()):
        if child.is_dir():
            continue
        if child.name in allowed_files:
            continue
        _add_warning(trip_report, "Unexpected file inside minute folder.", child)

    return MinuteSeiStatus(
        camera_dirs=camera_dirs,
        has_fused=has_valid_fused,
        raw_sei_camera_count=raw_sei_camera_count,
    )


def _validate_camera_dir(
    report: ValidationReport,
    trip_report: TripReport,
    camera_dir: Path,
    output: DatasetOutput,
) -> bool:
    nested_dirs = [child for child in camera_dir.iterdir() if child.is_dir()]
    for nested_dir in nested_dirs:
        _add_warning(
            trip_report, "Unexpected nested directory inside camera folder.", nested_dir
        )

    files = sorted(child for child in camera_dir.iterdir() if child.is_file())
    video_files = [path for path in files if path.suffix == ".mp4"]
    link_files = [path for path in files if path.name.endswith(VIDEO_LINK_SUFFIX)]
    telemetry_csv_files = [
        path
        for path in files
        if path.suffix == ".csv" and not path.name.endswith(".sei.csv")
    ]
    telemetry_mcap_files = [path for path in files if path.suffix == ".mcap"]
    sei_files = [path for path in files if path.name.endswith(".sei.csv")]

    report.stats.video_files += len(video_files)
    report.stats.link_files += len(link_files)
    report.stats.telemetry_csv_files += len(telemetry_csv_files)
    report.stats.telemetry_mcap_files += len(telemetry_mcap_files)
    report.stats.sei_files += len(sei_files)

    if not video_files and not link_files:
        _add_failure(
            trip_report,
            "Camera folder is missing both the video file and the .link file.",
            camera_dir,
        )
    if video_files and link_files:
        _add_warning(
            trip_report,
            "Camera folder contains both a real video file and a .link file.",
            camera_dir,
        )
    if len(video_files) + len(link_files) > 1:
        _add_warning(
            trip_report,
            "Camera folder contains multiple video or .link files.",
            camera_dir,
        )

    for video_file in video_files:
        _validate_non_empty_file(trip_report, video_file)
    for link_file in link_files:
        _validate_non_empty_file(trip_report, link_file)

    telemetry_csv_path: Path | None = None
    if output.footage.csv is not None:
        telemetry_csv_path = _pick_single_file(
            trip_report,
            telemetry_csv_files,
            camera_dir,
            expected_label="telemetry CSV",
        )
        if telemetry_csv_path is not None:
            _validate_non_empty_file(trip_report, telemetry_csv_path)
    elif telemetry_csv_files:
        for telemetry_csv_file in telemetry_csv_files:
            _add_warning(
                trip_report,
                "Telemetry CSV exists even though footage.csv is disabled in the dataset request.",
                telemetry_csv_file,
            )

    telemetry_mcap_path: Path | None = None
    if output.footage.mcap is not None:
        telemetry_mcap_path = _pick_single_file(
            trip_report,
            telemetry_mcap_files,
            camera_dir,
            expected_label="telemetry MCAP",
        )
        if telemetry_mcap_path is not None:
            _validate_non_empty_file(trip_report, telemetry_mcap_path)
    elif telemetry_mcap_files:
        for telemetry_mcap_file in telemetry_mcap_files:
            _add_warning(
                trip_report,
                "Telemetry MCAP exists even though footage.mcap is disabled in the dataset request.",
                telemetry_mcap_file,
            )

    if len(sei_files) > 1:
        _add_warning(
            trip_report,
            "Camera folder contains multiple SEI CSV files.",
            camera_dir,
        )
    has_valid_sei = bool(sei_files) and all(
        _validate_non_empty_file(trip_report, sei_file, message="SEI CSV is empty.")
        for sei_file in sei_files
    )

    allowed_names = {path.name for path in video_files + link_files + sei_files}
    if telemetry_csv_path is not None:
        allowed_names.add(telemetry_csv_path.name)
    if telemetry_mcap_path is not None:
        allowed_names.add(telemetry_mcap_path.name)

    for child in files:
        if child.name in allowed_names:
            continue
        _add_warning(trip_report, "Unexpected file inside camera folder.", child)

    return has_valid_sei


def _pick_single_file(
    trip_report: TripReport,
    files: list[Path],
    parent_dir: Path,
    expected_label: str,
) -> Path | None:
    if not files:
        _add_failure(
            trip_report,
            f"Missing {expected_label} in camera folder.",
            parent_dir,
        )
        return None
    if len(files) > 1:
        _add_failure(
            trip_report,
            f"Found multiple {expected_label} files in camera folder.",
            parent_dir,
        )
        return None
    return files[0]


def _validate_non_empty_file(
    issues_owner: ValidationReport | TripReport,
    path: Path,
    message: str | None = None,
) -> bool:
    if not path.exists():
        _add_failure(issues_owner, "Expected file is missing.", path)
        return False
    if path.stat().st_size <= 0:
        _add_failure(issues_owner, message or "Required file is empty.", path)
        return False
    return True


def _add_failure(
    issues_owner: ValidationReport | TripReport,
    message: str,
    path: Path | None = None,
) -> None:
    issues_owner.failures.append(ValidationIssue(message=message, path=path))


def _add_warning(
    issues_owner: ValidationReport | TripReport,
    message: str,
    path: Path | None = None,
) -> None:
    issues_owner.warnings.append(ValidationIssue(message=message, path=path))


def collect_missing_files(
    failures: list[ValidationIssue],
    dataset_dir: Path,
) -> list[str]:
    """Return sorted, deduplicated dataset-relative paths for missing expected files."""
    entries: list[str] = []
    seen: set[str] = set()
    for issue in failures:
        entry = _missing_file_entry(dataset_dir, issue)
        if entry is None or entry in seen:
            continue
        seen.add(entry)
        entries.append(entry)
    return sorted(entries)


def _missing_file_entry(dataset_dir: Path, issue: ValidationIssue) -> str | None:
    if issue.path is None or _is_aggregate_missing_summary(issue.message):
        return None
    if not _is_missing_file_issue(issue.message):
        return None

    rel = _relative_display_path(dataset_dir, issue.path)
    if _should_omit_missing_file_report(rel):
        return None
    if issue.path.suffix:
        return rel
    if "missing both the video file" in issue.message:
        return f"{rel}/*.mp4"
    if "Missing raw SEI CSV in camera folder." in issue.message:
        return f"{rel}/*.sei.csv"
    if "Missing telemetry CSV in camera folder." in issue.message:
        return f"{rel}/*.csv (telemetry)"
    if "Missing telemetry MCAP in camera folder." in issue.message:
        return f"{rel}/*.mcap"
    return rel


def _is_aggregate_missing_summary(message: str) -> bool:
    return any(
        marker in message
        for marker in (
            "Minute folder must contain raw SEI for all",
            "Trip must contain raw SEI for at least one",
            "Trip must contain fused SEI for at least one",
            "Minute folder must contain exactly",
            "Minute folder does not contain any camera folders.",
        )
    )


def _is_missing_file_issue(message: str) -> bool:
    return any(
        marker in message
        for marker in (
            "Missing ",
            "Expected file is missing.",
            "missing both the video file",
        )
    )


def _relative_display_path(dataset_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(dataset_dir).as_posix()
    except ValueError:
        return path.as_posix()


def _should_omit_missing_file_report(path: str) -> bool:
    return path.endswith(".mp4.link") or "/*.mp4.link" in path
