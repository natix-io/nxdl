from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from app.trip_downloader import get_ds_manifest, get_trip_manifest
from app.validation import _discover_trip_dirs, parse_trip_dir_name
from schemas.dataset_request import DatasetOutput, SavedDatasetRequest
from schemas.trip_manifest import TripManifest
from utils.path_helpers import (
    build_camera_directory_path,
    build_camera_file_path,
    build_camera_video_path,
    build_fused_sei_path,
    build_minute_directory_path,
    build_trip_insight_path,
    is_camera_allowed,
)
from utils.settings import settings
from utils.trip_list import format_trip_key


@dataclass(slots=True)
class ManifestCoverageResult:
    note: str | None = None
    missing_files: list[str] = field(default_factory=list)
    missing_minutes: list[str] = field(default_factory=list)
    # Trips in the dataset manifest that are entirely absent on disk/S3.
    missing_trips: list[str] = field(default_factory=list)
    trip_summaries: list[str] = field(default_factory=list)
    fetch_errors: list[str] = field(default_factory=list)


def build_expected_trip_files(
    trip_manifest: TripManifest,
    output: DatasetOutput,
    *,
    dry_mode: bool,
) -> list[Path]:
    """Return paths relative to the trip directory that a completed download should contain."""
    expected: list[Path] = []
    trip_root = Path(".")
    timezone = trip_manifest.timezone
    if timezone is None:
        return expected

    if output.fixed_metadata is not None:
        if output.fixed_metadata.csv is not None:
            expected.append(Path("fixed_metadata.csv"))
        if output.fixed_metadata.json_format is not None:
            expected.append(Path("fixed_metadata.json"))
        if output.fixed_metadata.mcap is not None:
            expected.extend(
                [
                    Path("fixed_metadata.mcap"),
                    Path("minute_metadata.proto"),
                    Path("trip_metadata.proto"),
                ]
            )

    if output.trip_insight is not None and trip_manifest.base_insights is not None:
        expected.append(build_trip_insight_path(trip_root).relative_to(trip_root))

    for minute in trip_manifest.minutes:
        if minute.minute_epoch is None:
            continue

        minute_dir = build_minute_directory_path(
            trip_root, minute.minute_epoch, timezone
        )

        if output.footage.fused_sei is not None and minute.fused_sei is not None:
            expected.append(
                build_fused_sei_path(
                    minute_dir, minute.minute_epoch, timezone
                ).relative_to(trip_root)
            )

        if output.minute.mapping_file:
            expected.append((minute_dir / "mapping.txt").relative_to(trip_root))

        for camera in minute.cameras:
            if not is_camera_allowed(camera.camera_type):
                continue
            camera_type = camera.camera_type or "unknown"
            camera_dir = build_camera_directory_path(minute_dir, camera_type)
            video_path = build_camera_video_path(
                camera_dir,
                camera_type,
                minute.minute_epoch,
                timezone,
            )
            expected.append(video_path.relative_to(trip_root))

            # Only the CSV format keeps the telemetry file in the delivered dataset:
            # with footage.csv disabled it is either never downloaded or deleted after
            # EXIF/MCAP consume it (see app.ds_formatter), so it must not be expected.
            if (
                trip_manifest.health_grade != 0
                and camera.telemetry is not None
                and output.footage.csv is not None
            ):
                telemetry_path = build_camera_file_path(
                    camera_dir, camera.telemetry.presigned_url
                )
                expected.append(telemetry_path.relative_to(trip_root))

            if output.footage.sei is not None and camera.sei is not None:
                sei_path = video_path.with_suffix(".sei")
                sei_path = sei_path.with_name(f"{sei_path.name}.csv")
                expected.append(sei_path.relative_to(trip_root))

    return _dedupe_paths(expected)


def collect_manifest_missing_files(
    dataset_dir: Path,
    saved_request: SavedDatasetRequest,
    *,
    trip_filter: set[tuple[str, int, int]] | None = None,
    max_trips: int | None = None,
    uploaded_only: bool = False,
) -> ManifestCoverageResult:
    """Compare on-disk/S3 content against trip manifests from the API."""
    api_key = settings.api_key.strip()
    dataset_id = settings.dataset_id.strip()
    if not api_key or not dataset_id:
        return ManifestCoverageResult(
            note="manifest file coverage skipped: API_KEY/DATASET_ID not set in the active .env",
        )

    try:
        ds_manifest = get_ds_manifest(api_key, dataset_id)
    except Exception as exc:
        return ManifestCoverageResult(
            note=f"manifest file coverage skipped: failed to fetch dataset manifest: {exc}",
        )

    discovered_trips = _index_discovered_trips(dataset_dir)
    output = saved_request.request.output
    dry_mode = settings.dry_mode

    missing_files: list[str] = []
    missing_minutes: list[str] = []
    missing_trips: list[str] = []
    trip_summaries: list[str] = []
    fetch_errors: list[str] = []

    trips_to_check = list(ds_manifest.trips)
    if trip_filter is not None:
        trips_to_check = [
            trip
            for trip in trips_to_check
            if (trip.trip_id, trip.sub_trip, trip.health_grade) in trip_filter
        ]
    if uploaded_only:
        trips_to_check = [
            trip
            for trip in trips_to_check
            if (trip.trip_id, trip.sub_trip, trip.health_grade) in discovered_trips
        ]
    trips_to_check.sort(
        key=lambda trip: (
            # discovered_trips is keyed by the full three-part identity — comparing a
            # pair here silently made this sort key constant.
            0
            if (trip.trip_id, trip.sub_trip, trip.health_grade) in discovered_trips
            else 1,
            trip.trip_id,
            trip.sub_trip,
            trip.health_grade,
        )
    )
    coverage_notes: list[str] = []
    if uploaded_only:
        coverage_notes.append(
            "manifest file coverage limited to trips present on disk/S3"
        )
    if max_trips is not None:
        trips_to_check = trips_to_check[:max_trips]
        coverage_notes.append(f"manifest file coverage limited to {max_trips} trip(s)")

    def _fetch(ds_trip):
        try:
            manifest = get_trip_manifest(
                api_key, dataset_id, ds_trip.trip_id, ds_trip.sub_trip, ds_trip.health_grade
            )
            return ds_trip, manifest, None
        except Exception as exc:
            return ds_trip, None, exc

    # Trip-manifest fetches are pure network I/O — run them concurrently.
    workers = max(1, settings.manifest_fetch_workers)
    if workers > 1 and len(trips_to_check) > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fetched = list(pool.map(_fetch, trips_to_check))
    else:
        fetched = [_fetch(ds_trip) for ds_trip in trips_to_check]

    for ds_trip, trip_manifest, exc in fetched:
        trip_key = (ds_trip.trip_id, ds_trip.sub_trip, ds_trip.health_grade)

        if exc is not None:
            fetch_errors.append(f"{format_trip_key(trip_key)}: {exc}")
            continue

        trip_dir = discovered_trips.get(trip_key)
        if trip_dir is None:
            # The whole trip is absent — report it once as a missing trip instead of
            # enumerating every expected file as "missing".
            missing_trips.append(format_trip_key(trip_key))
            continue

        trip_rel = _relative_path(dataset_dir, trip_dir)
        expected_files = build_expected_trip_files(
            trip_manifest, output, dry_mode=dry_mode
        )
        expected_minutes = _expected_minute_dirs(trip_manifest)
        present_minutes = _present_minute_dirs(trip_dir)
        covered_minutes = present_minutes & expected_minutes

        trip_missing_files: list[str] = []
        for relative_file in expected_files:
            if _is_link_path(relative_file):
                continue
            if _is_expected_file_present(trip_dir, relative_file):
                continue
            trip_missing_files.append(f"{trip_rel}/{relative_file.as_posix()}")

        missing_files.extend(trip_missing_files)

        trip_missing_minutes = sorted(expected_minutes - present_minutes)
        for minute_name in trip_missing_minutes:
            missing_minutes.append(f"{trip_rel}/{minute_name}")

        if trip_missing_minutes or trip_missing_files:
            trip_summaries.append(
                f"{trip_rel}: {len(covered_minutes)}/{len(expected_minutes)} minutes present, "
                f"{len(trip_missing_files)} missing file(s)"
            )

    note = "; ".join(coverage_notes) if coverage_notes else None
    if fetch_errors:
        fetch_note = f"manifest file coverage partial: failed to fetch {len(fetch_errors)} trip manifest(s)"
        note = f"{note}; {fetch_note}" if note else fetch_note

    return ManifestCoverageResult(
        note=note,
        missing_files=sorted(set(missing_files)),
        missing_minutes=sorted(set(missing_minutes)),
        missing_trips=sorted(set(missing_trips)),
        trip_summaries=sorted(trip_summaries),
        fetch_errors=fetch_errors,
    )


def _is_link_path(relative_file: Path) -> bool:
    return relative_file.name.endswith(".mp4.link")


def _is_expected_file_present(trip_dir: Path, relative_file: Path) -> bool:
    full_path = trip_dir / relative_file
    if full_path.is_file():
        return True
    if relative_file.suffix == ".mp4":
        link_path = full_path.with_name(f"{full_path.name}.link")
        return link_path.is_file()
    return False


def _index_discovered_trips(dataset_dir: Path) -> dict[tuple[str, int, int], Path]:
    indexed: dict[tuple[str, int, int], Path] = {}
    for trip_dir in _discover_trip_dirs(dataset_dir):
        try:
            trip_id, sub_trip, health_grade = parse_trip_dir_name(trip_dir.name)
        except ValueError:
            continue
        indexed[(trip_id, sub_trip, health_grade)] = trip_dir
    return indexed


def _expected_minute_dirs(trip_manifest: TripManifest) -> set[str]:
    timezone = trip_manifest.timezone
    if timezone is None:
        return set()

    minute_dirs: set[str] = set()
    for minute in trip_manifest.minutes:
        if minute.minute_epoch is None:
            continue
        minute_dirs.add(
            build_minute_directory_path(Path("."), minute.minute_epoch, timezone).name
        )
    return minute_dirs


def _present_minute_dirs(trip_dir: Path) -> set[str]:
    if not trip_dir.is_dir():
        return set()
    return {
        child.name
        for child in trip_dir.iterdir()
        if child.is_dir() and _looks_like_minute_dir(child.name)
    }


def _looks_like_minute_dir(name: str) -> bool:
    parts = name.split("-")
    return len(parts) == 3 and all(part.isdigit() and len(part) == 2 for part in parts)


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    deduped: list[Path] = []
    for path in paths:
        key = path.as_posix()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _relative_path(dataset_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(dataset_dir).as_posix()
    except ValueError:
        return path.as_posix()
