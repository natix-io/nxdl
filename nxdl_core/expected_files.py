"""What a formatted trip folder holds, and where it lands: relative paths and S3 keys.

The expected-file list drives the download's S3 fast-resume, QA's coverage check and
the delivery's upload set, so it must agree with what ``nxdl_core.format.format_ds``
writes for the same manifest, output block and camera selection.
"""

from pathlib import Path

from nxdl_core.paths import (
    TripKey,
    build_camera_directory_path,
    build_camera_file_path,
    build_camera_video_path,
    build_fused_sei_path,
    build_minute_directory_path,
    build_trip_directory_path,
    build_trip_insight_path,
    is_camera_allowed,
)
from nxdl_core.schemas.camera_selection import CAMERA_SELECTION
from nxdl_core.schemas.dataset_request import DatasetOutput
from nxdl_core.schemas.trip_manifest import TripManifest


def build_expected_trip_files(
    trip_manifest: TripManifest,
    output: DatasetOutput,
    *,
    dry_mode: bool,
    camera_selection: CAMERA_SELECTION | None = None,
) -> list[Path]:
    """Return paths relative to the trip directory that a completed download should contain.

    ``camera_selection`` None means every camera.
    """
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

    # As nxdl_core.format._build_trip_insight writes it: whenever the output asks for
    # it, except when the base_insights section is on and the feature is absent.
    if output.trip_insight is not None and (
        not output.trip_insight.base_insights or trip_manifest.base_insights is not None
    ):
        expected.append(build_trip_insight_path(trip_root).relative_to(trip_root))

    for minute in trip_manifest.minutes:
        if minute.minute_epoch is None:
            continue

        minute_dir = build_minute_directory_path(
            trip_root, minute.minute_epoch, timezone
        )

        # A restoring/archived file has its link withheld but exists server-side —
        # keep it expected so neither the S3 fast-resume nor QA can call the trip
        # complete.
        if output.footage.fused_sei is not None and (
            minute.fused_sei is not None
            or minute.fused_sei_status in ("restoring", "archived")
        ):
            expected.append(
                build_fused_sei_path(
                    minute_dir, minute.minute_epoch, timezone
                ).relative_to(trip_root)
            )

        if output.minute.mapping_file:
            expected.append((minute_dir / "mapping.txt").relative_to(trip_root))

        for camera in minute.cameras:
            if not is_camera_allowed(camera.camera_type, camera_selection):
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

            # Match the published telemetry outputs: MCAP is derived from the CSV,
            # which the formatter removes when CSV output is disabled.
            if trip_manifest.health_grade != 0 and camera.telemetry is not None:
                telemetry_path = build_camera_file_path(
                    camera_dir, camera.telemetry.presigned_url
                ).relative_to(trip_root)
                if output.footage.csv is not None:
                    expected.append(telemetry_path)
                if output.footage.mcap is not None:
                    expected.append(telemetry_path.with_suffix(".mcap"))

            if output.footage.sei is not None and (
                camera.sei is not None or camera.sei_status in ("restoring", "archived")
            ):
                sei_path = video_path.with_suffix(".sei")
                sei_path = sei_path.with_name(f"{sei_path.name}.csv")
                expected.append(sei_path.relative_to(trip_root))

    return _dedupe_paths(expected)


def expected_s3_keys(
    prefix: str, final_rel: str, expected_rel_paths: list[Path]
) -> list[str]:
    """S3 keys of a trip's files: ``<prefix>/<final_rel>/<rel>`` (no prefix → no leading part)."""
    base = f"{prefix}/{final_rel}" if prefix else final_rel
    return [f"{base}/{rel.as_posix()}" for rel in expected_rel_paths]


def trip_dir_rel(country: str | None, state: str | None, key: TripKey) -> str:
    """The trip folder relative to the dataset root, as a posix path."""
    return build_trip_directory_path(Path("."), country, state, *key).as_posix()


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
