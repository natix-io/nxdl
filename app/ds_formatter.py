import csv
import json
import logging
import os
import shutil
from pathlib import Path

from schemas import TripManifest
from schemas.dataset_request import (
    DatasetOutput,
    FootageCSVFormat,
    FootageMCAPFormat,
    TripInsightOutput,
)
from schemas.frame_data import FrameData
from schemas.trip_manifest import FixedMetadata, MinuteManifest, CameraItem
from schemas.trip_metadata import TripMetadata
from utils.csv_helpers import parse_telemetry_csv
from utils.exif_tools import extract_xmp_metadata, embed_xmp_into_video
from utils.footage import save_to_mcap_file
from utils.safety_critical_events import public_name_for_internal
from utils.path_helpers import (
    build_camera_directory_path, build_camera_file_path, build_camera_video_path,
    build_feature_file_path, build_fixed_metadata_path, build_minute_directory_path,
    build_trip_insight_path,
    get_camera_type,
    is_camera_allowed,
)
from utils.settings import settings

logger = logging.getLogger(__name__)


def _get_timezone(trip_manifest: TripManifest) -> str | None:
    return trip_manifest.timezone


def format_ds(trip_manifest: TripManifest, trip_dir: Path, output: DatasetOutput) -> None:
    trip = _format_fixed_metadata(trip_manifest, trip_dir, output)

    if output.trip_insight is not None:
        _build_trip_insight(trip_manifest, trip_dir, output.trip_insight)

    if trip is None:
        return

    embed_exif = output.footage.embed_exif
    # Grade 0 is video-only, categorically — no telemetry CSV/MCAP is ever
    # produced for it, even on a minute that happens to have telemetry.
    csv_enabled = output.footage.csv is not None and trip_manifest.health_grade != 0
    csv_fmt = output.footage.csv
    mcap_enabled = output.footage.mcap is not None and trip_manifest.health_grade != 0
    mcap_fmt = output.footage.mcap
    write_mapping = output.minute.mapping_file
    timezone = _get_timezone(trip_manifest)

    for minute in trip_manifest.minutes:
        minute_dir = build_minute_directory_path(trip_dir, minute.minute_epoch, timezone)
        for camera in minute.cameras:
            if not is_camera_allowed(camera.camera_type):
                continue
            camera_dir = build_camera_directory_path(
                minute_dir,
                camera_type=camera.camera_type,
            )

            if trip_manifest.health_grade == 0 or camera.telemetry is None:
                continue

            telemetry_path = build_camera_file_path(
                camera_dir, camera.telemetry.presigned_url,
            )

            # Nothing to read: needs_telemetry() is what gated the download in
            # app.trip_downloader._iter_trip_tasks, so no file was fetched. Still run
            # the CSV-disabled cleanup, which is idempotent — it catches a telemetry
            # CSV left in a resumed cache dir by an earlier CSV-enabled run.
            if not output.footage.needs_telemetry():
                _handle_footage_csv_disabled(telemetry_path)
                continue

            if not telemetry_path.exists():
                raise Exception(f"Telemetry file not found: {telemetry_path}")

            frames: list[FrameData] = parse_telemetry_csv(telemetry_path)

            if embed_exif and not settings.dry_mode:
                _handle_camera_exif(
                    camera_dir,
                    minute.minute_epoch, timezone,
                    camera, trip,
                    frames,
                )
            if csv_enabled:
                _handle_footage_csv_format(telemetry_path, frames, csv_fmt)
            if mcap_enabled:
                _handle_footage_mcap_format(telemetry_path, frames, timezone, mcap_fmt)
            if not csv_enabled:
                # Downloaded only because EXIF/MCAP need its frames — drop it now that
                # they're done, the same way fixed_metadata.csv is dropped.
                _handle_footage_csv_disabled(telemetry_path)
        if write_mapping:
            _handle_minute_mapping_file(minute_dir, minute)


# ---------------------------------------------------------------------------
# Fixed metadata helpers
# ---------------------------------------------------------------------------


def _format_fixed_metadata(
    trip_manifest: TripManifest, trip_dir: Path, output: DatasetOutput
) -> TripMetadata | None:
    """Build, parse, convert, and optionally clean up the fixed_metadata CSV for a trip."""
    csv_path = build_fixed_metadata_path(trip_dir)
    fixed_metadata = _load_fixed_metadata(trip_dir)
    if fixed_metadata is None:
        raise ValueError("Fixed metadata is required for trip manifest")

    vehicle_hw_version = getattr(trip_manifest, "vehicle_hw_version", None)
    trip = TripMetadata.from_manifest(trip_manifest.trip_id, 1, fixed_metadata, vehicle_hw_version)
    trip.save_to_csv_file(csv_path)

    if output.fixed_metadata is not None and output.fixed_metadata.json_format is not None:
        _handle_fixed_metadata_json_format(csv_path, trip, output.fixed_metadata.json_format)

    if output.fixed_metadata is not None and output.fixed_metadata.mcap is not None:
        _handle_fixed_metadata_mcap_format(csv_path, trip, output.fixed_metadata.mcap)

    if output.fixed_metadata is None or output.fixed_metadata.csv is None:
        _handle_fixed_metadata_csv_disabled(csv_path)

    return trip


def _load_fixed_metadata(trip_dir: Path) -> FixedMetadata | None:
    feature_path = build_feature_file_path(trip_dir, "fixed_metadata")
    if not feature_path.exists():
        return None
    with feature_path.open("r", encoding="utf-8") as f:
        return FixedMetadata.model_validate(json.load(f))


def _handle_fixed_metadata_json_format(
    csv_path: Path,
    trip: TripMetadata,
    fmt,
) -> None:
    """Save fixed_metadata as JSON according to configured format."""
    json_path = str(csv_path.with_suffix(".json"))
    trip.save_to_json_file(json_path, fmt)


def _handle_fixed_metadata_mcap_format(
    csv_path: Path,
    trip: TripMetadata,
    fmt,
) -> None:
    """Save fixed_metadata as MCAP according to configured format."""
    mcap_path = str(csv_path.with_suffix(".mcap"))
    trip.save_to_mcap_file(mcap_path, fmt)
    trip_dir = str(csv_path.parent)
    shutil.copy2(
        "./utils/mcap/minute_metadata.proto",
        os.path.join(trip_dir, "minute_metadata.proto"),
    )
    shutil.copy2(
        "./utils/mcap/trip_metadata.proto",
        os.path.join(trip_dir, "trip_metadata.proto"),
    )


def _handle_fixed_metadata_csv_disabled(csv_path: Path) -> None:
    """Delete fixed_metadata CSV when CSV output is disabled."""
    os.remove(csv_path)
    logger.info(f"Deleted fixed_metadata.csv: {csv_path}")


# ---------------------------------------------------------------------------
# Footage / camera helpers
# ---------------------------------------------------------------------------


def _handle_camera_exif(
    camera_dir: Path,
    minute_epoch: int,
    timezone: str,
    camera: CameraItem,
    trip: TripMetadata,
    frames: list[FrameData],
) -> None:
    """Embed XMP metadata into a camera's video using pre-parsed telemetry frames."""
    camera_type = camera.camera_type or "unknown"

    camera_file_path = build_camera_video_path(
        camera_dir, camera_type, minute_epoch, timezone,
    )
    camera_type = get_camera_type(camera_type)

    xmp_file_path = str(camera_file_path.with_suffix(".xmp"))
    extract_xmp_metadata(
        frame_data_list=frames,
        epoch_time_ms=minute_epoch,
        timezone=timezone,
        camera_type=camera_type,
        original_filename=camera.metadata_file_name,
        trip=trip,
        xmp_file_path=xmp_file_path,
    )
    embed_xmp_into_video(xmp_file_path, camera_file_path)


def _handle_footage_csv_format(
    telemetry_path: Path,
    frames: list[FrameData],
    footage_csv_format: FootageCSVFormat,
) -> None:
    """Re-write a telemetry CSV using the field layout defined by *footage_csv_format*."""
    rows: list[dict] = []
    for frame in frames:
        row = frame.to_csv_row(fmt=footage_csv_format)
        if row is not None:
            rows.append(row)

    if not rows:
        if frames:
            # Rows existed but the format dropped them all (here_tech skips frames without
            # GPS). Leave the file as downloaded rather than blanking real telemetry.
            logger.warning("No rows after format conversion, skipping: %s", telemetry_path)
            return
        # The source CSV had no telemetry rows at all — a minute whose device/phone logs
        # were absent. The service writes those with headers and no data on purpose, so
        # rewrite the header here too. Without this the file keeps the service's wider
        # column set while every populated sibling gets the narrower delivered one, and a
        # single dataset ends up with two different telemetry schemas.
        with telemetry_path.open("w", newline="") as f:
            csv.DictWriter(
                f, fieldnames=FrameData.csv_fieldnames(footage_csv_format)
            ).writeheader()
        logger.info(
            "Rewrote %s with 0 rows (header only) in '%s' format",
            telemetry_path, footage_csv_format.value,
        )
        return

    with telemetry_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logger.info(
        "Rewrote %s with %d rows in '%s' format",
        telemetry_path, len(rows), footage_csv_format.value,
    )


def _handle_footage_csv_disabled(telemetry_path: Path) -> None:
    """Delete a downloaded telemetry CSV when footage CSV output is disabled."""
    if not telemetry_path.exists():
        return
    telemetry_path.unlink()
    logger.info("Deleted telemetry CSV (footage.csv disabled): %s", telemetry_path)


def _handle_footage_mcap_format(
    telemetry_path: Path,
    frames: list[FrameData],
    timezone: str | None,
    footage_mcap_format: FootageMCAPFormat,
) -> None:
    """Re-write a telemetry MCAP using the field layout defined by *footage_mcap_format*."""
    mcap_path = str(telemetry_path.with_suffix(".mcap"))
    save_to_mcap_file(mcap_path, frames, timezone, footage_mcap_format)


# ---------------------------------------------------------------------------
# Minute helpers
# ---------------------------------------------------------------------------


def _handle_minute_mapping_file(
    minute_dir: Path,
    minute: MinuteManifest,
) -> None:
    """Write a mapping.txt inside the minute folder listing each camera type and its folder."""
    mapping_path = minute_dir / "mapping.txt"

    lines: list[str] = []
    for camera in minute.cameras:
        if not is_camera_allowed(camera.camera_type):
            continue
        camera_type = camera.camera_type or "unknown"
        camera_dir = build_camera_directory_path(minute_dir, camera_type=camera_type)
        folder_name = camera_dir.name
        lines.append(f"{camera_type.upper()}: {folder_name}")

    mapping_path.write_text("\n".join(lines) + "\n")
    logger.info("Wrote mapping file: %s", mapping_path)


# ---------------------------------------------------------------------------
# Trip insight
# ---------------------------------------------------------------------------


def _build_trip_insight(
    trip_manifest: TripManifest, trip_dir: Path, insight_output: TripInsightOutput
) -> None:
    """Merge feature JSON files into the final trip_insight.json response shape.

    Each section is included only when requested in ``insight_output`` — an
    unrequested section must not appear in the file at all (not even as an
    empty value), so downstream validation of "not requested => absent" holds.
    """
    trip_insight: dict = {}

    if insight_output.base_insights:
        base_insights = _load_base_insights(trip_dir)
        if base_insights is None:
            logger.info(
                "Skipping trip_insight.json because base_insights feature is absent"
            )
            return

        scalarized = _scalarize_minute_fields(base_insights)
        if scalarized:
            logger.info(
                "Unwrapped %d legacy array fields on minutes in %s", scalarized, trip_dir.name
            )

        filled = _fill_missing_minute_places(base_insights, trip_manifest)
        if filled:
            summary = ", ".join(f"{field}×{count}" for field, count in filled.items())
            logger.info(
                "Backfilled places (%s) on minutes in %s", summary, trip_dir.name
            )

        trip_insight["base_insights"] = base_insights

    if insight_output.safety_critical_events:
        trip_insight["safety_critical_events"] = _load_safety_critical_events(trip_dir)
    if insight_output.ego_actions:
        trip_insight["ego_actions"] = _load_ego_actions(trip_dir)

    trip_insight_path = build_trip_insight_path(trip_dir)
    with trip_insight_path.open("w", encoding="utf-8") as f:
        json.dump(trip_insight, f, indent=4)
    logger.info("Trip insight saved to %s", trip_insight_path)


# Per-minute fields the generator wraps in single-element arrays (legacy shape:
# `["Overcast"]` or `[]` when missing). Delivered trip_insight.json carries them
# as scalars instead: value, or null when the source array was empty.
_SCALARIZE_FIELDS = ("weather", "temperature", "timeOfDay", "roadType")


def _scalarize_minute_fields(base_insights: dict) -> int:
    """Unwrap legacy single-element arrays on ``_SCALARIZE_FIELDS`` to scalars.

    Idempotent: a value that is already a scalar (not a list) is left alone, so
    this handles both today's array-wrapped feature files and any future
    generator output that already emits scalars. Returns the number of fields
    unwrapped, for logging.
    """
    minutes = base_insights.get("minutes")
    if not isinstance(minutes, list):
        return 0

    changed = 0
    for entry in minutes:
        if not isinstance(entry, dict):
            continue
        for data in entry.values():
            if not isinstance(data, dict):
                continue
            for field in _SCALARIZE_FIELDS:
                value = data.get(field)
                if isinstance(value, list):
                    data[field] = value[0] if value else None
                    changed += 1
    return changed


# Per-minute geocoding fields we backfill when generation left a gap, mapped to the
# trip-level manifest attribute used as a last-resort source (or None for no such
# fallback). `country` is filed per-trip (one `<country>/` dir), so its trip-level
# value is authoritative and used when no minute carries it. `region` can vary
# minute-to-minute, so it is filled ONLY from the nearest sibling minute — if every
# minute of the trip lacks a region it stays null. Finer fields
# (place/district/postcode/…) are genuinely per-minute and are never fabricated.
_BACKFILL_FIELDS: tuple[tuple[str, str | None], ...] = (
    ("country", "country"),
    ("region", None),
)


def _fill_missing_minute_places(base_insights: dict, trip_manifest: TripManifest) -> dict[str, int]:
    """Backfill missing ``country``/``region`` on a trip's minutes in ``base_insights``.

    Some minutes come out of generation with these fields null (an upstream geocoding
    gap at the moment the dataset was generated). For each field we fill a gap from the
    nearest sibling minute that has a value (previous first, then next). ``country``
    additionally falls back to the trip-level manifest ``country`` when no minute carries
    it; ``region`` has no trip-level fallback, so if every minute lacks a region it stays
    null. Present values are never overwritten.

    Returns ``{field: count}`` for fields that were filled at least once.
    """
    minutes = base_insights.get("minutes")
    if not isinstance(minutes, list):
        return {}

    # Each entry is a single-key dict: {"YYYY-MM-DD_HH-MM": {...minute fields...}}.
    minute_datas = [
        data
        for entry in minutes
        if isinstance(entry, dict)
        for data in entry.values()
        if isinstance(data, dict)
    ]

    filled: dict[str, int] = {}
    for field, manifest_attr in _BACKFILL_FIELDS:
        trip_level_value = getattr(trip_manifest, manifest_attr, None) if manifest_attr else None
        count = _backfill_field(minute_datas, field, trip_level_value)
        if count:
            filled[field] = count
    return filled


def _backfill_field(
    minute_datas: list[dict], field: str, trip_level_value: str | None
) -> int:
    """Fill empty ``field`` values in the ordered ``minute_datas``.

    Nearest-neighbour carry: forward pass propagates the last seen value (nearest
    previous), then a backward pass covers any leading gap (nearest next). Whatever is
    still empty afterwards means no minute had the field, so the trip-level value is
    used. Returns the number of minutes changed from empty to populated.
    """
    was_empty = [not md.get(field) for md in minute_datas]

    carry: str | None = None
    for md in minute_datas:  # nearest previous
        if md.get(field):
            carry = md[field]
        elif carry:
            md[field] = carry

    carry = None
    for md in reversed(minute_datas):  # nearest next (fills any leading gap)
        if md.get(field):
            carry = md[field]
        elif carry:
            md[field] = carry

    if trip_level_value:  # whole trip lacked the field
        for md in minute_datas:
            if not md.get(field):
                md[field] = trip_level_value

    return sum(1 for i, md in enumerate(minute_datas) if was_empty[i] and md.get(field))


def _load_base_insights(trip_dir: Path) -> dict | None:
    feature_path = build_feature_file_path(trip_dir, "base_insights")
    if not feature_path.exists():
        return None
    with feature_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_safety_critical_events(trip_dir: Path) -> list:
    feature_path = build_feature_file_path(trip_dir, "safety_critical")
    if not feature_path.exists():
        return []

    with feature_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        events = payload
    elif isinstance(payload, dict):
        events = payload.get("safety_critical_events", [])
        if not isinstance(events, list):
            events = []
    else:
        events = []

    # Project to the public vocabulary: rename each event's internal name to its
    # public name, and drop events with no public mapping (internal-only types
    # such as u_turn / high_speed_range). No request-filter filtering here.
    public_events: list = []
    for event in events:
        if not isinstance(event, dict):
            continue
        public_name = public_name_for_internal(str(event.get("name")))
        if public_name is None:
            continue
        public_events.append({**event, "name": public_name})
    return public_events


def _load_ego_actions(trip_dir: Path) -> list:
    feature_path = build_feature_file_path(trip_dir, "ego_actions")
    if not feature_path.exists():
        return []

    with feature_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        events = payload
    elif isinstance(payload, dict):
        events = payload.get("ego_actions", [])
        if not isinstance(events, list):
            events = []
    else:
        events = []

    # Ego-action names are already the request-facing vocabulary — no
    # internal->public projection (unlike safety-critical events).
    return [event for event in events if isinstance(event, dict)]

