import os
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

from schemas.camera_selection import CAMERA_SELECTION

FIXED_METADATA_FILENAME = "fixed_metadata.csv"
FEATURES_DIRNAME = "features"


def get_filename_from_presigned_url(presigned_url: str) -> str:
    """Extract filename from a presigned URL path, stripping query parameters."""
    parsed = urlparse(presigned_url)
    return unquote(os.path.basename(parsed.path))


def get_r2_url_from_presigned_url(presigned_url: str) -> str:
    """
    Strip query parameters from a presigned URL to get the raw R2 object URL.
    Example:
    https://r2.object.com/v0/b/my-bucket/o/my-object.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=my-credential&X-Amz-Date=20210101T000000Z&X-Amz-Expires=3600&X-Amz-Signature=my-signature&X-Amz-SignedHeaders=host
    becomes:
    https://r2.object.com/v0/b/my-bucket/o/my-object.mp4
    """
    parsed = urlparse(presigned_url)
    return f"{parsed.scheme}://{parsed.netloc}{unquote(parsed.path)}"


def get_minute_dir_name(minute_epoch: int | None, timezone: str) -> str:
    """Format minute folder as HH-MM-SS from epoch milliseconds."""
    if minute_epoch is None:
        raise Exception("Minute epoch is None")
    return datetime.fromtimestamp(minute_epoch / 1000, tz=ZoneInfo(timezone)).strftime(
        "%H-%M-%S"
    )


def get_camera_folder_name(camera_type: str) -> str:
    """Build the per-camera subfolder name, e.g. 'FRONT_FOLDER'."""
    normalized_camera_type = get_camera_type(camera_type)
    return f"{normalized_camera_type.value.upper()}_FOLDER"


def _health_grade_suffix(health_grade: int) -> str:
    """Matches the server's health_grade_suffix() exactly — grade 500 stays
    unsuffixed for backward compatibility with existing trip folders."""
    return "" if health_grade == 500 else f"_{health_grade}"


def build_trip_directory_path(
    output_base: Path,
    country: str,
    state: str,
    trip_id: str,
    sub_trip: int,
    health_grade: int = 500,
) -> Path:
    output_dir = output_base
    if country:
        output_dir = output_dir / country
    else:
        output_dir = output_dir / "unknown"
    if country and country == "United States":
        if state:
            output_dir = output_dir / state
        else:
            output_dir = output_dir / "unknown"
    return output_dir / f"{trip_id}_{sub_trip}{_health_grade_suffix(health_grade)}"


def build_minute_directory_path(
    trip_dir: Path,
    minute_epoch: int | None,
    timezone: str,
) -> Path:
    return trip_dir / get_minute_dir_name(minute_epoch, timezone)


def build_camera_directory_path(
    minute_dir: Path,
    camera_type: str,
) -> Path:
    return minute_dir / get_camera_folder_name(camera_type)


def build_camera_file_path(
    camera_dir: Path,
    presigned_url: str,
) -> Path:
    filename = get_filename_from_presigned_url(presigned_url)
    return camera_dir / filename


def build_fused_sei_path(
    minute_dir: Path,
    minute_epoch: int | None,
    timezone: str,
) -> Path:
    time_part = get_minute_dir_name(minute_epoch, timezone)
    return minute_dir / f"FUSED_{time_part}.sei.csv"


def build_camera_video_path(
    camera_dir: Path,
    camera_type: str,
    minute_epoch: int,
    timezone: str,
) -> Path:
    """Build a video path like FRONT_2025-10-29_14-30-00.mp4."""
    cam = get_camera_type(camera_type)
    local_dt = datetime.fromtimestamp(minute_epoch / 1000, tz=ZoneInfo(timezone))
    date_part = local_dt.strftime("%Y-%m-%d")
    time_part = local_dt.strftime("%H-%M-%S")
    return camera_dir / f"{cam.value.upper()}_{date_part}_{time_part}.mp4"


def build_fixed_metadata_path(trip_dir: Path) -> Path:
    return trip_dir / FIXED_METADATA_FILENAME


def build_feature_file_path(trip_dir: Path, feature_name: str) -> Path:
    return trip_dir / FEATURES_DIRNAME / f"{feature_name}.json"


def build_features_directory_path(trip_dir: Path) -> Path:
    return trip_dir / FEATURES_DIRNAME


def build_cache_trip_path(
    cache_base: Path, trip_id: str, sub_trip: int, health_grade: int = 500
) -> Path:
    """Flat cache path for a trip — no country/state nesting."""
    return cache_base / f"{trip_id}_{sub_trip}{_health_grade_suffix(health_grade)}"


def get_camera_type(camera_type: str):
    """Map a camera label to the normalized internal camera type."""
    from schemas.trip_metadata import CAMERA_TYPE

    lower_filename = camera_type.lower()

    if "right_pillar" in lower_filename:
        return CAMERA_TYPE.RIGHT_PILLAR
    if "left_pillar" in lower_filename:
        return CAMERA_TYPE.LEFT_PILLAR
    if "right_repeater" in lower_filename:
        return CAMERA_TYPE.RIGHT_REPEATER
    if "left_repeater" in lower_filename:
        return CAMERA_TYPE.LEFT_REPEATER
    if "front" in lower_filename:
        return CAMERA_TYPE.FRONT
    if "back" in lower_filename:
        return CAMERA_TYPE.REAR
    if "right" in lower_filename:
        return CAMERA_TYPE.RIGHT_REPEATER
    if "left" in lower_filename:
        return CAMERA_TYPE.LEFT_REPEATER

    return None


def allowed_camera_types_for_selection(selection: CAMERA_SELECTION):
    """Camera allow-list for a client-side camera selection.

    Returns None for CAMERA_SELECTION.ALL, meaning unrestricted — every camera
    the server's trip manifest returns is kept. The health grade deliberately
    plays no part here: the server is authoritative about which cameras a minute
    actually has, and narrowing that set is the operator's explicit choice.
    """
    from schemas.trip_metadata import CAMERA_TYPE

    if selection == CAMERA_SELECTION.FRONT:
        return {CAMERA_TYPE.FRONT}
    if selection == CAMERA_SELECTION.FRONT_REAR:
        return {CAMERA_TYPE.FRONT, CAMERA_TYPE.REAR}
    return None


def resolve_camera_selection(
    selection: CAMERA_SELECTION | None = None,
) -> CAMERA_SELECTION:
    """Fall back to the configured selection when no explicit one is given."""
    if selection is not None:
        return selection
    from utils.settings import settings

    return settings.camera_selection


def is_camera_allowed(
    camera_type: str | None, selection: CAMERA_SELECTION | None = None
) -> bool:
    """Whether a camera of a minute should be kept.

    Under CAMERA_SELECTION.ALL everything is kept — unknown/unnormalizable
    camera names included, since the manifest decides what exists. Under a
    restricted selection a name get_camera_type() can't normalize is excluded,
    because it can't be verified to be within the selected set.
    """
    allowed = allowed_camera_types_for_selection(resolve_camera_selection(selection))
    if allowed is None:
        return True
    normalized = get_camera_type(camera_type) if camera_type else None
    return normalized in allowed


def build_trip_insight_path(trip_dir: Path) -> Path:
    return trip_dir / "trip_insight.json"
