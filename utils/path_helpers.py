"""Moved to ``nxdl_core.paths``; this path stays for the shipped client's imports."""

from nxdl_core import paths as _core_paths
from nxdl_core.paths import (
    FEATURES_DIRNAME,
    FIXED_METADATA_FILENAME,
    SEGMENT_LABEL_RE,
    TRIP_DIR_RE,
    TripKey,
    _health_grade_suffix,
    _segment_suffix,
    allowed_camera_types_for_selection,
    build_cache_trip_path,
    build_camera_directory_path,
    build_camera_file_path,
    build_camera_video_path,
    build_dataset_cache_path,
    build_feature_file_path,
    build_features_directory_path,
    build_fixed_metadata_path,
    build_fused_sei_path,
    build_minute_directory_path,
    build_trip_directory_path,
    build_trip_insight_path,
    get_camera_folder_name,
    get_camera_type,
    get_filename_from_presigned_url,
    get_minute_dir_name,
    get_r2_url_from_presigned_url,
    parse_trip_dir_name,
    safe_name,
)
from nxdl_core.schemas.camera_selection import CAMERA_SELECTION


def resolve_camera_selection(
    selection: CAMERA_SELECTION | None = None,
) -> CAMERA_SELECTION:
    """Fall back to the configured selection when no explicit one is given.

    nxdl_core reads no settings (None means ALL there); the client keeps its
    ``settings.camera_selection`` default here.
    """
    if selection is not None:
        return selection
    from utils.settings import settings

    return settings.camera_selection


def is_camera_allowed(
    camera_type: str | None, selection: CAMERA_SELECTION | None = None
) -> bool:
    """``nxdl_core.paths.is_camera_allowed`` with the client's settings default."""
    return _core_paths.is_camera_allowed(
        camera_type, resolve_camera_selection(selection)
    )


__all__ = [
    "FEATURES_DIRNAME",
    "FIXED_METADATA_FILENAME",
    "SEGMENT_LABEL_RE",
    "TRIP_DIR_RE",
    "TripKey",
    "_health_grade_suffix",
    "_segment_suffix",
    "allowed_camera_types_for_selection",
    "build_cache_trip_path",
    "build_camera_directory_path",
    "build_camera_file_path",
    "build_camera_video_path",
    "build_dataset_cache_path",
    "build_feature_file_path",
    "build_features_directory_path",
    "build_fixed_metadata_path",
    "build_fused_sei_path",
    "build_minute_directory_path",
    "build_trip_directory_path",
    "build_trip_insight_path",
    "get_camera_folder_name",
    "get_camera_type",
    "get_filename_from_presigned_url",
    "get_minute_dir_name",
    "get_r2_url_from_presigned_url",
    "is_camera_allowed",
    "parse_trip_dir_name",
    "resolve_camera_selection",
    "safe_name",
]
