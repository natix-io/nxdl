"""Moved to ``nxdl_core.trip_assets``; this path stays for the shipped client's imports."""

from nxdl_core import trip_assets as _core_trip_assets
from nxdl_core.schemas import DatasetOutput, TripManifest
from nxdl_core.schemas.camera_selection import CAMERA_SELECTION
from nxdl_core.trip_assets import (
    PendingRestore,
    TripAsset,
)
from utils.path_helpers import resolve_camera_selection


def enumerate_trip_assets(
    manifest: TripManifest,
    output: DatasetOutput,
    camera_selection: CAMERA_SELECTION | None = None,
) -> tuple[list[TripAsset], PendingRestore]:
    """``nxdl_core.trip_assets.enumerate_trip_assets`` with the client's settings
    default for the camera selection (None means ALL in nxdl_core)."""
    return _core_trip_assets.enumerate_trip_assets(
        manifest, output, resolve_camera_selection(camera_selection)
    )


__all__ = [
    "PendingRestore",
    "TripAsset",
    "enumerate_trip_assets",
]
