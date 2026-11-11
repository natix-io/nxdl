"""Client shim: the formatter lives in ``nxdl_core.format``.

``format_ds`` here fills in the client's configured dry mode and camera selection;
everything else is forwarded unchanged.
"""

from pathlib import Path

import nxdl_core.format as _core
from nxdl_core.format import (
    _build_trip_insight,
    _handle_footage_csv_format,
    _scalarize_minute_fields,
)
from schemas.dataset_request import DatasetOutput
from schemas.trip_manifest import TripManifest
from utils.settings import settings

__all__ = [
    "_build_trip_insight",
    "_handle_footage_csv_format",
    "_scalarize_minute_fields",
    "format_ds",
]


def format_ds(
    trip_manifest: TripManifest,
    trip_dir: Path,
    output: DatasetOutput,
    *,
    partial: bool = False,
    embed_exif: bool | None = None,
) -> None:
    """``nxdl_core.format.format_ds`` with ``settings.dry_mode`` and ``settings.camera_selection``."""
    _core.format_ds(
        trip_manifest,
        trip_dir,
        output,
        partial=partial,
        embed_exif=embed_exif,
        dry_mode=settings.dry_mode,
        camera_selection=settings.camera_selection,
    )
