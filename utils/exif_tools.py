"""Moved to ``nxdl_core.exif_tools``; this path stays for the shipped client's imports."""

# Tests patch ``exif_tools.subprocess.run``: the shared module object, so the
# patch reaches nxdl_core.exif_tools too.
import subprocess

from nxdl_core.exif_tools import (
    NS,
    embed_xmp_into_video,
    extract_xmp_metadata,
    get_datetime,
)

__all__ = [
    "NS",
    "embed_xmp_into_video",
    "extract_xmp_metadata",
    "get_datetime",
    "subprocess",
]
