"""Moved to ``nxdl_core.schemas.trip_manifest``; this path stays for the shipped client's imports."""

from nxdl_core.schemas.trip_manifest import (
    CameraItem,
    FixedMetadata,
    MinuteManifest,
    S3Location,
    SignedFile,
    TripManifest,
    TripSegment,
)

__all__ = [
    "CameraItem",
    "FixedMetadata",
    "MinuteManifest",
    "S3Location",
    "SignedFile",
    "TripManifest",
    "TripSegment",
]
