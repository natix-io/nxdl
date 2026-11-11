"""Moved to ``nxdl_core.trip_stats``; this path stays for the shipped client's imports."""

from nxdl_core.trip_stats import (
    CONSECUTIVE_MINUTE_GAP_MS,
    EARTH_RADIUS_KM,
    MINUTE_MS,
    GpsFix,
    gps_fixes,
    partial_trip_stats,
)

__all__ = [
    "CONSECUTIVE_MINUTE_GAP_MS",
    "EARTH_RADIUS_KM",
    "MINUTE_MS",
    "GpsFix",
    "gps_fixes",
    "partial_trip_stats",
]
