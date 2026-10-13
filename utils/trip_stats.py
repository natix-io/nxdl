"""Trip-level stats for a delivered folder that holds only part of a trip.

``base_insights`` describes the whole trip: its ``startEpochMs``, ``endEpochMs``,
``duration``, ``estimatedDistance``, ``estimatedAverageSpeed`` and ``firstLocation``
come from the service's trip row. A folder that holds only some of the trip's
minutes — one segment of a ``seiData: with_and_chop`` trip, or a TRIPS_FILE minute
selection — must not carry those, so they are recomputed for the folder's minutes:

* times from the minutes themselves: start of the first minute, end of the last
  (its start plus one minute). Always available, telemetry or not.
* distance, speed and first location from the GPS fixes in the telemetry the client
  already downloaded. Without telemetry (grade 0, or no telemetry output requested)
  these are ``None`` rather than the whole trip's figures.

Distance is the great-circle length of the GPS track, summed within each run of
consecutive minutes and never across a gap between them: a straight line over
minutes that are not delivered is not distance driven in the folder. Average speed
is that distance over the time it was measured in, in km/h — the same relation the
service's own figures have (``estimatedDistance`` km over ``duration``). Measured
time is, per run, from the first to the end of the last minute that has fixes, so
minutes without GPS at a run's edges do not dilute the speed.
"""

import math
from collections.abc import Iterable, Mapping
from datetime import datetime

from schemas.frame_data import FrameData
from schemas.trip_manifest import TripManifest

MINUTE_MS = 60_000
# Consecutive minutes of a trip start 60-61 s apart; a bigger step means a minute in
# between is missing. Same threshold the service chops segments at.
CONSECUTIVE_MINUTE_GAP_MS = 90_000
EARTH_RADIUS_KM = 6371.0088

GpsFix = tuple[datetime, float, float]  # (frame time, latitude, longitude)


def gps_fixes(frames: Iterable[FrameData]) -> list[GpsFix]:
    """The frames' GPS positions in time order; frames without a usable fix are skipped."""
    fixes: list[GpsFix] = []
    for frame in frames:
        try:
            lat = float(frame.GPS_latitude_deg)  # type: ignore[arg-type]
            lon = float(frame.GPS_longitude_deg)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        # (0, 0) is what a receiver without a fix reports, not a place on the road.
        if math.isfinite(lat) and math.isfinite(lon) and (lat, lon) != (0.0, 0.0):
            fixes.append((frame.local_timestamp, lat, lon))
    fixes.sort(key=lambda fix: fix[0])
    return fixes


def _haversine_km(a: GpsFix, b: GpsFix) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[1], a[2], b[1], b[2]))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def partial_trip_stats(
    manifest: TripManifest, fixes_by_minute: Mapping[int, list[GpsFix]]
) -> dict | None:
    """The ``base_insights`` trip-level fields for the manifest's minutes only.

    ``fixes_by_minute`` maps a minute's ``minute_epoch`` to its GPS fixes, from ONE
    camera per minute (every camera of a minute carries the same device fixes).
    Returns None when no minute has an epoch, so there is nothing to place in time.
    """
    epochs = sorted({m.minute_epoch for m in manifest.minutes if m.minute_epoch is not None})
    if not epochs:
        return None

    runs: list[list[int]] = []
    for epoch in epochs:
        if runs and epoch - runs[-1][-1] < CONSECUTIVE_MINUTE_GAP_MS:
            runs[-1].append(epoch)
        else:
            runs.append([epoch])

    distance_km: float | None = None
    first_fix: GpsFix | None = None
    measured_ms = 0
    for run in runs:
        tracked = [epoch for epoch in run if fixes_by_minute.get(epoch)]
        if not tracked:
            continue
        track = [fix for epoch in tracked for fix in fixes_by_minute[epoch]]
        first_fix = first_fix or track[0]
        distance_km = (distance_km or 0.0) + sum(
            _haversine_km(a, b) for a, b in zip(track, track[1:])
        )
        measured_ms += tracked[-1] + MINUTE_MS - tracked[0]

    start_ms = epochs[0]
    end_ms = epochs[-1] + MINUTE_MS
    return {
        "startEpochMs": start_ms,
        "endEpochMs": end_ms,
        "duration": end_ms - start_ms,
        "estimatedDistance": distance_km,
        "estimatedAverageSpeed": (
            distance_km / (measured_ms / 3_600_000) if distance_km is not None else None
        ),
        "firstLocation": (
            {"longitude": first_fix[2], "latitude": first_fix[1]} if first_fix else None
        ),
    }
