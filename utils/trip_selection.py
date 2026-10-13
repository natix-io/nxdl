"""The JSON ``TRIPS_FILE``: which trips — and optionally which minutes — to download.

Format::

    {
      "trips": [
        {"trip_id": "<uuid>", "sub_trip": 1},
        {"trip_id": "<uuid>", "sub_trip": 1, "minutes": ["12-14-02", "12-15-03"]},
        {"trip_id": "<uuid>", "sub_trip": 1, "health_grade": 150, "minutes": ["06-33-22"]}
      ]
    }

* ``health_grade`` absent means **500** (the unsuffixed default grade). Every entry
  therefore names one concrete ``(trip_id, sub_trip, health_grade)`` trip; grades of
  the same pair are separate trips with separate minutes, so there is no
  "every grade" wildcard here (unlike the text format ``utils.trip_list`` still
  parses for the S3 maintenance tools).
* ``minutes`` absent means the whole trip. When present it is a non-empty list of
  minute folder names (``HH-MM-SS`` in the trip's local time, exactly as they appear
  in the downloaded dataset) and only those minutes are downloaded.

**Chopped trips** (``seiData: with_and_chop``): an entry still names the whole trip.
Its ``minutes`` are applied to the trip first, then each segment keeps the selected
minutes it contains (:func:`segment_units`). Labels always come from the server and
are never reassigned, so selecting minutes that fall only in segment B delivers just
``<trip>_B`` — there is no ``<trip>_A``.
"""

import json
import logging
import re
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from schemas.trip_manifest import TripManifest
from utils.path_helpers import TripKey, get_minute_dir_name

logger = logging.getLogger(__name__)

DEFAULT_HEALTH_GRADE = 500

# A concrete whole trip (segment ""). Value None = whole trip.
TripSelectionMap = dict[TripKey, frozenset[str] | None]

_MINUTE_NAME_RE = re.compile(r"^\d{2}-\d{2}-\d{2}$")


class TripSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trip_id: str = Field(min_length=1)
    sub_trip: int
    health_grade: int = DEFAULT_HEALTH_GRADE
    minutes: list[str] | None = None

    @field_validator("minutes")
    @classmethod
    def _check_minutes(cls, minutes: list[str] | None) -> list[str] | None:
        if minutes is None:
            return None
        if not minutes:
            raise ValueError(
                "'minutes' must not be empty — omit the key to download the whole trip"
            )
        for name in minutes:
            if not _MINUTE_NAME_RE.fullmatch(name):
                raise ValueError(
                    f"minute {name!r} is not a minute folder name (expected HH-MM-SS)"
                )
        return sorted(set(minutes))

    @property
    def key(self) -> TripKey:
        return TripKey(self.trip_id, self.sub_trip, self.health_grade)


class TripsFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trips: list[TripSelection]


def load_trips_selection(trips_file: Path) -> TripSelectionMap:
    """Parse ``trips_file`` into ``{TripKey(trip_id, sub_trip, health_grade): minutes | None}``.

    Order is preserved. Raises ``FileNotFoundError`` if the file is missing and
    ``ValueError`` (with a readable message) if it is not valid JSON, does not match
    the format, or lists the same trip twice.
    """
    if not trips_file.is_file():
        raise FileNotFoundError(f"Trips file not found: {trips_file}")

    try:
        payload = json.loads(trips_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{trips_file} is not valid JSON: {exc}") from exc

    try:
        parsed = TripsFile.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"{trips_file} is not a valid trips file: {exc}") from exc

    selection: TripSelectionMap = {}
    for entry in parsed.trips:
        if entry.key in selection:
            raise ValueError(
                f"{trips_file} lists trip {entry.trip_id}_{entry.sub_trip} "
                f"(grade {entry.health_grade}) more than once"
            )
        selection[entry.key] = (
            frozenset(entry.minutes) if entry.minutes is not None else None
        )
    return selection


def select_minutes(
    manifest: TripManifest, minutes: frozenset[str]
) -> tuple[TripManifest, list[str]]:
    """Return a copy of ``manifest`` holding only the named minutes.

    Minutes are matched by their folder name (``HH-MM-SS`` in the trip's timezone —
    the same name the download writes). The second element lists requested names
    that do not exist in the manifest, for the caller to warn about.
    """
    kept = []
    found: set[str] = set()
    for minute in manifest.minutes:
        if minute.minute_epoch is None or manifest.timezone is None:
            continue
        name = get_minute_dir_name(minute.minute_epoch, manifest.timezone)
        if name in minutes:
            kept.append(minute)
            found.add(name)
    missing = sorted(minutes - found)
    return manifest.model_copy(update={"minutes": kept}), missing


def segment_units(
    parent: TripKey, manifest: TripManifest, minutes: frozenset[str] | None = None
) -> list[tuple[TripKey, TripManifest]]:
    """The units trip ``parent`` is delivered as: ``[(key, manifest), ...]``.

    An unsegmented trip (``manifest.segments is None``) is one unit, keyed on
    ``parent``. A chopped trip is one unit per segment, keyed on ``parent`` with
    the segment's label, whose manifest holds only that segment's minutes (and no
    segments). Keys come from ``parent`` — the trip that was asked for — exactly
    as an unsegmented trip's directory always has, never from the manifest body.

    ``minutes`` is the TRIPS_FILE minute selection for the trip: each unit keeps
    only the selected minutes, matched by folder name. A segment left with no
    minutes is dropped — labels are the server's and are never reassigned.
    """
    if manifest.segments is None:
        if minutes is not None:
            manifest, _missing = select_minutes(manifest, minutes)
        return [(parent, manifest)]

    units: list[tuple[TripKey, TripManifest]] = []
    for segment in manifest.segments:
        unit = manifest.model_copy(update={"minutes": segment.minutes, "segments": None})
        if minutes is not None:
            unit, _missing = select_minutes(unit, minutes)
        if unit.minutes:
            units.append((parent._replace(segment=segment.label), unit))
    return units
