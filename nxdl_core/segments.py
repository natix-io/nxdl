"""Which minutes of a trip are delivered, and as which units.

``segment_units`` splits a chopped trip (``seiData: with_and_chop``) into one unit
per segment, and applies a minute selection (``TripSelectionMap``, parsed from a
``TRIPS_FILE`` by ``utils.trip_selection.load_trips_selection`` in the client) to
each unit. Its manifest is used as is; nothing here reads files or settings.

**Chopped trips**: a selection entry still names the whole trip. Its ``minutes`` are
applied to the trip first, then each segment keeps the selected minutes it contains.
Labels always come from the server and are never reassigned, so selecting minutes
that fall only in segment B delivers just ``<trip>_B`` — there is no ``<trip>_A``.
"""

from nxdl_core.paths import TripKey, get_minute_dir_name
from nxdl_core.schemas.trip_manifest import TripManifest

# A concrete whole trip (segment ""). Value None = whole trip.
TripSelectionMap = dict[TripKey, frozenset[str] | None]


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
        unit = manifest.model_copy(
            update={"minutes": segment.minutes, "segments": None}
        )
        if minutes is not None:
            unit, _missing = select_minutes(unit, minutes)
        if unit.minutes:
            units.append((parent._replace(segment=segment.label), unit))
    return units
