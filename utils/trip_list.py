"""Parse a text file of ``<trip_id>_<sub_trip>[_<health_grade>][_<segment>]`` lines into trip keys.

Used by the standalone S3 maintenance tools (e.g. ``delete_trips_from_s3.py``).
``app.main``'s ``TRIPS_FILE`` is a JSON file parsed by ``utils.trip_selection``
instead; :func:`trip_matches` / :func:`format_trip_key` are shared by both.

Lines are parsed with ``utils.path_helpers.TRIP_DIR_RE``, the regex trip
directory names are parsed with, so a line and a folder name always agree.

**A requested trip's health_grade and segment are three-state.** The parsed key is
``(trip_id, sub_trip, health_grade | None, segment | None)`` where ``None`` means
*any* and an explicit value means *only that one*. One dataset can hold the
same pair at several grades with different content, so both intents are real:
``abc_3`` selects all of them (the historical behaviour), ``abc_3_150`` selects
one. Likewise a chopped (``seiData: with_and_chop``) trip is delivered as one
folder per segment: ``abc_3_150`` selects every segment of that grade,
``abc_3_150_B`` only segment B. Use :func:`trip_matches` to test a concrete trip
against a requested set — never compare tuples directly, or an unqualified line
stops matching graded or segmented trips.
"""

import logging
from pathlib import Path

from utils.path_helpers import TRIP_DIR_RE, TripKey, _health_grade_suffix, _segment_suffix

logger = logging.getLogger(__name__)

# A requested trip: (trip_id, sub_trip, health_grade | None, segment | None),
# None = any. A concrete ``utils.path_helpers.TripKey`` is also a valid request.
RequestedTripKey = tuple[str, int, int | None, str | None]


def parse_trip_line(line: str) -> RequestedTripKey | None:
    """Parse a line into ``(trip_id, sub_trip, health_grade | None, segment | None)``.

    '008bd42d-...-232cb16ef2b6_3'       -> ('008bd42d-...', 3, None, None)  # any grade
    '008bd42d-...-232cb16ef2b6_3_150'   -> ('008bd42d-...', 3, 150, None)   # that grade only
    '008bd42d-...-232cb16ef2b6_3_150_B' -> ('008bd42d-...', 3, 150, 'B')    # one segment

    The suffix is parsed with the same non-greedy regex as
    ``utils.path_helpers.parse_trip_dir_name``, so it is never misabsorbed into
    ``sub_trip``.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if "_" not in stripped:
        logger.warning("Skipping invalid trip line (no '_'): %s", stripped)
        return None
    match = TRIP_DIR_RE.fullmatch(stripped)
    if not match:
        logger.warning("Skipping invalid trip line: %s", stripped)
        return None
    trip_id = match.group("trip_id")
    try:
        sub_trip = int(match.group("sub_trip"))
    except ValueError:
        logger.warning("Skipping invalid sub_trip in line: %s", stripped)
        return None
    raw_grade = match.group("health_grade")
    health_grade = int(raw_grade) if raw_grade is not None else None
    return trip_id, sub_trip, health_grade, match.group("segment")


def load_trips_from_file(trips_file: Path) -> list[RequestedTripKey]:
    """Return de-duplicated trip keys from ``trips_file`` (order preserved).

    De-duplication is on the full key, so ``abc_3`` and ``abc_3_150`` are two
    distinct requests ("all grades" and "grade 150") rather than one.
    """
    if not trips_file.is_file():
        raise FileNotFoundError(f"Trips file not found: {trips_file}")

    trips: list[RequestedTripKey] = []
    seen: set[RequestedTripKey] = set()
    for line in trips_file.read_text(encoding="utf-8").splitlines():
        parsed = parse_trip_line(line)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        trips.append(parsed)
    return trips


def format_trip_key(trip: RequestedTripKey | TripKey) -> str:
    """Render a trip key the way its directory is named, for logs and reports.

    ``(abc, 1, 150, "")`` -> ``abc_1_150``; ``(abc, 1, 500, "")`` -> ``abc_1`` (grade
    500 is unsuffixed by convention); ``(abc, 1, 150, "B")`` -> ``abc_1_150_B``;
    ``(abc, 1, None, None)`` -> ``abc_1 (any grade)``, which only occurs for a
    *requested* key, never a concrete trip.
    """
    trip_id, sub_trip, health_grade, segment = trip
    segment_suffix = _segment_suffix(segment or "")
    if health_grade is None:
        return f"{trip_id}_{sub_trip}{segment_suffix} (any grade)"
    return f"{trip_id}_{sub_trip}{_health_grade_suffix(health_grade)}{segment_suffix}"


def trip_matches(
    requested: "set[RequestedTripKey] | set[TripKey]",
    trip_id: str,
    sub_trip: int,
    health_grade: int,
    segment: str = "",
) -> bool:
    """True if a concrete trip (or segment) is selected by a set of requested keys.

    A requested grade or segment of ``None`` matches any value; an explicit one
    matches only itself. So ``abc_1`` selects every grade and every segment of the
    pair, ``abc_1_150`` every segment of grade 150, ``abc_1_150_B`` one segment.
    """
    return any(
        (trip_id, sub_trip, grade, seg) in requested
        for grade in (None, health_grade)
        for seg in (None, segment)
    )
