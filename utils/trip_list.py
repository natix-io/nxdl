"""Parse a text file of ``<trip_id>_<sub_trip>[_<health_grade>]`` lines into trip keys.

Used by the standalone S3 maintenance tools (e.g. ``delete_trips_from_s3.py``).
``app.main``'s ``TRIPS_FILE`` is a JSON file parsed by ``utils.trip_selection``
instead; :func:`trip_matches` / :func:`format_trip_key` are shared by both.

Note on ``_TRIP_LINE_RE``: this intentionally duplicates ``app.validation``'s
``TRIP_DIR_RE`` / ``parse_trip_dir_name`` (same non-greedy-trip_id, optional
trailing health_grade shape) rather than importing from there. ``app.validation``
already depends on ``utils`` (e.g. ``utils.settings``); having ``utils.trip_list``
import back from ``app.validation`` would invert that dependency direction for a
tiny bit of regex logic, so a small duplicated implementation was judged cleaner
than introducing a utils -> app import.

**A requested trip's health_grade is three-state.** The parsed key is
``(trip_id, sub_trip, health_grade | None)`` where ``None`` means *every grade of
this pair* and an explicit grade means *only that one*. One dataset can hold the
same pair at several grades with different content, so both intents are real:
``abc_3`` selects all of them (the historical behaviour), ``abc_3_150`` selects
one. Use :func:`trip_matches` to test a concrete trip against a requested set —
never compare tuples directly, or an unqualified line stops matching graded trips.
"""

import logging
import re
from pathlib import Path

from utils.path_helpers import _health_grade_suffix

logger = logging.getLogger(__name__)

# A requested trip: (trip_id, sub_trip, health_grade | None), None = any grade.
TripKey = tuple[str, int, int | None]

# Mirrors app.validation.TRIP_DIR_RE: non-greedy trip_id so a trailing
# ``_<health_grade>`` suffix isn't swallowed into sub_trip.
_TRIP_LINE_RE = re.compile(
    r"(?P<trip_id>.+?)_(?P<sub_trip>\d+)(?:_(?P<health_grade>\d+))?$"
)


def parse_trip_line(line: str) -> TripKey | None:
    """Parse a line into ``(trip_id, sub_trip, health_grade | None)``.

    '008bd42d-...-232cb16ef2b6_3'     -> ('008bd42d-...', 3, None)  # any grade
    '008bd42d-...-232cb16ef2b6_3_150' -> ('008bd42d-...', 3, 150)   # that grade only

    The suffix is parsed with the same non-greedy regex as
    ``app.validation.parse_trip_dir_name``, so it is never misabsorbed into
    ``sub_trip``.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if "_" not in stripped:
        logger.warning("Skipping invalid trip line (no '_'): %s", stripped)
        return None
    match = _TRIP_LINE_RE.fullmatch(stripped)
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
    return trip_id, sub_trip, health_grade


def load_trips_from_file(trips_file: Path) -> list[TripKey]:
    """Return de-duplicated trip keys from ``trips_file`` (order preserved).

    De-duplication is on the full three-part key, so ``abc_3`` and ``abc_3_150``
    are two distinct requests ("all grades" and "grade 150") rather than one.
    """
    if not trips_file.is_file():
        raise FileNotFoundError(f"Trips file not found: {trips_file}")

    trips: list[TripKey] = []
    seen: set[TripKey] = set()
    for line in trips_file.read_text(encoding="utf-8").splitlines():
        parsed = parse_trip_line(line)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        trips.append(parsed)
    return trips


def format_trip_key(trip: TripKey) -> str:
    """Render a trip key the way its directory is named, for logs and reports.

    ``(abc, 1, 150)`` -> ``abc_1_150``; ``(abc, 1, 500)`` -> ``abc_1`` (grade 500 is
    unsuffixed by convention); ``(abc, 1, None)`` -> ``abc_1 (any grade)``, which only
    occurs for a *requested* key, never a concrete trip.
    """
    trip_id, sub_trip, health_grade = trip
    if health_grade is None:
        return f"{trip_id}_{sub_trip} (any grade)"
    return f"{trip_id}_{sub_trip}{_health_grade_suffix(health_grade)}"


def trip_matches(
    requested: set[TripKey],
    trip_id: str,
    sub_trip: int,
    health_grade: int,
) -> bool:
    """True if a concrete trip is selected by a set of requested keys.

    A requested key whose grade is ``None`` matches the pair at any grade; one with
    an explicit grade matches only that grade.
    """
    return (trip_id, sub_trip, None) in requested or (
        trip_id,
        sub_trip,
        health_grade,
    ) in requested
