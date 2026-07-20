"""Parse a text file of ``<trip_id>_<sub_trip>`` lines into trip keys.

Used by the standalone S3 maintenance tools (e.g. ``delete_trips_from_s3.py``).
The main downloader no longer needs this — it resumes automatically from the
ledger and S3, so there is no manual "download these trips from a file" step.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_trip_line(line: str) -> tuple[str, int] | None:
    """Parse a line like '008bd42d-...-232cb16ef2b6_3' into ('008bd42d-...', 3)."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if "_" not in stripped:
        logger.warning("Skipping invalid trip line (no '_'): %s", stripped)
        return None
    trip_id, sub_trip_str = stripped.rsplit("_", 1)
    try:
        sub_trip = int(sub_trip_str)
    except ValueError:
        logger.warning("Skipping invalid sub_trip in line: %s", stripped)
        return None
    return trip_id, sub_trip


def load_trips_from_file(trips_file: Path) -> list[tuple[str, int]]:
    """Return de-duplicated trip keys from ``trips_file`` (order preserved)."""
    if not trips_file.is_file():
        raise FileNotFoundError(f"Trips file not found: {trips_file}")

    trips: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for line in trips_file.read_text(encoding="utf-8").splitlines():
        parsed = parse_trip_line(line)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        trips.append(parsed)
    return trips
