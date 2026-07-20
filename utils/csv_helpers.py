import csv
import io
from pathlib import Path

from schemas.frame_data import FrameData


def parse_telemetry_csv(csv_path: Path) -> list[FrameData]:
    """Parse a per-minute telemetry CSV into a list of FrameData."""
    frames: list[FrameData] = []
    with csv_path.open(newline="") as f:
        for row in csv.DictReader(f):
            frames.append(FrameData.from_csv_row(row))
    return frames


def split_csv_sections(text: str) -> list[tuple[str, list[dict]]]:
    """Split a comment-delimited CSV into (section_name, rows) pairs.

    Lines starting with '#' are treated as section headers.
    Each section's data lines are parsed as CSV with the first
    non-comment line as the header row.
    """
    sections: list[tuple[str, list[str]]] = []
    current_name = ""
    current_lines: list[str] = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            if current_name or current_lines:
                sections.append((current_name, current_lines))
            current_name = stripped.lstrip("# ").strip()
            current_lines = []
        elif stripped:
            current_lines.append(line)

    if current_name or current_lines:
        sections.append((current_name, current_lines))

    parsed: list[tuple[str, list[dict]]] = []
    for name, lines in sections:
        if not lines:
            parsed.append((name, []))
            continue
        reader = csv.DictReader(io.StringIO("\n".join(lines)))
        rows = [_auto_type_row(row) for row in reader]
        parsed.append((name, rows))

    return parsed


def _auto_type_row(row: dict[str, str]) -> dict:
    """Cast string values to int/float where possible."""
    return {key: _try_numeric(value) for key, value in row.items()}


def _try_numeric(value: str):
    """Convert a string to int or float based on its original representation.

    Values written with a decimal point (e.g. "184.0") stay as float;
    integer literals (e.g. "3620") become int.
    """
    try:
        if "." in value:
            return float(value)
        return int(value)
    except (ValueError, TypeError):
        return value
