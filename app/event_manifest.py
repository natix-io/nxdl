"""Build the safety-critical events manifest of a downloaded dataset.

Scans every ``trip_insight.json`` under the dataset directory and lists, per trip
and per minute folder that is actually on disk, the safety-critical events that
happened in it. The result is written to ``<dataset>/safety_critical_events_manifest.json``
— the same folder nesting as the dataset (country / [state] / trip), so a reader
can go from a manifest entry straight to the minute folder.

Runs at the end of ``app.main`` when ``CREATE_MANIFEST`` is set (``./run.sh
--create-manifest``), or standalone on an existing download::

    python -m app.event_manifest --dataset-dir ./dataset
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import re
from pathlib import Path

from app.validation import DATASET_REQUEST_FILENAME, EVENT_MANIFEST_FILENAME

logger = logging.getLogger(__name__)

MANIFEST_FILENAME = EVENT_MANIFEST_FILENAME

# Brake/accel/turn events come in two tiers: an event is recorded as "harsh" once
# it passes the lower threshold and recorded AGAIN as "severe" if it also passes
# the higher one. The severe entry overlapping a harsh entry of the same name is
# the same physical event, so it is not counted twice in the events total.
_TIERED_EVENTS = {"harsh_brake", "harsh_accel", "harsh_turn"}
_MINUTE_DIR_RE = re.compile(r"\d\d-\d\d-\d\d")


def _is_severe_tier(event: dict) -> bool:
    return event.get("name") in _TIERED_EVENTS and event.get("severity_label") == "severe"


def _overlaps(a: dict, b: dict) -> bool:
    return (
        a.get("name") == b.get("name")
        and a["start_frame"] <= b["end_frame"]
        and b["start_frame"] <= a["end_frame"]
    )


def _dataset_identity(dataset_dir: Path) -> tuple[str, str | None]:
    """(name, dataset_id) from the saved dataset_request.json, blank when absent."""
    request_path = dataset_dir / DATASET_REQUEST_FILENAME
    if not request_path.is_file():
        return "", None
    try:
        payload = json.loads(request_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return "", None
    name = (payload.get("request") or {}).get("settings", {}).get("name") or ""
    return name, payload.get("request_id")


def build_event_manifest(
    dataset_dir: Path, *, name: str | None = None, dataset_id: str | None = None
) -> dict:
    """Return the manifest dict for ``dataset_dir`` (see module docstring).

    Trips without any safety-critical event in a minute that is on disk are left
    out. Events whose ``minute_dir`` is not on disk (a minute the download did
    not include) are skipped, and exact duplicate entries are dropped.
    """
    dataset_dir = dataset_dir.resolve()
    default_name, default_id = _dataset_identity(dataset_dir)
    tree: dict = {}
    by_name: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    minutes = events = entries = severe = copies = 0

    for insight_path in sorted(dataset_dir.rglob("trip_insight.json")):
        trip_dir = insight_path.parent
        on_disk = {
            child.name
            for child in trip_dir.iterdir()
            if child.is_dir() and _MINUTE_DIR_RE.fullmatch(child.name)
        }
        try:
            payload = json.loads(insight_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Skipping unreadable %s", insight_path)
            continue
        raw_events = payload.get("safety_critical_events") if isinstance(payload, dict) else None
        if not isinstance(raw_events, list):
            continue

        per_minute: dict[str, list[dict]] = collections.defaultdict(list)
        for event in raw_events:
            if not isinstance(event, dict) or event.get("minute_dir") not in on_disk:
                continue
            if event in per_minute[event["minute_dir"]]:
                copies += 1
                continue
            per_minute[event["minute_dir"]].append(event)
        if not per_minute:
            continue

        node = tree
        for part in trip_dir.relative_to(dataset_dir).parts:  # country / [state] / trip
            node = node.setdefault(part, {})
        node["minutes"] = {
            minute: {"safety_critical_events": per_minute[minute]}
            for minute in sorted(per_minute)
        }
        for minute_events in per_minute.values():
            minutes += 1
            entries += len(minute_events)
            for event in minute_events:
                by_name[str(event.get("name"))][str(event.get("severity_label"))] += 1
                if _is_severe_tier(event):
                    severe += 1
                    has_harsh_twin = any(
                        _overlaps(event, other) and not _is_severe_tier(other)
                        for other in minute_events
                    )
                    events += not has_harsh_twin
                else:
                    events += 1

    logger.info(
        "Safety-critical events manifest: %d minute(s), %d event(s), %d entries, "
        "%d severe-tier entries, %d duplicate entries dropped",
        minutes, events, entries, severe, copies,
    )
    return {
        "dataset": name if name is not None else default_name,
        "dataset_id": dataset_id if dataset_id is not None else default_id,
        "summary": {
            "minutes_with_safety_critical_events": minutes,
            "safety_critical_events": events,
            "safety_critical_event_entries": entries,
            "safety_critical_event_entries_by_name": {
                event_name: dict(sorted(counter.items()))
                for event_name, counter in sorted(by_name.items())
            },
            "severe_tier_entries": severe,
        },
        **tree,
    }


def write_event_manifest(
    dataset_dir: Path, *, name: str | None = None, dataset_id: str | None = None
) -> Path:
    """Build the manifest and write it to ``<dataset_dir>/MANIFEST_FILENAME``."""
    manifest = build_event_manifest(dataset_dir, name=name, dataset_id=dataset_id)
    output = dataset_dir / MANIFEST_FILENAME
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    logger.info("Saved safety-critical events manifest to %s", output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build safety_critical_events_manifest.json for a downloaded dataset.",
    )
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--name", help="Dataset name (default: from dataset_request.json)")
    parser.add_argument("--dataset-id", help="Dataset id (default: from dataset_request.json)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if not args.dataset_dir.is_dir():
        parser.error(f"not a directory: {args.dataset_dir}")
    write_event_manifest(args.dataset_dir, name=args.name, dataset_id=args.dataset_id)


if __name__ == "__main__":
    main()
