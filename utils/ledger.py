"""Persisted download/upload state — the resume signal and the progress source.

A single SQLite database at ``output_dir/.download_state.db`` records the status
of every trip and file. It is written by exactly ONE thread in the parent
process (``LedgerWriter``); the multiprocessing workers never touch the DB — they
only ``put`` :class:`StatusEvent` objects onto a queue via :class:`Reporter`.
This removes all multi-writer contention and every worker-side fork-safety
question (no SQLite fd is ever inherited by a fork).

Resume works from two independent signals:

* **The ledger** — trips marked ``complete`` are not re-enqueued at all (no
  manifest fetch), the fast path.
* **S3 presence** (:func:`build_present_keys`) — for trips that ARE re-processed,
  already-uploaded files are skipped per-file. This is also the recovery path
  when the ledger is missing (prod deletes local files after upload): every trip
  is enqueued, each worker finds its files already on S3 and marks the trip
  complete without downloading anything.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue

logger = logging.getLogger(__name__)

TRIP_COMPLETE = "complete"
TRIP_PARTIAL = "partial"
TRIP_FAILED = "failed"
TRIP_IN_PROGRESS = "in_progress"
_TRIP_TERMINAL = frozenset({TRIP_COMPLETE})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trips (
    trip_id      TEXT NOT NULL,
    sub_trip     INTEGER NOT NULL,
    status       TEXT NOT NULL,
    country      TEXT,
    state        TEXT,
    timezone     TEXT,
    file_total   INTEGER DEFAULT 0,
    error_stage  TEXT,
    error_message TEXT,
    retry_count  INTEGER DEFAULT 0,
    manifest_generated_at TEXT,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (trip_id, sub_trip)
);
CREATE TABLE IF NOT EXISTS files (
    trip_id     TEXT NOT NULL,
    sub_trip    INTEGER NOT NULL,
    rel_path    TEXT NOT NULL,
    kind        TEXT,
    status      TEXT NOT NULL,
    size_bytes  INTEGER,
    error_stage TEXT,
    error_message TEXT,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (trip_id, sub_trip, rel_path)
);
"""


@dataclass(frozen=True)
class StatusEvent:
    """One state transition emitted by a worker. Must stay picklable (module-level)."""

    scope: str  # "trip" | "file"
    trip_id: str
    sub_trip: int
    status: str
    rel_path: str | None = None
    kind: str | None = None
    size_bytes: int | None = None
    error_stage: str | None = None
    error_message: str | None = None
    file_total: int | None = None
    country: str | None = None
    state: str | None = None
    timezone: str | None = None
    manifest_generated_at: str | None = None


class Reporter:
    """Worker-side handle: enqueues events, never blocks the download loop."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def trip(self, trip_id: str, sub_trip: int, status: str, **fields) -> None:
        self._queue.put(StatusEvent("trip", trip_id, sub_trip, status, **fields))

    def file(
        self, trip_id: str, sub_trip: int, rel_path: str, status: str, **fields
    ) -> None:
        self._queue.put(
            StatusEvent("file", trip_id, sub_trip, status, rel_path=rel_path, **fields)
        )


def connect(db_path: Path) -> sqlite3.Connection:
    """Open (and initialize) the ledger DB at ``db_path``. Open AFTER any fork.

    ``check_same_thread=False`` because the parent opens the connection and hands
    it to the ``LedgerWriter`` thread, which is its sole user during a run (the
    main thread does not touch it again until after ``writer.join()``), so there
    is never concurrent cross-thread use.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def completed_trip_keys(conn: sqlite3.Connection) -> set[tuple[str, int]]:
    """Trip keys already marked terminal — these are NOT re-enqueued."""
    rows = conn.execute(
        f"SELECT trip_id, sub_trip FROM trips WHERE status = '{TRIP_COMPLETE}'"
    ).fetchall()
    return {(row[0], int(row[1])) for row in rows}


class LedgerWriter(threading.Thread):
    """Parent-side single writer: drains the event queue into SQLite and renders progress.

    Start this AFTER all workers have been forked so no child inherits the DB
    connection or this thread. Call :meth:`stop` (or put ``None``) then ``join``.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        queue: Queue,
        total_trips: int,
        *,
        render_interval: float = 2.0,
    ) -> None:
        super().__init__(name="ledger-writer", daemon=True)
        self._conn = conn
        self._queue = queue
        self._total = total_trips
        self._render_interval = render_interval
        self._failure_log: Path | None = None
        self._last_render = 0.0
        self._dirty = 0

    def attach_failure_log(self, path: Path) -> None:
        self._failure_log = path

    def run(self) -> None:
        while True:
            try:
                event = self._queue.get(timeout=0.5)
            except Empty:
                self._flush()
                self._maybe_render()
                continue
            if event is None:
                break
            self._apply(event)
            self._maybe_render()
        self._flush()
        self._render(force=True)

    def stop(self) -> None:
        self._queue.put(None)

    def _apply(self, event: StatusEvent) -> None:
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        if event.scope == "trip":
            self._conn.execute(
                """
                INSERT INTO trips (trip_id, sub_trip, status, country, state, timezone,
                                   file_total, error_stage, error_message,
                                   manifest_generated_at, updated_at, retry_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(trip_id, sub_trip) DO UPDATE SET
                    status=excluded.status,
                    country=COALESCE(excluded.country, trips.country),
                    state=COALESCE(excluded.state, trips.state),
                    timezone=COALESCE(excluded.timezone, trips.timezone),
                    file_total=COALESCE(excluded.file_total, trips.file_total),
                    error_stage=excluded.error_stage,
                    error_message=excluded.error_message,
                    manifest_generated_at=COALESCE(excluded.manifest_generated_at,
                                                   trips.manifest_generated_at),
                    retry_count=trips.retry_count + (CASE WHEN excluded.status='in_progress'
                                                          THEN 1 ELSE 0 END),
                    updated_at=excluded.updated_at
                """,
                (
                    event.trip_id,
                    event.sub_trip,
                    event.status,
                    event.country,
                    event.state,
                    event.timezone,
                    event.file_total,
                    event.error_stage,
                    event.error_message,
                    event.manifest_generated_at,
                    now,
                ),
            )
            if event.status in (TRIP_FAILED, TRIP_PARTIAL):
                self._append_failure(event, now)
        else:
            self._conn.execute(
                """
                INSERT INTO files (trip_id, sub_trip, rel_path, kind, status, size_bytes,
                                   error_stage, error_message, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trip_id, sub_trip, rel_path) DO UPDATE SET
                    kind=COALESCE(excluded.kind, files.kind),
                    status=excluded.status,
                    size_bytes=COALESCE(excluded.size_bytes, files.size_bytes),
                    error_stage=excluded.error_stage,
                    error_message=excluded.error_message,
                    updated_at=excluded.updated_at
                """,
                (
                    event.trip_id,
                    event.sub_trip,
                    event.rel_path,
                    event.kind,
                    event.status,
                    event.size_bytes,
                    event.error_stage,
                    event.error_message,
                    now,
                ),
            )
        self._dirty += 1
        if self._dirty >= 100:
            self._flush()

    def _append_failure(self, event: StatusEvent, now: str) -> None:
        if self._failure_log is None:
            return
        line = (
            f"{now}\t{event.trip_id}\t{event.sub_trip}\t"
            f"{event.error_stage or ''}\t{event.error_message or ''}\n"
        )
        with self._failure_log.open("a", encoding="utf-8") as handle:
            handle.write(line)

    def _flush(self) -> None:
        if self._dirty:
            self._conn.commit()
            self._dirty = 0

    def _maybe_render(self) -> None:
        if time.monotonic() - self._last_render >= self._render_interval:
            self._render()

    def _render(self, *, force: bool = False) -> None:
        self._last_render = time.monotonic()
        trip_counts = dict(
            self._conn.execute(
                "SELECT status, COUNT(*) FROM trips GROUP BY status"
            ).fetchall()
        )
        file_counts = dict(
            self._conn.execute(
                "SELECT status, COUNT(*) FROM files GROUP BY status"
            ).fetchall()
        )
        done = trip_counts.get(TRIP_COMPLETE, 0)
        logger.info(
            "PROGRESS trips %d/%d (complete=%d partial=%d failed=%d in_progress=%d) | "
            "files dl=%d up=%d failed=%d",
            done,
            self._total,
            done,
            trip_counts.get(TRIP_PARTIAL, 0),
            trip_counts.get(TRIP_FAILED, 0),
            trip_counts.get(TRIP_IN_PROGRESS, 0),
            file_counts.get("downloaded", 0),
            file_counts.get("uploaded", 0),
            file_counts.get("failed", 0),
        )


def build_present_keys(uri: str) -> set[str]:
    """One LIST sweep of ``uri`` → the set of S3 keys already present.

    Uses a locally-scoped boto3 client and resets the ``s3_uploader`` module
    global to ``None`` afterwards, so no client leaks across the later fork.
    """
    from utils import s3_uploader
    from utils.s3_dataset import build_snapshot

    client_kwargs: dict[str, str] = {}
    from utils.settings import settings

    if settings.aws_access_key_id:
        client_kwargs["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key:
        client_kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_region:
        client_kwargs["region_name"] = settings.aws_region
    if settings.aws_endpoint_url:
        client_kwargs["endpoint_url"] = settings.aws_endpoint_url

    import boto3

    local_client = boto3.client("s3", **client_kwargs)
    try:
        snapshot = build_snapshot(uri, client=local_client)
    finally:
        # Never leave a client in the module global before workers fork.
        s3_uploader._s3_client = None

    key_offset = len(f"/{snapshot.bucket}/")
    return {virtual[key_offset:] for virtual in snapshot.files}


def present_keys_cover(
    present_keys: Iterable[str], expected_keys: Iterable[str]
) -> bool:
    """True if every expected key (or its .link variant) is present."""
    present = (
        present_keys
        if isinstance(present_keys, (set, frozenset))
        else set(present_keys)
    )
    for key in expected_keys:
        if key in present:
            continue
        if key.endswith(".mp4") and f"{key}.link" in present:
            continue
        return False
    return True
