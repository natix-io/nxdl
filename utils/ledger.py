"""Persisted download/upload state — the resume signal and the progress source.

A single SQLite database at ``state/<dataset_id>.db`` records the status
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

A trip is identified by ``(trip_id, sub_trip, health_grade)`` — all three, always.
One dataset can contain the same ``(trip_id, sub_trip)`` at several health grades
(a ``healthGrade: {min, max}`` request returns one row per grade, and those grades
hold genuinely different content), so a pair-only key lets one grade's completion
mask the others and silently drops them on resume. Grade 500 is the legacy value —
what the service reports for trips generated before health grades existed — and is
rendered without a directory suffix.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import os
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue

logger = logging.getLogger(__name__)

TRIP_COMPLETE = "complete"
TRIP_PARTIAL = "partial"
TRIP_FAILED = "failed"
TRIP_IN_PROGRESS = "in_progress"
# Everything available was downloaded, but some files are still being restored
# from S3 Deep Archive; the trip is held in the cache until a later run.
TRIP_PENDING_RESTORE = "pending_restore"
# Some files sit in S3 Deep Archive and the dataset does not (currently)
# request restores. The trip is held in the cache and skipped for the rest of
# the run; a future run re-checks it, because the dataset may have been
# switched to request_restore=true server-side in the meantime.
TRIP_ARCHIVED = "archived"

# The only `files` statuses that mean "this file was delivered". A file row is
# also written when a download FAILS, to record which file failed and why, and
# under the cache-relative path rather than the published one — so a failed row
# never names a file that publishing was going to create. Anything reading the
# table as evidence of delivery must filter on these, or a transient failure
# followed by a successful retry leaves a row that permanently disproves the
# completion it belongs to.
FILE_DELIVERED = ("downloaded", "uploaded")
_DELIVERED_SQL = "(" + ", ".join(f"'{s}'" for s in FILE_DELIVERED) + ")"

# Bumped whenever the table shape changes. A DB written by an older version is
# reset rather than migrated — see `connect`.
_SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trips (
    trip_id      TEXT NOT NULL,
    sub_trip     INTEGER NOT NULL,
    health_grade INTEGER NOT NULL,
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
    PRIMARY KEY (trip_id, sub_trip, health_grade)
);
CREATE TABLE IF NOT EXISTS files (
    trip_id     TEXT NOT NULL,
    sub_trip    INTEGER NOT NULL,
    health_grade INTEGER NOT NULL,
    rel_path    TEXT NOT NULL,
    kind        TEXT,
    status      TEXT NOT NULL,
    size_bytes  INTEGER,
    error_stage TEXT,
    error_message TEXT,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (trip_id, sub_trip, health_grade, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# meta key recording which output dir this ledger's progress refers to.
_META_OUTPUT_DIR = "output_dir"
# ...and which dataset wrote it. The filename cannot answer that: it is
# <safe_dataset_id>__<digest>, which is neither the id nor reversible.
_META_DATASET_ID = "dataset_id"
# Set in the same commit as rows rebuilt from markers, cleared once the per-file
# check has run over them. A start-up killed in between must not come back to a
# ledger that looks verified.
_META_VERIFY_PENDING = "verify_pending"


@dataclass(frozen=True)
class StatusEvent:
    """One state transition emitted by a worker. Must stay picklable (module-level)."""

    scope: str  # "trip" | "file"
    trip_id: str
    sub_trip: int
    # Positional and required on purpose: a defaulted grade would let a call site
    # that forgets it write grade-500 rows for a graded trip — the exact bug the
    # three-part key exists to prevent.
    health_grade: int
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

    def trip(
        self, trip_id: str, sub_trip: int, health_grade: int, status: str, **fields
    ) -> None:
        self._queue.put(
            StatusEvent("trip", trip_id, sub_trip, health_grade, status, **fields)
        )

    def file(
        self,
        trip_id: str,
        sub_trip: int,
        health_grade: int,
        rel_path: str,
        status: str,
        **fields,
    ) -> None:
        self._queue.put(
            StatusEvent(
                "file",
                trip_id,
                sub_trip,
                health_grade,
                status,
                rel_path=rel_path,
                **fields,
            )
        )


# State lives OUTSIDE the dataset dir, so an output dir holds only dataset content.
STATE_DIR = Path("state")


def ledger_output_dir(db_path: Path) -> str | None:
    """The output dir a ledger describes, or ``None`` for one written before this
    was recorded (or an unreadable file — this is only ever advisory)."""
    if not db_path.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = ?", (_META_OUTPUT_DIR,)
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def stamp_output_dir(
    conn: sqlite3.Connection, output_dir: Path, dataset_id: str = ""
) -> None:
    """Record which output dir this ledger describes, and which dataset owns it."""
    rows = [(_META_OUTPUT_DIR, str(output_dir))]
    if dataset_id:
        rows.append((_META_DATASET_ID, dataset_id))
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rows,
    )
    conn.commit()


def ledger_dataset_id(db_path: Path) -> str | None:
    """The dataset that wrote a ledger, or None if it predates the stamp."""
    if not db_path.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = ?", (_META_DATASET_ID,)
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def scope_digest(
    output_dir: Path,
    s3_upload: str = "",
    *,
    dry_mode: bool = False,
    camera_selection: str = "",
    endpoint_url: str = "",
) -> str:
    """The 8-char identity of a delivery scope: where, to what target, in what mode.

    Shared by the ledger filename and the completion markers (``utils.markers``),
    so both agree on what "this trip is already done" is scoped to.
    """
    # Runtime modes that change WHAT a completed trip contains are part of the
    # scope too. A completed dry run wrote .mp4.link files; a real run after it
    # would otherwise find every trip complete and exit 0 over links. A run with
    # camera_selection=front then one with front_rear would skip trips that
    # never fetched REAR. A fresh ledger means those trips are re-fetched.

    # The delivery target is part of the scope, not just the directory. "Trip
    # complete" means "delivered THERE": a dataset finished locally and then
    # re-run with S3_UPLOAD set, or re-pointed from one bucket to another, would
    # otherwise find every trip already complete, enqueue nothing, and deliver
    # nothing to the new target while reporting success.
    target = s3_upload.strip().rstrip("/") or "local"
    if target != "local" and endpoint_url.strip():
        # The same bucket/prefix on two endpoints (R2 vs MinIO) is two targets.
        target += "@" + endpoint_url.strip().rstrip("/").lower()
    mode = f"dry={int(bool(dry_mode))}\ncameras={camera_selection or 'all'}"
    # 16 hex chars, not 8: this identity decides whether one delivery's completion
    # counts as another's, and a 32-bit digest has demonstrable collisions between
    # ordinary S3 prefixes.
    return hashlib.sha256(f"{output_dir}\n{target}\n{mode}".encode()).hexdigest()[:16]


def _scoped_ledger_name(
    dataset_id: str,
    output_dir: Path,
    s3_upload: str = "",
    *,
    dry_mode: bool = False,
    camera_selection: str = "",
    endpoint_url: str = "",
) -> str:
    from utils.path_helpers import safe_name  # noqa: PLC0415 - avoids an import cycle

    digest = scope_digest(
        output_dir,
        s3_upload,
        dry_mode=dry_mode,
        camera_selection=camera_selection,
        endpoint_url=endpoint_url,
    )
    return f"{safe_name(dataset_id)}__{digest}.db"


def _migrate_legacy_ledger(
    legacy: Path,
    scoped: Path,
    output_dir: Path,
    *,
    dir_is_ours: bool = False,
    plain_scope: bool = True,
) -> None:
    """Move a pre-scoping ledger onto its scoped name, when it is provably ours.

    A legacy ledger predates the output-dir stamp, so by itself it cannot say
    which directory its completed rows describe, and `main` trusts completed
    rows: claimed for the wrong directory, it would skip trips over an empty
    output and report success. So an unstamped ledger is migrated only when the
    caller has independent evidence the directory is this dataset's —
    ``dir_is_ours`` — which app.main establishes from the ownership marker or the
    dataset_request.json the older client wrote at the root. That is the upgrade
    path: an in-flight download keeps resuming instead of starting over.

    And only into the PLAIN scope: local delivery, not a dry run. The directory
    proves nothing about where the legacy rows were delivered, so migrating them
    into an S3-scoped ledger would let a first upgraded run with a new S3 target
    enqueue nothing and exit 0 with nothing in the bucket. Camera selection is
    likewise unknowable for legacy rows; that is unchanged from the unscoped
    ledger main had, and is accepted.

    Serialized on a lock file, so two runs starting against two directories
    cannot both claim the same ledger.
    """
    lock_path = STATE_DIR / f"{legacy.stem}.migrate.lock"
    with lock_path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX)
        except OSError:
            pass  # filesystem without flock; the window is a one-off migration
        if scoped.exists() or not legacy.exists():
            return
        recorded = ledger_output_dir(legacy)
        if recorded is None and dir_is_ours and plain_scope:
            # `dir_is_ours` says WHOSE directory this is, not WHAT was delivered
            # into it. A pre-upgrade dry run left `.mp4.link` files and complete
            # rows, and a first real run would otherwise import them and skip
            # downloading the videos; a directory that was claimed and then left
            # empty by a failed first run would likewise inherit another
            # directory's progress. So the data has to agree.
            if not _prune_legacy_rows_to_what_is_on_disk(legacy, output_dir):
                logger.info(
                    "Ignoring pre-upgrade resume state in %s: none of what it "
                    "records is in %s. Starting fresh.",
                    legacy.name,
                    output_dir,
                )
                return
            recorded = str(output_dir)  # proven by the caller AND by the data
        if recorded != str(output_dir):
            if recorded is None:
                logger.info(
                    "Ignoring pre-upgrade resume state in %s: it does not record "
                    "which output dir it describes, so it cannot be trusted for "
                    "%s. Starting fresh — trips already in that directory are "
                    "adopted rather than re-downloaded.",
                    legacy.name,
                    output_dir,
                )
            return
        for suffix in ("", "-wal", "-shm"):
            source = legacy.with_name(legacy.name + suffix)
            if source.exists():
                source.rename(scoped.with_name(scoped.name + suffix))
        logger.debug("Migrated ledger %s -> %s", legacy.name, scoped.name)


def _prune_legacy_rows_to_what_is_on_disk(legacy: Path, output_dir: Path) -> bool:
    """Keep only the legacy completions the output dir can actually back.

    A legacy ledger predates the output-dir stamp, so it can hold progress from
    more than one directory, and rows from more than one delivery shape: an
    older real run interrupted and resumed with ``--dry-run`` leaves real rows
    and link-only rows side by side in one file. So there is no sampling. Every
    completed row is checked on its own: each file it recorded must exist, and
    none of them may be a ``.mp4.link`` (that row was a dry run, and this
    migration only ever feeds a real, local scope). Rows that fail are deleted
    before the ledger is adopted; rows with no recorded files cannot be checked
    and are deleted too.

    This is a stat per recorded file of each completed row, once, on an
    upgrade — not on every start. Returns whether anything usable is left.
    """
    try:
        conn = sqlite3.connect(legacy)
    except sqlite3.Error:
        return False
    try:
        delivered = completed_rows_with_files(conn, output_dir)
    except sqlite3.Error:
        conn.close()
        return False

    if not delivered:
        conn.close()
        return True  # nothing claimed complete, so nothing to disprove

    unbacked = []
    dry = 0
    for key, (_trip_dir, files) in delivered.items():
        if any(path.name.endswith(".mp4.link") for path in files):
            dry += 1
            unbacked.append(key)
        elif not files or not all(path.exists() for path in files):
            unbacked.append(key)
    if dry:
        logger.info(
            "%d pre-upgrade completion(s) describe a dry run and do not apply to a "
            "real download.",
            dry,
        )
    # Nothing is deleted until the ledger is being adopted. The prune used to
    # commit first and answer afterwards, so a declined migration left the
    # legacy file in place with its completions already gone — and a later run
    # against the directory those rows DID describe found no resume state and
    # re-downloaded the whole dataset.
    usable = len(unbacked) < len(delivered)
    if not usable:
        conn.close()
        return False
    dropped = drop_completions(conn, unbacked)
    conn.close()
    if dropped:
        logger.info(
            "Dropped %d of %d pre-upgrade completion(s) that %s does not hold; "
            "those trips will be fetched again.",
            dropped,
            len(delivered),
            output_dir,
        )
    return True


def ledger_path(
    dataset_id: str,
    output_dir: Path,
    s3_upload: str = "",
    *,
    dry_mode: bool = False,
    camera_selection: str = "",
    endpoint_url: str = "",
    dir_is_ours: bool = False,
) -> Path:
    """The ledger DB for one dataset downloading into one output dir.

    Progress is only meaningful for the directory it was recorded against: a trip
    marked complete means "its folder is in THAT dir". Keyed on the dataset alone,
    running the same dataset into a second directory resumed against the first
    one's progress and left the second empty.

    The name is a pure function of (dataset, output dir, delivery target), so two
    runs starting at the same moment cannot land on the same file, and changing
    where the data is delivered starts fresh state rather than reusing progress
    recorded against somewhere else. A ledger written before this scoping
    existed is migrated onto its scoped name once, under a lock, so an in-flight
    download keeps resuming instead of starting over.
    """
    from utils.path_helpers import safe_name  # noqa: PLC0415 - avoids an import cycle

    plain_scope = not s3_upload.strip() and not dry_mode
    scoped = STATE_DIR / _scoped_ledger_name(
        dataset_id,
        output_dir,
        s3_upload,
        dry_mode=dry_mode,
        camera_selection=camera_selection,
        endpoint_url=endpoint_url,
    )
    if scoped.exists():
        return scoped
    legacy = STATE_DIR / f"{safe_name(dataset_id)}.db"
    if legacy.exists():
        _migrate_legacy_ledger(
            legacy, scoped, output_dir, dir_is_ours=dir_is_ours, plain_scope=plain_scope
        )
    return scoped



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
    _reset_if_legacy_schema(conn, db_path)
    conn.executescript(_SCHEMA)
    conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
    conn.commit()
    return conn


def _reset_if_legacy_schema(conn: sqlite3.Connection, db_path: Path) -> None:
    """Drop tables written before ``health_grade`` was part of the trip key.

    SQLite cannot add a column to a primary key, so a pre-v2 ledger cannot be
    migrated in place — and its rows are un-migratable anyway: they never recorded
    which grade completed, so there is no honest value to backfill. The resume
    state is discarded (files already on disk or on S3 are still skipped per-file,
    so nothing is re-downloaded needlessly) and the run continues.

    Idempotent: ``connect`` is called once per pass plus twice per resume check.
    """
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('trips','files')"
        ).fetchall()
    }
    if not tables:
        return
    columns = {row[1] for row in conn.execute("PRAGMA table_info(trips)").fetchall()}
    if "health_grade" in columns:
        return
    logger.warning(
        "Ledger %s uses the pre-health-grade schema; resetting its resume state "
        "(trip status was keyed by (trip_id, sub_trip) only)",
        db_path,
    )
    conn.executescript("DROP TABLE IF EXISTS trips; DROP TABLE IF EXISTS files;")
    conn.commit()


def completed_rows_with_files(
    conn: sqlite3.Connection, output_dir: Path
) -> dict[tuple[str, int, int], tuple[Path, list[Path]]]:
    """``{completed key: (its trip directory, the files its row recorded)}``.

    A completed row is a claim; the files it recorded are the evidence. Every
    check that asks "is this completion still backed by data" reads this rather
    than walking the dataset: only the rows that claim to be done, and only their
    own trip directory. The directory comes too because what a row does NOT name
    is evidence as well -- a broader scope's publication leaves every recorded
    file in place and adds to them.
    """
    from utils.path_helpers import build_trip_directory_path  # noqa: PLC0415

    out: dict[tuple[str, int, int], list[Path]] = {}
    rows = conn.execute(
        "SELECT trip_id, sub_trip, health_grade, country, state FROM trips "
        f"WHERE status = '{TRIP_COMPLETE}'"
    ).fetchall()
    for trip_id, sub_trip, grade, country, state in rows:
        key = (trip_id, int(sub_trip), int(grade))
        trip_dir = build_trip_directory_path(
            output_dir, country or "", state or "", trip_id, key[1], key[2]
        )
        out[key] = (
            trip_dir,
            [
                trip_dir / row[0]
                for row in conn.execute(
                    "SELECT rel_path FROM files WHERE trip_id = ? AND sub_trip = ? "
                    f"AND health_grade = ? AND status IN {_DELIVERED_SQL}",
                    key,
                ).fetchall()
            ],
        )
    return out


def drop_completions(
    conn: sqlite3.Connection, keys: Iterable[tuple[str, int, int]]
) -> int:
    """Forget these completions so the trips are fetched again. Returns how many."""
    keys = list(keys)
    if keys:
        # The file rows go too, in the same transaction. They key only on the
        # trip, not on the completion, so rows left behind are re-attached to
        # the NEXT completion of the same trip and disprove that one as well —
        # a dropped completion that can never be re-earned, and a `files` table
        # that grows without bound across runs.
        conn.executemany(
            "DELETE FROM files WHERE trip_id = ? AND sub_trip = ? AND health_grade = ?",
            keys,
        )
        conn.executemany(
            "DELETE FROM trips WHERE trip_id = ? AND sub_trip = ? AND health_grade = ?",
            keys,
        )
        conn.commit()
    return len(keys)


# Every scope that has ever run against an output dir leaves its digest here, so
# a scope can tell that others exist WITHOUT seeing their ledgers — which live in
# each copy of the client's own state/ and are invisible from any other copy.
SCOPES_DIRNAME = "scopes"


def register_scope(output_dir: Path, digest: str) -> None:
    """Note in the output dir itself that this scope has run against it."""
    scopes = output_dir / ".natix" / SCOPES_DIRNAME
    try:
        scopes.mkdir(parents=True, exist_ok=True)
        (scopes / digest).touch()
    except OSError:  # pragma: no cover - read-only output; the run fails later anyway
        logger.debug("Could not register scope %s under %s", digest, output_dir)


def other_scopes_touched(db_path: Path, output_dir: Path) -> bool:
    """Has any OTHER delivery scope of this dataset written into ``output_dir``?

    Scopes (local / S3 / dry / camera selection) keep separate ledgers, and only
    another scope can have replaced or removed what this one delivered. When no
    other scope has ever run here, nothing has, and the per-file check that
    protects against it can be skipped — which is the ordinary single-scope case.

    Two places are asked, because one is not enough. This copy's ``state/`` holds
    the sibling ledgers of scopes run FROM THIS COPY; another copy of the client
    (its own ``state/``, its own ledgers) is invisible there, so every scope also
    registers itself in the output dir, where every copy can see it.
    """
    digest = db_path.stem.rsplit("__", 1)[-1]
    scopes = output_dir / ".natix" / SCOPES_DIRNAME
    if scopes.is_dir():
        try:
            with os.scandir(scopes) as it:
                if any(e.name != digest and not e.name.startswith(".") for e in it):
                    return True
        except OSError:
            pass
    dataset_prefix = db_path.name.rsplit("__", 1)[0]
    for other in db_path.parent.glob(f"{dataset_prefix}__*.db"):
        if other == db_path:
            continue
        if ledger_output_dir(other) == str(output_dir):
            return True
    return False


def completed_trip_keys(conn: sqlite3.Connection) -> set[tuple[str, int, int]]:
    """``(trip_id, sub_trip, health_grade)`` keys already complete — NOT re-enqueued."""
    rows = conn.execute(
        f"SELECT trip_id, sub_trip, health_grade FROM trips WHERE status = '{TRIP_COMPLETE}'"
    ).fetchall()
    return {(row[0], int(row[1]), int(row[2])) for row in rows}


@dataclass(frozen=True)
class LegacyCompletion:
    """What a pre-marker ledger recorded for one completed trip."""

    country: str
    state: str
    trip_dir: Path
    rel_paths: list[str]

    @property
    def paths(self) -> list[Path]:
        return [self.trip_dir / rel for rel in self.rel_paths]


def ledgers_for_output_dir(
    output_dir: Path, state_dir: Path | None = None
) -> list[tuple[str, dict[tuple[str, int, int], LegacyCompletion], set[tuple[str, int, int]]]]:
    """``(scope_digest, {completed key: its delivered files}, all keys mentioned)``.

    Used once, by the marker import: the scope digest is the ``__<digest>`` part of
    the filename, and the ``meta`` stamp says which output dir the rows describe.
    Unstamped (pre-stamp) ledgers are skipped — they cannot prove which directory
    they belong to, which is exactly why the stamp exists.

    The files come along because a completed row is a claim, not evidence. Another
    copy of the client — its own ``state/``, invisible from here — can have
    replaced the trip since, and a narrower camera selection leaves the directory
    in place with files missing. The import checks the paths rather than trusting
    the row, so the paths have to travel with it.
    """
    from utils.path_helpers import build_trip_directory_path  # noqa: PLC0415

    root = state_dir if state_dir is not None else STATE_DIR
    if not root.is_dir():
        return []
    found: list[
        tuple[str, dict[tuple[str, int, int], LegacyCompletion], set[tuple[str, int, int]]]
    ] = []
    for db_path in sorted(root.glob("*__*.db")):
        if ledger_output_dir(db_path) != str(output_dir):
            continue
        digest = db_path.stem.rsplit("__", 1)[-1]
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.Error:  # pragma: no cover - unreadable file
            continue
        delivered: dict[tuple[str, int, int], LegacyCompletion] = {}
        try:
            for trip_id, sub_trip, grade, country, state in conn.execute(
                "SELECT trip_id, sub_trip, health_grade, country, state FROM trips "
                f"WHERE status = '{TRIP_COMPLETE}'"
            ).fetchall():
                key = (trip_id, int(sub_trip), int(grade))
                trip_dir = build_trip_directory_path(
                    output_dir, country or "", state or "", trip_id, key[1], key[2]
                )
                delivered[key] = LegacyCompletion(
                    country or "",
                    state or "",
                    trip_dir,
                    [
                        row[0]
                        for row in conn.execute(
                            "SELECT rel_path FROM files WHERE trip_id = ? AND "
                            "sub_trip = ? AND health_grade = ?",
                            key,
                        ).fetchall()
                    ],
                )
            # Every key this ledger KNOWS ABOUT, whatever its status. A scope
            # that was killed part-way through replacing a trip leaves an
            # in-progress or partial row rather than a complete one, and that is
            # still evidence that it touched the trip's directory — enough to
            # make another scope's claim on it unsafe to import.
            mentioned = {
                (row[0], int(row[1]), int(row[2]))
                for row in conn.execute(
                    "SELECT trip_id, sub_trip, health_grade FROM trips"
                ).fetchall()
            }
        except sqlite3.Error:
            delivered, mentioned = {}, set()
        finally:
            conn.close()
        if delivered or mentioned:
            found.append((digest, delivered, mentioned))
    return found


def verification_pending(conn: sqlite3.Connection) -> bool:
    """Were rows rebuilt from markers that the per-file check has not yet seen?"""
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (_META_VERIFY_PENDING,)
    ).fetchone()
    return bool(row and row[0] == "1")


def verification_done(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM meta WHERE key = ?", (_META_VERIFY_PENDING,))
    conn.commit()


def reconcile_with_markers(
    conn: sqlite3.Connection,
    marker_keys: set[tuple[str, int, int]],
    evidence_for: "Callable[[tuple[str, int, int]], tuple[str, str, list[str]]] | None" = None,
) -> tuple[int, int]:
    """Make the ledger agree with what is actually on disk. Returns (added, dropped).

    The markers are the authority: they are written by the process that published
    the trip, at the moment it published it, and revoked by whoever replaced it.
    A ``complete`` row with no marker is a claim on data another scope has since
    overwritten (or on a directory this package's ledger never saw), so it is
    dropped and the trip is re-enqueued. A marker with no row is a ledger that was
    lost or is a fresh copy of the client; the row is recreated so every later
    reader — retry passes, the final tally — sees one truth.
    """
    ledger_keys = completed_trip_keys(conn)
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    added = marker_keys - ledger_keys
    dropped = ledger_keys - marker_keys
    if added:
        # Rebuilt WITH the marker's evidence — where the trip lives and which
        # files it delivered — so the per-file start-up check can verify the row
        # like any other, in the right directory. Only the added rows pay for
        # reading their markers. A marker with no evidence gives an unlocated
        # row with no files, which that check then drops rather than trusts.
        evidence = {
            key: (evidence_for(key) if evidence_for is not None else ("", "", []))
            for key in sorted(added)
        }
        conn.executemany(
            """
            INSERT INTO trips (trip_id, sub_trip, health_grade, status, country, state,
                               updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trip_id, sub_trip, health_grade) DO UPDATE SET
                status=excluded.status, country=excluded.country,
                state=excluded.state, updated_at=excluded.updated_at
            """,
            [
                (t, s, g, TRIP_COMPLETE, evidence[(t, s, g)][0] or None,
                 evidence[(t, s, g)][1] or None, now)
                for t, s, g in sorted(added)
            ],
        )
        # REPLACE the inventory, do not merge into it. An earlier attempt can
        # have left failed-file rows for intermediates a later, successful
        # attempt never delivered; checked alongside the marker's evidence they
        # would sink an intact trip.
        conn.executemany(
            "DELETE FROM files WHERE trip_id = ? AND sub_trip = ? AND health_grade = ?",
            sorted(added),
        )
        conn.executemany(
            """
            INSERT INTO files (trip_id, sub_trip, health_grade, rel_path, status,
                               updated_at)
            VALUES (?, ?, ?, ?, 'downloaded', ?)
            """,
            [
                (t, s, g, rel, now)
                for t, s, g in sorted(added)
                for rel in evidence[(t, s, g)][2]
            ],
        )
        # Committed together with the rows: the rebuilt completions are not to be
        # trusted until the per-file check has looked at them, and a kill between
        # this commit and that check must not leave a ledger that looks verified.
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, '1') "
            "ON CONFLICT(key) DO UPDATE SET value = '1'",
            (_META_VERIFY_PENDING,),
        )
    if dropped:
        conn.executemany(
            "DELETE FROM trips WHERE trip_id = ? AND sub_trip = ? AND health_grade = ?",
            sorted(dropped),
        )
    if added or dropped:
        conn.commit()
    return len(added), len(dropped)


def pending_restore_trip_keys(conn: sqlite3.Connection) -> set[tuple[str, int, int]]:
    """Keys held back by a Deep-Archive restore — excluded from in-run retry passes.

    A restore takes hours (~12h standard / ~48h bulk tier), so re-fetching the
    manifest seconds later cannot help. The trips are NOT complete, so a future
    run of the same command re-enqueues and finishes them.
    """
    rows = conn.execute(
        f"SELECT trip_id, sub_trip, health_grade FROM trips WHERE status = '{TRIP_PENDING_RESTORE}'"
    ).fetchall()
    return {(row[0], int(row[1]), int(row[2])) for row in rows}


def archived_trip_keys(conn: sqlite3.Connection) -> set[tuple[str, int, int]]:
    """Keys held back by archived (restore-not-requested) files.

    Excluded from IN-RUN retry passes only: the verdict changes when the dataset
    is edited server-side (request_restore flipped to true), never within a run.
    The trips are NOT complete, so a future run of the same command re-enqueues
    them and re-fetches their manifests.
    """
    rows = conn.execute(
        f"SELECT trip_id, sub_trip, health_grade FROM trips WHERE status = '{TRIP_ARCHIVED}'"
    ).fetchall()
    return {(row[0], int(row[1]), int(row[2])) for row in rows}


# Written by pending_restore_message, parsed back by pending_restore_stats — keep in sync.
_RESTORE_FILES_RE = re.compile(r"^(\d+) file")
_RESTORE_ETA_RE = re.compile(r"available_at_ms=(\d+)")


def pending_restore_message(files: int, available_at_ms: int | None) -> str:
    """The trip-row ``error_message`` for a pending_restore trip."""
    message = f"{files} file(s) awaiting archive restore"
    if available_at_ms is not None:
        message += f"; available_at_ms={available_at_ms}"
    return message


def archived_message(files: int) -> str:
    """The trip-row ``error_message`` for an archived trip."""
    return (
        f"{files} file(s) in Deep Archive; restore not requested for this dataset "
        "(settings.request_restore=false)"
    )


def archived_stats(
    conn: sqlite3.Connection, in_scope: set[tuple[str, int, int]]
) -> tuple[int, int]:
    """``(trips, files)`` across in-scope archived rows."""
    rows = conn.execute(
        "SELECT trip_id, sub_trip, health_grade, error_message FROM trips "
        f"WHERE status = '{TRIP_ARCHIVED}'"
    ).fetchall()
    trips = 0
    files = 0
    for trip_id, sub_trip, health_grade, message in rows:
        if (trip_id, int(sub_trip), int(health_grade)) not in in_scope:
            continue
        trips += 1
        files_match = _RESTORE_FILES_RE.match(message or "")
        files += int(files_match.group(1)) if files_match else 1
    return trips, files


def pending_restore_stats(
    conn: sqlite3.Connection, in_scope: set[tuple[str, int, int]]
) -> tuple[int, int, int | None]:
    """``(trips, files, max_available_at_ms)`` across in-scope pending_restore rows."""
    rows = conn.execute(
        "SELECT trip_id, sub_trip, health_grade, error_message FROM trips "
        f"WHERE status = '{TRIP_PENDING_RESTORE}'"
    ).fetchall()
    trips = 0
    files = 0
    max_eta: int | None = None
    for trip_id, sub_trip, health_grade, message in rows:
        if (trip_id, int(sub_trip), int(health_grade)) not in in_scope:
            continue
        trips += 1
        files_match = _RESTORE_FILES_RE.match(message or "")
        files += int(files_match.group(1)) if files_match else 1
        eta_match = _RESTORE_ETA_RE.search(message or "")
        if eta_match:
            eta = int(eta_match.group(1))
            if max_eta is None or eta > max_eta:
                max_eta = eta
    return trips, files, max_eta


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
        # File progress is counted from the events as they pass through, never
        # queried. `files` has one row per file of the dataset — millions on a
        # real one — and this thread also drains the worker queue, so a
        # GROUP BY over it every render made the writer the bottleneck. These
        # count events applied since this pass started; the authoritative
        # per-file state is still in the table for the end-of-run report.
        self._file_events: dict[str, int] = {}

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
                INSERT INTO trips (trip_id, sub_trip, health_grade, status, country,
                                   state, timezone, file_total, error_stage,
                                   error_message, manifest_generated_at, updated_at,
                                   retry_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(trip_id, sub_trip, health_grade) DO UPDATE SET
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
                    event.health_grade,
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
                INSERT INTO files (trip_id, sub_trip, health_grade, rel_path, kind,
                                   status, size_bytes, error_stage, error_message,
                                   updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trip_id, sub_trip, health_grade, rel_path) DO UPDATE SET
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
                    event.health_grade,
                    event.rel_path,
                    event.kind,
                    event.status,
                    event.size_bytes,
                    event.error_stage,
                    event.error_message,
                    now,
                ),
            )
            self._file_events[event.status] = self._file_events.get(event.status, 0) + 1
        self._dirty += 1
        # A trip's terminal status must reach disk right away: it is the only
        # thing a resume after SIGKILL has to go on, and a busy run may not hit
        # the idle-flush or the 100-event batch for the whole of the next trip.
        # One commit per trip is a few per minute; file rows stay batched.
        if event.scope == "trip" or self._dirty >= 100:
            self._flush()

    def _append_failure(self, event: StatusEvent, now: str) -> None:
        if self._failure_log is None:
            return
        line = (
            f"{now}\t{event.trip_id}\t{event.sub_trip}\t{event.health_grade}\t"
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
        # `trips` only: one row per trip, and status is indexed. The `files` table
        # is one row per file (millions on a real dataset) and is NOT queried here
        # — those figures come from the events this writer has applied.
        file_counts = self._file_events
        done = trip_counts.get(TRIP_COMPLETE, 0)
        logger.info(
            "PROGRESS trips %d/%d (complete=%d partial=%d failed=%d pending_restore=%d "
            "archived=%d in_progress=%d) | files this pass dl=%d up=%d failed=%d",
            done,
            self._total,
            done,
            trip_counts.get(TRIP_PARTIAL, 0),
            trip_counts.get(TRIP_FAILED, 0),
            trip_counts.get(TRIP_PENDING_RESTORE, 0),
            trip_counts.get(TRIP_ARCHIVED, 0),
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
    present_keys: Iterable[str], expected_keys: Iterable[str], *, allow_links: bool = False
) -> bool:
    """True if every expected key is present.

    A ``.mp4.link`` only stands in for its ``.mp4`` when ``allow_links`` is set,
    i.e. the current run is a dry run. A real run must not treat a dry run's
    leftover links as delivered footage.
    """
    present = (
        present_keys
        if isinstance(present_keys, (set, frozenset))
        else set(present_keys)
    )
    for key in expected_keys:
        if key in present:
            continue
        if allow_links and key.endswith(".mp4") and f"{key}.link" in present:
            continue
        return False
    return True
