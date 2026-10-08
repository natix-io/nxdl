#!/usr/bin/env python3

import errno
import fcntl
import json
import logging
import os
import sys
import tempfile
import time
from datetime import UTC, datetime
from multiprocessing import Process, Queue
from pathlib import Path
from queue import Empty, Full

from schemas import DatasetOutput
from schemas.dataset_request import DatasetRequest
from app.event_manifest import write_event_manifest
from app.trip_downloader import (
    get_dataset_request,
    get_ds_manifest,
    process_trip,
    recover_interrupted_publishes,
)
from utils import ledger, markers
from utils.ledger import LedgerWriter, Reporter, connect
from utils.path_helpers import FEATURES_DIRNAME, build_dataset_cache_path, safe_name
from utils.s3_uploader import (
    parse_s3_uri,
    prune_empty_region_dirs,
    upload_and_remove_file,
)
from utils.settings import settings
from utils.trip_list import TripKey, format_trip_key, trip_matches
from utils.trip_selection import load_trips_selection

logger = logging.getLogger(__name__)

DATASET_REQUEST_FILENAME = "dataset_request.json"
# Logs live OUTSIDE the dataset dir so it holds only dataset content (and so
# nothing here can ever be mistaken for an uploadable trip file). Resume state is
# ledger.STATE_DIR, shared with app.validate.
LOGS_DIR = Path("logs")
# Client state that must be visible to every copy of the client sharing an output
# dir: the run lock and the dataset marker. Dot-prefixed so QA skips it.
CLIENT_STATE_DIRNAME = ".natix"
# errnos that mean "this filesystem cannot lock", as opposed to "locking failed".
_NO_LOCKING_ERRNOS = frozenset(
    {errno.ENOSYS, errno.ENOLCK, errno.EOPNOTSUPP, errno.ENOTSUP, errno.EINVAL}
)

# Task queue depth. Deliberately bounded and fed while the workers run, never filled
# up front: `Queue()` only *looks* unbounded — it is capped at the platform's
# SEM_VALUE_MAX (2**31-1 on Linux, but **32767 on macOS**), so pre-loading every trip
# blocks forever on the 32768th put on macOS for any dataset bigger than that, before
# a single worker has been started. A small window also keeps memory flat.
TASK_QUEUE_MAXSIZE = 1000
# How long a blocked put waits before re-checking that workers are still alive.
FEED_POLL_SECONDS = 1.0


def _ledger_path(dataset_id: str, output_dir: Path, *, dir_is_ours: bool = False) -> Path:
    return ledger.ledger_path(
        dataset_id,
        output_dir,
        settings.s3_upload,
        dry_mode=settings.dry_mode,
        camera_selection=str(settings.camera_selection),
        endpoint_url=settings.aws_endpoint_url or "",
        dir_is_ours=dir_is_ours,
    )


# Held for the life of the process. An flock lives on the open file description,
# so letting the handle be garbage-collected silently releases the lock.
_dataset_lock_handle = None


def _acquire_dataset_lock(dataset_id: str, output_dir: Path) -> None:
    """One run per (dataset, output dir); the second one exits.

    Two runs of one dataset into one dir race on a single SQLite ledger, share a
    cache, and each startup sweep deletes the other's in-flight .part files. The
    lock lives IN the output dir, not the package's state dir, so two unpacked
    copies of the client see each other. Released by the OS when the process
    dies, so a killed run does not leave the dataset locked.
    """
    global _dataset_lock_handle
    lock_dir = output_dir / CLIENT_STATE_DIRNAME
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{safe_name(dataset_id)}.lock"
    handle = lock_path.open("a")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        logger.error(
            "Dataset %s is already being downloaded into %s by another run "
            "(lock: %s). Wait for it to finish, or use a different --output-dir.",
            dataset_id,
            output_dir,
            lock_path,
        )
        sys.exit(1)
    except OSError as exc:
        handle.close()
        if exc.errno not in _NO_LOCKING_ERRNOS:
            logger.error(
                "Could not acquire the run lock %s: %s. Refusing to start without it.",
                lock_path,
                exc,
            )
            sys.exit(1)
        # Lustre without -o flock (ENOSYS), NFS without lockd (ENOLCK): the
        # filesystem genuinely cannot lock. Not a silent fallback — unlocked runs
        # corrupt each other — so it takes an explicit decision.
        if not settings.allow_unlocked_runs:
            logger.error(
                "This filesystem does not support locking (%s), so %s cannot "
                "guarantee dataset %s is not already running here. Re-run with "
                "ALLOW_UNLOCKED_RUNS=true if you are certain no other run is active.",
                exc,
                lock_path,
                dataset_id,
            )
            sys.exit(1)
        logger.warning(
            "This filesystem does not support locking (%s) and ALLOW_UNLOCKED_RUNS "
            "is set; nothing will stop a second run of dataset %s here.",
            exc,
            dataset_id,
        )
        return
    handle.truncate(0)
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    _dataset_lock_handle = handle


# Held for the life of a worker process, and of the parent. See _claim_worker_slot.
_worker_lock_handle = None


def worker_lock_path(output_dir: Path, dataset_id: str) -> Path:
    return output_dir / CLIENT_STATE_DIRNAME / f"{safe_name(dataset_id)}.workers.lock"


def _refuse_if_workers_are_still_running(lock_path: Path, dataset_id: str) -> bool:
    """Refuse to start while a previous run's workers are still alive.

    The run lock above lives only in the PARENT, and on macOS workers are
    spawned, so they never inherit its file descriptor. Kill only the parent —
    an OOM kill, a closed terminal — and the lock is released while its workers
    keep downloading and publishing. The next run then takes the lock and runs
    start-up recovery against files a live worker still owns, deleting its
    in-flight ``.copying-`` tree or its ``.part`` files.

    So workers hold a SHARED lock of their own for as long as they live, and a
    starting run first takes that same lock EXCLUSIVELY: it succeeds only when
    no worker of any previous run is left. The exclusive hold is then downgraded
    to shared for this run's own lifetime, so the file always has a holder while
    the run is alive and the next run's probe is equally honest.

    Returns whether the lock is in force, which the workers need: only the parent
    can tell "this filesystem cannot lock" (already reported, and the user opted
    in) from "locking works here and my acquisition failed".
    """
    global _worker_lock_handle
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        logger.error(
            "Worker processes from an earlier run of dataset %s are still active in "
            "this output dir (lock: %s). They are still writing files, so starting "
            "now would corrupt them. Wait for them to finish, or kill them.",
            dataset_id,
            lock_path,
        )
        sys.exit(1)
    except OSError as exc:
        handle.close()
        if exc.errno in _NO_LOCKING_ERRNOS:
            return False  # already reported by the run-lock acquisition above
        logger.error(
            "Could not probe the worker lock %s: %s. Refusing to start without it.",
            lock_path,
            exc,
        )
        sys.exit(1)
    fcntl.flock(handle, fcntl.LOCK_SH)
    _worker_lock_handle = handle
    return True


def _claim_worker_slot(lock_path: Path, *, required: bool) -> bool:
    """Hold the shared worker lock for this worker's lifetime. Did it hold?

    Called in the worker process, so it works under spawn as well as fork. The
    handle is kept in a module global: an flock lives on the open file
    description, so letting it be garbage-collected would silently release it.

    ``required`` is the parent's own result, passed down rather than guessed at
    from the errno. Swallowing every OSError here let a transient EIO leave a
    worker publishing with no slot held: invisible to the next run's exclusive
    probe, which would then run recovery over its live ``.copying-`` tree. An
    errno cannot stand in for the parent's decision -- ENOLCK is "no lock
    support" on one filesystem and "ran out of lock table" on another.
    """
    global _worker_lock_handle
    try:
        handle = lock_path.open("a")
        fcntl.flock(handle, fcntl.LOCK_SH)
    except OSError as exc:
        if required:
            logger.error(
                "Could not take the worker slot %s: %s. Stopping rather than "
                "working where the next run cannot see this process.",
                lock_path,
                exc,
            )
            return False
        return True  # filesystem without locking; the parent already decided
    _worker_lock_handle = handle
    return True

def _scope_digest(output_dir: Path) -> str:
    return ledger.scope_digest(
        output_dir,
        settings.s3_upload,
        dry_mode=settings.dry_mode,
        camera_selection=str(settings.camera_selection),
        endpoint_url=settings.aws_endpoint_url or "",
    )


def _import_legacy_ledgers_once(output_dir: Path, db_path: Path) -> None:
    """Seed markers from pre-marker ledgers, output-wide, exactly once.

    Every scope whose ledger describes this output dir is imported in the same
    step, before any trip of this run is published, and legacy import is then
    disabled for this directory for good.

    Doing it per scope instead would leave a hole: a scope that had not yet
    migrated could import its pre-upgrade ledger AFTER another scope had already
    replaced a trip's directory, reviving a claim on data that is no longer
    there. There is exactly one moment at which the old ledgers are all still
    true — before anything new is published — and that is when they are read.
    """
    if markers.is_migrated(output_dir):
        return
    ledgers = ledger.ledgers_for_output_dir(output_dir, db_path.parent)
    imported = markers.import_legacy_ledgers(output_dir, ledgers)
    if imported:
        logger.info(
            "Recorded %d completed trip(s) from %d existing ledger(s) as markers "
            "under %s; future runs resume from there even without state/",
            imported,
            len(ledgers),
            markers.markers_root(output_dir),
        )


def _dedupe_trips(
    trips: list[tuple[str, int, int]], dataset_id: str
) -> list[tuple[str, int, int]]:
    """Drop repeated trip keys from the dataset manifest, keeping the first.

    The service can list the same ``(trip_id, sub_trip, health_grade)`` twice in
    one manifest (4,865 of dataset 6a8717's 119,775 rows, 2026-09-15). A repeat
    is not a second trip — the key names the cache dir AND the output folder, so
    two workers would download the same files into the same cache dir and race
    to publish into the same folder.
    """
    seen: set[tuple[str, int, int]] = set()
    unique: list[tuple[str, int, int]] = []
    for trip in trips:
        if trip in seen:
            continue
        seen.add(trip)
        unique.append(trip)
    dropped = len(trips) - len(unique)
    if dropped:
        logger.warning(
            "Dataset %s manifest lists %d duplicate trip(s) of its %d row(s); "
            "each trip is processed once.",
            dataset_id,
            dropped,
            len(trips),
        )
    return unique


def _unmatched_requested_trips(
    all_trips: list[tuple[str, int, int]],
    requested: list[TripKey],
) -> list[TripKey]:
    """Requested TRIPS_FILE keys that no trip in the dataset satisfies."""
    return [
        trip
        for trip in requested
        if not any(trip_matches({trip}, *candidate) for candidate in all_trips)
    ]


def _filter_requested_trips(
    all_trips: list[tuple[str, int, int]],
    requested: list[TripKey],
) -> list[tuple[str, int, int]]:
    """Keep only trips selected by TRIPS_FILE.

    Keys come from ``utils.trip_selection`` and are concrete (an entry without a
    ``health_grade`` means grade 500), so each selects exactly one dataset trip.
    """
    requested_set = set(requested)
    return [trip for trip in all_trips if trip_matches(requested_set, *trip)]


def _filter_pending_trips(
    all_trips: list[tuple[str, int, int]],
    done: set[tuple[str, int, int]],
) -> list[tuple[str, int, int]]:
    """Keep only trips not yet marked complete in the ledger.

    The comparison is on the whole ``(trip_id, sub_trip, health_grade)`` key: one
    dataset can hold the same pair at several grades, and each is a separate trip
    with its own files. Matching on the pair alone would drop every other grade as
    soon as one of them completed — a silent loss on resume.
    """
    return [trip for trip in all_trips if trip not in done]


def _filter_retryable_trips(
    pending: list[tuple[str, int, int]],
    waiting: set[tuple[str, int, int]],
) -> list[tuple[str, int, int]]:
    """Drop trips whose only blocker is a Deep-Archive restore.

    A restore takes hours (~12h standard / ~48h bulk tier), so an in-run retry
    pass seconds later cannot help them and would only hammer the manifest
    endpoint. They are not complete, so a future run re-enqueues them.
    """
    return [trip for trip in pending if trip not in waiting]


_RESTORE_TIER_ETA_HINT = {"standard": "~12h", "bulk": "~48h"}


def _pending_restore_message(
    trips: int, files: int, max_eta_ms: int | None, restore_tier: str
) -> str:
    """End-of-run summary for files still being restored from S3 Deep Archive.

    Deliberately non-technical: the person running the client just needs to know
    part of the dataset is still being prepared and when to re-run.
    """
    if max_eta_ms is not None:
        when = datetime.fromtimestamp(max_eta_ms / 1000).astimezone()
        retry = f"after ~{when.strftime('%Y-%m-%d %H:%M %Z')}"
    else:
        retry = f"in {_RESTORE_TIER_ETA_HINT.get(restore_tier, '~48h')}"
    return (
        f"{files} file(s) across {trips} trip(s) are still being prepared and "
        f"will take a while. Re-run the same command {retry} to download them."
    )


def _setup_logging(stamp: str, dataset_id: str) -> Path:
    """Terminal shows progress + milestones (INFO); a per-run file gets full detail (DEBUG).

    The file handler and console handler are inherited across the multiprocessing
    fork, so worker output flows to the same places without per-worker setup.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"{safe_name(dataset_id)}_downloader_{stamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(processName)s  %(message)s"
        )
    )

    root.addHandler(console)
    root.addHandler(file_handler)

    # Keep the AWS SDK's per-request chatter out of even the debug file.
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return log_file


def _dir_owner_from_request(output_dir: Path) -> str | None:
    """The dataset a pre-marker output dir belongs to, from the request file
    every client version writes at its root. None if there is none to read."""
    request = output_dir / DATASET_REQUEST_FILENAME
    if not request.is_file():
        return None
    try:
        return str(json.loads(request.read_text(encoding="utf-8"))["request_id"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _has_dataset_content(output_dir: Path) -> bool:
    """Anything in the dir that is a delivered trip.

    The client's own dot-state does not count, and neither does
    ``dataset_request.json``: it is bookkeeping this run writes itself, before
    anything is downloaded, so counting it would make every directory look
    occupied — including one whose trips have all been delivered elsewhere and
    removed.
    """
    return any(
        not p.name.startswith(".") and p.name not in _NOT_DATASET_CONTENT
        for p in output_dir.iterdir()
    )


# Written into the output dir but not part of the delivered dataset.
_NOT_DATASET_CONTENT = frozenset({DATASET_REQUEST_FILENAME, "qa_report.txt"})


def _guard_single_dataset(output_dir: Path, dataset_id: str) -> bool:
    """Refuse to mix datasets in one output dir. Returns whether the dir was
    already proven to be this dataset's (marker or request file), which lets a
    pre-scoping ledger be migrated with confidence.

    One dataset per directory is the design: the trip tree is keyed by
    country/trip/grade with no dataset in the path, so a second dataset writes
    over the first's folders. Ownership is a marker INSIDE the output dir, so
    every copy of the client sharing that directory sees it.

    A directory with content but no marker was written by an older client. It is
    claimed only if its dataset_request.json names THIS dataset — otherwise
    whichever dataset ran first would have taken over another's data.
    """
    marker_dir = output_dir / CLIENT_STATE_DIRNAME
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker = marker_dir / "dataset"

    if not marker.exists():
        if _has_dataset_content(output_dir):
            owner = _dir_owner_from_request(output_dir)
            if owner != dataset_id:
                logger.error(
                    "Output dir %s already contains data%s and has no dataset "
                    "marker — refusing to claim it for dataset %s. Use a fresh "
                    "--output-dir.",
                    output_dir,
                    f" belonging to dataset {owner}" if owner else "",
                    dataset_id,
                )
                sys.exit(1)
            proven = True
        else:
            proven = False
        # Publish the marker atomically WITH its content: a private temp file
        # (mkstemp — pids repeat across containers sharing a filesystem) linked
        # into place, then re-read, so no competitor can ever observe it empty
        # or believe it linked its own content.
        fd, tmp_name = tempfile.mkstemp(dir=marker_dir, prefix=".dataset.", suffix=".tmp")
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(dataset_id + "\n")
            try:
                os.link(tmp, marker)
            except FileExistsError:
                pass
        finally:
            tmp.unlink(missing_ok=True)
    else:
        proven = True

    owner = marker.read_text(encoding="utf-8").strip() if marker.exists() else ""
    if not owner:
        logger.error(
            "Output dir %s has an unreadable dataset marker (%s); refusing to "
            "guess who owns it. Fix or remove the marker if the dir is yours.",
            output_dir,
            marker,
        )
        sys.exit(1)
    if owner != dataset_id:
        logger.error(
            "Output dir %s already holds dataset %s — refusing to download dataset "
            "%s into it. Each dataset needs its own --output-dir.",
            output_dir,
            owner,
            dataset_id,
        )
        sys.exit(1)
    return proven


def _unverified_completions(
    done: set[tuple[str, int, int]],
    in_scope: set[tuple[str, int, int]],
    s3_target: tuple[str, str] | None,
    present_keys: set[str] | None,
) -> int:
    """How many completions this run will skip on the resume state's word alone.

    With a listing, every completion is checked against the bucket at start-up and
    the ones whose objects are gone are dropped. Without one — the LIST threw —
    the rows are kept, because dropping them costs a second download of the whole
    dataset over what is usually a transient throttle. Kept is not verified
    though: these trips are skipped without anything confirming their objects are
    still there, so the run must not go on to report success.
    """
    if s3_target is None or present_keys is not None:
        return 0
    return len(done & in_scope)


def _drop_completions_over_an_empty_dir(conn, output_dir: Path) -> None:
    """A local delivery cannot be complete into a directory with nothing in it.

    Progress is scoped per delivery target, which stops an S3 run from reusing a
    local run's progress — but not the other way round. Run locally, run the same
    dataset and directory with S3_UPLOAD (which uploads each trip and deletes its
    local copy), then run locally again: the local ledger still marks every trip
    complete, so nothing is downloaded and an empty directory is reported as a
    finished dataset.

    An S3 run legitimately leaves the directory empty, so this applies only to
    local delivery. One listing, not a walk.
    """
    if settings.s3_upload:
        return
    if _has_dataset_content(output_dir):
        return
    # Through ``drop_completions``, not a bare DELETE: the completion's file rows
    # key on the trip alone, so ones left behind here are re-attached to the next
    # completion of the same trip and disprove it too.
    dropped = ledger.drop_completions(conn, ledger.completed_trip_keys(conn))
    if dropped:
        logger.warning(
            "Resume state claimed %d completed trip(s) but %s holds no dataset "
            "content — another run has delivered them elsewhere and removed them. "
            "Starting over.",
            dropped,
            output_dir,
        )


def _delivered_files(trip_dir: Path) -> set[Path]:
    """What the trip directory actually holds, ignoring this client's own state.

    Compared against the row's recorded files as a SET, not a subset: a file
    missing means the completion lost data, and a file the row never named means
    something else published here. Only the second direction catches a WIDENING
    scope -- run `front`, run `all` into the same dir, run `front` again, and
    every file the front run recorded is still there, under rear footage the
    front run never asked for and would not report.

    Our own transients are not another scope's work, so they are not evidence of
    one. ``features/`` is deleted before publication and an ``.xmp`` sidecar is
    consumed by exiftool, but both are removed in ways that can leave the file
    behind (``rmtree(ignore_errors=True)``; a sidecar kept for retry after a
    failed embed). Replaying a real 38-file delivery through this check showed
    either one costing that trip a full re-download.
    """
    return {
        path
        for path in trip_dir.rglob("*")
        if path.is_file()
        and not path.name.startswith(".")
        and path.suffix != ".xmp"
        and FEATURES_DIRNAME not in path.relative_to(trip_dir).parts
    }


def _drop_completions_without_their_files(
    conn,
    db_path: Path,
    output_dir: Path,
    present_keys: set[str] | None,
    *,
    force: bool = False,
) -> None:
    """A completion stands only while the files it recorded are still there.

    Scopes keep separate ledgers, and a scope only ever writes its own — so when
    another scope publishes over this one's output, nothing tells this ledger.
    Real then dry then real replaces every video with a ``.mp4.link`` and the
    real ledger still says complete; ``all`` then ``front`` then ``all`` leaves
    the rear footage gone the same way; a local run followed by an S3 run that
    uploaded half the trips and removed them leaves a directory that is neither
    empty nor whole. In every case the row's recorded files are what changed, so
    they are what is checked.

    Locally the check is a stat per recorded file, and it runs only when another
    scope's ledger names this directory — a single-scope directory cannot have
    been published over, and pays nothing. With an S3 target the files are meant
    to be gone locally; the fresh bucket listing is the evidence instead, and it
    is consulted every run because another output dir's run can roll objects
    out of the shared prefix and cannot reach this ledger to say so.
    """
    if settings.s3_upload:
        if present_keys is None:
            # Cannot verify is not the same as invalid. Dropping here costs the
            # whole dataset: the fast-resume branch is gated on the listing too,
            # and an uploaded trip has no local copy left, so every completion
            # would be downloaded again -- and a LIST over a large prefix is
            # exactly where an expiring token or a throttle lands. Keeping a
            # completion whose objects another run rolled back costs one
            # re-upload, noticed by the next run that can list.
            logger.warning(
                "Could not list %s; keeping the resume state unverified this run. "
                "Trips it calls complete are skipped without being checked against "
                "the bucket, so this run cannot report success.",
                settings.s3_upload,
            )
            return
        _, prefix = parse_s3_uri(settings.s3_upload)

        def s3_key(path: Path) -> str:
            rel = path.relative_to(output_dir).as_posix()
            return f"{prefix}/{rel}" if prefix else rel

        # Only what the row names is checked, never what else the prefix holds: S3
        # keys carry no dataset identity and one trip belongs to many datasets, so
        # another dataset's objects under the same trip prefix are legitimate.
        gone = [
            key
            for key, (_dir, files) in ledger.completed_rows_with_files(
                conn, output_dir
            ).items()
            if not files or any(s3_key(p) not in present_keys for p in files)
        ]
        where = settings.s3_upload
    else:
        if not force and not ledger.other_scopes_touched(db_path, output_dir):
            return
        gone = [
            key
            for key, (trip_dir, files) in ledger.completed_rows_with_files(
                conn, output_dir
            ).items()
            if not files or _delivered_files(trip_dir) != set(files)
        ]
        where = str(output_dir)
    dropped = ledger.drop_completions(conn, gone)
    if dropped:
        logger.warning(
            "Resume state claimed %d completed trip(s) whose files are no longer in "
            "%s — another run has replaced or removed them. They will be fetched "
            "again.",
            dropped,
            where,
        )


def _guard_single_dataset_on_s3(
    s3_target: tuple[str, str], dataset_id: str, present_keys: set[str] | None
) -> None:
    """Refuse to deliver two datasets into one S3 prefix.

    An S3 key is the prefix plus the trip's path — ``<prefix>/<country>/<trip>/…``
    — with no dataset anywhere in it. Two datasets pointed at one prefix therefore
    collide on every trip they share, and they DO share trips: a trip can belong
    to many datasets. The collision is not benign, because the two deliveries are
    not the same bytes (a different camera selection or output config produces a
    different file set at the same paths), and because an upload that fails
    part-way used to delete every key of the trip, including the other dataset's.

    So ownership is recorded in the prefix itself, the same way it is in the
    output dir. A prefix that already holds content but no marker predates this
    check. It is adopted when the prefix can prove whose it is — the client
    uploads the saved ``dataset_request.json`` to the prefix root at the end of
    every run, and it names the dataset — and refused when that file names
    another dataset. A prefix with content and no request file at all (an
    interrupted pre-guard delivery) is refused unless ``S3_ADOPT_PREFIX=true``
    says this run knows it is the owner. Runs before any worker is forked, so
    nothing is uploaded first.
    """
    from utils import s3_uploader  # noqa: PLC0415 - boto3 stays a lazy import

    bucket, prefix = s3_target
    client = _local_s3_client()
    try:
        owner = s3_uploader.read_prefix_owner(client, bucket, prefix)
        if owner is None:
            if present_keys is None:
                # The listing failed, so "no content" is not something we know.
                # Claiming on that basis is how an occupied legacy prefix gets
                # adopted by the wrong dataset.
                logger.error(
                    "Could not list s3://%s/%s, so whether it already holds another "
                    "dataset is unknown. Refusing to claim it for dataset %s.",
                    bucket,
                    prefix,
                    dataset_id,
                )
                sys.exit(1)
            named = None
            if s3_uploader.prefix_request_key(prefix) in present_keys:
                named = s3_uploader.read_prefix_request_owner(client, bucket, prefix)
            if named is not None and named != dataset_id:
                logger.error(
                    "s3://%s/%s holds dataset %s (per its dataset_request.json) — "
                    "refusing to upload dataset %s into it. Each dataset needs its own "
                    "S3 prefix.",
                    bucket,
                    prefix,
                    named,
                    dataset_id,
                )
                sys.exit(1)
            if named is None and s3_uploader.prefix_has_dataset_content(
                prefix, present_keys
            ):
                if not settings.s3_adopt_prefix:
                    logger.error(
                        "s3://%s/%s already holds data and does not record which "
                        "dataset it belongs to. Refusing to add dataset %s to it: two "
                        "datasets would share object keys. If this prefix IS dataset "
                        "%s's (a delivery interrupted before this check existed), "
                        "re-run once with S3_ADOPT_PREFIX=true to record that; "
                        "otherwise use a prefix of its own.",
                        bucket,
                        prefix,
                        dataset_id,
                        dataset_id,
                    )
                    sys.exit(1)
                logger.warning(
                    "Adopting s3://%s/%s for dataset %s on S3_ADOPT_PREFIX=true; it "
                    "holds data that records no owner.",
                    bucket,
                    prefix,
                    dataset_id,
                )
            elif named == dataset_id:
                logger.info(
                    "s3://%s/%s holds this dataset's request file; adopting it for "
                    "dataset %s.",
                    bucket,
                    prefix,
                    dataset_id,
                )
            winner = s3_uploader.claim_prefix(client, bucket, prefix, dataset_id)
            if winner != dataset_id:
                # Another run claimed it at the same moment and won. Both wrote;
                # the read-back decides, and exactly one proceeds.
                logger.error(
                    "s3://%s/%s was claimed by dataset %s while this run was "
                    "claiming it for %s. Each dataset needs its own S3 prefix.",
                    bucket,
                    prefix,
                    winner,
                    dataset_id,
                )
                sys.exit(1)
            logger.info(
                "Claimed s3://%s/%s for dataset %s", bucket, prefix, dataset_id
            )
            return
        if owner != dataset_id:
            logger.error(
                "s3://%s/%s holds dataset %s — refusing to upload dataset %s into it. "
                "Each dataset needs its own S3 prefix.",
                bucket,
                prefix,
                owner,
                dataset_id,
            )
            sys.exit(1)
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "Could not read or write the dataset marker under s3://%s/%s; refusing "
            "to upload rather than risk mixing two datasets in one prefix.",
            bucket,
            prefix,
        )
        sys.exit(1)
    finally:
        # Never leave a boto3 client in the module global before the fork.
        s3_uploader._s3_client = None


def _local_s3_client():
    """A boto3 client scoped to this call, built from the same settings the
    uploader uses. Deliberately not the module-global one: this runs before the
    workers are forked, and a client must not be inherited by them."""
    import boto3  # noqa: PLC0415

    kwargs: dict[str, str] = {}
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key:
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_region:
        kwargs["region_name"] = settings.aws_region
    if settings.aws_endpoint_url:
        kwargs["endpoint_url"] = settings.aws_endpoint_url
    return boto3.client("s3", **kwargs)


def _save_dataset_request(output_dir: Path, dataset_request) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    request_path = output_dir / DATASET_REQUEST_FILENAME
    payload = {
        "request_id": dataset_request.request_id,
        "request": dataset_request.request,
    }
    request_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    logger.info("Saved dataset request to %s", request_path)


# How long a worker waits for a task before re-checking that its parent is alive.
_WORKER_GET_TIMEOUT = 5.0


def _trip_worker(
    task_queue: Queue,
    status_queue: Queue,
    api_key: str,
    dataset_id: str,
    output_dir: Path,
    cache_dir: Path,
    output: DatasetOutput,
    present_keys: set[str] | None,
    s3_target: tuple[str, str] | None,
    worker_lock: Path,
    parent_pid: int,
    locking_required: bool,
    minute_selection: dict[tuple[str, int, int], frozenset[str]] | None = None,
) -> None:
    # Before any work: hold the shared worker lock, so a run that starts after
    # this process's parent dies cannot run recovery over files still being
    # written here. Under spawn this process inherits nothing, so it takes its
    # own.
    if not _claim_worker_slot(worker_lock, required=locking_required):
        return
    reporter = Reporter(status_queue)
    try:
        _worker_loop(
            task_queue,
            api_key,
            dataset_id,
            output_dir,
            cache_dir,
            output,
            present_keys,
            s3_target,
            reporter,
            parent_pid,
            minute_selection,
        )
    finally:
        # Every way out of the loop passes here, and the sentinel is one of them:
        # the parent can queue the sentinels and die before this worker reads one.
        # Exiting joins the status queue's feeder thread, which blocks forever on
        # a full pipe once the ledger writer that drained it is gone -- and this
        # process still holds the shared worker lock, so every later run in this
        # output dir would refuse to start. Nothing will read that buffer, so drop
        # it. A live parent still gets every event, which is why this is not
        # unconditional. (A parent that dies DURING the flush still hangs; that
        # needs the feeder watched, not a check at one point in time.)
        if os.getppid() != parent_pid:
            status_queue.cancel_join_thread()


def _worker_loop(
    task_queue: Queue,
    api_key: str,
    dataset_id: str,
    output_dir: Path,
    cache_dir: Path,
    output: DatasetOutput,
    present_keys: set[str] | None,
    s3_target: tuple[str, str] | None,
    reporter: Reporter,
    parent_pid: int,
    minute_selection: dict[tuple[str, int, int], frozenset[str]] | None = None,
) -> None:
    while True:
        # The lock alone leaves a window: between being started and taking it,
        # this process is invisible to a replacement run's probe, so one can
        # start and then race a worker that only claims its slot afterwards.
        # Outliving the parent is the condition that makes that dangerous, and
        # it is directly observable — so stop instead of publishing into a
        # directory another run now owns.
        if os.getppid() != parent_pid:
            logger.warning(
                "Parent process %d is gone; stopping rather than writing into an "
                "output dir another run may now own.",
                parent_pid,
            )
            break
        # Blocking get + a None sentinel per worker: `get_nowait` raced the parent's
        # feeder thread, so a worker saw Empty on a queue that still held thousands of
        # trips and exited — most workers died on startup and trips were skipped.
        # The timeout only wakes the loop so the ppid check above runs again:
        # Empty means "nothing yet", never "done". Without it a worker killed
        # between trips blocks here forever — the parent that would queue the
        # sentinels is gone, the siblings still hold the queue's write end, and
        # the worker keeps the shared flock, so every later run in this output
        # dir refuses to start and there is nothing left to wait for.
        try:
            task = task_queue.get(timeout=_WORKER_GET_TIMEOUT)
        except Empty:
            continue
        if task is None:
            break
        trip_id, sub_trip, health_grade = task
        process_trip(
            api_key,
            dataset_id,
            trip_id,
            sub_trip,
            output_dir,
            output,
            cache_dir=cache_dir,
            reporter=reporter,
            present_keys=present_keys,
            s3_target=s3_target,
            health_grade=health_grade,
            minutes=(minute_selection or {}).get(task),
        )


def _feed_queue(
    task_queue: Queue,
    trips: list[tuple[str, int, int]],
    workers: list[Process],
) -> bool:
    """Feed ``trips`` then one ``None`` sentinel per worker onto a bounded queue.

    Call this only AFTER the workers are running — a bounded queue blocks on ``put``
    once full, so with no consumer it would never return. Returns ``False`` (and logs)
    if every worker died while the queue was full, instead of blocking forever.
    """
    items: list[tuple[str, int, int] | None] = [*trips, *([None] * len(workers))]
    for index, item in enumerate(items):
        while True:
            try:
                task_queue.put(item, timeout=FEED_POLL_SECONDS)
                break
            except Full:
                if not any(worker.is_alive() for worker in workers):
                    logger.error(
                        "All %d worker(s) exited early; %d of %d queue item(s) were "
                        "never handed out. Remaining trips stay pending for the next "
                        "pass or run.",
                        len(workers),
                        len(items) - index,
                        len(items),
                    )
                    return False
    return True


def _run_pass(
    trips: list[tuple[str, int, int]],
    *,
    api_key: str,
    dataset_id: str,
    output_dir: Path,
    cache_dir: Path,
    dataset_output: DatasetOutput,
    present_keys: set[str] | None,
    s3_target: tuple[str, str] | None,
    minute_selection: dict[tuple[str, int, int], frozenset[str]],
    total_trips: int,
    worker_count: int,
    db_path: Path,
    failure_log: Path,
    locking_required: bool,
) -> None:
    """Fork workers over ``trips``, running a single ledger writer in the parent."""
    task_queue: Queue = Queue(maxsize=TASK_QUEUE_MAXSIZE)
    status_queue: Queue = Queue()

    workers = [
        Process(
            target=_trip_worker,
            args=(
                task_queue,
                status_queue,
                api_key,
                dataset_id,
                output_dir,
                cache_dir,
                dataset_output,
                present_keys,
                s3_target,
                worker_lock_path(output_dir, dataset_id),
                os.getpid(),
                locking_required,
                minute_selection,
            ),
        )
        for _ in range(worker_count)
    ]

    # Fork FIRST, then open the writer connection + start the writer thread, so no
    # child inherits the SQLite fd or the writer thread.
    for worker in workers:
        worker.start()

    conn = connect(db_path)
    writer = LedgerWriter(conn, status_queue, total_trips=total_trips)
    writer.attach_failure_log(failure_log)
    writer.start()

    # Feed last: the queue is bounded, so the workers must already be draining it.
    _feed_queue(task_queue, trips, workers)

    for worker in workers:
        worker.join()

    writer.stop()
    writer.join()
    conn.close()


def main() -> None:
    run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    dataset_id = settings.dataset_id.strip()
    api_key = settings.api_key.strip()
    # Dataset in the filename: two datasets started in the same UTC second would
    # otherwise write one log file.
    log_file = _setup_logging(run_stamp, dataset_id)
    failure_log = LOGS_DIR / f"{safe_name(dataset_id)}_failed_trips_{run_stamp}.log"
    logger.info("Full logs: %s", log_file)

    output_dir: Path = settings.output_dir.resolve()
    if output_dir.exists() and not output_dir.is_dir():
        logger.error("Output path '%s' exists but is not a directory.", output_dir)
        sys.exit(1)

    if not api_key or not dataset_id:
        logger.error(
            "Missing API_KEY or DATASET_ID in the active .env file. "
            "Please update .env.%s before running the downloader.",
            os.getenv("APP_ENV", "development"),
        )
        sys.exit(1)

    worker_count = settings.max_parallel_trips or 1
    output_dir.mkdir(parents=True, exist_ok=True)
    # One dataset per output dir, and one run of it at a time. The guard also
    # tells us whether the dir is already provably ours, which decides whether a
    # pre-scoping ledger may be migrated rather than started over.
    dir_is_ours = _guard_single_dataset(output_dir, dataset_id)
    _acquire_dataset_lock(dataset_id, output_dir)
    # Only now is it safe to touch files: nothing from a previous run is writing.
    locking_required = _refuse_if_workers_are_still_running(
        worker_lock_path(output_dir, dataset_id), dataset_id
    )
    db_path = _ledger_path(dataset_id, output_dir, dir_is_ours=dir_is_ours)
    # Leave this scope's digest in the output dir, where any copy of the client
    # can see that a scope other than its own has run here.
    ledger.register_scope(output_dir, db_path.stem.rsplit("__", 1)[-1])

    # Per-(dataset, output dir, mode) cache: two instances used to share one
    # subtree, where either startup sweep deleted the other's in-flight .part
    # files — and a dry run's .link files must not leak into a real run's trip.
    cache_dir = build_dataset_cache_path(
        settings.cache_dir,
        dataset_id,
        output_dir,
        dry_mode=settings.dry_mode,
        camera_selection=str(settings.camera_selection),
    )

    # Finish any publish a previous run was killed halfway through. This looks at
    # the region level only; nothing at start-up may walk the dataset or cache
    # tree (``.part`` leftovers are cleaned per trip by the worker that owns it).
    # Each start-up phase is timed so a slow start can be attributed from the log.
    phase_start = time.monotonic()
    recover_interrupted_publishes(output_dir)
    logger.info("Start-up: publish recovery took %.1fs", time.monotonic() - phase_start)

    try:
        phase_start = time.monotonic()
        ds_manifest = get_ds_manifest(api_key, dataset_id)
        logger.info(
            "Fetched ds_manifest for dataset %s — %d trip(s) in %.1fs",
            dataset_id,
            len(ds_manifest.trips),
            time.monotonic() - phase_start,
        )
    except Exception:
        logger.exception("Failed to fetch ds_manifest for dataset %s", dataset_id)
        sys.exit(1)

    try:
        dataset_request = get_dataset_request(api_key, dataset_id)
        _save_dataset_request(output_dir, dataset_request)
    except Exception:
        logger.exception(
            "Failed to fetch/save dataset request for dataset %s", dataset_id
        )
        sys.exit(1)

    dataset_request_model = DatasetRequest.model_validate(dataset_request.request)
    dataset_output: DatasetOutput = dataset_request_model.output

    if not ds_manifest.trips:
        logger.info("No trips found for dataset %s", dataset_id)
        return

    s3_target: tuple[str, str] | None = None
    present_keys: set[str] | None = None
    if settings.s3_upload:
        s3_target = parse_s3_uri(settings.s3_upload)
        # One LIST sweep → what is already on S3, so re-runs skip completed trips
        # even when the local ledger is gone (prod deletes local files on upload).
        try:
            present_keys = ledger.build_present_keys(settings.s3_upload)
            logger.info(
                "S3 already holds %d object(s) under the prefix", len(present_keys)
            )
        except Exception:
            logger.exception("Could not list S3 for resume; continuing without it")
        _guard_single_dataset_on_s3(s3_target, dataset_id, present_keys)

    all_trips = _dedupe_trips(
        [(trip.trip_id, trip.sub_trip, trip.health_grade) for trip in ds_manifest.trips],
        dataset_id,
    )

    # Optional subset: process only the trips (and, per trip, optionally only the
    # minutes) listed in the JSON TRIPS_FILE. Minute names are resolved against
    # each trip's manifest inside process_trip.
    minute_selection: dict[tuple[str, int, int], frozenset[str]] = {}
    if settings.trips_file.strip():
        trips_path = Path(settings.trips_file).expanduser()
        try:
            selection = load_trips_selection(trips_path)
        except FileNotFoundError:
            logger.error("TRIPS_FILE not found: %s", trips_path)
            sys.exit(1)
        except ValueError as exc:
            logger.error("TRIPS_FILE is invalid: %s", exc)
            sys.exit(1)
        requested: list[TripKey] = list(selection.keys())
        minute_selection = {
            key: minutes for key, minutes in selection.items() if minutes is not None
        }
        dataset_trip_count = len(all_trips)
        missing = _unmatched_requested_trips(all_trips, requested)
        if missing:
            logger.warning(
                "%d requested trip(s) are not in dataset %s and will be skipped: %s",
                len(missing),
                dataset_id,
                ", ".join(format_trip_key(trip) for trip in missing),
            )
        all_trips = _filter_requested_trips(all_trips, requested)
        if not all_trips:
            logger.error(
                "None of the %d requested trip(s) are in dataset %s; nothing to do.",
                len(requested),
                dataset_id,
            )
            return
        logger.info(
            "Trip filter active (%s): processing %d of %d dataset trip(s), "
            "%d with a minute selection",
            trips_path,
            len(all_trips),
            dataset_trip_count,
            sum(1 for trip in all_trips if trip in minute_selection),
        )

    total = len(all_trips)
    # Only trips in scope for this run count towards the tally — the ledger may hold
    # complete rows for trips a TRIPS_FILE subset excluded.
    in_scope = set(all_trips)

    # Resume: skip trips already marked complete in the ledger. Archived trips
    # are NOT skipped: the dataset may have been switched to request_restore=true
    # since the last run, so a fresh run re-fetches their manifests.
    phase_start = time.monotonic()
    # Resume state that travels with the data: one marker file per delivered trip
    # under <output>/.natix/done/<scope>/. The ledger lives in the client package,
    # so a fresh copy of the client, a moved package or a wiped state/ dir would
    # otherwise re-download the whole dataset.
    # Markers record LOCAL delivery only. An S3 run's trips live in a bucket
    # that another output dir's run can roll back objects out of, and markers
    # under this output dir cannot be revoked from there — so the fresh bucket
    # listing is the resume signal in S3 mode, and the marker machinery is left
    # alone rather than consulted or reconciled against.
    use_markers = not settings.s3_upload
    if use_markers:
        _import_legacy_ledgers_once(output_dir, db_path)
        marker_keys = markers.drop_markers_without_a_directory(
            output_dir,
            _scope_digest(output_dir),
            markers.completed_keys(output_dir, _scope_digest(output_dir)),
        )
    else:
        marker_keys = set()

    conn = connect(db_path)
    # Bind this ledger to the dir and dataset it describes.
    ledger.stamp_output_dir(conn, output_dir, dataset_id)
    # The markers are what is actually on disk; make the ledger say the same, so
    # retry passes and the final tally read one truth rather than two.
    marker_digest = _scope_digest(output_dir)
    added, dropped = (
        ledger.reconcile_with_markers(
            conn,
            marker_keys,
            evidence_for=lambda key: markers.marker_evidence(
                output_dir, marker_digest, key
            ),
        )
        if use_markers
        else (0, 0)
    )
    if added or dropped:
        logger.info(
            "Resume state reconciled with %s: %d trip(s) recovered from disk, "
            "%d stale ledger row(s) dropped",
            markers.markers_root(output_dir),
            added,
            dropped,
        )
    # Belt and braces for a directory that predates markers entirely: a local
    # delivery cannot be complete into a directory with nothing in it.
    _drop_completions_over_an_empty_dir(conn, output_dir)
    # Rows rebuilt from markers are checked against their files whatever the
    # scope situation: this is the one-time cost of a lost ledger, and the
    # moment a marker's word is worth verifying rather than taking. The flag,
    # not `added`, decides: a start-up killed between the rebuild's commit and
    # this check comes back with nothing to add and the flag still set.
    must_verify = ledger.verification_pending(conn)
    _drop_completions_without_their_files(
        conn, db_path, output_dir, present_keys, force=must_verify
    )
    if must_verify:
        ledger.verification_done(conn)
    done = ledger.completed_trip_keys(conn)
    conn.close()
    unverified = _unverified_completions(done, in_scope, s3_target, present_keys)
    pending = _filter_pending_trips(all_trips, done)
    if done:
        logger.info(
            "Resuming: %d/%d trip(s) already complete, %d to process (ledger read in %.1fs)",
            len(done & in_scope),
            total,
            len(pending),
            time.monotonic() - phase_start,
        )

    pass_num = 0
    while pending:
        label = "main pass" if pass_num == 0 else f"retry pass {pass_num}"
        logger.info("Starting %s over %d trip(s)", label, len(pending))
        _run_pass(
            pending,
            api_key=api_key,
            dataset_id=dataset_id,
            output_dir=output_dir,
            cache_dir=cache_dir,
            dataset_output=dataset_output,
            present_keys=present_keys,
            s3_target=s3_target,
            minute_selection=minute_selection,
            total_trips=total,
            worker_count=worker_count,
            db_path=db_path,
            failure_log=failure_log,
            locking_required=locking_required,
        )

        conn = connect(db_path)
        done = ledger.completed_trip_keys(conn)
        # Both hold-states are excluded from IN-RUN retries only: a restore takes
        # hours, and an archived verdict only changes when the dataset is edited
        # server-side — re-fetching seconds later helps neither. A future run of
        # the same command re-enqueues both.
        waiting = ledger.pending_restore_trip_keys(conn) | ledger.archived_trip_keys(conn)
        conn.close()
        pending = _filter_pending_trips(all_trips, done)
        retryable = _filter_retryable_trips(pending, waiting)
        if not retryable:
            break
        if pass_num >= settings.max_retry_passes:
            logger.warning(
                "%d trip(s) still incomplete after %d retry pass(es); see %s",
                len(retryable),
                settings.max_retry_passes,
                failure_log,
            )
            break
        pass_num += 1
        pending = retryable

    # Upload the saved request last (never before forking a worker that creates a
    # boto3 client — keeps the client per-worker and fork-safe).
    if s3_target is not None:
        bucket, prefix = s3_target
        if not upload_and_remove_file(
            output_dir / DATASET_REQUEST_FILENAME, output_dir, bucket, prefix
        ):
            logger.error(
                "%s not uploaded; left locally for retry", DATASET_REQUEST_FILENAME
            )
        prune_empty_region_dirs(output_dir)

    completed = len(done & in_scope)
    logger.info(
        "Finished dataset %s: %d/%d trip(s) complete", dataset_id, completed, total
    )

    if settings.create_manifest:
        if s3_target is not None:
            logger.warning(
                "CREATE_MANIFEST skipped: trips are removed locally after the S3 "
                "upload, so there is nothing to scan"
            )
        else:
            try:
                write_event_manifest(
                    output_dir,
                    name=dataset_request_model.settings.name,
                    dataset_id=dataset_id,
                )
            except Exception:
                logger.exception("Failed to write the safety-critical events manifest")

    # Files in S3 Deep Archive can't be downloaded until their restore completes;
    # tell the user when to re-run. Last thing on screen on purpose.
    conn = connect(db_path)
    restore_trips, restore_files, max_eta_ms = ledger.pending_restore_stats(
        conn, in_scope
    )
    archived_trips, archived_files = ledger.archived_stats(conn, in_scope)
    conn.close()
    if restore_trips:
        logger.warning(
            _pending_restore_message(
                restore_trips,
                restore_files,
                max_eta_ms,
                dataset_request_model.settings.restore_tier,
            )
        )
    if archived_trips:
        logger.warning(
            "%d trip(s) skipped: %d file(s) are archived and not available for "
            "download in this dataset. If you need these files, please contact "
            "the NATIX team to have them included, then re-run the same command.",
            archived_trips,
            archived_files,
        )

    # A run that did not produce every requested trip must not look successful to
    # whatever called it. Deep-Archive waits are the exception: a documented
    # "re-run later", not a failure, and already reported above.
    unfinished = total - completed - restore_trips - archived_trips
    if unfinished > 0:
        logger.error(
            "%d of %d requested trip(s) were not completed; see %s",
            unfinished,
            total,
            failure_log,
        )
        sys.exit(1)

    # Every requested trip is accounted for, but some of that is the resume
    # state's word alone: the listing that would have checked it failed at
    # start-up. Keeping those rows is what stops a transient throttle costing
    # the whole dataset a second download, and the next run that CAN list drops
    # the ones whose objects are really gone. What must not happen is this run
    # claiming success for trips it never looked at -- a caller that reads the
    # exit code would take unverified for delivered.
    if unverified:
        logger.error(
            "%d completed trip(s) were skipped on the resume state alone: %s could "
            "not be listed at start-up, so nothing confirmed their objects are "
            "still there. Re-run once the bucket can be listed.",
            unverified,
            settings.s3_upload,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
