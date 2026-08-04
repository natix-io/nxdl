#!/usr/bin/env python3

import json
import logging
import os
import re
import sys
from datetime import UTC, datetime
from multiprocessing import Process, Queue
from pathlib import Path
from queue import Full

from schemas import DatasetOutput
from schemas.dataset_request import DatasetRequest
from app.trip_downloader import get_dataset_request, get_ds_manifest, process_trip
from utils import ledger
from utils.download import sweep_partial_files
from utils.ledger import LedgerWriter, Reporter, connect
from utils.s3_uploader import parse_s3_uri, prune_empty_dirs, upload_and_remove_file
from utils.settings import settings
from utils.trip_list import (
    TripKey,
    format_trip_key,
    load_trips_from_file,
    trip_matches,
)

logger = logging.getLogger(__name__)

DATASET_REQUEST_FILENAME = "dataset_request.json"
# State + logs live OUTSIDE the dataset dir so it holds only dataset content
# (and so nothing here can ever be mistaken for an uploadable trip file).
LOGS_DIR = Path("logs")
STATE_DIR = Path("state")
REGISTRY_FILE = STATE_DIR / "output_dirs.json"

# Task queue depth. Deliberately bounded and fed while the workers run, never filled
# up front: `Queue()` only *looks* unbounded — it is capped at the platform's
# SEM_VALUE_MAX (2**31-1 on Linux, but **32767 on macOS**), so pre-loading every trip
# blocks forever on the 32768th put on macOS for any dataset bigger than that, before
# a single worker has been started. A small window also keeps memory flat.
TASK_QUEUE_MAXSIZE = 1000
# How long a blocked put waits before re-checking that workers are still alive.
FEED_POLL_SECONDS = 1.0


def _safe_name(value: str) -> str:
    """Filesystem-safe token from a dataset id."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", value) or "dataset"


def _ledger_path(dataset_id: str) -> Path:
    return STATE_DIR / f"{_safe_name(dataset_id)}.db"


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

    A line without a health_grade means "whichever grade(s) the dataset has for this
    trip"; a line with one selects that grade only. See ``utils.trip_list``.
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


def _setup_logging(stamp: str) -> Path:
    """Terminal shows progress + milestones (INFO); a per-run file gets full detail (DEBUG).

    The file handler and console handler are inherited across the multiprocessing
    fork, so worker output flows to the same places without per-worker setup.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"downloader_{stamp}.log"

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


def _guard_single_dataset(output_dir: Path, dataset_id: str) -> None:
    """Refuse to mix datasets in one output dir.

    ``dataset_request.json`` and the trip tree are one-dataset-per-directory. A
    registry outside the dataset dir (``state/output_dirs.json``) records which
    dataset owns each output dir; a later run with a different ``DATASET_ID``
    against the same dir is rejected instead of silently overwriting the request
    file (locally and at the S3 prefix root).
    """
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    key = str(output_dir)
    registry: dict[str, str] = {}
    if REGISTRY_FILE.exists():
        try:
            registry = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            registry = {}

    owner = registry.get(key)
    if owner is None:
        registry[key] = dataset_id
        REGISTRY_FILE.write_text(json.dumps(registry, indent=2), encoding="utf-8")
    elif owner != dataset_id:
        logger.error(
            "Output dir %s already holds dataset %s — refusing to download dataset "
            "%s into it. Set OUTPUT_DIR to a fresh directory (or clear this one) and "
            "re-run.",
            output_dir,
            owner,
            dataset_id,
        )
        sys.exit(1)


def _save_dataset_request(output_dir: Path, dataset_request) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    request_path = output_dir / DATASET_REQUEST_FILENAME
    payload = {
        "request_id": dataset_request.request_id,
        "request": dataset_request.request,
    }
    request_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    logger.info("Saved dataset request to %s", request_path)


def _trip_worker(
    task_queue: Queue,
    status_queue: Queue,
    api_key: str,
    dataset_id: str,
    output_dir: Path,
    output: DatasetOutput,
    present_keys: set[str] | None,
    s3_target: tuple[str, str] | None,
) -> None:
    reporter = Reporter(status_queue)
    while True:
        # Blocking get + a None sentinel per worker: `get_nowait` raced the parent's
        # feeder thread, so a worker saw Empty on a queue that still held thousands of
        # trips and exited — most workers died on startup and trips were skipped.
        task = task_queue.get()
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
            reporter=reporter,
            present_keys=present_keys,
            s3_target=s3_target,
            health_grade=health_grade,
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
    dataset_output: DatasetOutput,
    present_keys: set[str] | None,
    s3_target: tuple[str, str] | None,
    total_trips: int,
    worker_count: int,
    db_path: Path,
    failure_log: Path,
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
                dataset_output,
                present_keys,
                s3_target,
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
    log_file = _setup_logging(run_stamp)
    failure_log = LOGS_DIR / f"failed_trips_{run_stamp}.log"
    logger.info("Full logs: %s", log_file)

    output_dir: Path = settings.output_dir.resolve()
    if output_dir.exists() and not output_dir.is_dir():
        logger.error("Output path '%s' exists but is not a directory.", output_dir)
        sys.exit(1)

    dataset_id = settings.dataset_id.strip()
    api_key = settings.api_key.strip()
    if not api_key or not dataset_id:
        logger.error(
            "Missing API_KEY or DATASET_ID in the active .env file. "
            "Please update .env.%s before running the downloader.",
            os.getenv("APP_ENV", "development"),
        )
        sys.exit(1)

    worker_count = settings.max_parallel_trips or 1
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = _ledger_path(dataset_id)

    # One dataset per output dir — reject a second, different dataset here.
    _guard_single_dataset(output_dir, dataset_id)

    # Remove partial files from any killed run before starting.
    sweep_partial_files(output_dir)
    sweep_partial_files(settings.cache_dir)

    try:
        ds_manifest = get_ds_manifest(api_key, dataset_id)
        logger.info(
            "Fetched ds_manifest for dataset %s — %d trip(s)",
            dataset_id,
            len(ds_manifest.trips),
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

    dataset_output: DatasetOutput = DatasetRequest.model_validate(
        dataset_request.request
    ).output

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

    all_trips = [
        (trip.trip_id, trip.sub_trip, trip.health_grade) for trip in ds_manifest.trips
    ]

    # Optional subset: process only the trips listed in TRIPS_FILE.
    if settings.trips_file.strip():
        trips_path = Path(settings.trips_file).expanduser()
        try:
            requested = load_trips_from_file(trips_path)
        except FileNotFoundError:
            logger.error("TRIPS_FILE not found: %s", trips_path)
            sys.exit(1)
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
            "Trip filter active (%s): processing %d of %d dataset trip(s)",
            trips_path,
            len(all_trips),
            dataset_trip_count,
        )

    total = len(all_trips)
    # Only trips in scope for this run count towards the tally — the ledger may hold
    # complete rows for trips a TRIPS_FILE subset excluded.
    in_scope = set(all_trips)

    # Resume: skip trips already marked complete in the ledger.
    conn = connect(db_path)
    done = ledger.completed_trip_keys(conn)
    conn.close()
    pending = _filter_pending_trips(all_trips, done)
    if done:
        logger.info(
            "Resuming: %d/%d trip(s) already complete, %d to process",
            len(done & in_scope),
            total,
            len(pending),
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
            dataset_output=dataset_output,
            present_keys=present_keys,
            s3_target=s3_target,
            total_trips=total,
            worker_count=worker_count,
            db_path=db_path,
            failure_log=failure_log,
        )

        conn = connect(db_path)
        done = ledger.completed_trip_keys(conn)
        conn.close()
        pending = _filter_pending_trips(all_trips, done)
        if not pending:
            break
        if pass_num >= settings.max_retry_passes:
            logger.warning(
                "%d trip(s) still incomplete after %d retry pass(es); see %s",
                len(pending),
                settings.max_retry_passes,
                failure_log,
            )
            break
        pass_num += 1

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
        prune_empty_dirs(output_dir, remove_root=False)

    completed = len(done & in_scope)
    logger.info(
        "Finished dataset %s: %d/%d trip(s) complete", dataset_id, completed, total
    )


if __name__ == "__main__":
    main()
