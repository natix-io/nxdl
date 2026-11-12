"""End-of-run report to the downloader service.

The service knows which links it signed, never what a client fetched. Once a run
reaches its end the client says what it did: trips complete / failed / held, files
and bytes downloaded, failures by stage. Always sent; never allowed to fail or slow
a run. A run that is killed sends nothing.
"""

import logging
import os
import re
import sqlite3
import threading
import time
from collections.abc import Collection
from pathlib import Path

from app.trip_downloader import api_headers
from utils import ledger
from utils.http_client import client
from utils.ledger import (
    TRIP_ARCHIVED,
    TRIP_FAILED,
    TRIP_PARTIAL,
    TRIP_PENDING_RESTORE,
    StatsTotals,
)
from utils.path_helpers import TripKey
from utils.settings import settings

logger = logging.getLogger(__name__)

# Wall clock for the whole request, not httpx's per-read timeout: a server that
# trickles a response would otherwise hold the end of the run for as long as it likes.
REPORT_TIMEOUT_SECONDS = 10.0
# The service refuses more stages or longer names than these.
_MAX_STAGES = 20
_MAX_STAGE_CHARS = 32
_FAILED = (TRIP_FAILED, TRIP_PARTIAL)
_HELD = (TRIP_PENDING_RESTORE, TRIP_ARCHIVED)
_UNKNOWN = "unknown"


def delivery_id(conn: sqlite3.Connection, id_file: Path) -> str:
    """The delivery's id: one output dir of one dataset, the same on every re-run.

    Kept beside the data (``id_file``, under the output dir's ``.natix``) because
    the ledger is replaceable: a fresh client unpack resumes from the completion
    markers with a new ledger, and is still the same delivery. Where the file
    cannot be read or written the ledger's own id is used.
    """
    try:
        known = id_file.read_text().strip()
        if known:
            return known
    except OSError:
        pass
    value = ledger.delivery_id(conn)
    try:
        id_file.parent.mkdir(parents=True, exist_ok=True)
        part = id_file.with_name(f"{id_file.name}.{os.getpid()}.part")
        part.write_text(value)
        try:
            os.link(part, id_file)  # the first writer wins; a second run adopts its id
        except FileExistsError:
            value = id_file.read_text().strip() or value
        finally:
            part.unlink(missing_ok=True)
    except OSError:
        logger.debug("Delivery id not saved to %s", id_file, exc_info=True)
    return value


def build_report(
    conn: sqlite3.Connection,
    *,
    dataset_id: str,
    in_scope: Collection[TripKey],
    total: int,
    completed: int,
    totals: StatsTotals,
    duration_s: float,
    to_s3: bool,
    id_file: Path,
    unverified: int = 0,
) -> dict:
    """The report body. Attribute names are the service's JSON keys.

    ``trips_*`` and the plain ``files``/``bytes`` describe the delivery as it stands:
    each trip's files once, and a re-run with nothing to do repeats them. ``run_*``
    are what this run alone transferred, a file fetched twice counted twice.
    """
    held = 0
    failures: dict[str, int] = {}
    rows = conn.execute(
        "SELECT trip_id, sub_trip, health_grade, status, error_stage FROM trips "
        "WHERE segment = '' AND status IN (?, ?, ?, ?)",
        _FAILED + _HELD,
    )
    for trip_id, sub_trip, health_grade, status, stage in rows:
        if TripKey(trip_id, int(sub_trip), int(health_grade)) not in in_scope:
            continue
        if status in _HELD:
            held += 1
            continue
        # The service takes plain tokens only; one odd name must not cost the report.
        stage = re.sub(r"[^A-Za-z0-9_.-]", "_", stage or _UNKNOWN)[:_MAX_STAGE_CHARS]
        if stage not in failures and len(failures) >= _MAX_STAGES - 2:
            stage = "other"
        failures[stage] = failures.get(stage, 0) + 1
    # Everything in scope that is neither complete nor held has failed, whatever its
    # row says: a trip whose worker died is still `in_progress`, or has no row at all.
    failed = max(total - completed - held, 0)
    unexplained = failed - sum(failures.values())
    if unexplained > 0:
        failures[_UNKNOWN] = failures.get(_UNKNOWN, 0) + unexplained

    files, size, sized = ledger.downloaded_totals(conn, in_scope)
    return {
        "dataset_id": dataset_id,
        "delivery_id": delivery_id(conn, id_file),
        # `finished`: everything in scope is complete or held for a later run, and
        # nothing was taken as complete on the resume state alone (`unverified`:
        # the run exits 1 for those, so it must not report a clean finish).
        "outcome": "incomplete" if failed or unverified else "finished",
        "delivery": "s3" if to_s3 else "local",
        "trips_total": total,
        "trips_complete": completed,
        "trips_failed": failed,
        "trips_held": held,
        "files_downloaded": files,
        "bytes_downloaded": size,
        # How many trips those two cover: fewer than `trips_complete` for a delivery
        # begun before sizes were recorded, or resumed with a new ledger.
        "trips_sized": sized,
        "run_files_downloaded": totals.sums.get("files_downloaded", 0),
        "run_bytes_downloaded": totals.sums.get("bytes_downloaded", 0),
        "failures": failures,
        "duration_s": int(duration_s),
    }


def _post(api_key: str, report: dict) -> None:
    try:
        response = client().post(
            f"{settings.base_url}/v1/run/report",
            json=report,
            headers=api_headers(api_key),
            timeout=REPORT_TIMEOUT_SECONDS,
        )
        # A service without the route (404) is not an error.
        if response.status_code >= 400 and response.status_code != 404:
            logger.debug("Run report refused: HTTP %s", response.status_code)
    except Exception:
        logger.debug("Run report not sent", exc_info=True)


def send_run_report(api_key: str, report: dict) -> None:
    """One attempt, abandoned after REPORT_TIMEOUT_SECONDS whatever the server does."""
    sender = threading.Thread(
        target=_post, args=(api_key, report), name="run-report", daemon=True
    )
    sender.start()
    sender.join(REPORT_TIMEOUT_SECONDS)


def report_run(
    db_path: Path,
    api_key: str,
    *,
    dataset_id: str,
    in_scope: Collection[TripKey],
    total: int,
    completed: int,
    totals: StatsTotals,
    started: float,
    to_s3: bool,
    id_file: Path,
    unverified: int = 0,
) -> None:
    """Build and send the report. Never raises: the run's outcome does not depend on it."""
    try:
        conn = ledger.connect(db_path)
        try:
            report = build_report(
                conn,
                dataset_id=dataset_id,
                in_scope=in_scope,
                total=total,
                completed=completed,
                totals=totals,
                duration_s=time.monotonic() - started,
                to_s3=to_s3,
                id_file=id_file,
                unverified=unverified,
            )
        finally:
            conn.close()
        send_run_report(api_key, report)
    except Exception:
        logger.debug("Run report not sent", exc_info=True)
