"""Tell the downloader service which trips this client has finished.

The service tags a trip's anonymized videos once the client reports it delivered.
Reported trips are remembered in the ledger (``reported_trips``), so a trip is sent
until a 2xx and then never again. Never allowed to fail or slow a run: every error
is swallowed and the unreported trips simply wait for the next flush.
"""

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path

from app.trip_downloader import api_headers
from utils import ledger
from utils.http_client import client
from utils.path_helpers import TripKey
from utils.settings import settings

logger = logging.getLogger(__name__)

FLUSH_INTERVAL_S = 60.0
BATCH = 100  # the service takes 1..100 trips per request
REQUEST_TIMEOUT_S = 10.0
# What the end-of-run flushes may spend: a big resumed backlog must not hold the run.
FINAL_FLUSH_BUDGET_S = 30.0


# Refusals that name the batch, not the client: that batch is skipped for the rest
# of the flush and the next one is tried.
_BATCH_REFUSED = (400, 422)
_AUTH_REFUSED = (401, 403)
# The service's bounds on a trip's `minutes`: count and each minute_id. minute_ids are
# 64-bit ids (negative or huge), not minute offsets; the SQLite ledger cannot hold a
# value outside that range, so the count is the bound that can actually trip.
MAX_MINUTES = 1440
MIN_MINUTE_ID = -(2**63)
MAX_MINUTE_ID = 2**63 - 1


def _breaks_contract(minutes: list[int] | None) -> bool:
    return minutes is not None and (
        len(minutes) > MAX_MINUTES
        or any(not MIN_MINUTE_ID <= m <= MAX_MINUTE_ID for m in minutes)
    )


def _post(url: str, body: dict, headers: dict[str, str]) -> int | None:
    """One request bounded by ``REQUEST_TIMEOUT_S`` of wall clock, whatever the server
    does; the HTTP status, or None for an error or an abandoned request."""
    result: list[int] = []

    def send() -> None:
        try:
            response = client().post(
                url, json=body, headers=headers, timeout=REQUEST_TIMEOUT_S
            )
            result.append(response.status_code)
        except Exception:
            logger.debug("Delivered report not sent", exc_info=True)

    sender = threading.Thread(target=send, name="delivered-report-post", daemon=True)
    sender.start()
    sender.join(REQUEST_TIMEOUT_S)
    return result[0] if result else None


def flush(
    db_path: Path,
    api_key: str,
    dataset_id: str,
    deadline_s: float | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> int:
    """Send every unreported finished trip; returns how many were marked reported.

    Each trip carries the scope it was delivered with: the run's camera selection,
    and the minutes of a narrowed (chopped or minute-selected) trip — null for a
    whole trip. A trip whose minutes break the service's bounds is never sent
    (one WARNING per flush). A batch is marked only on a 2xx. A batch refused as
    invalid (400 / 422) is skipped for the rest of this flush and the next one is
    sent; any other
    refusal (a 404 from an old service included), error or abandoned request stops
    the flush and keeps the rest. Never raises. A dry run delivers nothing, so it
    reports nothing. After ``deadline_s`` seconds no new batch is started; the rest
    waits for the next run. Nor once ``should_stop()`` is true.
    """
    if settings.dry_mode:
        return 0
    marked = 0
    skipped: set[TripKey] = set()
    warned_invalid = warned_refused = False
    end = None if deadline_s is None else time.monotonic() + deadline_s
    camera_selection = str(settings.camera_selection)
    try:
        conn = ledger.connect(db_path)
        try:
            while (end is None or time.monotonic() < end) and not (
                should_stop and should_stop()
            ):
                keys = ledger.unreported_delivered_keys(conn, BATCH, exclude=skipped)
                if not keys:
                    break
                minutes = ledger.delivered_minutes(conn, keys)
                # Left unreported, never sent: one such trip would get its whole
                # batch refused.
                invalid = [k for k in keys if _breaks_contract(minutes.get(k))]
                if invalid:
                    if not warned_invalid:
                        logger.warning(
                            "Delivered report: %s has minutes the service cannot "
                            "take; not reported",
                            invalid[0].dir_name,
                        )
                        warned_invalid = True
                    skipped.update(invalid)
                    keys = [k for k in keys if k not in skipped]
                    if not keys:
                        continue
                status = _post(
                    f"{settings.base_url}/v1/run/delivered",
                    {
                        "dataset_id": dataset_id,
                        "trips": [
                            {
                                "trip_id": k.trip_id,
                                "sub_trip": k.sub_trip,
                                "health_grade": k.health_grade,
                                "camera_selection": camera_selection,
                                "minutes": minutes.get(k),
                            }
                            for k in keys
                        ],
                    },
                    api_headers(api_key),
                )
                if status in _BATCH_REFUSED:
                    if not warned_refused:
                        logger.warning(
                            "Delivered report: a batch of %d trip(s) was refused "
                            "(HTTP %s); skipped until the next report",
                            len(keys),
                            status,
                        )
                    warned_refused = True
                    skipped.update(keys)
                    continue
                if status in _AUTH_REFUSED:
                    logger.warning("Delivered report refused: HTTP %s", status)
                    break
                if status is None or not 200 <= status < 300:
                    logger.debug("Delivered report refused: HTTP %s", status)
                    break
                ledger.mark_reported(conn, keys)
                marked += len(keys)
        finally:
            conn.close()
    except Exception:
        logger.debug("Delivered report not sent", exc_info=True)
    return marked


class DeliveredReporter(threading.Thread):
    """Flushes every ``interval`` seconds until :meth:`stop`, then once more.

    Uses its own ledger connection per flush, never the ``LedgerWriter``'s.
    """

    def __init__(
        self,
        db_path: Path,
        api_key: str,
        dataset_id: str,
        interval: float = FLUSH_INTERVAL_S,
    ) -> None:
        super().__init__(name="delivered-report", daemon=True)
        self._args = (db_path, api_key, dataset_id)
        self._interval = interval
        self._stopped = threading.Event()

    def run(self) -> None:
        while not self._stopped.wait(self._interval):
            # Unbounded, but cut between batches by stop(): only the final flush below
            # holds join(), for at most its budget.
            flush(*self._args, should_stop=self._stopped.is_set)
        flush(*self._args, deadline_s=FINAL_FLUSH_BUDGET_S)

    def stop(self) -> None:
        """Ask the thread to finish: it makes one last, time-boxed flush; ``join`` waits."""
        self._stopped.set()
