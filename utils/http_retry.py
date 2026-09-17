import logging
import time

import httpx

from utils.http_client import client

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})


def get_with_retry(
    url: str,
    *,
    headers: dict[str, str],
    timeout: float,
    max_retries: int = 4,
    base_delay_seconds: float = 1.0,
) -> httpx.Response:
    """GET with exponential backoff on transient HTTP and network errors."""
    last_exc: BaseException | None = None

    for attempt in range(max_retries + 1):
        try:
            response = client().get(url, headers=headers, timeout=timeout)
            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt < max_retries
            ):
                delay = base_delay_seconds * (2**attempt)
                logger.warning(
                    "GET %s returned %s (attempt %d/%d), retrying in %.1fs",
                    url,
                    response.status_code,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError:
            raise
        except httpx.RequestError as exc:
            last_exc = exc
            if attempt >= max_retries:
                raise
            delay = base_delay_seconds * (2**attempt)
            logger.warning(
                "GET %s failed with %s (attempt %d/%d), retrying in %.1fs",
                url,
                exc,
                attempt + 1,
                max_retries + 1,
                delay,
            )
            time.sleep(delay)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("get_with_retry exhausted without response")
