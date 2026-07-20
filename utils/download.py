"""Resilient single-file download.

``download_file_with_retry`` streams one presigned URL to disk with the same
exponential-backoff policy that ``utils.http_retry`` applies to API GETs, plus
two properties the raw ``httpx.stream`` in ``trip_downloader`` lacked:

* **Atomic write** — the body streams into ``<dest>.part`` and is renamed onto
  ``dest`` only after it is fully written, so a killed process never leaves a
  truncated file that later looks complete.
* **Error signalling** — any non-retryable error status (``>= 400``) raises
  ``DownloadHTTPError`` carrying the status code plus the real S3/R2
  ``<Code>``/``<Message>`` (or a bounded raw snippet for non-XML bodies). No status
  is assumed to mean anything — a 403 may be ``AccessDenied`` (missing object /
  missing permission), a 404 ``NoSuchKey``, etc. — the surfaced message makes the
  actual error visible. The caller can refetch the trip manifest for fresh URLs
  instead of burning retries on a dead link.
"""

import logging
import os
import re
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})
PART_SUFFIX = ".part"
_MAX_ERROR_BODY_CHARS = 500

_S3_CODE_RE = re.compile(r"<Code>([^<]+)</Code>")
_S3_MESSAGE_RE = re.compile(r"<Message>([^<]+)</Message>")


class DownloadHTTPError(Exception):
    """A download returned a non-retryable error status. ``status_code`` is the HTTP
    status; the message carries the real S3/R2 error body (code + message, or a
    bounded raw snippet). Not assumed to be expiry — could be AccessDenied,
    NoSuchKey, etc."""

    def __init__(self, status_code: int, detail: str, dest_name: str) -> None:
        self.status_code = status_code
        super().__init__(f"{status_code} {detail} downloading {dest_name}")


def _describe_http_error(response: httpx.Response) -> str:
    """Extract a human-readable detail from any error response body.

    Reads a bounded prefix of the body (never buffering a huge WAF/HTML page),
    prefers the structured S3/R2 ``<Code>``/``<Message>``, and falls back to a
    whitespace-collapsed raw snippet so ANY error format is visible.
    """
    raw = b""
    try:
        for chunk in response.iter_bytes():
            raw += chunk
            if len(raw) >= _MAX_ERROR_BODY_CHARS:
                break
    except Exception:  # pragma: no cover - body read is best-effort
        pass
    body = " ".join(raw.decode("utf-8", errors="replace").split())[
        :_MAX_ERROR_BODY_CHARS
    ]
    code_match = _S3_CODE_RE.search(body)
    message_match = _S3_MESSAGE_RE.search(body)
    code = code_match.group(1) if code_match else None
    message = message_match.group(1) if message_match else None
    structured = ": ".join(part for part in (code, message) if part)
    return structured or body or "error"


def _cleanup_part(part: Path) -> None:
    try:
        part.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        logger.warning("Could not remove partial file %s", part)


def download_file_with_retry(
    url: str,
    dest: Path,
    *,
    timeout: float,
    chunk_size: int,
    max_retries: int = 4,
    base_delay_seconds: float = 1.0,
) -> None:
    """Stream ``url`` to ``dest`` atomically, retrying transient failures.

    Retries on network errors (``httpx.RequestError``) and the retryable status
    codes ``{429, 502, 503, 504}`` with delay ``base_delay_seconds * 2**attempt``.
    Any other error status (``>= 400``), or a retryable one whose retries are
    exhausted, raises ``DownloadHTTPError`` carrying the status code + the real
    S3/R2 error body. Any leftover ``.part`` file is removed on every failure so a
    later attempt starts clean.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + PART_SUFFIX)
    last_exc: BaseException | None = None

    for attempt in range(max_retries + 1):
        try:
            with httpx.stream(
                "GET", url, timeout=timeout, follow_redirects=True
            ) as response:
                if (
                    response.status_code in RETRYABLE_STATUS_CODES
                    and attempt < max_retries
                ):
                    delay = base_delay_seconds * (2**attempt)
                    logger.warning(
                        "GET %s returned %s (attempt %d/%d), retrying in %.1fs",
                        dest.name,
                        response.status_code,
                        attempt + 1,
                        max_retries + 1,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                if response.status_code >= 400:
                    raise DownloadHTTPError(
                        response.status_code,
                        _describe_http_error(response),
                        dest.name,
                    )
                with part.open("wb") as handle:
                    for chunk in response.iter_bytes(chunk_size=chunk_size):
                        handle.write(chunk)
            os.replace(part, dest)
            return
        except DownloadHTTPError:
            _cleanup_part(part)
            raise
        except httpx.HTTPStatusError:
            _cleanup_part(part)
            raise
        except httpx.RequestError as exc:
            last_exc = exc
            _cleanup_part(part)
            if attempt >= max_retries:
                raise
            delay = base_delay_seconds * (2**attempt)
            logger.warning(
                "GET %s failed with %s (attempt %d/%d), retrying in %.1fs",
                dest.name,
                exc,
                attempt + 1,
                max_retries + 1,
                delay,
            )
            time.sleep(delay)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("download_file_with_retry exhausted without success")


def sweep_partial_files(root: Path) -> int:
    """Delete stray ``*.part`` files left by a killed run. Returns the count removed."""
    if not root.exists():
        return 0
    removed = 0
    for part in root.rglob(f"*{PART_SUFFIX}"):
        if part.is_file():
            _cleanup_part(part)
            removed += 1
    if removed:
        logger.info("Removed %d stray partial file(s) under %s", removed, root)
    return removed
