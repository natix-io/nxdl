import logging
import os
import shutil
from pathlib import Path
import httpx

from schemas import (
    DSManifest,
    DatasetOutput,
    DatasetRequestResponse,
    TripManifest,
)
from utils.path_helpers import (
    build_cache_trip_path,
    build_camera_file_path,
    build_feature_file_path,
    build_features_directory_path,
    build_camera_video_path,
    build_fused_sei_path,
    build_minute_directory_path,
    build_trip_directory_path,
    build_camera_directory_path,
    get_r2_url_from_presigned_url,
    is_camera_allowed,
)
from app.ds_formatter import format_ds
from app.validation import validate_trip
from utils.download import download_file_with_retry
from utils.http_retry import get_with_retry
from utils.ledger import (
    TRIP_COMPLETE,
    TRIP_FAILED,
    TRIP_IN_PROGRESS,
    TRIP_PARTIAL,
    Reporter,
    present_keys_cover,
)
from utils.s3_uploader import upload_trip_atomic
from utils.settings import settings

logger = logging.getLogger(__name__)

AUTH_HEADER_NAME = "ApiKey"


def _api_get(url: str, api_key: str) -> httpx.Response:
    return get_with_retry(
        url,
        headers={AUTH_HEADER_NAME: api_key},
        timeout=settings.api_timeout,
        max_retries=settings.api_max_retries,
        base_delay_seconds=settings.api_retry_base_delay,
    )


def get_ds_manifest(api_key: str, dataset_id: str) -> DSManifest:
    """Fetch dataset manifest. Raises httpx.HTTPStatusError or httpx.RequestError on failure."""
    url = f"{settings.base_url}/v1/dataset/manifest?dataset_id={dataset_id}"
    response = _api_get(url, api_key)
    return DSManifest.model_validate(response.json())


def get_dataset_request(api_key: str, dataset_id: str) -> DatasetRequestResponse:
    """Fetch stored dataset request criteria. Raises httpx.HTTPStatusError or httpx.RequestError on failure."""
    url = f"{settings.base_url}/v1/dataset/request?dataset_id={dataset_id}"
    response = _api_get(url, api_key)
    return DatasetRequestResponse.model_validate(response.json())


def get_trip_manifest(
    api_key: str,
    dataset_id: str,
    trip_id: str,
    sub_trip: int,
    health_grade: int = 500,
) -> TripManifest:
    """Fetch and parse the trip manifest response."""
    url = (
        f"{settings.base_url}/v1/trip/manifest"
        f"?dataset_id={dataset_id}&trip_id={trip_id}&sub_trip={sub_trip}"
        f"&health_grade={health_grade}"
    )
    response = _api_get(url, api_key)
    return TripManifest.model_validate(response.json())


def _get_trip_location(manifest: TripManifest) -> tuple[str | None, str | None]:
    return manifest.country, manifest.state


def _get_trip_timezone(manifest: TripManifest) -> str | None:
    return manifest.timezone


class _DownloadTask:
    """One file to fetch: either a streamed download or a dry-mode link file."""

    __slots__ = ("kind", "url", "dest", "is_link")

    def __init__(
        self, kind: str, url: str, dest: Path, *, is_link: bool = False
    ) -> None:
        self.kind = kind
        self.url = url
        self.dest = dest
        self.is_link = is_link


def _iter_trip_tasks(
    trip_dir: Path, manifest: TripManifest, output: DatasetOutput
) -> list[_DownloadTask]:
    """Build the list of files to fetch for a trip, mirroring the manifest structure."""
    timezone = _get_trip_timezone(manifest)
    tasks: list[_DownloadTask] = []

    for minute in manifest.minutes:
        minute_dir = build_minute_directory_path(
            trip_dir, minute.minute_epoch, timezone
        )
        if (
            output.footage.fused_sei is not None
            and getattr(minute, "fused_sei", None) is not None
        ):
            fused_sei_path = build_fused_sei_path(
                minute_dir, minute.minute_epoch, timezone
            )
            tasks.append(
                _DownloadTask(
                    "fused_sei", minute.fused_sei.presigned_url, fused_sei_path
                )
            )
        for camera in minute.cameras:
            if not is_camera_allowed(camera.camera_type):
                continue
            camera_dir = build_camera_directory_path(
                minute_dir=minute_dir, camera_type=camera.camera_type
            )
            camera_path = build_camera_video_path(
                camera_dir, camera.camera_type, minute.minute_epoch, timezone
            )
            if settings.dry_mode:
                link_path = camera_path.parent / (camera_path.name + ".link")
                tasks.append(
                    _DownloadTask(
                        "link", camera.camera.presigned_url, link_path, is_link=True
                    )
                )
            else:
                tasks.append(
                    _DownloadTask("video", camera.camera.presigned_url, camera_path)
                )

            if (
                manifest.health_grade != 0
                and camera.telemetry is not None
                and output.footage.needs_telemetry()
            ):
                telemetry_path = build_camera_file_path(
                    camera_dir, camera.telemetry.presigned_url
                )
                tasks.append(
                    _DownloadTask(
                        "telemetry", camera.telemetry.presigned_url, telemetry_path
                    )
                )

            if output.footage.sei is not None and camera.sei is not None:
                sei_path = camera_path.with_suffix(".sei")
                sei_path = sei_path.with_name(sei_path.name + ".csv")
                tasks.append(_DownloadTask("sei", camera.sei.presigned_url, sei_path))

    for feature_name in (
        "fixed_metadata",
        "base_insights",
        "safety_critical",
        "ego_actions",
    ):
        feature = getattr(manifest, feature_name, None)
        if feature is None:
            continue
        feature_path = build_feature_file_path(trip_dir, feature_name)
        tasks.append(_DownloadTask("feature", feature.presigned_url, feature_path))

    return tasks


def _write_link_file(presigned_url: str, link_path: Path) -> None:
    """Atomically write a .link file containing the raw R2 URL."""
    link_path.parent.mkdir(parents=True, exist_ok=True)
    r2_url = get_r2_url_from_presigned_url(presigned_url)
    part = link_path.with_name(link_path.name + ".part")
    part.write_text(r2_url)
    os.replace(part, link_path)


def _run_task(task: _DownloadTask) -> None:
    # Skip a file already fully present locally (atomic rename means any existing
    # final file is complete) — makes a partial-trip retry re-fetch only what is
    # missing instead of re-downloading the whole trip.
    if task.dest.exists() and task.dest.stat().st_size > 0:
        return
    if task.is_link:
        _write_link_file(task.url, task.dest)
        return
    download_file_with_retry(
        task.url,
        task.dest,
        timeout=settings.download_timeout,
        chunk_size=settings.download_chunk_size,
        max_retries=settings.download_max_retries,
        base_delay_seconds=settings.download_retry_base_delay,
    )


# Cap how many per-item error strings we inline into a trip's summary message; the
# full per-file detail always lives in the ledger's `files` table regardless.
_MAX_ERROR_DETAIL_ITEMS = 5


def _download_trip_tasks(
    tasks: list[_DownloadTask],
) -> list[tuple[_DownloadTask, str]]:
    """Run every task with per-file isolation. Returns the list of failures.

    A single file failure never aborts the trip — it is captured with its error
    message (task + ``"ExcType: msg"``) so the trip is marked PARTIAL, the per-file
    detail is persisted to the ledger, and the file is re-attempted on a later pass.
    """
    failures: list[tuple[_DownloadTask, str]] = []
    for task in tasks:
        try:
            _run_task(task)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            failures.append((task, message))
            # Summary reaches the ledger via the caller; log the per-file reason
            # here (WARNING so it surfaces without DEBUG), traceback at DEBUG.
            logger.warning("Failed to fetch %s: %s", task.dest.name, message)
            logger.debug("Traceback for %s", task.dest.name, exc_info=True)
    return failures


def _record_download_failures(
    reporter: Reporter,
    trip_id: str,
    sub_trip: int,
    health_grade: int,
    cache_trip_dir: Path,
    failures: list[tuple[_DownloadTask, str]],
) -> None:
    """Persist per-file failure rows + a detailed trip PARTIAL message.

    Each failed file becomes a ``files`` row (status ``failed``, real error
    message) so a post-mortem can see exactly which file failed and why; the trip
    message inlines the first few for a human-readable summary.
    """
    details: list[str] = []
    for task, message in failures:
        try:
            rel = task.dest.relative_to(cache_trip_dir).as_posix()
        except ValueError:
            rel = task.dest.name
        reporter.file(
            trip_id,
            sub_trip,
            health_grade,
            rel,
            "failed",
            kind=task.kind,
            error_stage="download",
            error_message=message,
        )
        details.append(f"{rel}: {message}")

    summary = "; ".join(details[:_MAX_ERROR_DETAIL_ITEMS])
    if len(details) > _MAX_ERROR_DETAIL_ITEMS:
        summary += f" (+{len(details) - _MAX_ERROR_DETAIL_ITEMS} more)"
    reporter.trip(
        trip_id,
        sub_trip,
        health_grade,
        TRIP_PARTIAL,
        error_stage="download",
        error_message=f"{len(failures)} file(s) failed to download: {summary}",
    )
    logger.error(
        "Trip %s_%d: %d file(s) failed to download; kept cache for retry: %s",
        trip_id,
        sub_trip,
        len(failures),
        summary,
    )


def _expected_s3_keys(
    prefix: str, final_rel: str, expected_rel_paths: list[Path]
) -> list[str]:
    base = f"{prefix}/{final_rel}" if prefix else final_rel
    return [f"{base}/{rel.as_posix()}" for rel in expected_rel_paths]


def process_trip(
    api_key: str,
    dataset_id: str,
    trip_id: str,
    sub_trip: int,
    output_dir: Path,
    output: DatasetOutput,
    *,
    reporter: Reporter,
    present_keys: set[str] | None = None,
    s3_target: tuple[str, str] | None = None,
    health_grade: int = 500,
) -> None:
    """Download, format, move and (optionally) upload one trip, tracking status.

    Never raises: every failure is recorded via ``reporter`` and the trip is left
    in a resumable state (PARTIAL/FAILED). The local cache dir is kept on failure
    so a later pass re-downloads only what is missing.
    """
    # build_expected_trip_files lives in manifest_coverage, which imports this
    # module — import lazily to avoid a circular import at load time.
    from app.manifest_coverage import build_expected_trip_files  # noqa: PLC0415

    try:
        trip_manifest = get_trip_manifest(
            api_key, dataset_id, trip_id, sub_trip, health_grade
        )
    except Exception as exc:
        logger.exception(
            "Failed to fetch trip manifest for trip %s_%d", trip_id, sub_trip
        )
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="manifest_fetch",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return

    country, state = _get_trip_location(trip_manifest)
    expected_rel = build_expected_trip_files(
        trip_manifest, output, dry_mode=settings.dry_mode
    )
    reporter.trip(
        trip_id,
        sub_trip,
        health_grade,
        TRIP_IN_PROGRESS,
        country=country,
        state=state,
        timezone=trip_manifest.timezone,
        file_total=len(expected_rel),
        manifest_generated_at=str(trip_manifest.manifest_generated_at),
    )

    final_dir = build_trip_directory_path(
        output_dir, country, state, trip_id, sub_trip, health_grade
    )
    final_rel = final_dir.relative_to(output_dir).as_posix()

    # Fast resume: if the trip is already fully on S3, mark complete without
    # downloading anything (recovers a lost ledger, and skips done trips).
    if s3_target is not None and present_keys is not None:
        _bucket, prefix = s3_target
        expected_keys = _expected_s3_keys(prefix, final_rel, expected_rel)
        if expected_keys and present_keys_cover(present_keys, expected_keys):
            for rel in expected_rel:
                reporter.file(
                    trip_id, sub_trip, health_grade, rel.as_posix(), "uploaded"
                )
            reporter.trip(trip_id, sub_trip, health_grade, TRIP_COMPLETE)
            logger.debug(
                "Trip %s_%d already complete on S3; skipped", trip_id, sub_trip
            )
            return

    cache_trip_dir = build_cache_trip_path(
        settings.cache_dir, trip_id, sub_trip, health_grade
    )
    try:
        tasks = _iter_trip_tasks(cache_trip_dir, trip_manifest, output)
        failures = _download_trip_tasks(tasks)
        if failures:
            # Retry once with a fresh manifest in case presigned URLs expired.
            try:
                trip_manifest = get_trip_manifest(
                    api_key, dataset_id, trip_id, sub_trip, health_grade
                )
                tasks = _iter_trip_tasks(cache_trip_dir, trip_manifest, output)
                failures = _download_trip_tasks(tasks)
            except Exception:
                logger.exception(
                    "Manifest refresh failed for trip %s_%d", trip_id, sub_trip
                )

        if failures:
            _record_download_failures(
                reporter, trip_id, sub_trip, health_grade, cache_trip_dir, failures
            )
            return

        format_ds(trip_manifest, cache_trip_dir, output)
        shutil.rmtree(build_features_directory_path(cache_trip_dir), ignore_errors=True)
    except Exception as exc:
        logger.exception("Failed to process trip %s_%d", trip_id, sub_trip)
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="process",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return

    # Validate the assembled trip in the cache BEFORE moving it into the dataset dir:
    # the whitelist stops extra files, but only validation catches missing files or a
    # trip that does not satisfy the request (SEI policy, camera folders, metadata...).
    # Only a trip that passes is moved; a failing trip keeps its cache dir so a later
    # retry pass re-fetches only what is missing (partial resume) and the dataset dir
    # never holds an invalid trip. Country/state layout checks are skipped here (the
    # cache dir is flat) and covered by the full dataset QA (validate_dataset) instead.
    try:
        trip_report = validate_trip(output_dir, cache_trip_dir, check_layout=False)
    except Exception as exc:
        logger.exception("Validation errored for trip %s_%d", trip_id, sub_trip)
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="validation",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return
    if trip_report.failures:
        messages = [
            getattr(issue, "message", str(issue)) for issue in trip_report.failures
        ]
        summary = "; ".join(messages[:_MAX_ERROR_DETAIL_ITEMS])
        if len(messages) > _MAX_ERROR_DETAIL_ITEMS:
            summary += f" (+{len(messages) - _MAX_ERROR_DETAIL_ITEMS} more)"
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="validation",
            error_message=f"{len(messages)} validation failure(s): {summary}",
        )
        logger.error(
            "Trip %s_%d failed validation (%d failure(s)); kept cache for retry, not moved: %s",
            trip_id,
            sub_trip,
            len(messages),
            summary,
        )
        return

    # Passed validation — move the trip from the cache into the dataset directory.
    try:
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            shutil.rmtree(final_dir, ignore_errors=True)
        shutil.move(str(cache_trip_dir), str(final_dir))
        logger.debug("Moved trip %s_%d to %s", trip_id, sub_trip, final_dir)
    except Exception as exc:
        logger.exception(
            "Failed to move trip %s_%d into the dataset dir", trip_id, sub_trip
        )
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="process",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return

    if s3_target is None:
        for rel in expected_rel:
            reporter.file(trip_id, sub_trip, health_grade, rel.as_posix(), "downloaded")
        reporter.trip(trip_id, sub_trip, health_grade, TRIP_COMPLETE)
        return

    _upload_and_finalize(
        final_dir,
        output_dir,
        trip_id,
        sub_trip,
        health_grade,
        expected_rel,
        s3_target,
        reporter,
    )


def _upload_and_finalize(
    final_dir: Path,
    output_dir: Path,
    trip_id: str,
    sub_trip: int,
    health_grade: int,
    expected_rel: list[Path],
    s3_target: tuple[str, str],
    reporter: Reporter,
) -> None:
    """Upload the trip all-or-nothing, then mark it complete or partial. Never raises.

    ``upload_trip_atomic`` guarantees S3 ends up with either 100% of the trip's
    files or none — a mid-trip failure rolls back the objects it uploaded.
    """
    bucket, prefix = s3_target
    try:
        result = upload_trip_atomic(final_dir, output_dir, bucket, prefix, expected_rel)
    except Exception as exc:
        logger.exception(
            "S3 upload failed for trip %s_%d; keeping local copy", trip_id, sub_trip
        )
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="s3_upload",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return

    if not result.committed:
        detail = (
            f"{result.missing_local} expected file(s) missing locally"
            if result.missing_local
            else f"{result.failed} file(s) failed to upload"
        )
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_PARTIAL,
            error_stage="s3_upload",
            error_message=f"{detail}; rolled back (no partial trip on S3)",
        )
        logger.error(
            "Trip %s_%d not uploaded (%s); rolled back, kept local",
            trip_id,
            sub_trip,
            detail,
        )
        return

    for rel in expected_rel:
        reporter.file(trip_id, sub_trip, health_grade, rel.as_posix(), "uploaded")
    reporter.trip(trip_id, sub_trip, health_grade, TRIP_COMPLETE)
    logger.debug(
        "Uploaded all %d file(s) of trip %s_%d to s3://%s/%s",
        result.uploaded,
        trip_id,
        sub_trip,
        bucket,
        prefix,
    )
