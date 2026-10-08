import errno
import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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
from utils import markers
from utils.download import download_file_with_retry, sweep_partial_files
from utils.http_retry import get_with_retry, post_with_retry
from utils.ledger import (
    TRIP_ARCHIVED,
    TRIP_COMPLETE,
    TRIP_FAILED,
    TRIP_IN_PROGRESS,
    TRIP_PARTIAL,
    TRIP_PENDING_RESTORE,
    Reporter,
    archived_message,
    pending_restore_message,
    present_keys_cover,
)
from utils.s3_uploader import upload_trip_atomic
from utils.settings import settings
from utils.trip_selection import select_minutes

logger = logging.getLogger(__name__)


_marker_digest: tuple[str, str] | None = None


def _scope_digest(output_dir: Path) -> str:
    """This run's scope identity. Cached: a pure function of the settings."""
    global _marker_digest
    if _marker_digest is None or _marker_digest[0] != str(output_dir):
        from utils import ledger as _ledger  # noqa: PLC0415 - avoids an import cycle

        _marker_digest = (
            str(output_dir),
            _ledger.scope_digest(
                output_dir,
                settings.s3_upload,
                dry_mode=settings.dry_mode,
                camera_selection=str(settings.camera_selection),
                endpoint_url=settings.aws_endpoint_url or "",
            ),
        )
    return _marker_digest[1]


def _revoke_claims(output_dir: Path, key: tuple[str, int, int]) -> None:
    """Drop every scope's claim on ``key`` — including this run's own.

    Called before a trip is processed and again before it is published, and the
    scope list is re-read each time rather than cached for the process lifetime.

    * **Own claim.** A marker is written before the trip's COMPLETE status has
      reached the ledger (that goes through a queue). A kill in between leaves a
      marker with no ledger row, so the next pass re-processes the trip — and for
      an S3 target, ``upload_trip_atomic`` rolls back by deleting the very objects
      the marker was vouching for. Dropping our own claim first means a trip that
      is being worked on is never certified by a marker, whatever happens to it.
    * **Re-read, don't cache.** A cached scope list cannot see a scope that
      started afterwards. On macOS workers are spawned and do not inherit the
      parent's run lock, so a worker orphaned by a parent kill can still be
      running when the next run takes the lock; with a cached list it would
      publish over that run's trip without revoking its marker.
    """
    markers.invalidate_claims(output_dir, key, markers.scope_digests(output_dir))

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


# Conservative client default, below the service's 100-key maximum. Trip count
# alone does not bound response bytes or runtime for long, multi-camera trips.
MANIFEST_BATCH_SIZE = 25
# Feature detection, resolved once per process: an older service has no batch
# route and answers 404/405, and there is no point asking again for every batch.
_batch_route_available: bool | None = None


def get_trip_manifests(
    api_key: str,
    dataset_id: str,
    keys: list[tuple[str, int, int]],
) -> tuple[dict[tuple[str, int, int], TripManifest], dict[tuple[str, int, int], str]]:
    """Manifests for many trips, in as few requests as the service allows.

    Returns ``(manifests, errors)`` keyed by ``(trip_id, sub_trip, health_grade)``;
    a trip appears in exactly one of them. Duplicate keys are collapsed first:
    the same trip in two chunks was fetched twice, and if one chunk errored for
    it and the other succeeded it ended up in BOTH dictionaries, which is the one
    thing the docstring above promises cannot happen.

    One manifest per trip is a round trip per trip, and these datasets run to
    ~180k trips, so anything that walks the whole dataset — a resume with no local
    state, the QA coverage check — spends its time waiting rather than working.
    Where the service offers the batch route this asks for 25 at a time; where it
    does not, it falls back to the per-trip route and the caller cannot tell.
    """
    global _batch_route_available
    manifests: dict[tuple[str, int, int], TripManifest] = {}
    errors: dict[tuple[str, int, int], str] = {}
    remaining = list(dict.fromkeys(keys))

    # THIS call's outcome, never the shared global. The global is only a hint
    # that saves a doomed request; reading it back to decide whether this call
    # succeeded is a race: another thread that got a 200 sets it to True between
    # our 404 and our check, and we then return without falling back — the keys
    # of this call appear in neither dictionary.
    if _batch_route_available is not False:
        batched_everything = True
        for start in range(0, len(remaining), MANIFEST_BATCH_SIZE):
            chunk = remaining[start : start + MANIFEST_BATCH_SIZE]
            try:
                got, failed = _post_trip_manifests(api_key, dataset_id, chunk)
            except _BatchRouteUnavailable:
                _batch_route_available = False
                batched_everything = False
                break
            except Exception as exc:
                # Anything else — a transport failure that outlived its retries,
                # an error status, a response that will not parse. Fall back for
                # this chunk rather than letting it escape: one bad batch used to
                # abort the whole caller, discarding the batches that had already
                # succeeded and every per-trip diagnostic with them.
                logger.warning(
                    "Batch manifest request for %d trip(s) failed (%s: %s); "
                    "falling back to one request per trip for them",
                    len(chunk),
                    type(exc).__name__,
                    exc,
                )
                batched_everything = False
                continue
            _batch_route_available = True
            manifests.update(got)
            errors.update(failed)
        if batched_everything:
            return manifests, errors

    for key in remaining:
        if key in manifests or key in errors:
            continue
        try:
            manifests[key] = get_trip_manifest(api_key, dataset_id, *key)
        except Exception as exc:
            errors[key] = f"{type(exc).__name__}: {exc}"
    return manifests, errors


class _BatchRouteUnavailable(Exception):
    """The service does not serve the batch route (older deployment)."""


def _post_trip_manifests(
    api_key: str, dataset_id: str, keys: list[tuple[str, int, int]]
) -> tuple[dict[tuple[str, int, int], TripManifest], dict[tuple[str, int, int], str]]:
    payload = {
        "dataset_id": dataset_id,
        "trips": [
            {"trip_id": t, "sub_trip": s, "health_grade": g} for t, s, g in keys
        ],
    }
    response = post_with_retry(
        f"{settings.base_url}/v1/trip/manifests",
        json=payload,
        headers={AUTH_HEADER_NAME: api_key},
        timeout=settings.api_timeout,
        max_retries=settings.api_max_retries,
        base_delay_seconds=settings.api_retry_base_delay,
        pass_through_statuses=(404, 405),
    )
    if response.status_code in (404, 405):
        raise _BatchRouteUnavailable
    response.raise_for_status()
    body = response.json()

    manifests: dict[tuple[str, int, int], TripManifest] = {}
    for item in body.get("manifests", []):
        manifest = TripManifest.model_validate(item)
        manifests[(manifest.trip_id, manifest.sub_trip, manifest.health_grade)] = manifest
    errors: dict[tuple[str, int, int], str] = {}
    for item in body.get("errors", []):
        key = (item["trip_id"], int(item["sub_trip"]), int(item["health_grade"]))
        if key in manifests:
            # The service contradicted itself: a manifest AND an error for one
            # trip. The manifest is the usable half; recording the error too would
            # put the key in both returned dictionaries, which callers rely on
            # never happening.
            logger.warning(
                "Batch manifest response both served and failed trip %s_%d; "
                "keeping the manifest",
                key[0],
                key[1],
            )
            continue
        errors[key] = f"{item.get('status')}: {item.get('detail')}"
    # A key the service answered for neither way would otherwise vanish silently.
    for key in keys:
        if key not in manifests and key not in errors:
            errors[key] = "no manifest and no error returned for this trip"
    return manifests, errors


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


@dataclass
class _PendingRestore:
    """Files the manifest withholds because they sit in S3 Deep Archive.

    Two categories: ``files`` are restoring (link comes after the restore
    completes) and ``archived`` files will never come — the dataset was created
    without ``settings.request_restore``.
    """

    files: int = 0
    max_available_at: int | None = None  # epoch ms; None when no ETA is known
    archived: int = 0

    def add(self, available_at: int | None) -> None:
        self.files += 1
        if available_at is not None and (
            self.max_available_at is None or available_at > self.max_available_at
        ):
            self.max_available_at = available_at

    def add_archived(self) -> None:
        self.archived += 1


def _iter_trip_tasks(
    trip_dir: Path, manifest: TripManifest, output: DatasetOutput
) -> tuple[list[_DownloadTask], _PendingRestore]:
    """Build the list of files to fetch for a trip, mirroring the manifest structure.

    Files still restoring from Deep Archive have no link yet — they yield no task
    and are tallied in the returned ``_PendingRestore`` instead, so the caller can
    hold the trip until a later run re-fetches the manifest and backfills them.
    """
    timezone = _get_trip_timezone(manifest)
    tasks: list[_DownloadTask] = []
    pending = _PendingRestore()

    for minute in manifest.minutes:
        minute_dir = build_minute_directory_path(
            trip_dir, minute.minute_epoch, timezone
        )
        if output.footage.fused_sei is not None:
            if minute.fused_sei_status == "archived":
                pending.add_archived()
            elif minute.fused_sei_status == "restoring":
                pending.add(minute.fused_sei_available_at)
            elif getattr(minute, "fused_sei", None) is not None:
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
            if camera.video_status == "archived":
                pending.add_archived()
            elif camera.video_status == "restoring" or camera.camera is None:
                pending.add(camera.video_available_at)
            elif settings.dry_mode:
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

            if output.footage.sei is not None:
                if camera.sei_status == "archived":
                    pending.add_archived()
                elif camera.sei_status == "restoring":
                    pending.add(camera.sei_available_at)
                elif camera.sei is not None:
                    sei_path = camera_path.with_suffix(".sei")
                    sei_path = sei_path.with_name(sei_path.name + ".csv")
                    tasks.append(
                        _DownloadTask("sei", camera.sei.presigned_url, sei_path)
                    )

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

    return tasks, pending


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


def _attempt_task(task: _DownloadTask) -> tuple[_DownloadTask, str] | None:
    """Run one task. Returns ``(task, message)`` on failure, ``None`` on success."""
    try:
        _run_task(task)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        # Summary reaches the ledger via the caller; log the per-file reason
        # here (WARNING so it surfaces without DEBUG), traceback at DEBUG.
        logger.warning("Failed to fetch %s: %s", task.dest.name, message)
        logger.debug("Traceback for %s", task.dest.name, exc_info=True)
        return task, message
    return None


def _by_destination(tasks: list[_DownloadTask]) -> list[list[_DownloadTask]]:
    """Group tasks that write the same file, keeping manifest order.

    Two tasks for one destination must not run concurrently: they open the same
    ``<dest>.part`` and race the rename, so the file that survives is undefined.
    The manifest does repeat entries — 4,865 of one dataset's rows in 2026-09.

    They are grouped rather than deduplicated because the repeated entries are
    the same object under different presigned URLs, and a presigned URL can be
    expired or revoked on its own. Dropping all but the first meant one 403 lost
    a file the very next entry would have delivered, and a manifest refresh that
    preserved the order could not recover it.
    """
    groups: dict[Path, list[_DownloadTask]] = {}
    for task in tasks:
        groups.setdefault(task.dest, []).append(task)
    extra = len(tasks) - len(groups)
    if extra:
        logger.warning(
            "Trip manifest yielded %d duplicate file destination(s); each is "
            "fetched once, the repeats held as fallback URLs.",
            extra,
        )
    return list(groups.values())


def _attempt_destination(group: list[_DownloadTask]) -> tuple[_DownloadTask, str] | None:
    """Try one destination's URLs in manifest order; the first that works wins."""
    outcome = None
    for index, task in enumerate(group):
        outcome = _attempt_task(task)
        if outcome is None:
            return None
        if index + 1 < len(group):
            logger.info(
                "Falling back to the next manifest URL for %s", task.dest.name
            )
    return outcome


def _download_trip_tasks(
    tasks: list[_DownloadTask],
) -> list[tuple[_DownloadTask, str]]:
    """Fetch a trip's files, up to ``max_parallel_files`` at once. Returns failures.

    A single file failure never aborts the trip — it is captured with its error
    message (task + ``"ExcType: msg"``) so the trip is marked PARTIAL, the per-file
    detail is persisted to the ledger, and the file is re-attempted on a later pass.

    Threads rather than more processes: a worker is idle on the network for most
    of its wall time (it downloads a trip's ~7-10 objects one by one, then spends
    seconds writing mcap, running exiftool and validating), so the concurrency is
    wanted only around the I/O. Each task writes its own ``<dest>.part`` and
    renames it, and the pooled ``httpx.Client`` is thread-safe, so nothing is
    shared but the connection pool.
    """
    groups = _by_destination(tasks)

    workers = max(settings.max_parallel_files, 1)
    if workers == 1 or len(groups) < 2:
        return [
            out for g in groups if (out := _attempt_destination(g)) is not None
        ]

    pool = ThreadPoolExecutor(
        max_workers=min(workers, len(groups)), thread_name_prefix="dl"
    )
    try:
        # Keep manifest order so the reported failures read the same as before.
        outcomes = list(pool.map(_attempt_destination, groups))
    finally:
        # `map` submits every group up front. When its iteration is abandoned
        # (Ctrl-C mid-trip) the iterator cancels the futures it has not yielded,
        # so queued downloads do not start — but that is a property of `map`'s
        # internals. Say it here, where a reader looks, and do not wait: the
        # `with` form called shutdown(wait=True), which blocked until the
        # in-flight transfers had finished their retries and fallback URLs.
        # Those still run to completion in the background (each writes its own
        # `.part` and renames it, so nothing half-written is ever published).
        pool.shutdown(wait=False, cancel_futures=True)
    return [outcome for outcome in outcomes if outcome is not None]


def _record_download_failures(
    reporter: Reporter,
    trip_id: str,
    sub_trip: int,
    health_grade: int,
    cache_trip_dir: Path,
    failures: list[tuple[_DownloadTask, str]],
    restoring_files: int = 0,
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
    message = f"{len(failures)} file(s) failed to download: {summary}"
    if restoring_files:
        message += f"; {restoring_files} file(s) awaiting archive restore"
    reporter.trip(
        trip_id,
        sub_trip,
        health_grade,
        TRIP_PARTIAL,
        error_stage="download",
        error_message=message,
    )
    logger.error(
        "Trip %s_%d: %d file(s) failed to download; kept cache for retry: %s",
        trip_id,
        sub_trip,
        len(failures),
        summary,
    )


def _delivered_rel(expected_rel: list[Path]) -> list[Path]:
    """The paths publishing actually leaves behind, for the ``files`` rows.

    ``build_expected_trip_files`` returns the canonical delivery shape, and
    deliberately never emits links (``test_dry_mode_expects_video_files_not_links``)
    — the S3 fast-resume and QA each translate for themselves via
    ``allow_links`` / ``_is_expected_file_present``. The ledger cannot: a row is
    read back later as evidence that a named file was delivered, so it has to
    name the file that was. A dry run writes ``<video>.mp4.link`` instead of the
    ``.mp4``, and recording the ``.mp4`` made every dry-run completion revocable
    by its own start-up check, and re-fetched every trip manifest each run.
    """
    if not settings.dry_mode:
        return expected_rel
    return [
        path.with_name(f"{path.name}.link") if path.suffix == ".mp4" else path
        for path in expected_rel
    ]


def _delivered_rel_on_s3(
    expected_rel: list[Path], expected_keys: list[str], present_keys: set[str]
) -> list[Path]:
    """The paths the BUCKET holds, for the ``files`` rows of an S3 fast resume.

    ``_delivered_rel`` answers for a delivery this run performed, where the mode
    decides the shape. Fast resume performs none: it accepts what is already
    there, and in a dry run ``present_keys_cover`` accepts either a ``.mp4`` left
    by an earlier real run or a ``.mp4.link``. Recording the dry shape regardless
    named objects the bucket does not hold, so the next run's listing disproved
    the completion, fast resume re-made it, and the pair repeated forever --
    re-fetching every trip manifest each time. Only reachable once the cover
    check has passed, so a key that is absent is one a ``.link`` stood in for.
    """
    return [
        rel if key in present_keys else rel.with_name(f"{rel.name}.link")
        for rel, key in zip(expected_rel, expected_keys)
    ]


def _apply_minute_selection(
    manifest: TripManifest, minutes: frozenset[str] | None, trip_id: str, sub_trip: int
) -> TripManifest:
    """Trim ``manifest`` to the TRIPS_FILE minute selection (no-op when None).

    Unknown minute names only warn; a selection matching nothing leaves the
    manifest with zero minutes, which the caller treats as a failed trip — never
    as "download the whole trip".
    """
    if minutes is None:
        return manifest
    trimmed, missing = select_minutes(manifest, minutes)
    if missing:
        logger.warning(
            "Trip %s_%d: %d requested minute(s) not in the trip and skipped: %s",
            trip_id,
            sub_trip,
            len(missing),
            ", ".join(missing),
        )
    return trimmed


def _expected_s3_keys(
    prefix: str, final_rel: str, expected_rel_paths: list[Path]
) -> list[str]:
    base = f"{prefix}/{final_rel}" if prefix else final_rel
    return [f"{base}/{rel.as_posix()}" for rel in expected_rel_paths]


def recover_interrupted_publishes(output_dir: Path) -> int:
    """Finish or roll back publishes a previous run was killed in the middle of.

    Replacing a trip is two renames — the old copy to ``.replaced-*``, then the
    staged new copy onto the canonical path — and a SIGKILL or power loss between
    them leaves the canonical path ABSENT with both copies hidden in dot dirs QA
    ignores. Nothing else would ever look there again, so this runs at startup:

    * canonical absent, a staged copy present → the new copy is complete (it was
      fully downloaded and validated before staging); install it.
    * canonical absent, a backup present → restore the old copy.
    * canonical present → whatever is left in dot dirs is redundant; drop it.
    * a ``.copying-*`` dir is a cache-to-staging copy that never finished; it
      is deleted, never promoted — the cache still has the full trip.

    Returns the number of trips touched.

    Only the region level is looked at: ``_publish_trip`` creates its dot dirs at
    ``final_dir.parent``, which ``build_trip_directory_path`` makes
    ``<out>/<country>`` or ``<out>/United States/<state>`` — never deeper. So this
    lists the root, the country dirs and the US state dirs, and nothing below. A
    full ``rglob`` here cost a stat per file of the dataset (~1M metadata ops on a
    4 TB tree, i.e. hours on a network filesystem) to find dirs that cannot exist
    there. Trip names are still read (readdir streams them), but never stat'ed:
    only dot-prefixed entries are type-checked.
    """
    touched = 0
    if not output_dir.is_dir():
        return touched

    # Symlinks ARE followed: a country or state dir may be a symlink onto another
    # volume, and _publish_trip happily stages beside the destination through it.
    # DirEntry.is_dir() answers from readdir's d_type for real dirs and files, so
    # only symlink entries (and the few dot entries) can ever cost a stat.
    def _dot_dirs(parent: Path) -> list[Path]:
        with os.scandir(parent) as it:
            return [Path(e.path) for e in it if e.name.startswith(".") and e.is_dir()]

    def _plain_dirs(parent: Path) -> list[Path]:
        with os.scandir(parent) as it:
            return [
                Path(e.path) for e in it if not e.name.startswith(".") and e.is_dir()
            ]

    countries = _plain_dirs(output_dir)
    candidates = [
        output_dir,
        *countries,
        *(s for c in countries if c.name == "United States" for s in _plain_dirs(c)),
    ]
    for parent in candidates:
        if not parent.is_dir():
            continue
        # Staged copies FIRST, deterministically. Listing order is
        # filesystem-dependent, and when both a staged copy and a backup of the
        # same trip survive, the staged one — newer, fully downloaded and
        # validated, the copy the run meant to install — must win. On ext4 the
        # backup happened to be listed first and was restored instead.
        entries = sorted(
            _dot_dirs(parent),
            key=lambda e: (not e.name.startswith(".staging-"), e.name),
        )
        for entry in entries:
            if entry.name.startswith(".copying-"):
                # A cache-to-staging copy that never finished. The cache still
                # holds the full trip (shutil.move deletes the source only after
                # a complete copy), so the next run re-processes it cheaply.
                shutil.rmtree(entry, ignore_errors=True)
                logger.info("Discarded incomplete publish copy %s", entry)
                continue
            if entry.name.startswith(".staging-"):
                for trip in list(entry.iterdir()):
                    canonical = parent / trip.name
                    if not canonical.exists():
                        os.rename(trip, canonical)
                        logger.warning(
                            "Recovered trip %s from an interrupted publish", canonical
                        )
                        touched += 1
                shutil.rmtree(entry, ignore_errors=True)
            elif entry.name.startswith(".replaced-"):
                # ".replaced-<trip>-.staging-<rand>": the trip name is between the
                # prefix and the staging token that produced it.
                trip_name = entry.name[len(".replaced-"):].rsplit("-.staging-", 1)[0]
                canonical = parent / trip_name
                if not canonical.exists():
                    os.rename(entry, canonical)
                    logger.warning(
                        "Restored previous copy of trip %s after an interrupted publish",
                        canonical,
                    )
                    touched += 1
                else:
                    shutil.rmtree(entry, ignore_errors=True)
    return touched


def _publish_trip(
    cache_trip_dir: Path, final_dir: Path, trip_id: str, sub_trip: int
) -> None:
    """Put the finished trip at ``final_dir`` atomically, never losing every copy.

    ``shutil.move`` straight onto an existing directory moves the source INSIDE
    it, so a stale copy at the destination nested the new trip a level deep and
    the run reported success. Instead: stage beside the destination, then
    ``os.rename``, which replaces nothing or fails.

    The staging dir comes from ``mkdtemp`` (exclusive creation), not a
    pid-derived name — pids repeat across containers sharing one filesystem.
    The cache may sit on another mount, so the first hop cannot be a rename.

    With one dataset per output dir, anything already at ``final_dir`` is this
    dataset's own stale copy. It is moved aside to a backup, the new copy is
    installed, and only then is the backup removed — so a failure at any point
    leaves at least one copy on disk: the backup is restored and the staged copy
    is kept and named in the log. Staging is deleted ONLY after success.
    """
    for attempt in (0, 1):
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        staging_parent = Path(
            tempfile.mkdtemp(dir=final_dir.parent, prefix=".staging-")
        )
        staging = staging_parent / final_dir.name
        cache_still_holds_the_trip = _stage_from_cache(cache_trip_dir, staging)
        backup: Path | None = None
        try:
            if final_dir.exists():
                backup = final_dir.parent / f".replaced-{final_dir.name}-{staging_parent.name}"
                os.rename(final_dir, backup)
            try:
                os.rename(staging, final_dir)
            except OSError as exc:
                if exc.errno == errno.ENOENT and attempt == 0:
                    # Parent pruned under us by a concurrent run. Restore, put
                    # the trip back in the cache, and go again.
                    if backup is not None and backup.exists():
                        os.rename(backup, final_dir)
                    if not cache_still_holds_the_trip:
                        shutil.move(str(staging), str(cache_trip_dir))
                    shutil.rmtree(staging_parent, ignore_errors=True)
                    continue
                raise
        except Exception:
            if backup is not None and backup.exists() and not final_dir.exists():
                os.rename(backup, final_dir)  # the old copy is back where it was
            logger.error(
                "Trip %s_%d could not be published; its downloaded copy is kept at %s",
                trip_id,
                sub_trip,
                staging,
            )
            raise
        if backup is not None:
            shutil.rmtree(backup, ignore_errors=True)
        shutil.rmtree(staging_parent, ignore_errors=True)
        if cache_still_holds_the_trip:
            # The trip is installed; only now is the cache copy redundant.
            shutil.rmtree(cache_trip_dir, ignore_errors=True)
        return


def _stage_from_cache(cache_trip_dir: Path, staging: Path) -> bool:
    """Put the cached trip at ``staging``. Returns whether the cache still has it.

    Same filesystem: one ``os.rename``, which is atomic — staging either does not
    exist or is the complete trip, and there is no window in which a kill could
    leave a partial copy that looks finished.

    Across filesystems a rename is impossible, so the trip is COPIED and the
    cache is left alone until the trip is installed. That matters: ``shutil.move``
    deletes the source as soon as the copy lands, so a kill between that and the
    promotion used to leave the only copy inside a ``.copying-`` dir — which
    start-up recovery deletes on sight, on the assumption that the cache still
    had it. It did not.

    The copy lands in ``.copying-`` (garbage to recovery) and is promoted to the
    caller's ``.staging-`` (a complete, validated trip to recovery) by a rename,
    so a partial copy can never be mistaken for a finished one.
    """
    try:
        os.rename(cache_trip_dir, staging)
        return False
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise

    copying_parent = Path(
        tempfile.mkdtemp(dir=staging.parent.parent, prefix=".copying-")
    )
    try:
        shutil.copytree(cache_trip_dir, copying_parent / staging.name)
        os.replace(copying_parent / staging.name, staging)
    except Exception:
        shutil.rmtree(copying_parent, ignore_errors=True)
        raise
    shutil.rmtree(copying_parent, ignore_errors=True)
    return True


def process_trip(
    api_key: str,
    dataset_id: str,
    trip_id: str,
    sub_trip: int,
    output_dir: Path,
    output: DatasetOutput,
    *,
    reporter: Reporter,
    cache_dir: Path | None = None,
    present_keys: set[str] | None = None,
    s3_target: tuple[str, str] | None = None,
    health_grade: int = 500,
    minutes: frozenset[str] | None = None,
) -> None:
    """Download, format, move and (optionally) upload one trip, tracking status.

    Never raises: every failure is recorded via ``reporter`` and the trip is left
    in a resumable state (PARTIAL/FAILED). The local cache dir is kept on failure
    so a later pass re-downloads only what is missing.

    ``minutes`` (folder names, ``HH-MM-SS``) restricts the trip to those minutes:
    the manifest is trimmed right after each fetch, so every downstream step —
    downloads, expected files, formatting, validation, upload — sees only them.
    """
    # build_expected_trip_files lives in manifest_coverage, which imports this
    # module — import lazily to avoid a circular import at load time.
    from app.manifest_coverage import build_expected_trip_files  # noqa: PLC0415

    trip_key = (trip_id, sub_trip, health_grade)
    # A trip that is being worked on is certified by nobody, this run included:
    # everything below can replace or (on S3) roll back and delete its data.
    try:
        _revoke_claims(output_dir, trip_key)
    except OSError as exc:
        logger.exception("Could not clear completion markers for trip %s_%d", trip_id, sub_trip)
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="markers",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return

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

    trip_manifest = _apply_minute_selection(trip_manifest, minutes, trip_id, sub_trip)
    if minutes is not None and not trip_manifest.minutes:
        message = f"none of the {len(minutes)} requested minute(s) exist in this trip"
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="minute_selection",
            error_message=message,
        )
        logger.error("Trip %s_%d skipped: %s", trip_id, sub_trip, message)
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
        if expected_keys and present_keys_cover(
            present_keys, expected_keys, allow_links=settings.dry_mode
        ):
            for rel in _delivered_rel_on_s3(
                expected_rel, expected_keys, present_keys
            ):
                reporter.file(
                    trip_id, sub_trip, health_grade, rel.as_posix(), "uploaded"
                )
            # No marker here. This branch decides from `present_keys`, a listing
            # taken once at start-up, and never re-checks S3: another run can have
            # rolled those very objects back since. The S3 listing is re-taken on
            # every run anyway, so it is the authority for an S3 delivery — a
            # marker would only add a claim nothing verified.
            reporter.trip(trip_id, sub_trip, health_grade, TRIP_COMPLETE)
            logger.debug(
                "Trip %s_%d already complete on S3; skipped", trip_id, sub_trip
            )
            return

    # ``cache_dir`` is the caller's per-(dataset, output dir) subtree; the
    # settings default is only for callers outside app.main (tests, one-offs).
    cache_trip_dir = build_cache_trip_path(
        cache_dir if cache_dir is not None else settings.cache_dir,
        trip_id,
        sub_trip,
        health_grade,
    )
    try:
        # A killed run may have left ``*.part`` files in THIS trip's cache dir.
        # They are harmless (the next attempt truncates and replaces them), so
        # this is hygiene, done here per trip — a handful of entries — instead of
        # a start-up sweep of the whole cache tree, which on a network filesystem
        # holding a failed run's 2 TB cost hours before the first download.
        sweep_partial_files(cache_trip_dir)
        tasks, restoring = _iter_trip_tasks(cache_trip_dir, trip_manifest, output)
        failures = _download_trip_tasks(tasks)
        if failures:
            # Retry once with a fresh manifest in case presigned URLs expired. The
            # fresh manifest also re-checks Deep-Archive state, so `restoring` is
            # replaced with the server's current view.
            try:
                trip_manifest = _apply_minute_selection(
                    get_trip_manifest(
                        api_key, dataset_id, trip_id, sub_trip, health_grade
                    ),
                    minutes,
                    trip_id,
                    sub_trip,
                )
                tasks, restoring = _iter_trip_tasks(
                    cache_trip_dir, trip_manifest, output
                )
                failures = _download_trip_tasks(tasks)
            except Exception:
                logger.exception(
                    "Manifest refresh failed for trip %s_%d", trip_id, sub_trip
                )

        # A file can be archived between the manifest check and the download — S3
        # then answers InvalidObjectState. Count it as restoring (the next manifest
        # fetch requests the restore server-side), not as a hard failure.
        still_failed = []
        for task, message in failures:
            if "InvalidObjectState" in message:
                restoring.add(None)
            else:
                still_failed.append((task, message))
        failures = still_failed

        if failures:
            _record_download_failures(
                reporter,
                trip_id,
                sub_trip,
                health_grade,
                cache_trip_dir,
                failures,
                restoring_files=restoring.files,
            )
            return

        if restoring.archived > 0:
            # No restore is coming for these files while the dataset opts out of
            # restores. Held in cache and skipped for the rest of the run; a
            # future run re-checks (the dataset may get request_restore=true).
            message = archived_message(restoring.archived)
            if restoring.files > 0:
                message += f" (+{restoring.files} file(s) awaiting archive restore)"
            reporter.trip(
                trip_id,
                sub_trip,
                health_grade,
                TRIP_ARCHIVED,
                error_stage="archived",
                error_message=message,
            )
            logger.info(
                "Trip %s_%d: %s; everything available is downloaded — trip held in "
                "the cache until a later run finds the files downloadable",
                trip_id,
                sub_trip,
                message,
            )
            return

        if restoring.files > 0:
            message = pending_restore_message(
                restoring.files, restoring.max_available_at
            )
            reporter.trip(
                trip_id,
                sub_trip,
                health_grade,
                TRIP_PENDING_RESTORE,
                error_stage="pending_restore",
                error_message=message,
            )
            logger.info(
                "Trip %s_%d: %s; everything available is downloaded — trip held in "
                "the cache until a later run completes it",
                trip_id,
                sub_trip,
                message,
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

    # Passed validation — publish the trip from the cache into the dataset directory.
    # Publishing REPLACES the trip directory, so any claim on it (a different
    # camera selection, dry mode, or S3 target) stops being true the moment the
    # rename lands. Revoke again here, re-reading the scope list: a scope may
    # have appeared since this trip started, and a failure to revoke must stop
    # the publish rather than orphan a claim on data we are about to overwrite.
    try:
        _revoke_claims(output_dir, trip_key)
    except OSError as exc:
        logger.exception(
            "Could not revoke completion markers before publishing trip %s_%d",
            trip_id,
            sub_trip,
        )
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_FAILED,
            error_stage="markers",
            error_message=f"{type(exc).__name__}: {exc}",
        )
        return
    try:
        _publish_trip(cache_trip_dir, final_dir, trip_id, sub_trip)
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
        for rel in _delivered_rel(expected_rel):
            reporter.file(trip_id, sub_trip, health_grade, rel.as_posix(), "downloaded")
        markers.mark_complete(
            output_dir,
            _scope_digest(output_dir),
            trip_key,
            [rel.as_posix() for rel in expected_rel],
            country=country or "",
            state=state or "",
        )
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
        marker_digest=None,
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
    marker_digest: str | None = None,
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
        # Scoped to THIS attempt's writes on purpose. A retry after an incomplete
        # rollback uploads nothing new (the leftover is "already present"), rolls
        # back nothing, and must not be read as having cleaned the trip up.
        if result.rollback_left:
            rollback = (
                f"rollback INCOMPLETE: {result.rollback_left} object(s) of this "
                "attempt could not be deleted and remain on S3"
            )
        elif result.rolled_back:
            rollback = f"this attempt's {result.rolled_back} upload(s) rolled back"
        else:
            rollback = "nothing of this attempt to roll back"
        reporter.trip(
            trip_id,
            sub_trip,
            health_grade,
            TRIP_PARTIAL,
            error_stage="s3_upload",
            error_message=f"{detail}; {rollback}",
        )
        logger.error(
            "Trip %s_%d not uploaded (%s); %s, kept local",
            trip_id,
            sub_trip,
            detail,
            rollback,
        )
        return

    for rel in _delivered_rel(expected_rel):
        reporter.file(trip_id, sub_trip, health_grade, rel.as_posix(), "uploaded")
    # No marker for an S3 delivery. A marker lives under the output dir, and two
    # runs of one dataset can share a bucket prefix from two different output
    # dirs: when the second run's upload fails and rolls back, it deletes the
    # shared objects but cannot reach the first run's markers, so the first run
    # would resume trusting a marker for data no longer in the bucket. The
    # bucket listing is the resume signal for an S3 run, and it is read fresh
    # every time. `marker_digest` stays as a seam the tests drive.
    if marker_digest is not None:
        markers.mark_complete(
            output_dir, marker_digest, (trip_id, sub_trip, health_grade)
        )
    reporter.trip(trip_id, sub_trip, health_grade, TRIP_COMPLETE)
    logger.debug(
        "Uploaded all %d file(s) of trip %s_%d to s3://%s/%s",
        result.uploaded,
        trip_id,
        sub_trip,
        bucket,
        prefix,
    )
