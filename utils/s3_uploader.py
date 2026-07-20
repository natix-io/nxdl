"""Optional S3 upload of finalized trips.

Activated by the ``s3_upload`` setting (see ``schemas/app_settings.py``). When a
trip has been moved into ``output_dir`` it is uploaded to the configured S3
location, mirroring the local ``<country>/<state?>/<trip>/...`` layout under the
URI's key prefix.

``boto3`` is imported lazily so a run without S3 upload never depends on it, and
the client is created after the multiprocessing fork (one cached client per
worker process), which keeps it fork-safe.
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from utils.settings import settings

logger = logging.getLogger(__name__)

_s3_client = None


@dataclass(slots=True)
class UploadResult:
    """Outcome of an all-or-nothing trip upload."""

    committed: bool = False  # True only when 100% of expected files are on S3
    uploaded: int = 0
    failed: int = 0
    missing_local: int = 0  # expected files not found on disk (can't reach 100%)
    uploaded_paths: list[Path] = field(default_factory=list)
    # Files present on disk that are NOT in the manifest's expected set — left
    # local, never uploaded. This is what keeps strays / foreign trips off S3.
    extra_files: list[Path] = field(default_factory=list)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/optional/prefix`` URI into ``(bucket, prefix)``.

    The prefix is normalized without leading/trailing slashes and may be empty.
    """
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI (expected 's3://bucket/prefix'): {uri!r}")
    bucket = parsed.netloc
    prefix = parsed.path.strip("/")
    return bucket, prefix


def get_s3_client():
    """Return a cached boto3 S3 client for the current process.

    Credentials/region/endpoint from settings are passed only when non-empty;
    otherwise boto3 uses its standard credential chain.
    """
    global _s3_client
    if _s3_client is None:
        import boto3

        client_kwargs: dict[str, str] = {}
        if settings.aws_access_key_id:
            client_kwargs["aws_access_key_id"] = settings.aws_access_key_id
        if settings.aws_secret_access_key:
            client_kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
        if settings.aws_region:
            client_kwargs["region_name"] = settings.aws_region
        if settings.aws_endpoint_url:
            client_kwargs["endpoint_url"] = settings.aws_endpoint_url
        _s3_client = boto3.client("s3", **client_kwargs)
    return _s3_client


def _build_key(local_path: Path, dataset_root: Path, prefix: str) -> str:
    """S3 key mirroring ``local_path`` relative to ``dataset_root`` under ``prefix``."""
    relative = local_path.relative_to(dataset_root).as_posix()
    return f"{prefix}/{relative}" if prefix else relative


def _s3_object_size(client, bucket: str, key: str) -> int | None:
    """Return remote object size, or ``None`` if the key does not exist."""
    from botocore.exceptions import ClientError

    try:
        return client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def _put_and_verify(client, local_file: Path, bucket: str, key: str) -> bool:
    """Ensure ``local_file`` is present on S3 at ``key``. Does NOT delete the local copy.

    If a non-empty object already exists it is treated as a complete upload (S3
    objects are atomic; an interrupted upload leaves no visible object). Otherwise
    the file is uploaded and verified by a ``head_object`` size match. Never raises;
    returns ``True`` only when the object is confirmed present and correct on S3.
    """
    try:
        size = local_file.stat().st_size
        remote_size = _s3_object_size(client, bucket, key)
        # Existence, not exact byte match: local and remote may be equivalent-but-
        # differently-serialized JSON, which would false-fail a size comparison.
        if remote_size is not None and remote_size > 0:
            logger.debug("Already on S3 s3://%s/%s", bucket, key)
            return True

        client.upload_file(str(local_file), bucket, key)
        remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        if remote_size != size:
            logger.error(
                "Size mismatch for s3://%s/%s: local=%d remote=%d",
                bucket,
                key,
                size,
                remote_size,
            )
            return False
        logger.debug("Uploaded+verified s3://%s/%s", bucket, key)
        return True
    except Exception:
        logger.exception(
            "Upload/verify failed for %s -> s3://%s/%s", local_file, bucket, key
        )
        return False


def delete_key(client, bucket: str, key: str) -> None:
    """Delete one object (idempotent; deleting a missing key is a no-op). Never raises."""
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        logger.exception("Failed to delete s3://%s/%s", bucket, key)


def upload_and_remove_file(
    local_file: Path, dataset_root: Path, bucket: str, prefix: str
) -> bool:
    """Upload one file, confirm it landed, then delete the local copy.

    Used for standalone single files (e.g. ``dataset_request.json``). Returns
    ``True`` only after the local file has been removed; ``False`` on any failure
    (never raises) so the file is left in place for a later retry.
    """
    client = get_s3_client()
    key = _build_key(local_file, dataset_root, prefix)
    if not _put_and_verify(client, local_file, bucket, key):
        return False
    try:
        local_file.unlink()
    except OSError:
        logger.warning(
            "Uploaded s3://%s/%s but could not remove local copy", bucket, key
        )
    return True


def prune_empty_dirs(root: Path, *, remove_root: bool = True) -> None:
    """Remove empty directories under ``root`` bottom-up.

    With ``remove_root`` (default) ``root`` itself is removed too if it ends up empty —
    used for a trip dir. Pass ``remove_root=False`` to keep ``root`` (e.g. the dataset
    output dir) while still clearing its emptied children.
    """
    if not root.exists():
        return
    dirs = sorted(
        (p for p in root.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    )
    if remove_root:
        dirs.append(root)
    for directory in dirs:
        try:
            directory.rmdir()
        except OSError:
            pass  # directory not empty — leave it (still holds un-uploaded files)


def _resolve_expected_on_disk(final_dir: Path, rel_path: Path) -> Path | None:
    """Map an expected trip-relative path to the file actually on disk.

    In dry mode a video is stored as ``<name>.mp4.link`` instead of ``<name>.mp4``,
    so an expected ``.mp4`` resolves to its ``.link`` sibling when present.
    """
    candidate = final_dir / rel_path
    if candidate.is_file():
        return candidate
    if rel_path.suffix == ".mp4":
        link = candidate.with_name(candidate.name + ".link")
        if link.is_file():
            return link
    return None


def upload_trip_atomic(
    final_dir: Path,
    dataset_root: Path,
    bucket: str,
    prefix: str,
    expected_rel_paths: Iterable[Path],
) -> UploadResult:
    """Upload a trip's manifest-expected files ALL-OR-NOTHING.

    Only the files in ``expected_rel_paths`` are considered — anything else on disk
    is recorded in ``extra_files`` and never uploaded (keeps strays/foreign trips
    off S3). Local copies are kept until the ENTIRE expected set is confirmed on
    S3; only then are they deleted and ``committed`` is set. If any expected file
    is missing locally or fails to upload, every object this trip controls is
    deleted from S3 (rollback) so a partially-uploaded trip never exists there, and
    all local files are kept for a later retry. Never raises.
    """
    client = get_s3_client()
    result = UploadResult()

    resolved: list[tuple[Path, str]] = []  # (on_disk_path, s3_key)
    for rel_path in expected_rel_paths:
        on_disk = _resolve_expected_on_disk(final_dir, rel_path)
        if on_disk is None:
            result.missing_local += 1
        else:
            resolved.append((on_disk, _build_key(on_disk, dataset_root, prefix)))

    allowed_paths = {on_disk.resolve() for on_disk, _ in resolved}
    result.extra_files = [
        f
        for f in sorted(final_dir.rglob("*"))
        if f.is_file() and f.resolve() not in allowed_paths
    ]
    if result.extra_files:
        logger.debug(
            "%d file(s) under %s are not in the trip manifest; never uploaded",
            len(result.extra_files),
            final_dir,
        )

    all_keys = [key for _, key in resolved]

    # Can't reach 100% if an expected file is missing on disk → upload nothing,
    # ensure no stale partial remains on S3.
    if result.missing_local:
        _rollback(client, bucket, all_keys)
        logger.error(
            "Trip %s missing %d expected file(s) locally; not uploaded",
            final_dir,
            result.missing_local,
        )
        return result

    for on_disk, key in resolved:
        if _put_and_verify(client, on_disk, bucket, key):
            result.uploaded += 1
            result.uploaded_paths.append(on_disk)
        else:
            result.failed += 1

    if result.failed:
        _rollback(client, bucket, all_keys)
        result.uploaded = 0
        result.uploaded_paths.clear()
        return result

    # Full success: every expected file is on S3 → now safe to delete locals.
    for on_disk in result.uploaded_paths:
        try:
            on_disk.unlink()
        except OSError:
            logger.warning("Uploaded %s but could not remove local copy", on_disk)
    result.committed = True
    prune_empty_dirs(final_dir)
    return result


def _rollback(client, bucket: str, keys: list[str]) -> None:
    """Delete every given key so no partially-uploaded trip is left on S3."""
    if not keys:
        return
    for key in keys:
        delete_key(client, bucket, key)
    logger.warning(
        "Rolled back %d object(s) on s3://%s so no partial trip remains",
        len(keys),
        bucket,
    )
