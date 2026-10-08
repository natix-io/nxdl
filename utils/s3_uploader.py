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
import os
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
    # Objects THIS attempt wrote: how many the rollback deleted and how many it
    # could not. A clean rollback says nothing about objects an earlier attempt
    # left behind, so neither number is a statement about the whole trip.
    rolled_back: int = 0
    rollback_left: int = 0
    uploaded_paths: list[Path] = field(default_factory=list)
    # Files present on disk that are NOT in the manifest's expected set — left
    # local, never uploaded. This is what keeps strays / foreign trips off S3.
    extra_files: list[Path] = field(default_factory=list)


# One dataset per S3 prefix, recorded in the prefix itself. Dot-prefixed, and the
# S3 QA snapshot drops dot-prefixed segments (utils.s3_dataset.snapshot_from_listing)
# so validation never sees it as content.
PREFIX_MARKER_KEY = ".natix/dataset"
# The client uploads the saved dataset request to the prefix root at the end of a
# run. It names the dataset, so a prefix without a marker can still prove whose it
# is — that is the adopt path for deliveries that predate the marker.
PREFIX_REQUEST_KEY = "dataset_request.json"


def prefix_marker_key(prefix: str) -> str:
    return f"{prefix}/{PREFIX_MARKER_KEY}" if prefix else PREFIX_MARKER_KEY


def prefix_request_key(prefix: str) -> str:
    return f"{prefix}/{PREFIX_REQUEST_KEY}" if prefix else PREFIX_REQUEST_KEY


def prefix_has_dataset_content(prefix: str, keys: Iterable[str]) -> bool:
    """Whether the prefix already holds delivered TRIPS.

    Client state under ``.natix/`` and the root ``dataset_request.json`` do not
    count: both are bookkeeping, and a prefix holding only the request file is an
    aborted run's, not a delivery.
    """
    marker_prefix = f"{prefix}/.natix/" if prefix else ".natix/"
    required = f"{prefix}/" if prefix else ""
    request = prefix_request_key(prefix)
    return any(
        key.startswith(required) and not key.startswith(marker_prefix) and key != request
        for key in keys
    )


def read_prefix_request_owner(client, bucket: str, prefix: str) -> str | None:
    """The dataset id the prefix's root ``dataset_request.json`` names, or None.

    None when the file is absent or unreadable as the client's own format; the
    caller treats that as "no evidence", never as a match.
    """
    from botocore.exceptions import ClientError

    try:
        body = client.get_object(Bucket=bucket, Key=prefix_request_key(prefix))["Body"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    try:
        import json  # noqa: PLC0415

        value = json.loads(body.read().decode("utf-8")).get("request_id")
    except (ValueError, AttributeError, UnicodeDecodeError):
        return None
    return str(value) if value else None


def read_prefix_owner(client, bucket: str, prefix: str) -> str | None:
    """The dataset id recorded for this prefix, or ``None`` if unclaimed."""
    from botocore.exceptions import ClientError

    try:
        body = client.get_object(Bucket=bucket, Key=prefix_marker_key(prefix))["Body"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return body.read().decode("utf-8").strip() or None


def claim_prefix(client, bucket: str, prefix: str, dataset_id: str) -> str | None:
    """Claim this prefix for ``dataset_id``. Returns the dataset that actually owns it.

    The write is conditional: ``If-None-Match: *`` makes S3 accept the PUT only
    when no object is there, so of two runs that both found the prefix unclaimed
    exactly one write lands and the other gets 412. An unconditional PUT plus a
    read-back does not do this — A writes A, reads A and proceeds, then B writes
    B, reads B and proceeds, and both are running against one prefix with B's
    name on it.

    The read-back stays, for a narrower reason. Not every S3-compatible endpoint
    implements the condition, and one that ignores the header makes the PUT
    unconditional again. The read-back then catches the loser whose write was
    already overwritten by the time it looks — but NOT the writer whose marker
    is overwritten afterwards: A writes A, reads A and proceeds, then B writes
    B, reads B and proceeds. On such an endpoint the claim is not exclusive and
    nothing here can make it so; the guarantee holds on S3 proper.
    """
    from botocore.exceptions import ClientError

    try:
        client.put_object(
            Bucket=bucket,
            Key=prefix_marker_key(prefix),
            Body=(dataset_id + "\n").encode("utf-8"),
            IfNoneMatch="*",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code not in ("PreconditionFailed", "412") and status != 412:
            raise
        # Someone claimed it between our read and our write; theirs stands.
    return read_prefix_owner(client, bucket, prefix)


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


# Outcome of one object's upload. The distinction matters for rollback: an
# object this run did not write is not this run's to delete.
PUT_UPLOADED = "uploaded"
PUT_ALREADY_PRESENT = "already_present"
PUT_FAILED = "failed"
# The PUT went out and may well have landed, but confirming it did not answer.
# Counts as a failure for the trip, and as OURS for the rollback: leaving it
# behind would leave a partial trip on S3, which is what all-or-nothing exists
# to prevent.
PUT_UNVERIFIED = "unverified"
# Uploaded over an object of a different size that was already at the key — a
# leftover of an earlier attempt. Counts as delivered, but is never rolled back:
# the key existed before this attempt, so it is not this attempt's to delete.
PUT_REPLACED = "replaced"


def _put_and_verify(client, local_file: Path, bucket: str, key: str) -> str:
    """Ensure ``local_file`` is present on S3 at ``key``. Does NOT delete the local copy.

    An object already at the key with the local file's size is a complete upload
    (``.json`` excepted: re-serialised JSON can differ in whitespace and key
    order, so for it existence counts). Any other pre-existing object — a
    truncated leftover of a rollback that could not delete it, or an empty one —
    is replaced with the full file. Otherwise the file is uploaded. Every upload
    is verified by a ``head_object`` size match. Never raises.

    Returns which of these happened, rather than a bool, because a rollback must
    be able to tell "I created this key" from "it was already there". That
    distinction is decided by the FIRST HEAD alone and holds whatever happens
    afterwards: a key that pre-existed is never reported as this attempt's to
    delete, even when the replacement upload raised or could not be verified.
    """
    try:
        size = local_file.stat().st_size
        remote_size = _s3_object_size(client, bucket, key)
        pre_existed = remote_size is not None
        if pre_existed and remote_size > 0 and (remote_size == size or key.endswith(".json")):
            logger.debug("Already on S3 s3://%s/%s", bucket, key)
            return PUT_ALREADY_PRESENT
        if pre_existed:
            logger.warning(
                "s3://%s/%s is %d byte(s) but the local file is %d; re-uploading over "
                "what looks like a leftover from an interrupted attempt",
                bucket,
                key,
                remote_size,
                size,
            )
        # A failure on a key that pre-existed is a plain failure: the object there
        # is not this attempt's to roll back, whether it is still the leftover or
        # our half-landed replacement. Only a key we created is ours to undo.
        on_uncertain = PUT_FAILED if pre_existed else PUT_UNVERIFIED

        try:
            client.upload_file(str(local_file), bucket, key)
        except Exception:
            logger.exception(
                "Upload failed for %s -> s3://%s/%s", local_file, bucket, key
            )
            if pre_existed:
                return PUT_FAILED
            # S3 can commit the object and still raise here — a lost response, a
            # retry that succeeded server-side. Ask whether it is there: an
            # object we may have written is ours to roll back, and reporting a
            # plain failure would leave it behind as part of a trip that failed.
            try:
                landed = _s3_object_size(client, bucket, key) is not None
            except Exception:
                # Could not even ask. Absence is not established, so the object
                # is treated as possibly ours: rolling back a key that turns out
                # not to exist is a no-op, leaving one that does is a partial trip.
                logger.exception(
                    "s3://%s/%s: upload raised and the follow-up HEAD failed too; "
                    "treating it as this run's object",
                    bucket,
                    key,
                )
                return PUT_UNVERIFIED
            if landed:
                logger.warning(
                    "s3://%s/%s exists although the upload raised; treating it as "
                    "this run's object",
                    bucket,
                    key,
                )
                return PUT_UNVERIFIED
            return PUT_FAILED
        try:
            remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except Exception:
            logger.exception(
                "Uploaded s3://%s/%s but could not confirm it%s",
                bucket,
                key,
                "" if pre_existed else "; treating it as this run's object so the "
                "rollback can remove it",
            )
            return on_uncertain
        if remote_size != size:
            logger.error(
                "Size mismatch for s3://%s/%s: local=%d remote=%d",
                bucket,
                key,
                size,
                remote_size,
            )
            return on_uncertain
        logger.debug("Uploaded+verified s3://%s/%s", bucket, key)
        return PUT_REPLACED if pre_existed else PUT_UPLOADED
    except Exception:
        logger.exception(
            "Upload/verify failed for %s -> s3://%s/%s", local_file, bucket, key
        )
        return PUT_FAILED


def delete_key(client, bucket: str, key: str) -> bool:
    """Delete one object (idempotent; deleting a missing key is a no-op). Never raises.

    Returns whether it went. A denied or failed DELETE leaves the object there,
    and a rollback that swallowed it reported a clean failure over a trip that is
    still half on S3.
    """
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        logger.exception("Failed to delete s3://%s/%s", bucket, key)
        return False
    return True


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
    # Only a CONFIRMED object may cost us the local copy. `PUT_UNVERIFIED` means
    # the upload may have landed but the size could not be confirmed — deleting
    # then would leave nothing to retry from.
    if _put_and_verify(client, local_file, bucket, key) not in (
        PUT_UPLOADED,
        PUT_ALREADY_PRESENT,
        PUT_REPLACED,
    ):
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


def prune_empty_region_dirs(output_dir: Path) -> None:
    """Remove the country and state dirs left empty once their trips are uploaded.

    Everything below a trip is already pruned per trip by ``upload_trip_atomic``,
    so only the region level can be left behind — and that is the only level this
    looks at. The whole-tree ``rglob`` this replaces was the same O(files) walk
    that used to run at start-up: on a 4 TB dataset it is hundreds of thousands of
    metadata calls at the end of every S3 run, to delete a handful of directories.

    Dot entries are the client's own state and are never touched.
    """
    if not output_dir.is_dir():
        return

    def child_dirs(parent: Path) -> list[Path]:
        try:
            with os.scandir(parent) as entries:
                return [
                    Path(e.path)
                    for e in entries
                    if not e.name.startswith(".") and e.is_dir()
                ]
        except (FileNotFoundError, NotADirectoryError):
            return []

    countries = child_dirs(output_dir)
    # States first: a country only becomes empty once its states are gone.
    for country in countries:
        for state in child_dirs(country):
            try:
                state.rmdir()
            except OSError:
                pass  # still holds trips that did not upload
    for country in countries:
        try:
            country.rmdir()
        except OSError:
            pass


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

    # Can't reach 100% if an expected file is missing on disk → upload nothing.
    # Nothing is deleted either: this run has written no object, and the keys it
    # WOULD have written may already hold another delivery's data (an S3 key is
    # the prefix plus the trip's path, with no dataset in it).
    if result.missing_local:
        logger.error(
            "Trip %s missing %d expected file(s) locally; not uploaded",
            final_dir,
            result.missing_local,
        )
        return result

    written: list[str] = []  # keys THIS call created — the only ones it may undo
    for on_disk, key in resolved:
        outcome = _put_and_verify(client, on_disk, bucket, key)
        if outcome == PUT_UPLOADED:
            written.append(key)
            result.uploaded += 1
            result.uploaded_paths.append(on_disk)
        elif outcome in (PUT_ALREADY_PRESENT, PUT_REPLACED):
            # Not in `written`: the key existed before this attempt (a full
            # object, or a leftover we overwrote), so a rollback must leave it.
            result.uploaded += 1
            result.uploaded_paths.append(on_disk)
        else:
            if outcome == PUT_UNVERIFIED:
                # We wrote it, or may have. Ours to roll back.
                written.append(key)
            result.failed += 1

    if result.failed:
        # Roll back only what this call wrote. Deleting every expected key would
        # destroy objects that were already there — including a different
        # dataset's, when two share an S3 prefix.
        result.rollback_left = len(_rollback(client, bucket, written))
        result.rolled_back = len(written) - result.rollback_left
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


def _rollback(client, bucket: str, keys: list[str]) -> list[str]:
    """Delete the objects this run created, so no partial trip is left on S3.

    Only ever called with keys the caller actually wrote. An object that was
    already on S3 is somebody else's — a previous run's, or another dataset
    sharing the prefix — and deleting it would turn a failed upload into data
    loss for a delivery that had already succeeded.

    Returns the keys it could not delete, so the caller can say so instead of
    reporting a clean rollback over a trip that is still half on S3.
    """
    if not keys:
        return []
    left = [key for key in keys if not delete_key(client, bucket, key)]
    if left:
        logger.error(
            "Rolled back %d of %d object(s) on s3://%s; %d could NOT be deleted, so "
            "a PARTIAL trip remains at these keys: %s",
            len(keys) - len(left),
            len(keys),
            bucket,
            len(left),
            ", ".join(left[:5]) + (" ..." if len(left) > 5 else ""),
        )
        return left
    logger.warning(
        "Rolled back this attempt's %d object(s) on s3://%s", len(keys), bucket
    )
    return []
