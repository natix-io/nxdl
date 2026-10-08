"""Read-only virtual filesystem view of a dataset uploaded to S3.

``build_snapshot`` performs ONE paginated ``list_objects_v2`` sweep over the
configured ``s3://bucket/prefix`` and captures every object key and size.
``S3Path`` then exposes that snapshot through the same pathlib surface that
``app.validation`` uses (``iterdir``/``is_dir``/``is_file``/``exists``/
``stat().st_size``/``read_text``/``resolve``), so ``validate_dataset`` runs on
an uploaded dataset unchanged. Everything here is strictly read-only against
S3: only LIST and GET calls, never PUT/DELETE.

``read_text`` is the only per-object GET and validation calls it just for
``dataset_request.json`` (once) and ``trip_insight.json`` (per trip).

TODO (future work): diff per-trip API manifests against the S3 listing for
true completeness checking; retry/backoff on GETs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Iterator

from utils.s3_uploader import get_s3_client, parse_s3_uri

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class S3Stat:
    """Minimal stand-in for ``os.stat_result`` — validation only reads st_size."""

    st_size: int


@dataclass(frozen=True, slots=True)
class S3DatasetSnapshot:
    """Immutable index of one S3 listing sweep.

    Paths are "virtual absolute" posix strings rooted at ``/bucket[/prefix]``
    so rendered issue locations stay meaningful.
    """

    bucket: str
    prefix: str  # normalized, no leading/trailing slashes; may be empty
    root: str  # "/bucket" or "/bucket/prefix"
    files: dict[str, int]  # virtual path -> object size
    dirs: frozenset[str]  # every ancestor dir of a file, incl. root
    children: dict[str, tuple[str, ...]]  # dir virtual path -> sorted child names
    read_bytes: Callable[[str], bytes]  # virtual path -> object body


def snapshot_from_listing(
    bucket: str,
    prefix: str,
    entries: Iterable[tuple[str, int]],
    read_bytes: Callable[[str], bytes],
) -> S3DatasetSnapshot:
    """Build a snapshot from raw ``(key, size)`` listing entries.

    Pure (no network) so tests can fabricate listings. Directory-marker keys
    (ending ``/``), keys outside the prefix, and an object stored at exactly
    the prefix key are skipped. An empty result leaves ``dirs`` empty, so the
    root does not "exist" — validation then reports the dataset as missing.
    """
    root = f"/{bucket}/{prefix}" if prefix else f"/{bucket}"
    required_key_prefix = f"{prefix}/" if prefix else ""

    files: dict[str, int] = {}
    child_sets: dict[str, set[str]] = {}

    for key, size in entries:
        if key.endswith("/"):
            continue  # directory marker
        if required_key_prefix and not key.startswith(required_key_prefix):
            continue  # sibling prefix bleed (e.g. "pilot1-old/" next to "pilot1/")
        virtual = f"/{bucket}/{key}"
        if virtual == root:
            continue  # object stored at exactly the prefix key would alias the root
        # Client state (`.natix/…`, the prefix ownership marker) is not dataset
        # content; skip exactly that reserved top-level segment so the QA layout
        # checks never report it. Only that one: this snapshot also feeds the
        # ownership check (build_present_keys), and dropping every dotted name
        # would let a prefix full of `.abc_1`-style trips look empty and be claimed.
        if key[len(required_key_prefix):].split("/", 1)[0] == ".natix":
            continue
        files[virtual] = size

        # Register every ancestor between the file and the root.
        path = virtual
        while path != root:
            parent, _, name = path.rpartition("/")
            child_sets.setdefault(parent, set()).add(name)
            path = parent

    dirs = frozenset(child_sets)
    children = {parent: tuple(sorted(names)) for parent, names in child_sets.items()}
    return S3DatasetSnapshot(
        bucket=bucket,
        prefix=prefix,
        root=root,
        files=files,
        dirs=dirs,
        children=children,
        read_bytes=read_bytes,
    )


def build_snapshot(uri: str, client=None) -> S3DatasetSnapshot:
    """List ``s3://bucket/prefix`` (one paginated sweep) into a snapshot."""
    bucket, prefix = parse_s3_uri(uri)
    s3 = client if client is not None else get_s3_client()

    # The trailing slash is mandatory: a bare prefix would also match sibling
    # prefixes that merely start with the same characters.
    list_prefix = f"{prefix}/" if prefix else ""
    entries: list[tuple[str, int]] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            entries.append((obj["Key"], obj["Size"]))
    logger.info("Listed %d object(s) under s3://%s/%s", len(entries), bucket, list_prefix)

    key_offset = len(f"/{bucket}/")

    def _read_bytes(virtual_path: str) -> bytes:
        key = virtual_path[key_offset:]
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    return snapshot_from_listing(bucket, prefix, entries, _read_bytes)


def overlay_local_file(
    snapshot: S3DatasetSnapshot, name: str, local_path: Path
) -> S3DatasetSnapshot:
    """Return a snapshot with a root-level entry for ``name`` backed by a local file.

    Used to validate a mid-upload dataset whose ``dataset_request.json`` has
    not landed on S3 yet: the entry appears at the snapshot root but reads
    from the local filesystem. Nothing is written to S3.
    """
    virtual = f"{snapshot.root}/{name}"
    files = dict(snapshot.files)
    files[virtual] = local_path.stat().st_size

    child_sets = {parent: set(names) for parent, names in snapshot.children.items()}
    child_sets.setdefault(snapshot.root, set()).add(name)
    children = {parent: tuple(sorted(names)) for parent, names in child_sets.items()}

    base_read = snapshot.read_bytes

    def _read_bytes(virtual_path: str) -> bytes:
        if virtual_path == virtual:
            return local_path.read_bytes()
        return base_read(virtual_path)

    return replace(
        snapshot,
        files=files,
        dirs=frozenset(children),
        children=children,
        read_bytes=_read_bytes,
    )


def exclude_dirs(
    snapshot: S3DatasetSnapshot, virtual_dirs: Iterable[str]
) -> S3DatasetSnapshot:
    """Return a snapshot without anything under the given virtual directories.

    Used to skip trip folders that belong to other datasets sharing the same
    S3 prefix, so validation only judges the trips of the requested dataset.
    """
    excluded = tuple(f"{directory.rstrip('/')}/" for directory in virtual_dirs)
    if not excluded:
        return snapshot
    key_offset = len(f"/{snapshot.bucket}/")
    entries = [
        (virtual_path[key_offset:], size)
        for virtual_path, size in snapshot.files.items()
        if not virtual_path.startswith(excluded)
    ]
    return snapshot_from_listing(snapshot.bucket, snapshot.prefix, entries, snapshot.read_bytes)


def dataset_root(snapshot: S3DatasetSnapshot) -> "S3Path":
    """The dataset root as an ``S3Path`` carrying the snapshot."""
    return S3Path(snapshot.root, snapshot=snapshot)


class S3Path(PurePosixPath):
    """A ``PurePosixPath`` that answers filesystem queries from a snapshot.

    ``with_segments`` propagates the snapshot, so every derived path
    (``/``, ``.parent``, ``.relative_to``) stays an ``S3Path``. Equality,
    hashing, and ordering are inherited string-based semantics, matching how
    ``app.validation`` sorts paths and uses them in sets.
    """

    __slots__ = ("_snapshot",)

    def __init__(self, *args, snapshot: S3DatasetSnapshot | None = None) -> None:
        super().__init__(*args)
        self._snapshot = snapshot

    def with_segments(self, *segments) -> "S3Path":
        return type(self)(*segments, snapshot=self._snapshot)

    def _snap(self) -> S3DatasetSnapshot:
        if self._snapshot is None:
            raise RuntimeError(f"S3Path {str(self)!r} has no snapshot attached")
        return self._snapshot

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        return self.is_file() or self.is_dir()

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        return self.as_posix() in self._snap().files

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        return self.as_posix() in self._snap().dirs

    def iterdir(self) -> Iterator["S3Path"]:
        snapshot = self._snap()
        names = snapshot.children.get(self.as_posix())
        if names is None:
            raise FileNotFoundError(f"Not a directory in the S3 snapshot: {self}")
        for name in names:
            yield self / name

    def stat(self, *, follow_symlinks: bool = True) -> S3Stat:
        size = self._snap().files.get(self.as_posix())
        if size is None:
            raise FileNotFoundError(f"Not a file in the S3 snapshot: {self}")
        return S3Stat(st_size=size)

    def read_bytes(self) -> bytes:
        snapshot = self._snap()
        virtual = self.as_posix()
        if virtual not in snapshot.files:
            raise FileNotFoundError(f"Not a file in the S3 snapshot: {self}")
        try:
            return snapshot.read_bytes(virtual)
        except OSError:
            raise
        except Exception as exc:
            # botocore ClientError etc. -> OSError so validation's existing
            # error handling treats a failed GET as a file problem, not a crash.
            raise OSError(f"Failed to read S3 object for {self}: {exc}") from exc

    def read_text(self, encoding: str | None = None, errors: str | None = None) -> str:
        return self.read_bytes().decode(encoding or "utf-8", errors or "strict")

    def resolve(self, strict: bool = False) -> "S3Path":
        return self
