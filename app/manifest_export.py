"""Open a dataset manifest export (gzipped JSON lines) from a file, S3 or the API.

Line 1 is a header ``{dataset_id, exported_at, trips, s3_locations, output}``; every
further line is one ``TripManifest`` whose links are unsigned (no query, no
``expires_at``). Manifests are streamed, never held all at once. Ships to customers.
"""

import gzip
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any
from urllib.parse import quote

from pydantic import BaseModel, ValidationError

from app.trip_downloader import api_headers
from schemas import DatasetOutput, TripManifest
from utils.http_retry import get_with_retry
from utils.s3_uploader import get_s3_client, parse_s3_uri
from utils.settings import settings

_GZIP_MAGIC = b"\x1f\x8b"


class ExportError(Exception):
    """The export cannot be read, or is not the requested dataset's."""


class _Header(BaseModel):
    dataset_id: str
    exported_at: str
    trips: int
    s3_locations: bool = False
    output: DatasetOutput


def _open_lines(path: Path) -> IO[str]:
    with path.open("rb") as f:
        magic = f.read(2)
    if magic == _GZIP_MAGIC:
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


@dataclass
class ManifestExport:
    dataset_id: str
    exported_at: str
    trips: int
    output: DatasetOutput
    s3_locations: bool
    path: Path

    def manifests(self) -> Iterator[TripManifest]:
        """Stream the trip manifests; re-reads the file on every call."""
        try:
            with _open_lines(self.path) as f:
                for n, line in enumerate(f, start=1):
                    if n == 1 or not line.strip():
                        continue
                    try:
                        yield TripManifest.model_validate_json(line)
                    except ValidationError as exc:
                        raise ExportError(f"bad manifest on line {n}: {exc}") from exc
        except (OSError, EOFError, UnicodeDecodeError) as exc:
            raise ExportError(f"cannot read the export {self.path}: {exc}") from exc


def fetch_export_info(api_key: str, dataset_id: str) -> dict[str, Any]:
    """The API's answer: ``{export_id, exported_at, trips, s3_locations, url, expires_at}``."""
    url = f"{settings.base_url}/v1/dataset/manifests/export?dataset_id={quote(dataset_id)}"
    try:
        return get_with_retry(
            url, headers=api_headers(api_key), timeout=settings.api_timeout
        ).json()
    except Exception as exc:
        raise ExportError(f"cannot fetch the export info: {exc}") from exc


def _read_header(path: Path, dataset_id: str) -> ManifestExport:
    try:
        with _open_lines(path) as f:
            first = f.readline()
    except (OSError, EOFError, UnicodeDecodeError) as exc:
        raise ExportError(f"cannot read the export {path}: {exc}") from exc
    if not first.strip():
        raise ExportError(f"the export {path} is empty")
    try:
        header = _Header.model_validate(json.loads(first))
    except (ValueError, ValidationError) as exc:
        raise ExportError(f"bad export header: {exc}") from exc
    if header.dataset_id != dataset_id:
        raise ExportError(
            f"export is for dataset {header.dataset_id}, not {dataset_id}"
        )
    return ManifestExport(
        dataset_id=header.dataset_id,
        exported_at=header.exported_at,
        trips=header.trips,
        output=header.output,
        s3_locations=header.s3_locations,
        path=path,
    )


def _download(source: str, api_key: str, dataset_id: str, tmp: Path) -> None:
    try:
        if source == "api":
            info = fetch_export_info(api_key, dataset_id)
            url = info.get("url")
            if not url:
                raise ExportError("the export info has no url")
            # Presigned URL: no API headers.
            body = get_with_retry(
                url, headers={}, timeout=settings.download_timeout
            ).content
            tmp.write_bytes(body)
        else:
            bucket, key = parse_s3_uri(source)
            get_s3_client().download_file(bucket, key, str(tmp))
    except ExportError:
        raise
    except Exception as exc:
        raise ExportError(f"cannot fetch the export from {source}: {exc}") from exc


@contextmanager
def open_export(
    source: str, *, api_key: str, dataset_id: str
) -> Iterator[ManifestExport]:
    """``source`` is ``"api"``, an ``s3://`` URI or a local path.

    api / s3 sources are downloaded to a temp file removed on exit.
    """
    if source != "api" and not source.startswith("s3://"):
        yield _read_header(Path(source), dataset_id)
        return
    fd, name = tempfile.mkstemp(suffix=".jsonl.gz", prefix="nxdl-export-")
    os.close(fd)
    tmp = Path(name)
    try:
        _download(source, api_key, dataset_id, tmp)
        yield _read_header(tmp, dataset_id)
    finally:
        tmp.unlink(missing_ok=True)
