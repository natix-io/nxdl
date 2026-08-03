from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

from schemas.camera_selection import CAMERA_SELECTION


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env.development",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str = "http://localhost:3002"
    api_key: str = ""
    dataset_id: str = ""
    api_timeout: float = 60.0
    api_max_retries: int = 4
    api_retry_base_delay: float = 1.0
    download_timeout: float = 120.0
    download_chunk_size: int = 8192
    download_max_retries: int = 4
    download_retry_base_delay: float = 1.0
    max_parallel_trips: int = 1
    # Concurrent trip-manifest fetches during validation coverage checks.
    manifest_fetch_workers: int = 10
    # Automatic re-processing passes over trips still failed/partial after the
    # main pass. 0 disables the retry loop.
    max_retry_passes: int = 2
    cache_dir: Path = Path("./cache")
    output_dir: Path = Path("./dataset")
    dry_mode: bool = False

    # Which cameras to keep out of the ones the server manifest returns for a
    # minute. 'all' (default) keeps every camera the manifest has; 'front' and
    # 'front_rear' narrow it. This also relaxes validation's per-grade minimum
    # camera requirement to the selected subset.
    camera_selection: CAMERA_SELECTION = CAMERA_SELECTION.ALL

    # Optional path to a text file of "<trip_id>_<sub_trip>" lines (one per line,
    # '#' comments allowed). When set, only those trips of the dataset are
    # processed; empty means the whole dataset.
    trips_file: str = ""

    # S3 upload (optional). When s3_upload is empty, trips stay local as before.
    # When set to an "s3://bucket/optional/prefix" URI, each finalized trip is
    # validated, uploaded to S3, and then removed locally.
    s3_upload: str = ""
    # AWS credentials are optional: when blank, boto3 falls back to its standard
    # credential chain (env vars, ~/.aws/credentials, IAM role).
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = ""
    aws_endpoint_url: str = ""  # optional, for S3-compatible endpoints (R2/MinIO)
