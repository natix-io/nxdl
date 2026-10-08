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
    # 64 KiB, measured: the read+write loop alone caps at ~1.5 GB/s at 8 KiB and
    # ~3.0 GB/s from 64 KiB up (80 MB body over loopback, so no network in the
    # way). An 80 MB camera minute is 10k iterations at 8 KiB and 1.2k at 64 KiB.
    # Larger chunks measured no faster and only add per-connection buffer.
    download_chunk_size: int = 65536
    download_max_retries: int = 4
    download_retry_base_delay: float = 1.0
    max_parallel_trips: int = 1
    # Files fetched at once WITHIN one trip. A trip is ~7-10 small objects and a
    # worker spends most of its wall time post-processing, so fetching them one
    # at a time leaves the connection idle. 1 restores the old serial behaviour.
    #
    # 2 by default, from measurement rather than taste: over 6 real trips it was
    # the fastest point at every trip count tried, and at max_parallel_trips=1 it
    # already captures the whole win (39.5s vs 39.4s for 4, against 63.3s
    # serial). Higher values only add streams to a link that is usually already
    # saturated by max_parallel_trips. Raise it on a fat pipe.
    #
    # Re-measured 2026-09-17 against prod, 6 four-camera trips per run at
    # max_parallel_trips=2: 13.3 MB/s serial, 24.8 MB/s at 2, 22.9 MB/s at 4.
    # Note it does nothing for a single-file trip (a front-only request with no
    # telemetry/SEI output is one .mp4 per trip) — there max_parallel_trips is
    # the only lever.
    max_parallel_files: int = 2
    # Concurrent trip-manifest fetches during validation coverage checks.
    manifest_fetch_workers: int = 10
    # Trips validated concurrently by `python -m app.validate`. QA is a few dozen
    # metadata calls per trip and nothing else; on a network filesystem each one
    # waits milliseconds, so the scan is latency-bound and threads recover most
    # of it. 1 restores the serial pass.
    qa_workers: int = 8
    # How many downloaded trips `python -m app.validate` checks file-by-file
    # against their trip manifests. That check is one API request per trip, so on
    # a 47k-trip dataset it dominates the QA run.
    #
    # 0 (every trip) by default, and sampling is opt-in. A sample cannot be the
    # default while this check decides the verdict: with 501 present trips and
    # the incomplete one outside the sample, QA went from FAIL to PASS. The cost
    # is addressed by asking for manifests in batches, not by checking fewer.
    manifest_file_sample: int = 0
    # Automatic re-processing passes over trips still failed/partial after the
    # main pass. 0 disables the retry loop.
    max_retry_passes: int = 2
    cache_dir: Path = Path("./cache")
    output_dir: Path = Path("./dataset")
    dry_mode: bool = False
    # Allow a run to continue when the filesystem cannot provide an exclusive
    # lock (Lustre without -o flock, NFS without lockd). Off by default: two runs
    # of one dataset in one output dir share a ledger and a cache and corrupt
    # each other, so proceeding without the lock is an explicit decision.
    allow_unlocked_runs: bool = False
    # Write <output_dir>/safety_critical_events_manifest.json at the end of a run
    # (local downloads only — with s3_upload the files are gone). See
    # app.event_manifest.
    create_manifest: bool = False

    # Which cameras to keep out of the ones the server manifest returns for a
    # minute. 'all' (default) keeps every camera the manifest has; 'front' and
    # 'front_rear' narrow it. This also relaxes validation's per-grade minimum
    # camera requirement to the selected subset.
    camera_selection: CAMERA_SELECTION = CAMERA_SELECTION.ALL

    # Optional path to a JSON file selecting a subset of the dataset:
    #   {"trips": [{"trip_id": "<uuid>", "sub_trip": 1,
    #               "health_grade": 500,            # optional, default 500
    #               "minutes": ["12-14-02", ...]}]} # optional, default = whole trip
    # Only the listed trips (and, per trip, only the listed minute folders) are
    # downloaded; validate uses the same file. Empty means the whole dataset.
    # See utils.trip_selection.
    trips_file: str = ""

    # S3 upload (optional). When s3_upload is empty, trips stay local as before.
    # When set to an "s3://bucket/optional/prefix" URI, each finalized trip is
    # validated, uploaded to S3, and then removed locally.
    s3_upload: str = ""
    # Escape hatch for the one-dataset-per-prefix guard: claim a prefix that already
    # holds objects but records no owner and carries no dataset_request.json to
    # prove whose they are. Off by default — that is exactly the state in which two
    # datasets get mixed. Turn it on for one run to adopt a prefix you know is this
    # dataset's (an interrupted delivery from before the guard existed).
    s3_adopt_prefix: bool = False
    # AWS credentials are optional: when blank, boto3 falls back to its standard
    # credential chain (env vars, ~/.aws/credentials, IAM role).
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = ""
    aws_endpoint_url: str = ""  # optional, for S3-compatible endpoints (R2/MinIO)
