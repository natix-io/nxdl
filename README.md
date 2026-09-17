# Downloader Client

Client script to fetch dataset from the downloader service, download all assets, and optionally run validation.


## Requirements

Supported on Ubuntu/Debian, macOS, and **WSL on Windows** (use the WSL terminal, not CMD/PowerShell). Python 3.12 is installed automatically by the installer if it isn't already present.

## Install

Open a terminal in this project folder and run:

```bash
bash install.sh
```

This installs Python 3.12 (if needed), the `exiftool` system dependency, and all Python packages into a local virtual environment.

Then make the run scripts executable (once):

```bash
chmod +x run.sh validate.sh
```

## Run (download a dataset)

`--api-key`, `--dataset-id`, and `--output-dir` are all required:

```bash
./run.sh --api-key <YOUR_API_KEY> --dataset-id <YOUR_DATASET_ID> --output-dir /path/to/dataset
```

For larger datasets, optionally raise the number of trips downloaded concurrently, default is 4:

```bash
./run.sh --api-key <YOUR_API_KEY> --dataset-id <YOUR_DATASET_ID> --output-dir /path/to/dataset --max-parallel-trips 4
```

To do a dry run (fetch manifests without downloading any files):

```bash
./run.sh --api-key <YOUR_API_KEY> --dataset-id <YOUR_DATASET_ID> --output-dir /path/to/dataset --dry-run
```

To keep only a subset of cameras (`all` (default), `front`, or `front_rear`):

```bash
./run.sh --api-key <YOUR_API_KEY> --dataset-id <YOUR_DATASET_ID> --output-dir /path/to/dataset --camera-selection front
```

If you always want the same subset, set `CAMERA_SELECTION=front` in `.env.production` instead — both `./run.sh` and `./validate.sh` read it, so they stay in agreement.

Run `./run.sh --help` for the full usage.

## Validate

`--output-dir` is required and must match what you passed to `./run.sh`:

```bash
./validate.sh --output-dir /path/to/dataset
```

To save a full report with every trip listed (including `PASS`):

```bash
./validate.sh --output-dir /path/to/dataset --output /path/to/dataset/qa_report.txt
```

If you downloaded with `--camera-selection`, pass the same value here so the validator doesn't report the cameras you deliberately skipped as missing (not needed if the value comes from `.env.production`):

```bash
./validate.sh --output-dir /path/to/dataset --camera-selection front
```

The validator scans the local dataset folder and prints:

- base stats for trips, minute folders, camera folders, and generated artifacts
- hard failures for missing or empty required files, layout issues, and request/feature mismatches
- warnings for structure or consistency issues that do not make the dataset unusable


## Troubleshooting

- **`bash\r: Permission denied` when running `install.sh`**: the package likely has Windows line endings. Run `bash install.sh` directly (not `./install.sh`), or fix the line endings first:
  ```bash
  sed -i 's/\r$//' install.sh run.sh validate.sh
  ```
- **`Installation is missing. Please run ./install.sh first.`**: run `bash install.sh` before `./run.sh` or `./validate.sh`.
- **`N file(s) across M trip(s) are still being prepared and will take a while.`**: part of the dataset sits in long-term storage and is being restored for you. The download itself is fine — re-run the exact same command after the time given in the message (roughly 12h, or ~48h for a bulk restore) and the client fetches only what is still missing. Everything already downloaded is kept.
- **`N trip(s) skipped: M file(s) are archived and not available for download in this dataset.`**: those files are not being restored. Contact the NATIX team to have them included, then re-run the same command.
