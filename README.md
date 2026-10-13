# Downloader Client

Client script to fetch dataset from the downloader service, download all assets, and optionally run validation.


## Requirements

Supported on Ubuntu/Debian, Rocky/RHEL 9, macOS, and **WSL on Windows** (use the WSL terminal, not CMD/PowerShell). Python 3.12 is installed automatically by the installer if it isn't already present.

## Install

Open a terminal in this project folder and run:

```bash
bash install.sh
```

This installs Python 3.12 (if needed), the `exiftool` system dependency, and all Python packages into a local virtual environment.

**No sudo?** Run `bash install.sh --user`. It installs no system packages: it uses the `python3.12`
already on the machine (which must be present) and downloads `exiftool` into `.natix-tools/` inside
this folder. Nothing is written outside the project directory.

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

To download only some trips of the dataset — and, per trip, optionally only some minutes — pass a JSON trips file:

```bash
./run.sh --api-key <YOUR_API_KEY> --dataset-id <YOUR_DATASET_ID> --output-dir /path/to/dataset --trips-file ./trips.json
```

```json
{
  "trips": [
    { "trip_id": "ea114785-55c7-44b1-b66d-5c5a07ea37f6", "sub_trip": 1 },
    { "trip_id": "598b022f-cf12-4c8f-af49-18218883420d", "sub_trip": 1,
      "minutes": ["12-14-02", "12-15-03"] },
    { "trip_id": "23286d0c-84ae-4a3d-8ca7-7239d615cb05", "sub_trip": 1,
      "health_grade": 150, "minutes": ["06-33-22"] }
  ]
}
```

- `trip_id` and `sub_trip` are required. `health_grade` is optional and defaults to `500`; a dataset can hold the same trip at several grades, and each grade is a separate trip with its own minutes.
- `minutes` is optional: leave it out to download the whole trip. When present it must be a non-empty list of minute folder names (`HH-MM-SS`, local time — exactly as they appear in the dataset), and only those minutes are downloaded. The dataset layout is unchanged; the trip simply has fewer minute folders, and its `trip_insight.json` only describes the minutes on disk, trip-level stats included (see [Dataset layout](#dataset-layout)).
- A trip not in the dataset, or a minute not in its trip, is reported and skipped. A trip whose listed minutes all fail to match is skipped entirely.
- In an SEI-segment dataset, list the trip itself, not a segment. Its `minutes` apply to the whole trip first, then each segment keeps the selected minutes it contains; a segment left with none is not delivered, and the letters are never reassigned (selecting only minutes of segment B delivers `abc_1_B` alone).
- Pass the same `--trips-file` to `./validate.sh` so the QA check expects only the selected content.
- Changing the `minutes` of a trip that an earlier run already completed has no effect on a re-run: delete that trip's folder first.

To get an overview of the safety-critical events in what was downloaded, add `--create-manifest`:

```bash
./run.sh --api-key <YOUR_API_KEY> --dataset-id <YOUR_DATASET_ID> --output-dir /path/to/dataset --create-manifest
```

This writes `safety_critical_events_manifest.json` into the dataset root after the download: for every trip, the events of each downloaded minute folder, nested the same way as the dataset (`country / state / trip / minutes`), plus a summary. It covers exactly the minutes on disk, so with `--trips-file` it describes the selected minutes only.

Run `./run.sh --help` for the full usage.

### Dataset layout

Each trip is a folder `<country>/[<state>/]<trip_id>_<sub_trip>[_<health_grade>][_<segment>]`; the state level is used only for the United States. `_<health_grade>` is left out for grade 500, so `abc_1` is grade 500 and `abc_1_150` is grade 150.

`_<segment>` appears only in datasets requested as SEI segments (`seiData: "with_and_chop"`). Such a dataset delivers every run of consecutive minutes in which each camera has SEI as a separate folder: `abc_1_A`, `abc_1_B`, … (`abc_1_150_A` at grade 150), lettered in time order. Each segment folder is a complete trip of its own: only that segment's minute folders, its own `fixed_metadata`, and a `trip_insight.json` for those minutes only. A segmented trip has no `abc_1` folder, and a trip without any such run is skipped and logged, not reported as a failure.

When a folder holds only part of a trip (a segment, or a minute selection from `--trips-file`), the trip-level fields of `trip_insight.json` → `base_insights` describe that folder, not the whole trip: `startEpochMs` / `endEpochMs` / `duration` from its minutes, and `estimatedDistance` (km), `estimatedAverageSpeed` (km/h) and `firstLocation` from the GPS in its telemetry. The GPS fields are `null` when the folder has no GPS telemetry.

### Re-running and running several datasets

Give each dataset its **own** `--output-dir`; the client refuses to put a second dataset into a directory that already holds one. Several datasets can download at the same time into their own directories.

**To retry a dataset**, run the same command again once the previous run has finished: completed trips are skipped and only what is missing is fetched. The same dataset cannot run twice into the same output dir at once; the second run says so and exits.

`./run.sh` exits non-zero if any requested trip was not completed (the log names them), so a wrapper script can tell a partial run from a finished one. Trips still waiting on a long-term-storage restore are the exception — a documented "re-run later", not a failure — and still exit 0.

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

Likewise, if you downloaded with `--trips-file`, pass the same file here:

```bash
./validate.sh --output-dir /path/to/dataset --trips-file ./trips.json
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
- **The run refuses to start on a network filesystem** (Lustre mounted without `-o flock`, NFS without lockd): the client cannot tell whether the same dataset is already running into this directory, and two such runs would corrupt each other's progress, so it stops rather than guess. If you are certain no other run is active, re-run with `ALLOW_UNLOCKED_RUNS=true` in front of the command.
