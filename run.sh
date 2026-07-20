#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="${SCRIPT_DIR}/.venv_natix/bin/python3"

if [[ ! -x "${VENV_PYTHON}" ]]; then
    VENV_PYTHON="${SCRIPT_DIR}/.venv_natix/bin/python"
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "Installation is missing. Please run ./install.sh first."
    exit 1
fi

if [[ ! -f "${SCRIPT_DIR}/.env.production" ]]; then
    echo "Missing .env.production. This package must include that file."
    exit 1
fi

usage() {
    cat <<EOF
Usage: ./run.sh --api-key <key> --dataset-id <id> --output-dir <dir> [options]

Required:
  --api-key <key>            API key used to authenticate requests
  --dataset-id <id>          Dataset ID to download
  --output-dir <dir>         Directory to write the dataset to

Optional:
  --max-parallel-trips <n>   Number of trips to process concurrently (default: 4)
  --dry-run                  Skip actual downloads; only fetch manifests (default: false)
  -h, --help                 Show this help and exit

Other settings (BASE_URL, timeouts, etc.) are read from .env.production.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --api-key=*)             export API_KEY="${1#*=}"; shift ;;
        --dataset-id=*)          export DATASET_ID="${1#*=}"; shift ;;
        --output-dir=*)          export OUTPUT_DIR="${1#*=}"; shift ;;
        --max-parallel-trips=*)  export MAX_PARALLEL_TRIPS="${1#*=}"; shift ;;
        --dry-run)               export DRY_MODE=true; shift ;;
        --api-key|--dataset-id|--output-dir|--max-parallel-trips)
            if [[ $# -lt 2 ]]; then
                echo "Error: $1 requires a value." >&2
                usage >&2
                exit 1
            fi
            case "$1" in
                --api-key)             export API_KEY="$2" ;;
                --dataset-id)          export DATASET_ID="$2" ;;
                --output-dir)          export OUTPUT_DIR="$2" ;;
                --max-parallel-trips)  export MAX_PARALLEL_TRIPS="$2" ;;
            esac
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

MISSING=()
[[ -z "${API_KEY:-}" ]] && MISSING+=("--api-key")
[[ -z "${DATASET_ID:-}" ]] && MISSING+=("--dataset-id")
[[ -z "${OUTPUT_DIR:-}" ]] && MISSING+=("--output-dir")
if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "Error: missing required option(s): ${MISSING[*]}" >&2
    usage >&2
    exit 1
fi

if [[ -n "${MAX_PARALLEL_TRIPS:-}" && ! "${MAX_PARALLEL_TRIPS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --max-parallel-trips must be a positive integer, got: ${MAX_PARALLEL_TRIPS}" >&2
    exit 1
fi

cd "${SCRIPT_DIR}"

# Resolve OUTPUT_DIR to an absolute path (GNU realpath -m isn't available on macOS).
RESOLVED_OUTPUT_DIR="$("${VENV_PYTHON}" -c 'import os, sys
raw, base = sys.argv[1], sys.argv[2]
if not os.path.isabs(raw):
    raw = os.path.join(base, raw)
print(os.path.normpath(os.path.abspath(raw)))' "${OUTPUT_DIR}" "${SCRIPT_DIR}")"
export OUTPUT_DIR="${RESOLVED_OUTPUT_DIR}"

echo "Starting dataset download..."
echo "Dataset ID: ${DATASET_ID}"
echo "Output will be written to ${RESOLVED_OUTPUT_DIR}"
if [[ "${DRY_MODE:-}" == "true" ]]; then
    echo "Dry run: no files will be downloaded."
fi

APP_ENV=production "${VENV_PYTHON}" -m app.main
