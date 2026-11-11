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
Usage: ./inventory.sh --dataset-id <id> --api-key <key> (--output-dir <dir> | --s3 <uri>) [options]

Checks a delivered dataset against its manifest export (a snapshot of what every
trip should contain, produced by NATIX on request) and reports missing, extra,
misplaced, duplicated and held files.

Required:
  --dataset-id <id>         The dataset to check
  --api-key <key>           Your API key (not needed with --manifests <file | s3-uri>)
  --output-dir <dir>        The dataset directory you gave to ./run.sh --output-dir
    or --s3 <uri>           An S3 prefix holding the dataset; repeat for several

Optional:
  --manifests <source>      api (default), a path to an export file, or an s3:// URI
  --trips-file <path>       The same JSON trips file given to ./run.sh
  --camera-selection <sel>  all | front | front_rear; the value used for ./run.sh
  --dry-run                 The download ran with ./run.sh --dry-run
  --out <dir>               Report directory (default ./inventory/<dataset_id>/<exported_at>/)
  -h, --help                Show this help and exit

Relative paths are taken from this folder, as in ./run.sh. Exit codes: 0 the
delivery matches the export, 1 any finding, 2 the export or a target cannot be read.
EOF
}

# Consume the flags run.sh also takes (exported as the env vars the settings read)
# and keep everything else for app.inventory.
MANIFESTS="api"
HAS_TARGET=false
PASSTHROUGH=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --api-key=*)    export API_KEY="${1#*=}"; shift ;;
        --dataset-id=*) export DATASET_ID="${1#*=}"; shift ;;
        --output-dir=*) OUTPUT_DIR="${1#*=}"; HAS_TARGET=true; shift ;;
        --trips-file=*) export TRIPS_FILE="${1#*=}"; shift ;;
        --dry-run)      export DRY_MODE=true; shift ;;
        --api-key|--dataset-id|--output-dir|--trips-file)
            if [[ $# -lt 2 ]]; then
                echo "Error: $1 requires a value." >&2
                usage >&2
                exit 1
            fi
            case "$1" in
                --api-key)    export API_KEY="$2" ;;
                --dataset-id) export DATASET_ID="$2" ;;
                --output-dir) OUTPUT_DIR="$2"; HAS_TARGET=true ;;
                --trips-file) export TRIPS_FILE="$2" ;;
            esac
            shift 2
            ;;
        --manifests=*)
            MANIFESTS="${1#*=}"
            PASSTHROUGH+=("$1")
            shift
            ;;
        --manifests)
            if [[ $# -lt 2 ]]; then
                echo "Error: $1 requires a value." >&2
                usage >&2
                exit 1
            fi
            MANIFESTS="$2"
            PASSTHROUGH+=("$1" "$2")
            shift 2
            ;;
        --s3|--s3=*|--dataset-dir|--dataset-dir=*)
            HAS_TARGET=true
            PASSTHROUGH+=("$1")
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            PASSTHROUGH+=("$1")
            shift
            ;;
    esac
done

MISSING=()
[[ -z "${DATASET_ID:-}" ]] && MISSING+=("--dataset-id")
[[ "${MANIFESTS}" == "api" && -z "${API_KEY:-}" ]] && MISSING+=("--api-key")
[[ "${HAS_TARGET}" == false ]] && MISSING+=("--output-dir or --s3")
if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "Error: missing required option(s): ${MISSING[*]}" >&2
    usage >&2
    exit 1
fi

cd "${SCRIPT_DIR}"

if [[ -n "${OUTPUT_DIR:-}" ]]; then
    [[ "${OUTPUT_DIR}" == /* ]] || OUTPUT_DIR="${SCRIPT_DIR}/${OUTPUT_DIR}"
    PASSTHROUGH+=("--dataset-dir" "${OUTPUT_DIR}")
fi

echo "Starting inventory check..."
echo "Dataset ID: ${DATASET_ID}"

APP_ENV=production "${VENV_PYTHON}" -m app.inventory ${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}
