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
Usage: ./validate.sh --output-dir <dir> [validator args...]

Required:
  --output-dir <dir>    Dataset directory to scan (pass the same directory
                        you gave to ./run.sh --output-dir)

Optional:
  --camera-selection <sel>  Cameras each minute is required to have:
                            all (default) | front | front_rear. Omit to use
                            CAMERA_SELECTION from .env.production; pass the same
                            value that was used for ./run.sh
  -h, --help            Show this help and exit

Any other arguments are passed through to the validator, e.g.:
  ./validate.sh --output-dir <dir> --output ./qa_report.txt
EOF
}

# Consume --output-dir (matching run.sh) and keep everything else for the
# validator (e.g. --output, an explicit --dataset-dir).
PASSTHROUGH=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir=*)
            export OUTPUT_DIR="${1#*=}"
            shift
            ;;
        --output-dir)
            if [[ $# -lt 2 ]]; then
                echo "Error: $1 requires a value." >&2
                usage >&2
                exit 1
            fi
            export OUTPUT_DIR="$2"
            shift 2
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

if [[ -z "${OUTPUT_DIR:-}" ]]; then
    echo "Error: missing required option(s): --output-dir" >&2
    usage >&2
    exit 1
fi

cd "${SCRIPT_DIR}"

# Resolve OUTPUT_DIR to an absolute path (GNU realpath -m isn't available on macOS).
DATASET_DIR="$("${VENV_PYTHON}" -c 'import os, sys
raw, base = sys.argv[1], sys.argv[2]
if not os.path.isabs(raw):
    raw = os.path.join(base, raw)
print(os.path.normpath(os.path.abspath(raw)))' "${OUTPUT_DIR}" "${SCRIPT_DIR}")"

echo "Starting dataset QA..."
echo "Scanning ${DATASET_DIR}"

APP_ENV=production "${VENV_PYTHON}" -m app.validate --dataset-dir "${DATASET_DIR}" ${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}
