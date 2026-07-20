#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv_natix"
PYTHON_BIN=""

if [[ ! -f "${SCRIPT_DIR}/requirements.txt" ]]; then
    echo "Could not find requirements.txt. Please run this script from the downloader-client package."
    exit 1
fi

UNAME_S="$(uname -s)"

case "${UNAME_S}" in
    Linux)
        if [[ ! -f /etc/os-release ]]; then
            echo "Unsupported Linux distribution. This installer currently supports Ubuntu/Debian only."
            exit 1
        fi

        source /etc/os-release

        if [[ "${ID:-}" != "ubuntu" && "${ID:-}" != "debian" && "${ID_LIKE:-}" != *debian* ]]; then
            echo "Unsupported Linux distribution. This installer currently supports Ubuntu/Debian only."
            exit 1
        fi

        if [[ "${EUID}" -eq 0 ]]; then
            SUDO=""
        elif command -v sudo >/dev/null 2>&1; then
            SUDO="sudo"
        else
            echo "This installer needs sudo access to install system packages."
            exit 1
        fi

        echo "Detected Ubuntu/Debian. Installing system packages with apt..."
        ${SUDO} apt-get update
        ${SUDO} apt-get install -y python3.12 python3.12-venv libimage-exiftool-perl
        # Use apt's Python, not conda/pyenv shims earlier on PATH.
        if [[ -x /usr/bin/python3.12 ]]; then
            PYTHON_BIN="/usr/bin/python3.12"
        elif [[ -x /usr/bin/python3 ]]; then
            PYTHON_BIN="/usr/bin/python3"
        else
            PYTHON_BIN="$(command -v python3)"
        fi
        ;;

    Darwin)
        echo "Detected macOS."
        if ! command -v brew >/dev/null 2>&1; then
            echo "Homebrew is required to install dependencies on macOS."
            echo "Please install Homebrew from https://brew.sh and then re-run: bash install.sh"
            exit 1
        fi

        echo "Installing system packages with Homebrew (python and exiftool)..."
        brew update
        brew install python@3.12 exiftool
        PYTHON_BIN="$(brew --prefix python@3.12)/bin/python3.12"
        ;;

    *)
        echo "Unsupported operating system: ${UNAME_S}"
        echo "This installer supports Ubuntu/Debian Linux and macOS."
        exit 1
        ;;
esac

if [[ -z "${PYTHON_BIN}" || ! -x "${PYTHON_BIN}" ]]; then
    echo "A compatible Python interpreter is not available after installation."
    exit 1
fi

if ! "${PYTHON_BIN}" -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 1)'; then
    detected="$("${PYTHON_BIN}" --version 2>&1)"
    echo
    echo "Python 3.12 is required, but the installer found a different version."
    echo
    echo "Detected: ${detected}"
    echo "Interpreter: ${PYTHON_BIN}"
    echo
    echo "Install Python 3.12, then run: bash install.sh"
    echo "Need help? Contact whoever sent you this downloader package."
    exit 1
fi

echo "Creating Python environment..."
rm -rf "${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"

VENV_PYTHON="${VENV_DIR}/bin/python3"
if [[ ! -x "${VENV_PYTHON}" ]]; then
    VENV_PYTHON="${VENV_DIR}/bin/python"
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "Virtual environment creation failed: no Python executable found in ${VENV_DIR}/bin."
    exit 1
fi

echo "Installing Python dependencies..."
"${VENV_PYTHON}" -m pip install --upgrade pip
"${VENV_PYTHON}" -m pip install -r "${SCRIPT_DIR}/requirements.txt"

echo
echo "Installation finished."
echo "Next step: chmod +x run.sh validate.sh  # if needed"
echo "          ./run.sh"
echo "          ./validate.sh  # optional QA after download"
