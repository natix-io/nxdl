#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv_natix"
# Vendored exiftool lives here. Kept OUTSIDE .venv_natix, which is wiped on
# every install, so a re-install does not re-download it.
TOOLS_DIR="${SCRIPT_DIR}/.natix-tools"
PYTHON_BIN=""

# Userland mode: install no system packages and need no root. Opt-in, never
# automatic — a failed system install tells you to re-run with --user rather
# than silently changing what gets installed.
USER_INSTALL=0
case "${1:-}" in
    --user) USER_INSTALL=1 ;;
    "") ;;
    *) echo "usage: bash install.sh [--user]" >&2; exit 2 ;;
esac

# Pinned exiftool (pure Perl; needs only a system perl). GitHub tags keep old
# versions around — exiftool.org drops them. The digest is checked because a git
# tag is mutable: whoever controls that repo can repoint it, and run.sh then
# executes whatever came down, in a process holding API_KEY and able to write the
# dataset. Re-compute with:
#   curl -fsSL <url> | shasum -a 256
EXIFTOOL_VERSION="13.10"
EXIFTOOL_SHA256="a2e2739435c3b5106d411a538cecb2e138a1bf1b82fd5ad5059a678b4ed2b7da"

if [[ ! -f "${SCRIPT_DIR}/requirements.txt" ]]; then
    echo "Could not find requirements.txt. Please run this script from the downloader-client package."
    exit 1
fi

have() { command -v "$1" >/dev/null 2>&1; }

die() { echo; printf '%s\n' "$@" >&2; exit 1; }

fetch() {  # fetch <url> <dest>
    if have curl; then
        curl -fsSL --retry 3 -o "$2" "$1"
    elif have wget; then
        wget -q -O "$2" "$1"
    else
        echo "Neither curl nor wget is available; cannot download $1" >&2
        return 1
    fi
}

verify_sha256() {  # verify_sha256 <file> <expected>
    local actual=""
    if have shasum; then
        actual="$(shasum -a 256 "$1" | cut -d" " -f1)"
    elif have sha256sum; then
        actual="$(sha256sum "$1" | cut -d" " -f1)"
    else
        echo "Neither shasum nor sha256sum is available; cannot verify $1" >&2
        return 1
    fi
    if [[ "${actual}" != "$2" ]]; then
        echo >&2
        echo "Checksum mismatch for $1" >&2
        echo "  expected: $2" >&2
        echo "  actual:   ${actual}" >&2
        echo "Refusing to use it. Either the download was corrupted, or the pinned" >&2
        echo "upstream artifact changed — do not work around this without checking." >&2
        return 1
    fi
}

# exiftool without root: it is a Perl script, so vendoring it is just an untar.
# Required — app/ds_formatter.py embeds XMP into EVERY video via `exiftool`.
install_vendored_exiftool() {
    local dest="${TOOLS_DIR}/exiftool/exiftool"
    if [[ ! -x "${dest}" ]]; then
        if ! have perl; then
            echo
            echo "exiftool is missing and cannot be vendored: it is a Perl script and this"
            echo "machine has no perl. Install perl (or exiftool) and re-run: bash install.sh"
            return 1
        fi
        local url tarball
        url="https://github.com/exiftool/exiftool/archive/refs/tags/${EXIFTOOL_VERSION}.tar.gz"
        tarball="${TOOLS_DIR}/exiftool.tar.gz"
        echo "Fetching exiftool ${EXIFTOOL_VERSION} into ${TOOLS_DIR}..."
        mkdir -p "${TOOLS_DIR}"
        fetch "${url}" "${tarball}" || { echo "Download failed: ${url}" >&2; return 1; }
        verify_sha256 "${tarball}" "${EXIFTOOL_SHA256}" || {
            rm -f "${tarball}"
            return 1
        }
        rm -rf "${TOOLS_DIR}/exiftool"
        tar -xzf "${tarball}" -C "${TOOLS_DIR}"
        rm -f "${tarball}"
        mv "${TOOLS_DIR}/exiftool-${EXIFTOOL_VERSION}" "${TOOLS_DIR}/exiftool"
        chmod +x "${dest}"
        # Smoke test before anything is allowed to use it.
        "${dest}" -ver >/dev/null 2>&1 || {
            echo "Vendored exiftool does not run on this machine (is perl working?)." >&2
            rm -rf "${TOOLS_DIR}/exiftool"
            return 1
        }
    fi
    # run.sh and validate.sh prepend the venv's bin to PATH, so the subprocess
    # call in utils/exif_tools.py finds it there.
    mkdir -p "${VENV_DIR}/bin"
    ln -sf "${dest}" "${VENV_DIR}/bin/exiftool"
}

UNAME_S="$(uname -s)"

case "${UNAME_S}" in
    Linux)
        DISTRO_ID=""
        DISTRO_LIKE=""
        if [[ -f /etc/os-release ]]; then
            # shellcheck disable=SC1091
            source /etc/os-release
            DISTRO_ID="${ID:-}"
            DISTRO_LIKE="${ID_LIKE:-}"
        fi

        if [[ "${USER_INSTALL}" -eq 0 ]]; then
            SUDO=""
            if [[ "${EUID}" -ne 0 ]]; then
                have sudo || die "This installer needs sudo to install system packages." \
                                 "If python3.12 is already installed, skip them:" \
                                 "  bash install.sh --user"
                SUDO="sudo"
            fi

            if [[ "${DISTRO_ID}" == "ubuntu" || "${DISTRO_ID}" == "debian" || "${DISTRO_LIKE}" == *debian* ]]; then
                echo "Detected ${DISTRO_ID:-Debian-like}. Installing system packages with apt..."
                ${SUDO} apt-get update && \
                ${SUDO} apt-get install -y python3.12 python3.12-venv libimage-exiftool-perl || \
                    die "Could not install system packages with apt (no sudo rights, or a release" \
                        "without python3.12 in its repos, such as Ubuntu 22.04)." \
                        "If python3.12 is already installed, skip the system packages:" \
                        "  bash install.sh --user"
            elif [[ "${DISTRO_ID}" == "rocky" || "${DISTRO_ID}" == "rhel" || "${DISTRO_ID}" == "almalinux" || "${DISTRO_ID}" == "centos" || "${DISTRO_ID}" == "fedora" || "${DISTRO_LIKE}" == *rhel* || "${DISTRO_LIKE}" == *fedora* ]]; then
                # Rocky/RHEL 9 ship python3.12 in AppStream. exiftool itself is
                # EPEL-only, so install a perl interpreter and vendor exiftool below.
                echo "Detected ${DISTRO_ID:-RHEL-like}. Installing system packages with dnf..."
                ${SUDO} dnf install -y python3.12 perl-interpreter || \
                    die "Could not install system packages with dnf (no sudo rights, or no" \
                        "python3.12 in the enabled repos)." \
                        "If python3.12 is already installed, skip the system packages:" \
                        "  bash install.sh --user"
            else
                die "Unsupported Linux distribution: ${DISTRO_ID:-unknown}." \
                    "This installer knows apt (Ubuntu/Debian) and dnf (Rocky/RHEL/Fedora)." \
                    "Install python3.12 and exiftool yourself, then re-run:" \
                    "  bash install.sh --user"
            fi
        fi

        # Use the distro's python, not a conda/pyenv shim earlier on PATH.
        for candidate in /usr/bin/python3.12 /usr/local/bin/python3.12; do
            [[ -x "${candidate}" ]] || continue
            if "${candidate}" -c 'import venv, ensurepip' 2>/dev/null; then
                PYTHON_BIN="${candidate}"
                break
            fi
        done

        if [[ -z "${PYTHON_BIN}" ]]; then
            if [[ "${USER_INSTALL}" -eq 1 ]]; then
                die "--user installs no system packages, and this machine has no usable" \
                    "Python 3.12 at /usr/bin or /usr/local/bin." \
                    "Ask an administrator to install it:" \
                    "  Debian/Ubuntu: sudo apt-get install -y python3.12 python3.12-venv" \
                    "  Rocky/RHEL 9:  sudo dnf install -y python3.12"
            else
                die "Python 3.12 is still not available after installing system packages." \
                    "  Debian/Ubuntu: sudo apt-get install -y python3.12 python3.12-venv" \
                    "  Rocky/RHEL 9:  sudo dnf install -y python3.12"
            fi
        fi

        if ! have exiftool; then
            install_vendored_exiftool || exit 1
        fi
        ;;

    Darwin)
        echo "Detected macOS."
        if [[ "${USER_INSTALL}" -eq 1 ]]; then
            # --user means the same thing on every platform: install no system
            # packages. Homebrew needs no root, but `brew update` and `brew
            # install` still mutate a shared prefix outside this package dir —
            # which is exactly what someone passing --user is avoiding. So take
            # what is already installed, and vendor exiftool into the package.
            echo "--user: using the Python 3.12 already on this machine, installing nothing."
            for candidate in \
                "$(command -v python3.12 2>/dev/null || true)" \
                /opt/homebrew/bin/python3.12 \
                /usr/local/bin/python3.12; do
                [[ -n "${candidate}" && -x "${candidate}" ]] || continue
                if "${candidate}" -c 'import venv, ensurepip' 2>/dev/null; then
                    PYTHON_BIN="${candidate}"
                    break
                fi
            done
            [[ -n "${PYTHON_BIN}" ]] || die \
                "--user installs no system packages, and this machine has no usable" \
                "Python 3.12 on PATH or under /opt/homebrew or /usr/local." \
                "Install it yourself, then re-run:" \
                "  brew install python@3.12"
            if ! have exiftool; then
                install_vendored_exiftool || exit 1
            fi
        else
            if ! have brew; then
                die "Homebrew is required to install dependencies on macOS." \
                    "Install it from https://brew.sh and re-run: bash install.sh" \
                    "Already have Python 3.12 and exiftool? Skip the system packages:" \
                    "  bash install.sh --user"
            fi

            echo "Installing system packages with Homebrew (python and exiftool)..."
            brew update
            brew install python@3.12 exiftool
            PYTHON_BIN="$(brew --prefix python@3.12)/bin/python3.12"
        fi
        ;;

    *)
        echo "Unsupported operating system: ${UNAME_S}"
        echo "This installer supports Linux (Ubuntu/Debian, Rocky/RHEL) and macOS."
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

# The venv was just recreated, so re-link a vendored exiftool into it.
if [[ -x "${TOOLS_DIR}/exiftool/exiftool" ]]; then
    ln -sf "${TOOLS_DIR}/exiftool/exiftool" "${VENV_DIR}/bin/exiftool"
fi

echo "Installing Python dependencies..."
"${VENV_PYTHON}" -m pip install --upgrade pip
"${VENV_PYTHON}" -m pip install -r "${SCRIPT_DIR}/requirements.txt"

if ! PATH="${VENV_DIR}/bin:${PATH}" command -v exiftool >/dev/null 2>&1; then
    echo
    echo "WARNING: exiftool is not available. Downloads will fail when embedding"
    echo "XMP metadata into videos. Install exiftool, or install perl and re-run"
    echo "this installer so it can vendor exiftool, before running ./run.sh."
fi

echo
echo "Installation finished."
echo "Next step: chmod +x run.sh validate.sh  # if needed"
echo "          ./run.sh"
echo "          ./validate.sh  # optional QA after download"
