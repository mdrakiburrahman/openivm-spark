#!/usr/bin/env bash
# Shared helpers for the Docker-first host bootstrap.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo "[common.sh] FATAL: source this file, do not execute it" >&2
    exit 1
fi

CMN_NODE_VERSION="24.11.1"
CMN_NPM_VERSION="11.6.2"
CMN_NODE_ROOT="${CMN_NODE_ROOT:-/opt/openivm-node}"

cmn_log() {
    local prefix="$1"
    shift
    echo "[$prefix] $*"
}

cmn_has() {
    command -v "$1" >/dev/null 2>&1
}

cmn_require() {
    local command="$1"
    local hint="${2:-Install '${command}' and rerun the bootstrap.}"
    if cmn_has "${command}"; then
        return 0
    fi
    cmn_log bootstrap "FATAL: required command '${command}' is missing. ${hint}" >&2
    return 1
}

cmn_require_docker() {
    cmn_require docker "Install Docker Engine in WSL before running this script."
    if ! docker info >/dev/null 2>&1; then
        cmn_log bootstrap "FATAL: Docker is installed, but the daemon is not reachable." >&2
        return 1
    fi
    cmn_log bootstrap "Docker ready ($(docker --version))"
}

cmn_node_archive() {
    case "$(uname -m)" in
        x86_64 | amd64)
            printf 'node-v%s-linux-x64.tar.xz|%s\n' \
                "${CMN_NODE_VERSION}" \
                "60e3b0a8500819514aca603487c254298cd776de0698d3cd08f11dba5b8289a8"
            ;;
        aarch64 | arm64)
            printf 'node-v%s-linux-arm64.tar.xz|%s\n' \
                "${CMN_NODE_VERSION}" \
                "6b0863fb9f627bf4a6c5948dce1de4398174a2e05dbe717503d828e211ca01f0"
            ;;
        *)
            cmn_log bootstrap "FATAL: unsupported host architecture '$(uname -m)'." >&2
            return 1
            ;;
    esac
}

cmn_ensure_download_tools() {
    local missing=()
    local command
    for command in curl sha256sum tar xz; do
        cmn_has "${command}" || missing+=("${command}")
    done
    if (( ${#missing[@]} == 0 )); then
        return 0
    fi

    cmn_require sudo "The bootstrap needs sudo once to install download utilities and Node.js."
    cmn_require apt-get "Use an Ubuntu/Debian WSL distribution or install ${missing[*]} manually."
    cmn_log bootstrap "Installing host bootstrap utilities: ${missing[*]}"
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl xz-utils >/dev/null
}

cmn_ensure_node() {
    if cmn_has node &&
        [[ "$(node --version 2>/dev/null)" == "v${CMN_NODE_VERSION}" ]] &&
        cmn_has npm &&
        [[ "$(npm --version 2>/dev/null)" == "${CMN_NPM_VERSION}" ]]; then
        cmn_log bootstrap "Node ${CMN_NODE_VERSION} and npm ${CMN_NPM_VERSION} already installed"
        return 0
    fi

    cmn_ensure_download_tools
    cmn_require sudo "The bootstrap installs the pinned Node.js runtime under ${CMN_NODE_ROOT}."

    local archive_info archive sha256 extracted tmp
    archive_info="$(cmn_node_archive)"
    archive="${archive_info%%|*}"
    sha256="${archive_info##*|}"
    extracted="${archive%.tar.xz}"
    tmp="$(mktemp -d)"

    cmn_log bootstrap "Installing Node ${CMN_NODE_VERSION} and npm ${CMN_NPM_VERSION}"
    curl -fsSL "https://nodejs.org/dist/v${CMN_NODE_VERSION}/${archive}" -o "${tmp}/${archive}"
    echo "${sha256}  ${tmp}/${archive}" | sha256sum -c -

    sudo mkdir -p "${CMN_NODE_ROOT}"
    sudo rm -rf "${CMN_NODE_ROOT:?}/${extracted}"
    sudo tar -xJf "${tmp}/${archive}" -C "${CMN_NODE_ROOT}"
    sudo ln -sfn "${CMN_NODE_ROOT}/${extracted}" "${CMN_NODE_ROOT}/current"

    local executable
    for executable in node npm npx corepack; do
        sudo ln -sfn "${CMN_NODE_ROOT}/current/bin/${executable}" "/usr/local/bin/${executable}"
    done
    rm -rf "${tmp}"
    hash -r

    if [[ "$(node --version)" != "v${CMN_NODE_VERSION}" ]] ||
        [[ "$(npm --version)" != "${CMN_NPM_VERSION}" ]]; then
        cmn_log bootstrap "FATAL: installed Node/npm versions do not match the repository contract." >&2
        return 1
    fi
}

cmn_npm_ci() {
    local repository_root="$1"
    cmn_log bootstrap "Installing locked repository tooling"
    (
        cd "${repository_root}"
        npm ci
    )
}
