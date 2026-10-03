#!/usr/bin/env bash
# Shared helpers for the Docker-first host bootstrap.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo "[common.sh] FATAL: source this file, do not execute it" >&2
    exit 1
fi

CMN_NODE_VERSION="24.11.1"
CMN_NPM_VERSION="11.6.2"
CMN_NODE_ROOT="${CMN_NODE_ROOT:-/opt/openivm-node}"
CMN_DOCKER_VERSION="${CMN_DOCKER_VERSION:-5:27.5.1-1~ubuntu.24.04~noble}"
CMN_DOCKER_MAX_CONCURRENT_DOWNLOADS="${CMN_DOCKER_MAX_CONCURRENT_DOWNLOADS:-32}"
CMN_DOCKER_MAX_CONCURRENT_UPLOADS="${CMN_DOCKER_MAX_CONCURRENT_UPLOADS:-32}"
CMN_APT_UPDATED=0

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

cmn_apt_update() {
    if [[ "${CMN_APT_UPDATED}" == "1" ]]; then
        return 0
    fi
    cmn_require sudo "The bootstrap needs sudo to install missing host prerequisites."
    cmn_require apt-get "Use an Ubuntu/Debian WSL distribution."
    sudo apt-get update -qq
    CMN_APT_UPDATED=1
}

cmn_apt_install() {
    cmn_apt_update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@" >/dev/null
}

cmn_strip_windows_paths() {
    echo "${PATH}" | tr ':' '\n' | grep -v '^/mnt/c' | tr '\n' ':' | sed 's/:$//'
}

cmn_ensure_host_packages() {
    cmn_log bootstrap "Installing or validating host prerequisites"
    cmn_apt_install ca-certificates curl git gnupg jq tar xz-utils
}

cmn_docker_cli_complete() {
    cmn_has docker &&
        docker compose version >/dev/null 2>&1 &&
        docker buildx version >/dev/null 2>&1
}

cmn_require_docker() {
    cmn_require docker "Run contrib/bootstrap-dev-env.sh to install Docker Engine."
    docker compose version >/dev/null 2>&1 ||
        {
            cmn_log bootstrap "FATAL: Docker Compose is not available." >&2
            return 1
        }
    docker buildx version >/dev/null 2>&1 ||
        {
            cmn_log bootstrap "FATAL: Docker Buildx is not available." >&2
            return 1
        }
    if ! docker info >/dev/null 2>&1; then
        cmn_log bootstrap "FATAL: Docker is installed, but the daemon is not reachable." >&2
        return 1
    fi
    cmn_log bootstrap "Docker ready ($(docker --version))"
}

cmn_ensure_docker_repository() {
    cmn_require dpkg "The Docker repository setup requires dpkg."
    [[ -r /etc/os-release ]] ||
        {
            cmn_log bootstrap "FATAL: /etc/os-release is unavailable." >&2
            return 1
        }

    local version_codename distribution architecture repository
    # shellcheck disable=SC1091
    source /etc/os-release
    distribution="${ID:-}"
    version_codename="${VERSION_CODENAME:-}"
    architecture="$(dpkg --print-architecture)"
    if [[ "${distribution}" != "ubuntu" || -z "${version_codename}" ]]; then
        cmn_log bootstrap "FATAL: Docker bootstrap supports Ubuntu WSL; found ${distribution:-unknown}." >&2
        return 1
    fi

    sudo install -m 0755 -d /etc/apt/keyrings
    if [[ ! -s /etc/apt/keyrings/docker.asc ]]; then
        cmn_log bootstrap "Installing Docker apt signing key"
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg |
            sudo tee /etc/apt/keyrings/docker.asc >/dev/null
        sudo chmod 0644 /etc/apt/keyrings/docker.asc
    fi

    repository="deb [arch=${architecture} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${version_codename} stable"
    if [[ ! -f /etc/apt/sources.list.d/docker.list ]] ||
        [[ "$(cat /etc/apt/sources.list.d/docker.list)" != "${repository}" ]]; then
        echo "${repository}" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
        CMN_APT_UPDATED=0
    fi
    cmn_apt_update
}

cmn_install_docker_packages() {
    cmn_ensure_docker_repository

    local -a packages=(docker-buildx-plugin docker-compose-plugin)
    if ! cmn_has docker; then
        if apt-cache madison docker-ce | awk '{print $3}' | grep -Fxq "${CMN_DOCKER_VERSION}"; then
            packages=(
                "docker-ce=${CMN_DOCKER_VERSION}"
                "docker-ce-cli=${CMN_DOCKER_VERSION}"
                containerd.io
                "${packages[@]}"
            )
        else
            cmn_log bootstrap "Pinned Docker ${CMN_DOCKER_VERSION} is unavailable; installing the repository version"
            packages=(docker-ce docker-ce-cli containerd.io "${packages[@]}")
        fi
    fi

    cmn_log bootstrap "Installing missing Docker components"
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        --allow-downgrades "${packages[@]}" >/dev/null
}

cmn_restart_docker() {
    if cmn_has systemctl && sudo systemctl restart docker >/dev/null 2>&1; then
        return 0
    fi
    sudo service docker restart >/dev/null
}

cmn_start_docker() {
    if docker info >/dev/null 2>&1; then
        return 0
    fi
    if cmn_has systemctl && sudo systemctl enable --now docker >/dev/null 2>&1; then
        return 0
    fi
    sudo service docker start >/dev/null
}

cmn_configure_docker_daemon() {
    local target="/etc/docker/daemon.json"
    local current="{}"
    local normalized_current desired

    sudo mkdir -p /etc/docker
    if [[ -s "${target}" ]]; then
        current="$(sudo cat "${target}")"
    fi
    normalized_current="$(printf '%s\n' "${current}" | jq -S .)" ||
        {
            cmn_log bootstrap "FATAL: ${target} is not valid JSON." >&2
            return 1
        }
    desired="$(
        printf '%s\n' "${current}" |
            jq -S \
                --argjson downloads "${CMN_DOCKER_MAX_CONCURRENT_DOWNLOADS}" \
                --argjson uploads "${CMN_DOCKER_MAX_CONCURRENT_UPLOADS}" \
                '
                .["max-concurrent-downloads"] = $downloads
                | .["max-concurrent-uploads"] = $uploads
                | .["default-ulimits"].nofile = {"Name": "nofile", "Hard": 1048576, "Soft": 1048576}
                | .["default-ulimits"].nproc = {"Name": "nproc", "Hard": 1048576, "Soft": 1048576}
                | .["default-ulimits"].memlock = {"Name": "memlock", "Hard": -1, "Soft": -1}
                | .features.buildkit = true
                | .["log-driver"] = "json-file"
                | .["log-opts"]["max-size"] = "50m"
                | .["log-opts"]["max-file"] = "3"
                '
    )"

    if [[ "${normalized_current}" != "${desired}" ]]; then
        cmn_log bootstrap "Updating ${target}"
        printf '%s\n' "${desired}" | sudo tee "${target}" >/dev/null
        cmn_restart_docker
    fi
}

cmn_enable_docker_access() {
    local user="${SUDO_USER:-${USER}}"
    sudo groupadd --force docker
    if [[ "${user}" != "root" ]]; then
        sudo usermod --append --groups docker "${user}"
    fi
    if [[ -S /var/run/docker.sock ]]; then
        sudo chmod 666 /var/run/docker.sock
    fi
}

cmn_ensure_docker() {
    if cmn_docker_cli_complete; then
        cmn_log bootstrap "Docker CLI, Buildx, and Compose already installed"
    else
        cmn_install_docker_packages
        hash -r
    fi

    cmn_start_docker
    cmn_configure_docker_daemon
    cmn_start_docker
    cmn_enable_docker_access

    local attempt
    for attempt in {1..30}; do
        if docker info >/dev/null 2>&1; then
            cmn_require_docker
            return 0
        fi
        sleep 1
    done
    cmn_log bootstrap "FATAL: Docker daemon did not become ready." >&2
    return 1
}

cmn_pull_devcontainer_image() {
    local repository_root="$1"
    local config="${repository_root}/.devcontainer/devcontainer.json"
    local image

    [[ -f "${config}" ]] ||
        {
            cmn_log bootstrap "FATAL: devcontainer configuration not found at ${config}." >&2
            return 1
        }
    image="$(jq -er '.image | select(type == "string" and length > 0)' "${config}")" ||
        {
            cmn_log bootstrap "FATAL: ${config} must define a non-empty image." >&2
            return 1
        }

    cmn_log bootstrap "Pulling pinned devcontainer image: ${image}"
    docker pull "${image}"
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

    cmn_log bootstrap "Installing host bootstrap utilities: ${missing[*]}"
    cmn_apt_install ca-certificates curl xz-utils
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
