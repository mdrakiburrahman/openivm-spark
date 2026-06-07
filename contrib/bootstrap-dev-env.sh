#!/bin/bash
# ============================================================================
# contrib/bootstrap-dev-env.sh — full developer-host bootstrap.
#
#   Bootstraps a Linux Devbox host idempotently.
#   If your Devbox restarts, rerun this script.
#
# All reusable helpers live in contrib/common.sh; this file only orchestrates
# the dev-relevant sequence (interactive logins, WSL path-stripping, etc.).
# CI uses contrib/bootstrap-ci.sh which is a Docker-only subset.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

# 1. Passwordless sudo — every subsequent step shells out to `sudo`.
cmn_ensure_passwordless_sudo

# 2. Strip WSL-mounted Windows paths so we don't accidentally pick up the
#    Windows az / gh executables (slower and don't share state with the
#    Linux-side dev image).
export PATH="$(cmn_strip_windows_paths)"

# 3. Apt-side basics.
cmn_ensure_jq

# 4. Docker — install if missing, configure daemon, restart only on change.
cmn_ensure_docker
cmn_configure_docker_daemon
cmn_kill_running_containers

# 5. Azure CLI (devbox uses ~/.azure for openivm-spark image push/pull tests).
cmn_ensure_az_cli
cmn_ensure_az_login

# 6. GitHub CLI.
cmn_ensure_gh_cli
cmn_ensure_gh_login

echo
echo "Docker:     $(docker --version)"
echo "GitHub CLI: $(gh --version | head -1)"
echo "Azure CLI:  $(az --version | head -1)"