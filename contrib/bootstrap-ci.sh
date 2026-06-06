#!/bin/bash
# ============================================================================
# contrib/bootstrap-ci.sh — minimal CI-host bootstrap.
#
# Sourced by .github/workflows/gci.yaml. On GitHub-hosted `ubuntu-latest`
# Docker + jq are pre-installed, so every helper below is effectively a
# no-op. Kept as a script (not inlined into the workflow) so any new CI-side
# dep is added in one place and shares logic with the dev bootstrap.
#
# Intentionally OMITTED vs bootstrap-dev-env.sh:
#   * Azure CLI + az login  (CI doesn't need Azure)
#   * gh auth login         (workflow already has GITHUB_TOKEN)
#   * Passwordless sudo     (GH runners are already passwordless sudo)
#   * Strip /mnt/c paths    (no WSL on GH runners)
#   * Kill running containers (runner is ephemeral)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

cmn_ensure_jq
cmn_ensure_docker
cmn_configure_docker_daemon

echo
echo "Docker: $(docker --version)"
echo "jq:     $(jq --version)"