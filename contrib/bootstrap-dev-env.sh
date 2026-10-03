#!/usr/bin/env bash
# Bootstrap the minimal host tooling needed to build and run the devcontainer.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

export PATH="$(cmn_strip_windows_paths)"

cmn_ensure_host_packages
cmn_ensure_docker
cmn_pull_devcontainer_image "${REPOSITORY_ROOT}"
cmn_ensure_node
cmn_npm_ci "${REPOSITORY_ROOT}"

cat <<EOF

Host bootstrap complete.
  Node:   $(node --version)
  npm:    $(npm --version)
  Docker: $(docker --version)
  Buildx: $(docker buildx version | head -1)
  Compose: $(docker compose version)

Build the prebuilt image:
  npx --no-install nx run devcontainer:build

Start the repository headlessly:
  npx --no-install nx run devcontainer:up
EOF
