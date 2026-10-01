#!/usr/bin/env bash
# Bootstrap the minimal host tooling needed to build and run the devcontainer.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

cmn_require bash
cmn_require git
cmn_require_docker
cmn_ensure_node
cmn_npm_ci "${REPOSITORY_ROOT}"

cat <<EOF

Host bootstrap complete.
  Node:   $(node --version)
  npm:    $(npm --version)
  Docker: $(docker --version)

Build the prebuilt image:
  npx --no-install nx run devcontainer:build

Start the repository headlessly:
  npx --no-install nx run devcontainer:up
EOF
