#!/usr/bin/env bash
# Validate the runner prerequisites used to build or publish the devcontainer.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

cmn_require git
cmn_require_docker

echo "Runner Docker: $(docker --version)"
