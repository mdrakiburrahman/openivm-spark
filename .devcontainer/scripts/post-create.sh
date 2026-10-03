#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if ! npm ls --depth=0 --silent >/dev/null 2>&1; then
  npm ci --no-audit --no-fund
fi

(
  cd /opt/openivm
  sha256sum -c SHA256SUMS
)

if [[ -S /var/run/docker.sock ]] && ! docker info >/dev/null 2>&1; then
  socket_gid="$(stat -c '%g' /var/run/docker.sock)"
  socket_group="$(getent group "$socket_gid" | cut -d: -f1 || true)"
  if [[ -z "$socket_group" ]]; then
    socket_group="docker-host"
    sudo groupadd --gid "$socket_gid" "$socket_group"
  fi
  sudo usermod --append --groups "$socket_group" "$(id -un)"
  echo "Docker socket group added; reconnect the devcontainer shell before using Docker."
fi
