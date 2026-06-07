#!/usr/bin/env bash
# spark-ext container entrypoint.
#
# Responsibilities:
#   - confirm the OpenIVM extension is present
#   - run the requested command (sbt, bash, etc.)
#
# Anything that needs to run on every container start lives here.

set -euo pipefail

if [[ -f "${OPENIVM_EXTENSION_PATH:-/opt/openivm/openivm.duckdb_extension}" ]]; then
    : # ok
else
    echo "[entrypoint] FATAL: openivm extension missing at ${OPENIVM_EXTENSION_PATH:-/opt/openivm/openivm.duckdb_extension}" >&2
    exit 1
fi

if [[ "$#" -eq 0 ]]; then
    exec bash
fi

exec "$@"
