#!/usr/bin/env bash
# ============================================================================
# spark-ext/dev/stress.sh — fire `./spark-ext/dev/dev.sh verify` N times.
#
# Each run streams to its own log under .logs/stress-<UTC-timestamp>/run-NN.log
# so a failing run leaves a self-contained artifact behind even after the
# script aborts.  On the FIRST failure we:
#   * print the path of the failing log,
#   * print the path of the run directory (so the user can also inspect
#     successful runs that preceded it),
#   * exit non-zero immediately (no further runs are attempted).
#
# Important: we use `set -o pipefail` so that `cmd | tee` correctly propagates
# the exit status of `cmd`.  The naive `cmd 2>&1 | tee log` pattern (used
# elsewhere) silently masks every non-zero exit.
#
# Usage:
#   ./spark-ext/dev/stress.sh <N>
#
# Example:
#   ./spark-ext/dev/stress.sh 25      # run verify 25 times, fast-fail on the
#                                       # first red one.
# ============================================================================
set -euo pipefail
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/.." && pwd)"
DEV_SH="$SCRIPT_DIR/dev.sh"

# ── arg parsing ────────────────────────────────────────────────────────────

if [[ $# -ne 1 ]]; then
    echo "[stress] FATAL: expected exactly one argument (the run count)" >&2
    echo "         usage: $(basename "$0") <N>" >&2
    exit 2
fi

N="$1"
if ! [[ "$N" =~ ^[0-9]+$ ]] || [[ "$N" -lt 1 ]]; then
    echo "[stress] FATAL: <N> must be a positive integer, got '$N'" >&2
    exit 2
fi

if [[ ! -x "$DEV_SH" ]]; then
    echo "[stress] FATAL: $DEV_SH is not executable" >&2
    exit 1
fi

# ── log layout ─────────────────────────────────────────────────────────────

TS="$(date -u +%Y%m%d-%H%M%S)"
RUN_DIR="$REPO_ROOT/.logs/stress-$TS"
mkdir -p "$RUN_DIR"

WIDTH=${#N}

echo "[stress] firing verify ×$N — logs at $RUN_DIR"
echo "[stress] each run will also stream to stdout; press Ctrl-C to abort."

STARTED_AT=$(date -u +%s)

for ((i = 1; i <= N; i++)); do
    LOG="$(printf '%s/run-%0*d.log' "$RUN_DIR" "$WIDTH" "$i")"
    HEADER="[stress] === run $i/$N — $(date -u +%H:%M:%S) UTC ==="
    echo
    echo "$HEADER"
    echo "$HEADER" >"$LOG"

    # `set -o pipefail` makes the exit status of the pipeline the rightmost
    # non-zero status (or 0 if all succeeded), so a `verify` failure here is
    # correctly surfaced as a non-zero `$?`.
    if ! (cd "$REPO_ROOT" && "$DEV_SH" verify) 2>&1 | tee -a "$LOG"; then
        ELAPSED=$(( $(date -u +%s) - STARTED_AT ))
        echo
        echo "[stress] *** FAILED on run $i/$N after ${ELAPSED}s ***" >&2
        echo "[stress]     failing log : $LOG" >&2
        echo "[stress]     all logs    : $RUN_DIR" >&2
        # Surface the failing-test lines from the log if any are present so the
        # caller doesn't need to grep for them.
        if grep -E '\*\*\* FAILED \*\*\*|TESTS FAILED|sbt.TestsFailedException' "$LOG" >/tmp/stress-fail-$$.txt 2>/dev/null; then
            echo "[stress]     failing lines:" >&2
            sed 's/^/[stress]       /' /tmp/stress-fail-$$.txt >&2
            rm -f /tmp/stress-fail-$$.txt
        fi
        exit 1
    fi
done

ELAPSED=$(( $(date -u +%s) - STARTED_AT ))
echo
echo "[stress] ✓ All $N runs PASSED in ${ELAPSED}s — logs at $RUN_DIR"
