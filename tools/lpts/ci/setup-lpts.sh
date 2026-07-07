#!/usr/bin/env bash
# Clone (or fetch + checkout) the lpts repo into .temp/lpts at the SHA pinned
# in spark-ext/dev/pins.env. Idempotent and safe to re-run.
#
# .temp/ is gitignored (.gitignore line 7), so the lpts checkout never lands
# in this repo's index.
#
# Usage:
#   ./setup-lpts.sh                       # use LPTS_REPO/COMMIT from pins.env
#   ./setup-lpts.sh --sha <sha>           # override the SHA
#   ./setup-lpts.sh --repo <url>          # override the repo URL
#   ./setup-lpts.sh --branch <name>       # additionally fetch a named branch tip

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
PINS_FILE="${REPO_ROOT}/spark-ext/dev/pins.env"
LPTS_DIR_DEFAULT="${REPO_ROOT}/.temp/lpts"

LPTS_DIR="${LPTS_DIR:-${LPTS_DIR_DEFAULT}}"
REPO_OVERRIDE=""
SHA_OVERRIDE=""
BRANCH_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)    REPO_OVERRIDE="$2"; shift 2 ;;
        --sha)     SHA_OVERRIDE="$2"; shift 2 ;;
        --branch)  BRANCH_OVERRIDE="$2"; shift 2 ;;
        -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
        *)         echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

[[ -f "${PINS_FILE}" ]] || { echo "ERROR: pins.env not found at ${PINS_FILE}" >&2; exit 1; }

LPTS_REPO="${REPO_OVERRIDE:-$(grep -E '^LPTS_REPO=' "${PINS_FILE}" | cut -d= -f2-)}"
LPTS_COMMIT="${SHA_OVERRIDE:-$(grep -E '^LPTS_COMMIT=' "${PINS_FILE}" | cut -d= -f2-)}"
LPTS_BRANCH="${BRANCH_OVERRIDE:-$(grep -E '^LPTS_BRANCH=' "${PINS_FILE}" | cut -d= -f2-)}"

[[ -n "${LPTS_REPO}" && -n "${LPTS_COMMIT}" ]] || \
    { echo "ERROR: failed to resolve LPTS_REPO/COMMIT from pins.env" >&2; exit 1; }

mkdir -p "$(dirname "${LPTS_DIR}")"

if [[ ! -d "${LPTS_DIR}/.git" ]]; then
    echo "[setup] cloning ${LPTS_REPO} into ${LPTS_DIR}"
    git clone --quiet "${LPTS_REPO}" "${LPTS_DIR}"
fi

cd "${LPTS_DIR}"

if [[ "$(git config --get remote.origin.url)" != "${LPTS_REPO}" ]]; then
    echo "[setup] retargeting origin to ${LPTS_REPO}"
    git remote set-url origin "${LPTS_REPO}"
fi

echo "[setup] fetching ${LPTS_BRANCH} + ${LPTS_COMMIT}"
git fetch --quiet origin "${LPTS_BRANCH}" || true
git fetch --quiet origin "${LPTS_COMMIT}"

CURRENT_SHA="$(git rev-parse HEAD)"
if [[ "${CURRENT_SHA}" != "${LPTS_COMMIT}" ]]; then
    echo "[setup] checkout ${LPTS_COMMIT}"
    git -c advice.detachedHead=false checkout --quiet "${LPTS_COMMIT}"
fi

echo "[setup] lpts @ $(git rev-parse --short HEAD) ($(git log -1 --format=%s))"
echo "[setup] ready at ${LPTS_DIR}"
