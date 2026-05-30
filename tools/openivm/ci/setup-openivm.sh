#!/usr/bin/env bash
# Clone (or fetch + checkout) the upstream openivm repo into .temp/openivm at
# the SHA pinned in spark-ext/dev/pins.env. Idempotent and safe to re-run.
#
# .temp/ is gitignored (.gitignore line 7), so the openivm checkout never lands
# in this repo's index.
#
# Usage:
#   ./setup-openivm.sh                       # use OPENIVM_REPO/COMMIT from pins.env
#   ./setup-openivm.sh --sha <sha>           # override the SHA
#   ./setup-openivm.sh --repo <url>          # override the repo URL
#   ./setup-openivm.sh --branch <name>       # additionally fetch a named branch tip

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
PINS_FILE="${REPO_ROOT}/spark-ext/dev/pins.env"
OPENIVM_DIR_DEFAULT="${REPO_ROOT}/.temp/openivm"

OPENIVM_DIR="${OPENIVM_DIR:-${OPENIVM_DIR_DEFAULT}}"
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

OPENIVM_REPO="${REPO_OVERRIDE:-$(grep -E '^OPENIVM_REPO=' "${PINS_FILE}" | cut -d= -f2-)}"
OPENIVM_COMMIT="${SHA_OVERRIDE:-$(grep -E '^OPENIVM_COMMIT=' "${PINS_FILE}" | cut -d= -f2-)}"
OPENIVM_BRANCH="${BRANCH_OVERRIDE:-$(grep -E '^OPENIVM_BRANCH=' "${PINS_FILE}" | cut -d= -f2-)}"

[[ -n "${OPENIVM_REPO}" && -n "${OPENIVM_COMMIT}" ]] || \
    { echo "ERROR: failed to resolve OPENIVM_REPO/COMMIT from pins.env" >&2; exit 1; }

mkdir -p "$(dirname "${OPENIVM_DIR}")"

if [[ ! -d "${OPENIVM_DIR}/.git" ]]; then
    echo "[setup] cloning ${OPENIVM_REPO} into ${OPENIVM_DIR}"
    git clone --quiet "${OPENIVM_REPO}" "${OPENIVM_DIR}"
fi

cd "${OPENIVM_DIR}"

if [[ "$(git config --get remote.origin.url)" != "${OPENIVM_REPO}" ]]; then
    echo "[setup] retargeting origin to ${OPENIVM_REPO}"
    git remote set-url origin "${OPENIVM_REPO}"
fi

echo "[setup] fetching ${OPENIVM_BRANCH} + ${OPENIVM_COMMIT}"
git fetch --quiet origin "${OPENIVM_BRANCH}" || true
git fetch --quiet origin "${OPENIVM_COMMIT}"

CURRENT_SHA="$(git rev-parse HEAD)"
if [[ "${CURRENT_SHA}" != "${OPENIVM_COMMIT}" ]]; then
    echo "[setup] checkout ${OPENIVM_COMMIT}"
    git -c advice.detachedHead=false checkout --quiet "${OPENIVM_COMMIT}"
fi

echo "[setup] openivm @ $(git rev-parse --short HEAD) ($(git log -1 --format=%s))"
echo "[setup] ready at ${OPENIVM_DIR}"
