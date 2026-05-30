#!/usr/bin/env bash
# Local-Docker replica of the openivm GitHub CI for the openivm-spark fork.
#
# Lives at tools/openivm/ci/ and drives four jobs against a checkout of the
# openivm repo (default: .temp/openivm, which is gitignored). Mirrors:
#   - format-check (ubuntu:22.04 + clang-format-11 + black + cmake-format)
#   - tidy-check   (ubuntu:24.04 + clang-tidy)
#   - build + sqllogictest (manylinux_2_28_x86_64 + vcpkg + ninja, linux_amd64)
#
# Logs are written under .logs/openivm-ci/<UTC-timestamp>/ (gitignored).
#
# Usage:
#   ./run-all.sh [--only build|test|format|tidy|all] [--clean] [--no-chown]
#                [--unittest <glob>] [--merge-base <ref>]
#                [--openivm-dir <path>] [-h|--help]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
OPENIVM_DIR="${OPENIVM_DIR:-${REPO_ROOT}/.temp/openivm}"
DOCKER_DIR="${SCRIPT_DIR}/docker"
LOG_ROOT="${REPO_ROOT}/.logs/openivm-ci"

DUCKDB_VERSION="v1.5.2"
CI_TOOLS_VERSION="v1.5.2"
VCPKG_URL="https://github.com/microsoft/vcpkg.git"
VCPKG_COMMIT="84bab45d415d22042bd0b9081aea57f362da3f35"
VCPKG_TRIPLET="x64-linux-release"
DUCKDB_PLATFORM="linux_amd64"

IMG_BUILD="openivm-ci-build:${CI_TOOLS_VERSION}"
IMG_FORMAT="openivm-ci-format:${CI_TOOLS_VERSION}"
IMG_TIDY="openivm-ci-tidy:${CI_TOOLS_VERSION}"

CCACHE_VOLUME="openivm-ci-ccache"

ONLY="all"
CLEAN=0
NO_CHOWN=0
UNITTEST_GLOB=""
MERGE_BASE=""

usage() {
    sed -n '2,16p' "$0"
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --only)         ONLY="$2"; shift 2 ;;
        --clean)        CLEAN=1; shift ;;
        --no-chown)     NO_CHOWN=1; shift ;;
        --unittest)     UNITTEST_GLOB="$2"; shift 2 ;;
        --merge-base)   MERGE_BASE="$2"; shift 2 ;;
        --openivm-dir)  OPENIVM_DIR="$2"; shift 2 ;;
        -h|--help)      usage 0 ;;
        *)              echo "Unknown arg: $1" >&2; usage 1 ;;
    esac
done

case "${ONLY}" in
    all|build|test|format|tidy) ;;
    *) echo "--only must be one of: all build test format tidy" >&2; exit 1 ;;
esac

TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${LOG_ROOT}/${TS}"
mkdir -p "${LOG_DIR}"
ln -sfn "${TS}" "${LOG_ROOT}/latest"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() { echo "ERROR: $*" >&2; exit 1; }

if [[ ! -d "${OPENIVM_DIR}/.git" ]]; then
    cat >&2 <<EOF
ERROR: openivm checkout not found at ${OPENIVM_DIR}

Bootstrap it (uses the SHA pinned in spark-ext/dev/pins.env):

    ${SCRIPT_DIR}/setup-openivm.sh

Or point this script at an existing checkout:

    ./run-all.sh --openivm-dir /path/to/openivm
    OPENIVM_DIR=/path/to/openivm ./run-all.sh
EOF
    exit 1
fi
[[ -d "${DOCKER_DIR}" ]] || fail "${DOCKER_DIR} missing"

CUR_BRANCH="$(cd "${OPENIVM_DIR}" && git rev-parse --abbrev-ref HEAD)"
CUR_SHA="$(cd "${OPENIVM_DIR}" && git rev-parse --short HEAD)"
log "openivm dir : ${OPENIVM_DIR}"
log "branch / sha: ${CUR_BRANCH} @ ${CUR_SHA}"
log "logs        : ${LOG_DIR}"

declare -A STATUS DURATION LOGFILE

run_step() {
    local name="$1"; shift
    local logf="${LOG_DIR}/${name}.log"
    LOGFILE[$name]="${logf}"
    local t0 t1
    t0="$(date +%s)"
    log ">>> ${name}"
    if "$@" >"${logf}" 2>&1; then
        STATUS[$name]="PASS"
    else
        STATUS[$name]="FAIL"
    fi
    t1="$(date +%s)"
    DURATION[$name]="$(( t1 - t0 ))s"
    log "<<< ${name} ${STATUS[$name]} (${DURATION[$name]}, log: ${logf})"
}

bootstrap_submodules() {
    cd "${OPENIVM_DIR}"
    log "bootstrapping submodules (idempotent)"
    git submodule update --init -- third_party/lpts third_party/benchbase >/dev/null
    git submodule update --init -- duckdb extension-ci-tools >/dev/null
    (cd third_party/lpts && \
        git submodule update --init --recursive --quiet)
    (cd duckdb && \
        git fetch --tags --quiet origin && \
        git checkout --quiet "${DUCKDB_VERSION}" && \
        git submodule update --init --recursive --quiet)
    (cd extension-ci-tools && \
        git fetch --tags --quiet origin && \
        git checkout --quiet "${CI_TOOLS_VERSION}")
    log "duckdb @ $(cd duckdb && git describe --tags --always)"
    log "extension-ci-tools @ $(cd extension-ci-tools && git describe --tags --always)"
    log "lpts @ $(cd third_party/lpts && git describe --tags --always 2>/dev/null || cd third_party/lpts && git rev-parse --short HEAD)"
    if [[ -f "third_party/lpts/third_party/ducklake/src/include/storage/ducklake_scan.hpp" ]]; then
        log "lpts/ducklake header present"
    else
        log "WARN: lpts/ducklake header missing; tidy and build will fail"
    fi
}

apply_merge_base() {
    [[ -z "${MERGE_BASE}" ]] && return 0
    cd "${OPENIVM_DIR}"
    log "fetching upstream/${MERGE_BASE} for merge-result test"
    git fetch --quiet upstream "${MERGE_BASE}"
    git -c user.name=ci -c user.email=ci@local merge --no-edit --no-ff "upstream/${MERGE_BASE}" \
        || fail "merge with upstream/${MERGE_BASE} failed; resolve manually"
}

build_images() {
    log "building docker image: ${IMG_BUILD}"
    docker build \
        --build-arg "vcpkg_url=${VCPKG_URL}" \
        --build-arg "vcpkg_commit=${VCPKG_COMMIT}" \
        -t "${IMG_BUILD}" \
        -f "${DOCKER_DIR}/Dockerfile.build" \
        "${DOCKER_DIR}" >>"${LOG_DIR}/docker-build.log" 2>&1

    log "building docker image: ${IMG_FORMAT}"
    docker build \
        -t "${IMG_FORMAT}" \
        -f "${DOCKER_DIR}/Dockerfile.format" \
        "${DOCKER_DIR}" >>"${LOG_DIR}/docker-build.log" 2>&1

    log "building docker image: ${IMG_TIDY}"
    docker build \
        -t "${IMG_TIDY}" \
        -f "${DOCKER_DIR}/Dockerfile.tidy" \
        "${DOCKER_DIR}" >>"${LOG_DIR}/docker-build.log" 2>&1
}

clean_build_artifacts() {
    [[ "${CLEAN}" -eq 1 ]] || return 0
    log "cleaning openivm build dirs (--clean)"
    rm -rf "${OPENIVM_DIR}/build" "${OPENIVM_DIR}/duckdb/build" 2>/dev/null || true
}

do_format() {
    docker run --rm \
        -v "${OPENIVM_DIR}:/work" \
        -w /work \
        "${IMG_FORMAT}" \
        make format-check
}

do_tidy() {
    docker run --rm \
        -v "${OPENIVM_DIR}:/work" \
        -w /work \
        -e TIDY_THREADS=4 \
        "${IMG_TIDY}" \
        make tidy-check
}

build_env_args=(
    -e GEN=ninja
    -e BUILD_SHELL=1
    -e DUCKDB_PLATFORM="${DUCKDB_PLATFORM}"
    -e VCPKG_TOOLCHAIN_PATH=/vcpkg/scripts/buildsystems/vcpkg.cmake
    -e VCPKG_TARGET_TRIPLET="${VCPKG_TRIPLET}"
    -e VCPKG_HOST_TRIPLET="${VCPKG_TRIPLET}"
    -e VCPKG_OVERLAY_TRIPLETS=/duckdb_build_dir/extension-ci-tools/toolchains
    -e VCPKG_OVERLAY_PORTS=/duckdb_build_dir/extension-ci-tools/vcpkg_ports
    -e "OPENSSL_ROOT_DIR=/duckdb_build_dir/build/release/vcpkg_installed/${VCPKG_TRIPLET}"
    -e "VCPKG_BINARY_SOURCES=clear;http,https://vcpkg-cache.duckdb.org,read"
    -e ENABLE_EXTENSION_AUTOLOADING=1
    -e ENABLE_EXTENSION_AUTOINSTALL=1
    -e LINUX_CI_IN_DOCKER=1
)

do_build() {
    docker run --rm \
        -v "${OPENIVM_DIR}:/duckdb_build_dir" \
        -v "${CCACHE_VOLUME}:/ccache_dir" \
        -w /duckdb_build_dir \
        "${build_env_args[@]}" \
        "${IMG_BUILD}" \
        make
}

do_test() {
    local pattern="${UNITTEST_GLOB:-test/*}"
    docker run --rm \
        -v "${OPENIVM_DIR}:/duckdb_build_dir" \
        -v "${CCACHE_VOLUME}:/ccache_dir" \
        -w /duckdb_build_dir \
        "${build_env_args[@]}" \
        "${IMG_BUILD}" \
        ./build/release/test/unittest "${pattern}"
}

chown_back() {
    [[ "${NO_CHOWN}" -eq 1 ]] && return 0
    [[ ! -d "${OPENIVM_DIR}/build" && ! -d "${OPENIVM_DIR}/duckdb/build" ]] && return 0
    local uid gid
    uid="$(id -u)"; gid="$(id -g)"
    log "chown -R ${uid}:${gid} on root-owned build dirs"
    docker run --rm \
        -v "${OPENIVM_DIR}:/work" \
        alpine:3 \
        sh -c "chown -R ${uid}:${gid} /work/build /work/duckdb/build 2>/dev/null || true" \
        || true
}

trap chown_back EXIT

bootstrap_submodules
apply_merge_base
build_images
clean_build_artifacts

case "${ONLY}" in
    all)
        run_step format-check do_format
        run_step tidy-check   do_tidy
        run_step build        do_build
        [[ "${STATUS[build]}" == "PASS" ]] && run_step test do_test || true
        ;;
    format) run_step format-check do_format ;;
    tidy)   run_step tidy-check   do_tidy ;;
    build)  run_step build        do_build ;;
    test)
        if [[ ! -x "${OPENIVM_DIR}/build/release/test/unittest" ]]; then
            log "no test binary; running build first"
            run_step build do_build
            [[ "${STATUS[build]}" == "PASS" ]] || { log "build failed, skipping test"; }
        fi
        if [[ -x "${OPENIVM_DIR}/build/release/test/unittest" ]]; then
            run_step test do_test
        fi
        ;;
esac

printf '\n=== Summary (UTC %s) ===\n' "${TS}"
printf '%-14s %-6s %-8s %s\n' "JOB" "STATUS" "TIME" "LOG"
overall=0
for j in format-check tidy-check build test; do
    [[ -z "${STATUS[$j]:-}" ]] && continue
    printf '%-14s %-6s %-8s %s\n' "$j" "${STATUS[$j]}" "${DURATION[$j]:--}" "${LOGFILE[$j]:--}"
    [[ "${STATUS[$j]}" == "PASS" ]] || overall=1
done

exit "${overall}"
