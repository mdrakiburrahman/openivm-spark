#!/usr/bin/env bash
# ============================================================================
# spark-ext/dev/dev — single-entry dev-loop wrapper.
#
# Subcommands (alphabetical):
#   assembly                Build the ivmExtension fat jar.
#   build                   `sbt compile` inside the dev container.
#   dev-build [all|build|test [filter]]
#                           Quick iteration on local .temp/openivm + .temp/lpts.
#   fmt                     Auto-format Scala sources with scalafmt.
#   help                    Print this message.
#   image-build [args]      `docker compose build` (force rebuild of dev image).
#   openivm-test            Run upstream openivm sqllogictest suite.
#   pins-sync               Clone/align .temp/{openivm,lpts,ivm-bench} to the
#                           branches in pins.env and warn if local HEAD has
#                           drifted from the pinned COMMIT.
#   shell                   Drop into bash inside the spark-ext container.
#   test [sbt-args]         `sbt test` (all suites) inside the dev container.
#                           Extra args are appended to sbt (e.g. `testOnly ...`).
#   verify                  pins-sync + lint + compile + assembly + test in
#                           one Docker call.
#
# Environment: only Docker is required on the host.  Pinned image SHAs come
# from `spark-ext/dev/pins.env`.
#
# Environment variables honoured:
#   PRE_CLEAN=1   ANY subcommand first force-removes every running Docker
#                 container on the host (named cache volumes survive).
# ============================================================================
set -euo pipefail

# ── helpers ────────────────────────────────────────────────────────────────

SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEV_DIR="$SCRIPTS_DIR"
PROJECT_DIR="$( cd "$DEV_DIR/.." && pwd )"
REPO_ROOT="$( cd "$PROJECT_DIR/.." && pwd )"

PINS_FILE="$DEV_DIR/pins.env"
COMPOSE_FILE="$DEV_DIR/docker/docker-compose.yml"

if [[ ! -f "$PINS_FILE" ]]; then
    echo "[spark-ext/dev] FATAL: $PINS_FILE not found" >&2
    exit 1
fi

# Load pinned SHAs / versions into environment so docker-compose can substitute them.
# shellcheck disable=SC1090
set -a
source "$PINS_FILE"
set +a

# Wrapper around `docker compose` that always points at our compose file.
compose() {
    docker compose --env-file "$PINS_FILE" -f "$COMPOSE_FILE" "$@"
}

# Standard SBT opts — heap, GC, color, encoding. Applied to every `sbt …` call
# routed through this wrapper so the JVM has enough memory to run 24-way forked
# tests in parallel.
sbt_opts() {
    echo "-Xmx10G -XX:+UseG1GC -Dsbt.color=always -Dfile.encoding=UTF-8"
}

# Run sbt inside the `build` service with the standard opts and any caller-
# supplied tasks/arguments.
#
# OPENIVM_TEST_LOG_DIR (set by `setup_test_log_dir`) is forwarded both as an
# env var (via docker-compose's environment block) AND as a -D system
# property on the sbt JVM (via SBT_OPTS). The sbt JVM in turn propagates
# -D's to forked test JVMs through `Test/javaOptions`, so log4j2.properties
# can resolve `${sys:openivm.test.log.dir}` reliably.
run_sbt() {
    local extra_sys_props=""
    if [[ -n "${OPENIVM_TEST_LOG_DIR:-}" ]]; then
        extra_sys_props=" -Dopenivm.test.log.dir=${OPENIVM_TEST_LOG_DIR}"
    fi
    compose run --rm -T -e SBT_OPTS="$(sbt_opts)${extra_sys_props}" build sbt "$@"
}

# Provision the per-run test-log directory on the host BEFORE we invoke the
# docker container, so log4j2's File appender can open files there without
# racing the test-classes resource copy. Sets OPENIVM_TEST_LOG_DIR to the
# IN-CONTAINER path (matching the bind mount), which is what log4j2.properties
# resolves at appender-init time inside each forked test JVM.
#
# A separate per-fork file is emitted under that directory:
#   .logs/test-<sbt-launch-timestamp>/fork-<jvm-startup-millis>-<thread>.log
#
# The PARENT timestamp is fixed for the entire sbt invocation; the per-fork
# filename uses log4j2's ${date:HHmmss-SSS} lookup which resolves at
# appender-init in each forked JVM, yielding a unique file per fork.
setup_test_log_dir() {
    local ts
    ts="$(date +%Y%m%d-%H%M%S)"
    # The container's /work/spark-ext bind-mounts the HOST's spark-ext/ directory
    # (compose YAML: `../..:/work/spark-ext`). So .logs/ MUST live under
    # spark-ext/.logs/ on the host to be visible from both sides.
    local parent_dir="$PROJECT_DIR/.logs"
    local host_dir="$parent_dir/test-$ts"

    # Ensure the parent .logs/ exists and is world-writable, so the host user
    # (mdrrahman) can mkdir subdirectories even if Docker (running as root)
    # has previously created the parent. World-writable is fine — these are
    # disposable per-run debug logs, not secrets.
    if [[ ! -d "$parent_dir" ]]; then
        mkdir -p "$parent_dir" 2>/dev/null \
            || compose run --rm -T build mkdir -p /work/spark-ext/.logs >/dev/null
    fi
    if [[ ! -w "$parent_dir" ]]; then
        compose run --rm -T build chmod 777 /work/spark-ext/.logs >/dev/null
    fi
    mkdir -p "$host_dir"
    export OPENIVM_TEST_LOG_DIR="/work/spark-ext/.logs/test-$ts"
    echo "[dev] Test logs → spark-ext/.logs/test-$ts/ (DEBUG-level full trace; console keeps WARN+ only)"
}

# Honoured by every subcommand below. If PRE_CLEAN=1, force-remove every
# running Docker container on the host before invoking the next compose
# call. Useful for stripping orphan containers (e.g. from a Ctrl+C'd
# previous run) that may still be holding the shared sbt-cache /
# ivy-cache / coursier-cache named volumes. Named volumes themselves
# survive container removal, so the next `compose run` reuses the warm
# cache.
pre_clean_if_requested() {
    if [[ "${PRE_CLEAN:-0}" == "1" ]]; then
        local running
        running="$(docker ps -q)"
        if [[ -n "$running" ]]; then
            echo "[dev] PRE_CLEAN=1 → force-removing $(echo "$running" | wc -l | tr -d ' ') running container(s)..."
            # shellcheck disable=SC2086
            docker rm -f $running
        else
            echo "[dev] PRE_CLEAN=1 → no running containers to remove."
        fi
    fi
}

usage() {
    sed -n '/^# =====/,/^# =====/p' "${BASH_SOURCE[0]}" \
        | sed -e 's/^# \{0,1\}//' -e '/^=\{3,\}$/d'
}

# ── subcommand implementations ─────────────────────────────────────────────

cmd_fmt()         { pre_clean_if_requested; compose run --rm fmt; }
cmd_build()       { pre_clean_if_requested; compose run --rm build "$@"; }
cmd_assembly()    { pre_clean_if_requested; compose run --rm assembly; }
cmd_shell()       { pre_clean_if_requested; compose run --rm shell; }
cmd_image_build() { pre_clean_if_requested; compose build "$@"; }
cmd_openivm_test(){ pre_clean_if_requested; compose run --rm openivm-test; }

# Ensure $REPO_ROOT/.temp/{openivm,lpts,ivm-bench} are cloned on the branches
# declared in pins.env, then compare each local HEAD against the pinned COMMIT.
#
#   * If the working copy is missing, clone fresh on the pinned branch.
#   * If the working copy exists, fetch origin and switch to the pinned branch
#     (creating a local tracking branch if needed).
#   * If the repo/branch does not exist remotely, abort with a hard error.
#   * After alignment, log per-repo whether HEAD == pin; emit a WARNING for
#     each repo that has drifted. Drift is non-fatal (exit 0).
cmd_pins_sync() {
    local temp_dir="$REPO_ROOT/.temp"
    mkdir -p "$temp_dir"

    # Tuples: dir_name | repo_url_var | branch_var | commit_var
    local entries=(
        "openivm|OPENIVM_REPO|OPENIVM_BRANCH|OPENIVM_COMMIT"
        "lpts|LPTS_REPO|LPTS_BRANCH|LPTS_COMMIT"
        "ivm-bench|IVM_BENCH_REPO|IVM_BENCH_BRANCH|IVM_BENCH_COMMIT"
    )

    local drift=0
    for entry in "${entries[@]}"; do
        IFS='|' read -r name repo_var branch_var commit_var <<< "$entry"
        local repo="${!repo_var}"
        local branch="${!branch_var}"
        local commit="${!commit_var}"
        local dest="$temp_dir/$name"

        echo
        echo "[pins-sync] ── $name ──"
        echo "[pins-sync]   repo   = $repo"
        echo "[pins-sync]   branch = $branch"
        echo "[pins-sync]   pin    = $commit"

        if [[ ! -d "$dest/.git" ]]; then
            if [[ -e "$dest" ]]; then
                echo "[pins-sync] FATAL: $dest exists but is not a git checkout" >&2
                exit 1
            fi
            echo "[pins-sync]   cloning into .temp/$name ..."
            if ! git clone --branch "$branch" "$repo" "$dest"; then
                echo "[pins-sync] FATAL: failed to clone $repo @ $branch" >&2
                echo "[pins-sync]        verify the repository and branch exist remotely." >&2
                exit 1
            fi
        else
            echo "[pins-sync]   already cloned at .temp/$name"
            local current_remote
            current_remote="$(git -C "$dest" remote get-url origin 2>/dev/null || echo '')"
            if [[ -n "$current_remote" && "$current_remote" != "$repo" ]]; then
                echo "[pins-sync]   WARNING: origin remote mismatch"
                echo "[pins-sync]     local  = $current_remote"
                echo "[pins-sync]     pinned = $repo"
            fi

            echo "[pins-sync]   fetching origin ..."
            if ! git -C "$dest" fetch origin --quiet --prune; then
                echo "[pins-sync] FATAL: 'git fetch origin' failed in .temp/$name" >&2
                exit 1
            fi

            if ! git -C "$dest" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
                echo "[pins-sync] FATAL: branch '$branch' does not exist on origin in .temp/$name" >&2
                exit 1
            fi

            local current_branch
            current_branch="$(git -C "$dest" symbolic-ref --short HEAD 2>/dev/null || echo '')"
            if [[ "$current_branch" != "$branch" ]]; then
                echo "[pins-sync]   switching from '${current_branch:-<detached>}' → '$branch'"
                if git -C "$dest" show-ref --verify --quiet "refs/heads/$branch"; then
                    if ! git -C "$dest" checkout --quiet "$branch"; then
                        echo "[pins-sync] FATAL: 'git checkout $branch' failed in .temp/$name" >&2
                        exit 1
                    fi
                else
                    if ! git -C "$dest" checkout --quiet -b "$branch" --track "origin/$branch"; then
                        echo "[pins-sync] FATAL: failed to create tracking branch '$branch' in .temp/$name" >&2
                        exit 1
                    fi
                fi
            fi
        fi

        local head
        head="$(git -C "$dest" rev-parse HEAD)"
        if [[ "$head" == "$commit" ]]; then
            echo "[pins-sync]   ✓ HEAD matches pin ($head)"
        else
            echo "[pins-sync]   ⚠ WARNING: HEAD has drifted from pin"
            echo "[pins-sync]     HEAD = $head"
            echo "[pins-sync]     pin  = $commit"
            drift=1
        fi
    done

    echo
    if [[ "$drift" -eq 0 ]]; then
        echo "[pins-sync] ✓ All 3 repos are on their pinned branches AND match their pinned commits."
    else
        echo "[pins-sync] ⚠ One or more repos have drifted from the pin. See WARNINGs above."
    fi
}

cmd_test() {
    pre_clean_if_requested
    setup_test_log_dir
    if [[ "$#" -eq 0 ]]; then
        compose run --rm test
    else
        run_sbt "$@"
    fi
}

cmd_verify() {
    # One-shot: lint → compile → assembly → full test in a single sbt JVM.
    # Forwarded arguments are inserted before the task list so callers can pass
    # global `set` commands or `-D…` system properties (e.g. test-fork count).
    #
    # Set PRE_CLEAN=1 to force-remove every running Docker container on the
    # host before invoking sbt (see `pre_clean_if_requested` at the top of
    # this file).
    #
    # Always reconcile .temp/{openivm,lpts,ivm-bench} against pins.env first
    # so a `verify` run flags any drifted dependency before spending compute
    # on the full sbt cycle. Drift is non-fatal (WARNING only); a missing
    # repo/branch hard-fails per cmd_pins_sync's contract.
    cmd_pins_sync
    pre_clean_if_requested
    setup_test_log_dir

    run_sbt "$@" \
        scalafmtCheckAll \
        scalafmtSbtCheck \
        compile \
        Test/compile \
        ivmExtension/assembly \
        test
}

# Quick local-source iteration on .temp/openivm + .temp/lpts.  Forwards
# ACTION + optional filter to the existing per-cycle docker-build flow.
cmd_dev_build() {
    local action="${1:-all}"
    shift || true

    local openivm_local="$REPO_ROOT/.temp/openivm"
    local lpts_local="$REPO_ROOT/.temp/lpts"

    if [[ ! -d "$openivm_local" || ! -d "$lpts_local" ]]; then
        echo "[dev-build] FATAL: expected .temp/openivm and .temp/lpts checkouts in $REPO_ROOT" >&2
        exit 1
    fi

    local base_image="openivm-spark/openivm-tester:${OPENIVM_COMMIT}-${LPTS_COMMIT}"
    local dev_image="openivm-spark/openivm-dev:local"

    # Stage local sources into a build context (omits build/, .git/, large submodules).
    local work
    work=$(mktemp -d /tmp/openivm-dev-XXXXXX)
    trap "rm -rf $work" EXIT

    mkdir -p "$work/openivm" "$work/lpts"
    rsync -a --exclude='build/' --exclude='.git/' --exclude='duckdb/' \
              --exclude='extension-ci-tools/' --exclude='third_party/' \
              "$openivm_local/" "$work/openivm/"
    rsync -a --exclude='build/' --exclude='.git/' --exclude='duckdb/' \
              --exclude='third_party/' \
              "$lpts_local/" "$work/lpts/"

    cat > "$work/Dockerfile" <<EOF
FROM ${base_image}
WORKDIR /src
RUN find /src -maxdepth 1 -mindepth 1 -not -name 'duckdb' -not -name 'extension-ci-tools' \\
                                       -not -name 'third_party' -not -name 'build' \\
                                       -not -name '.git' -exec rm -rf {} + && \\
    find /src/third_party/lpts -maxdepth 1 -mindepth 1 -not -name 'duckdb' \\
                                            -not -name 'third_party' -not -name '.git' \\
                                            -exec rm -rf {} +
COPY openivm/ /src/
COPY lpts/ /src/third_party/lpts/
EOF

    echo "[dev-build] Building dev image with local sources..."
    docker build -t "$dev_image" "$work"

    case "$action" in
        build)
            echo "[dev-build] Compiling openivm + patched lpts..."
            docker run --rm -w /src "$dev_image" bash -c "GEN=ninja make -j\$(nproc)"
            ;;
        all)
            echo "[dev-build] Compiling + running upstream tests..."
            docker run --rm -w /src "$dev_image" bash -c "GEN=ninja make -j\$(nproc) && make test $*"
            ;;
        test)
            echo "[dev-build] Building + running tests..."
            if [[ "$#" -eq 0 ]]; then
                docker run --rm -w /src "$dev_image" bash -c "GEN=ninja make -j\$(nproc) && make test"
            else
                docker run --rm -w /src "$dev_image" \
                    bash -c "GEN=ninja make -j\$(nproc) && build/release/test/unittest $*"
            fi
            ;;
        *)
            echo "[dev-build] Unknown action '$action' — expected one of: build, test, all" >&2
            exit 2
            ;;
    esac
    echo "[dev-build] OK (sources from $openivm_local + $lpts_local)"
}

# ── dispatch ───────────────────────────────────────────────────────────────

if [[ "$#" -eq 0 ]]; then
    usage
    exit 0
fi

cmd="$1"
shift

case "$cmd" in
    fmt)          cmd_fmt "$@" ;;
    build)        cmd_build "$@" ;;
    assembly)     cmd_assembly "$@" ;;
    test)         cmd_test "$@" ;;
    verify)       cmd_verify "$@" ;;
    shell)        cmd_shell "$@" ;;
    image-build|image_build) cmd_image_build "$@" ;;
    openivm-test|openivm_test) cmd_openivm_test "$@" ;;
    pins-sync|pins_sync)     cmd_pins_sync "$@" ;;
    dev-build|dev_build)     cmd_dev_build "$@" ;;
    help|-h|--help)          usage ;;
    *)
        echo "[spark-ext/dev] Unknown subcommand: $cmd" >&2
        echo >&2
        usage >&2
        exit 2
        ;;
esac
