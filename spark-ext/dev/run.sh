#!/usr/bin/env bash
# Direct spark-ext command runner for an already-provisioned devcontainer.
#
# Unlike dev.sh, this script never invokes Docker. Nx and other in-container
# callers use it to run the same SBT pipelines against the mounted workspace.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/.." && pwd)"

PINS_FILE="$SCRIPT_DIR/pins.env"
TARGETS_DIR="$SCRIPT_DIR/targets"
DEV_SH="$SCRIPT_DIR/dev.sh"

die() {
    echo "[spark-ext/run] FATAL: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage:
  run.sh [--target spark-3.5|spark-4.1] [--skip-pins-sync] <command> [args...]

Commands:
  fmt                       Format Scala and SBT sources.
  lint                      Check Scala and SBT formatting.
  build [sbt-args]          Compile the selected Spark target.
  clean [sbt-args]          Clean SBT outputs.
  test [sbt-command...]     Write inventory and run all tests by default.
                            With arguments, pass them directly to SBT.
  test-suite [sbt-command...]
                            Run all tests by default, without inventory.
  test-inventory [sbt-args] Write the selected target's discovered tests.
  compare-test-inventory    Compare Spark 3.5 and Spark 4.1 inventories.
  assembly [sbt-args]       Build the ivmExtension assembly JAR.
  verify [sbt-args]         pins-sync, lint, compile, inventory, assembly, test.
  verify-all [sbt-args]     Verify both targets and compare inventories.
  window-benchmark          Run the WindowNoopWriteHarness micro-benchmark.
  openivm-test [make-args]  Run upstream tests from a baked or mounted source.
  pins-sync                 Align pinned dependency worktrees.
  help                      Print this message.

Environment:
  OPENIVM_SPARK_TARGET       Target alias for --target (default spark-3.5).
  TARGET                     Secondary target alias used by Nx/CI callers.
  OPENIVM_JAVA_HOME_SPARK_35 Spark 3.5 JDK home (default /opt/java/jdk-17).
  OPENIVM_JAVA_HOME_SPARK_41 Spark 4.1 JDK home (default /opt/java/jdk-21).
  OPENIVM_NATIVE_DIR         Native artifact directory (default /opt/openivm).
  OPENIVM_SBT_BIN            SBT executable (default sbt).
  OPENIVM_SBT_OPTS           Override the standard SBT JVM options.
  OPENIVM_TEST_FORKS         Fork cap forwarded as openivm.test.forks.
  OPENIVM_TEST_LOG_ROOT      Log root (default repo-root .logs).
  OPENIVM_SOURCE_DIR         OpenIVM source tree for openivm-test.
EOF
}

OPENIVM_SPARK_TARGET="${OPENIVM_SPARK_TARGET:-${TARGET:-spark-3.5}}"
SKIP_PINS_SYNC=0

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --target)
            [[ "$#" -ge 2 ]] || die "--target requires spark-3.5 or spark-4.1"
            OPENIVM_SPARK_TARGET="$2"
            shift 2
            ;;
        --skip-pins-sync)
            SKIP_PINS_SYNC=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            break
            ;;
        -*)
            die "unknown option '$1'"
            ;;
        *)
            break
            ;;
    esac
done

COMMAND="${1:-help}"
if [[ "$#" -gt 0 ]]; then
    shift
fi

[[ -f "$PINS_FILE" ]] || die "$PINS_FILE not found"
RUNTIME_PINS_FILE="$TARGETS_DIR/$OPENIVM_SPARK_TARGET.env"
if [[ ! -f "$RUNTIME_PINS_FILE" ]]; then
    echo "[spark-ext/run] FATAL: unsupported target '$OPENIVM_SPARK_TARGET'" >&2
    echo "[spark-ext/run]        expected spark-3.5 or spark-4.1" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
source "$PINS_FILE"
# shellcheck disable=SC1090
source "$RUNTIME_PINS_FILE"
export OPENIVM_SPARK_TARGET
set +a

export OPENIVM_JAVA_HOME_SPARK_35="${OPENIVM_JAVA_HOME_SPARK_35:-/opt/java/jdk-17}"
export OPENIVM_JAVA_HOME_SPARK_41="${OPENIVM_JAVA_HOME_SPARK_41:-/opt/java/jdk-21}"

java_major_from() {
    local java_bin="$1"
    local version_line
    version_line="$("$java_bin" -version 2>&1 | head -n 1)" || return 1
    if [[ "$version_line" =~ version\ \"1\.([0-9]+) ]]; then
        printf '%s\n' "${BASH_REMATCH[1]}"
    elif [[ "$version_line" =~ version\ \"([0-9]+) ]]; then
        printf '%s\n' "${BASH_REMATCH[1]}"
    else
        return 1
    fi
}

select_java_runtime() {
    local expected="$JDK_VERSION"
    local target_var configured_home
    case "$OPENIVM_SPARK_TARGET" in
        spark-3.5) target_var="OPENIVM_JAVA_HOME_SPARK_35" ;;
        spark-4.1) target_var="OPENIVM_JAVA_HOME_SPARK_41" ;;
        *) die "unsupported target '$OPENIVM_SPARK_TARGET'" ;;
    esac

    configured_home="${!target_var}"
    [[ -x "$configured_home/bin/java" ]] \
        || die "$OPENIVM_SPARK_TARGET requires JDK $expected at $configured_home; override with $target_var"

    local actual
    actual="$(java_major_from "$configured_home/bin/java" || true)"
    [[ "$actual" == "$expected" ]] \
        || die "$target_var=$configured_home provides JDK ${actual:-unknown}, expected JDK $expected"

    export JAVA_HOME="$configured_home"
    export PATH="$JAVA_HOME/bin:$PATH"
}

configure_native_artifacts() {
    export OPENIVM_NATIVE_DIR="${OPENIVM_NATIVE_DIR:-/opt/openivm}"
    export OPENIVM_CLI_PATH="${OPENIVM_CLI_PATH:-$OPENIVM_NATIVE_DIR/duckdb}"
    export OPENIVM_EXTENSION_PATH="${OPENIVM_EXTENSION_PATH:-$OPENIVM_NATIVE_DIR/openivm.duckdb_extension}"
}

require_native_artifacts() {
    configure_native_artifacts
    [[ -x "$OPENIVM_CLI_PATH" ]] \
        || die "OpenIVM CLI missing or not executable at $OPENIVM_CLI_PATH"
    [[ -f "$OPENIVM_EXTENSION_PATH" ]] \
        || die "OpenIVM extension missing at $OPENIVM_EXTENSION_PATH"
    [[ -x "$OPENIVM_NATIVE_DIR/duckdb" ]] \
        || die "assembly native CLI missing or not executable at $OPENIVM_NATIVE_DIR/duckdb"
    [[ -f "$OPENIVM_NATIVE_DIR/openivm.duckdb_extension" ]] \
        || die "assembly native extension missing at $OPENIVM_NATIVE_DIR/openivm.duckdb_extension"
}

setup_test_log_dir() {
    local root="${OPENIVM_TEST_LOG_ROOT:-$REPO_ROOT/.logs}"
    local timestamp="${OPENIVM_TEST_LOG_TIMESTAMP:-$(date -u +%Y%m%d-%H%M%S)}"
    local base="$root/test-$timestamp"
    local log_dir="$base"
    local suffix=0

    while [[ -e "$log_dir" ]]; do
        suffix=$((suffix + 1))
        log_dir="$base-$suffix"
    done
    mkdir -p "$log_dir"
    export OPENIVM_TEST_LOG_DIR="$log_dir"

    local display="$log_dir"
    if [[ "$log_dir" == "$PROJECT_DIR/"* ]]; then
        display="${log_dir#"$PROJECT_DIR/"}"
    elif [[ "$log_dir" == "$REPO_ROOT/"* ]]; then
        display="${log_dir#"$REPO_ROOT/"}"
    fi
    echo "[spark-ext/run] Test logs -> $display/ (per-fork DEBUG logs; console keeps WARN+ only)"
}

run_sbt() {
    local sbt_bin="${OPENIVM_SBT_BIN:-sbt}"
    if [[ "$sbt_bin" == */* ]]; then
        [[ -x "$sbt_bin" ]] || die "SBT executable not found at $sbt_bin"
    else
        command -v "$sbt_bin" >/dev/null 2>&1 || die "SBT executable '$sbt_bin' not found"
    fi

    local opts="${OPENIVM_SBT_OPTS:--Xmx10G -XX:+UseG1GC -Dsbt.color=always -Dfile.encoding=UTF-8}"
    if [[ -n "${OPENIVM_TEST_LOG_DIR:-}" ]]; then
        opts+=" -Dopenivm.test.log.dir=${OPENIVM_TEST_LOG_DIR}"
    fi

    local has_fork_arg=0 arg
    for arg in "$@"; do
        if [[ "$arg" == -Dopenivm.test.forks=* ]]; then
            has_fork_arg=1
            break
        fi
    done
    if [[ -n "${OPENIVM_TEST_FORKS:-}" && "$has_fork_arg" == "0" ]]; then
        [[ "$OPENIVM_TEST_FORKS" =~ ^[1-9][0-9]*$ ]] \
            || die "OPENIVM_TEST_FORKS must be a positive integer"
        opts+=" -Dopenivm.test.forks=${OPENIVM_TEST_FORKS}"
    fi
    export SBT_OPTS="$opts"

    local rc
    set +e
    (
        cd "$PROJECT_DIR"
        "$sbt_bin" "$@"
    )
    rc=$?
    set -e

    if [[ "$rc" -ne 0 && -n "${OPENIVM_TEST_LOG_DIR:-}" ]]; then
        echo "[spark-ext/run] FAILED (exit $rc); per-fork logs: $OPENIVM_TEST_LOG_DIR/" >&2
    fi
    return "$rc"
}

prepare_sbt() {
    local needs_native="$1"
    select_java_runtime
    configure_native_artifacts
    if [[ "$needs_native" == "1" ]]; then
        require_native_artifacts
    fi
}

cmd_pins_sync() {
    if [[ "$SKIP_PINS_SYNC" == "1" ]]; then
        echo "[spark-ext/run] pins-sync skipped by caller"
        return 0
    fi
    git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
        || die "pins-sync needs the repository root mounted at $REPO_ROOT"
    [[ -x "$DEV_SH" ]] || die "$DEV_SH is not executable"
    "$DEV_SH" --target "$OPENIVM_SPARK_TARGET" pins-sync
}

cmd_fmt() {
    prepare_sbt 0
    run_sbt "$@" scalafmtAll scalafmtSbt
}

cmd_lint() {
    prepare_sbt 0
    run_sbt "$@" scalafmtCheckAll scalafmtSbtCheck
}

cmd_build() {
    prepare_sbt 1
    run_sbt "$@" compile
}

cmd_clean() {
    prepare_sbt 0
    run_sbt "$@" clean
}

cmd_test() {
    prepare_sbt 1
    setup_test_log_dir
    if [[ "$#" -eq 0 ]]; then
        run_sbt testInventory test
    else
        run_sbt "$@"
    fi
}

cmd_test_suite() {
    prepare_sbt 1
    setup_test_log_dir
    if [[ "$#" -eq 0 ]]; then
        run_sbt test
    else
        run_sbt "$@"
    fi
}

cmd_test_inventory() {
    prepare_sbt 1
    run_sbt "$@" testInventory
}

cmd_compare_test_inventory() {
    local inventory_dir="${OPENIVM_TEST_INVENTORY_DIR:-$PROJECT_DIR/target/test-inventory}"
    local spark35="$inventory_dir/spark-3.5.txt"
    local spark41="$inventory_dir/spark-4.1.txt"
    [[ -f "$spark35" ]] || die "missing Spark 3.5 test inventory: $spark35"
    [[ -f "$spark41" ]] || die "missing Spark 4.1 test inventory: $spark41"

    if ! cmp -s "$spark35" "$spark41"; then
        echo "[compare-test-inventory] FATAL: Spark 3.5 and Spark 4.1 discovered different tests" >&2
        diff -u "$spark35" "$spark41" >&2 || true
        return 1
    fi
    echo "[compare-test-inventory] identical Spark 3.5 and Spark 4.1 test inventories"
}

cmd_assembly() {
    prepare_sbt 1
    run_sbt "$@" ivmExtension/assembly
}

cmd_verify() {
    cmd_pins_sync
    prepare_sbt 1
    setup_test_log_dir
    run_sbt "$@" \
        ivmExtension/clean \
        scalafmtCheckAll \
        scalafmtSbtCheck \
        compile \
        Test/compile \
        testInventory \
        ivmExtension/assembly \
        test
}

cmd_verify_all() {
    local -a pin_args=()
    if [[ "$SKIP_PINS_SYNC" == "1" ]]; then
        pin_args+=(--skip-pins-sync)
    fi
    "$0" --target spark-3.5 "${pin_args[@]}" verify "$@"
    "$0" --target spark-4.1 "${pin_args[@]}" verify "$@"
    "$0" compare-test-inventory
}

cmd_window_benchmark() {
    prepare_sbt 1
    setup_test_log_dir
    run_sbt "$@" \
        "ivmIt/testOnly org.openivm.spark.bench.WindowNoopWriteHarness -- -n org.openivm.spark.tags.MicroBenchmark"
}

cmd_openivm_test() {
    local source_dir="${OPENIVM_SOURCE_DIR:-}"
    local candidate

    if [[ -z "$source_dir" ]]; then
        for candidate in /opt/openivm-src /src; do
            if [[ -f "$candidate/Makefile" ]]; then
                source_dir="$candidate"
                break
            fi
        done
    fi
    [[ -n "$source_dir" && -f "$source_dir/Makefile" ]] \
        || die "OpenIVM source not found; set OPENIVM_SOURCE_DIR or bake it at /opt/openivm-src"
    command -v make >/dev/null 2>&1 || die "make is required for openivm-test"

    if [[ -e "$source_dir/.git" ]]; then
        local source_commit
        source_commit="$(git -C "$source_dir" rev-parse HEAD)"
        [[ "$source_commit" == "$OPENIVM_COMMIT" ]] \
            || die "OpenIVM source is at $source_commit, expected pinned commit $OPENIVM_COMMIT"
    fi

    echo "[spark-ext/run] OpenIVM tests -> $source_dir"
    (
        cd "$source_dir"
        export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-${NATIVE_BUILD_JOBS:-8}}"
        make test "$@"
    )
}

case "$COMMAND" in
    fmt)                            cmd_fmt "$@" ;;
    lint)                           cmd_lint "$@" ;;
    build)                          cmd_build "$@" ;;
    clean)                          cmd_clean "$@" ;;
    test)                           cmd_test "$@" ;;
    test-suite|test_suite)          cmd_test_suite "$@" ;;
    test-inventory|test_inventory)  cmd_test_inventory "$@" ;;
    compare-test-inventory|compare_test_inventory)
                                    cmd_compare_test_inventory "$@" ;;
    assembly)                       cmd_assembly "$@" ;;
    verify)                         cmd_verify "$@" ;;
    verify-all|verify_all)          cmd_verify_all "$@" ;;
    window-benchmark|window_benchmark)
                                    cmd_window_benchmark "$@" ;;
    openivm-test|openivm_test)      cmd_openivm_test "$@" ;;
    pins-sync|pins_sync)            cmd_pins_sync "$@" ;;
    help)                           usage ;;
    *)
        echo "[spark-ext/run] Unknown command: $COMMAND" >&2
        echo >&2
        usage >&2
        exit 2
        ;;
esac
