#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$DEV_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/.." && pwd)"
RUNNER="$DEV_DIR/run.sh"
WORK_DIR="$SCRIPT_DIR/.work-$$"
DEFAULT_LOG_TIMESTAMP="focused-runner-$$"
DEFAULT_LOG_DIR="$REPO_ROOT/.logs/test-$DEFAULT_LOG_TIMESTAMP"
LEGACY_TARGET_212="$PROJECT_DIR/ivm-extension/target/scala-2.12"
LEGACY_TARGET_213="$PROJECT_DIR/ivm-extension/target/scala-2.13"

cleanup() {
    rm -rf -- "$WORK_DIR"
    rm -rf -- "$DEFAULT_LOG_DIR"
    rm -rf -- "$LEGACY_TARGET_212" "$LEGACY_TARGET_213"
}
trap cleanup EXIT

mkdir -p "$WORK_DIR/jdk/bin" "$WORK_DIR/jdk21/bin" "$WORK_DIR/bin" "$WORK_DIR/native" \
    "$WORK_DIR/logs" "$WORK_DIR/inventory" "$WORK_DIR/openivm-source"
mkdir -p "$LEGACY_TARGET_212" "$LEGACY_TARGET_213"
touch "$LEGACY_TARGET_212/stale.jar" "$LEGACY_TARGET_213/stale.jar"

cat >"$WORK_DIR/jdk/bin/java" <<'EOF'
#!/usr/bin/env bash
echo 'openjdk version "17.0.12" 2024-07-16' >&2
EOF

cat >"$WORK_DIR/jdk21/bin/java" <<'EOF'
#!/usr/bin/env bash
echo 'openjdk version "21.0.4" 2024-07-16 LTS' >&2
EOF

cat >"$WORK_DIR/bin/sbt" <<'EOF'
#!/usr/bin/env bash
{
    printf 'target=%s\n' "$OPENIVM_SPARK_TARGET"
    printf 'java_home=%s\n' "$JAVA_HOME"
    printf 'native_dir=%s\n' "$OPENIVM_NATIVE_DIR"
    printf 'cli=%s\n' "$OPENIVM_CLI_PATH"
    printf 'extension=%s\n' "$OPENIVM_EXTENSION_PATH"
    printf 'sbt_opts=%s\n' "$SBT_OPTS"
} >"$DIRECT_RUNNER_CAPTURE.env"
printf '%s\n' "$@" >"$DIRECT_RUNNER_CAPTURE.args"
exit "${DIRECT_RUNNER_FAKE_RC:-0}"
EOF

cat >"$WORK_DIR/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '<%s>' "$@" >>"$DEV_DOCKER_CAPTURE"
printf '\n' >>"$DEV_DOCKER_CAPTURE"
EOF

cat >"$WORK_DIR/bin/make" <<'EOF'
#!/usr/bin/env bash
printf 'cwd=%s\n' "$PWD" >"$OPENIVM_MAKE_CAPTURE.env"
printf '%s\n' "$@" >"$OPENIVM_MAKE_CAPTURE.args"
EOF

cat >"$WORK_DIR/native/duckdb" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
: >"$WORK_DIR/native/openivm.duckdb_extension"
: >"$WORK_DIR/openivm-source/Makefile"
chmod +x "$WORK_DIR/jdk/bin/java" "$WORK_DIR/jdk21/bin/java" \
    "$WORK_DIR/bin/sbt" "$WORK_DIR/bin/docker" "$WORK_DIR/bin/make" \
    "$WORK_DIR/native/duckdb"

CAPTURE="$WORK_DIR/verify"
OPENIVM_JAVA_HOME_SPARK_35="$WORK_DIR/jdk" \
OPENIVM_SBT_BIN="$WORK_DIR/bin/sbt" \
OPENIVM_NATIVE_DIR="$WORK_DIR/native" \
OPENIVM_TEST_FORKS=3 \
OPENIVM_TEST_LOG_ROOT="$WORK_DIR/logs" \
OPENIVM_TEST_LOG_TIMESTAMP=20261001-152913 \
DIRECT_RUNNER_CAPTURE="$CAPTURE" \
    "$RUNNER" --target spark-3.5 --skip-pins-sync verify -Dprobe=true \
    >"$WORK_DIR/verify.stdout"

[[ ! -e "$LEGACY_TARGET_212" ]]
[[ ! -e "$LEGACY_TARGET_213" ]]

cat >"$WORK_DIR/expected.args" <<'EOF'
-Dprobe=true
ivmExtension/clean
scalafmtCheckAll
scalafmtSbtCheck
compile
Test/compile
testInventory
ivmExtension/assembly
ivmExtension/testPackagedAssemblyGuard
test
EOF
cmp "$WORK_DIR/expected.args" "$CAPTURE.args"
grep -Fqx "target=spark-3.5" "$CAPTURE.env"
grep -Fqx "java_home=$WORK_DIR/jdk" "$CAPTURE.env"
grep -Fqx "native_dir=$WORK_DIR/native" "$CAPTURE.env"
grep -Fq -- "-Dopenivm.test.forks=3" "$CAPTURE.env"
grep -Fq -- "-Dopenivm.test.log.dir=$WORK_DIR/logs/test-20261001-152913" "$CAPTURE.env"
[[ -d "$WORK_DIR/logs/test-20261001-152913" ]]

OPENIVM_JAVA_HOME_SPARK_35="$WORK_DIR/jdk" \
OPENIVM_SBT_BIN="$WORK_DIR/bin/sbt" \
OPENIVM_NATIVE_DIR="$WORK_DIR/native" \
OPENIVM_TEST_LOG_TIMESTAMP="$DEFAULT_LOG_TIMESTAMP" \
DIRECT_RUNNER_CAPTURE="$WORK_DIR/default-log-root" \
    "$RUNNER" --target spark-3.5 test-suite >"$WORK_DIR/default-log-root.stdout"
[[ -d "$DEFAULT_LOG_DIR" ]]
grep -Fq -- "-Dopenivm.test.log.dir=$DEFAULT_LOG_DIR" "$WORK_DIR/default-log-root.env"
grep -Fq "Test logs -> .logs/test-$DEFAULT_LOG_TIMESTAMP/" "$WORK_DIR/default-log-root.stdout"

TARGET=spark-4.1 \
OPENIVM_JAVA_HOME_SPARK_41="$WORK_DIR/jdk21" \
OPENIVM_SBT_BIN="$WORK_DIR/bin/sbt" \
OPENIVM_NATIVE_DIR="$WORK_DIR/native" \
DIRECT_RUNNER_CAPTURE="$WORK_DIR/build41" \
    "$RUNNER" build >"$WORK_DIR/build41.stdout"
grep -Fqx "target=spark-4.1" "$WORK_DIR/build41.env"
grep -Fqx "java_home=$WORK_DIR/jdk21" "$WORK_DIR/build41.env"
grep -Fqx "compile" "$WORK_DIR/build41.args"

printf 'a.Test\nb.Test\n' >"$WORK_DIR/inventory/spark-3.5.txt"
cp "$WORK_DIR/inventory/spark-3.5.txt" "$WORK_DIR/inventory/spark-4.1.txt"
OPENIVM_TEST_INVENTORY_DIR="$WORK_DIR/inventory" \
    "$RUNNER" compare-test-inventory >"$WORK_DIR/compare.stdout"

printf 'different.Test\n' >"$WORK_DIR/inventory/spark-4.1.txt"
set +e
OPENIVM_TEST_INVENTORY_DIR="$WORK_DIR/inventory" \
    "$RUNNER" compare-test-inventory \
    >"$WORK_DIR/compare-failure.stdout" 2>"$WORK_DIR/compare-failure.stderr"
compare_rc=$?
set -e
[[ "$compare_rc" -eq 1 ]]
grep -Fq "discovered different tests" "$WORK_DIR/compare-failure.stderr"

set +e
OPENIVM_JAVA_HOME_SPARK_35="$WORK_DIR/jdk" \
OPENIVM_SBT_BIN="$WORK_DIR/bin/sbt" \
OPENIVM_NATIVE_DIR="$WORK_DIR/native" \
OPENIVM_TEST_LOG_ROOT="$WORK_DIR/logs" \
OPENIVM_TEST_LOG_TIMESTAMP=20261001-152914 \
DIRECT_RUNNER_CAPTURE="$WORK_DIR/failure" \
DIRECT_RUNNER_FAKE_RC=37 \
    "$RUNNER" --target spark-3.5 test-suite \
    >"$WORK_DIR/failure.stdout" 2>"$WORK_DIR/failure.stderr"
failure_rc=$?
set -e
[[ "$failure_rc" -eq 37 ]]
grep -Fq "FAILED (exit 37)" "$WORK_DIR/failure.stderr"
grep -Fq "$WORK_DIR/logs/test-20261001-152914/" "$WORK_DIR/failure.stderr"

PATH="$WORK_DIR/bin:$PATH" \
OPENIVM_TEST_FORKS=5 \
DEV_DOCKER_CAPTURE="$WORK_DIR/docker.args" \
    "$DEV_DIR/dev.sh" --target spark-4.1 lint >"$WORK_DIR/dev-lint.stdout"
grep -Fq "<-e><OPENIVM_TEST_FORKS>" "$WORK_DIR/docker.args"
grep -Fq "<-e><OPENIVM_JAVA_HOME_SPARK_41=/opt/java/openjdk>" "$WORK_DIR/docker.args"
grep -Fq "<-e><OPENIVM_TEST_LOG_ROOT=/work/spark-ext/.logs>" "$WORK_DIR/docker.args"
grep -Fq "<build><./dev/run.sh><--target><spark-4.1><lint>" "$WORK_DIR/docker.args"

PATH="$WORK_DIR/bin:$PATH" \
DEV_DOCKER_CAPTURE="$WORK_DIR/docker35.args" \
    "$DEV_DIR/dev.sh" --target spark-3.5 lint >"$WORK_DIR/dev-lint35.stdout"
grep -Fq "<-e><OPENIVM_JAVA_HOME_SPARK_35=/opt/java/openjdk>" "$WORK_DIR/docker35.args"

PATH="$WORK_DIR/bin:$PATH" \
OPENIVM_JAVA_HOME_SPARK_41=/custom/jdk-21 \
DEV_DOCKER_CAPTURE="$WORK_DIR/docker-override.args" \
    "$DEV_DIR/dev.sh" --target spark-4.1 lint >"$WORK_DIR/dev-lint-override.stdout"
grep -Fq "<-e><OPENIVM_JAVA_HOME_SPARK_41=/custom/jdk-21>" "$WORK_DIR/docker-override.args"

LINKED_REPO="$WORK_DIR/linked-repo"
LINKED_GIT_DIR="$WORK_DIR/linked-git-dir"
mkdir -p "$LINKED_REPO/spark-ext/dev/targets"
git init --quiet "$LINKED_REPO"
mv "$LINKED_REPO/.git" "$LINKED_GIT_DIR"
printf 'gitdir: %s\n' "$LINKED_GIT_DIR" >"$LINKED_REPO/.git"
cp "$RUNNER" "$LINKED_REPO/spark-ext/dev/run.sh"
cp "$DEV_DIR/pins.env" "$LINKED_REPO/spark-ext/dev/pins.env"
cp "$DEV_DIR/targets/spark-3.5.env" "$LINKED_REPO/spark-ext/dev/targets/spark-3.5.env"
cat >"$LINKED_REPO/spark-ext/dev/dev.sh" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$LINKED_PINS_CAPTURE"
EOF
chmod +x "$LINKED_REPO/spark-ext/dev/run.sh" "$LINKED_REPO/spark-ext/dev/dev.sh"
LINKED_PINS_CAPTURE="$WORK_DIR/linked-pins.args" \
    "$LINKED_REPO/spark-ext/dev/run.sh" --target spark-3.5 pins-sync
printf '%s\n' --target spark-3.5 pins-sync >"$WORK_DIR/expected-linked-pins.args"
cmp "$WORK_DIR/expected-linked-pins.args" "$WORK_DIR/linked-pins.args"

PATH="$WORK_DIR/bin:$PATH" \
OPENIVM_SOURCE_DIR="$WORK_DIR/openivm-source" \
OPENIVM_MAKE_CAPTURE="$WORK_DIR/make" \
    "$RUNNER" openivm-test FILTER=smoke >"$WORK_DIR/openivm-test.stdout"
grep -Fqx "cwd=$WORK_DIR/openivm-source" "$WORK_DIR/make.env"
printf 'test\nFILTER=smoke\n' >"$WORK_DIR/expected-make.args"
cmp "$WORK_DIR/expected-make.args" "$WORK_DIR/make.args"

set +e
"$RUNNER" --target spark-9.9 lint >"$WORK_DIR/target.stdout" 2>"$WORK_DIR/target.stderr"
target_rc=$?
set -e
[[ "$target_rc" -eq 2 ]]
grep -Fq "unsupported target 'spark-9.9'" "$WORK_DIR/target.stderr"

echo "run-test: PASS"
