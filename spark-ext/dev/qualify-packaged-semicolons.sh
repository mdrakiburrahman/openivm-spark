#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  printf 'Usage: %s <spark-3.5|spark-4.1> <assembly.jar> <test-classpath.log> <output-dir>\n' "$0" >&2
  exit 2
fi

script="$(realpath "$0")"
if [[ ${OPENIVM_PACKAGE_NAMESPACE:-0} != 1 ]]; then
  # Hide the native tools privately; the devcontainer user cannot rename /opt/openivm.
  exec unshare --user --map-root-user --mount bash -c '
    set -euo pipefail
    mount --make-rprivate /
    test -d /opt/openivm
    mount -t tmpfs -o size=4096,nosuid,nodev openivm-no-external-native /opt/openivm
    export OPENIVM_PACKAGE_NAMESPACE=1
    exec bash "$1" "${@:2}"
  ' bash "$script" "$@"
fi

target="$1"
assembly="$(realpath "$2")"
classpath_log="$(realpath "$3")"
out="$(realpath -m "$4")"
root="$(git -C "$(dirname "$script")/../.." rev-parse --show-toplevel)"
mkdir -p "$out"
test ! -e /opt/openivm/duckdb
test ! -e /opt/openivm/openivm.duckdb_extension
unset OPENIVM_CLI_PATH OPENIVM_EXTENSION_PATH OPENIVM_NATIVE_DIR OPENIVM_NATIVE_LIBRARY_PATH

case "$target" in
  spark-3.5) java=/opt/java/jdk-17/bin/java ;;
  spark-4.1) java=/opt/java/jdk-21/bin/java ;;
  *) printf 'Unsupported target: %s\n' "$target" >&2; exit 2 ;;
esac

python3 - "$classpath_log" "$assembly" "$root" "$out/classpath.txt" <<'PY'
import pathlib
import re
import sys

log, assembly, root, output = sys.argv[1:]
text = re.sub(r"\x1b\[[0-9;]*m", "", pathlib.Path(log).read_text())
lines = [
    line.strip() for line in text.splitlines()
    if line.startswith("/") and "/ivm-it/target/" in line and "/test-classes" in line
]
assert len(lines) == 1, f"Expected exactly one exported test classpath, got {len(lines)}"
entries = lines[0].split(":")
kept = [
    entry for entry in entries
    if not (entry.startswith(root + "/spark-ext/") and entry.endswith("/classes"))
]
assert len(kept) < len(entries), "No unpackaged production class directories removed"
assert any(entry.endswith("/test-classes") for entry in kept)
pathlib.Path(output).write_text(":".join([assembly] + kept))
print(f"PACKAGED_CLASSPATH removed={len(entries) - len(kept)} target_classes_only=true")
PY

find /tmp -maxdepth 1 -type d -name 'openivm-assets-*' | sort > "$out/assets-before.txt"
mapfile -t jvm_opts < <(sed -n 's/^-J//p' "$root/spark-ext/.sbtopts")
cd "$root/spark-ext"
SPARK_LOCAL_IP=127.0.0.1 "$java" "${jvm_opts[@]}" \
  "-Dlog4j.configurationFile=file:$root/spark-ext/ivm-common/src/test/resources/log4j2.properties" \
  "-Dopenivm.test.log.dir=$out" \
  "-Xlog:class+load=info:file=$out/classload.log" \
  -cp "$(cat "$out/classpath.txt")" \
  org.scalatest.tools.Runner -oD \
  -s org.openivm.spark.parity.MixedCaseCommentCdfSpec \
  2>&1 | tee "$out/runner.log"
find /tmp -maxdepth 1 -type d -name 'openivm-assets-*' | sort > "$out/assets-after.txt"
comm -13 "$out/assets-before.txt" "$out/assets-after.txt" > "$out/new-assets.txt"

python3 - "$assembly" "$out" <<'PY'
import hashlib
import pathlib
import sys
import zipfile

assembly, output = sys.argv[1:]
out = pathlib.Path(output)
loaded = (out / "classload.log").read_text()
for name in (
    "org.openivm.spark.OpenIvmSparkExtensions",
    "org.openivm.spark.compiler.OpenIvmCompiler$",
    "org.openivm.spark.commands.OpenIvmCompilers$",
):
    lines = [line for line in loaded.splitlines() if f"] {name} source: " in line]
    assert lines and all(f"source: file:{assembly}" in line for line in lines), (name, lines)
    print(lines[0])

assets = (out / "new-assets.txt").read_text().splitlines()
assert assets, "No new embedded-native extraction directory observed"
with zipfile.ZipFile(assembly) as jar:
    for name in ("duckdb", "openivm.duckdb_extension"):
        expected = hashlib.sha256(jar.read(f"openivm-native/{name}")).hexdigest()
        for directory in assets:
            path = pathlib.Path(directory) / name
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            assert actual == expected, (str(path), actual, expected)
            print(f"EMBEDDED_NATIVE {actual} {path}")
print("PACKAGED_SEMICOLON_CREATE_REFRESH_PASSED")
PY
