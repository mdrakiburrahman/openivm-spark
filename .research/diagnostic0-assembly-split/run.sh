#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
label=${1:?run label}
out=$PWD/.logs/assembly/$label
mkdir "$out"; mkdir -p .logs/jvm-tmp
export TMPDIR=$PWD/.logs/jvm-tmp SPARK_LOCAL_DIRS=$PWD/.logs/jvm-tmp
export NX_DAEMON=false OPENIVM_TEST_LOG_ROOT="$out" OPENIVM_TEST_FORKS=1 OPENIVM_ASSEMBLY_PROBE=1
export OPENIVM_SBT_OPTS="-Xmx4G -XX:ActiveProcessorCount=2 -Dsbt.task.cpus=2 -Dsbt.server.autostart=false -Djava.io.tmpdir=/proc/$$/cwd/.logs/jvm-tmp -Dfile.encoding=UTF-8"
{
  git rev-parse HEAD
  git diff --stat
  sha256sum /opt/openivm/duckdb /opt/openivm/openivm.duckdb_extension
  date -u; cat /proc/loadavg; ps -p 574332,574877 -o pid,stat,time || :
} > "$out/resource-start.txt"
set +e
npx --no-install nx run spark-ext:test --configuration=spark-4.1 -- 'ivmIt/testOnly org.openivm.spark.parity.AssemblySplitSpec org.openivm.spark.parity.PublicMvSchemaCdfSpec' 2>&1 | tee "$out/nx.log"
status=${PIPESTATUS[0]}
set -e
{ date -u; cat /proc/loadavg; ps -p 574332,574877 -o pid,stat,time || :; } > "$out/resource-end.txt"
grep -rahF '[assembly-probe]' "$out"/test-* > "$out/split.log" || :
exit "$status"
