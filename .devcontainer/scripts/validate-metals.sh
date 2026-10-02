#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPOSITORY_ROOT}/spark-ext"

rm -rf .bloop .bsp
OPENIVM_SPARK_TARGET=spark-3.5 \
    JAVA_HOME=/opt/java/jdk-17 \
    sbt -batch bloopInstall

test -n "$(find .bloop -maxdepth 1 -name '*.json' -print -quit)"
