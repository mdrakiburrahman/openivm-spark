# syntax=docker/dockerfile:1.7

# =============================================================================
#  openivm-spark — multi-stage development image.
#
#  Stages:
#    1. openivm-builder      Builds OpenIVM with the explicitly pinned LPTS
#                            sources as a DuckDB extension. Produces
#                            /artifacts/openivm.duckdb_extension and the
#                            statically-linked duckdb CLI.
#    2. openivm-tester       Inherits stage 1's full build tree so `make test`
#                            can run the upstream sqllogictest suite + our new
#                            compile_refresh tests. Used as the docker-compose
#                            `openivm-test` service target.
#    3. spark-ext            Runtime image. JDK 17 + sbt + scala. Bind-mounts
#                            /home/mdrrahman/openivm-spark/spark-ext at /work
#                            and the prebuilt extension at /opt/openivm.
#
#  Build args (defaults match spark-ext/dev/pins.env):
#    OPENIVM_REPO, OPENIVM_COMMIT
#    LPTS_REPO,    LPTS_COMMIT
#    JDK_VERSION,  SBT_VERSION, SCALA_VERSION
#    DUCKDB_REF, DUCKDB_COMMIT, NATIVE_BUILD_JOBS
#    DUCKDB_JDBC_VERSION
#    UBUNTU_MIRROR (optional)
# =============================================================================

ARG JDK_VERSION=17

# -----------------------------------------------------------------------------
# Stage 1: openivm-builder
# -----------------------------------------------------------------------------
FROM ubuntu:22.04 AS openivm-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_REPO
ARG OPENIVM_BRANCH=main
ARG OPENIVM_COMMIT
ARG LPTS_REPO
ARG LPTS_BRANCH=main
ARG LPTS_COMMIT
ARG DUCKDB_REF=v2.0.0
ARG DUCKDB_COMMIT=d54cbbe3df17e8e5ce68312f483b4155836112aa
ARG NATIVE_BUILD_JOBS=8

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
            /etc/apt/sources.list; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        build-essential clang-15 cmake ninja-build git ca-certificates \
        python3 python3-pip libssl-dev pkg-config zip unzip curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Clone OpenIVM at the pinned commit and use its matching DuckDB and LPTS
# submodules. Check the redundant pins in pins.env rather than overriding them.
RUN --mount=type=cache,target=/root/.gitcache \
    git clone --branch "${OPENIVM_BRANCH}" --depth 50 "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools third_party/lpts && \
    test "$(git -C duckdb rev-parse HEAD)" = "${DUCKDB_COMMIT}" && \
    test "$(git -C third_party/lpts rev-parse HEAD)" = "${LPTS_COMMIT}"

# Build (release). The extension-ci-tools makefile produces both the duckdb
# CLI and the loadable .duckdb_extension binary. We bypass the optional
# third_party/benchbase + sqlstorm benchmarks (heavyweight, not on the test
# critical path).
RUN CC=clang-15 CXX=clang++-15 CMAKE_BUILD_PARALLEL_LEVEL="${NATIVE_BUILD_JOBS}" GEN=ninja make

# Collect deliverables into a stable location.
RUN mkdir -p /artifacts && \
    cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ && \
    cp build/release/duckdb /artifacts/duckdb && \
    cp -L "$(g++ -print-file-name=libstdc++.so.6)" /artifacts/libstdc++.so.6 && \
    cp -L "$(gcc -print-file-name=libgcc_s.so.1)" /artifacts/libgcc_s.so.1 && \
    chmod +x /artifacts/duckdb && \
    sha256sum /artifacts/openivm.duckdb_extension /artifacts/duckdb > /artifacts/SHA256SUMS

# Keep the native compiler usable on stock Ubuntu 22.04, including managed
# Spark drivers whose system C++ runtime is older than the build host's.
FROM ubuntu:22.04 AS native-runtime-check
COPY --from=openivm-builder /artifacts/ /artifacts/
RUN /artifacts/duckdb -unsigned -noheader -csv -c "LOAD '/artifacts/openivm.duckdb_extension'; \
    SET openivm_files_path='/tmp'; CREATE TABLE openivm_runtime_source(id INTEGER); \
    CREATE MATERIALIZED VIEW openivm_runtime_probe AS SELECT id FROM openivm_runtime_source; \
    SELECT DISTINCT refresh_type_name FROM openivm_compile_with_facts('openivm_runtime_probe', \
    '{\"target_dialect\":\"spark\",\"compile_only\":true}');" | grep -Fx SIMPLE_PROJECTION

# -----------------------------------------------------------------------------
# Stage 2: openivm-tester — `make test` over the OpenIVM source tree.
#
# This stage exists so `docker compose run --rm openivm-test` exercises the
# upstream sqllogictest suite (including our new compile_refresh.test) inside
# CI. It keeps the heavy build cache from stage 1.
# -----------------------------------------------------------------------------
FROM openivm-builder AS openivm-tester
WORKDIR /src
CMD ["make", "test"]

# -----------------------------------------------------------------------------
# Stage 3: spark-ext — target-selected JDK + sbt runtime.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:${JDK_VERSION}-jdk AS spark-ext

ARG OPENIVM_SPARK_TARGET=spark-3.5
ARG JDK_VERSION
ARG SBT_VERSION=1.9.7
ARG SCALA_VERSION=2.12.17
ARG DUCKDB_JDBC_VERSION=2.0.0.1
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu

ENV DEBIAN_FRONTEND=noninteractive
ENV OPENIVM_SPARK_TARGET=${OPENIVM_SPARK_TARGET}
ENV JDK_VERSION=${JDK_VERSION}
ENV SBT_VERSION=${SBT_VERSION}
ENV SCALA_VERSION=${SCALA_VERSION}
ENV DUCKDB_JDBC_VERSION=${DUCKDB_JDBC_VERSION}
ENV OPENIVM_EXTENSION_PATH=/opt/openivm/openivm.duckdb_extension

# JVM module-opens block (RESEARCH §10 jvmModuleOpts).
#
# We intentionally do NOT set `JAVA_TOOL_OPTIONS`/`JDK_JAVA_OPTIONS`/`_JAVA_OPTIONS`
# at the container level — every JDK 17 launcher echoes the contents of those
# vars to stderr on startup ("Picked up JAVA_TOOL_OPTIONS: …" / "NOTE: Picked up
# JDK_JAVA_OPTIONS: …"), which sbt routes to its `[error]` channel and clutters
# every test run.
#
# The `--add-opens` / `--add-exports` flags are instead provided silently via:
#   * `spark-ext/.sbtopts` (`-J--add-opens=...`) for the sbt JVM itself
#   * `Settings.scala::jvmModuleOpts` + `Test/javaOptions` for every forked test JVM
#   * `spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions`  when
#     used with spark-shell / spark-submit (caller's responsibility — see README).

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
            /etc/apt/sources.list.d/ubuntu.sources; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg git make procps less jq && \
    rm -rf /var/lib/apt/lists/*

# Install sbt from sbt-launcher (no apt-repo dependency to keep things hermetic).
RUN mkdir -p /opt/sbt && \
    curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
        -o /tmp/sbt.tgz && \
    tar -xzf /tmp/sbt.tgz -C /opt && \
    rm /tmp/sbt.tgz && \
    ln -sf /opt/sbt/bin/sbt /usr/local/bin/sbt && \
    sbt --version || true   # warm cache on first run; ignore exit code

# Copy openivm artifacts from the builder stage.
COPY --from=native-runtime-check /artifacts/openivm.duckdb_extension /opt/openivm/openivm.duckdb_extension
COPY --from=native-runtime-check /artifacts/duckdb                   /opt/openivm/duckdb
COPY --from=native-runtime-check /artifacts/libstdc++.so.6         /opt/openivm/libstdc++.so.6
COPY --from=native-runtime-check /artifacts/libgcc_s.so.1          /opt/openivm/libgcc_s.so.1
COPY --from=native-runtime-check /artifacts/SHA256SUMS               /opt/openivm/SHA256SUMS

# Sanity check the artifact landed.
RUN ls -l /opt/openivm && /opt/openivm/duckdb --version

# Source mount point (overridden via docker compose to bind /work/spark-ext).
WORKDIR /work/spark-ext

# Pre-cache the sbt launcher dependencies so subsequent sbt runs are fast.
# We do this against an empty project so the first real `sbt compile` doesn't
# pull from Maven Central inside the inner loop.
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
