# syntax=docker/dockerfile:1.7

# =============================================================================
#  openivm-spark — multi-stage development image.
#
#  Stages:
#    1. openivm-builder      Builds OpenIVM (with our forked lpts submodule
#                            substituted in) as a DuckDB extension. Produces
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
#    DUCKDB_JDBC_VERSION
#    UBUNTU_MIRROR (optional)
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: openivm-builder
# -----------------------------------------------------------------------------
FROM ubuntu:24.04 AS openivm-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_REPO
ARG OPENIVM_BRANCH=openivm-spark
ARG OPENIVM_COMMIT
ARG LPTS_REPO
ARG LPTS_BRANCH=openivm-spark
ARG LPTS_COMMIT

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
            /etc/apt/sources.list.d/ubuntu.sources; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates \
        python3 python3-pip libssl-dev pkg-config zip unzip curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Clone openivm at the pinned commit on the openivm-spark branch, then
# override the lpts submodule to point at OUR fork so we never need to commit
# a submodule-URL bump into openivm to pick up new lpts work.
RUN --mount=type=cache,target=/root/.gitcache \
    git clone --branch "${OPENIVM_BRANCH}" --depth 50 "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git config -f .gitmodules submodule.third_party/lpts.url "${LPTS_REPO}" && \
    git submodule sync --recursive && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools && \
    (cd third_party/lpts && \
        git init -q && \
        (git remote add origin "${LPTS_REPO}" 2>/dev/null || git remote set-url origin "${LPTS_REPO}") && \
        git fetch --depth 50 origin "${LPTS_BRANCH}" && \
        git checkout "${LPTS_COMMIT}" && \
        git submodule update --init --recursive --depth 1)

# Build (release). The extension-ci-tools makefile produces both the duckdb
# CLI and the loadable .duckdb_extension binary. We bypass the optional
# third_party/benchbase + sqlstorm benchmarks (heavyweight, not on the test
# critical path).
RUN GEN=ninja make -j"$(nproc)"

# Collect deliverables into a stable location.
RUN mkdir -p /artifacts && \
    cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ && \
    cp build/release/duckdb /artifacts/duckdb && \
    chmod +x /artifacts/duckdb && \
    sha256sum /artifacts/openivm.duckdb_extension /artifacts/duckdb > /artifacts/SHA256SUMS

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
# Stage 3: spark-ext — JDK 17 + sbt 1.9.7 runtime.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk AS spark-ext

ARG SBT_VERSION=1.9.7
ARG SCALA_VERSION=2.12.17
ARG DUCKDB_JDBC_VERSION=1.5.2
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu

ENV DEBIAN_FRONTEND=noninteractive
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
COPY --from=openivm-builder /artifacts/openivm.duckdb_extension /opt/openivm/openivm.duckdb_extension
COPY --from=openivm-builder /artifacts/duckdb                   /opt/openivm/duckdb
COPY --from=openivm-builder /artifacts/SHA256SUMS               /opt/openivm/SHA256SUMS

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
