# syntax=docker/dockerfile:1.7

ARG NODE_VERSION

FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 AS openivm-builder

ARG DUCKDB_COMMIT
ARG DUCKDB_REF
ARG LPTS_BRANCH
ARG LPTS_COMMIT
ARG LPTS_REPO
ARG NATIVE_BUILD_JOBS
ARG OPENIVM_BRANCH
ARG OPENIVM_COMMIT
ARG OPENIVM_REPO
ARG UBUNTU_MIRROR

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
      sed -i \
        "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
        /etc/apt/sources.list.d/ubuntu.sources; \
    fi \
    && apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 \
    && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      curl \
      git \
      libssl-dev \
      ninja-build \
      pkg-config \
      python3 \
      python3-pip \
      unzip \
      zip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN --mount=type=cache,target=/root/.gitcache \
    git clone --branch "${OPENIVM_BRANCH}" --depth 50 "${OPENIVM_REPO}" . \
    && git checkout "${OPENIVM_COMMIT}" \
    && git config --global url."https://github.com/".insteadOf "git@github.com:" \
    && git config -f .gitmodules submodule.third_party/lpts.url "${LPTS_REPO}" \
    && git submodule sync --recursive \
    && git submodule update --init --recursive --depth 1 duckdb extension-ci-tools \
    && git -C duckdb fetch --depth 1 origin tag "${DUCKDB_REF}" \
    && git -C duckdb checkout --detach "${DUCKDB_COMMIT}" \
    && test "$(git -C duckdb rev-parse HEAD)" = "${DUCKDB_COMMIT}" \
    && cd third_party/lpts \
    && git init -q \
    && (git remote add origin "${LPTS_REPO}" 2>/dev/null || git remote set-url origin "${LPTS_REPO}") \
    && git fetch --depth 50 origin "${LPTS_BRANCH}" \
    && git checkout "${LPTS_COMMIT}" \
    && git submodule update --init --recursive --depth 1

RUN CMAKE_BUILD_PARALLEL_LEVEL="${NATIVE_BUILD_JOBS}" GEN=ninja make

RUN mkdir -p /artifacts \
    && cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ \
    && cp build/release/duckdb /artifacts/duckdb \
    && chmod +x /artifacts/duckdb \
    && cd /artifacts \
    && sha256sum openivm.duckdb_extension duckdb > SHA256SUMS

FROM eclipse-temurin:17-jdk-noble@sha256:b73c483fd77e77ad71b25cc146de5172bf9cf34339ccd8aad4996e1878280810 AS jdk17

FROM node:${NODE_VERSION}-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57 AS node-runtime

FROM docker:27.5.1-cli@sha256:851f91d241214e7c6db86513b270d58776379aacc5eb9c4a87e5b47115e3065c AS docker-cli

FROM eclipse-temurin:21-jdk-noble@sha256:1afee26c547e1aa89f47404854dd09203b8bd35a2be269e16bdfd69c4a2c2b50 AS devcontainer

ARG NODE_VERSION
ARG NPM_VERSION
ARG GH_CLI_SHA256
ARG GH_CLI_VERSION
ARG SBT_VERSION
ARG TARGETARCH
ARG UBUNTU_MIRROR

RUN if [ "${TARGETARCH}" != "amd64" ]; then \
      echo "openivm-spark-devcontainer supports only linux/amd64; TARGETARCH=${TARGETARCH:-unset}" >&2; \
      exit 1; \
    fi

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/opt/java/jdk-17 \
    NPM_CONFIG_AUDIT=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    OPENIVM_CLI_PATH=/opt/openivm/duckdb \
    OPENIVM_EXTENSION_PATH=/opt/openivm/openivm.duckdb_extension \
    OPENIVM_JAVA_HOME_SPARK_35=/opt/java/jdk-17 \
    OPENIVM_JAVA_HOME_SPARK_41=/opt/java/jdk-21 \
    OPENIVM_NATIVE_DIR=/opt/openivm \
    OPENIVM_SPARK_TARGET=spark-3.5 \
    PATH=/opt/java/jdk-17/bin:/opt/openivm-node-tooling/node_modules/.bin:${PATH}

LABEL org.opencontainers.image.description="OpenIVM Spark development and CI toolchain" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/openivm-spark" \
      org.opencontainers.image.title="openivm-spark-devcontainer"

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
      sed -i \
        "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
        /etc/apt/sources.list.d/ubuntu.sources; \
    fi \
    && apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 \
    && apt-get install -y --no-install-recommends \
      bash \
      build-essential \
      ca-certificates \
      curl \
      git \
      jq \
      less \
      lsof \
      make \
      openssh-client \
      procps \
      python3 \
      python3-pip \
      rsync \
      sudo \
      unzip \
      zip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=jdk17 /opt/java/openjdk /opt/java/jdk-17
COPY --from=node-runtime /usr/local/ /usr/local/
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
COPY --from=openivm-builder /artifacts/ /opt/openivm/

RUN ln -s /opt/java/openjdk /opt/java/jdk-21 \
    && ln -s /opt/java/jdk-17 /opt/java/openjdk-17 \
    && groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash vscode \
    && printf 'vscode ALL=(root) NOPASSWD:ALL\n' > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode \
    && mkdir -p /opt/downloads /opt/openivm-node-tooling /opt/sbt \
    && chown -R vscode:vscode /opt/openivm-node-tooling \
    && curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      -o /opt/downloads/sbt.tgz \
    && printf '%s  %s\n' \
      '23543bc4597b552e8e2c27d695fe672ec235ab8a64f766b37828fb68c6d9d910' \
      '/opt/downloads/sbt.tgz' \
      | sha256sum -c - \
    && tar -xzf /opt/downloads/sbt.tgz -C /opt \
    && GH_CLI_ARCHIVE="/opt/downloads/gh_${GH_CLI_VERSION}_linux_${TARGETARCH}.tar.gz" \
    && curl -fsSL \
      "https://github.com/cli/cli/releases/download/v${GH_CLI_VERSION}/gh_${GH_CLI_VERSION}_linux_${TARGETARCH}.tar.gz" \
      -o "${GH_CLI_ARCHIVE}" \
    && printf '%s  %s\n' "${GH_CLI_SHA256}" "${GH_CLI_ARCHIVE}" | sha256sum -c - \
    && tar -xzf "${GH_CLI_ARCHIVE}" -C /opt/downloads \
    && install -m 0755 \
      "/opt/downloads/gh_${GH_CLI_VERSION}_linux_${TARGETARCH}/bin/gh" \
      /usr/local/bin/gh \
    && rm -rf /opt/downloads \
    && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt \
    && chown -R vscode:vscode /opt/openivm /opt/sbt /home/vscode

WORKDIR /opt/openivm-node-tooling

COPY --chown=vscode:vscode package.json package-lock.json ./

USER vscode

RUN HOME=/home/vscode test "$(node --version)" = "v${NODE_VERSION}" \
    && test "$(npm --version)" = "${NPM_VERSION}" \
    && HOME=/home/vscode npm ci

USER root

RUN for binary in devcontainer nx prettier tsc tsx; do \
      ln -s "/opt/openivm-node-tooling/node_modules/.bin/${binary}" "/usr/local/bin/${binary}"; \
    done

WORKDIR /opt/openivm-sbt

COPY --chown=vscode:vscode spark-ext/.sbtopts ./
COPY --chown=vscode:vscode spark-ext/build.sbt ./
COPY --chown=vscode:vscode spark-ext/project/ ./project/

RUN mkdir -p ivm-common ivm-compiler ivm-executor ivm-extension ivm-it \
    && chown -R vscode:vscode /opt/openivm-sbt

USER vscode

RUN HOME=/home/vscode OPENIVM_SPARK_TARGET=spark-3.5 JAVA_HOME=/opt/java/jdk-17 sbt -batch update \
    && HOME=/home/vscode OPENIVM_SPARK_TARGET=spark-4.1 JAVA_HOME=/opt/java/jdk-21 sbt -batch update

USER root

COPY --chmod=755 spark-ext/dev/docker/entrypoint.sh /usr/local/bin/openivm-entrypoint
COPY --chmod=755 .devcontainer/scripts/post-create.sh /usr/local/share/openivm-spark/post-create.sh

RUN cd /opt/openivm \
    && sha256sum -c SHA256SUMS \
    && /opt/openivm/duckdb --version \
    && /opt/java/jdk-17/bin/java -version \
    && /opt/java/jdk-21/bin/java -version \
    && gh --version \
    && sbt --script-version

ENV HOME=/home/vscode

USER vscode
WORKDIR /workspaces/openivm-spark

ENTRYPOINT ["/usr/local/bin/openivm-entrypoint"]
CMD ["sleep", "infinity"]
