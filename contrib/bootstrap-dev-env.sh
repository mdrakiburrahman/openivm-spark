#!/bin/bash
# ============================================================================
# contrib/bootstrap-dev-env.sh — full developer-host bootstrap.
#
#   Bootstraps a Linux Devbox host idempotently.
#   If your Devbox restarts, rerun this script.
#
# All reusable helpers live in contrib/common.sh; this file only orchestrates
# the dev-relevant sequence (interactive logins, WSL path-stripping, etc.).
# CI uses contrib/bootstrap-ci.sh which is a Docker-only subset.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

# Keep the WSL bridge to Windows VS Code before removing Windows paths below.
CODE_BIN="$(command -v code 2>/dev/null || true)"

# Pinned host-side JVM/Scala toolchain. Keep these aligned with spark-ext.
TOOLCHAIN_ROOT="/opt/openivm-toolchain"
JAVA_VERSION="17.0.20.1+1"
JAVA_ARCHIVE_VERSION="17.0.20.1_1"
JAVA_SHA256="3808d1d15e3ec6bd5b84057fb5d84c33d8a1536a258146bcea2e603fc726e08e"
MAVEN_VERSION="3.9.16"
MAVEN_SHA512="831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6"
COURSIER_VERSION="2.1.25"
COURSIER_SHA256="415fa0e9514dcffd521eca13dbf05bddaf3f542795faec728021defcaa25abdf"
SBT_VERSION="1.9.7"
SBT_SHA256="23543bc4597b552e8e2c27d695fe672ec235ab8a64f766b37828fb68c6d9d910"
SCALA_VERSION="2.12.17"
METALS_SERVER_VERSION="1.6.9"
METALS_VSCODE_VERSION="1.71.0"
SCALA_SYNTAX_VSCODE_VERSION="0.5.10"
JAVA_PACK_VSCODE_VERSION="0.31.1"
JAVA_VSCODE_VERSION="1.56.0"
JAVA_DEBUG_VSCODE_VERSION="0.59.0"
JAVA_TEST_VSCODE_VERSION="0.46.0"
JAVA_MAVEN_VSCODE_VERSION="0.45.3"
JAVA_GRADLE_VSCODE_VERSION="3.18.0"
JAVA_DEPENDENCY_VSCODE_VERSION="0.27.6"

ensure_java() {
    local java_home="${TOOLCHAIN_ROOT}/jdk-${JAVA_VERSION}"
    if [[ -x "${java_home}/bin/java" ]] &&
        "${java_home}/bin/java" -version 2>&1 | head -1 | grep -Fq "\"17.0.20.1\""; then
        cmn_log dev "Temurin JDK ${JAVA_VERSION} already installed"
    else
        cmn_log dev "installing Temurin JDK ${JAVA_VERSION}..."
        local tmp
        tmp="$(mktemp -d)"
        curl -fsSL \
            "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-${JAVA_VERSION/+/%2B}/OpenJDK17U-jdk_x64_linux_hotspot_${JAVA_ARCHIVE_VERSION}.tar.gz" \
            -o "${tmp}/jdk.tar.gz"
        echo "${JAVA_SHA256}  ${tmp}/jdk.tar.gz" | sha256sum -c -
        sudo mkdir -p "${TOOLCHAIN_ROOT}"
        sudo tar -xzf "${tmp}/jdk.tar.gz" -C "${TOOLCHAIN_ROOT}"
        rm -rf "${tmp}"
    fi

    sudo ln -sfn "${java_home}" "${TOOLCHAIN_ROOT}/java"
    local executable
    for executable in java javac jar javadoc javap; do
        sudo ln -sfn "${TOOLCHAIN_ROOT}/java/bin/${executable}" "/usr/local/bin/${executable}"
    done
    export JAVA_HOME="${TOOLCHAIN_ROOT}/java"
}

ensure_maven() {
    local maven_home="${TOOLCHAIN_ROOT}/apache-maven-${MAVEN_VERSION}"
    if [[ -x "${maven_home}/bin/mvn" ]] &&
        "${maven_home}/bin/mvn" --version | head -1 | grep -Fq "Apache Maven ${MAVEN_VERSION}"; then
        cmn_log dev "Maven ${MAVEN_VERSION} already installed"
    else
        cmn_log dev "installing Maven ${MAVEN_VERSION}..."
        local tmp
        tmp="$(mktemp -d)"
        curl -fsSL \
            "https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
            -o "${tmp}/maven.tar.gz"
        echo "${MAVEN_SHA512}  ${tmp}/maven.tar.gz" | sha512sum -c -
        sudo mkdir -p "${TOOLCHAIN_ROOT}"
        sudo tar -xzf "${tmp}/maven.tar.gz" -C "${TOOLCHAIN_ROOT}"
        rm -rf "${tmp}"
    fi

    sudo ln -sfn "${maven_home}" "${TOOLCHAIN_ROOT}/maven"
    sudo ln -sfn "${TOOLCHAIN_ROOT}/maven/bin/mvn" /usr/local/bin/mvn
    export MAVEN_HOME="${TOOLCHAIN_ROOT}/maven"
}

ensure_coursier() {
    if cmn_has cs && [[ "$(cs version 2>/dev/null)" == "${COURSIER_VERSION}" ]]; then
        cmn_log dev "Coursier ${COURSIER_VERSION} already installed"
        return 0
    fi

    cmn_log dev "installing Coursier ${COURSIER_VERSION}..."
    local tmp
    tmp="$(mktemp -d)"
    curl -fsSL \
        "https://github.com/coursier/coursier/releases/download/v${COURSIER_VERSION}/cs-x86_64-pc-linux.gz" \
        -o "${tmp}/cs.gz"
    echo "${COURSIER_SHA256}  ${tmp}/cs.gz" | sha256sum -c -
    gzip -dc "${tmp}/cs.gz" > "${tmp}/cs"
    chmod +x "${tmp}/cs"
    sudo install -m 0755 "${tmp}/cs" /usr/local/bin/cs
    rm -rf "${tmp}"
}

ensure_scala_toolchain() {
    if cmn_has scala && scala -version 2>&1 | grep -Fq "version ${SCALA_VERSION}" &&
        cmn_has scalac && scalac -version 2>&1 | grep -Fq "version ${SCALA_VERSION}" &&
        cmn_has metals && metals --version 2>&1 | grep -Fq "metals ${METALS_SERVER_VERSION}"; then
        cmn_log dev "Scala ${SCALA_VERSION} and Metals ${METALS_SERVER_VERSION} already installed"
        return 0
    fi

    cmn_log dev "installing pinned Scala tooling..."
    local tmp
    tmp="$(mktemp -d)"
    JAVA_HOME="${JAVA_HOME}" PATH="${JAVA_HOME}/bin:${PATH}" \
        cs install --install-dir "${tmp}/bin" --force \
        "scala:${SCALA_VERSION}" \
        "scalac:${SCALA_VERSION}" \
        "metals:${METALS_SERVER_VERSION}"
    sudo install -m 0755 "${tmp}/bin/scala" "${tmp}/bin/scalac" "${tmp}/bin/metals" /usr/local/bin/
    rm -rf "${tmp}"
}

ensure_sbt() {
    local sbt_home="${TOOLCHAIN_ROOT}/sbt-${SBT_VERSION}"
    if [[ -x "${sbt_home}/bin/sbt" ]]; then
        cmn_log dev "sbt ${SBT_VERSION} already installed"
    else
        cmn_log dev "installing sbt ${SBT_VERSION}..."
        local tmp
        tmp="$(mktemp -d)"
        curl -fsSL \
            "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
            -o "${tmp}/sbt.tgz"
        echo "${SBT_SHA256}  ${tmp}/sbt.tgz" | sha256sum -c -
        mkdir -p "${tmp}/extracted"
        tar -xzf "${tmp}/sbt.tgz" --strip-components=1 -C "${tmp}/extracted"
        sudo mkdir -p "${sbt_home}"
        sudo cp -a "${tmp}/extracted/." "${sbt_home}/"
        rm -rf "${tmp}"
    fi
    sudo ln -sfn "${sbt_home}" "${TOOLCHAIN_ROOT}/sbt"
    sudo ln -sfn "${TOOLCHAIN_ROOT}/sbt/bin/sbt" /usr/local/bin/sbt
}

ensure_vscode_extension() {
    local extension="$1"
    local version="$2"
    if "${CODE_BIN}" --list-extensions --show-versions 2>/dev/null |
        grep -Fqx "${extension}@${version}"; then
        cmn_log dev "VS Code extension ${extension}@${version} already installed"
        return 0
    fi
    cmn_log dev "installing VS Code extension ${extension}@${version}..."
    "${CODE_BIN}" --install-extension "${extension}@${version}" --force
}

ensure_vscode_extensions() {
    if [[ -z "${CODE_BIN}" ]]; then
        cmn_log dev "FATAL: VS Code CLI 'code' was not found before WSL PATH sanitization" >&2
        return 1
    fi
    ensure_vscode_extension "scala-lang.scala" "${SCALA_SYNTAX_VSCODE_VERSION}"
    ensure_vscode_extension "scalameta.metals" "${METALS_VSCODE_VERSION}"
    ensure_vscode_extension "vscjava.vscode-java-pack" "${JAVA_PACK_VSCODE_VERSION}"
    ensure_vscode_extension "redhat.java" "${JAVA_VSCODE_VERSION}"
    ensure_vscode_extension "vscjava.vscode-java-debug" "${JAVA_DEBUG_VSCODE_VERSION}"
    ensure_vscode_extension "vscjava.vscode-java-test" "${JAVA_TEST_VSCODE_VERSION}"
    ensure_vscode_extension "vscjava.vscode-maven" "${JAVA_MAVEN_VSCODE_VERSION}"
    ensure_vscode_extension "vscjava.vscode-gradle" "${JAVA_GRADLE_VSCODE_VERSION}"
    ensure_vscode_extension "vscjava.vscode-java-dependency" "${JAVA_DEPENDENCY_VSCODE_VERSION}"
}

write_toolchain_profile() {
    local target="/etc/profile.d/openivm-toolchain.sh"
    local tmp
    tmp="$(mktemp)"
    cat > "${tmp}" <<EOF
export JAVA_HOME="${TOOLCHAIN_ROOT}/java"
export MAVEN_HOME="${TOOLCHAIN_ROOT}/maven"
export PATH="/usr/local/bin:\${PATH}"
EOF
    if [[ ! -f "${target}" ]] || ! cmp -s "${tmp}" "${target}"; then
        sudo install -m 0644 -o root -g root "${tmp}" "${target}"
        cmn_log dev "wrote ${target}"
    else
        cmn_log dev "${target} already up to date"
    fi
    rm -f "${tmp}"
}

cmn_ensure_passwordless_sudo

# Strip WSL-mounted Windows paths so we don't accidentally pick up the
#    Windows az / gh executables (slower and don't share state with the
#    Linux-side dev image).
export PATH="$(cmn_strip_windows_paths)"

cmn_ensure_jq
cmn_apt_install ca-certificates curl gzip tar
ensure_java
ensure_maven
ensure_coursier
ensure_scala_toolchain
ensure_sbt
write_toolchain_profile
ensure_vscode_extensions
cmn_ensure_docker
cmn_configure_docker_daemon
cmn_kill_running_containers
cmn_ensure_az_cli
cmn_ensure_az_login
cmn_ensure_gh_cli
cmn_ensure_gh_login
cmn_ensure_terraform

echo
echo "Java:       $(java -version 2>&1 | head -1)"
echo "Maven:      $(mvn --version | head -1)"
echo "Coursier:   $(cs version)"
echo "Scala:      $(scala -version 2>&1 | head -1)"
echo "sbt:        $(sbt --script-version 2>&1 | tail -1)"
echo "Metals:     $(metals --version 2>&1 | grep -m1 '^metals ')"
echo "Docker:     $(docker --version)"
echo "GitHub CLI: $(gh --version | head -1)"
echo "Azure CLI:  $(az --version | head -1)"
echo "Terraform:  $(terraform version | head -1)"