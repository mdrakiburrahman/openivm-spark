import { existsSync, statSync } from 'node:fs';
import { join } from 'node:path';

import {
  smokeContainerLabel,
  sourceRepository,
  supportedImagePlatform,
  workspaceRoot,
} from './constants.js';
import {
  readEnvFile,
  readPinnedDevDependency,
  readToolchainVersions,
  withoutRegistrySecrets,
} from './env.js';
import { imageReference } from './image.js';
import { CommandRunner, ProcessFailure, runCommand } from './process.js';

export interface SmokeOptions {
  logger?: (message: string) => void;
  reference?: string;
  root?: string;
  runner?: CommandRunner;
}

export interface CleanupOptions {
  logger?: (message: string) => void;
  runner?: CommandRunner;
}

export interface RuntimeCheck {
  name: string;
  script: string;
}

export function runtimeChecks(root: string = workspaceRoot): RuntimeCheck[] {
  const toolchain = readToolchainVersions(root);
  const nxVersion = readPinnedDevDependency(root, 'nx');
  const devcontainerVersion = readPinnedDevDependency(root, '@devcontainers/cli');
  const typescriptVersion = readPinnedDevDependency(root, 'typescript');
  const sbtVersion = readEnvFile(join(root, 'spark-ext/dev/pins.env')).SBT_VERSION;
  const ghVersion = readEnvFile(join(root, '.devcontainer/toolchain.env')).GH_CLI_VERSION;
  const spark35 = readEnvFile(join(root, 'spark-ext/dev/targets/spark-3.5.env'));
  const spark41 = readEnvFile(join(root, 'spark-ext/dev/targets/spark-4.1.env'));
  if (!sbtVersion) {
    throw new Error('spark-ext/dev/pins.env must define SBT_VERSION.');
  }
  if (!ghVersion) {
    throw new Error('.devcontainer/toolchain.env must define GH_CLI_VERSION.');
  }
  if (!spark35.SCALA_VERSION || !spark35.JDK_VERSION) {
    throw new Error(
      'spark-ext/dev/targets/spark-3.5.env must define JDK_VERSION and SCALA_VERSION.',
    );
  }
  if (!spark41.SCALA_VERSION || !spark41.JDK_VERSION) {
    throw new Error(
      'spark-ext/dev/targets/spark-4.1.env must define JDK_VERSION and SCALA_VERSION.',
    );
  }
  return [
    {
      name: 'non-root user and workspace',
      script:
        'test "$(id -un)" = vscode && test "$HOME" = /home/vscode && test "$(pwd)" = /workspaces/openivm-spark && test -f package-lock.json',
    },
    {
      name: 'pinned Node and repository tooling',
      script: [
        `test "$(node --version)" = "v${toolchain.node}"`,
        `test "$(npm --version)" = "${toolchain.npm}"`,
        `node -e "const p=require('/opt/openivm-node-tooling/node_modules/nx/package.json');if(p.version!=='${nxVersion}')process.exit(1)"`,
        `node -e "const p=require('/opt/openivm-node-tooling/node_modules/@devcontainers/cli/package.json');if(p.version!=='${devcontainerVersion}')process.exit(1)"`,
        `node -e "const p=require('/opt/openivm-node-tooling/node_modules/typescript/package.json');if(p.version!=='${typescriptVersion}')process.exit(1)"`,
        'npx --no-install nx --version >/dev/null',
        'npx --no-install devcontainer --version >/dev/null',
        'tsc --version >/dev/null',
      ].join(' && '),
    },
    {
      name: 'pinned GitHub CLI',
      script: `gh --version | head -n 1 | grep -F 'gh version ${ghVersion}'`,
    },
    {
      name: 'JDKs, sbt, and native OpenIVM artifacts',
      script: [
        'test "$JAVA_HOME" = /opt/java/jdk-17',
        `test "$OPENIVM_JAVA_HOME_SPARK_35" = /opt/java/jdk-17`,
        `test "$OPENIVM_JAVA_HOME_SPARK_41" = /opt/java/jdk-21`,
        `/opt/java/jdk-17/bin/java -version 2>&1 | grep -F '${spark35.JDK_VERSION}.' >/dev/null`,
        `/opt/java/jdk-21/bin/java -version 2>&1 | grep -F '${spark41.JDK_VERSION}.' >/dev/null`,
        `test "$(sbt --script-version)" = "${sbtVersion}"`,
        'cd /opt/openivm',
        'sha256sum -c SHA256SUMS',
        './duckdb --version',
      ].join(' && '),
    },
    {
      name: 'lifecycle hook',
      script:
        'test -x /usr/local/share/openivm-spark/post-create.sh && bash .devcontainer/scripts/post-create.sh',
    },
    {
      name: 'warmed Spark 3.5 and Spark 4.1 dependency caches',
      script: [
        'test -d /home/vscode/.sbt',
        'test -d /home/vscode/.cache/coursier',
        `cd spark-ext && OPENIVM_SPARK_TARGET=spark-3.5 JAVA_HOME=/opt/java/jdk-17 sbt -batch 'show scalaVersion' | grep -F '${spark35.SCALA_VERSION}'`,
        `OPENIVM_SPARK_TARGET=spark-4.1 JAVA_HOME=/opt/java/jdk-21 sbt -batch 'show scalaVersion' | grep -F '${spark41.SCALA_VERSION}'`,
      ].join(' && '),
    },
    {
      name: 'host Docker socket access',
      script: "docker version --format '{{.Client.Version}}/{{.Server.Version}}'",
    },
  ];
}

export function cleanupSmokeContainers(options: CleanupOptions = {}): string[] {
  const runner = options.runner ?? runCommand;
  const logger = options.logger ?? console.log;
  const result = runner({
    args: ['ps', '--all', '--quiet', '--filter', `label=${smokeContainerLabel}`],
    capture: true,
    command: 'docker',
    env: withoutRegistrySecrets(process.env),
    log: false,
  });
  const containerIds = result.stdout
    .split(/\r?\n/u)
    .map((containerId) => containerId.trim())
    .filter((containerId) => containerId.length > 0);
  if (containerIds.length === 0) {
    logger('No stale devcontainer smoke containers found.');
    return [];
  }

  const removal = runner({
    args: ['rm', '--force', ...containerIds],
    capture: true,
    check: false,
    command: 'docker',
    env: withoutRegistrySecrets(process.env),
    log: false,
  });
  const removalDetail = `${removal.stderr}\n${removal.stdout}`.toLowerCase();
  if (removal.status !== 0 && !removalDetail.includes('no such container')) {
    throw new ProcessFailure(
      `docker rm --force ${containerIds.join(' ')}`,
      removal.status,
      removal.stderr,
    );
  }
  logger(`Removed ${containerIds.length} stale devcontainer smoke container(s).`);
  return containerIds;
}

export function smokeTestImage(options: SmokeOptions = {}): void {
  const root = options.root ?? workspaceRoot;
  const runner = options.runner ?? runCommand;
  const logger = options.logger ?? console.log;
  const reference = options.reference ?? imageReference(root);
  const dockerSocket = '/var/run/docker.sock';
  if (!existsSync(dockerSocket)) {
    throw new Error(`Docker socket is unavailable: ${dockerSocket}`);
  }

  cleanupSmokeContainers({ logger, runner });

  const platform = runner({
    args: ['image', 'inspect', '--format', '{{.Os}}/{{.Architecture}}', reference],
    capture: true,
    command: 'docker',
    env: withoutRegistrySecrets(process.env),
    log: false,
  }).stdout.trim();
  if (platform !== supportedImagePlatform) {
    throw new Error(
      `Unsupported devcontainer image platform ${platform}; expected ${supportedImagePlatform}.`,
    );
  }

  const label = runner({
    args: [
      'image',
      'inspect',
      '--format',
      '{{ index .Config.Labels "org.opencontainers.image.source" }}',
      reference,
    ],
    capture: true,
    command: 'docker',
    env: withoutRegistrySecrets(process.env),
    log: false,
  }).stdout.trim();
  if (label !== sourceRepository) {
    throw new Error(`Unexpected OCI source label on ${reference}: ${label}`);
  }

  const containerName = `openivm-spark-devcontainer-smoke-${process.pid}`;
  const runArgs = [
    'run',
    '--detach',
    '--name',
    containerName,
    '--label',
    smokeContainerLabel,
    '--platform',
    supportedImagePlatform,
    '--user',
    'vscode',
    '--env',
    'HOME=/home/vscode',
    '--group-add',
    String(statSync(dockerSocket).gid),
    '--volume',
    `${root}:/workspaces/openivm-spark`,
    '--volume',
    `${dockerSocket}:${dockerSocket}`,
    '--workdir',
    '/workspaces/openivm-spark',
  ];
  if (existsSync('/dev/fuse')) {
    runArgs.push('--cap-add=SYS_ADMIN', '--device=/dev/fuse', '--security-opt=apparmor:unconfined');
  }
  runArgs.push(reference, 'sleep', 'infinity');

  runner({
    args: runArgs,
    capture: true,
    command: 'docker',
    env: withoutRegistrySecrets(process.env),
  });

  let containerActive = true;
  const removeCurrentContainer = (): void => {
    if (!containerActive) {
      return;
    }
    containerActive = false;
    runner({
      args: ['rm', '--force', containerName],
      capture: true,
      check: false,
      command: 'docker',
      env: withoutRegistrySecrets(process.env),
      log: false,
    });
  };
  const signalHandlers = new Map<NodeJS.Signals, () => void>();
  for (const [signal, exitCode] of [
    ['SIGINT', 130],
    ['SIGTERM', 143],
  ] as const) {
    const handler = (): void => {
      try {
        removeCurrentContainer();
      } finally {
        process.exit(exitCode);
      }
    };
    signalHandlers.set(signal, handler);
    process.once(signal, handler);
  }

  try {
    for (const check of runtimeChecks(root)) {
      logger(`Runtime smoke check: ${check.name}`);
      runner({
        args: ['exec', containerName, 'bash', '-lc', `set -euo pipefail; ${check.script}`],
        command: 'docker',
        env: withoutRegistrySecrets(process.env),
      });
    }
  } finally {
    for (const [signal, handler] of signalHandlers) {
      process.removeListener(signal, handler);
    }
    removeCurrentContainer();
  }
}
