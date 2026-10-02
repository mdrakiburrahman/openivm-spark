import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import {
  imageRepository,
  publishedImageTagAliases,
  smokeContainerLabel,
  sourceRepository,
  supportedImagePlatform,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { EnvironmentConfiguration } from '../../infrastructure/config/environment.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { GitImageInputSource } from '../../infrastructure/git/git-image-input-source.js';
import { NodeProcessRunner } from '../../infrastructure/process/node-process-runner.js';
import { ContentHashService } from './content-hash.service.js';
import { ImageReferenceService } from './image-reference.service.js';
import { SmokeTestService } from './smoke-test.service.js';

const fileSystem = new NodeFileSystem();
const runner = new NodeProcessRunner(() => undefined);
const contentHash = new ContentHashService(
  fileSystem,
  new GitImageInputSource(runner),
  workspaceRoot,
  publishedImageTagAliases,
);
const references = new ImageReferenceService(fileSystem, {
  imageRepository,
  root: workspaceRoot,
  sourceRepository,
});
const smoke = new SmokeTestService(runner, fileSystem, new EnvironmentConfiguration(fileSystem), {
  containerLabel: smokeContainerLabel,
  imagePlatform: supportedImagePlatform,
  root: workspaceRoot,
  sourceRepository,
});

test('consumer and every workflow occurrence use the computed immutable image reference', () => {
  const expected = references.expected(contentHash.compute());
  assert.doesNotThrow(() => references.assertConsistent(expected));
  assert.equal(references.occurrences().length >= 1, true);
});

test('image consistency validation reports a mismatched workflow reference', () => {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  const expected = references.expected('a'.repeat(64));
  mkdirSync(join(root, '.devcontainer'), { recursive: true });
  mkdirSync(join(root, '.github/workflows'), { recursive: true });
  try {
    writeFileSync(
      join(root, '.devcontainer/devcontainer.json'),
      JSON.stringify({ image: expected }),
      'utf8',
    );
    writeFileSync(
      join(root, '.github/workflows/test.yaml'),
      `jobs:\n  test:\n    container:\n      image: ${references.expected('b'.repeat(64))}\n`,
      'utf8',
    );
    assert.throws(
      () => references.assertConsistent(expected, root),
      /\.github\/workflows\/test\.yaml/u,
    );
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('consumer updater rewrites devcontainer and every committed workflow occurrence', () => {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  const expected = references.expected('c'.repeat(64));
  const unrelated = 'ghcr.io/example/unrelated:latest';
  mkdirSync(join(root, '.devcontainer'), { recursive: true });
  mkdirSync(join(root, '.github/workflows/nested'), { recursive: true });
  try {
    writeFileSync(
      join(root, '.devcontainer/devcontainer.json'),
      `${JSON.stringify({ image: references.expected('a'.repeat(64)) }, null, 2)}\n`,
      'utf8',
    );
    writeFileSync(
      join(root, '.github/workflows/gci.yaml'),
      [
        'jobs:',
        '  verify:',
        '    container:',
        `      image: ${references.expected('b'.repeat(64))}`,
        `  unrelated: ${unrelated}`,
        '',
      ].join('\n'),
      'utf8',
    );
    writeFileSync(
      join(root, '.github/workflows/nested/image.yml'),
      `env:\n  DEVCONTAINER_IMAGE: "${references.expected('d'.repeat(64))}"\n`,
      'utf8',
    );

    assert.deepEqual(references.updateConsumerReferences(expected, root).sort(), [
      '.devcontainer/devcontainer.json',
      '.github/workflows/gci.yaml',
      '.github/workflows/nested/image.yml',
    ]);
    assert.deepEqual(
      references.occurrences(root).map((occurrence) => occurrence.reference),
      [expected, expected, expected],
    );
    assert.equal(
      readFileSync(join(root, '.github/workflows/gci.yaml'), 'utf8').includes(unrelated),
      true,
    );
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('Dockerfile carries the repository OCI source label', () => {
  assert.doesNotThrow(() => references.assertOciSourceLabel());
});

test('single image exposes stable JDK 17 and JDK 21 homes with JDK 17 as default', () => {
  const dockerfile = readFileSync(join(workspaceRoot, '.devcontainer/Dockerfile'), 'utf8');
  assert.equal(dockerfile.includes('JAVA_HOME=/opt/java/jdk-17'), true);
  assert.equal(dockerfile.includes('OPENIVM_JAVA_HOME_SPARK_35=/opt/java/jdk-17'), true);
  assert.equal(dockerfile.includes('OPENIVM_JAVA_HOME_SPARK_41=/opt/java/jdk-21'), true);
  assert.equal(dockerfile.includes('ln -s /opt/java/openjdk /opt/java/jdk-21'), true);
  assert.equal(dockerfile.includes('ENV HOME=/home/vscode'), true);
  assert.equal(dockerfile.includes('vscode ALL=(root) NOPASSWD:ALL'), true);
  assert.equal(
    dockerfile.includes('mkdir -p /opt/downloads /opt/openivm-node-tooling /opt/sbt'),
    true,
  );
  assert.equal(dockerfile.includes('chown -R vscode:vscode /opt/openivm-node-tooling'), true);
  assert.match(dockerfile, /USER vscode\nWORKDIR \/workspaces\/openivm-spark/u);
});

test('Dockerfile reuses the required UID and GID 1000 identities for vscode', () => {
  const dockerfile = readFileSync(join(workspaceRoot, '.devcontainer/Dockerfile'), 'utf8');
  for (const expected of [
    'existing_group="$(getent group 1000 | cut -d: -f1)"',
    'groupmod --new-name vscode "${existing_group}"',
    'groupadd --gid 1000 vscode',
    'existing_user="$(getent passwd 1000 | cut -d: -f1)"',
    'usermod --login vscode "${existing_user}"',
    'useradd --uid 1000 --gid vscode --create-home --shell /bin/bash vscode',
    'usermod --home /home/vscode --move-home vscode',
    'usermod --gid vscode --shell /bin/bash vscode',
  ]) {
    assert.equal(dockerfile.includes(expected), true, `missing account reuse logic: ${expected}`);
  }
});

test('publish always depends on image build and runtime smoke testing', () => {
  const project = JSON.parse(
    readFileSync(join(workspaceRoot, '.devcontainer/project.json'), 'utf8'),
  ) as {
    targets?: {
      publish?: {
        dependsOn?: string[];
      };
    };
  };
  assert.deepEqual(project.targets?.publish?.dependsOn, ['test']);
});

test('GitHub CLI download is exact-versioned and checksum verified', () => {
  const toolchain = Object.fromEntries(
    readFileSync(join(workspaceRoot, '.devcontainer/toolchain.env'), 'utf8')
      .trim()
      .split(/\r?\n/u)
      .map((line) => line.split('=', 2)),
  );
  const dockerfile = readFileSync(join(workspaceRoot, '.devcontainer/Dockerfile'), 'utf8');
  assert.equal(toolchain.GH_CLI_VERSION, '2.94.0');
  assert.match(toolchain.GH_CLI_SHA256 ?? '', /^[a-f0-9]{64}$/u);
  assert.equal('GH_CLI_SHA256_ARM64' in toolchain, false);
  assert.equal(dockerfile.includes('GH_CLI_SHA256'), true);
  assert.equal(dockerfile.includes('sha256sum -c -'), true);
});

test('image contract is explicitly linux/amd64 only', () => {
  const dockerfile = readFileSync(join(workspaceRoot, '.devcontainer/Dockerfile'), 'utf8');
  assert.equal(supportedImagePlatform, 'linux/amd64');
  assert.equal(
    dockerfile.includes(
      'openivm-spark-devcontainer supports only linux/amd64; TARGETARCH=${TARGETARCH:-unset}',
    ),
    true,
  );
  assert.equal(dockerfile.includes('arm64'), false);
});

test('image build context excludes repository secrets and unrelated files', () => {
  const dockerfile = readFileSync(join(workspaceRoot, '.devcontainer/Dockerfile'), 'utf8');
  const dockerignore = readFileSync(
    join(workspaceRoot, '.devcontainer/Dockerfile.dockerignore'),
    'utf8',
  );
  assert.equal(dockerignore.startsWith('**\n'), true);
  assert.equal(dockerfile.includes('GHCR_TOKEN'), false);
  assert.equal(dockerfile.includes('GITHUB_TOKEN'), false);
  assert.equal(/^COPY\s+\.\s/mu.test(dockerfile), false);
  assert.equal(dockerignore.includes('!.devcontainer/src/'), false);
  assert.equal(dockerignore.includes('!.devcontainer/scripts/post-create.sh'), true);
});

test('post-create keeps passwordless-sudo Docker socket group repair', () => {
  const postCreate = readFileSync(
    join(workspaceRoot, '.devcontainer/scripts/post-create.sh'),
    'utf8',
  );
  assert.equal(postCreate.includes('sudo groupadd'), true);
  assert.equal(postCreate.includes('sudo usermod'), true);
});

test('Metals validation runs bloopInstall through the runtime script', () => {
  const validateMetals = readFileSync(
    join(workspaceRoot, '.devcontainer/scripts/validate-metals.sh'),
    'utf8',
  );
  assert.equal(validateMetals.includes('JAVA_HOME=/opt/java/jdk-17'), true);
  assert.equal(validateMetals.includes('sbt -batch bloopInstall'), true);
  assert.equal(validateMetals.includes("find .bloop -maxdepth 1 -name '*.json'"), true);
});

test('local and consumer configurations preserve required runtime settings', () => {
  for (const relativeFilename of [
    '.devcontainer/devcontainer.local.json',
    '.devcontainer/devcontainer.json',
  ]) {
    const config = JSON.parse(readFileSync(join(workspaceRoot, relativeFilename), 'utf8')) as {
      build?: {
        options?: string[];
      };
      containerUser?: string;
      containerEnv?: Record<string, string>;
      customizations?: {
        vscode?: {
          extensions?: string[];
          settings?: Record<string, unknown>;
        };
      };
      mounts?: string[];
      postCreateCommand?: string;
      remoteEnv?: Record<string, string>;
      remoteUser?: string;
      runArgs?: string[];
      workspaceFolder?: string;
      workspaceMount?: string;
    };
    assert.equal(config.containerUser, 'vscode');
    assert.equal(config.containerEnv?.OPENIVM_JAVA_HOME_SPARK_35, '/opt/java/jdk-17');
    assert.equal(config.containerEnv?.OPENIVM_JAVA_HOME_SPARK_41, '/opt/java/jdk-21');
    assert.equal(config.remoteUser, 'vscode');
    assert.equal(config.remoteEnv?.HOME, '/home/vscode');
    assert.equal(config.postCreateCommand, 'bash .devcontainer/scripts/post-create.sh');
    assert.deepEqual(config.customizations?.vscode?.extensions, [
      'scalameta.metals@1.42.0',
      'scala-lang.scala@0.5.8',
      'vscjava.vscode-java-pack@0.29.0',
    ]);
    assert.equal(config.customizations?.vscode?.settings?.['editor.formatOnSave'], true);
    assert.equal(
      config.customizations?.vscode?.settings?.['terminal.integrated.defaultProfile.linux'],
      'bash',
    );
    assert.equal(
      config.mounts?.some((mount) => mount.includes('/var/run/docker.sock')),
      true,
    );
    assert.equal(config.runArgs?.includes('--cap-add=SYS_ADMIN'), true);
    assert.equal(config.runArgs?.includes('--platform=linux/amd64'), true);
    assert.equal(config.workspaceFolder, '/workspaces/openivm-spark');
    assert.equal(
      config.workspaceMount,
      'source=${localWorkspaceFolder},target=/workspaces/openivm-spark,type=bind,consistency=cached',
    );
    if (relativeFilename.endsWith('devcontainer.local.json')) {
      assert.deepEqual(config.build?.options, ['--platform=linux/amd64']);
    }
  }
});

test('Spark workspace is flattened and pins the IDE JDKs', () => {
  const workspaceJson = readFileSync(join(workspaceRoot, 'spark.code-workspace'), 'utf8').replace(
    /,\s*([}\]])/gu,
    '$1',
  );
  const workspace = JSON.parse(workspaceJson) as {
    folders?: Array<{ name?: string; path?: string }>;
    settings?: Record<string, unknown>;
  };
  assert.deepEqual(workspace.folders, [{ name: 'spark-ext', path: 'spark-ext' }]);
  assert.equal(workspace.settings?.['metals.javaHome'], '/opt/java/jdk-17');
  assert.equal(workspace.settings?.['java.jdt.ls.java.home'], '/opt/java/jdk-21');
  assert.deepEqual(workspace.settings?.['files.watcherExclude'], {
    '**/.bloop/**': true,
    '**/.metals/**': true,
    '**/target/**': true,
  });
});

test('cleanup target is typed and smoke uses the stale-container label', () => {
  const project = JSON.parse(
    readFileSync(join(workspaceRoot, '.devcontainer/project.json'), 'utf8'),
  ) as {
    targets?: Record<string, unknown>;
  };
  const smokeSource = readFileSync(
    join(workspaceRoot, '.devcontainer/src/application/services/smoke-test.service.ts'),
    'utf8',
  );
  assert.equal('cleanup' in (project.targets ?? {}), true);
  assert.equal(smokeSource.includes('this.cleanup();'), true);
  assert.equal(smokeSource.includes("'--label'"), true);
  assert.equal(smokeSource.includes('this.settings.containerLabel'), true);
  assert.equal(smokeContainerLabel, 'org.openivm.spark.devcontainer.smoke=true');
});

test('every Nx automation target uses the DDD CLI entry point', () => {
  const project = JSON.parse(
    readFileSync(join(workspaceRoot, '.devcontainer/project.json'), 'utf8'),
  ) as {
    targets?: Record<string, { options?: { command?: string } }>;
  };
  for (const target of ['tag', 'build', 'test', 'publish', 'up', 'exec', 'down', 'cleanup']) {
    assert.match(
      project.targets?.[target]?.options?.command ?? '',
      /^tsx \.devcontainer\/src\/interfaces\/cli\/main\.ts /u,
    );
  }
});

test('runtime smoke suite covers tools, native artifacts, lifecycle, SBT, and Docker', () => {
  const checks = smoke.runtimeChecks();
  const names = checks.map((check) => check.name);
  assert.deepEqual(names, [
    'non-root user and workspace',
    'pinned Node and repository tooling',
    'pinned GitHub CLI',
    'JDKs, sbt, and native OpenIVM artifacts',
    'lifecycle hook',
    'warmed Spark 3.5 and Spark 4.1 dependency caches',
    'Metals Bloop import',
    'host Docker socket access',
  ]);
  const scripts = checks.map((check) => check.script).join('\n');
  assert.equal(scripts.includes('JAVA_HOME=/opt/java/jdk-17'), true);
  assert.equal(scripts.includes('JAVA_HOME=/opt/java/jdk-21'), true);
  assert.equal(scripts.includes('2.12.17'), true);
  assert.equal(scripts.includes('2.13.17'), true);
  assert.equal(scripts.includes('gh --version'), true);
  assert.equal(scripts.includes('gh version 2.94.0'), true);
  assert.equal(scripts.includes('bash .devcontainer/scripts/validate-metals.sh'), true);
});

test('headless README documents the exact Nx lifecycle interface', () => {
  const readme = readFileSync(join(workspaceRoot, '.devcontainer/README.md'), 'utf8');
  assert.equal(readme.includes('npx --no-install nx run devcontainer:up'), true);
  assert.equal(readme.includes('npx --no-install nx run devcontainer:exec -- <command...>'), true);
  assert.equal(readme.includes('npx --no-install nx run devcontainer:down'), true);
  assert.equal(readme.includes('npx --no-install nx run devcontainer:cleanup'), true);
  assert.equal(readme.includes('Linux amd64 only'), true);
});
