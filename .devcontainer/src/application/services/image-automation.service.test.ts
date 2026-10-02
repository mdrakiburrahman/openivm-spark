import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import {
  CommandResult,
  CommandSpec,
  ProcessFailure,
  ProcessRunner,
} from '../ports/process-runner.js';
import {
  imageRepository,
  sourceRepository,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { EnvironmentConfiguration } from '../../infrastructure/config/environment.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { DockerRegistry } from '../../infrastructure/registry/docker-registry.js';
import { ContentHashService } from './content-hash.service.js';
import { ImageAutomationService } from './image-automation.service.js';
import { ImageReferenceService } from './image-reference.service.js';

const reference = `ghcr.io/mdrakiburrahman/openivm-spark-devcontainer:${'a'.repeat(64)}`;

test('an existing remote manifest skips build and push after authentication', () => {
  const calls: CommandSpec[] = [];
  const root = scratchRoot();
  try {
    const service = createService(
      root,
      checkedFakeRunner((spec) => {
        calls.push(spec);
        return success('{}');
      }),
    );
    const result = service.publish({
      environment: {
        GHCR_TOKEN: 'super-secret',
        GHCR_USERNAME: 'owner',
      },
      reference,
    });

    assert.equal(result, 'skipped');
    assert.equal(calls.length, 2);
    assert.equal(calls[0]?.args?.[0], 'login');
    assert.deepEqual(calls[1]?.args?.slice(0, 2), ['manifest', 'inspect']);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('missing credentials fail before any registry, build, or push subprocess', () => {
  const calls: CommandSpec[] = [];
  const root = scratchRoot();
  try {
    const service = createService(
      root,
      checkedFakeRunner((spec) => {
        calls.push(spec);
        return success();
      }),
    );
    assert.throws(
      () =>
        service.publish({
          environment: {},
          reference,
        }),
      /requires GHCR_TOKEN and GHCR_USERNAME/u,
    );
    assert.equal(calls.length, 0);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('push failures preserve status and never log the token', () => {
  const calls: CommandSpec[] = [];
  const logs: string[] = [];
  const root = scratchRoot();
  const results: CommandResult[] = [
    success(),
    failure(1, 'manifest unknown'),
    success(),
    failure(17, 'push rejected'),
  ];

  try {
    const service = createService(
      root,
      checkedFakeRunner((spec) => {
        calls.push(spec);
        return results.shift() ?? success();
      }),
      (message) => logs.push(message),
    );
    assert.throws(
      () =>
        service.publish({
          environment: {
            GHCR_TOKEN: 'super-secret',
            GHCR_USERNAME: 'owner',
          },
          reference,
        }),
      (error: unknown) => error instanceof ProcessFailure && error.status === 17,
    );

    assert.equal(logs.join('\n').includes('super-secret'), false);
    const login = calls.find((call) => call.args?.[0] === 'login');
    assert.equal(login?.args?.includes('super-secret'), false);
    assert.equal(login?.input, 'super-secret\n');
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('build failures preserve the Dev Container CLI exit status', () => {
  const root = scratchRoot(true);
  try {
    const results: CommandResult[] = [failure(1), failure(42, 'build failed')];
    const service = createService(
      root,
      checkedFakeRunner(() => results.shift() ?? success()),
      () => undefined,
    );
    assert.throws(
      () => service.build({ reference }),
      (error: unknown) => error instanceof ProcessFailure && error.status === 42,
    );
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

function createService(
  root: string,
  runner: ProcessRunner,
  logger: (message: string) => void = () => undefined,
): ImageAutomationService {
  const fileSystem = new NodeFileSystem();
  const environment = new EnvironmentConfiguration(fileSystem);
  return new ImageAutomationService(
    {
      contentHash: new ContentHashService(fileSystem, { list: () => [] }, root),
      environment,
      references: new ImageReferenceService(fileSystem, {
        imageRepository,
        root,
        sourceRepository,
      }),
      registry: new DockerRegistry(runner),
      runner,
      smoke: { testImage: () => undefined },
    },
    root,
    logger,
  );
}

function checkedFakeRunner(handler: (spec: CommandSpec) => CommandResult): ProcessRunner {
  return {
    run(spec): CommandResult {
      const result = handler(spec);
      if (spec.check !== false && result.status !== 0) {
        throw new ProcessFailure(
          [spec.command, ...(spec.args ?? [])].join(' '),
          result.status,
          result.stderr || result.stdout,
        );
      }
      return result;
    },
  };
}

function scratchRoot(withBuildFiles = false): string {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  mkdirSync(root, { recursive: true });
  if (withBuildFiles) {
    mkdirSync(join(root, '.devcontainer'), { recursive: true });
    mkdirSync(join(root, 'spark-ext/dev'), { recursive: true });
    writeFileSync(
      join(root, 'package.json'),
      JSON.stringify({
        engines: { node: '24.11.1', npm: '11.6.2' },
        packageManager: 'npm@11.6.2',
      }),
      'utf8',
    );
    writeFileSync(
      join(root, '.devcontainer/toolchain.env'),
      ['GH_CLI_VERSION=2.94.0', `GH_CLI_SHA256=${'a'.repeat(64)}`].join('\n'),
      'utf8',
    );
    writeFileSync(
      join(root, 'spark-ext/dev/pins.env'),
      [
        'DUCKDB_COMMIT=commit',
        'DUCKDB_REF=ref',
        'LPTS_BRANCH=main',
        'LPTS_COMMIT=commit',
        'LPTS_REPO=https://example.invalid/lpts',
        'NATIVE_BUILD_JOBS=1',
        'OPENIVM_BRANCH=main',
        'OPENIVM_COMMIT=commit',
        'OPENIVM_REPO=https://example.invalid/openivm',
        'SBT_VERSION=1.9.7',
        'UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu',
      ].join('\n'),
      'utf8',
    );
  }
  return root;
}

function success(stdout = ''): CommandResult {
  return { status: 0, stderr: '', stdout };
}

function failure(status: number, stderr = ''): CommandResult {
  return { status, stderr, stdout: '' };
}
