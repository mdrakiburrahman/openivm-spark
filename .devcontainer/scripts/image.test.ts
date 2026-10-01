import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import { workspaceRoot } from './constants.js';
import { buildImage, checkedFakeRunner, publishImage } from './image.js';
import { CommandResult, CommandSpec, ProcessFailure } from './process.js';

const reference = `ghcr.io/mdrakiburrahman/openivm-spark-devcontainer:${'a'.repeat(64)}`;

test('an existing remote manifest skips login, build, and push', () => {
  const calls: CommandSpec[] = [];
  const root = scratchRoot();
  try {
    const result = publishImage({
      environment: {
        GHCR_TOKEN: 'super-secret',
        GHCR_USERNAME: 'owner',
      },
      logger: () => undefined,
      reference,
      root,
      runner: checkedFakeRunner((spec) => {
        calls.push(spec);
        return success('{}');
      }),
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
    assert.throws(
      () =>
        publishImage({
          environment: {},
          logger: () => undefined,
          reference,
          root,
          runner: checkedFakeRunner((spec) => {
            calls.push(spec);
            return success();
          }),
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
    assert.throws(
      () =>
        publishImage({
          environment: {
            GHCR_TOKEN: 'super-secret',
            GHCR_USERNAME: 'owner',
          },
          logger: (message) => logs.push(message),
          reference,
          root,
          runner: checkedFakeRunner((spec) => {
            calls.push(spec);
            return results.shift() ?? success();
          }),
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
    assert.throws(
      () =>
        buildImage({
          logger: () => undefined,
          reference,
          root,
          runner: checkedFakeRunner(() => results.shift() ?? success()),
        }),
      (error: unknown) => error instanceof ProcessFailure && error.status === 42,
    );
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

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
