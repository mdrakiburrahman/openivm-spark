import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import { workspaceRoot } from './constants.js';
import { down, execute, parseContainerId } from './lifecycle.js';
import { CommandSpec } from './process.js';

test('Dev Container CLI output yields the exact container ID', () => {
  assert.equal(
    parseContainerId('progress\n{"outcome":"success","containerId":"abc123"}\n'),
    'abc123',
  );
});

test('down is idempotent when no recorded container exists', () => {
  const root = scratchRoot();
  const logs: string[] = [];
  try {
    down({
      logger: (message) => logs.push(message),
      root,
      runner: () => {
        throw new Error('runner should not be called');
      },
    });
    assert.deepEqual(logs, ['Devcontainer is already stopped.']);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('exec forwards the exact container ID and command without a shell', () => {
  let call: CommandSpec | undefined;
  execute(['printf', '%s', 'hello world'], {
    containerId: 'abc123',
    runner: (spec) => {
      call = spec;
      return { status: 0, stderr: '', stdout: '' };
    },
  });

  assert.deepEqual(call?.args?.slice(-5), [
    '--container-id',
    'abc123',
    'printf',
    '%s',
    'hello world',
  ]);
  assert.equal(call?.log, false);
});

function scratchRoot(): string {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  mkdirSync(root, { recursive: true });
  return root;
}
