import assert from 'node:assert/strict';
import test from 'node:test';

import { smokeContainerLabel } from './constants.js';
import { CommandSpec } from './process.js';
import { cleanupSmokeContainers } from './smoke.js';

test('cleanup removes only labeled stale smoke containers', () => {
  const calls: CommandSpec[] = [];
  const removed = cleanupSmokeContainers({
    logger: () => undefined,
    runner: (spec) => {
      calls.push(spec);
      if (spec.args?.[0] === 'ps') {
        return { status: 0, stderr: '', stdout: 'container-a\ncontainer-b\n' };
      }
      return { status: 0, stderr: '', stdout: '' };
    },
  });

  assert.deepEqual(removed, ['container-a', 'container-b']);
  assert.deepEqual(calls[0]?.args, [
    'ps',
    '--all',
    '--quiet',
    '--filter',
    `label=${smokeContainerLabel}`,
  ]);
  assert.deepEqual(calls[1]?.args, ['rm', '--force', 'container-a', 'container-b']);
});

test('cleanup is idempotent when no labeled smoke containers exist', () => {
  const calls: CommandSpec[] = [];
  const logs: string[] = [];
  const removed = cleanupSmokeContainers({
    logger: (message) => logs.push(message),
    runner: (spec) => {
      calls.push(spec);
      return { status: 0, stderr: '', stdout: '' };
    },
  });

  assert.deepEqual(removed, []);
  assert.equal(calls.length, 1);
  assert.deepEqual(logs, ['No stale devcontainer smoke containers found.']);
});

test('cleanup tolerates containers disappearing after discovery', () => {
  const removed = cleanupSmokeContainers({
    logger: () => undefined,
    runner: (spec) =>
      spec.args?.[0] === 'ps'
        ? { status: 0, stderr: '', stdout: 'container-a\n' }
        : { status: 1, stderr: 'Error: No such container: container-a', stdout: '' },
  });

  assert.deepEqual(removed, ['container-a']);
});
