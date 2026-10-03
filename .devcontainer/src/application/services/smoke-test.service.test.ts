import assert from 'node:assert/strict';
import test from 'node:test';

import { CommandSpec, ProcessRunner } from '../ports/process-runner.js';
import {
  smokeContainerLabel,
  sourceRepository,
  supportedImagePlatform,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { EnvironmentConfiguration } from '../../infrastructure/config/environment.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { SmokeTestService } from './smoke-test.service.js';

test('cleanup removes only labeled stale smoke containers', () => {
  const calls: CommandSpec[] = [];
  const removed = createService({
    run: (spec) => {
      calls.push(spec);
      if (spec.args?.[0] === 'ps') {
        return { status: 0, stderr: '', stdout: 'container-a\ncontainer-b\n' };
      }
      return { status: 0, stderr: '', stdout: '' };
    },
  }).cleanup({ logger: () => undefined });

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
  const removed = createService({
    run: (spec) => {
      calls.push(spec);
      return { status: 0, stderr: '', stdout: '' };
    },
  }).cleanup({ logger: (message) => logs.push(message) });

  assert.deepEqual(removed, []);
  assert.equal(calls.length, 1);
  assert.deepEqual(logs, ['No stale devcontainer smoke containers found.']);
});

test('cleanup tolerates containers disappearing after discovery', () => {
  const removed = createService({
    run: (spec) =>
      spec.args?.[0] === 'ps'
        ? { status: 0, stderr: '', stdout: 'container-a\n' }
        : { status: 1, stderr: 'Error: No such container: container-a', stdout: '' },
  }).cleanup({ logger: () => undefined });

  assert.deepEqual(removed, ['container-a']);
});

function createService(runner: ProcessRunner): SmokeTestService {
  const fileSystem = new NodeFileSystem();
  return new SmokeTestService(
    runner,
    fileSystem,
    new EnvironmentConfiguration(fileSystem),
    {
      containerLabel: smokeContainerLabel,
      imagePlatform: supportedImagePlatform,
      root: workspaceRoot,
      sourceRepository,
    },
    () => undefined,
  );
}
