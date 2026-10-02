import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import { CommandSpec, ProcessRunner } from '../ports/process-runner.js';
import { ContainerId } from '../../domain/lifecycle/container-id.js';
import {
  imageRepository,
  sourceRepository,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { EnvironmentConfiguration } from '../../infrastructure/config/environment.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { ContentHashService } from './content-hash.service.js';
import { ImageReferenceService } from './image-reference.service.js';
import { DevcontainerLifecycleService } from './lifecycle.service.js';

test('Dev Container CLI output yields the exact container ID', () => {
  assert.equal(
    ContainerId.fromDevcontainerOutput('progress\n{"outcome":"success","containerId":"abc123"}\n')
      .value,
    'abc123',
  );
});

test('down is idempotent when no recorded container exists', () => {
  const root = scratchRoot();
  const logs: string[] = [];
  try {
    createService(
      root,
      {
        run: () => {
          throw new Error('runner should not be called');
        },
      },
      (message) => logs.push(message),
    ).down();
    assert.deepEqual(logs, ['Devcontainer is already stopped.']);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('exec forwards the exact container ID and command without a shell', () => {
  const root = scratchRoot();
  let call: CommandSpec | undefined;
  try {
    createService(root, {
      run: (spec) => {
        call = spec;
        return { status: 0, stderr: '', stdout: '' };
      },
    }).execute(['printf', '%s', 'hello world'], { containerId: 'abc123' });

    assert.deepEqual(call?.args?.slice(-5), [
      '--container-id',
      'abc123',
      'printf',
      '%s',
      'hello world',
    ]);
    assert.equal(call?.log, false);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

function createService(
  root: string,
  runner: ProcessRunner,
  logger: (message: string) => void = () => undefined,
): DevcontainerLifecycleService {
  const fileSystem = new NodeFileSystem();
  const environment = new EnvironmentConfiguration(fileSystem);
  return new DevcontainerLifecycleService(
    runner,
    fileSystem,
    environment,
    new ContentHashService(fileSystem, { list: () => [] }, root),
    new ImageReferenceService(fileSystem, {
      imageRepository,
      root,
      sourceRepository,
    }),
    root,
    logger,
  );
}

function scratchRoot(): string {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  mkdirSync(root, { recursive: true });
  return root;
}
