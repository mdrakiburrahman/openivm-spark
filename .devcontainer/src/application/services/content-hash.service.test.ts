import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import {
  publishedImageTagAliases,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { GitImageInputSource } from '../../infrastructure/git/git-image-input-source.js';
import { NodeProcessRunner } from '../../infrastructure/process/node-process-runner.js';
import { ContentHashService } from './content-hash.service.js';

const fileSystem = new NodeFileSystem();
const contentHash = new ContentHashService(
  fileSystem,
  new GitImageInputSource(new NodeProcessRunner(() => undefined)),
  workspaceRoot,
  publishedImageTagAliases,
);

test('content hashes are deterministic, path-aware, and line-ending normalized', () => {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  mkdirSync(root, { recursive: true });
  try {
    writeFileSync(join(root, 'a.txt'), 'first\r\nsecond\r\n', 'utf8');
    writeFileSync(join(root, 'b.txt'), 'value\n', 'utf8');
    const first = contentHash.compute(root, ['b.txt', 'a.txt']);
    const second = contentHash.compute(root, ['a.txt', 'b.txt']);
    assert.equal(first, second);

    writeFileSync(join(root, 'a.txt'), 'first\nsecond\n', 'utf8');
    assert.equal(contentHash.compute(root, ['a.txt', 'b.txt']), first);

    writeFileSync(join(root, 'c.txt'), 'value\n', 'utf8');
    assert.notEqual(contentHash.compute(root, ['a.txt', 'c.txt']), first);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('declared image inputs mirror build/runtime assets and exclude host automation', () => {
  const inputs = new Set(contentHash.imageInputFiles());
  for (const expected of [
    'package-lock.json',
    '.devcontainer/Dockerfile',
    '.devcontainer/scripts/post-create.sh',
    'spark-ext/build.sbt',
    'spark-ext/dev/pins.env',
    'spark-ext/project/Dependencies.scala',
  ]) {
    assert.equal(inputs.has(expected), true, `missing hash input: ${expected}`);
  }
  assert.equal(inputs.has('.devcontainer/devcontainer.json'), false);
  assert.equal(inputs.has('.devcontainer/.image-tag'), false);
  assert.equal(inputs.has('.devcontainer/README.md'), false);
  assert.equal(inputs.has('.devcontainer/project.json'), false);
  assert.equal(
    [...inputs].some((filename) => filename.startsWith('.devcontainer/src/')),
    false,
  );
});

test('scripts contains only runtime-executed hooks', () => {
  assert.deepEqual(readdirSync(join(workspaceRoot, '.devcontainer/scripts')).sort(), [
    'post-create.sh',
    'validate-metals.sh',
  ]);
});
