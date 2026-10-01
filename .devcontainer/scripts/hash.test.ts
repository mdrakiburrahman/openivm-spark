import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import { workspaceRoot } from './constants.js';
import { computeContentHash, imageInputFiles } from './hash.js';

test('content hashes are deterministic, path-aware, and line-ending normalized', () => {
  const root = join(workspaceRoot, '.nx', 'test-work', randomUUID());
  mkdirSync(root, { recursive: true });
  try {
    writeFileSync(join(root, 'a.txt'), 'first\r\nsecond\r\n', 'utf8');
    writeFileSync(join(root, 'b.txt'), 'value\n', 'utf8');
    const first = computeContentHash(root, ['b.txt', 'a.txt']);
    const second = computeContentHash(root, ['a.txt', 'b.txt']);
    assert.equal(first, second);

    writeFileSync(join(root, 'a.txt'), 'first\nsecond\n', 'utf8');
    assert.equal(computeContentHash(root, ['a.txt', 'b.txt']), first);

    writeFileSync(join(root, 'c.txt'), 'value\n', 'utf8');
    assert.notEqual(computeContentHash(root, ['a.txt', 'c.txt']), first);
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test('declared image inputs include locks, automation, pins, and SBT definitions', () => {
  const inputs = new Set(imageInputFiles());
  for (const expected of [
    'package-lock.json',
    '.devcontainer/Dockerfile',
    '.devcontainer/scripts/image.ts',
    'spark-ext/build.sbt',
    'spark-ext/dev/pins.env',
    'spark-ext/project/Dependencies.scala',
  ]) {
    assert.equal(inputs.has(expected), true, `missing hash input: ${expected}`);
  }
  assert.equal(inputs.has('.devcontainer/devcontainer.json'), false);
  assert.equal(inputs.has('.devcontainer/.image-tag'), false);
  assert.equal(inputs.has('.devcontainer/README.md'), false);
});
