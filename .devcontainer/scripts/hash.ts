import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve, sep } from 'node:path';

import { imageTagFile, immutableTagPattern, workspaceRoot } from './constants.js';

const excludedDevcontainerFiles = new Set([
  '.devcontainer/.container-id',
  '.devcontainer/.env',
  '.devcontainer/.image-tag',
  '.devcontainer/README.md',
  '.devcontainer/devcontainer.json',
]);

const rootImageInputs = [
  'package.json',
  'package-lock.json',
  'nx.json',
  'prettier.config.mjs',
  'tsconfig.base.json',
  'tsconfig.json',
  'spark-ext/.sbtopts',
  'spark-ext/build.sbt',
  'spark-ext/dev/docker/entrypoint.sh',
  'spark-ext/dev/pins.env',
  'spark-ext/dev/targets',
  'spark-ext/project',
  '.devcontainer',
] as const;

const requiredImageFiles = [
  '.devcontainer/Dockerfile',
  '.devcontainer/Dockerfile.dockerignore',
  '.devcontainer/devcontainer.local.json',
  '.devcontainer/project.json',
  '.devcontainer/scripts/cli.ts',
  '.devcontainer/scripts/post-create.sh',
  '.devcontainer/toolchain.env',
  'package-lock.json',
  'package.json',
  'spark-ext/.sbtopts',
  'spark-ext/build.sbt',
  'spark-ext/dev/docker/entrypoint.sh',
  'spark-ext/dev/pins.env',
  'spark-ext/project/Dependencies.scala',
  'spark-ext/project/RuntimeTarget.scala',
  'spark-ext/project/Settings.scala',
] as const;

export function imageInputFiles(root: string = workspaceRoot): string[] {
  const deleted = new Set(gitFiles(root, ['ls-files', '--deleted', '--', ...rootImageInputs]));
  const files = gitFiles(root, [
    'ls-files',
    '--cached',
    '--others',
    '--exclude-standard',
    '--',
    ...rootImageInputs,
  ])
    .filter((filename) => !deleted.has(filename))
    .filter((filename) => !excludedDevcontainerFiles.has(filename));
  const included = new Set(files);
  const missing = requiredImageFiles.filter((filename) => !included.has(filename));
  if (missing.length > 0) {
    throw new Error(`Missing required devcontainer image inputs: ${missing.join(', ')}`);
  }
  return files.sort();
}

export function computeContentHash(
  root: string = workspaceRoot,
  files: readonly string[] = imageInputFiles(root),
): string {
  const hash = createHash('sha256');
  hash.update('openivm-spark-devcontainer-hash-v1\0');

  for (const relativeFilename of [...files].sort()) {
    const normalizedFilename = normalizeRelativePath(relativeFilename);
    const absoluteFilename = resolve(root, normalizedFilename);
    const contents = readFileSync(absoluteFilename, 'utf8').replaceAll('\r\n', '\n');
    hash.update(`${normalizedFilename}\0${Buffer.byteLength(contents)}\0`);
    hash.update(contents);
    hash.update('\0');
  }

  return hash.digest('hex');
}

export function writeImageTag(
  root: string = workspaceRoot,
  outputFile: string = imageTagFile,
): string {
  const tag = computeContentHash(root);
  mkdirSync(dirname(outputFile), { recursive: true });
  writeFileSync(outputFile, `${tag}\n`, 'utf8');
  return tag;
}

export function readImageTag(filename: string = imageTagFile): string {
  const tag = readFileSync(filename, 'utf8').trim();
  if (!immutableTagPattern.test(tag)) {
    throw new Error(`Invalid devcontainer image tag in ${filename}. Run the tag target first.`);
  }
  return tag;
}

function gitFiles(root: string, args: readonly string[]): string[] {
  const result = spawnSync('git', args, {
    cwd: root,
    encoding: 'utf8',
  });
  if (result.error) {
    throw new Error(`Unable to enumerate image inputs: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`Unable to enumerate image inputs: ${result.stderr.trim()}`);
  }
  return result.stdout
    .split(/\r?\n/u)
    .map(normalizeRelativePath)
    .filter((filename) => filename.length > 0);
}

function normalizeRelativePath(filename: string): string {
  return filename.split(sep).join('/');
}
