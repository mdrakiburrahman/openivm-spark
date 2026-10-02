import { sep } from 'node:path';

import { ImageInputSource } from '../../application/ports/image-input-source.js';
import { ProcessRunner } from '../../application/ports/process-runner.js';

const excludedDevcontainerFiles = new Set([
  '.devcontainer/.container-id',
  '.devcontainer/.env',
  '.devcontainer/.image-tag',
  '.devcontainer/README.md',
  '.devcontainer/devcontainer.json',
]);

const rootImageInputs = [
  '.devcontainer/Dockerfile',
  '.devcontainer/Dockerfile.dockerignore',
  '.devcontainer/devcontainer.local.json',
  '.devcontainer/scripts/post-create.sh',
  '.devcontainer/toolchain.env',
  'package.json',
  'package-lock.json',
  'spark-ext/.sbtopts',
  'spark-ext/build.sbt',
  'spark-ext/dev/docker/entrypoint.sh',
  'spark-ext/dev/pins.env',
  'spark-ext/project',
] as const;

const requiredImageFiles = [
  '.devcontainer/Dockerfile',
  '.devcontainer/Dockerfile.dockerignore',
  '.devcontainer/devcontainer.local.json',
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

export class GitImageInputSource implements ImageInputSource {
  constructor(private readonly runner: ProcessRunner) {}

  list(root: string): string[] {
    const deleted = new Set(
      this.gitFiles(root, ['ls-files', '--deleted', '--', ...rootImageInputs]),
    );
    const files = this.gitFiles(root, [
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

  private gitFiles(root: string, args: readonly string[]): string[] {
    const result = this.runner.run({
      args,
      capture: true,
      command: 'git',
      cwd: root,
      log: false,
    });
    return result.stdout
      .split(/\r?\n/u)
      .map(normalizeRelativePath)
      .filter((filename) => filename.length > 0);
  }
}

export function normalizeRelativePath(filename: string): string {
  return filename.split(sep).join('/');
}
