import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import { containerIdFile, workspaceRoot } from './constants.js';
import { withoutRegistrySecrets } from './env.js';
import { computeContentHash } from './hash.js';
import { assertImageReferencesConsistent, expectedImageReference } from './references.js';
import { CommandRunner, ProcessFailure, runCommand } from './process.js';

export interface LifecycleOptions {
  containerId?: string;
  logger?: (message: string) => void;
  root?: string;
  runner?: CommandRunner;
}

export function up(options: LifecycleOptions = {}): string {
  const root = options.root ?? workspaceRoot;
  const runner = options.runner ?? runCommand;
  const logger = options.logger ?? console.log;
  const expected = expectedImageReference(computeContentHash(root));
  assertImageReferencesConsistent(expected, root);

  const result = runner({
    args: [
      '--no-install',
      'devcontainer',
      'up',
      '--workspace-folder',
      root,
      '--config',
      join(root, '.devcontainer/devcontainer.json'),
    ],
    capture: true,
    command: 'npx',
    cwd: root,
    env: withoutRegistrySecrets(process.env),
  });
  const containerId = parseContainerId(result.stdout);
  writeFileSync(join(root, '.devcontainer/.container-id'), `${containerId}\n`, 'utf8');
  logger(`Devcontainer is running: ${containerId}`);
  return containerId;
}

export function execute(command: readonly string[], options: LifecycleOptions = {}): void {
  if (command.length === 0) {
    throw new Error('The exec target requires a command after "--".');
  }
  const root = options.root ?? workspaceRoot;
  const runner = options.runner ?? runCommand;
  const containerId =
    options.containerId ?? readContainerId(join(root, '.devcontainer/.container-id'));

  runner({
    args: [
      '--no-install',
      'devcontainer',
      'exec',
      '--workspace-folder',
      root,
      '--container-id',
      containerId,
      ...command,
    ],
    command: 'npx',
    cwd: root,
    env: withoutRegistrySecrets(process.env),
    log: false,
  });
}

export function down(options: LifecycleOptions = {}): void {
  const root = options.root ?? workspaceRoot;
  const runner = options.runner ?? runCommand;
  const logger = options.logger ?? console.log;
  const stateFile = join(root, '.devcontainer/.container-id');
  if (!options.containerId && !existsSync(stateFile)) {
    logger('Devcontainer is already stopped.');
    return;
  }
  const containerId = options.containerId ?? readContainerId(stateFile);
  const result = runner({
    args: ['rm', '--force', containerId],
    capture: true,
    check: false,
    command: 'docker',
    env: withoutRegistrySecrets(process.env),
    log: false,
  });
  const detail = `${result.stderr}\n${result.stdout}`.toLowerCase();
  if (result.status !== 0 && !detail.includes('no such container')) {
    throw new ProcessFailure(`docker rm --force ${containerId}`, result.status, result.stderr);
  }
  rmSync(stateFile, { force: true });
  logger(`Devcontainer removed: ${containerId}`);
}

export function readContainerId(filename: string = containerIdFile): string {
  if (!existsSync(filename)) {
    throw new Error('No recorded devcontainer ID. Run the up target first.');
  }
  const containerId = readFileSync(filename, 'utf8').trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]+$/u.test(containerId)) {
    throw new Error(`Invalid container ID in ${filename}.`);
  }
  return containerId;
}

export function parseContainerId(output: string): string {
  const candidates = [output.trim(), ...output.trim().split(/\r?\n/u).reverse()];
  for (const candidate of candidates) {
    try {
      const parsed = JSON.parse(candidate) as { containerId?: unknown };
      if (typeof parsed.containerId === 'string' && parsed.containerId.length > 0) {
        return parsed.containerId;
      }
    } catch {
      continue;
    }
  }
  throw new Error('Dev Container CLI output did not contain a containerId.');
}
