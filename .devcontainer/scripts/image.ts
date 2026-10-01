import {
  buildEnvironment,
  publishingEnvironment,
  resolveRegistryCredentials,
  withoutRegistrySecrets,
} from './env.js';
import { imageRepository, workspaceRoot } from './constants.js';
import { join } from 'node:path';
import { readImageTag } from './hash.js';
import { expectedImageReference } from './references.js';
import {
  CommandResult,
  CommandRunner,
  CommandSpec,
  ProcessFailure,
  runCommand,
} from './process.js';

export interface ImageOperationOptions {
  force?: boolean;
  logger?: (message: string) => void;
  reference?: string;
  root?: string;
  runner?: CommandRunner;
}

export interface PublishOptions extends ImageOperationOptions {
  environment?: NodeJS.ProcessEnv;
}

export type PublishResult = 'published' | 'skipped';

export function imageReference(root: string = workspaceRoot): string {
  return expectedImageReference(readImageTag(`${root}/.devcontainer/.image-tag`));
}

export function localImageExists(
  reference: string,
  runner: CommandRunner = runCommand,
  environment: NodeJS.ProcessEnv = process.env,
): boolean {
  return (
    runner({
      args: ['image', 'inspect', reference],
      capture: true,
      check: false,
      command: 'docker',
      env: withoutRegistrySecrets(environment),
      log: false,
    }).status === 0
  );
}

export function remoteManifestExists(
  reference: string,
  runner: CommandRunner = runCommand,
  environment: NodeJS.ProcessEnv = process.env,
): boolean {
  const result = runner({
    args: ['manifest', 'inspect', reference],
    capture: true,
    check: false,
    command: 'docker',
    env: withoutRegistrySecrets(environment),
    log: false,
  });
  if (result.status === 0) {
    return true;
  }

  const detail = `${result.stderr}\n${result.stdout}`.toLowerCase();
  if (
    detail.includes('manifest unknown') ||
    detail.includes('no such manifest') ||
    detail.includes('not found')
  ) {
    return false;
  }
  throw new ProcessFailure(`docker manifest inspect ${reference}`, result.status, result.stderr);
}

export function buildImage(options: ImageOperationOptions = {}): string {
  const root = options.root ?? workspaceRoot;
  const runner = options.runner ?? runCommand;
  const logger = options.logger ?? console.log;
  const reference = options.reference ?? imageReference(root);

  if (!options.force && localImageExists(reference, runner)) {
    logger(`Image already exists locally; skipping build: ${reference}`);
    return reference;
  }

  logger(`Building immutable devcontainer image: ${reference}`);
  runner({
    args: [
      '--no-install',
      'devcontainer',
      'build',
      '--workspace-folder',
      root,
      '--config',
      join(root, '.devcontainer/devcontainer.local.json'),
      '--image-name',
      reference,
    ],
    command: 'npx',
    cwd: root,
    env: buildEnvironment(root),
  });

  if (!localImageExists(reference, runner)) {
    throw new Error(`Dev Container CLI completed without creating ${reference}.`);
  }
  return reference;
}

export function publishImage(options: PublishOptions = {}): PublishResult {
  const root = options.root ?? workspaceRoot;
  const runner = options.runner ?? runCommand;
  const logger = options.logger ?? console.log;
  const environment = publishingEnvironment(root, options.environment);
  const credentials = resolveRegistryCredentials(environment);
  const reference = options.reference ?? imageReference(root);

  logger(`Authenticating to ghcr.io for immutable image publication.`);
  runner({
    args: ['login', 'ghcr.io', '--username', credentials.username, '--password-stdin'],
    command: 'docker',
    env: withoutRegistrySecrets(environment),
    input: `${credentials.token}\n`,
    log: false,
  });

  if (remoteManifestExists(reference, runner, environment)) {
    logger(`Remote manifest already exists; skipping publish: ${reference}`);
    return 'skipped';
  }

  if (!localImageExists(reference, runner, environment)) {
    buildImage({
      force: false,
      logger,
      reference,
      root,
      runner,
    });
  }

  logger(`Publishing immutable devcontainer image: ${reference}`);
  runner({
    args: ['image', 'push', reference],
    command: 'docker',
    env: withoutRegistrySecrets(environment),
  });

  if (!remoteManifestExists(reference, runner, environment)) {
    throw new Error(`Push completed but the remote manifest is unavailable: ${reference}`);
  }
  return 'published';
}

export function checkedFakeRunner(handler: (spec: CommandSpec) => CommandResult): CommandRunner {
  return (spec) => {
    const result = handler(spec);
    if (spec.check !== false && result.status !== 0) {
      throw new ProcessFailure(
        [spec.command, ...(spec.args ?? [])].join(' '),
        result.status,
        result.stderr || result.stdout,
      );
    }
    return result;
  };
}

export { imageRepository };
