import {
  ContainerRegistry,
  RegistryCredentials,
} from '../../application/ports/container-registry.js';
import { ProcessFailure, ProcessRunner } from '../../application/ports/process-runner.js';
import { withoutRegistrySecrets } from '../config/environment.js';

export class DockerRegistry implements ContainerRegistry {
  constructor(private readonly runner: ProcessRunner) {}

  authenticate(credentials: RegistryCredentials, environment: NodeJS.ProcessEnv): void {
    this.runner.run({
      args: ['login', 'ghcr.io', '--username', credentials.username, '--password-stdin'],
      command: 'docker',
      env: withoutRegistrySecrets(environment),
      input: `${credentials.token}\n`,
      log: false,
    });
  }

  manifestExists(reference: string, environment: NodeJS.ProcessEnv): boolean {
    const result = this.runner.run({
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

  push(reference: string, environment: NodeJS.ProcessEnv): void {
    this.runner.run({
      args: ['image', 'push', reference],
      command: 'docker',
      env: withoutRegistrySecrets(environment),
    });
  }
}
