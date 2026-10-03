import { join } from 'node:path';

import { LifecycleAutomation, LifecycleOptions } from '../ports/automation.js';
import { EnvironmentGateway } from '../ports/environment.js';
import { FileSystem } from '../ports/file-system.js';
import { ProcessFailure, ProcessRunner } from '../ports/process-runner.js';
import { ContainerId } from '../../domain/lifecycle/container-id.js';
import { ContentHashService } from './content-hash.service.js';
import { ImageReferenceService } from './image-reference.service.js';

export class DevcontainerLifecycleService implements LifecycleAutomation {
  constructor(
    private readonly runner: ProcessRunner,
    private readonly fileSystem: FileSystem,
    private readonly environment: EnvironmentGateway,
    private readonly contentHash: ContentHashService,
    private readonly references: ImageReferenceService,
    private readonly root: string,
    private readonly logger: (message: string) => void = console.log,
  ) {}

  up(): string {
    const expected = this.references.expected(this.contentHash.compute(this.root));
    this.references.assertConsistent(expected, this.root);

    const result = this.runner.run({
      args: [
        '--no-install',
        'devcontainer',
        'up',
        '--workspace-folder',
        this.root,
        '--config',
        join(this.root, '.devcontainer/devcontainer.json'),
      ],
      capture: true,
      command: 'npx',
      cwd: this.root,
      env: this.environment.withoutRegistrySecrets(process.env),
    });
    const containerId = ContainerId.fromDevcontainerOutput(result.stdout).value;
    this.fileSystem.writeText(join(this.root, '.devcontainer/.container-id'), `${containerId}\n`);
    this.logger(`Devcontainer is running: ${containerId}`);
    return containerId;
  }

  execute(command: readonly string[], options: LifecycleOptions = {}): void {
    if (command.length === 0) {
      throw new Error('The exec target requires a command after "--".');
    }
    const containerId =
      options.containerId ??
      this.readContainerId(join(this.root, '.devcontainer/.container-id')).value;

    this.runner.run({
      args: [
        '--no-install',
        'devcontainer',
        'exec',
        '--workspace-folder',
        this.root,
        '--container-id',
        containerId,
        ...command,
      ],
      command: 'npx',
      cwd: this.root,
      env: this.environment.withoutRegistrySecrets(process.env),
      log: false,
    });
  }

  down(options: LifecycleOptions = {}): void {
    const stateFile = join(this.root, '.devcontainer/.container-id');
    if (!options.containerId && !this.fileSystem.exists(stateFile)) {
      this.logger('Devcontainer is already stopped.');
      return;
    }
    const containerId = options.containerId ?? this.readContainerId(stateFile).value;
    const result = this.runner.run({
      args: ['rm', '--force', containerId],
      capture: true,
      check: false,
      command: 'docker',
      env: this.environment.withoutRegistrySecrets(process.env),
      log: false,
    });
    const detail = `${result.stderr}\n${result.stdout}`.toLowerCase();
    if (result.status !== 0 && !detail.includes('no such container')) {
      throw new ProcessFailure(`docker rm --force ${containerId}`, result.status, result.stderr);
    }
    this.fileSystem.remove(stateFile);
    this.logger(`Devcontainer removed: ${containerId}`);
  }

  readContainerId(filename: string): ContainerId {
    if (!this.fileSystem.exists(filename)) {
      throw new Error('No recorded devcontainer ID. Run the up target first.');
    }
    return ContainerId.from(
      this.fileSystem.readText(filename).trim(),
      `container ID in ${filename}`,
    );
  }
}
