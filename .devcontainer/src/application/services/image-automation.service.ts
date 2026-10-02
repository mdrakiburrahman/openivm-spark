import { join } from 'node:path';

import {
  ImageAutomation,
  ImageOperationOptions,
  PublishOptions,
  PublishResult,
} from '../ports/automation.js';
import { ContainerRegistry } from '../ports/container-registry.js';
import { EnvironmentGateway } from '../ports/environment.js';
import { ProcessRunner } from '../ports/process-runner.js';
import { ContentHashService } from './content-hash.service.js';
import { ImageReferenceService } from './image-reference.service.js';
import { resolveRegistryCredentials } from './registry-credentials.js';

export interface ImageSmokeTester {
  testImage(reference: string): void;
}

export interface ImageAutomationDependencies {
  contentHash: ContentHashService;
  environment: EnvironmentGateway;
  references: ImageReferenceService;
  registry: ContainerRegistry;
  runner: ProcessRunner;
  smoke: ImageSmokeTester;
}

export class ImageAutomationService implements ImageAutomation {
  constructor(
    private readonly dependencies: ImageAutomationDependencies,
    private readonly root: string,
    private readonly logger: (message: string) => void = console.log,
  ) {}

  tag(updateConsumer: boolean): string {
    const tag = this.dependencies.contentHash.writeTag(this.root);
    const reference = this.dependencies.references.expected(tag);
    if (updateConsumer) {
      this.dependencies.references.updateConsumerReferences(reference, this.root);
    }
    return reference;
  }

  build(options: ImageOperationOptions = {}): string {
    const reference = options.reference ?? this.imageReference();

    if (!options.force && this.localImageExists(reference)) {
      this.logger(`Image already exists locally; skipping build: ${reference}`);
      return reference;
    }

    this.logger(`Building immutable devcontainer image: ${reference}`);
    this.dependencies.runner.run({
      args: [
        '--no-install',
        'devcontainer',
        'build',
        '--workspace-folder',
        this.root,
        '--config',
        join(this.root, '.devcontainer/devcontainer.local.json'),
        '--image-name',
        reference,
      ],
      command: 'npx',
      cwd: this.root,
      env: this.dependencies.environment.buildEnvironment(this.root),
    });

    if (!this.localImageExists(reference)) {
      throw new Error(`Dev Container CLI completed without creating ${reference}.`);
    }
    return reference;
  }

  test(): void {
    const reference = this.imageReference();
    this.dependencies.references.assertConsistent(reference, this.root);
    this.dependencies.references.assertOciSourceLabel(this.root);
    this.dependencies.smoke.testImage(reference);
  }

  publish(options: PublishOptions = {}): PublishResult {
    const environment = this.dependencies.environment.publishingEnvironment(
      this.root,
      options.environment,
    );
    const credentials = resolveRegistryCredentials(environment);
    const reference = options.reference ?? this.imageReference();

    this.logger('Authenticating to ghcr.io for immutable image publication.');
    this.dependencies.registry.authenticate(credentials, environment);

    if (this.dependencies.registry.manifestExists(reference, environment)) {
      this.logger(`Remote manifest already exists; skipping publish: ${reference}`);
      return 'skipped';
    }

    if (!this.localImageExists(reference, environment)) {
      this.build({
        force: false,
        reference,
      });
    }

    this.logger(`Publishing immutable devcontainer image: ${reference}`);
    this.dependencies.registry.push(reference, environment);

    if (!this.dependencies.registry.manifestExists(reference, environment)) {
      throw new Error(`Push completed but the remote manifest is unavailable: ${reference}`);
    }
    return 'published';
  }

  private imageReference(): string {
    const tag = this.dependencies.contentHash.readTag(join(this.root, '.devcontainer/.image-tag'));
    return this.dependencies.references.expected(tag);
  }

  private localImageExists(
    reference: string,
    environment: NodeJS.ProcessEnv = process.env,
  ): boolean {
    return (
      this.dependencies.runner.run({
        args: ['image', 'inspect', reference],
        capture: true,
        check: false,
        command: 'docker',
        env: this.dependencies.environment.withoutRegistrySecrets(environment),
        log: false,
      }).status === 0
    );
  }
}
