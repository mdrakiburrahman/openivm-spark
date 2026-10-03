import { CommandRegistry } from '../../application/commands/command-registry.js';
import { ImageCommandExtension } from '../../application/commands/image-command-extension.js';
import { LifecycleCommandExtension } from '../../application/commands/lifecycle-command-extension.js';
import { ContentHashService } from '../../application/services/content-hash.service.js';
import { ImageAutomationService } from '../../application/services/image-automation.service.js';
import { ImageReferenceService } from '../../application/services/image-reference.service.js';
import { DevcontainerLifecycleService } from '../../application/services/lifecycle.service.js';
import { SmokeTestService } from '../../application/services/smoke-test.service.js';
import {
  imageRepository,
  publishedImageTagAliases,
  smokeContainerLabel,
  sourceRepository,
  supportedImagePlatform,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { EnvironmentConfiguration } from '../../infrastructure/config/environment.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { GitImageInputSource } from '../../infrastructure/git/git-image-input-source.js';
import { NodeProcessRunner } from '../../infrastructure/process/node-process-runner.js';
import { DockerRegistry } from '../../infrastructure/registry/docker-registry.js';
import { CliApplication } from './cli-application.js';

export function createCliApplication(): CliApplication {
  const fileSystem = new NodeFileSystem();
  const runner = new NodeProcessRunner();
  const environment = new EnvironmentConfiguration(fileSystem);
  const contentHash = new ContentHashService(
    fileSystem,
    new GitImageInputSource(runner),
    workspaceRoot,
    publishedImageTagAliases,
  );
  const references = new ImageReferenceService(fileSystem, {
    imageRepository,
    root: workspaceRoot,
    sourceRepository,
  });
  const smoke = new SmokeTestService(runner, fileSystem, environment, {
    containerLabel: smokeContainerLabel,
    imagePlatform: supportedImagePlatform,
    root: workspaceRoot,
    sourceRepository,
  });
  const images = new ImageAutomationService(
    {
      contentHash,
      environment,
      references,
      registry: new DockerRegistry(runner),
      runner,
      smoke,
    },
    workspaceRoot,
  );
  const lifecycle = new DevcontainerLifecycleService(
    runner,
    fileSystem,
    environment,
    contentHash,
    references,
    workspaceRoot,
  );
  const commands = new CommandRegistry()
    .extend(new ImageCommandExtension(images))
    .extend(new LifecycleCommandExtension(lifecycle, smoke));

  return new CliApplication(commands);
}
