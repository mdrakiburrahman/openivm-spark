import { ImageAutomation } from '../ports/automation.js';
import { CliCommand, CommandExtension, CommandRegistryPort } from './command.js';

export class ImageCommandExtension implements CommandExtension {
  constructor(
    private readonly images: ImageAutomation,
    private readonly output: (message: string) => void = console.log,
  ) {}

  register(registry: CommandRegistryPort): void {
    for (const command of [
      new TagCommand(this.images, this.output),
      new BuildCommand(this.images),
      new TestCommand(this.images),
      new PublishCommand(this.images),
    ]) {
      registry.register(command);
    }
  }
}

class TagCommand implements CliCommand {
  readonly name = 'tag';

  constructor(
    private readonly images: ImageAutomation,
    private readonly output: (message: string) => void,
  ) {}

  execute(args: readonly string[]): void {
    this.output(this.images.tag(args.includes('--update-consumer')));
  }
}

class BuildCommand implements CliCommand {
  readonly name = 'build';

  constructor(private readonly images: ImageAutomation) {}

  execute(args: readonly string[]): void {
    this.images.build({ force: args.includes('--force') });
  }
}

class TestCommand implements CliCommand {
  readonly name = 'test';

  constructor(private readonly images: ImageAutomation) {}

  execute(): void {
    this.images.test();
  }
}

class PublishCommand implements CliCommand {
  readonly name = 'publish';

  constructor(private readonly images: ImageAutomation) {}

  execute(args: readonly string[]): void {
    this.images.publish({ force: args.includes('--force') });
  }
}
