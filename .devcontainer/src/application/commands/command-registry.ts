import { CliCommand, CommandExtension, CommandRegistryPort } from './command.js';

export class CommandRegistry implements CommandRegistryPort {
  private readonly commands = new Map<string, CliCommand>();

  extend(extension: CommandExtension): this {
    extension.register(this);
    return this;
  }

  register(command: CliCommand): void {
    if (this.commands.has(command.name)) {
      throw new Error(`Devcontainer command is already registered: ${command.name}`);
    }
    this.commands.set(command.name, command);
  }

  async execute(name: string | undefined, args: readonly string[]): Promise<void> {
    const command = name ? this.commands.get(name) : undefined;
    if (!command) {
      throw new Error(this.usage());
    }
    await command.execute(args);
  }

  usage(): string {
    return `Usage: devcontainer <${[...this.commands.keys()].join(
      '|',
    )}> [--force] [--container-id ID] [-- command]`;
  }
}
