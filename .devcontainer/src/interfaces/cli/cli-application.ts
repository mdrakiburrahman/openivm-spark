import { CommandRegistry } from '../../application/commands/command-registry.js';

export class CliApplication {
  constructor(private readonly commands: CommandRegistry) {}

  async run(argv: readonly string[]): Promise<void> {
    const [command, ...rawArgs] = argv;
    const args = rawArgs[0] === '--' ? rawArgs.slice(1) : rawArgs;
    await this.commands.execute(command, args);
  }
}
