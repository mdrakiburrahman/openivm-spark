import { LifecycleAutomation, SmokeAutomation } from '../ports/automation.js';
import { CliCommand, CommandExtension, CommandRegistryPort } from './command.js';

export class LifecycleCommandExtension implements CommandExtension {
  constructor(
    private readonly lifecycle: LifecycleAutomation,
    private readonly smoke: SmokeAutomation,
  ) {}

  register(registry: CommandRegistryPort): void {
    for (const command of [
      new UpCommand(this.lifecycle),
      new ExecCommand(this.lifecycle),
      new DownCommand(this.lifecycle),
      new CleanupCommand(this.smoke),
    ]) {
      registry.register(command);
    }
  }
}

class UpCommand implements CliCommand {
  readonly name = 'up';

  constructor(private readonly lifecycle: LifecycleAutomation) {}

  execute(): void {
    this.lifecycle.up();
  }
}

class ExecCommand implements CliCommand {
  readonly name = 'exec';

  constructor(private readonly lifecycle: LifecycleAutomation) {}

  execute(args: readonly string[]): void {
    const { containerId, commandArgs } = parseExecArguments(args);
    this.lifecycle.execute(commandArgs, containerId ? { containerId } : {});
  }
}

class DownCommand implements CliCommand {
  readonly name = 'down';

  constructor(private readonly lifecycle: LifecycleAutomation) {}

  execute(args: readonly string[]): void {
    const containerId = optionValue(args, '--container-id');
    this.lifecycle.down(containerId ? { containerId } : {});
  }
}

class CleanupCommand implements CliCommand {
  readonly name = 'cleanup';

  constructor(private readonly smoke: SmokeAutomation) {}

  execute(): void {
    this.smoke.cleanup();
  }
}

export function parseExecArguments(args: readonly string[]): {
  commandArgs: string[];
  containerId?: string;
} {
  if (args[0] === '--container-id') {
    const containerId = args[1];
    if (!containerId) {
      throw new Error('--container-id requires a value.');
    }
    return {
      commandArgs: args.slice(2),
      containerId,
    };
  }
  return { commandArgs: [...args] };
}

export function optionValue(args: readonly string[], option: string): string | undefined {
  const index = args.indexOf(option);
  if (index < 0) {
    return undefined;
  }
  const value = args[index + 1];
  if (!value) {
    throw new Error(`${option} requires a value.`);
  }
  return value;
}
