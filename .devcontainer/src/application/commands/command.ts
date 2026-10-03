export interface CliCommand {
  readonly name: string;
  execute(args: readonly string[]): Promise<void> | void;
}

export interface CommandExtension {
  register(registry: CommandRegistryPort): void;
}

export interface CommandRegistryPort {
  register(command: CliCommand): void;
}
