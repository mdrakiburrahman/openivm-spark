export interface CommandResult {
  status: number;
  stderr: string;
  stdout: string;
}

export interface CommandSpec {
  args?: readonly string[];
  capture?: boolean;
  check?: boolean;
  command: string;
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  input?: string;
  log?: boolean;
}

export interface ProcessRunner {
  run(spec: CommandSpec): CommandResult;
}

export class ProcessFailure extends Error {
  readonly status: number;

  constructor(command: string, status: number, detail?: string) {
    const suffix = detail?.trim() ? `: ${detail.trim()}` : '';
    super(`Command failed with exit code ${status}: ${command}${suffix}`);
    this.name = 'ProcessFailure';
    this.status = status;
  }
}
