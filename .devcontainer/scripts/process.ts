import {
  spawnSync,
  type SpawnSyncOptionsWithStringEncoding,
  type StdioOptions,
} from 'node:child_process';

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

export type CommandRunner = (spec: CommandSpec) => CommandResult;

export class ProcessFailure extends Error {
  readonly status: number;

  constructor(command: string, status: number, detail?: string) {
    const suffix = detail?.trim() ? `: ${detail.trim()}` : '';
    super(`Command failed with exit code ${status}: ${command}${suffix}`);
    this.name = 'ProcessFailure';
    this.status = status;
  }
}

export function runCommand(
  spec: CommandSpec,
  logger: (message: string) => void = console.log,
): CommandResult {
  const args = [...(spec.args ?? [])];
  if (spec.log !== false) {
    logger(`$ ${[spec.command, ...args].map(shellQuote).join(' ')}`);
  }

  const capture = spec.capture === true;
  const stdio: StdioOptions = capture
    ? ['pipe', 'pipe', 'pipe']
    : spec.input === undefined
      ? ('inherit' as const)
      : ['pipe', 'inherit', 'inherit'];
  const options: SpawnSyncOptionsWithStringEncoding = {
    encoding: 'utf8',
    stdio,
  };
  if (spec.cwd !== undefined) {
    options.cwd = spec.cwd;
  }
  if (spec.env !== undefined) {
    options.env = spec.env;
  }
  if (spec.input !== undefined) {
    options.input = spec.input;
  }
  const result = spawnSync(spec.command, args, options);

  if (result.error) {
    throw new ProcessFailure(spec.command, 1, result.error.message);
  }

  const normalized: CommandResult = {
    status: result.status ?? 1,
    stderr: typeof result.stderr === 'string' ? result.stderr : '',
    stdout: typeof result.stdout === 'string' ? result.stdout : '',
  };

  if (spec.check !== false && normalized.status !== 0) {
    const detail = normalized.stderr || normalized.stdout;
    throw new ProcessFailure(
      [spec.command, ...args].map(shellQuote).join(' '),
      normalized.status,
      detail.slice(0, 2_000),
    );
  }

  return normalized;
}

function shellQuote(value: string): string {
  return /^[A-Za-z0-9_./:@%+=,-]+$/u.test(value) ? value : `'${value.replaceAll("'", "'\\''")}'`;
}
