import {
  spawnSync,
  type SpawnSyncOptionsWithStringEncoding,
  type StdioOptions,
} from 'node:child_process';

import {
  CommandResult,
  CommandSpec,
  ProcessFailure,
  ProcessRunner,
} from '../../application/ports/process-runner.js';

export class NodeProcessRunner implements ProcessRunner {
  constructor(private readonly logger: (message: string) => void = console.log) {}

  run(spec: CommandSpec): CommandResult {
    const args = [...(spec.args ?? [])];
    if (spec.log !== false) {
      this.logger(`$ ${[spec.command, ...args].map(shellQuote).join(' ')}`);
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
}

function shellQuote(value: string): string {
  return /^[A-Za-z0-9_./:@%+=,-]+$/u.test(value) ? value : `'${value.replaceAll("'", "'\\''")}'`;
}
