export class ContainerId {
  private constructor(readonly value: string) {}

  static from(value: string, source = 'container ID'): ContainerId {
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]+$/u.test(value)) {
      throw new Error(`Invalid ${source}: ${value}`);
    }
    return new ContainerId(value);
  }

  static fromDevcontainerOutput(output: string): ContainerId {
    const candidates = [output.trim(), ...output.trim().split(/\r?\n/u).reverse()];
    for (const candidate of candidates) {
      try {
        const parsed = JSON.parse(candidate) as { containerId?: unknown };
        if (typeof parsed.containerId === 'string' && parsed.containerId.length > 0) {
          return ContainerId.from(parsed.containerId, 'Dev Container CLI containerId');
        }
      } catch {
        continue;
      }
    }
    throw new Error('Dev Container CLI output did not contain a containerId.');
  }
}
