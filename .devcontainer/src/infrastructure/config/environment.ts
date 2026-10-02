import { join } from 'node:path';

import { EnvironmentGateway, ToolchainVersions } from '../../application/ports/environment.js';
import { FileSystem } from '../../application/ports/file-system.js';

interface PackageManifest {
  devDependencies?: Record<string, string>;
  engines?: {
    node?: string;
    npm?: string;
  };
  packageManager?: string;
}

const requiredBuildPins = [
  'DUCKDB_COMMIT',
  'DUCKDB_REF',
  'LPTS_BRANCH',
  'LPTS_COMMIT',
  'LPTS_REPO',
  'NATIVE_BUILD_JOBS',
  'OPENIVM_BRANCH',
  'OPENIVM_COMMIT',
  'OPENIVM_REPO',
  'SBT_VERSION',
  'UBUNTU_MIRROR',
] as const;

const requiredToolchainPins = ['GH_CLI_SHA256', 'GH_CLI_VERSION'] as const;

export class EnvironmentConfiguration implements EnvironmentGateway {
  constructor(private readonly fileSystem: FileSystem) {}

  readEnvFile(filename: string): Record<string, string> {
    return this.fileSystem.exists(filename) ? parseEnvFile(this.fileSystem.readText(filename)) : {};
  }

  publishingEnvironment(
    root: string,
    processEnvironment: NodeJS.ProcessEnv = process.env,
  ): NodeJS.ProcessEnv {
    return {
      ...this.readEnvFile(join(root, '.env')),
      ...definedEnvironment(processEnvironment),
    };
  }

  readToolchainVersions(root: string): ToolchainVersions {
    const manifest = this.readPackageManifest(root);
    const node = manifest.engines?.node;
    const npmEngine = manifest.engines?.npm;
    const npmPackageManager = manifest.packageManager?.match(/^npm@(.+)$/u)?.[1];

    if (!node || !npmEngine || !npmPackageManager || npmEngine !== npmPackageManager) {
      throw new Error(
        'package.json must exact-pin matching Node and npm versions in engines and packageManager.',
      );
    }

    return {
      node,
      npm: npmEngine,
    };
  }

  readPinnedDevDependency(root: string, name: string): string {
    const version = this.readPackageManifest(root).devDependencies?.[name];
    if (!version || !/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/u.test(version)) {
      throw new Error(`package.json must exact-pin ${name}.`);
    }
    return version;
  }

  buildEnvironment(
    root: string,
    processEnvironment: NodeJS.ProcessEnv = process.env,
  ): NodeJS.ProcessEnv {
    const pins = this.readEnvFile(join(root, 'spark-ext/dev/pins.env'));
    const toolchainPins = this.readEnvFile(join(root, '.devcontainer/toolchain.env'));
    const missing = requiredBuildPins.filter((key) => !pins[key]);
    const missingToolchainPins = requiredToolchainPins.filter((key) => !toolchainPins[key]);
    if (missing.length > 0 || missingToolchainPins.length > 0) {
      throw new Error(
        `Missing required image build pins: ${[...missing, ...missingToolchainPins].join(', ')}`,
      );
    }

    const toolchain = this.readToolchainVersions(root);
    return withoutRegistrySecrets({
      ...definedEnvironment(processEnvironment),
      ...pins,
      ...toolchainPins,
      OPENIVM_NODE_VERSION: toolchain.node,
      OPENIVM_NPM_VERSION: toolchain.npm,
    });
  }

  withoutRegistrySecrets(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
    return withoutRegistrySecrets(environment);
  }

  private readPackageManifest(root: string): PackageManifest {
    return JSON.parse(this.fileSystem.readText(join(root, 'package.json'))) as PackageManifest;
  }
}

export function parseEnvFile(contents: string): Record<string, string> {
  const parsed: Record<string, string> = {};

  for (const originalLine of contents.split(/\r?\n/u)) {
    const line = originalLine.trim();
    if (line.length === 0 || line.startsWith('#')) {
      continue;
    }

    const match = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/u.exec(line);
    if (!match) {
      throw new Error(`Invalid environment assignment: ${originalLine}`);
    }

    const key = match[1]!;
    let value = match[2] ?? '';
    if (
      value.length >= 2 &&
      ((value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'")))
    ) {
      value = value.slice(1, -1);
    }
    parsed[key] = value;
  }

  return parsed;
}

export function withoutRegistrySecrets(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const sanitized = { ...environment };
  delete sanitized.GHCR_TOKEN;
  delete sanitized.GITHUB_TOKEN;
  return sanitized;
}

function definedEnvironment(environment: NodeJS.ProcessEnv): Record<string, string> {
  return Object.fromEntries(
    Object.entries(environment).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
}
