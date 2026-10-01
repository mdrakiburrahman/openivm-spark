import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

interface PackageManifest {
  devDependencies?: Record<string, string>;
  engines?: {
    node?: string;
    npm?: string;
  };
  packageManager?: string;
}

export interface RegistryCredentials {
  source: 'github-actions' | 'local';
  token: string;
  username: string;
}

export interface ToolchainVersions {
  node: string;
  npm: string;
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

export function readEnvFile(filename: string): Record<string, string> {
  return existsSync(filename) ? parseEnvFile(readFileSync(filename, 'utf8')) : {};
}

export function publishingEnvironment(
  root: string,
  processEnvironment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  return {
    ...readEnvFile(join(root, '.env')),
    ...definedEnvironment(processEnvironment),
  };
}

export function resolveRegistryCredentials(environment: NodeJS.ProcessEnv): RegistryCredentials {
  const localToken = environment.GHCR_TOKEN;
  const localUsername = environment.GHCR_USERNAME;

  if (localToken !== undefined || localUsername !== undefined) {
    if (!localToken || !localUsername) {
      throw new Error('Local GHCR publication requires both GHCR_TOKEN and GHCR_USERNAME.');
    }
    return {
      source: 'local',
      token: localToken,
      username: localUsername,
    };
  }

  if (environment.GITHUB_ACTIONS === 'true' || environment.CI === 'true') {
    const token = environment.GITHUB_TOKEN;
    const username = environment.GITHUB_ACTOR ?? environment.GITHUB_REPOSITORY_OWNER;
    if (!token || !username) {
      throw new Error(
        'CI GHCR publication requires GITHUB_TOKEN and GITHUB_ACTOR (or GITHUB_REPOSITORY_OWNER).',
      );
    }
    return {
      source: 'github-actions',
      token,
      username,
    };
  }

  throw new Error('Local GHCR publication requires GHCR_TOKEN and GHCR_USERNAME in .env.');
}

export function readToolchainVersions(root: string): ToolchainVersions {
  const manifest = readPackageManifest(root);
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

export function readPinnedDevDependency(root: string, name: string): string {
  const version = readPackageManifest(root).devDependencies?.[name];
  if (!version || !/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/u.test(version)) {
    throw new Error(`package.json must exact-pin ${name}.`);
  }
  return version;
}

export function buildEnvironment(
  root: string,
  processEnvironment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const pins = readEnvFile(join(root, 'spark-ext/dev/pins.env'));
  const toolchainPins = readEnvFile(join(root, '.devcontainer/toolchain.env'));
  const missing = requiredBuildPins.filter((key) => !pins[key]);
  const missingToolchainPins = requiredToolchainPins.filter((key) => !toolchainPins[key]);
  if (missing.length > 0 || missingToolchainPins.length > 0) {
    throw new Error(
      `Missing required image build pins: ${[...missing, ...missingToolchainPins].join(', ')}`,
    );
  }

  const toolchain = readToolchainVersions(root);
  return withoutRegistrySecrets({
    ...definedEnvironment(processEnvironment),
    ...pins,
    ...toolchainPins,
    OPENIVM_NODE_VERSION: toolchain.node,
    OPENIVM_NPM_VERSION: toolchain.npm,
  });
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

function readPackageManifest(root: string): PackageManifest {
  return JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')) as PackageManifest;
}
