export interface ToolchainVersions {
  node: string;
  npm: string;
}

export interface EnvironmentGateway {
  buildEnvironment(root: string, processEnvironment?: NodeJS.ProcessEnv): NodeJS.ProcessEnv;
  publishingEnvironment(root: string, processEnvironment?: NodeJS.ProcessEnv): NodeJS.ProcessEnv;
  readEnvFile(filename: string): Record<string, string>;
  readPinnedDevDependency(root: string, name: string): string;
  readToolchainVersions(root: string): ToolchainVersions;
  withoutRegistrySecrets(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv;
}
