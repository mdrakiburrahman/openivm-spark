export interface RegistryCredentials {
  source: 'github-actions' | 'local';
  token: string;
  username: string;
}

export interface ContainerRegistry {
  authenticate(credentials: RegistryCredentials, environment: NodeJS.ProcessEnv): void;
  manifestExists(reference: string, environment: NodeJS.ProcessEnv): boolean;
  push(reference: string, environment: NodeJS.ProcessEnv): void;
}
