import { RegistryCredentials } from '../ports/container-registry.js';

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
