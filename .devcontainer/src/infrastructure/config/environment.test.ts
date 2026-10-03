import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveRegistryCredentials } from '../../application/services/registry-credentials.js';
import { parseEnvFile } from './environment.js';

test('environment parsing supports comments, export, and quoted values', () => {
  assert.deepEqual(
    parseEnvFile(`
      # ignored
      export FIRST=value
      SECOND="two words"
      THIRD='three words'
    `),
    {
      FIRST: 'value',
      SECOND: 'two words',
      THIRD: 'three words',
    },
  );
});

test('local GHCR credentials fail fast when either value is missing', () => {
  assert.throws(
    () => resolveRegistryCredentials({ GHCR_USERNAME: 'owner' }),
    /requires both GHCR_TOKEN and GHCR_USERNAME/u,
  );
  assert.throws(
    () => resolveRegistryCredentials({ GHCR_TOKEN: 'token' }),
    /requires both GHCR_TOKEN and GHCR_USERNAME/u,
  );
  assert.throws(() => resolveRegistryCredentials({}), /requires GHCR_TOKEN and GHCR_USERNAME/u);
});

test('GitHub Actions publication uses GITHUB_TOKEN without requiring local credentials', () => {
  assert.deepEqual(
    resolveRegistryCredentials({
      GITHUB_ACTIONS: 'true',
      GITHUB_ACTOR: 'octocat',
      GITHUB_TOKEN: 'actions-token',
    }),
    {
      source: 'github-actions',
      token: 'actions-token',
      username: 'octocat',
    },
  );
});
