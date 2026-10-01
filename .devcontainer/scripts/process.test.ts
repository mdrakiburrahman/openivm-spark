import assert from 'node:assert/strict';
import test from 'node:test';

import { ProcessFailure, runCommand } from './process.js';

test('nonzero subprocess exit status is preserved', () => {
  assert.throws(
    () =>
      runCommand({
        args: ['-e', 'process.exit(23)'],
        capture: true,
        command: process.execPath,
        log: false,
      }),
    (error: unknown) => error instanceof ProcessFailure && error.status === 23,
  );
});

test('captured subprocess output remains available to callers', () => {
  const result = runCommand({
    args: ['-e', "process.stdout.write('ok')"],
    capture: true,
    command: process.execPath,
    log: false,
  });
  assert.equal(result.stdout, 'ok');
  assert.equal(result.status, 0);
});
