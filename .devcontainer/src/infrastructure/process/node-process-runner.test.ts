import assert from 'node:assert/strict';
import test from 'node:test';

import { ProcessFailure } from '../../application/ports/process-runner.js';
import { NodeProcessRunner } from './node-process-runner.js';

test('nonzero subprocess exit status is preserved', () => {
  const runner = new NodeProcessRunner(() => undefined);
  assert.throws(
    () =>
      runner.run({
        args: ['-e', 'process.exit(23)'],
        capture: true,
        command: process.execPath,
        log: false,
      }),
    (error: unknown) => error instanceof ProcessFailure && error.status === 23,
  );
});

test('captured subprocess output remains available to callers', () => {
  const runner = new NodeProcessRunner(() => undefined);
  const result = runner.run({
    args: ['-e', "process.stdout.write('ok')"],
    capture: true,
    command: process.execPath,
    log: false,
  });
  assert.equal(result.stdout, 'ok');
  assert.equal(result.status, 0);
});
