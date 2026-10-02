import assert from 'node:assert/strict';
import test from 'node:test';

import { ImageAutomation, LifecycleAutomation, SmokeAutomation } from '../ports/automation.js';
import { CliApplication } from '../../interfaces/cli/cli-application.js';
import { CommandRegistry } from './command-registry.js';
import { ImageCommandExtension } from './image-command-extension.js';
import { LifecycleCommandExtension } from './lifecycle-command-extension.js';

test('command extensions register independently without a central command switch', async () => {
  const events: string[] = [];
  const images: ImageAutomation = {
    build: (options = {}) => {
      events.push(`build:${String(options.force)}`);
      return 'reference';
    },
    publish: () => 'skipped',
    tag: (updateConsumer) => {
      events.push(`tag:${String(updateConsumer)}`);
      return 'reference';
    },
    test: () => undefined,
  };
  const registry = new CommandRegistry().extend(
    new ImageCommandExtension(images, (message) => events.push(`output:${message}`)),
  );

  await registry.execute('tag', ['--update-consumer']);
  await registry.execute('build', ['--force']);

  assert.deepEqual(events, ['tag:true', 'output:reference', 'build:true']);
});

test('CLI and lifecycle extension preserve separator and container-id argument behavior', async () => {
  const events: string[] = [];
  const lifecycle: LifecycleAutomation = {
    down: (options = {}) => events.push(`down:${options.containerId ?? ''}`),
    execute: (command, options = {}) =>
      events.push(`exec:${options.containerId ?? ''}:${command.join('|')}`),
    up: () => 'container',
  };
  const smoke: SmokeAutomation = {
    cleanup: () => [],
  };
  const app = new CliApplication(
    new CommandRegistry().extend(new LifecycleCommandExtension(lifecycle, smoke)),
  );

  await app.run(['exec', '--', 'printf', '%s', 'hello world']);
  await app.run(['exec', '--container-id', 'abc123', 'true']);
  await app.run(['down', '--container-id', 'abc123']);

  assert.deepEqual(events, ['exec::printf|%s|hello world', 'exec:abc123:true', 'down:abc123']);
});

test('duplicate command registration fails fast', () => {
  const registry = new CommandRegistry();
  const command = { name: 'extension', execute: () => undefined };
  registry.register(command);
  assert.throws(() => registry.register(command), /already registered/u);
});
