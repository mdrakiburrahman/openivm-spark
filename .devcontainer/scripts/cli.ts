import { workspaceRoot } from './constants.js';
import { writeImageTag } from './hash.js';
import { buildImage, imageReference, publishImage } from './image.js';
import { down, execute, up } from './lifecycle.js';
import {
  assertImageReferencesConsistent,
  assertOciSourceLabel,
  expectedImageReference,
  updateConsumerImageReferences,
} from './references.js';
import { ProcessFailure } from './process.js';
import { cleanupSmokeContainers, smokeTestImage } from './smoke.js';

async function main(): Promise<void> {
  const [command, ...rawArgs] = process.argv.slice(2);
  const args = rawArgs[0] === '--' ? rawArgs.slice(1) : rawArgs;

  switch (command) {
    case 'tag': {
      const tag = writeImageTag(workspaceRoot);
      const reference = expectedImageReference(tag);
      if (args.includes('--update-consumer')) {
        updateConsumerImageReferences(reference);
      }
      console.log(reference);
      return;
    }
    case 'build':
      buildImage({ force: args.includes('--force') });
      return;
    case 'test': {
      const reference = imageReference();
      assertImageReferencesConsistent(reference);
      assertOciSourceLabel();
      smokeTestImage({ reference });
      return;
    }
    case 'publish':
      assertImageReferencesConsistent(imageReference());
      assertOciSourceLabel();
      publishImage({ force: args.includes('--force') });
      return;
    case 'up':
      up();
      return;
    case 'exec': {
      const { containerId, commandArgs } = parseExecArguments(args);
      execute(commandArgs, containerId ? { containerId } : {});
      return;
    }
    case 'down': {
      const containerId = optionValue(args, '--container-id');
      down(containerId ? { containerId } : {});
      return;
    }
    case 'cleanup':
      cleanupSmokeContainers();
      return;
    default:
      throw new Error(
        'Usage: cli.ts <tag|build|test|publish|up|exec|down|cleanup> [--force] [--container-id ID] [-- command]',
      );
  }
}

function parseExecArguments(args: readonly string[]): {
  commandArgs: string[];
  containerId?: string;
} {
  if (args[0] === '--container-id') {
    const containerId = args[1];
    if (!containerId) {
      throw new Error('--container-id requires a value.');
    }
    return {
      commandArgs: args.slice(2),
      containerId,
    };
  }
  return { commandArgs: [...args] };
}

function optionValue(args: readonly string[], option: string): string | undefined {
  const index = args.indexOf(option);
  if (index < 0) {
    return undefined;
  }
  const value = args[index + 1];
  if (!value) {
    throw new Error(`${option} requires a value.`);
  }
  return value;
}

try {
  await main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[devcontainer] ${message}`);
  process.exitCode = error instanceof ProcessFailure ? error.status : 1;
}
