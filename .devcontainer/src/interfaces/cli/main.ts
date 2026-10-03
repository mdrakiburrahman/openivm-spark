import { ProcessFailure } from '../../application/ports/process-runner.js';
import { createCliApplication } from './composition-root.js';

try {
  await createCliApplication().run(process.argv.slice(2));
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[devcontainer] ${message}`);
  process.exitCode = error instanceof ProcessFailure ? error.status : 1;
}
