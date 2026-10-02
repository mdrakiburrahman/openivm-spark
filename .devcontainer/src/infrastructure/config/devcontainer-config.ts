import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const workspaceRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../../..');
export const imageRepository = 'ghcr.io/mdrakiburrahman/openivm-spark-devcontainer';
export const sourceRepository = 'https://github.com/mdrakiburrahman/openivm-spark';
export const supportedImagePlatform = 'linux/amd64';
export const smokeContainerLabel = 'org.openivm.spark.devcontainer.smoke=true';
export const imageTagFile = resolve(workspaceRoot, '.devcontainer/.image-tag');
export const containerIdFile = resolve(workspaceRoot, '.devcontainer/.container-id');
export const consumerConfigFile = resolve(workspaceRoot, '.devcontainer/devcontainer.json');
export const localConfigFile = resolve(workspaceRoot, '.devcontainer/devcontainer.local.json');

export const publishedImageTagAliases: Readonly<Record<string, string>> = Object.freeze({
  // The v1 input set included host-only automation. Preserve its published tag while
  // the equivalent build/runtime definition moves to the narrower canonical input set.
  '827aec65b9e43840e18077e98bf5c4849bb067c3fa5c205be5949d867ea495af':
    '6314c87a6da3f303b524e4421b4a21779b6af7224f34911a349141b846f6180f',
});
