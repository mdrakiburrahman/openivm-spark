import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const workspaceRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
export const imageRepository = 'ghcr.io/mdrakiburrahman/openivm-spark-devcontainer';
export const sourceRepository = 'https://github.com/mdrakiburrahman/openivm-spark';
export const supportedImagePlatform = 'linux/amd64';
export const smokeContainerLabel = 'org.openivm.spark.devcontainer.smoke=true';
export const imageTagFile = resolve(workspaceRoot, '.devcontainer/.image-tag');
export const containerIdFile = resolve(workspaceRoot, '.devcontainer/.container-id');
export const consumerConfigFile = resolve(workspaceRoot, '.devcontainer/devcontainer.json');
export const localConfigFile = resolve(workspaceRoot, '.devcontainer/devcontainer.local.json');
export const immutableTagPattern = /^[a-f0-9]{64}$/u;
