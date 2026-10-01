import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';

import {
  consumerConfigFile,
  imageRepository,
  sourceRepository,
  workspaceRoot,
} from './constants.js';

interface DevcontainerConfig {
  image?: string;
  [key: string]: unknown;
}

export interface ImageReferenceOccurrence {
  filename: string;
  reference: string;
}

export function expectedImageReference(tag: string): string {
  return `${imageRepository}:${tag}`;
}

export function readConsumerImageReference(filename: string = consumerConfigFile): string {
  const config = JSON.parse(readFileSync(filename, 'utf8')) as DevcontainerConfig;
  if (!config.image) {
    throw new Error(`${filename} does not define an image.`);
  }
  return config.image;
}

export function updateConsumerImageReferences(
  reference: string,
  root: string = workspaceRoot,
): string[] {
  const updatedFiles: string[] = [];
  const consumer = join(root, '.devcontainer/devcontainer.json');
  updateDevcontainerImageReference(reference, consumer);
  updatedFiles.push(relative(root, consumer));

  const workflows = join(root, '.github/workflows');
  if (!existsSync(workflows)) {
    return updatedFiles;
  }
  for (const filename of collectYamlFiles(workflows)) {
    const contents = readFileSync(filename, 'utf8');
    const updated = contents.replace(imageReferencePattern(), reference);
    if (updated !== contents) {
      writeFileSync(filename, updated, 'utf8');
      updatedFiles.push(relative(root, filename));
    }
  }
  return updatedFiles;
}

function updateDevcontainerImageReference(reference: string, filename: string): void {
  const contents = readFileSync(filename, 'utf8');
  const config = JSON.parse(contents) as DevcontainerConfig;
  if (!config.image) {
    throw new Error(`${filename} does not define an image.`);
  }
  const updated = contents.replace(
    /("image"\s*:\s*")[^"]+(")/u,
    (_match, prefix: string, suffix: string) => `${prefix}${reference}${suffix}`,
  );
  writeFileSync(filename, updated, 'utf8');
}

export function imageReferenceOccurrences(
  root: string = workspaceRoot,
): ImageReferenceOccurrence[] {
  const consumer = join(root, '.devcontainer/devcontainer.json');
  const occurrences: ImageReferenceOccurrence[] = [
    {
      filename: relative(root, consumer),
      reference: readConsumerImageReference(consumer),
    },
  ];
  const workflows = join(root, '.github/workflows');
  if (!existsSync(workflows)) {
    return occurrences;
  }

  for (const filename of collectYamlFiles(workflows)) {
    const contents = readFileSync(filename, 'utf8');
    for (const match of contents.matchAll(imageReferencePattern())) {
      occurrences.push({
        filename: relative(root, filename),
        reference: match[0],
      });
    }
  }
  return occurrences;
}

export function assertImageReferencesConsistent(
  expected: string,
  root: string = workspaceRoot,
): void {
  const mismatches = imageReferenceOccurrences(root).filter(
    (occurrence) => occurrence.reference !== expected,
  );
  if (mismatches.length > 0) {
    throw new Error(
      `Devcontainer image reference mismatch:\n${mismatches
        .map((mismatch) => `- ${mismatch.filename}: ${mismatch.reference}`)
        .join('\n')}\nExpected: ${expected}`,
    );
  }
}

export function assertOciSourceLabel(root: string = workspaceRoot): void {
  const dockerfile = readFileSync(join(root, '.devcontainer/Dockerfile'), 'utf8');
  const expected = `org.opencontainers.image.source="${sourceRepository}"`;
  if (!dockerfile.includes(expected)) {
    throw new Error(`The devcontainer Dockerfile must include ${expected}.`);
  }
}

function imageReferencePattern(): RegExp {
  return new RegExp(`${escapeRegExp(imageRepository)}:[A-Za-z0-9._-]+`, 'gu');
}

function collectYamlFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const filename = join(directory, entry.name);
    if (entry.isDirectory()) {
      return collectYamlFiles(filename);
    }
    return entry.isFile() && /\.(?:yaml|yml)$/u.test(entry.name) ? [filename] : [];
  });
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}
