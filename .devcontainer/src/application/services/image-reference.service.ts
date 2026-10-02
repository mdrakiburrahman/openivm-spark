import { join, relative } from 'node:path';

import { FileSystem } from '../ports/file-system.js';
import { ImageReference } from '../../domain/image/image-reference.js';

interface DevcontainerConfig {
  image?: string;
  [key: string]: unknown;
}

export interface ImageReferenceOccurrence {
  filename: string;
  reference: string;
}

export interface ImageReferenceSettings {
  imageRepository: string;
  root: string;
  sourceRepository: string;
}

export class ImageReferenceService {
  constructor(
    private readonly fileSystem: FileSystem,
    private readonly settings: ImageReferenceSettings,
  ) {}

  expected(tag: string): string {
    return ImageReference.fromTag(this.settings.imageRepository, tag).toString();
  }

  readConsumer(
    filename: string = join(this.settings.root, '.devcontainer/devcontainer.json'),
  ): string {
    const config = JSON.parse(this.fileSystem.readText(filename)) as DevcontainerConfig;
    if (!config.image) {
      throw new Error(`${filename} does not define an image.`);
    }
    return config.image;
  }

  updateConsumerReferences(reference: string, root: string = this.settings.root): string[] {
    const updatedFiles: string[] = [];
    const consumer = join(root, '.devcontainer/devcontainer.json');
    this.updateDevcontainerReference(reference, consumer);
    updatedFiles.push(relative(root, consumer));

    const workflows = join(root, '.github/workflows');
    if (!this.fileSystem.exists(workflows)) {
      return updatedFiles;
    }
    for (const filename of this.collectYamlFiles(workflows)) {
      const contents = this.fileSystem.readText(filename);
      const updated = contents.replace(this.imageReferencePattern(), reference);
      if (updated !== contents) {
        this.fileSystem.writeText(filename, updated);
        updatedFiles.push(relative(root, filename));
      }
    }
    return updatedFiles;
  }

  occurrences(root: string = this.settings.root): ImageReferenceOccurrence[] {
    const consumer = join(root, '.devcontainer/devcontainer.json');
    const occurrences: ImageReferenceOccurrence[] = [
      {
        filename: relative(root, consumer),
        reference: this.readConsumer(consumer),
      },
    ];
    const workflows = join(root, '.github/workflows');
    if (!this.fileSystem.exists(workflows)) {
      return occurrences;
    }

    for (const filename of this.collectYamlFiles(workflows)) {
      const contents = this.fileSystem.readText(filename);
      for (const match of contents.matchAll(this.imageReferencePattern())) {
        occurrences.push({
          filename: relative(root, filename),
          reference: match[0],
        });
      }
    }
    return occurrences;
  }

  assertConsistent(expected: string, root: string = this.settings.root): void {
    const mismatches = this.occurrences(root).filter(
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

  assertOciSourceLabel(root: string = this.settings.root): void {
    const dockerfile = this.fileSystem.readText(join(root, '.devcontainer/Dockerfile'));
    const expected = `org.opencontainers.image.source="${this.settings.sourceRepository}"`;
    if (!dockerfile.includes(expected)) {
      throw new Error(`The devcontainer Dockerfile must include ${expected}.`);
    }
  }

  private updateDevcontainerReference(reference: string, filename: string): void {
    const contents = this.fileSystem.readText(filename);
    const config = JSON.parse(contents) as DevcontainerConfig;
    if (!config.image) {
      throw new Error(`${filename} does not define an image.`);
    }
    const updated = contents.replace(
      /("image"\s*:\s*")[^"]+(")/u,
      (_match, prefix: string, suffix: string) => `${prefix}${reference}${suffix}`,
    );
    this.fileSystem.writeText(filename, updated);
  }

  private collectYamlFiles(directory: string): string[] {
    return this.fileSystem.entries(directory).flatMap((entry) => {
      const filename = join(directory, entry.name);
      if (entry.kind === 'directory') {
        return this.collectYamlFiles(filename);
      }
      return entry.kind === 'file' && /\.(?:yaml|yml)$/u.test(entry.name) ? [filename] : [];
    });
  }

  private imageReferencePattern(): RegExp {
    return new RegExp(`${escapeRegExp(this.settings.imageRepository)}:[A-Za-z0-9._-]+`, 'gu');
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}
