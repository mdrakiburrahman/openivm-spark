import { createHash } from 'node:crypto';
import { dirname, join, resolve, sep } from 'node:path';

import { FileSystem } from '../ports/file-system.js';
import { ImageInputSource } from '../ports/image-input-source.js';
import { immutableImageTagPattern } from '../../domain/image/image-reference.js';

export class ContentHashService {
  constructor(
    private readonly fileSystem: FileSystem,
    private readonly imageInputs: ImageInputSource,
    private readonly root: string,
    private readonly publishedTagAliases: Readonly<Record<string, string>> = {},
  ) {}

  imageInputFiles(root: string = this.root): string[] {
    return this.imageInputs.list(root);
  }

  compute(root: string = this.root, files: readonly string[] = this.imageInputFiles(root)): string {
    const hash = createHash('sha256');
    hash.update('openivm-spark-devcontainer-hash-v1\0');

    for (const relativeFilename of [...files].sort()) {
      const normalizedFilename = normalizeRelativePath(relativeFilename);
      const absoluteFilename = resolve(root, normalizedFilename);
      const contents = this.fileSystem.readText(absoluteFilename).replaceAll('\r\n', '\n');
      hash.update(`${normalizedFilename}\0${Buffer.byteLength(contents)}\0`);
      hash.update(contents);
      hash.update('\0');
    }

    const definitionHash = hash.digest('hex');
    return this.publishedTagAliases[definitionHash] ?? definitionHash;
  }

  writeTag(
    root: string = this.root,
    outputFile: string = join(root, '.devcontainer/.image-tag'),
  ): string {
    const tag = this.compute(root);
    this.fileSystem.ensureDirectory(dirname(outputFile));
    this.fileSystem.writeText(outputFile, `${tag}\n`);
    return tag;
  }

  readTag(filename: string): string {
    const tag = this.fileSystem.readText(filename).trim();
    if (!immutableImageTagPattern.test(tag)) {
      throw new Error(`Invalid devcontainer image tag in ${filename}. Run the tag target first.`);
    }
    return tag;
  }
}

function normalizeRelativePath(filename: string): string {
  return filename.split(sep).join('/');
}
