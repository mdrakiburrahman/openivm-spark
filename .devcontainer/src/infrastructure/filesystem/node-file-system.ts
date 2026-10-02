import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';

import { FileSystem, FileSystemEntry } from '../../application/ports/file-system.js';

export class NodeFileSystem implements FileSystem {
  ensureDirectory(path: string): void {
    mkdirSync(path, { recursive: true });
  }

  entries(path: string): FileSystemEntry[] {
    return readdirSync(path, { withFileTypes: true }).map((entry) => ({
      kind: entry.isDirectory() ? 'directory' : entry.isFile() ? 'file' : 'other',
      name: entry.name,
    }));
  }

  exists(path: string): boolean {
    return existsSync(path);
  }

  groupId(path: string): number {
    return statSync(path).gid;
  }

  readText(path: string): string {
    return readFileSync(path, 'utf8');
  }

  remove(path: string): void {
    rmSync(path, { force: true });
  }

  writeText(path: string, contents: string): void {
    writeFileSync(path, contents, 'utf8');
  }
}
