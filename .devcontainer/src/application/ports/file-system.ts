export interface FileSystemEntry {
  kind: 'directory' | 'file' | 'other';
  name: string;
}

export interface FileSystem {
  ensureDirectory(path: string): void;
  entries(path: string): FileSystemEntry[];
  exists(path: string): boolean;
  groupId(path: string): number;
  readText(path: string): string;
  remove(path: string): void;
  writeText(path: string, contents: string): void;
}
