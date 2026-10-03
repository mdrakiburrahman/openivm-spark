export const immutableImageTagPattern = /^[a-f0-9]{64}$/u;

export class ImageReference {
  private constructor(
    readonly repository: string,
    readonly tag: string,
  ) {}

  static fromTag(repository: string, tag: string): ImageReference {
    if (repository.trim().length === 0) {
      throw new Error('Image repository must not be empty.');
    }
    if (!immutableImageTagPattern.test(tag)) {
      throw new Error(`Invalid immutable devcontainer image tag: ${tag}`);
    }
    return new ImageReference(repository, tag);
  }

  toString(): string {
    return `${this.repository}:${this.tag}`;
  }
}
