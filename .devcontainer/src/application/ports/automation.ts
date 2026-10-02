export type PublishResult = 'published' | 'skipped';

export interface ImageOperationOptions {
  force?: boolean;
  reference?: string;
}

export interface PublishOptions extends ImageOperationOptions {
  environment?: NodeJS.ProcessEnv;
}

export interface ImageAutomation {
  build(options?: ImageOperationOptions): string;
  publish(options?: PublishOptions): PublishResult;
  tag(updateConsumer: boolean): string;
  test(): void;
}

export interface LifecycleOptions {
  containerId?: string;
}

export interface LifecycleAutomation {
  down(options?: LifecycleOptions): void;
  execute(command: readonly string[], options?: LifecycleOptions): void;
  up(): string;
}

export interface SmokeAutomation {
  cleanup(): string[];
}
