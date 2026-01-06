export interface DocHandle {
  get(): Promise<string>;
  onChange(cb: (content: string) => void): void;
  onEvent(name: string, cb: (payload: unknown) => void): void;
  command(verb: string, payload?: unknown): Promise<void>;
}

export interface OutputHandle {
  get(): Promise<string>;
  set(content: string, opts?: { message?: string }): Promise<void>;
}

export interface CommonplaceSDK {
  doc(path: string): DocHandle;
  output: OutputHandle;
  onCommand(verb: string, cb: (payload: unknown) => void): void;
  emit(name: string, payload?: unknown): void;
  start(): Promise<void>;
}
