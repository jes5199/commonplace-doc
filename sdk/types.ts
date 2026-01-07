// Content types determined by file extension
export type ContentType = 'json' | 'jsonl' | 'text';

// JSON-compatible value types
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

// Parsed content type - what get() returns based on file extension
export type ParsedContent = JsonValue | JsonValue[] | string;

export interface DocHandle {
  get(): Promise<ParsedContent>;
  onChange(cb: (content: ParsedContent) => void): void;
  onEvent(name: string, cb: (payload: unknown) => void): void;
  command(verb: string, payload?: unknown): Promise<void>;
}

export interface OutputHandle {
  get(): Promise<ParsedContent>;
  set(content: ParsedContent, opts?: { message?: string }): Promise<void>;
}

export interface CommonplaceSDK {
  doc(path: string): DocHandle;
  output: OutputHandle;
  onCommand(verb: string, cb: (payload: unknown) => void): void;
  emit(name: string, payload?: unknown): void;
  start(): Promise<void>;
}
