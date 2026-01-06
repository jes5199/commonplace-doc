import type { CommonplaceSDK, DocHandle, OutputHandle } from "./types.ts";

const SERVER = Deno.env.get("COMMONPLACE_SERVER") || "http://localhost:3000";
const BROKER = Deno.env.get("COMMONPLACE_BROKER") || "localhost:1883";
const OUTPUT_PATH = Deno.env.get("COMMONPLACE_OUTPUT") || "";
const CLIENT_ID = Deno.env.get("COMMONPLACE_CLIENT_ID") || `cp-${crypto.randomUUID()}`;

class DocHandleImpl implements DocHandle {
  constructor(private path: string) {}

  async get(): Promise<string> {
    // TODO: Implement sync protocol
    throw new Error("Not implemented");
  }

  onChange(_cb: (content: string) => void): void {
    // TODO: Subscribe to blue port
    throw new Error("Not implemented");
  }

  onEvent(_name: string, _cb: (payload: unknown) => void): void {
    // TODO: Subscribe to red port
    throw new Error("Not implemented");
  }

  async command(_verb: string, _payload?: unknown): Promise<void> {
    // TODO: Publish to magenta port
    throw new Error("Not implemented");
  }
}

class OutputHandleImpl implements OutputHandle {
  constructor(private path: string) {}

  async get(): Promise<string> {
    // TODO: Get current content
    throw new Error("Not implemented");
  }

  async set(_content: string, _opts?: { message?: string }): Promise<void> {
    // TODO: Publish to blue port
    throw new Error("Not implemented");
  }
}

const commandHandlers = new Map<string, (payload: unknown) => void>();

export const cp: CommonplaceSDK = {
  doc(path: string): DocHandle {
    return new DocHandleImpl(path);
  },

  output: new OutputHandleImpl(OUTPUT_PATH),

  onCommand(verb: string, cb: (payload: unknown) => void): void {
    commandHandlers.set(verb, cb);
  },

  emit(_name: string, _payload?: unknown): void {
    // TODO: Publish to red port
    throw new Error("Not implemented");
  },

  async start(): Promise<void> {
    console.log(`[cp] Starting with client_id=${CLIENT_ID}`);
    console.log(`[cp] Server: ${SERVER}, Broker: ${BROKER}`);
    console.log(`[cp] Output: ${OUTPUT_PATH}`);
    // TODO: Connect to MQTT and start event loop
  },
};

export type { CommonplaceSDK, DocHandle, OutputHandle };
