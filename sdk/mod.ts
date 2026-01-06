import type { CommonplaceSDK, DocHandle, OutputHandle } from "./types.ts";
import { connect, publish, subscribe } from "./mqtt.ts";
import * as Y from "npm:yjs@13";

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
  private doc = new Y.Doc();
  private text = this.doc.getText("content");
  private parentCommit: string | null = null;

  constructor(private path: string) {}

  async get(): Promise<string> {
    return this.text.toString();
  }

  async set(content: string, opts?: { message?: string }): Promise<void> {
    // Compute Yjs update
    const oldContent = this.text.toString();
    if (content !== oldContent) {
      this.doc.transact(() => {
        this.text.delete(0, this.text.length);
        this.text.insert(0, content);
      });
    }

    const update = Y.encodeStateAsUpdate(this.doc);
    const updateB64 = btoa(String.fromCharCode(...update));

    const editMsg = {
      update: updateB64,
      parents: this.parentCommit ? [this.parentCommit] : [],
      author: CLIENT_ID,
      message: opts?.message || "SDK update",
      timestamp: Date.now(),
    };

    publish(`${this.path}/edits`, editMsg);
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

    await connect(BROKER, CLIENT_ID);

    // Subscribe to commands for our output
    if (OUTPUT_PATH) {
      subscribe(`${OUTPUT_PATH}/commands/#`, (topic, payload) => {
        const verb = topic.split("/").pop()!;
        const handler = commandHandlers.get(verb);
        if (handler) {
          try {
            const msg = JSON.parse(payload.toString());
            handler(msg.payload);
          } catch (e) {
            console.error(`[cp] Error handling command ${verb}:`, e);
          }
        }
      });
    }

    console.log(`[cp] Ready`);
  },
};

export type { CommonplaceSDK, DocHandle, OutputHandle };
