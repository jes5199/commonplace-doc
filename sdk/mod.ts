import type { CommonplaceSDK, DocHandle, OutputHandle } from "./types.ts";
import { connect, publish, subscribe } from "./mqtt.ts";
import * as Y from "npm:yjs@13";

function sanitizePath(path: string): string {
  if (path.includes('#') || path.includes('+') || path.includes('\0')) {
    throw new Error(`Invalid path contains MQTT wildcards: ${path}`);
  }
  return path;
}

const SERVER = Deno.env.get("COMMONPLACE_SERVER") || "http://localhost:3000";
const BROKER = Deno.env.get("COMMONPLACE_BROKER") || "localhost:1883";
const OUTPUT_PATH = Deno.env.get("COMMONPLACE_OUTPUT") || "";
const CLIENT_ID = Deno.env.get("COMMONPLACE_CLIENT_ID") || `cp-${crypto.randomUUID()}`;

// Track MQTT connection state - subscriptions deferred until started
let mqttStarted = false;
const pendingDocHandles: DocHandleImpl[] = [];

class DocHandleImpl implements DocHandle {
  private doc = new Y.Doc();
  private text = this.doc.getText("content");
  private changeCallbacks: ((content: string) => void)[] = [];
  private eventSubscriptions: { topic: string; handler: (t: string, p: Buffer) => void; activated: boolean }[] = [];
  private editsSubscribed = false;
  private needsSubscription = false;
  private initialFetched = false;

  constructor(private path: string) {
    sanitizePath(path);
  }

  // Fetch initial state from server (separated from subscription activation)
  private async fetchInitialState(): Promise<void> {
    if (this.initialFetched) return;
    this.initialFetched = true;

    try {
      const pathEncoded = this.path.split("/").map(encodeURIComponent).join("/");
      const res = await fetch(`${SERVER}/files/${pathEncoded}/head`);
      if (res.ok) {
        const head = await res.json();
        if (head.state) {
          const state = Uint8Array.from(atob(head.state), (c) => c.charCodeAt(0));
          Y.applyUpdate(this.doc, state);
        }
      }
    } catch (e) {
      console.error(`[cp] Error fetching initial state for ${this.path}:`, e);
    }
  }

  // Called by cp.start() or when adding subscriptions after start - sets up MQTT subscriptions
  // Idempotent: can be called multiple times, only activates new subscriptions
  async activateSubscriptions(): Promise<void> {
    if (!this.needsSubscription) return;

    // Subscribe to edits for onChange callbacks (only once)
    if (!this.editsSubscribed && this.changeCallbacks.length > 0) {
      this.editsSubscribed = true;
      subscribe(`${this.path}/edits`, (_topic, payload) => {
        try {
          const msg = JSON.parse(payload.toString());
          const update = Uint8Array.from(atob(msg.update), (c) => c.charCodeAt(0));
          Y.applyUpdate(this.doc, update);

          const content = this.text.toString();
          for (const cb of this.changeCallbacks) {
            cb(content);
          }
        } catch (e) {
          console.error(`[cp] Error applying edit for ${this.path}:`, e);
        }
      });
    }

    // Activate event subscriptions that haven't been activated yet
    for (const sub of this.eventSubscriptions) {
      if (!sub.activated) {
        sub.activated = true;
        subscribe(sub.topic, sub.handler);
      }
    }

    // Fetch initial state via HTTP
    await this.fetchInitialState();
  }

  async get(): Promise<string> {
    // get() waits for MQTT to be ready
    if (!mqttStarted) {
      throw new Error("MQTT not connected. Call cp.start() before using doc.get()");
    }
    // Always fetch initial state if not done (even without subscriptions)
    await this.fetchInitialState();
    // Also activate subscriptions if needed (activateSubscriptions is idempotent)
    if (this.needsSubscription) {
      await this.activateSubscriptions();
    }
    return this.text.toString();
  }

  onChange(cb: (content: string) => void): void {
    this.changeCallbacks.push(cb);
    this.needsSubscription = true;
    // If MQTT already started, activate immediately; otherwise queue for later
    if (mqttStarted) {
      this.activateSubscriptions();
    } else if (!pendingDocHandles.includes(this)) {
      pendingDocHandles.push(this);
    }
  }

  onEvent(name: string, cb: (payload: unknown) => void): void {
    const topic = name === "*" ? `${this.path}/events/#` : `${this.path}/events/${name}`;

    const handler = (t: string, payload: Buffer) => {
      try {
        const msg = JSON.parse(payload.toString());
        if (name === "*") {
          const eventName = t.split("/events/")[1];
          (cb as (name: string, payload: unknown) => void)(eventName, msg.payload);
        } else {
          cb(msg.payload);
        }
      } catch (e) {
        console.error(`[cp] Error handling event:`, e);
      }
    };

    this.eventSubscriptions.push({ topic, handler, activated: false });
    this.needsSubscription = true;
    // If MQTT already started, activate immediately; otherwise queue for later
    if (mqttStarted) {
      this.activateSubscriptions();
    } else if (!pendingDocHandles.includes(this)) {
      pendingDocHandles.push(this);
    }
  }

  async command(verb: string, payload?: unknown): Promise<void> {
    sanitizePath(verb);
    publish(`${this.path}/commands/${verb}`, {
      payload: payload || {},
      source: CLIENT_ID,
    });
  }
}

class OutputHandleImpl implements OutputHandle {
  private doc = new Y.Doc();
  private text = this.doc.getText("content");
  private parentCommit: string | null = null;
  private initialized = false;

  constructor(private path: string) {
    if (path) sanitizePath(path);
  }

  // Fetch current state from server before first update
  private async ensureInitialized(): Promise<void> {
    if (this.initialized || !this.path) return;
    this.initialized = true;

    try {
      const pathEncoded = this.path.split("/").map(encodeURIComponent).join("/");
      const res = await fetch(`${SERVER}/files/${pathEncoded}/head`);
      if (res.ok) {
        const head = await res.json();
        if (head.state) {
          const state = Uint8Array.from(atob(head.state), (c) => c.charCodeAt(0));
          Y.applyUpdate(this.doc, state);
        }
        if (head.cid) {
          this.parentCommit = head.cid;
        }
      }
    } catch (e) {
      console.error(`[cp] Error fetching output state:`, e);
    }
  }

  async get(): Promise<string> {
    await this.ensureInitialized();
    return this.text.toString();
  }

  async set(content: string, opts?: { message?: string }): Promise<void> {
    // Ensure we have current state before updating
    await this.ensureInitialized();

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

  emit(name: string, payload?: unknown): void {
    if (!OUTPUT_PATH) {
      console.warn("[cp] Cannot emit: no output path configured");
      return;
    }

    sanitizePath(name);
    publish(`${OUTPUT_PATH}/events/${name}`, {
      payload: payload || {},
      source: CLIENT_ID,
    });
  },

  async start(): Promise<void> {
    console.log(`[cp] Starting with client_id=${CLIENT_ID}`);
    console.log(`[cp] Server: ${SERVER}, Broker: ${BROKER}`);
    console.log(`[cp] Output: ${OUTPUT_PATH}`);

    await connect(BROKER, CLIENT_ID);
    mqttStarted = true;

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

    // Activate pending doc handle subscriptions (registered before start)
    for (const handle of pendingDocHandles) {
      await handle.activateSubscriptions();
    }

    console.log(`[cp] Ready`);
  },
};

export type { CommonplaceSDK, DocHandle, OutputHandle };
