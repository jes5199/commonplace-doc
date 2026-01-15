import type { CommonplaceSDK, DocHandle, OutputHandle, ContentType, JsonValue, ParsedContent } from "./types.ts";
import { connect, publish, subscribe } from "./mqtt.ts";
import * as Y from "npm:yjs@13";

// Detect content type from file extension
function getContentType(path: string): ContentType {
  const ext = path.split('.').pop()?.toLowerCase() || '';
  switch (ext) {
    case 'json':
      return 'json';
    case 'jsonl':
      return 'jsonl';
    default:
      return 'text';
  }
}

// Parse string content based on content type
function parseContent(content: string, type: ContentType): JsonValue | JsonValue[] | string {
  switch (type) {
    case 'json':
      return content.trim() ? JSON.parse(content) : null;
    case 'jsonl':
      return content.trim()
        .split('\n')
        .filter(line => line.trim())
        .map(line => JSON.parse(line));
    case 'text':
    default:
      return content;
  }
}

// Serialize value to string based on content type
function serializeContent(value: JsonValue | JsonValue[] | string, type: ContentType): string {
  switch (type) {
    case 'json':
      return JSON.stringify(value, null, 2);
    case 'jsonl':
      if (!Array.isArray(value)) {
        throw new Error('JSONL output requires an array');
      }
      return value.map(item => JSON.stringify(item)).join('\n') + (value.length > 0 ? '\n' : '');
    case 'text':
    default:
      return String(value);
  }
}

// Read raw string content from Yjs doc based on content type
// Uses the correct Yjs type (Text/Array/Map) for each content type
function readYjsContent(doc: Y.Doc, type: ContentType): string {
  switch (type) {
    case 'json': {
      const map = doc.getMap("content");
      if (map.size === 0) return '';
      // Convert Y.Map to JSON object
      const obj: Record<string, unknown> = {};
      for (const [key, value] of map.entries()) {
        obj[key] = value;
      }
      return JSON.stringify(obj, null, 2);
    }
    case 'jsonl': {
      const arr = doc.getArray("content");
      if (arr.length === 0) return '';
      // Convert Y.Array to JSONL string
      const lines: string[] = [];
      for (let i = 0; i < arr.length; i++) {
        lines.push(JSON.stringify(arr.get(i)));
      }
      return lines.join('\n') + '\n';
    }
    case 'text':
    default: {
      const text = doc.getText("content");
      return text.toString();
    }
  }
}

// Write parsed content to Yjs doc based on content type
// Uses the correct Yjs type (Text/Array/Map) for each content type
// Note: For JSON, we dynamically choose Y.Map or Y.Array based on value type
// (matches server behavior in yjs.rs create_yjs_json_update_impl)
function writeYjsContent(doc: Y.Doc, type: ContentType, value: ParsedContent): void {
  doc.transact(() => {
    switch (type) {
      case 'json': {
        // JSON can be object or array at root - choose Yjs type dynamically
        if (Array.isArray(value)) {
          // Use Y.Array for array roots
          const arr = doc.getArray("content");
          if (arr.length > 0) {
            arr.delete(0, arr.length);
          }
          for (const item of value) {
            arr.push([item]);
          }
        } else if (value !== null && typeof value === 'object') {
          // Use Y.Map for object roots
          const map = doc.getMap("content");
          for (const key of [...map.keys()]) {
            map.delete(key);
          }
          for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
            map.set(k, v);
          }
        }
        // Note: primitives (null, string, number, bool) at JSON root are not
        // supported by Y.Map/Y.Array - server also rejects these
        break;
      }
      case 'jsonl': {
        const arr = doc.getArray("content");
        // Clear existing items
        if (arr.length > 0) {
          arr.delete(0, arr.length);
        }
        // Add new items
        if (Array.isArray(value)) {
          for (const item of value) {
            arr.push([item]);
          }
        }
        break;
      }
      case 'text':
      default: {
        const text = doc.getText("content");
        text.delete(0, text.length);
        text.insert(0, String(value));
        break;
      }
    }
  });
}

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
const WORKSPACE = Deno.env.get("COMMONPLACE_WORKSPACE") || "commonplace";

// Track MQTT connection state - subscriptions deferred until started
let mqttStarted = false;
const pendingDocHandles: DocHandleImpl[] = [];

class DocHandleImpl implements DocHandle {
  private doc = new Y.Doc();
  private changeCallbacks: ((content: ParsedContent) => void)[] = [];
  private eventSubscriptions: { topic: string; handler: (t: string, p: Buffer) => void; activated: boolean }[] = [];
  private editsSubscribed = false;
  private needsSubscription = false;
  private initialFetched = false;
  private contentType: ContentType;
  private cachedContent: string | null = null; // Cache for initial content before Yjs updates

  constructor(private path: string) {
    sanitizePath(path);
    this.contentType = getContentType(path);
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
        // Apply Yjs state if available (for CRDT sync compatibility)
        if (head.state) {
          const state = Uint8Array.from(atob(head.state), (c) => c.charCodeAt(0));
          Y.applyUpdate(this.doc, state);
        }
        // Store raw content in cache for get() to use
        this.cachedContent = head.content || '';
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

          // Clear cache so subsequent get() reads from Yjs
          this.cachedContent = null;

          // Read from correct Yjs type based on content type
          const raw = readYjsContent(this.doc, this.contentType);
          const parsed = parseContent(raw, this.contentType);
          for (const cb of this.changeCallbacks) {
            cb(parsed);
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

  async get(): Promise<ParsedContent> {
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
    // Use cached content if available (for initial read), otherwise read from Yjs
    const raw = this.cachedContent !== null
      ? this.cachedContent
      : readYjsContent(this.doc, this.contentType);
    return parseContent(raw, this.contentType);
  }

  onChange(cb: (content: ParsedContent) => void): void {
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
  private parentCommit: string | null = null;
  private initialized = false;
  private contentType: ContentType;
  private cachedContent: string | null = null;

  constructor(private path: string) {
    if (path) sanitizePath(path);
    this.contentType = path ? getContentType(path) : 'text';
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
        // Apply Yjs state if available (for CRDT sync compatibility)
        if (head.state) {
          const state = Uint8Array.from(atob(head.state), (c) => c.charCodeAt(0));
          Y.applyUpdate(this.doc, state);
        }
        // Cache content for initial read
        this.cachedContent = head.content || '';
        if (head.cid) {
          this.parentCommit = head.cid;
        }
      }
    } catch (e) {
      console.error(`[cp] Error fetching output state:`, e);
    }
  }

  async get(): Promise<ParsedContent> {
    await this.ensureInitialized();
    const raw = this.cachedContent !== null
      ? this.cachedContent
      : readYjsContent(this.doc, this.contentType);
    return parseContent(raw, this.contentType);
  }

  async set(value: ParsedContent, opts?: { message?: string }): Promise<void> {
    // Ensure we have current state before updating
    await this.ensureInitialized();

    // Use HTTP API to replace content (handles CRDT internally)
    const pathEncoded = this.path.split("/").map(encodeURIComponent).join("/");
    const content = serializeContent(value, this.contentType);

    // Build URL with parent_cid query param if we have it
    let url = `${SERVER}/files/${pathEncoded}/replace`;
    const params = new URLSearchParams();
    if (this.parentCommit) {
      params.set("parent_cid", this.parentCommit);
    }
    params.set("author", CLIENT_ID);
    if (params.toString()) {
      url += `?${params.toString()}`;
    }

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body: content,
      });

      if (!res.ok) {
        const text = await res.text();
        console.error(`[cp] Failed to set output: ${res.status} ${text}`);
      } else {
        // Update local state to match what we sent
        writeYjsContent(this.doc, this.contentType, value);
        this.cachedContent = content;
        // Get the new commit ID if returned
        const data = await res.json().catch(() => ({}));
        if (data.cid) {
          this.parentCommit = data.cid;
        }
      }
    } catch (e) {
      console.error(`[cp] Error setting output:`, e);
    }
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
    // Topic format: {workspace}/commands/{path}/{verb}
    if (OUTPUT_PATH) {
      const pathWithoutLeadingSlash = OUTPUT_PATH.replace(/^\//, '');
      const commandTopic = `${WORKSPACE}/commands/${pathWithoutLeadingSlash}/#`;
      console.log(`[cp] Subscribing to commands at: ${commandTopic}`);
      subscribe(commandTopic, (topic, payload) => {
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
