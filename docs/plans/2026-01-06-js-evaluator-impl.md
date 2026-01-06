# JS Evaluator Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable TypeScript/JavaScript files in commonplace to run as reactive transforms, communicating via MQTT.

**Architecture:** Scripts are documents in commonplace. Orchestrator sees `evaluate` key in `__processes.json`, fetches script via HTTP, spawns Deno with the URL. SDK served by server at `/sdk/mod.ts` provides the `cp` API for MQTT communication.

**Tech Stack:** Rust (server/orchestrator), Deno (runtime), TypeScript (SDK), MQTT (messaging), Yjs (CRDTs)

---

## Task 1: Add TypeScript/JavaScript to Allowed Extensions

**Files:**
- Modify: `src/sync/content_type.rs:93-95`
- Test: `src/sync/content_type.rs:306-329` (existing test)

**Step 1: Add extensions to ALLOWED_EXTENSIONS**

In `src/sync/content_type.rs`, change:
```rust
pub const ALLOWED_EXTENSIONS: &[&str] = &[
    "json", "jsonl", "ndjson", "txt", "xml", "xhtml", "bin", "md",
];
```

To:
```rust
pub const ALLOWED_EXTENSIONS: &[&str] = &[
    "json", "jsonl", "ndjson", "txt", "xml", "xhtml", "bin", "md", "ts", "js",
];
```

**Step 2: Add test cases**

Add to the existing `test_allowed_extensions` test:
```rust
// TypeScript and JavaScript
assert!(is_allowed_extension(Path::new("foo.ts")));
assert!(is_allowed_extension(Path::new("foo.js")));
assert!(is_allowed_extension(Path::new("foo.TS")));
assert!(is_allowed_extension(Path::new("foo.JS")));
```

**Step 3: Run tests**

Run: `cargo test test_allowed_extensions`
Expected: PASS

**Step 4: Commit**

```bash
git add src/sync/content_type.rs
git commit -m "feat: add ts/js to allowed sync extensions for JS evaluator"
```

---

## Task 2: Create SDK Skeleton

**Files:**
- Create: `sdk/mod.ts`
- Create: `sdk/types.ts`

**Step 1: Create types file**

Create `sdk/types.ts`:
```typescript
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
```

**Step 2: Create main module skeleton**

Create `sdk/mod.ts`:
```typescript
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
```

**Step 3: Commit**

```bash
git add sdk/
git commit -m "feat: add SDK skeleton with type definitions"
```

---

## Task 3: Add /sdk Endpoint to Server

**Files:**
- Modify: `src/lib.rs` (router setup)
- Create: `src/sdk.rs` (SDK serving handler)

**Step 1: Create SDK handler module**

Create `src/sdk.rs`:
```rust
//! SDK serving endpoints for JS evaluator.

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use std::path::Path;
use tokio::fs;

/// Serve SDK files from the sdk/ directory.
pub async fn serve_sdk(
    axum::extract::Path(file_path): axum::extract::Path<String>,
) -> Response {
    let base_path = Path::new("sdk");
    let full_path = base_path.join(&file_path);

    // Security: ensure we don't escape sdk directory
    if !full_path.starts_with(base_path) {
        return (StatusCode::FORBIDDEN, "Path traversal not allowed").into_response();
    }

    match fs::read_to_string(&full_path).await {
        Ok(content) => {
            let content_type = if file_path.ends_with(".ts") {
                "text/typescript"
            } else {
                "text/javascript"
            };
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                content,
            )
                .into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "SDK file not found").into_response(),
    }
}
```

**Step 2: Add module to lib.rs**

Add to `src/lib.rs` near other module declarations:
```rust
pub mod sdk;
```

**Step 3: Add route to router**

In `src/lib.rs`, find the router setup and add:
```rust
.route("/sdk/*file_path", get(sdk::serve_sdk))
```

**Step 4: Write test**

Add test in `src/sdk.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn test_serve_sdk_mod() {
        // This test requires sdk/mod.ts to exist
        let response = serve_sdk(axum::extract::Path("mod.ts".to_string())).await;
        // In real test, would check StatusCode::OK if file exists
    }
}
```

**Step 5: Run and verify**

Run: `cargo build`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/sdk.rs src/lib.rs
git commit -m "feat: add /sdk endpoint to serve SDK files"
```

---

## Task 4: Add `evaluate` Key to ProcessEntry

**Files:**
- Modify: `src/orchestrator/discovery.rs`

**Step 1: Add evaluate field to ProcessEntry**

In `src/orchestrator/discovery.rs`, find `pub struct ProcessEntry` and add:
```rust
/// Script to evaluate with Deno (for JS evaluator processes).
/// Path is relative to the process directory.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub evaluate: Option<String>,
```

**Step 2: Add test for evaluate parsing**

Add test:
```rust
#[test]
fn test_evaluate_process() {
    let json = r#"{
        "processes": {
            "transform": {
                "evaluate": "transform.ts",
                "owns": "output.txt"
            }
        }
    }"#;

    let config = ProcessesConfig::parse(json).unwrap();
    let transform = &config.processes["transform"];
    assert_eq!(transform.evaluate, Some("transform.ts".to_string()));
    assert_eq!(transform.owns, Some("output.txt".to_string()));
}
```

**Step 3: Run tests**

Run: `cargo test test_evaluate_process`
Expected: PASS

**Step 4: Commit**

```bash
git add src/orchestrator/discovery.rs
git commit -m "feat: add evaluate key to ProcessEntry for JS evaluator"
```

---

## Task 5: Implement Evaluate Process Spawning in Orchestrator

**Files:**
- Modify: `src/orchestrator/discovered_manager.rs`

**Step 1: Find process spawning logic**

Locate where `sandbox_exec` and `command` are handled for spawning processes.

**Step 2: Add evaluate handling**

Add handling for `evaluate` key that:
1. Constructs script URL: `http://{server}/files/{dir}/{script}`
2. Builds deno command: `deno run --allow-net={server},{broker} {url}`
3. Sets environment variables
4. Spawns the process

```rust
// In the spawn logic, add:
if let Some(ref script) = entry.evaluate {
    let server = std::env::var("COMMONPLACE_SERVER")
        .unwrap_or_else(|_| "http://localhost:3000".to_string());
    let broker = std::env::var("COMMONPLACE_BROKER")
        .unwrap_or_else(|_| "localhost:1883".to_string());

    let script_url = format!("{}/files/{}/{}", server, document_path, script);
    let client_id = format!("{}-{}", name, uuid::Uuid::new_v4().to_string()[..8].to_string());

    let mut cmd = tokio::process::Command::new("deno");
    cmd.arg("run")
        .arg(format!("--allow-net={},{}", server.replace("http://", "").replace("https://", ""), broker))
        .arg(&script_url)
        .env("COMMONPLACE_SERVER", &server)
        .env("COMMONPLACE_BROKER", &broker)
        .env("COMMONPLACE_CLIENT_ID", &client_id);

    if let Some(ref output) = entry.owns {
        let output_path = format!("{}/{}", document_path, output);
        cmd.env("COMMONPLACE_OUTPUT", output_path);
    }

    // Continue with existing spawn logic...
}
```

**Step 3: Run clippy and tests**

Run: `cargo clippy && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/orchestrator/discovered_manager.rs
git commit -m "feat: implement evaluate process spawning with Deno"
```

---

## Task 6: Implement SDK output.set() with MQTT

**Files:**
- Modify: `sdk/mod.ts`
- Create: `sdk/mqtt.ts`

**Step 1: Add MQTT client dependency**

We'll use Deno's npm compatibility. Update `sdk/mqtt.ts`:
```typescript
// Using mqtt.js via npm: specifier
import mqtt from "npm:mqtt@5";

let client: mqtt.MqttClient | null = null;

export function getClient(): mqtt.MqttClient {
  if (!client) {
    throw new Error("MQTT not connected. Call cp.start() first.");
  }
  return client;
}

export async function connect(brokerUrl: string, clientId: string): Promise<mqtt.MqttClient> {
  return new Promise((resolve, reject) => {
    client = mqtt.connect(`mqtt://${brokerUrl}`, {
      clientId,
      clean: true,
    });

    client.on("connect", () => {
      console.log(`[mqtt] Connected as ${clientId}`);
      resolve(client!);
    });

    client.on("error", (err) => {
      console.error(`[mqtt] Error:`, err);
      reject(err);
    });
  });
}

export function publish(topic: string, message: string | object): void {
  const client = getClient();
  const payload = typeof message === "string" ? message : JSON.stringify(message);
  client.publish(topic, payload);
}

export function subscribe(topic: string, handler: (topic: string, payload: Buffer) => void): void {
  const client = getClient();
  client.subscribe(topic);
  client.on("message", (t, p) => {
    if (t === topic || topic.endsWith("#") && t.startsWith(topic.slice(0, -1))) {
      handler(t, p);
    }
  });
}
```

**Step 2: Implement output.set()**

Update `sdk/mod.ts` OutputHandleImpl:
```typescript
import { connect, publish, subscribe, getClient } from "./mqtt.ts";
import * as Y from "npm:yjs@13";

// ... in OutputHandleImpl:

private doc = new Y.Doc();
private text = this.doc.getText("content");
private parentCommit: string | null = null;

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
```

**Step 3: Implement cp.start()**

```typescript
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
}
```

**Step 4: Commit**

```bash
git add sdk/
git commit -m "feat: implement SDK MQTT connection and output.set()"
```

---

## Task 7: Implement SDK doc.get() and onChange()

**Files:**
- Modify: `sdk/mod.ts`

**Step 1: Implement DocHandleImpl with sync protocol**

```typescript
class DocHandleImpl implements DocHandle {
  private doc = new Y.Doc();
  private text = this.doc.getText("content");
  private changeCallbacks: ((content: string) => void)[] = [];
  private subscribed = false;

  constructor(private path: string) {}

  private async ensureSubscribed(): Promise<void> {
    if (this.subscribed) return;
    this.subscribed = true;

    // Subscribe to edits
    subscribe(`${this.path}/edits`, (_topic, payload) => {
      try {
        const msg = JSON.parse(payload.toString());
        const update = Uint8Array.from(atob(msg.update), c => c.charCodeAt(0));
        Y.applyUpdate(this.doc, update);

        const content = this.text.toString();
        for (const cb of this.changeCallbacks) {
          cb(content);
        }
      } catch (e) {
        console.error(`[cp] Error applying edit for ${this.path}:`, e);
      }
    });

    // Fetch initial state via HTTP
    const res = await fetch(`${SERVER}/docs/${encodeURIComponent(this.path)}/head`);
    if (res.ok) {
      const head = await res.json();
      if (head.state) {
        const state = Uint8Array.from(atob(head.state), c => c.charCodeAt(0));
        Y.applyUpdate(this.doc, state);
      }
    }
  }

  async get(): Promise<string> {
    await this.ensureSubscribed();
    return this.text.toString();
  }

  onChange(cb: (content: string) => void): void {
    this.changeCallbacks.push(cb);
    this.ensureSubscribed();
  }

  onEvent(name: string, cb: (payload: unknown) => void): void {
    const topic = name === "*"
      ? `${this.path}/events/#`
      : `${this.path}/events/${name}`;

    subscribe(topic, (t, payload) => {
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
    });
  }

  async command(verb: string, payload?: unknown): Promise<void> {
    publish(`${this.path}/commands/${verb}`, {
      payload: payload || {},
      source: CLIENT_ID,
    });
  }
}
```

**Step 2: Commit**

```bash
git add sdk/
git commit -m "feat: implement SDK doc.get(), onChange(), onEvent(), command()"
```

---

## Task 8: Implement cp.emit()

**Files:**
- Modify: `sdk/mod.ts`

**Step 1: Implement emit**

```typescript
emit(name: string, payload?: unknown): void {
  if (!OUTPUT_PATH) {
    console.warn("[cp] Cannot emit: no output path configured");
    return;
  }

  publish(`${OUTPUT_PATH}/events/${name}`, {
    payload: payload || {},
    source: CLIENT_ID,
  });
}
```

**Step 2: Commit**

```bash
git add sdk/mod.ts
git commit -m "feat: implement SDK cp.emit() for red port events"
```

---

## Task 9: Script Change Detection and Restart

**Files:**
- Modify: `src/orchestrator/discovered_manager.rs`

**Step 1: Subscribe to script document edits**

When spawning an evaluate process, also subscribe to the script's edits topic.
On receiving an edit, kill and respawn the process.

```rust
// After spawning the process, if it's an evaluate process:
if entry.evaluate.is_some() {
    let script_path = format!("{}/{}", document_path, entry.evaluate.as_ref().unwrap());
    // Subscribe to script changes via MQTT
    // When change detected, call restart_process(name)
}
```

**Step 2: Commit**

```bash
git add src/orchestrator/discovered_manager.rs
git commit -m "feat: restart evaluate processes when script changes"
```

---

## Task 10: Integration Test

**Files:**
- Create: `tests/evaluate_test.rs` or manual test

**Step 1: Create test script**

Create `workspace/test-eval/transform.ts`:
```typescript
import { cp } from "http://localhost:3000/sdk/mod.ts";

const input = cp.doc("test-eval/input.txt");

input.onChange(async (content) => {
  const upper = content.toUpperCase();
  await cp.output.set(upper, { message: "Transformed to uppercase" });
  cp.emit("transformed", { length: upper.length });
});

cp.onCommand("refresh", async () => {
  const content = await input.get();
  await cp.output.set(content.toUpperCase());
});

await cp.start();
```

**Step 2: Create __processes.json**

Create `workspace/test-eval/__processes.json`:
```json
{
  "processes": {
    "transform": {
      "evaluate": "transform.ts",
      "owns": "output.txt"
    }
  }
}
```

**Step 3: Create input file**

Create `workspace/test-eval/input.txt`:
```
hello world
```

**Step 4: Test manually**

1. Start orchestrator
2. Verify transform process starts
3. Edit input.txt
4. Verify output.txt updates
5. Send command via mosquitto_pub
6. Verify response

**Step 5: Commit**

```bash
git add workspace/test-eval/
git commit -m "test: add integration test for JS evaluator"
```

---

## Summary

| Task | Description | Est. Complexity |
|------|-------------|-----------------|
| 1 | Add ts/js extensions | Simple |
| 2 | SDK skeleton | Simple |
| 3 | /sdk endpoint | Simple |
| 4 | evaluate key in ProcessEntry | Simple |
| 5 | Evaluate process spawning | Medium |
| 6 | SDK output.set() with MQTT | Medium |
| 7 | SDK doc.get() and onChange() | Medium |
| 8 | SDK cp.emit() | Simple |
| 9 | Script change restart | Medium |
| 10 | Integration test | Medium |
