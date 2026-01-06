# JS Evaluator Design

**Issue:** CP-egb
**Date:** 2026-01-06
**Status:** Approved

## Overview

Run TypeScript/JavaScript files as reactive transforms within commonplace. Scripts live in commonplace as documents, are launched by the orchestrator, and communicate via MQTT using a provided SDK.

## Configuration

`__processes.json`:
```json
{
  "processes": {
    "my-transform": {
      "evaluate": "transform.ts",
      "owns": "result.txt"
    }
  }
}
```

**Orchestrator behavior for `evaluate` key:**
1. Constructs script URL: `http://{server}/files/{dir}/{script}`
2. Spawns: `deno run --allow-net={server},{broker} {url}`
3. Sets env vars:
   - `COMMONPLACE_SERVER` - HTTP API base URL
   - `COMMONPLACE_BROKER` - MQTT broker host:port
   - `COMMONPLACE_OUTPUT` - path to owned output document
   - `COMMONPLACE_CLIENT_ID` - unique client ID for MQTT
4. Subscribes to script document edits, restarts process on change

## SDK API

**Import:**
```typescript
import { cp } from "http://localhost:3000/sdk/mod.ts";
```

### Reading Dependencies

```typescript
const dep = cp.doc("some/input.txt");

// Get current content (blocks until initial sync)
await dep.get();

// Subscribe to content changes (blue in)
dep.onChange((content) => { ... });

// Subscribe to events (red in)
dep.onEvent("completed", (payload) => { ... });
dep.onEvent("*", (name, payload) => { ... });  // all events

// Send command to doc owner (magenta out)
dep.command("refresh", { force: true });
```

### Writing to Owned Output

```typescript
// Pre-bound from COMMONPLACE_OUTPUT env var
await cp.output.get();
await cp.output.set("new content");
await cp.output.set("new content", { message: "commit message" });
```

### Commands and Events on Owned Doc

```typescript
// Receive commands (magenta in)
cp.onCommand("refresh", async (payload) => {
  const input = await cp.doc("source.txt").get();
  await cp.output.set(transform(input));
});

// Emit events (red out)
cp.emit("processed", { count: 42 });
```

### Lifecycle

```typescript
await cp.start();  // Connect to MQTT, start event loop
```

## Port Mapping

| Method | Direction | Port | Color | Topic Pattern |
|--------|-----------|------|-------|---------------|
| `doc.get()` | in | Sync | Cyan | `{path}/sync/{client-id}` |
| `doc.onChange()` | in | Edits | Blue | `{path}/edits` |
| `doc.onEvent()` | in | Events | Red | `{path}/events/{name}` |
| `doc.command()` | out | Commands | Magenta | `{path}/commands/{verb}` |
| `output.set()` | out | Edits | Blue | `{path}/edits` |
| `cp.onCommand()` | in | Commands | Magenta | `{path}/commands/{verb}` |
| `cp.emit()` | out | Events | Red | `{path}/events/{name}` |

## SDK Internals

- Single MQTT connection per script, multiplexed subscriptions
- Each `cp.doc(path)` creates a local Yjs doc for CRDT merging
- Lazy subscription: actual MQTT subscribe on first `.get()`/`.onChange()`/`.onEvent()`
- `output.set()` computes Yjs diff and publishes with parent commit
- Connection failures retry with backoff
- Script crashes handled by orchestrator restart policy

## Implementation Milestones

### M1: Foundation
- Add `ts`, `js` to `ALLOWED_EXTENSIONS`
- Add `/sdk/mod.ts` endpoint to commonplace server
- Skeleton SDK with MQTT + Yjs deps bundled

### M2: Basic Output
- Add `evaluate` key handling in orchestrator
- Implement `cp.output.set()` in SDK
- Test: script imports SDK from server, writes to owned doc

### M3: Reading Dependencies
- Implement `cp.doc(path).get()` with sync protocol
- Implement `cp.doc(path).onChange()`
- Test: script reads dep, transforms, writes output

### M4: Commands and Events
- `cp.onCommand()`, `cp.emit()`
- `doc.command()`, `doc.onEvent()`
- Test: command -> transform -> event round-trip

### M5: Restart on Change
- Orchestrator subscribes to script document edits
- Kill + respawn deno on change
- Test: edit script, verify restart

## Prerequisites

- Deno installed on host
- MQTT broker running
- Commonplace server with `/files/` endpoint working
