# CP-k5bq: Directory Sync UUID Subscriptions

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix directory sync to receive file content edits by dynamically subscribing to all file UUIDs in the schema.

**Architecture:** MQTT-only approach. Sync client subscribes to fs-root (for schema) plus all file UUIDs. When schema changes, diff subscriptions. Remove SSE fallback entirely.

**Tech Stack:** Rust, MQTT (rumqttc), tokio

---

## Background

Directory sync currently only subscribes to the fs-root document's edits topic. This means:
- Schema changes are received (files added/removed)
- File content edits are NOT received

When files share UUIDs via `commonplace-link`, edits to one file don't propagate to the other because we're not subscribed to the file UUID's edit topic.

## Design

### Single Shared Workspace

All sync clients use the same MQTT workspace namespace (the top-level workspace identifier, not individual fs-root IDs). This allows cross-directory UUID linking to work.

### Dynamic UUID Subscriptions

On startup:
1. Subscribe to fs-root edit topic (for schema changes)
2. Parse schema to get all file UUIDs
3. Subscribe to each file UUID's edit topic
4. Build reverse map: uuid → [relative_paths]

On schema change:
1. Parse new schema to get new set of UUIDs
2. Diff against currently subscribed UUIDs
3. Subscribe to new UUIDs
4. Unsubscribe from removed UUIDs
5. Update reverse map

On file edit received:
1. Look up UUID in reverse map to find local path(s)
2. Apply Yjs update to get new content
3. Write content to local file(s)

### Data Structures

```rust
// Currently subscribed file UUIDs (not including fs-root)
subscribed_uuids: HashSet<String>

// Reverse map for writing file edits to local paths
// Multiple paths possible due to UUID linking
uuid_to_paths: HashMap<String, Vec<String>>
```

### Remove SSE Fallback (Directory Mode Only)

SSE requires one connection per subscription, making dynamic UUID subscriptions impractical (100 files = 100 connections). MQTT handles unlimited subscriptions over one connection.

Remove from directory/exec modes:
- `directory_sse_task()`
- `subdir_sse_task()`
- SSE-related code paths in `run_directory_mode()` and `run_exec_mode()`

MQTT becomes required for directory sync mode.

**Note:** SSE remains supported for single-file sync mode (one file = one connection = reasonable).

## Files to Modify

- `src/sync/subscriptions.rs` - Add UUID subscription management, remove SSE tasks
- `src/sync/dir_sync.rs` - Build/update reverse map, handle file edits
- `src/bin/sync.rs` - Require MQTT, remove SSE fallback paths
- `src/sync/types.rs` - Add new data structures if needed

## Testing

- Unit test: UUID subscription diffing
- Unit test: Reverse map building from schema
- Integration test: File edit propagation via MQTT
- Integration test: UUID linking across directories

## Follow-up Work

File separate bead: Audit and clean up any remaining SSE code paths after this change.
