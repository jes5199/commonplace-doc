# CRDT Peer Sync Design

The sync client is a CRDT peer, not a mirror. Every synced entity (file content, directory schema) is a Y.Doc. The sync client maintains its own Yjs state and merges with the server like any other peer.

## Core Model

**State per document:**
- `Y.Doc` - the current Yjs state
- `head_cid` - the commit we've "checked out" (last merged server state)
- `local_head_cid` - our latest commit (may be ahead of server)

**On local change (file edited, file created/deleted):**
1. Apply change to local Y.Doc → produces Yjs update
2. Create commit with parent = `local_head_cid`
3. Update `local_head_cid` to new commit
4. Publish commit to MQTT

**On MQTT commit received:**
1. If commit CID is known (we have it) → ignore
2. If commit is descendant of our `local_head_cid` → fast-forward, apply update
3. If commit diverged → apply update anyway (Yjs merges), create merge commit
4. Update `head_cid` to track server state

**No echo suppression needed:** If we receive our own commit, the CID check rejects it. If we receive a concurrent commit, Yjs merges it automatically.

## Directory Schemas as Y.Docs

A directory schema is a Y.Doc containing a YMap.

**Schema structure (as Yjs):**
```
YMap "root" {
  "entries": YMap {
    "foo.txt": YMap { "type": "doc", "node_id": "uuid-1" }
    "subdir": YMap { "type": "dir", "node_id": "uuid-2" }
  }
}
```

File contents and directory schemas are treated identically - both are Y.Docs the sync client is a peer for.

## Sync Client State

Each node-backed directory has its own state file: `.commonplace-sync.json`

```json
{
  "schema": {
    "node_id": "uuid-of-this-directory",
    "head_cid": "abc123",
    "local_head_cid": "abc123",
    "yjs_state": "base64..."
  },
  "files": {
    "foo.txt": {
      "node_id": "uuid-of-foo",
      "head_cid": "def456",
      "local_head_cid": "def456",
      "yjs_state": "base64..."
    }
  }
}
```

**Benefits:**
- Self-contained: move a directory, its sync state moves with it
- Parallel sync: different directories can sync independently
- Matches the "each directory is a document" model

**Single-file sync (rare case):** Gets a hidden sibling dotfile, e.g., `foo.txt` → `.foo.txt.commonplace-sync.json`

## Sync Flow (MQTT)

**Startup:**
1. Load `.commonplace-sync.json` for this directory
2. Connect to MQTT broker
3. Subscribe to `{workspace}/edits/{schema-node-id}` for directory schema
4. Subscribe to `{workspace}/edits/{file-node-id}` for each tracked file
5. Use `sync` port to fetch any commits since our `head_cid`

**File watcher detects local change:**
1. Compute Yjs update, create commit
2. Apply locally, update `local_head_cid`
3. Publish commit to `{workspace}/edits/{node-id}`

**MQTT receives commit:**
1. Check CID - if we have it, ignore
2. Otherwise apply update, merge if divergent

## New File Creation

The sync client generates UUIDs locally (UUIDv4). No server round-trip needed.

**Local file created:**
1. Detect new file via watcher
2. Generate UUIDv4 locally → `uuid-123`
3. Create Yjs update for directory schema: `entries.set("newfile.txt", { node_id: "uuid-123", type: "doc" })`
4. Create commit for schema, publish to MQTT
5. Create initial commit for file content, publish to MQTT
6. Add to local `.commonplace-sync.json`

**Server creates file (received via MQTT):**
1. Receive schema commit with new entry
2. Apply to local Y.Doc for schema
3. See new entry `newfile.txt` with `node_id`
4. Subscribe to MQTT for that node_id
5. Fetch current content, write to disk
6. Add to local state

**Concurrent creation (same filename, both sides generate different UUIDs):**
Yjs last-writer-wins on schema entry. One UUID wins, other becomes orphan. Rare edge case, acceptable.

## Nested Node-Backed Directories

Each node-backed directory is an independent sync unit with its own `.commonplace-sync.json`.

```
workspace/
  .commonplace-sync.json        # state for workspace schema + its files
  foo.txt
  subdir/                       # node-backed (has its own node_id)
    .commonplace-sync.json      # state for subdir schema + its files
    bar.txt
    nested/                     # node-backed
      .commonplace-sync.json    # state for nested schema + its files
      baz.txt
```

**Key insight:** There's no "recursive sync" logic. Each directory syncs itself. The parent only knows the child's `node_id` in its schema entry - it doesn't manage the child's contents.

**Creating a new subdirectory:**
1. Generate UUID for new directory
2. Update parent schema: `entries.set("newsubdir", { type: "dir", node_id: "uuid-456" })`
3. Create initial schema commit for new directory (empty entries)
4. Create `.commonplace-sync.json` in new directory
5. Subscribe to MQTT for new directory's node_id

No walking up/down the tree. Each directory is responsible for itself.

## What This Design Eliminates

**Echo suppression:** Gone. CID check handles duplicates.

**Reconciler dependency:** Gone. Sync client generates UUIDs directly.

**"Document before schema" rule:** Gone. Create both locally, publish both. Order doesn't matter.

**scan_directory side effects:** Gone. Scanning just reads filesystem.

**`null` node_id handling:** Gone. We never create entries without UUIDs.

**Special schema vs file logic:** Gone. Both are Y.Docs, same code path.

**RECURSIVE_SYNC_THEORY.md:** Obsolete. It documents workarounds for the wrong architecture.

## Summary

1. Sync client is a CRDT peer with its own Yjs state
2. Like git: track checked-out commit, fetch/merge on reconnect
3. Auto-merge always - that's what CRDTs are for
4. Files and schemas are both Y.Docs - no special cases
5. Per-directory state files (`.commonplace-sync.json`)
6. MQTT for transport (bidirectional pub/sub)
7. Generate UUIDs locally - no server round-trip for creation
8. Each directory syncs itself - no recursive logic
