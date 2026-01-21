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
Yjs last-writer-wins on schema entry. One UUID wins, other becomes orphan. Rare edge case, acceptable. Orphan cleanup is a future TODO.

## File Deletion

**Local file deleted:**
1. Detect deletion via watcher
2. Create Yjs update for directory schema: `entries.delete("oldfile.txt")`
3. Create commit for schema, publish to MQTT
4. Remove from local `.commonplace-sync.json`

The file's document becomes an orphan on the server (no schema references it). This is fine - orphan cleanup is a separate concern.

**Delete vs edit conflict:** If one peer deletes while another edits, the delete wins (entry removed from schema). The edited content becomes an orphan. This matches filesystem semantics.

## File Rename

**Local file renamed:**
1. Detect rename via watcher (same inode, different name)
2. Create Yjs update: `entries.delete("old.txt")` + `entries.set("new.txt", { same node_id })`
3. The UUID stays the same - it's the same document with a new name

This preserves edit history across renames.

## Cross-Directory Moves

Moving a file between directories requires coordinated schema updates across independent sync units. The filesystem watcher cannot reliably detect this (looks like delete + create).

**Solution:** `commonplace-mv` command for cross-directory moves:
1. Remove entry from source directory schema (keeps UUID)
2. Add entry to destination directory schema (same UUID)
3. Commit both schemas

Without `commonplace-mv`, a cross-directory move looks like delete + create with a new UUID.

## Missed Messages and Recovery

The merkle tree provides consistency guarantees:

**If you miss an MQTT message:**
- The next commit you receive will have a parent you don't know
- Request the missing parent(s) via the `sync` port
- Recursively fetch until you have the full chain
- Then apply in order

**First sync (no local state):**
- Fetch HEAD commit from server
- Fetch full commit chain back to initial commit
- Build Y.Doc state by replaying updates
- Save to `.commonplace-sync.json`

## Write Invariants

**No rollback writes:** When writing remote updates to disk, only write if the new state is a descendant of our local state. Never write a commit that would roll back local changes.

**No no-op commits:** The file watcher should detect that a write didn't change content (or changed it to match what we just received) and skip creating a commit.

These invariants prevent echo loops without explicit "echo suppression" tracking.

## Shadow Hardlinks for Non-Atomic Writes

When we atomic-write a file (temp + rename), we replace the inode. If another process had the old inode open and was writing to it, those writes would be lost.

The shadow hardlink system (see `docs/commonplace-hardlink-sync-spec.md`) solves this:
- Before replacing, hardlink the old inode to a shadow directory
- Continue watching the shadow for writes
- Writes to the old inode become edits against the older commit
- CRDT merge combines them with the new content

This works because the shadow inode is just another peer - it has its own commit reference, and writes to it create Yjs updates that merge into HEAD. The CRDT model makes this "free."

## After Merge

When creating a merge commit:
1. Apply both branches to Y.Doc (Yjs merges)
2. Create merge commit with both parents
3. Update BOTH `head_cid` AND `local_head_cid` to the merge commit
4. Publish merge commit

This ensures subsequent local commits branch from the merged state, not the pre-merge state.

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

## Implementation Status

### Completed
- State model with head_cid/local_head_cid tracking
- Per-directory state files (`.commonplace-sync.json`)
- MQTT transport for publish/subscribe
- Local UUID generation for new files
- CID-based echo prevention
- Merge commit creation with two parents
- Independent directory sync
- **YMap-based schema storage** (`src/sync/ymap_schema.rs`): Directory schemas now use native YMap structures for per-entry CRDT merge semantics. Concurrent file additions from different peers merge correctly. Includes automatic migration from legacy JSON text format.

### Cleanup Pending
- Legacy HTTP/SSE code paths remain alongside CRDT MQTT paths
- `pending_write` tracking in non-CRDT paths can be removed
- RECURSIVE_SYNC_THEORY.md archived to `docs/archive/`
