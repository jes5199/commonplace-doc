# Sync Tool State Persistence (CP-dgu)

## Problem

The sync tool (`commonplace-sync`) currently stores all state in memory. On restart:
- It fetches HEAD from server
- Overwrites local files with server content
- Any offline local changes are lost

## Solution

Persist sync state to a local file so the tool can:
1. Detect local changes made while offline
2. Merge them with server changes using CRDT

## Design

### State File Location

Store state file **beside** the synced target (not inside) to avoid syncing it:

| Target | State File |
|--------|------------|
| `notes/` | `.notes.commonplace-sync.json` |
| `readme.txt` | `.readme.txt.commonplace-sync.json` |

Pattern: `.{basename}.commonplace-sync.json` in the parent directory.

### State File Contents

```json
{
  "server": "http://localhost:3000",
  "node_id": "abc123",
  "last_synced_cid": "QmXyz...",
  "last_synced_at": "2025-01-02T10:30:00Z",
  "files": {
    "notes/todo.txt": {
      "hash": "sha256:abc123...",
      "last_modified": "2025-01-02T10:30:00Z"
    }
  }
}
```

Fields:
- `server`: The commonplace server URL
- `node_id`: The document/node ID being synced
- `last_synced_cid`: Commit ID of last successful sync
- `last_synced_at`: Timestamp of last sync
- `files`: Map of relative paths to content hashes

### Restart Flow

```
On startup:
1. Load state file (if exists)
2. For each local file:
   a. Compute current hash
   b. Compare to saved hash
   c. If different → local change detected
3. If local changes:
   a. Fetch Yjs state at `last_synced_cid` from server
   b. Compute diff from that state → current local content
   c. Create Yjs update from diff
   d. Push update to server
   e. CRDT automatically merges with any server changes
4. Fetch current HEAD from server
5. Apply to local files
6. Update state file
```

### Conflict Resolution

**None needed!** This is the beauty of CRDT:
- Local changes become a Yjs update based on `last_synced_cid`
- Server may have additional commits since then
- Yjs merges both automatically
- Result is deterministic across all clients

### API Prerequisite

**Blocking:** The server needs an endpoint to fetch document state at a specific commit.

**Current state:** `GET /docs/:id/head` only returns current HEAD.

**Required change:** Add `?at_commit=<cid>` query parameter:
- `GET /docs/:id/head` → current HEAD (unchanged)
- `GET /docs/:id/head?at_commit=QmXyz` → state at specific commit

The `CommitReplayer` infrastructure already exists (used by fork endpoint).

## Implementation Steps

1. **Add `at_commit` query param to `/head` endpoint**
   - Modify `get_doc_head` in `src/api.rs`
   - Use `CommitReplayer::get_content_and_state_at_commit`
   - Add tests

2. **Add state file persistence to sync tool**
   - Create `SyncStateFile` struct in `src/sync/`
   - Load on startup, save after each successful sync
   - Store beside target path

3. **Implement local change detection**
   - Compute file hashes using SHA-256
   - Compare to saved hashes
   - Build list of changed files

4. **Implement offline merge flow**
   - Fetch historical Yjs state via `?at_commit`
   - Compute diff from historical → current local
   - Create Yjs update
   - Push to server

5. **Update sync loop**
   - Integrate state file save after each sync
   - Update file hashes after writing
