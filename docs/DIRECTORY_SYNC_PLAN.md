# Directory Sync Implementation Plan

This document outlines the plan to extend `commonplace-sync` to mirror entire directories, not just single files.

## Current State

- `commonplace-sync` syncs one local file ↔ one server node
- Server has filesystem-in-JSON reconciler that creates nodes from a JSON manifest
- Bidirectional sync works: local edits push to server, server edits update local file

## Goal

```bash
# Mirror a local directory to the server
commonplace-sync --directory ./my-notes --node fs-root

# Changes flow both directions:
# - Edit local file → server node updates
# - Edit server node → local file updates
# - Create local file → fs JSON updated → server creates node
# - Delete entry from fs JSON → local file untouched (non-destructive)
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    commonplace-sync                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────┐  │
│  │   Directory  │     │   FS JSON    │     │  File Sync  │  │
│  │   Scanner    │────▶│   Manager    │────▶│    Pool     │  │
│  └──────────────┘     └──────────────┘     └─────────────┘  │
│         │                    │                    │          │
│         ▼                    ▼                    ▼          │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────┐  │
│  │   Directory  │     │   SSE for    │     │ Per-file    │  │
│  │   Watcher    │     │   fs-root    │     │ sync tasks  │  │
│  └──────────────┘     └──────────────┘     └─────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Commonplace    │
                    │     Server      │
                    └─────────────────┘
```

## Implementation Phases

### Phase 1: Directory Scanner & FS JSON Generator

**Goal:** Scan a local directory and generate filesystem JSON.

**Files:** `src/bin/sync.rs` (or new module `src/bin/sync/directory.rs`)

```rust
/// Scan a directory and build filesystem JSON
fn scan_directory(path: &Path) -> FsSchema {
    // Recursively walk directory
    // Build entries map
    // Return FsSchema { version: 1, root: Entry::Dir(...) }
}

/// Convert FsSchema to JSON string
fn schema_to_json(schema: &FsSchema) -> String;
```

**Behavior:**
- Walk directory tree recursively
- Skip hidden files (configurable?)
- Map file extensions to content_type (`.json` → `application/json`, etc.)
- Generate derived node IDs: `<fs-root>:<relative-path>`

**Output example:**
```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "notes": {
        "type": "dir",
        "entries": {
          "ideas.txt": { "type": "doc", "content_type": "text/plain" }
        }
      },
      "config.json": { "type": "doc", "content_type": "application/json" }
    }
  }
}
```

### Phase 2: Initial Sync (Local → Server)

**Goal:** On startup, push local directory state to server.

**Steps:**
1. Scan local directory → generate FS JSON
2. Fetch current fs-root content from server
3. If empty/different, push new FS JSON to fs-root node
4. Wait for server reconciler to create nodes
5. For each file, push content to corresponding node

**Key decisions:**
- **Conflict resolution:** If server has content, who wins?
  - Option A: Local wins (overwrite server)
  - Option B: Server wins (overwrite local)
  - Option C: Merge (complex, maybe later)
  - **Recommendation:** Flag `--initial-sync=local|server|error`

### Phase 3: Directory Watcher

**Goal:** Detect local file create/delete/rename and update FS JSON.

**Changes to file watcher:**
```rust
enum FileEvent {
    Modified(PathBuf),      // existing
    Created(PathBuf),       // new
    Deleted(PathBuf),       // new
    Renamed(PathBuf, PathBuf), // new (from, to)
}
```

**On file created:**
1. Update in-memory FS schema
2. Push updated JSON to fs-root node
3. Server reconciler creates the new node
4. Start sync task for the new file

**On file deleted:**
1. Update in-memory FS schema (remove entry)
2. Push updated JSON to fs-root node
3. Stop sync task for that file
4. (Node persists on server - non-destructive)

**On file renamed:**
1. If using derived IDs: treat as delete + create
2. If using explicit node_id: just update path in schema

### Phase 4: File Sync Pool

**Goal:** Manage multiple concurrent file sync tasks.

**Current:** One file, one upload task, one SSE task.

**New:**
```rust
struct FileSyncPool {
    /// Map of relative path -> sync task handle
    tasks: HashMap<PathBuf, FileSyncHandle>,
}

struct FileSyncHandle {
    upload_handle: JoinHandle<()>,
    // SSE is shared - we subscribe to fs-root and route events
}
```

**SSE strategy:**
- Subscribe to fs-root node (get all edit events)
- Route events to appropriate file sync task based on source node ID
- Or: Subscribe to each file node individually (more connections, simpler routing)

**Recommendation:** Start with per-file SSE subscriptions (simpler), optimize later if needed.

### Phase 5: Server → Local Sync

**Goal:** Handle server-side changes to FS JSON.

**When fs-root JSON changes on server:**
1. Receive SSE event for fs-root edit
2. Fetch new FS JSON
3. Diff against local FS schema
4. For new entries: create local file (empty or fetch content)
5. For removed entries: keep local file (non-destructive) or delete (configurable)
6. Update sync task pool

**Edge cases:**
- File exists locally but not in server FS JSON → ?
- Server creates node with content, local file doesn't exist → create it

## CLI Changes

```bash
# Current (single file)
commonplace-sync --server http://localhost:3000 --node my-doc --file ./note.txt

# New (directory mode)
commonplace-sync --server http://localhost:3000 --node fs-root --directory ./my-notes

# Options
--initial-sync=local|server|skip   # Who wins on first sync (default: skip if conflict)
--delete-local                      # Delete local files removed from server FS JSON
--ignore=<pattern>                  # Glob patterns to ignore (e.g., "*.tmp", ".git")
--content-type-map=<file>           # Custom extension → MIME type mapping
```

## File Structure Changes

```
src/
├── bin/
│   └── sync.rs              # Entry point, CLI parsing
├── sync/                     # New module
│   ├── mod.rs
│   ├── single_file.rs       # Existing single-file logic (extracted)
│   ├── directory.rs         # Directory scanner, FS JSON generator
│   ├── pool.rs              # File sync task pool
│   └── watcher.rs           # Enhanced directory watcher
```

Or keep it simpler initially - just extend `sync.rs` with directory mode.

## Testing Strategy

### Unit Tests
- Directory scanner generates correct FS JSON
- FS JSON diff detects additions/removals
- Content type inference from extensions

### Integration Tests
1. **Initial sync (empty server):** Local dir → server
2. **Initial sync (empty local):** Server FS JSON → local dir
3. **Bidirectional file edit:** Edit file locally, verify server; edit on server, verify local
4. **File creation:** Create local file, verify server node created
5. **File deletion:** Delete local file, verify FS JSON updated
6. **Concurrent edits:** Edit same file locally and on server

### Manual Testing
```bash
# Terminal 1: Start server with fs-root
cargo run --bin commonplace-server -- --fs-root my-fs

# Terminal 2: Start directory sync
cargo run --bin commonplace-sync -- --directory ./test-notes --node my-fs

# Terminal 3: Make changes and observe
echo "hello" > ./test-notes/new-file.txt
# Watch sync tool log, verify server has new node
```

## Design Decisions

1. **Symlinks:** Ignore (skip during scan, don't follow)

2. **Binary files:** Base64 encode content, store in text nodes
   - Detect binary via file extension or content sniffing
   - Mark in schema with `content_type: application/octet-stream` or specific MIME

3. **Conflict resolution:**
   - Use Yjs CRDT resolution for content conflicts (automatic merge)
   - Structural conflicts (file vs dir, rename collisions) → document edge cases
   - Optimize for **not losing data** - prefer duplication over deletion

4. **Nested node-backed directories:** Start with inline-only, add later

5. **Large directories:** Test with 100+ files, add batching if needed

## Edge Cases & Conflict Handling

### Content Conflicts (Same File Edited Both Sides)
- Yjs handles this automatically via CRDT merge
- Both edits are preserved, interleaved by position
- No data loss

### Structural Conflicts

| Scenario | Resolution | Data Loss? |
|----------|------------|------------|
| Local creates `foo.txt`, server creates `foo.txt` | Yjs merges content | No |
| Local creates `foo/`, server creates `foo.txt` | Error, keep both somehow? | No |
| Local renames `a.txt`→`b.txt`, server edits `a.txt` | Tricky - see below | No |
| Local deletes file, server edits file | Server content preserved in node | No |

### Rename + Edit Conflict
If using derived node IDs (`fs-root:path`):
- Local rename `a.txt` → `b.txt` creates new node ID
- Server edit to `a.txt` goes to old node ID
- Result: Two nodes exist, no data lost
- User sees both files after sync

If using explicit `node_id`:
- Rename just changes path in FS JSON
- Server edit goes to same node
- No conflict, content merges via Yjs

**Recommendation:** For important files, use explicit `node_id` in FS JSON.

### File ↔ Directory Type Conflict
- Local has `notes/` directory
- Server FS JSON has `notes` as a doc
- Resolution: Treat as error, don't sync that entry, log warning
- Both sides keep their version until manually resolved

## Implementation Order

1. **Phase 1:** Directory scanner (can test standalone)
2. **Phase 2:** Initial sync local → server (useful immediately)
3. **Phase 4:** File sync pool (needed for multi-file)
4. **Phase 3:** Directory watcher enhancements
5. **Phase 5:** Server → local sync

Estimated effort: Medium (2-3 focused sessions)

## Success Criteria

- [ ] Can mirror a local directory to server on startup
- [ ] Local file edits sync to server
- [ ] Server edits sync to local files
- [ ] Creating a local file creates server node
- [ ] Deleting a local file updates FS JSON (node persists)
- [ ] Works with nested directories
- [ ] Graceful handling of conflicts
