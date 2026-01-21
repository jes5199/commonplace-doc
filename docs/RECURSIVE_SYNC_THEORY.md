# Theory of Recursive Syncing

> **⚠️ DEPRECATED**: This document describes the legacy "mirror with echo suppression"
> architecture. The codebase is migrating to a new "CRDT peer" architecture where:
> - The sync client maintains its own Y.Doc state and merges like any other peer
> - Echo prevention is via CID checks, not content/path tracking
> - UUIDs are generated locally (no server round-trip)
> - Each directory syncs independently (no recursive management)
>
> See `docs/plans/2026-01-21-crdt-peer-sync-design.md` for the new design.
> New code should use the modules in `src/sync/crdt_*.rs` instead of the legacy
> echo suppression mechanisms described below.

This document establishes the rules and invariants for bidirectional sync between a local filesystem and the commonplace server, with support for arbitrarily nested node-backed directories.

> **Note:** This document describes the *intended* design. The current implementation
> has legacy code paths that violate some of these rules. See "Implementation Status"
> at the end for known gaps.

## Core Concepts

### Node-Backed Directories

A **node-backed directory** is a local directory that has its own document on the server, identified by a UUID (`node_id`). The directory's schema (`.commonplace.json`) describes its contents.

```
workspace/                     # node_id: "fs-root" (the root)
  .commonplace.json            # schema for workspace/
  foo.txt                      # file owned by fs-root
  subdir/                      # node_id: "abc-123" (node-backed)
    .commonplace.json          # schema for subdir/
    bar.txt                    # file owned by abc-123
    nested/                    # node_id: "def-456" (node-backed)
      .commonplace.json        # schema for nested/
      baz.txt                  # file owned by def-456
```

### Ownership

Every file belongs to exactly one node-backed directory - the nearest ancestor with a `node_id`. The owning directory's schema contains an entry for that file with the file's `node_id`.

### Schemas

A schema (`.commonplace.json`) describes directory contents:

```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "foo.txt": { "type": "doc", "node_id": "uuid-of-foo" },
      "subdir": { "type": "dir", "node_id": "uuid-of-subdir" }
    }
  }
}
```

## The Golden Rule

> **A schema pushed to the server must have valid UUIDs for all entries.**
>
> The sync client creates documents BEFORE referencing them in schemas.
> Schemas with `node_id: null` are never pushed to the server.

## Invariants

1. **Document-Before-Schema**: A document must exist on the server before any schema references it by UUID.

2. **No Null Node IDs**: Schemas pushed to the server never contain `node_id: null`. If scanning produces null entries, we must create the documents and fill in UUIDs before pushing.

3. **Single Source of Truth**: The server is the source of truth for UUIDs. When we create a document, the server assigns the UUID.

4. **Echo Suppression**: The sync client must ignore file watcher events caused by its own writes. We track paths we've written to and skip pushing when we see events for those paths.

5. **Atomic Schema Updates**: When updating a schema, we must write locally AND push to server. Never write locally without pushing (creates drift) or push without writing (causes echo issues).

## Canonical Operations

### Creating a New File in an Existing Directory

```
Input: User creates /workspace/subdir/newfile.txt
Owner: subdir (node_id: "abc-123")

Steps:
1. Detect file creation via watcher
2. Find owning directory (subdir) and its node_id
3. POST /docs { content_type: "text/plain" } → server returns UUID "xyz-789"
4. Read current schema for subdir
5. Add entry: { "newfile.txt": { "type": "doc", "node_id": "xyz-789" } }
6. Write schema to /workspace/subdir/.commonplace.json
7. Record path in written_schemas (for echo suppression)
8. Push schema to server: POST /docs/abc-123/replace
9. Push file content: POST /docs/xyz-789/edit
```

### Creating a New Subdirectory with Files

```
Input: User runs `mkdir -p /workspace/newdir && echo "hi" > /workspace/newdir/test.txt`
Owner of newdir: workspace (fs-root)
Owner of test.txt: newdir (once created)

Steps:
1. Detect directory creation via watcher
2. Create document for newdir: POST /docs { content_type: "application/json" } → "dir-uuid"
3. Update workspace schema to include newdir with dir-uuid
4. Write workspace schema, record in written_schemas
5. Push workspace schema to server
6. Detect file creation in newdir
7. Create document for test.txt: POST /docs { content_type: "text/plain" } → "file-uuid"
8. Create schema for newdir with test.txt entry
9. Write newdir schema to /workspace/newdir/.commonplace.json
10. Record in written_schemas
11. Push newdir schema to server
12. Push file content to server
```

### Handling Nested Node-Backed Directories

When traversing for sync operations:

```
function findOwningDocument(path):
    current = path.parent
    while current != root:
        schema = readSchema(current)
        if schema has node_id for current:
            return { directory: current, node_id: schema.node_id }
        current = current.parent
    return { directory: root, node_id: fs-root-id }
```

The key insight: we walk UP the tree to find the nearest node-backed ancestor, not down.

## File Watcher Event Handling

### Events We Handle

| Event | Action |
|-------|--------|
| File created | Create doc, update owner's schema, push both |
| File modified | Push content to existing doc |
| File deleted | Remove from owner's schema, push schema |
| Directory created | Create doc for dir, update parent's schema |
| Schema modified (by user) | Push to server (unless echo) |

### Echo Detection

When we write a schema file, we record its path in `written_schemas`. When a SchemaModified event fires:

```rust
if written_schemas.contains(event.path):
    // We caused this event - skip pushing
    return
else:
    // User edited the schema - push to server
    push_schema(event.content)
```

We use path-based detection, not content-based, because:
- Intermediate writes (like scan_directory_to_json) may have different content
- We want to suppress ALL events for paths we've touched recently

## The Reconciler's Role

The reconciler runs server-side and:
- Creates documents for schema entries with `node_id: null`
- Updates schemas with assigned UUIDs

**However**, the sync client should NOT rely on the reconciler for real-time operations:
- The reconciler runs asynchronously
- Waiting for reconciler adds latency and race conditions
- The sync client should create documents directly

The reconciler is a **fallback** for schemas created by other means (manual edits, other tools), not the primary document creation path.

## What Can Go Wrong

### Race Condition: Push Before Document Exists

```
BAD:
1. scan_directory_to_json writes schema with node_id: null
2. Watcher fires SchemaModified
3. We push schema with null to server
4. Reconciler creates document, updates schema
5. Our null overwrites reconciler's UUID

GOOD:
1. Create document first → get UUID
2. Update schema with UUID
3. Write and push schema (never null)
```

### Race Condition: Echo Causes Revert

```
BAD:
1. We update schema with correct UUID
2. Write to disk
3. scan_directory_to_json runs, writes schema with null
4. Watcher fires for scan's write
5. We push the null version, reverting our change

GOOD:
1. Echo detection catches event for path we wrote
2. Skip pushing
3. Our correct version remains on server
```

### Missing Parent Directory Document

```
BAD:
1. Create file in new subdirectory
2. Try to update subdir's schema
3. Subdir has no document yet - where do we push?

GOOD:
1. Detect new subdirectory
2. Create document for subdirectory first
3. Update parent schema with subdir's UUID
4. Then handle file within subdirectory
```

## Testing Checklist

Any recursive sync implementation should pass these tests:

1. New file in root directory updates root schema on server
2. New file in existing subdirectory updates subdir schema on server
3. New subdirectory gets its own document
4. New file in new subdirectory works (two-level creation)
5. Deeply nested creation works (3+ levels)
6. File deletion removes entry from owner's schema
7. Directory deletion removes entry from parent's schema
8. User-edited schema pushes to server
9. Our schema writes don't cause echo push
10. Concurrent file operations don't corrupt schemas

## Additional Modes and Edge Cases

### Path-Based Identifier Mode (`use_paths`)

When `use_paths` is enabled, the sync client uses path-based identifiers (`fs-root:path/to/file.txt`) for API calls instead of UUIDs. This mode:

- Uses path-based identifiers for API calls (edit, replace, head endpoints)
- Does not create new documents for new files
- Existing `node_id` values in schemas are still preserved by `scan_directory`
- Some flows (like forking) may still expect UUIDs when present

This is a simpler mode for single-client scenarios where UUID coordination isn't needed, but loses the ability to track file identity across renames.

### Symlinks and commonplace-link

Symlinks in the workspace are converted to `commonplace-link` entries:

```json
{
  "linked-file.txt": {
    "type": "doc",
    "node_id": "uuid-of-target",
    "link": true
  }
}
```

Multiple files can share the same `node_id`, making them aliases that stay in sync. The sync client:
- Detects symlinks during scanning
- Resolves the target's `node_id`
- Creates link entries (not actual symlinks) in the schema
- Skips symlinks to non-synced targets

See `docs/SANDBOX_LINKING.md` for details.

### File Forking

When a new file has content identical to an existing file, the sync client can fork it:

```
1. Detect content match via hash
2. Call POST /docs/{source}/fork → get new UUID
3. Use forked UUID in schema
4. Content is shared via CRDT ancestry
```

This optimizes for copy operations and ensures CRDT history is preserved.

## Summary

1. **Create documents before referencing them** - never push `node_id: null`
2. **The sync client owns document creation** - don't wait for reconciler
3. **Echo detection by path** - suppress events for paths we've written
4. **Walk up to find owner** - nearest node-backed ancestor owns the file
5. **Atomic local+remote updates** - always write and push together

---

## Implementation Status

The current codebase has legacy patterns that violate some rules above:

### Sanctioned Exceptions

**`use_paths` mode**: When enabled, the sync client uses path-based identifiers instead of UUIDs. This mode intentionally bypasses the "No null node_ids" and "Document-Before-Schema" rules because it doesn't use UUIDs at all. This is a simpler mode for single-client scenarios where file identity tracking across renames isn't needed.

### Known Violations

| Rule | Violation | Location |
|------|-----------|----------|
| No null node_ids | `scan_directory` leaves `node_id: None` for new entries | `src/sync/directory.rs:408-459` |
| Don't rely on reconciler | `sync_single_file` falls back to pushing schema and waiting for reconciler when `uuid_map` lacks an entry | `src/sync/file_sync.rs:1489-1535` |
| Atomic schema updates | Scanning writes nested `.commonplace.json` without pushing | `src/sync/directory.rs:417-425` |
| Echo tracking | `scan_dir_recursive` writes schemas via `fs::write`, bypassing `written_schemas` | `src/sync/directory.rs:417-425` |

**Note on echo suppression**: The codebase has two distinct mechanisms:

1. **Write avoidance** (`schema_io.rs:write_schema_file`): Compares content (JSON equality) before writing to disk. This prevents unnecessary disk I/O but is NOT echo detection.

2. **Push suppression** (`dir_sync.rs:handle_schema_modified`): Checks if path exists in `written_schemas` before pushing to server. This IS echo detection.

**Gap**: `scan_dir_recursive` in `directory.rs` writes nested `.commonplace.json` files directly via `std::fs::write`, bypassing both mechanisms. These writes are not tracked in `written_schemas`, so subsequent watcher events may trigger redundant pushes. This should be fixed by routing all schema writes through `write_schema_file`.

### Recent Fixes (aligned with theory)

- `file_events.rs`: `ensure_parent_directories_exist` now creates documents before updating schemas
- `dir_sync.rs`: `handle_schema_modified` uses path-based echo detection
- `schema_io.rs`: `write_schema_file` records paths in `written_schemas`

### Refactoring Roadmap

To fully align with this theory:

1. **Remove reconciler dependency from initial sync** - Create documents directly during `sync_single_file` instead of pushing schema and waiting
2. **Make scan_directory non-mutating** - Don't write nested `.commonplace.json` files as side effect; separate scanning from schema I/O
3. **Unify echo detection** - Use path-based detection everywhere, remove content comparison from watcher
4. **Add use_paths documentation** - Clearly document this as a separate mode with different guarantees
