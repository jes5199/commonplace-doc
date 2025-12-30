# CP-7m0: Flat Directory JSON with Subdirectory Documents

## Problem

Directory JSON schemas can contain deeply nested inline subdirectories. This prevents partial checkouts of large directory trees - you must fetch the entire schema even if you only need one subdirectory.

## Solution

Enforce flat directory schemas where each directory only contains its direct children. Subdirectories become separate documents referenced by `node_id`.

## Schema Changes

**Before (inline nesting):**
```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "hello.txt": {"type": "doc", "node_id": "abc123"},
      "subdir": {
        "type": "dir",
        "entries": {
          "nested.txt": {"type": "doc"}
        }
      }
    }
  }
}
```

**After (flat with references):**
```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "hello.txt": {"type": "doc", "node_id": "abc123"},
      "subdir": {"type": "dir", "node_id": "def456"}
    }
  }
}
```

Subdirectory document (`def456`):
```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "nested.txt": {"type": "doc", "node_id": "ghi789"}
    }
  }
}
```

## Migration Strategy

Automatic migration during reconciliation:

1. Parse directory schema
2. Find inline subdirectories (dirs with `entries` instead of `node_id`)
3. For each inline subdirectory:
   - Generate new UUID
   - Create new document with subdirectory's entries
   - Replace inline entries with `node_id` reference in parent
4. Persist updated parent schema

Migration is transparent and one-time per subdirectory.

## Implementation

### Files to Modify

1. **`src/fs/reconciler.rs`**
   - Add `migrate_inline_subdirectories()` method
   - Extract nested dirs into separate documents
   - Update parent schema with `node_id` references
   - Requires write access via `DocumentService`

2. **`src/lib.rs`**
   - Pass `DocumentService` to reconciler for write access

### Reconciler Flow

```
reconcile(content)
  → parse schema
  → migrate_inline_subdirectories()  // NEW
  → collect_entries()
  → create missing documents
```

### UUID Generation

Use `uuid::Uuid::new_v4()` for new subdirectory document IDs.

## Edge Cases

- **Deeply nested**: Migrate recursively, each level becomes its own document
- **Already migrated**: Skip dirs that already have `node_id`
- **Empty subdirectories**: Create document with empty entries `{}`
- **Mixed schemas**: Handle partial migration gracefully

## Error Handling

- **Document creation fails**: Log warning, skip that subdirectory, continue
- **Schema write fails**: Keep inline form (still works), retry next reconciliation
- **Concurrent edits**: Use `replace_content` with parent_cid for optimistic locking

## Backward Compatibility

- Reconciler reads both inline and node-backed forms indefinitely
- Migration only happens on write
- No breaking changes to existing sync clients
