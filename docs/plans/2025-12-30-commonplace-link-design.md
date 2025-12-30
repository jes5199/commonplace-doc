# CP-zif: Commonplace Link Tool

## Problem

Users need a way to have the same document content appear at multiple paths within a synced directory (aliasing). Currently, each file path maps to a separate document.

## Solution

A CLI tool `commonplace-link` that creates links in the local schema, similar to Unix `ln`. Two files with the same `node_id` in the schema share the same underlying document.

## Command Interface

```
commonplace-link <source> <target>
```

**Arguments:**
- `source` - Existing file path (must exist in schema)
- `target` - New link path (must not exist)

**Environment:**
- Works inside a synced directory (uses `.commonplace.json` in same directory)
- Follows same conventions as `commonplace-cmd`

**Exit codes:**
- 0: Success
- 1: Error

## Implementation Logic

1. Find `.commonplace.json` in the directory containing the source file
2. Parse the schema
3. Validate source path exists in schema entries
4. Validate target path does NOT exist (error if it does)
5. If source entry has no `node_id`, generate UUID and assign it
6. Add target entry with same `node_id` as source
7. Write updated schema atomically (temp file + rename)

The file is NOT created on disk - the sync tool will materialize it on next pull. This avoids race conditions with a running sync watcher.

## Schema Example

**Before:**
```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "config.json": {"type": "doc"}
    }
  }
}
```

**After** `commonplace-link config.json settings.json`:
```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "config.json": {"type": "doc", "node_id": "a1b2c3d4-..."},
      "settings.json": {"type": "doc", "node_id": "a1b2c3d4-..."}
    }
  }
}
```

## Error Cases

- "Not in a commonplace sync directory" - no `.commonplace.json` found
- "Source file not found: {path}" - source not in schema
- "Target already exists: {path}" - target already in schema
- "Cannot link across directories" - source and target in different directories

## Edge Cases

- **Source already has node_id**: Just copy it to target
- **Source is already a link**: Works fine - copies the shared node_id
- **Linking directories**: Not supported - only doc entries

## Testing

1. Basic link creation (source gets UUID, target shares it)
2. Source already has node_id (preserves it)
3. Target already exists (error)
4. Source doesn't exist (error)
5. No .commonplace.json (error)
6. Cross-directory attempt (error)

## Files to Create/Modify

- `src/bin/link.rs` - New binary
- `src/cli.rs` - Add LinkArgs struct
- `Cargo.toml` - Add binary entry
