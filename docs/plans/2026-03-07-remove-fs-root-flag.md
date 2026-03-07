# Remove --fs-root flag, auto-manage fs-root UUID in redb

**Ticket:** CP-h267
**Blocks:** CP-totc (workspace migration to repo/main layout)

## Problem

The server's `--fs-root` flag accepts arbitrary strings as document IDs (e.g., `"workspace"`). The document store is a `HashMap<String, Document>` that accepts any key, but downstream code (sync, CRDT, subdir tasks) expects UUIDs and has nil UUID fallbacks when the fs-root ID isn't a valid UUID. This is a latent bug source.

## Design

Remove `--fs-root` entirely. The server auto-creates the fs-root document with a proper UUID on first startup and persists the UUID in a redb metadata table.

### Changes

#### 1. Add METADATA_TABLE to CommitStore (`src/store.rs`)

```rust
const METADATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("metadata");
```

New methods:
- `get_metadata(key) -> Option<String>` — read from metadata table
- `set_metadata(key, value)` — write to metadata table
- `get_or_create_fs_root() -> String` — main entrypoint:
  1. Check `get_metadata("fs_root_uuid")` — if found, return it
  2. Check if `DOC_HEADS_TABLE["workspace"]` exists (legacy migration) — if so, generate UUID, copy the head entry, store UUID in metadata, return it
  3. Otherwise generate fresh UUID, store in metadata, return it

#### 2. Remove --fs-root from CLI (`src/cli.rs`)

Remove `fs_root: Option<String>` from `Args` struct.
Remove `fs_root: String` from `StoreArgs` struct.

#### 3. Update RouterConfig (`src/lib.rs`)

Keep `fs_root: Option<String>` in `RouterConfig` as a test-only override.
In `create_router_with_config`, derive fs-root from CommitStore when not overridden:

```rust
let fs_root_id = config.fs_root.or_else(|| {
    config.commit_store.as_ref().map(|store| {
        store.get_or_create_fs_root().expect("Failed to init fs-root")
    })
});
```

#### 4. Update server binary (`src/bin/server.rs`)

Remove `args.fs_root` from RouterConfig construction. The server no longer needs to know about fs-root — `create_router_with_config` handles it.

#### 5. Update store binary (`src/bin/store.rs`)

Replace `args.fs_root` usage with `commit_store.get_or_create_fs_root()`.

#### 6. Remove nil UUID fallbacks (`src/bin/sync.rs`)

Lines 3154 and 3871: replace `unwrap_or_else(|| Uuid::nil())` with `.expect("fs_root_id must be a valid UUID")`.

#### 7. Update config (`commonplace.json`)

Remove `"--fs-root", "workspace"` from server args.

#### 8. Update tests

Tests using `fs_root: Some("test-fs-root")` in RouterConfig continue to work (test override path). Tests referencing hardcoded fs-root string IDs in URI paths need updating to discover the auto-generated UUID via `GET /fs-root`.

#### 9. Update error messages

- `src/bin/orchestrator.rs:542` — change "Server was not started with --fs-root"
- `src/sync/transport/client.rs` — update doc comments

### Migration Path

On first startup with new binary:
1. If redb has `metadata["fs_root_uuid"]` → use it (already migrated)
2. If redb has `doc_heads["workspace"]` → legacy database, migrate:
   - Generate new UUID
   - Copy `doc_heads["workspace"]` to `doc_heads[new_uuid]`
   - Store `metadata["fs_root_uuid"] = new_uuid`
   - Delete `doc_heads["workspace"]`
3. If neither → fresh database, generate UUID and store

The in-memory DocumentStore also needs migration: the document keyed as `"workspace"` needs to be re-keyed to the new UUID. This happens at startup in `create_router_with_config` via `get_or_create_with_id(fs_root_uuid)`.

### What Doesn't Change

- `GET /fs-root` HTTP endpoint — still works, returns the UUID
- MQTT `_system/fs-root` retained message — still works
- CLI tools (branch, init, checkout) — discover fs-root via HTTP/MQTT, unchanged
- Sync client — discovers fs-root via MQTT, unchanged

### Risks

- Existing deployments need `--fs-root workspace` removed from config or the binary will error (unknown flag)
- The `--database` flag becomes effectively required (fs-root needs persistence)
- Tests with hardcoded fs-root string IDs need updating
