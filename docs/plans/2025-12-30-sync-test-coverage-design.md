# Test Coverage for sync.rs Refactoring

**Goal:** Add test coverage to `src/bin/sync.rs` (2581 lines) as a safety net before splitting into smaller modules (CP-3ve).

**Approach:** Pure function tests first, then mock-based tests for async functions. Inline tests in sync.rs that move with code during refactoring.

## Phase 1: Pure Function Tests

No refactoring needed. Add `#[cfg(test)] mod tests` block.

### URL Building Functions (~10 tests)

```rust
encode_node_id(node_id: &str) -> String
encode_path(path: &str) -> String
normalize_path(path: &str) -> String
build_head_url(server, path_or_id, use_paths) -> String
build_edit_url(server, path_or_id, use_paths) -> String
build_replace_url(server, path_or_id, parent_cid, use_paths) -> String
build_sse_url(server, path_or_id, use_paths) -> String
```

Test cases:
- Basic path encoding (spaces, special chars)
- Nested paths with slashes (`notes/todo.txt`)
- `use_paths=true` vs `use_paths=false` routing
- Edge cases: empty strings, unicode

### Yjs Update Functions (~8 tests)

```rust
create_yjs_text_update(content: &str) -> String
create_yjs_json_update(json: Value, path_prefix: &str) -> Result<String>
json_value_to_any(value: Value) -> Any
```

Test cases:
- Empty content
- Simple text
- Nested JSON objects/arrays
- JSON with various types (strings, numbers, bools, nulls)

### Utility Functions (~4 tests)

- `base64_encode` / `base64_decode` round-trip
- `normalize_path` edge cases

## Phase 2: Mock-Based Async Tests

Light refactoring to extract traits for HTTP and file operations.

### Dependencies

```toml
[dev-dependencies]
mockall = "0.11"
tokio-test = "0.4"
```

### Traits to Extract

```rust
#[cfg_attr(test, mockall::automock)]
trait SyncClient {
    async fn get_head(&self, url: &str) -> Result<HeadResponse>;
    async fn post_edit(&self, url: &str, update: &str) -> Result<()>;
    async fn post_replace(&self, url: &str, content: &str) -> Result<ReplaceResponse>;
}

#[cfg_attr(test, mockall::automock)]
trait FileSystem {
    async fn read(&self, path: &Path) -> Result<Vec<u8>>;
    async fn write(&self, path: &Path, content: &[u8]) -> Result<()>;
}
```

### Async Functions to Test (~10 tests)

- `upload_task`: file change triggers correct HTTP call
- `handle_server_edit`: SSE event triggers correct file write
- `refresh_from_head`: fetches and applies state correctly

## Phase 3: Ready for Refactoring

After tests are in place:
- Traits define natural module boundaries
- `SyncClient` → `sync/client.rs`
- `FileSystem` → `sync/fs.rs`
- URL builders → `sync/urls.rs`
- Yjs helpers → `sync/yjs.rs`

## Out of Scope

- `main()` argument parsing (clap handles it)
- SSE connection/reconnection logic (hard to mock, low refactor risk)
- Full directory watcher orchestration (integration test territory)

## Estimated Coverage

- ~22 pure function tests
- ~10 mock-based async tests
- Total: ~32 tests covering core sync logic
