# Development

## Requirements

- Rust toolchain (stable)

## Useful commands

- `cargo run` (server)
- `cargo test` (tests)
- `cargo fmt` (format)
- `cargo clippy -- -D warnings` (lint)

This repo also provides:

- `Makefile` targets (e.g. `make run`, `make check`)
- `justfile` recipes (same intent as the Makefile)

`make dev` / `make test` use `cargo watch`, which you may need to install:

```bash
cargo install cargo-watch
```

## Running

Default:

```bash
cargo run
```

Configure bind address/port:

```bash
cargo run -- --host 0.0.0.0 --port 3000
```

Enable commit storage:

```bash
cargo run -- --database ./commonplace.redb
```

If `--database` is not provided, the server logs warnings and the commit endpoint returns `501 Not Implemented`.

## Logging

The server uses `tracing_subscriber` with an `EnvFilter`.

Example:

```bash
RUST_LOG=commonplace_doc=debug,tower_http=debug cargo run
```

## Tests and CI

- Unit tests exist in `src/commit.rs` and `src/store.rs`.
- Integration tests for the REST API exist in `tests/api_tests.rs`.
- GitHub Actions runs `cargo test`, `cargo fmt -- --check`, `cargo clippy -- -D warnings`, and a release build.

## Sync Tool

The `commonplace-sync` binary syncs a local file with a server-side document node. Changes flow bidirectionally: local edits push to the server, server edits update the local file.

### Basic Usage

```bash
cargo run --bin commonplace-sync -- \
  --server http://localhost:3000 \
  --node <node-id> \
  --file ./myfile.txt
```

### Testing Bidirectional Sync

This test demonstrates two sync clients keeping separate files in sync through the server.

**1. Start the server with persistence:**

```bash
cargo run --bin commonplace-server -- --database ./test.redb
```

**2. Create a document node:**

```bash
curl -X POST http://localhost:3000/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_type": "document"}'
# Returns: {"id":"<node-id>","node_type":"document"}
```

**3. Start two sync clients in separate terminals:**

Terminal 1:
```bash
RUST_LOG=info cargo run --bin commonplace-sync -- \
  --server http://localhost:3000 \
  --node <node-id> \
  --file /tmp/file1.txt
```

Terminal 2:
```bash
RUST_LOG=info cargo run --bin commonplace-sync -- \
  --server http://localhost:3000 \
  --node <node-id> \
  --file /tmp/file2.txt
```

**4. Test the sync:**

```bash
# Edit file1 - should appear in file2
echo "Hello from client 1" > /tmp/file1.txt
cat /tmp/file2.txt  # Should show: Hello from client 1

# Edit file2 - should appear in file1
echo "Hello from client 2" > /tmp/file2.txt
cat /tmp/file1.txt  # Should show: Hello from client 2
```

Both files stay in sync through the server. The sync tool:
- Debounces rapid file changes (100ms)
- Uses SSE to receive server updates in real-time
- Handles first commit (empty document) automatically
- Prevents echo loops (ignores self-triggered file events)

### JSON Files

JSON files (`.json`) are synced as Yjs map/array types instead of plain text.
This means:
- JSON must be valid (top-level object or array).
- Server serialization may normalize whitespace or key ordering.

## Troubleshooting

- `POST /docs/:id/commit` returns `501`: start the server with `--database <path>`.
- `POST /docs/:id/commit` returns `409`: the commit graph you're adding does not descend from the current document head (see `docs/ARCHITECTURE.md` for the monotonic descent rule).
