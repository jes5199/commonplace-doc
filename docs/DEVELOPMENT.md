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

## cbd (Commonplace Bug Database)

`cbd` is a lightweight issue tracker CLI that stores issues in a JSONL file synced through commonplace. It's designed for use alongside [beads](https://github.com/steveyegge/beads) but operates in a simpler JSONL-only mode.

### Basic Usage

```bash
# List all open issues
cbd list --status open

# List issues ready to work on (no blockers)
cbd ready

# Show issue details
cbd show CP-abc1

# Create an issue
cbd create "Fix the login bug" -t bug -p 1

# Create with full options
cbd create "Add user profiles" \
  -t feature -p 2 \
  -a jes \
  -l backend,api \
  --deps blocks:CP-abc1 \
  -d "Users need profile pages"

# Update an issue
cbd update CP-abc1 --status closed
cbd update CP-abc1 --priority 1 --labels urgent,blocking

# Close an issue
cbd close CP-abc1 -r "Fixed in commit abc123"

# Manage dependencies
cbd dep add CP-abc1 CP-xyz9    # CP-abc1 is blocked by CP-xyz9
cbd dep remove CP-abc1 CP-xyz9
cbd dep list CP-abc1

# List blocked issues
cbd blocked
```

### Config Discovery

cbd looks for configuration in this order:

1. Command-line flags (`--server`, `--path`)
2. Environment variables (`COMMONPLACE_SERVER`, `CBD_PATH`)
3. Config file `.cbd.json` (searched up from cwd)
4. Config file `.beads/.cbd.json` (searched up from cwd)
5. Defaults (`http://localhost:3000`, `beads/commonplace-issues.jsonl`)

Example `.cbd.json`:
```json
{
  "path": "beads/my-project-issues.jsonl",
  "server": "http://localhost:3000"
}
```

### JSON Output

Use `--json` for machine-readable output:

```bash
# List as JSON array
cbd list --json

# Single issue as JSON
cbd show CP-abc1 --json
```

Example JSON output:
```json
{
  "id": "CP-abc1",
  "title": "Fix login bug",
  "status": "open",
  "priority": 1,
  "issue_type": "bug",
  "created_at": "2026-01-05T10:00:00Z",
  "created_by": "jes",
  "updated_at": "2026-01-05T10:00:00Z",
  "labels": ["urgent"],
  "dependencies": []
}
```

### Differences from bd

cbd is intentionally simpler than the full `bd` CLI:

| Feature | bd | cbd |
|---------|-----|-----|
| Storage | SQLite + JSONL export | JSONL only (via commonplace) |
| Daemon | Background sync daemon | None (direct HTTP) |
| Sync modes | `--no-db`, `--readonly`, `--sandbox` | Always JSONL-only |
| Prefix routing | Multi-rig support | Single JSONL file |
| Epics/molecules | Full hierarchy | Basic parent field |
| Events/agents | Specialized types | Standard types only |

**Why the difference?** cbd is designed for:
- Simple projects that don't need SQLite
- Environments where commonplace provides the sync layer
- Cases where JSONL portability matters more than query speed

For full beads functionality, use `bd` directly.

## Troubleshooting

- `POST /docs/:id/commit` returns `501`: start the server with `--database <path>`.
- `POST /docs/:id/commit` returns `409`: the commit graph you're adding does not descend from the current document head (see `docs/ARCHITECTURE.md` for the monotonic descent rule).
