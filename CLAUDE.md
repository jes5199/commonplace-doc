# CLAUDE.md

**Note**: This project uses [bd (beads)](https://github.com/steveyegge/beads)
for issue tracking. Use `bd` commands instead of markdown TODOs.
See AGENTS.md for workflow details.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Rust server for managing documents with support for multiple content types (JSON, XML, text). The server exposes a REST API and has SSE capabilities for real-time updates.

### Architecture

- **Language**: Rust (edition 2021)
- **Purpose**: Document management server with multi-format support and reactive node graph
- **Web Framework**: Axum 0.7
- **API Protocols**:
  - REST API for document CRUD and CRDT operations (`/docs`)
  - SSE (Server-Sent Events) for real-time document subscriptions

### Key Dependencies

- `axum` - Web framework
- `tokio` - Async runtime
- `tower-http` - Middleware (CORS, tracing)
- `serde` / `serde_json` - Serialization
- `uuid` - Document ID generation
- `yrs` - Yjs-compatible CRDT implementation
- `async-trait` - Async trait support
- `async-stream` - SSE stream support
- `redb` - Persistent key-value storage (enabled with `--database`)

### Code Structure

```
src/
├── lib.rs              # Library root, router setup
├── bin/
│   └── server.rs       # HTTP server binary (commonplace-server)
├── api.rs              # REST API endpoints (/docs)
├── sse.rs              # Server-Sent Events for subscriptions
├── document.rs         # DocumentStore, ContentType enum
├── commit.rs           # Commit model (merkle-tree)
├── store.rs            # CommitStore - redb persistence
├── diff.rs             # Character-level diffing → Yjs updates
├── replay.rs           # Commit DAG traversal, state reconstruction
├── node/               # Reactive node graph
│   ├── mod.rs          # Node trait (blue/red ports)
│   ├── types.rs        # Edit, Event, NodeId, Port
│   ├── document_node.rs # Persistent Yjs document wrapper
│   ├── connection_node.rs # Transient SSE client nodes
│   ├── registry.rs     # NodeRegistry with cycle detection
│   └── subscription.rs # Blue/Red subscriptions
├── events.rs           # CommitBroadcaster for SSE
├── b64.rs              # Base64 utilities
└── cli.rs              # CLI argument definitions
```

The server runs on `localhost:3000` by default.

### API Endpoints

#### Document API
- `POST /docs` - Create a document (JSON body with `content_type` or Content-Type header)
- `GET /docs/{id}` - Retrieve raw document content
- `DELETE /docs/{id}` - Delete a document
- `GET /docs/{id}/info` - Get document metadata (id, type)
- `GET /docs/{id}/head` - Get document HEAD (cid, content, Yjs state)
- `POST /docs/{id}/commit` - Persist a Yjs update (requires `--database`)
- `POST /docs/{id}/edit` - Send a Yjs update to a document
- `POST /docs/{id}/replace` - Replace content with diff computation
- `POST /docs/{id}/fork` - Fork a document at HEAD or specific commit
- `GET /health` - Health check

#### SSE
- `GET /sse/docs/{id}` - Subscribe to real-time document updates
- `GET /documents/{id}/changes` - Get commit history for a document
- `GET /documents/{id}/stream` - Stream document changes via SSE

### Blue and Red Edges

Documents communicate via two port types:

- **Blue (edits)**: Persistent Yjs commits. Subscribe to watch changes, push to edit. Must listen before editing (need parent context).
- **Red (events)**: Ephemeral JSON. Any client can POST to any document's red port. Subscribe to watch broadcasts.

SSE connections are transient with server-generated UUIDs. They subscribe to a document's blue port and have their own red port for receiving events.

See `docs/ARCHITECTURE.md` for detailed diagrams.

### Document Storage

Documents are stored in-memory with:
- UUID identifier
- Content (String)
- ContentType enum (Json, Xml, Text)

Default content by type:
- JSON: `{}`
- XML: `<?xml version="1.0" encoding="UTF-8"?><root/>`
- Text: empty string

## Running the System

The **orchestrator** is the main entry point for running the full commonplace system. It manages all processes including the server, sync client, and application processes like bartleby.

### Quick Start

```bash
# Build and run everything
cargo build --release
cargo run --release --bin commonplace-orchestrator
```

The orchestrator reads `commonplace.json` which defines:
- **server**: The document server (commonplace-server)
- **sync**: File sync client (commonplace-sync)
- **Application processes**: bartleby, text-to-telegram, etc.

### Configuration

Edit `commonplace.json` to configure:
- `mqtt_broker`: MQTT broker for process communication
- `processes`: Map of process definitions with commands, args, cwd, restart policies, and dependencies

### Running Individual Components

For development/testing, you can run components separately:

```bash
# Server only
cargo run --bin commonplace-server -- --database ./data.redb --fs-root workspace

# Sync only (requires server running)
cargo run --bin commonplace-sync -- --server http://localhost:3000 --node workspace --directory ./workspace
```

### Working in workspace/

The `workspace/` directory is synced bidirectionally by `commonplace-sync`. When the orchestrator is running:

- **File edits**: Creating, modifying, or deleting files in `workspace/` automatically syncs to the server
- **Schema updates**: Changes to `.commonplace.json` files propagate to the server
- **Directory operations**: `mkdir`, `rm -r`, etc. are detected and synced
- **No manual intervention needed**: The sync client watches for filesystem events and handles them

This means you can edit files in `workspace/` directly and trust that changes will propagate. The sync uses CRDTs (Yjs) so concurrent edits from multiple sources merge automatically.

**Note**: The sync client must be running (via orchestrator or standalone) for changes to propagate.

### CLI Tools

#### commonplace-ps

List all processes managed by the orchestrator:

```bash
# Human-readable table
commonplace-ps

# JSON output
commonplace-ps --json
```

Shows process name, PID, state, and working directory for each managed process.

#### commonplace-uuid

Resolve a synced file path to its UUID:

```bash
# Get UUID for a file
commonplace-uuid path/to/file.txt

# JSON output
commonplace-uuid --json path/to/file.txt
```

#### commonplace-replay

View commit history or content at any point in time:

```bash
# List all commits for a file
commonplace-replay path/to/file.txt --list

# Show current content
commonplace-replay path/to/file.txt

# Show content at specific commit
commonplace-replay path/to/file.txt --at <commit-id>

# JSON output
commonplace-replay path/to/file.txt --list --json
```

#### commonplace-signal

Send a signal to an orchestrator-managed process:

```bash
# Send SIGTERM (default) to restart a process
commonplace-signal -n bartleby

# Send specific signal
commonplace-signal -n sync -s KILL

# Filter by document path when multiple processes have same name
commonplace-signal -n sync -p /text-to-telegram
```

#### commonplace-link

Link two files to share the same UUID (see "File Linking" section below).

## Development Commands

- `cargo build` - Build all binaries
- `cargo run --bin commonplace-orchestrator` - Run the full system via orchestrator
- `cargo run --bin commonplace-server` - Run the HTTP server only
- `cargo run --bin commonplace-sync` - Run the file sync client only
- `cargo test` - Run tests
- `cargo clippy` - Run linter (required before commit)
- `cargo fmt` - Format code
- `RUST_LOG=debug cargo run --bin commonplace-server` - Run with debug logging

**Before committing:** Always run `cargo clippy` and `cargo test`. A pre-commit hook runs clippy automatically.

## File Linking in Workspaces

**CRITICAL**: NEVER create filesystem symlinks (`ln -s`) anywhere in workspace/ or synced directories.

### Why no symlinks?

- Symlinks break sandbox isolation (processes can escape their directory)
- Symlinks don't sync through commonplace - they become broken paths
- Symlinks bypass the CRDT sync mechanism entirely
- If you need to share content, use `commonplace-link` to assign the same UUID

### Instead of symlinks

- **To share file content**: Use `commonplace-link source.txt target.txt`
- **To access another sandbox's data**: The sandbox architecture handles this automatically - bartleby's root-level checkout includes all subdirectories like `tmux/`
- **To reference external files**: Copy the content, don't link

### Usage

```bash
# Link two files (they will share the same content via commonplace sync)
commonplace-link text-to-telegram/content.txt bartleby/prompts.txt

# Both files now have the same UUID in their .commonplace.json
# Changes to either file propagate to the other through sync
```

See `docs/SANDBOX_LINKING.md` for detailed architecture.

## Git Configuration

- Main branch: `main`

## Code Review Workflow

Use a dual-review approach alternating between local and GitHub-based code reviews:

### Local Code Review (before commit/PR)
Run `codex review` CLI to review uncommitted changes or branch differences:
- Use the `local-codex-review` skill for reviewing changes before committing
- Good for catching issues early before pushing

### GitHub Codex Review (on PRs)
Request inline code review comments from GitHub Codex bot:
- Use the `github-codex-review` skill after creating or updating PRs
- Retrieves inline comments and suggestions from the Codex bot
- Check review status before merging

### Workflow Pattern
1. Implement changes following TDD
2. Run `local-codex-review` before committing
3. Address any issues found
4. Create PR and push
5. Run `github-codex-review` to get PR feedback
6. Address ALL P1 issues (blocking)
7. Address P2 issues (non-blocking suggestions)
8. Push fixes and request `@codex review` again
9. Repeat steps 6-8 until no new issues
10. Merge ONLY when Codex review is clear (no unaddressed P1 issues)

**CRITICAL: DO NOT MERGE UNTIL CODEX REVIEW SAYS IT IS ALL CLEAR**

P1 issues are blocking and MUST be fixed before merging. If a P1 is found after merge, immediately create a bead to track the fix.
