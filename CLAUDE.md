# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Rust server for managing documents with support for multiple content types (JSON, XML, text). The server exposes a REST API and has SSE capabilities for real-time updates.

### Architecture

- **Language**: Rust (edition 2021)
- **Purpose**: Document management server with multi-format support and reactive node graph
- **Web Framework**: Axum 0.7
- **API Protocols**:
  - REST API for document CRUD operations (`/docs`)
  - Node API for reactive document graph (`/nodes`)
  - SSE (Server-Sent Events) for real-time node subscriptions

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
├── api.rs              # REST API endpoints (/docs, /nodes)
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

#### REST API
- `POST /docs` - Create a blank document (Content-Type header: application/json, application/xml, or text/plain)
- `GET /docs/{uuid}` - Retrieve document content
- `DELETE /docs/{uuid}` - Delete a document
- `POST /docs/{uuid}/commit` - Persist a Yjs update and apply it to the document (requires `--database`)
- `GET /health` - Health check

#### Node API
- `POST /nodes` - Create a node (type: "document")
- `GET /nodes` - List all nodes
- `GET /nodes/{id}` - Get node info
- `DELETE /nodes/{id}` - Delete a node
- `POST /nodes/{id}/edit` - Send an edit (Yjs update) to a node
- `POST /nodes/{id}/event` - Send an event (ephemeral JSON) to a node
- `POST /nodes/{from}/wire/{to}` - Wire two nodes together
- `DELETE /nodes/{from}/wire/{to}` - Remove wiring between nodes

#### SSE
- `GET /sse/nodes/{id}` - Subscribe to real-time updates from a node (creates transient ConnectionNode)
- `GET /documents/{id}/changes` - Get commit history for a document
- `GET /documents/{id}/stream` - Stream document changes via SSE

### Blue and Red Edges

Nodes communicate via two port types:

- **Blue (edits)**: Persistent Yjs commits. Subscribe to watch changes, push to edit. Must listen before editing (need parent context).
- **Red (events)**: Ephemeral JSON. Any client can POST to any node's red port. Subscribe to watch broadcasts.

SSE connections are transient nodes with server-generated UUIDs. They subscribe to a document's blue port and have their own red port for receiving events.

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

## Development Commands

- `cargo build` - Build all binaries
- `cargo run --bin commonplace-server` - Run the HTTP server
- `cargo run --bin commonplace-sync` - Run the file sync client
- `cargo test` - Run tests
- `cargo clippy` - Run linter (required before commit)
- `cargo fmt` - Format code
- `RUST_LOG=debug cargo run --bin commonplace-server` - Run with debug logging
- `cargo run --bin commonplace-server -- --database ./data.redb` - Run with persistence

**Before committing:** Always run `cargo clippy` and `cargo test`. A pre-commit hook runs clippy automatically.

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
