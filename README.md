# Commonplace Doc Server

A Rust server for managing documents with a small REST API, plus Server-Sent Events (SSE) for real-time updates.

## Features

- **REST API** for document CRUD and CRDT operations (`/docs`)
- Support for multiple content types: JSON, XML, and plain text
- **Commit endpoint** (`/docs/:id/commit`) that persists content-addressed commits to a local `redb` database (enabled with `--database`)
- For `text/plain`, `application/json`, and `application/xml` documents, commits are applied to the in-memory document body (so `GET /docs/:id` reflects committed edits)
- **SSE** for real-time document subscriptions and updates with history replay
- Built with [Axum](https://github.com/tokio-rs/axum) web framework
- In-memory document storage (document bodies are not persisted)

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Cargo

### Running the Server

```bash
cargo run
```

The server will start on `http://localhost:3000`.

### Running With Commit Storage

Commit storage is disabled unless you pass a database path:

```bash
cargo run -- --database ./commonplace.redb
```

Without `--database`, `POST /docs/:id/commit` returns `501 Not Implemented`.

### Development

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run

# Format code
cargo fmt

# Run linter
cargo clippy
```

## API Endpoints

See `docs/API.md` for detailed request/response examples.

### REST API

#### Create Document
```bash
POST /docs
Content-Type: application/json | application/xml | text/plain
```

Creates a new blank document with the specified content type. Returns:
```json
{"id": "uuid"}
```

**Examples:**
```bash
# Create JSON document
curl -X POST http://localhost:3000/docs \
  -H "Content-Type: application/json"

# Create XML document
curl -X POST http://localhost:3000/docs \
  -H "Content-Type: application/xml"

# Create text document
curl -X POST http://localhost:3000/docs \
  -H "Content-Type: text/plain"
```

#### Get Document
```bash
GET /docs/{uuid}
```

Retrieves the document content. The response Content-Type matches the document's type.

**Example:**
```bash
curl http://localhost:3000/docs/{uuid}
```

#### Delete Document
```bash
DELETE /docs/{uuid}
```

Deletes the specified document. Returns 204 No Content on success, 404 if not found.

**Example:**
```bash
curl -X DELETE http://localhost:3000/docs/{uuid}
```

#### Health Check
```bash
GET /health
```

Returns "OK" if the server is running.

### Document CRDT Operations

Additional endpoints for Yjs CRDT operations on documents:

#### Get Document Info
```bash
GET /docs/{id}/info
```

Returns document metadata:
```json
{"id": "uuid", "type": "document"}
```

#### Get Document Head
```bash
GET /docs/{id}/head
```

Returns the document HEAD with CID, content, and Yjs state:
```json
{"cid": "commit-id", "content": "document content", "state": "base64_yjs_state"}
```

#### Send Edit
```bash
POST /docs/{id}/edit
Content-Type: application/json

{"update": "base64_yjs_update", "author": "alice", "message": "optional"}
```

Sends a Yjs update to the document. Returns:
```json
{"cid": "new-commit-id"}
```

#### Replace Content
```bash
POST /docs/{id}/replace?parent_cid=...&author=...
Content-Type: text/plain

New document content here
```

Replaces document content with automatic diff computation. Returns:
```json
{"cid": "commit-id", "edit_cid": "edit-id", "summary": {"chars_inserted": 10, "chars_deleted": 5, "operations": 2}}
```

#### Fork Document
```bash
POST /docs/{id}/fork?at_commit=...
```

Creates a fork of the document at HEAD or a specific commit. Returns:
```json
{"id": "new-doc-id", "head": "commit-id"}
```

### SSE

#### Document Subscriptions

Subscribe to real-time updates from a document:

```bash
GET /sse/docs/{id}
```

Streams Server-Sent Events:
- `edit` events contain commit data (update, parents, timestamp, author, message)
- `warning` events if the subscription lags

**Example:**
```bash
curl -N http://localhost:3000/sse/docs/{id}
```

#### Document Change History

- `GET /documents/:id/changes?since=<timestamp>` - Fetch commit history for a document since an optional UNIX timestamp (milliseconds).
- `GET /documents/changes?doc_ids=<id1,id2,...>&since=<timestamp>` - Fetch commit history for multiple documents.
- `GET /documents/:id/stream?since=<timestamp>` - Stream commit history for a document, beginning at `since` and continuing with live updates via SSE.
- `GET /documents/stream?doc_ids=<id1,id2,...>&since=<timestamp>` - Stream commit history across multiple documents via SSE.

Commit notifications include canonical URLs of the form `commonplace://document/{doc_id}/commit/{commit_id}` that uniquely identify each commit within a document.

### Blue and Red Edges

Documents communicate through two distinct types of connections:

**Blue edges (edits)** carry persistent document changes:
- Yjs CRDT commits backed by merkle-tree storage
- Subscribe downstream to watch changes
- Push upstream to edit (requires parent context from listening first)

**Red edges (events)** carry ephemeral messages:
- JSON envelopes for cursors, presence, metadata
- Any client can POST to any document's red port (no subscription needed)
- Optionally subscribe to a document's red broadcasts

When you connect via SSE, the server creates a transient connection with a server-generated UUID. This connection subscribes to the document's blue port and has its own red port for receiving events. The connection is automatically cleaned up when the TCP connection closes.

See `docs/ARCHITECTURE.md` for detailed diagrams.

## Architecture

More detailed documentation:

- `docs/ARCHITECTURE.md`
- `docs/API.md`
- `docs/DEVELOPMENT.md`

The server is organized into a few main modules:

- `api.rs` - REST API endpoints for document management and CRDT operations
- `document.rs` - Document storage with content type support
- `node/` - Reactive document processing internals
  - `mod.rs` - Node trait definition
  - `document_node.rs` - Document node implementation
  - `registry.rs` - Document graph management
  - `types.rs` - Edit, Event, NodeId, NodeMessage types
  - `subscription.rs` - Subscription handling
- `commit.rs` / `store.rs` - Commit model and `redb`-backed commit storage (optional)
- `events.rs` - Commit broadcast for SSE change notifications
- `sse.rs` - Server-Sent Events for document subscriptions and change streams
- `main.rs` - Server initialization and routing

Documents are stored in-memory using a `DocumentStore`. Each document has:
- A UUID identifier
- Content (string)
- Content type (JSON, XML, or text)

### Default Document Content

When created, documents have default content based on their type:
- JSON: `{}`
- XML: `<?xml version="1.0" encoding="UTF-8"?><root/>`
- Text: (empty string)
