# Commonplace Doc Server

A Rust server for managing documents with a small REST API, plus Server-Sent Events (SSE) for real-time updates.

## Features

- **REST API** for document CRUD operations (`/docs`)
- **Node API** for reactive document graph (`/nodes`) - nodes receive/emit edits and events, can be wired together
- Support for multiple content types: JSON, XML, and plain text
- **Commit endpoint** (`/docs/:id/commit`) that persists content-addressed commits to a local `redb` database (enabled with `--database`)
- For `text/plain`, `application/json`, and `application/xml` documents, commits are applied to the in-memory document body (so `GET /docs/:id` reflects committed edits)
- **SSE** for real-time node subscriptions and document updates with history replay
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

### Node API

The Node API provides a reactive abstraction where nodes can receive and emit edits (commits) and events (ephemeral JSON messages). Nodes can be wired together to create processing graphs.

#### Create Node
```bash
POST /nodes
Content-Type: application/json

{"node_type": "document", "content_type": "application/json"}
```

Creates a new node. Currently only `"document"` type is supported. Returns:
```json
{"id": "uuid", "node_type": "document"}
```

#### List Nodes
```bash
GET /nodes
```

Returns all registered nodes with their status.

#### Get Node Info
```bash
GET /nodes/{id}
```

Returns node details including subscriber count and health status.

#### Delete Node
```bash
DELETE /nodes/{id}
```

Shuts down and removes a node.

#### Send Edit to Node
```bash
POST /nodes/{id}/edit
Content-Type: application/json

{"update": "base64_yjs_update", "author": "alice", "message": "optional"}
```

Sends a Yjs update to the node. The node applies it and emits to subscribers.

#### Send Event to Node
```bash
POST /nodes/{id}/event
Content-Type: application/json

{"event_type": "cursor", "payload": {"x": 100, "y": 200}}
```

Sends an ephemeral event to the node. Events are forwarded to subscribers but not persisted.

#### Wire Nodes
```bash
POST /nodes/{from}/wire/{to}
```

Wires two nodes together. Edits and events emitted by `from` are sent to `to`. Returns:
```json
{"subscription_id": "uuid", "from": "node1", "to": "node2"}
```

Cycle detection prevents creating circular dependencies.

#### Unwire Nodes
```bash
DELETE /nodes/{from}/wire/{to}
```

Removes the wiring between nodes.

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

Nodes communicate through two distinct types of connections:

**Blue edges (edits)** carry persistent document changes:
- Yjs CRDT commits backed by merkle-tree storage
- Subscribe downstream to watch changes
- Push upstream to edit (requires parent context from listening first)

**Red edges (events)** carry ephemeral messages:
- JSON envelopes for cursors, presence, metadata
- Any client can POST to any node's red port (no subscription needed)
- Optionally subscribe to a node's red broadcasts

When you connect via SSE, the server creates a transient **ConnectionNode** with a server-generated UUID. This node subscribes to the document's blue port and has its own red port for receiving events. The node is automatically cleaned up when the TCP connection closes.

See `docs/ARCHITECTURE.md` for detailed diagrams.

## Architecture

More detailed documentation:

- `docs/ARCHITECTURE.md`
- `docs/API.md`
- `docs/DEVELOPMENT.md`

The server is organized into a few main modules:

- `api.rs` - REST API endpoints for document and node management
- `document.rs` - Document storage with content type support
- `node/` - Node trait abstraction for reactive document processing
  - `mod.rs` - Node trait definition
  - `document_node.rs` - Document node implementation
  - `registry.rs` - Node graph management with cycle detection
  - `types.rs` - Edit, Event, NodeId, NodeMessage types
  - `subscription.rs` - Subscription handling
- `commit.rs` / `store.rs` - Commit model and `redb`-backed commit storage (optional)
- `events.rs` - Commit broadcast for SSE change notifications
- `sse.rs` - Server-Sent Events for node subscriptions and document change streams
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
