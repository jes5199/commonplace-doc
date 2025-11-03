# Commonplace Doc Server

A Rust server for managing documents with REST and Server-Sent Events (SSE) APIs.

## Features

- **REST API** for document CRUD operations
- Support for multiple content types: JSON, XML, and plain text
- **SSE** for real-time document updates with history replay
- Built with [Axum](https://github.com/tokio-rs/axum) web framework
- In-memory document storage

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Cargo

### Running the Server

```bash
cargo run
```

The server will start on `http://localhost:3000`.

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

### SSE and Change History APIs

- `GET /documents/:id/changes?since=<timestamp>` - Fetch commit history for a document since an optional UNIX timestamp (milliseconds).
- `GET /documents/changes?doc_ids=<id1,id2,...>&since=<timestamp>` - Fetch commit history for multiple documents.
- `GET /documents/:id/stream?since=<timestamp>` - Stream commit history for a document, beginning at `since` and continuing with live updates via SSE.
- `GET /documents/stream?doc_ids=<id1,id2,...>&since=<timestamp>` - Stream commit history across multiple documents via SSE.

Commit notifications include canonical URLs of the form `commonplace://document/{doc_id}/commit/{commit_id}` that uniquely identify each commit within a document.

## Architecture

The server is organized into three main modules:

- `api.rs` - REST API endpoints for document management
- `document.rs` - Document storage with content type support
- `sse.rs` - Server-Sent Events for real-time updates and subscriptions
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
