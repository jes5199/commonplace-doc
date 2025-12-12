# Commonplace Doc Server

A Rust server for managing documents with a small REST API, plus an (early/placeholder) Server-Sent Events (SSE) endpoint.

## Features

- **REST API** for document CRUD operations (`/docs`)
- Support for multiple content types: JSON, XML, and plain text
- **Commit endpoint** (`/docs/:id/commit`) that persists content-addressed commits to a local `redb` database (enabled with `--database`)
- For `text/plain` documents, commits are applied to the in-memory document body (so `GET /docs/:id` reflects committed edits)
- **SSE** endpoint (currently heartbeats only)
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

### SSE

- `GET /sse/documents/:id` - Subscribe to document updates (placeholder implementation)

## Architecture

More detailed documentation:

- `docs/ARCHITECTURE.md`
- `docs/API.md`
- `docs/DEVELOPMENT.md`

The server is organized into a few main modules:

- `api.rs` - REST API endpoints for document management
- `document.rs` - Document storage with content type support
- `commit.rs` / `store.rs` - Commit model and `redb`-backed commit storage (optional)
- `sse.rs` - Server-Sent Events endpoint (placeholder)
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
