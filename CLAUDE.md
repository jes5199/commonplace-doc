# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Rust server for managing documents with support for multiple content types (JSON, XML, text). The server exposes a REST API and has SSE capabilities for real-time updates.

### Architecture

- **Language**: Rust (edition 2021)
- **Purpose**: Document management server with multi-format support
- **Web Framework**: Axum 0.7
- **API Protocols**:
  - REST API for document CRUD operations
  - SSE (Server-Sent Events) for real-time updates (placeholder)

### Key Dependencies

- `axum` - Web framework
- `tokio` - Async runtime
- `tower-http` - Middleware (CORS, tracing)
- `serde` / `serde_json` - Serialization
- `uuid` - Document ID generation
- `async-stream` - SSE stream support

### Code Structure

- `src/main.rs` - Server initialization and routing
- `src/api.rs` - REST API endpoints (/docs)
- `src/document.rs` - DocumentStore with ContentType enum and in-memory storage
- `src/sse.rs` - Server-Sent Events endpoints

The server runs on `localhost:3000` by default.

### API Endpoints

#### REST API
- `POST /docs` - Create a blank document (Content-Type header: application/json, application/xml, or text/plain)
- `GET /docs/{uuid}` - Retrieve document content
- `DELETE /docs/{uuid}` - Delete a document
- `GET /health` - Health check

#### SSE (planned)
- `GET /documents/:id/stream` - Subscribe to document updates

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

- `cargo build` - Build the project
- `cargo run` - Run the server locally
- `cargo test` - Run tests
- `cargo clippy` - Run linter
- `cargo fmt` - Format code
- `RUST_LOG=debug cargo run` - Run with debug logging

## Git Configuration

- Main branch: `main`
