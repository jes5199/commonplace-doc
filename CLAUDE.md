# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Rust server for managing Yjs documents. The server exposes a custom API over REST and Server-Sent Events (SSE).

### Architecture

- **Language**: Rust
- **Purpose**: Yjs document management server
- **API Protocols**:
  - REST API for document operations
  - SSE (Server-Sent Events) for real-time updates

## Development Commands

Once the Rust project is initialized, common commands will include:

- `cargo build` - Build the project
- `cargo test` - Run tests
- `cargo run` - Run the server locally
- `cargo clippy` - Run linter
- `cargo fmt` - Format code

## Git Configuration

- Main branch: `main`
