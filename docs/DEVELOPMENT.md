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

## Troubleshooting

- `POST /docs/:id/commit` returns `501`: start the server with `--database <path>`.
- `POST /docs/:id/commit` returns `409`: the commit graph you’re adding does not descend from the current document head (see `docs/ARCHITECTURE.md` for the monotonic descent rule).
