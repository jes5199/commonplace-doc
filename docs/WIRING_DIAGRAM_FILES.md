# Wiring Diagram Files (Spec)

This document describes how to store router wiring JSON as files inside
directories synchronized by `commonplace-sync --directory`.

It builds on:
- `docs/FILESYSTEM.md` (filesystem-in-JSON and derived node IDs)
- `docs/ROUTER.md` (router document schema and runtime behavior)

## Goals

- Store wiring diagrams as normal JSON files in synced directories.
- Reuse the router document schema without new wire semantics.
- Keep node IDs deterministic for files in a directory sync root.
- Allow multiple wiring diagrams under a single fs-root.

## Non-Goals

- Access control or permissions for wiring changes.
- Persistent wiring state beyond router documents.
- Automatic merge or conflict resolution for wiring files.

## Terminology

- **fs-root**: the document node whose JSON defines the synced directory tree.
- **wiring diagram file**: a JSON file whose contents follow the router
  document schema and are treated as a router document.

## File Placement and Naming

- Wiring diagrams are stored as regular JSON files in the synced directory.
- Recommended folder: `wiring/` under the sync root.
- Recommended filename pattern: `*.router.json` to distinguish wiring diagrams
  from application data.

The file's document node ID is derived from the fs-root and relative path
(unless the filesystem JSON assigns an explicit `node_id`).

Example with fs-root `fs-root`:
- File path `wiring/main.router.json`
- Node ID `fs-root:wiring/main.router.json`

Use explicit `node_id` in the filesystem JSON if a wiring file must keep a
stable node ID across renames.

## File Content (Router Schema)

A wiring diagram file MUST contain a router document JSON payload exactly as
specified in `docs/ROUTER.md`.

Minimal example:

```json
{
  "version": 1,
  "edges": []
}
```

Example with edges:

```json
{
  "version": 1,
  "edges": [
    {
      "from": "fs-root:notes/source.txt",
      "to": "fs-root:notes/summary.txt",
      "port": "blue"
    }
  ]
}
```

## Node Reference Rules

- `from` and `to` are node IDs, not file paths.
- For files in the same synced directory, use derived IDs in the form
  `<fs-root>:<relative-path>` with forward slashes.
- For external nodes, use the full node ID directly.

## Activation and Runtime Behavior

The server activates router documents via `--router <node-id>`.
When wiring diagrams live in a synced directory, use the derived node IDs of
those files as `--router` arguments at server startup.

Once activated:
- Edits to the wiring JSON file are synced to the server node.
- The router manager re-applies wiring based on the new content.
- Invalid JSON or unsupported versions emit `router.error` on the router
  document's red port and keep the last valid wiring.

## Sync Behavior

- Wiring diagram files are synced like any other text file with
  `content_type = application/json`.
- Local edits propagate to the server and trigger router updates.
- Server edits propagate back to the local file via the sync client.
- Removing a wiring file from the filesystem JSON does not delete the document
  node or its history (non-destructive, consistent with filesystem behavior).

## Example Layout and Commands

```
workspace/
  wiring/
    main.router.json
  notes/
    source.txt
    summary.txt
```

```bash
# Start server
cargo run --bin commonplace-server -- --fs-root fs-root --router fs-root:wiring/main.router.json

# Start directory sync
cargo run --bin commonplace-sync -- --server http://localhost:3000 --node fs-root --directory ./workspace
```
