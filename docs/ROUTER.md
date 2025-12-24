# Router Documents (Spec)

This document specifies how to designate a document node as a routing
document via `--router <node-id>`, and how routing documents drive
graph wiring inside the server process.

## Goals

- Let a document define node wiring declaratively.
- Avoid adding a naming/folder system; node IDs are the identifiers.
- Keep routing in-process and in-memory for now.

## Non-Goals (for now)

- Permissions or capability checks (macaroons, auth).
- Persistent wiring state (wires are rebuilt at runtime).
- External router services.

## CLI Flag

- `--router <node-id>`
  - Repeatable; each entry identifies a routing document by node ID.
  - If the node does not exist, the server creates a document node with
    `content_type = application/json` and default content `{}`.
  - If the node exists, it is treated as a routing doc without renaming
    or type changes.

## Router Document Schema

The routing document content is JSON:

```json
{
  "version": 1,
  "nodes": {
    "doc-a": { "type": "document", "content_type": "text/plain" },
    "doc-b": { "type": "document", "content_type": "application/json" }
  },
  "edges": [
    { "from": "doc-a", "to": "doc-b", "port": "blue" },
    { "from": "doc-b", "to": "doc-a", "port": "red" }
  ]
}
```

Schema rules:

- `version` is required; only `1` is supported.
- `nodes` is optional; it is a hint for node creation, not a hard
  requirement. If present, `content_type` must be a supported MIME
  value.
- `edges` is required and is the authoritative wiring list.
- `port` defaults to `"both"` and must be one of `"blue"`, `"red"`,
  `"both"`.

## Runtime Behavior

### Initialization

- For each `--router <node-id>`:
  - Resolve or create the router document node.
  - Read its current content and attempt to apply wiring.

### Change Handling

- Subscribe to the router document's blue port.
- On each edit:
  - Read the full content.
  - Parse as JSON; if invalid, keep the previous wiring and emit a
    `router.error` event (red port) with details.
  - If valid, diff edges against the last-applied graph:
    - Add missing wires.
    - Remove wires that are no longer declared.

### Wiring Semantics

- The router only manages wires it created. It must not remove wires that
  pre-existed or were added by other subsystems.
- Wiring uses `NodeRegistry` directly:
  - `port = blue` => `wire_blue(from, to)`
  - `port = red` => `wire_red(from, to)`
  - `port = both` => `wire(from, to)`
- Cycle detection remains as implemented; if a new edge would create a
  cycle, the router skips it and emits `router.error`.

### Node Creation (Optional)

- If a node appears in `nodes` but does not exist:
  - Create a document node using `content_type`.
  - If `content_type` is missing, default to `application/json`.

## Persistence and Restarts

- Wires are in-memory only.
- On restart, the router re-applies wiring from the router document's
  current content.
- If the commit store is disabled, router documents are still in-memory
  and may be empty on restart.

## Observability

- Emit `router.error` events on the router document's red port for:
  - JSON parse errors.
  - Unsupported `version`.
  - Missing nodes or invalid edges.
  - Cycle detection failures.

## Security

- Router documents are trusted in this spec. No permission checks are
  performed yet.
