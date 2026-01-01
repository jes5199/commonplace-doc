# Filesystem-in-JSON (Spec)

This document specifies a JSON format for describing a directory tree.
The JSON lives in a document node; a server flag designates which node
is the filesystem root.

## Goals

- Represent a directory tree in a single JSON document.
- Keep node IDs stable without requiring a folder/filename system.
- Allow explicit IDs for files when stability across renames matters.

## Non-Goals (for now)

- Permissions or access control.
- Persisted wiring or indexing beyond the JSON doc itself.
- Rich file metadata (owners, timestamps, ACLs).

## CLI Flag

- `--fs-root <node-id>`
  - Identifies the document node whose JSON defines the filesystem.
  - If the node does not exist, the server creates a document node with
    `content_type = application/json` and default content `{}`.

## Schema

```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "notes": {
        "type": "dir",
        "entries": {
          "ideas.txt": { "type": "doc" }
        }
      },
      "tasks.json": {
        "type": "doc",
        "content_type": "application/json"
      }
    }
  }
}
```

### Entry Types

- `dir`
  - **Inline directory**: has `entries` and **no** `node_id`.
  - **Node-backed directory**: has `node_id` (and optional `content_type`)
    and **no** `entries`.
  - These two forms are mutually exclusive.
  - For node-backed directories, the referenced node's content should be
    another filesystem JSON document (same schema) to define its entries.
- `doc`
  - Optional fields:
    - `node_id` (string): explicit ID of the document node.
    - `content_type` (string): MIME type for new nodes.
      - For JSON arrays, use `application/json;root=array`.

### Name Rules

- Entry names are single path segments (no `/`).
- Reserved: `.` and `..` are invalid.
- Names are case-sensitive.

## Node ID Resolution

Each `doc` entry resolves to a document node ID. A `dir` entry only
resolves to a document node if it declares `node_id` (node-backed form).
Inline directories do not create nodes.

1. If `node_id` is present, use it directly.
2. Otherwise derive an ID from the filesystem root node ID and the path:
   - Format: `<fs-root-id>:<path>`
   - Example: root `fs-root` + path `notes/ideas.txt`
     => `fs-root:notes/ideas.txt`

If you need stable IDs across renames, always set `node_id` explicitly.

## Node Creation

For each resolved `doc` entry:

- If the node exists, it is reused as-is.
- If missing, create a new document node:
  - `content_type` if provided, otherwise `application/json`.

For each `dir` entry that declares `node_id`, apply the same creation
rules as for `doc` entries. Directories without `node_id` are inline
containers and do not create nodes.

## Change Handling

When the filesystem JSON changes:

- Parse the document; invalid JSON leaves the last valid view in effect.
- Diff entries and apply creates for new `doc` nodes.
- Deletes are non-destructive: removing an entry does not delete the node.

## Errors and Events

On parse errors or invalid schema, emit an error event on the root
document's red port:

```
event_type: "fs.error"
payload: { "message": "...", "path": "..." }
```

## Persistence

- The filesystem JSON lives in a document node and can be persisted if
  the commit store is enabled.
- Derived document nodes are independent and may outlive the filesystem
  JSON entry that referenced them.

## File Linking (Shared UUIDs)

Multiple files can sync to the same document by sharing a `node_id`. This
enables bi-directional file linking where edits to any linked file propagate
to all others via the shared document.

### Use Case

Link `telegram/content.txt` and `bartleby/prompts.txt` so telegram messages
become bartleby's input:

```json
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "bartleby": {
        "type": "dir",
        "entries": {
          "prompts.txt": {
            "type": "doc",
            "node_id": "0b250a22-3163-49df-8634-534585865cdd"
          }
        }
      },
      "telegram": {
        "type": "dir",
        "entries": {
          "content.txt": {
            "type": "doc",
            "node_id": "0b250a22-3163-49df-8634-534585865cdd"
          }
        }
      }
    }
  }
}
```

Both files share the same UUID. When the reconciler creates documents, it
respects pre-set `node_id` values. Sync will push/pull both files to the
same document.

### Setup Workflow (Current Workaround)

Until CP-81o and CP-7gx are implemented, manual setup is required:

1. Edit `.commonplace.json` with shared UUIDs
2. Start server WITHOUT `--fs-root`:
   ```bash
   cargo run --bin commonplace-server -- --database ./data.redb
   ```
3. Create fs-root document:
   ```bash
   FS_ROOT=$(cat .commonplace.json | curl -s -X POST http://localhost:3000/docs \
     -H "Content-Type: application/json" -d @- | jq -r '.id')
   ```
4. Restart server WITH `--fs-root`:
   ```bash
   cargo run --bin commonplace-server -- --database ./data.redb --fs-root "$FS_ROOT"
   ```
5. Run sync:
   ```bash
   cargo run --bin commonplace-sync -- --directory ./workspace \
     --server http://localhost:3000 --node "$FS_ROOT"
   ```

### Known Limitations

- **CP-81o**: Server doesn't expose fs-root ID at `/fs-root` endpoint
- **CP-7gx**: Sync `--use-paths` still requires `--node`
- **CP-eoy**: `commonplace-link` tool only works within same directory
