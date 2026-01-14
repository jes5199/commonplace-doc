# Commonplace API Reference

This document describes all HTTP endpoints and MQTT topics available in the commonplace server.

Base URL (default): `http://127.0.0.1:3000`

---

## HTTP Endpoints

### Health Check

#### `GET /health`

Returns `OK` if the server is running.

---

### Document API (ID-based)

All document operations use UUID-based identifiers.

#### `POST /docs`

Creates a blank document based on the request `Content-Type`.

**Supported Content-Types:**
- `application/json` - JSON documents (Y.Map)
- `application/jsonl` - JSON Lines documents
- `application/xml` or `text/xml` - XML documents (Y.XmlFragment)
- `text/plain` - Plain text documents (Y.Text)

**Response:** `{"id": "<uuid>"}`

**Errors:**
- `415 Unsupported Media Type` if Content-Type not supported

#### `GET /docs/:id`

Returns document content with appropriate Content-Type header.

**Errors:**
- `404 Not Found` if document doesn't exist

#### `DELETE /docs/:id`

Deletes a document.

**Response:** `204 No Content`

**Errors:**
- `404 Not Found` if document doesn't exist

#### `GET /docs/:id/info`

Returns document metadata.

**Response:** `{"id": "<uuid>", "content_type": "text/plain"}`

#### `GET /docs/:id/head`

Returns HEAD commit, content, and Yjs state.

**Query Parameters:**
- `at` - Optional commit ID to get historical state

**Response:**
```json
{
  "cid": "<commit_id>",
  "content": "<document content>",
  "state": "<base64 Yjs state>"
}
```

#### `POST /docs/:id/edit`

Applies a Yjs update to the document.

**Request Body:**
```json
{
  "update": "<base64 Yjs update>",
  "author": "optional",
  "message": "optional"
}
```

**Response:** `{"cid": "<commit_id>", "timestamp": 1234567890}`

#### `POST /docs/:id/replace`

Replaces document content with automatic diff computation.

**Request Body:**
```json
{
  "content": "<new content>",
  "parent_cid": "optional - required if document has history",
  "author": "optional"
}
```

**Response:**
```json
{
  "cid": "<commit_id>",
  "edit_cid": "<edit_commit_id>",
  "chars_inserted": 10,
  "chars_deleted": 5,
  "operations": 2
}
```

**Errors:**
- `428 Precondition Required` if document has history but no parent_cid provided

#### `POST /docs/:id/commit` (requires `--database`)

Creates a commit with explicit Yjs update.

**Request Body:**
```json
{
  "verb": "update",
  "value": "<base64 Yjs update>",
  "author": "optional",
  "message": "optional",
  "parent_cid": "optional"
}
```

**Response:** `{"cid": "<commit_id>", "merge_cid": "<optional merge commit>"}`

**Errors:**
- `501 Not Implemented` if server started without `--database`
- `409 Conflict` if commit violates monotonic descent

#### `POST /docs/:id/fork`

Forks a document at HEAD or specific commit.

**Request Body:**
```json
{
  "at_commit": "optional commit_id"
}
```

**Response:** `{"id": "<new_doc_id>", "head": "<head_commit_id>"}`

#### `GET /docs/:id/is-ancestor`

Checks if one commit is an ancestor of another.

**Query Parameters:**
- `ancestor` - Potential ancestor commit ID
- `descendant` - Potential descendant commit ID

**Response:** `{"is_ancestor": true}`

#### `GET /fs-root`

Returns the filesystem root document ID (if configured with `--fs-root`).

**Response:** `{"id": "<fs_root_id>"}`

---

### File API (Path-based)

Path-based access resolves filesystem paths to document IDs via the fs-root schema.
Requires server to be started with `--fs-root`.

#### `GET /files/*path`

Get file content by path.

#### `GET /files/*path/head`

Get HEAD commit for file.

#### `POST /files/*path/edit`

Apply Yjs update to file. Same body as `/docs/:id/edit`.

#### `POST /files/*path/replace`

Replace file content. Same body as `/docs/:id/replace`.

#### `DELETE /files/*path`

Delete file.

---

### Server-Sent Events (SSE)

Real-time document update streams.

#### `GET /sse/docs/:id`

Stream edits for a document by ID. Emits `commit` events with:
```json
{
  "doc_id": "<uuid>",
  "commit_id": "<cid>",
  "timestamp": 1234567890
}
```

#### `GET /sse/files/*path`

Stream edits for a document by path.

#### `GET /documents/:id/changes`

Get commit history as JSON array.

#### `GET /documents/:id/commits`

Get commits with full Yjs updates.

#### `GET /documents/:id/stream`

Stream commit changes as SSE.

#### `GET /documents/changes?ids=id1,id2`

Get changes for multiple documents.

#### `GET /documents/stream?ids=id1,id2`

Stream changes for multiple documents.

---

### WebSocket

Real-time Yjs synchronization using WebSocket.

#### `GET /ws/docs/:id`

WebSocket sync by document ID.

#### `GET /ws/files/*path`

WebSocket sync by file path.

**Subprotocols:**
- `y-websocket` - Standard Yjs protocol (browser compatibility)
- `commonplace` - Extended protocol with commit metadata

---

### Optional Routes

**Viewer** (requires `--static-dir`):
- `GET /view/docs/:id` - Document viewer page
- `GET /view/files/*path` - File viewer page
- `GET /static/*path` - Static assets

**SDK**:
- `GET /sdk/*file_path` - TypeScript SDK for Deno imports

---

## MQTT Topics

All MQTT topics follow the pattern: `{workspace}/{port}/{path}[/{qualifier}]`

Default workspace: `commonplace`

### Port Types

| Port | Purpose | QoS | Persistence |
|------|---------|-----|-------------|
| `edits` | Yjs document updates | AtLeastOnce (1) | Persistent (blue) |
| `sync` | Merkle tree synchronization | AtLeastOnce (1) | Persistent (blue) |
| `events` | Ephemeral broadcasts | AtMostOnce (0) | Ephemeral (red) |
| `commands` | Store-level operations | AtLeastOnce (1) | Request/Response |

### Edits Port

Persistent Yjs commits for document synchronization.

**Topics:**
- `{workspace}/edits/{doc_id}` - Single document
- `{workspace}/edits/#` - All documents (wildcard)

**Message Format (EditMessage):**
```json
{
  "update": "<base64 Yjs update>",
  "parents": ["<parent_cid>"],
  "author": "client-name",
  "message": "optional",
  "timestamp": 1234567890
}
```

### Sync Port

Merkle tree synchronization for state reconciliation.

**Topics:**
- `{workspace}/sync/{doc_id}/{client_id}` - Client-specific

### Events Port

Ephemeral JSON broadcasts.

**Topics:**
- `{workspace}/events/{doc_id}/{event_name}` - Specific event
- `{workspace}/events/{doc_id}/#` - All events for doc

### Commands Port

Store-level operations.

**Topics:**
- `{workspace}/commands/create-document`
- `{workspace}/commands/delete-document`
- `{workspace}/commands/get-content`
- `{workspace}/commands/get-info`

**Responses:** `{workspace}/responses`

---

## Notes

### Document ID Encoding

Document IDs containing `/` must be URL-encoded in HTTP paths.
Example: `workspace/file.txt` becomes `workspace%2Ffile.txt`

### CRDT Safety

When editing documents with existing history:
1. Fetch HEAD first to get current state
2. Include `parent_cid` in edit/replace requests
3. Server rejects blind edits (returns `428 Precondition Required`)
