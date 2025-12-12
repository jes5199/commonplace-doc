# API

Base URL (default): `http://127.0.0.1:3000`

## Health

### `GET /health`

- `200 OK`
- body: `OK`

## Documents

### `POST /docs`

Creates a blank document based on the request `Content-Type`.

Supported `Content-Type` values:

- `application/json`
- `application/xml` (or `text/xml`)
- `text/plain`

Response:

- `200 OK`
- `application/json`
- body: `{"id":"<uuid>"}` (UUIDv4 string)

Errors:

- `415 Unsupported Media Type` if `Content-Type` is not supported.

Example:

```bash
curl -X POST http://127.0.0.1:3000/docs -H 'Content-Type: application/json'
```

### `GET /docs/:id`

Returns the document content and sets the response `Content-Type` to match the document’s type.

Response:

- `200 OK` with body content
- `404 Not Found` if the ID does not exist

Example:

```bash
curl -i http://127.0.0.1:3000/docs/<uuid>
```

### `DELETE /docs/:id`

Response:

- `204 No Content` if deleted
- `404 Not Found` if not found

## Commits (requires `--database`)

The commit API persists a content-addressed commit graph (a commit’s CID is a SHA-256 hash of the JSON-serialized commit).

### `POST /docs/:id/commit`

Requires that the server is started with `--database <path>`. Otherwise:

- `501 Not Implemented`

Request body (JSON):

```json
{
  "verb": "update",
  "value": "<string>",
  "author": "optional string (defaults to anonymous)",
  "message": "optional string",
  "parent_cid": "optional string"
}
```

Notes:

- Only `"verb": "update"` is accepted today.
- `"value"` is stored as `Commit.update` and is expected to be a base64-encoded Yjs update.
- For `text/plain` documents, the server decodes and applies the update to the in-memory document body.
- If `parent_cid` is provided and there is an existing document head, the server will create a merge commit and advance the head to the merge.

Response (JSON):

```json
{
  "cid": "<edit or normal commit cid>",
  "merge_cid": "<merge cid, only for merge workflow>"
}
```

Status codes:

- `200 OK` on success
- `404 Not Found` if the document ID does not exist
- `400 Bad Request` if `verb` is not `"update"`
- `409 Conflict` if the new commit cannot be added without violating monotonic descent
- `500 Internal Server Error` on store failures

Example (simple commit):

```bash
curl -X POST http://127.0.0.1:3000/docs/<uuid>/commit \
  -H 'Content-Type: application/json' \
  -d '{"verb":"update","value":"AAEC...","author":"jes","message":"first"}'
```

Example (merge workflow):

```bash
curl -X POST http://127.0.0.1:3000/docs/<uuid>/commit \
  -H 'Content-Type: application/json' \
  -d '{"verb":"update","value":"AAEC...","author":"jes","parent_cid":"<some prior cid>"}'
```

## SSE (placeholder)

### `GET /sse/documents/:id`

Returns a `text/event-stream` that emits a `heartbeat` event every ~30s.

Example:

```bash
curl -N http://127.0.0.1:3000/sse/documents/<uuid>
```
