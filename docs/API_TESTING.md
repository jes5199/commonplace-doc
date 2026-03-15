# Manual API Testing Guide

How to exercise the REST API endpoints using curl. Start the server first:

```bash
cargo build --release
./target/release/commonplace-server --database /tmp/test.redb
```

Server runs on `localhost:5199` by default.

## 1. Document Creation

### Create with different content types

```bash
# JSON (default)
curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"json"}'

# Text
curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"text"}'

# XML
curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"xml"}'

# JSONL (newline-delimited JSON)
curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"jsonl"}'
```

Shorthand names (`text`, `json`, `xml`, `jsonl`) and full MIME types
(`text/plain`, `application/json`, etc.) are both accepted.

### Verify default content

```bash
ID=<uuid-from-above>

# JSON → {}
curl -s http://localhost:5199/docs/$ID

# Text → (empty)
curl -s http://localhost:5199/docs/$ID

# XML → <?xml version="1.0" encoding="UTF-8"?><root/>
curl -s http://localhost:5199/docs/$ID
```

### Verify invalid content_type is rejected

```bash
# Should return 415 Unsupported Media Type
curl -s -w "\nHTTP: %{http_code}\n" -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"foobar"}'
```

## 2. Document Info and HEAD

```bash
# Info includes content_type
curl -s http://localhost:5199/docs/$ID/info | jq .
# → {"id":"...","type":"document","content_type":"application/json"}

# HEAD shows current commit and Yjs state
curl -s http://localhost:5199/docs/$ID/head | jq .
# → {"cid":null,"content":"{}","state":"AAA="}
```

## 3. Replace Content

### First replace (no parent needed)

```bash
# Create a fresh doc
R=$(curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{}')
ID=$(echo "$R" | jq -r '.id')

# Replace content
curl -s -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '{"hello":"world"}'
# → {"cid":"...","edit_cid":"...","summary":{"chars_inserted":...}}

# Verify
curl -s http://localhost:5199/docs/$ID
# → {"hello":"world"}
```

### Subsequent replaces require parent_cid

After the first commit, the server requires `parent_cid` to prevent
blind overwrites:

```bash
# This will fail with 428 Precondition Required
curl -s -w "\nHTTP: %{http_code}\n" -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '{"step":2}'

# Get the current HEAD
CID=$(curl -s http://localhost:5199/docs/$ID/head | jq -r '.cid')

# Pass it as query parameter
curl -s -X POST "http://localhost:5199/docs/$ID/replace?parent_cid=$CID" \
  -H "Content-Type: application/json" -d '{"step":2}'

# Verify
curl -s http://localhost:5199/docs/$ID
# → {"step":2}
```

### Replace with JSON arrays

Arrays work as document content:

```bash
R=$(curl -s -X POST http://localhost:5199/docs -H "Content-Type: application/json" -d '{}')
ID=$(echo "$R" | jq -r '.id')

curl -s -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '[{"name":"Alice"},{"name":"Bob"}]'

curl -s http://localhost:5199/docs/$ID
# → [{"name":"Alice"},{"name":"Bob"}]
```

### Replace text documents

```bash
R=$(curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"text"}')
ID=$(echo "$R" | jq -r '.id')

curl -s -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: text/plain" -d 'Hello World'

curl -s http://localhost:5199/docs/$ID
# → Hello World
```

### Object ↔ Array transitions

A document can switch between object and array content:

```bash
R=$(curl -s -X POST http://localhost:5199/docs -H "Content-Type: application/json" -d '{}')
ID=$(echo "$R" | jq -r '.id')

# Start with object
R1=$(curl -s -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '{"step":1}')
CID=$(echo "$R1" | jq -r '.cid')
curl -s http://localhost:5199/docs/$ID  # → {"step":1}

# Switch to array
R2=$(curl -s -X POST "http://localhost:5199/docs/$ID/replace?parent_cid=$CID" \
  -H "Content-Type: application/json" -d '[10,20,30]')
CID=$(echo "$R2" | jq -r '.cid')
curl -s http://localhost:5199/docs/$ID  # → [10,20,30]

# Switch back to object
curl -s -X POST "http://localhost:5199/docs/$ID/replace?parent_cid=$CID" \
  -H "Content-Type: application/json" -d '{"step":3}'
curl -s http://localhost:5199/docs/$ID  # → {"step":3}
```

## 4. Fork

### Fork a committed document

```bash
# Create and populate
R=$(curl -s -X POST http://localhost:5199/docs -H "Content-Type: application/json" -d '{}')
ID=$(echo "$R" | jq -r '.id')
curl -s -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '{"original":true}' > /dev/null

# Fork it
FORK=$(curl -s -X POST http://localhost:5199/docs/$ID/fork)
FORK_ID=$(echo "$FORK" | jq -r '.id')
echo "$FORK" | jq .

# Verify fork has same content
curl -s http://localhost:5199/docs/$FORK_ID
# → {"original":true}
```

### Fork an uncommitted document

Documents without commits can also be forked:

```bash
R=$(curl -s -X POST http://localhost:5199/docs \
  -H "Content-Type: application/json" -d '{"content_type":"text"}')
ID=$(echo "$R" | jq -r '.id')

curl -s -X POST http://localhost:5199/docs/$ID/fork | jq .
# → {"id":"...","head":"..."}
```

## 5. Delete

```bash
curl -s -X DELETE http://localhost:5199/docs/$ID

# Verify it's gone
curl -s -w "\nHTTP: %{http_code}\n" http://localhost:5199/docs/$ID
# → HTTP: 404
```

## 6. SSE (Server-Sent Events)

### Subscribe to document edits

In one terminal, subscribe:

```bash
curl -s -N http://localhost:5199/sse/docs/$ID
```

In another terminal, make changes:

```bash
curl -s -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '{"live":"update"}'
```

The SSE stream emits `edit` events with the Yjs update, parents,
timestamp, and author:

```
event: edit
data: {"source":"server","commit":{"update":"...","parents":[...],"timestamp":...,"author":"anonymous"}}
```

### Subscribe to commit stream

```bash
# Includes initial history + live updates
curl -s -N "http://localhost:5199/documents/$ID/stream?since=0"
```

Emits `commit` events with doc_id, commit_id, timestamp, and URL.

## 7. Commit History

```bash
# Get all changes for a document
curl -s http://localhost:5199/documents/$ID/changes | jq .

# Get commits with Yjs updates (for replay)
curl -s http://localhost:5199/documents/$ID/commits | jq .

# Get commits since a timestamp
curl -s "http://localhost:5199/documents/$ID/commits?since=1700000000000" | jq .
```

## 8. Error Handling

### Wrong parent_cid → 409 Conflict

```bash
curl -s -w "\nHTTP: %{http_code}\n" \
  -X POST "http://localhost:5199/docs/$ID/replace?parent_cid=0000000000000000000000000000000000000000000000000000000000000000" \
  -H "Content-Type: application/json" -d '{"bad":"parent"}'
# → HTTP: 409
```

### Missing parent_cid on committed doc → 428

```bash
# After at least one commit exists:
curl -s -w "\nHTTP: %{http_code}\n" \
  -X POST http://localhost:5199/docs/$ID/replace \
  -H "Content-Type: application/json" -d '{"no":"parent"}'
# → ParentRequired("current-head-cid")
# → HTTP: 428
```

### Nonexistent document → 404

```bash
curl -s -w "\nHTTP: %{http_code}\n" \
  http://localhost:5199/docs/00000000-0000-0000-0000-000000000000
# → HTTP: 404
```

## 9. Health Check

```bash
curl -s http://localhost:5199/health
# → OK
```

## Cleanup

```bash
# Kill the server
pkill -f commonplace-server

# Remove test database
rm -f /tmp/test.redb /tmp/test.redb.lock
```
