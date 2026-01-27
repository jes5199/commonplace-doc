# Cyan Sync Protocol Specification

## Overview

The cyan sync protocol enables merkle-tree reconciliation between clients and the doc store. Each document has a commit DAG (directed acyclic graph) where commits reference parent commits by CID (content-addressed hash).

This protocol extends the existing MQTT topic structure defined in `docs/MQTT.md`, specifically the cyan (sync) port at `{path}/sync/{client-id}`.

## Roles

- **Doc store**: Authoritative holder of commit history. Responds to sync requests, persists commits, sends acks.
- **Client**: Holds local HEAD and possibly unpushed commits. Initiates sync, creates merge commits when diverged.

## Invariants

- The doc store's HEAD is always reachable from any commit it has persisted
- Clients must have all ancestors of their local HEAD before pushing
- Merge commits are only created by clients, never by the doc store
- The edits topic is the single source of new commits; sync topic is read-only for history

## Sync Triggers

A client initiates sync when:

1. **Subscribe**: Client connects to document, compares retained message HEAD to local HEAD
2. **Unknown parent**: Client receives edit with parent CID it doesn't have
3. **Push rejected**: Client's commit rejected with `missing_parents` error

## HEAD Advancement Rules

The doc store advances HEAD when a new commit satisfies either:

1. **Fast-forward**: New commit is a direct descendant of current HEAD
2. **Merge**: New commit is a merge that includes current HEAD as an ancestor

Commits that don't advance HEAD are still persisted (building up a divergent branch) but HEAD stays unchanged until a merge reconciles the branches.

Example:
```
Initial: HEAD = A

Client 1 pushes B (parent: A) → HEAD = B (fast-forward)
Client 2 pushes C (parent: A) → C persisted, HEAD stays B (divergent)
Client 2 pushes M (parents: B, C) → HEAD = M (merge includes B)
```

## HEAD Discovery

Clients learn the server's HEAD through two mechanisms:

1. **Retained message (cache)**: The edits topic retained message contains the current HEAD CID. Zero round-trip in the common case.
2. **Explicit request (authoritative)**: When in doubt (diverged, stale cache, recovery), client sends `head` request to confirm server state.

## Message Formats

### Client → Server (sync topic)

**Request server's current HEAD:**
```json
{ "type": "head", "req": "r-001" }
```

**Request specific commits by CID:**
```json
{ "type": "get", "req": "r-002", "commits": ["abc123", "def456"] }
```

**Find common ancestor and get missing commits:**
```json
{ "type": "pull", "req": "r-003",
  "have": ["clientHead", "ancestor1", "ancestor2"],
  "want": "HEAD" }
```

The `have` array should include the client's HEAD plus recent ancestors. The server finds the first commit in this set that exists in its history (the common ancestor) and returns all commits from there to `want`.

### Server → Client (sync topic)

**HEAD response:**
```json
{ "type": "head", "req": "r-001", "commit": "abc123" }
```

**Commit data:**
```json
{ "type": "commit", "req": "r-003",
  "id": "def456",
  "parents": ["abc123"],
  "data": "<base64>",
  "timestamp": 1704067200000,
  "author": "client-1" }
```

**Pull complete (includes common ancestor found):**
```json
{ "type": "done", "req": "r-003",
  "commits": ["def456", "ghi789"],
  "ancestor": "abc123" }
```

**Acknowledgment after persisting commit:**
```json
{ "type": "ack", "req": "e-001", "commit": "xyz789",
  "accepted": true, "head_advanced": true }
```

```json
{ "type": "ack", "req": "e-001", "commit": "xyz789",
  "accepted": true, "head_advanced": false }
```

### Client → All (edits topic)

**New commit (includes correlation ID for ack):**
```json
{ "type": "edit", "req": "e-001",
  "cid": "xyz789",
  "parent_cid": "def456",
  "update": "<base64>",
  "author": "client-1" }
```

## Structured Errors

All errors include a structured `error` object so clients can programmatically recover.

**Missing parent(s) - client should push these commits first:**
```json
{ "type": "ack", "req": "e-001", "commit": "xyz789",
  "accepted": false,
  "error": {
    "code": "missing_parents",
    "parents": ["def456", "abc123"]
  }
}
```

**CID mismatch - computed hash doesn't match claimed CID:**
```json
{ "type": "ack", "req": "e-001", "commit": "xyz789",
  "accepted": false,
  "error": {
    "code": "cid_mismatch",
    "expected": "xyz789",
    "computed": "aaa111"
  }
}
```

**Malformed commit - validation failed:**
```json
{ "type": "ack", "req": "e-001", "commit": "xyz789",
  "accepted": false,
  "error": {
    "code": "invalid_commit",
    "field": "parents",
    "message": "parents must be an array"
  }
}
```

**Commit not found (for sync requests):**
```json
{ "type": "error", "req": "r-002",
  "error": {
    "code": "not_found",
    "commits": ["nonexistent123"]
  }
}
```

**No common ancestor found in have set:**
```json
{ "type": "error", "req": "r-003",
  "error": {
    "code": "no_common_ancestor",
    "have": ["abc", "def"],
    "server_head": "xyz"
  }
}
```

### Client Recovery Actions

| Error Code | Recovery |
|------------|----------|
| `missing_parents` | Push the listed parent commits first, then retry |
| `cid_mismatch` | Bug - log and alert, don't retry |
| `invalid_commit` | Bug - fix and retry |
| `not_found` | Commit doesn't exist on server; client has stale reference |
| `no_common_ancestor` | Client's have set too shallow; retry with more ancestors |

## Sync Flow State Machine

```
                    ┌─────────────┐
                    │   IDLE      │
                    └──────┬──────┘
                           │ subscribe / HEAD mismatch detected
                           ▼
                    ┌─────────────┐
                    │  COMPARING  │ read retained msg or send head request
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │ IN_SYNC  │    │ PULLING  │    │ DIVERGED │
    └────┬─────┘    └────┬─────┘    └────┬─────┘
         │               │               │
         │               │ recv commits  │ send pull, recv commits
         │               ▼               ▼
         │          ┌──────────┐    ┌──────────┐
         │          │ APPLYING │    │ MERGING  │ create merge commit
         │          └────┬─────┘    └────┬─────┘
         │               │               │
         │               ▼               ▼
         │          ┌─────────────────────────┐
         │          │        PUSHING          │ publish to edits topic
         │          └───────────┬─────────────┘
         │                      │ recv acks
         │                      ▼
         │               ┌─────────────┐
         └──────────────►│    IDLE     │
                         └─────────────┘
```

### State Descriptions

| State | Description |
|-------|-------------|
| IDLE | No sync in progress. Listening for edits. |
| COMPARING | Determining relationship between local and server HEAD |
| IN_SYNC | HEADs match, nothing to do |
| PULLING | Fetching commits from server (client is behind) |
| DIVERGED | Both sides have commits the other lacks |
| MERGING | Creating merge commit locally (Yjs handles CRDT merge) |
| APPLYING | Applying received commits to local Y.Doc |
| PUSHING | Publishing local commits, waiting for acks |

### Error Transitions

- `missing_parents` ack → PUSHING (push parents first, retry)
- `no_common_ancestor` → COMPARING (retry with deeper have set)
- Network failure → IDLE (will retry on reconnect)

## Merge Commit Creation

When the client detects divergence (local HEAD A, server HEAD B, neither is ancestor of other), it creates a merge commit.

### Steps

1. Client sends `pull` with `have: [A, A's ancestors...]`
2. Server finds common ancestor X, returns commits from X to B
3. Client applies server commits to a temporary Y.Doc:
   - Start from state at X
   - Apply commits X→B in order
4. Client's local Y.Doc has state at A
5. Client computes Yjs state vector diff and merges:
   - `Y.applyUpdate(localDoc, Y.encodeStateAsUpdate(serverDoc))`
6. Client creates merge commit M:
   ```json
   {
     "parents": ["A", "B"],
     "update": "<Yjs delta from merged state>",
     "timestamp": <now>,
     "author": "<client-id>",
     "message": "merge"
   }
   ```
7. CID of M is computed from its content
8. Client publishes M to edits topic
9. Server receives M, verifies both parents exist, persists, advances HEAD to M

### Merge Commit Update Contents

The `update` field in a merge commit is usually **empty** because Yjs CRDT semantics handle the merge automatically. The merge commit simply records that both branches are now unified.

The `update` may be non-empty if the client makes additional edits during merge resolution, but this is rare.

## Race Conditions and Edge Cases

### New edit arrives while syncing

Client is in PULLING state, receives an edit from another client on the edits topic.

**Resolution:**
- Buffer incoming edits during sync
- After sync completes, check if buffered edits extend the new HEAD
- If yes, apply them (fast-forward)
- If no (they're based on old HEAD), the next sync cycle will handle it

### Two clients create merge commits simultaneously

Both Client 1 and Client 2 detect divergence, both create merge commits M1 and M2 with same parents [A, B].

**Resolution:**
- First merge to arrive advances HEAD (say M1)
- Second merge (M2) is persisted but `head_advanced: false`
- Client 2 sees M2 didn't advance HEAD, does another sync
- Client 2 finds M1, creates new merge M3 with parents [M1, M2]
- Eventually converges

### Client pushes while server HEAD moves

Client pushes commit C with parent=A, but server HEAD moved A→B while in flight.

**Resolution:**
- Server persists C (parent A exists) but `head_advanced: false`
- Client receives ack, sees HEAD didn't advance
- Client syncs, discovers B, creates merge

### Client has commits server lost (disaster recovery)

Server restored from backup, missing recent commits that client has.

**Resolution:**
- Client's push will fail with `missing_parents` (server doesn't have parent)
- Client walks back, pushes ancestors until one is accepted
- Rebuilds server's history from client's local state

## Relationship to Existing Protocol

This specification builds on the existing cyan sync port defined in `docs/MQTT.md`. Key additions:

1. **Structured errors** with recovery actions
2. **Ack messages** on client sync topic after commit persistence
3. **`head_advanced` field** to signal whether client needs to merge
4. **`ancestor` field** in `done` message to communicate common ancestor found
5. **State machine** formalizing client sync behavior

The existing message types (`head`, `get`, `pull`, `ancestors`, `commit`, `done`) remain compatible. The `error` message gains structured error objects.

## Implementation Notes

- Clients should include at least 10-20 ancestors in the `have` set for `pull` requests
- If `no_common_ancestor` error occurs, double the `have` set depth and retry
- Acks use the same `req` correlation ID from the original `EditMessage`
- The doc store must subscribe to each client's sync topic to send acks
