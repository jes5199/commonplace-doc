# Flock-Aware Sync Implementation Design

**Issue:** CP-qt6z
**Date:** 2026-01-08
**Status:** Approved

## Overview

Implement the flock-aware sync protocol from `docs/flock-aware-sync-spec.md` so inbound writes never overwrite local edits.

## Key Decisions

- **Ancestry checking:** Server-side endpoint (`GET /docs/{id}/is-ancestor`)
- **Pending outbound:** `HashSet<CommitId>` (not Option) to handle rapid edits
- **Flock timeout:** 30 seconds, then proceed with warning

## Data Structures

```rust
// New file: src/sync/flock_state.rs

pub struct PathState {
    pub current_inode: u64,
    pub pending_outbound: HashSet<CommitId>,
    pub pending_inbound: Option<InboundWrite>,
}

pub struct InboundWrite {
    pub content: Bytes,
    pub commit_id: CommitId,
    pub received_at: Instant,
}
```

Stored in `HashMap<PathBuf, PathState>` within sync task.

## Server-Side Ancestry Endpoint

```
GET /docs/{id}/is-ancestor?ancestor={cid}&descendant={cid}
Returns: { "is_ancestor": true/false }
```

Implementation: BFS backwards from descendant through commit parents.

## Flock Helper

```rust
// New file: src/sync/flock.rs

const FLOCK_TIMEOUT: Duration = Duration::from_secs(30);
const FLOCK_RETRY_INTERVAL: Duration = Duration::from_millis(100);

pub enum FlockResult {
    Acquired(FlockGuard),
    Timeout,
}

pub async fn try_flock_exclusive(path: &Path) -> io::Result<FlockResult>
```

Uses `libc::flock(fd, LOCK_EX | LOCK_NB)` with retry loop until timeout.

## Inbound Write Flow

```rust
async fn write_inbound_update(...) -> Result<WriteOutcome> {
    // 1. If path doesn't exist, just create (no contention)
    if !path.exists() {
        tokio::fs::write(path, content).await?;
        return Ok(WriteOutcome::Written);
    }

    // 2. Check pending_outbound ancestry
    if !path_state.pending_outbound.is_empty() {
        if !path_state.outbound_included_in(commit_id, client).await? {
            path_state.pending_inbound = Some(InboundWrite { ... });
            return Ok(WriteOutcome::Queued);
        }
    }

    // 3. Try flock
    match try_flock_exclusive(path).await? {
        FlockResult::Acquired(guard) => {
            atomic_write_with_shadow(path, content).await?;
        }
        FlockResult::Timeout => {
            tracing::warn!("flock timeout, proceeding anyway");
            atomic_write_with_shadow(path, content).await?;
        }
    }

    Ok(WriteOutcome::Written)
}
```

## Outbound Upload Flow

```rust
async fn upload_local_edit(...) -> Result<()> {
    let new_commit_id = client.commit_edit(doc_id, content).await?;
    path_state.pending_outbound.insert(new_commit_id);
    Ok(())
}
```

## SSE Event Handler

```rust
async fn handle_sse_commit(...) -> Result<()> {
    // 1. Clear confirmed pending_outbound
    let mut confirmed = Vec::new();
    for pending in &path_state.pending_outbound {
        if client.is_ancestor(pending, incoming_cid).await? {
            confirmed.push(pending.clone());
        }
    }
    for cid in confirmed {
        path_state.pending_outbound.remove(&cid);
    }

    // 2. Try to write inbound update
    write_inbound_update(...).await?;

    // 3. If pending_outbound now empty, process queued inbound
    if path_state.pending_outbound.is_empty() {
        if let Some(queued) = path_state.pending_inbound.take() {
            write_inbound_update(..., &queued.content, &queued.commit_id, ...).await?;
        }
    }

    Ok(())
}
```

## Edge Cases

**New inbound while one queued:** Replace with newer (newer server state supersedes).

**Path deleted locally:** Clear pending_inbound, deletion wins.

**Stale pending_inbound:** Only replace if incoming is descendant of existing.

## Files to Modify

- `src/api.rs` - Add `/docs/{id}/is-ancestor` endpoint
- `src/replay.rs` - Add `is_ancestor()` function
- `src/sync/flock.rs` - New file, flock helper
- `src/sync/flock_state.rs` - New file, PathState structs
- `src/sync/file_sync.rs` - Integrate flock + pending state
- `src/sync/sse.rs` - Update SSE handler for pending tracking
- `src/lib.rs` - Wire up new endpoint

## Test Cases

1. **Inbound waits for pending outbound** - SSE update queued until local edit confirmed
2. **Flock blocks write** - Write waits while agent holds lock
3. **Flock timeout proceeds** - After 30s, write anyway with warning
4. **Rapid edits all confirmed** - Multiple pending_outbound all must be ancestors
5. **Deletion stays deleted** - Inbound ignored after local delete
6. **is_ancestor unit tests** - Linear chain, branching, same commit
