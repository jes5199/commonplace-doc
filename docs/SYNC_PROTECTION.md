# Sync Protection: Shadow Hardlinks and Flock

This document explains how commonplace sync protects against data loss when
multiple processes edit the same file concurrently.

## The Problem

When syncing files bidirectionally between disk and server, race conditions
can cause data loss:

1. **The Atomic Write Problem**: Sync uses atomic writes (temp + rename) to
   prevent inotify watchers from seeing partial content. But this changes the
   file's inode. If an agent (like bartleby) has the file open, it continues
   writing to the old inode while sync watches the new one. The agent's edits
   are lost.

2. **The Overwrite Problem**: If sync receives a server update while a local
   upload is in flight, it might overwrite local changes with stale server
   content, losing the local edit.

3. **The Concurrent Edit Problem**: If an agent is actively writing to a file
   when sync tries to write server content, the writes can interleave and
   corrupt the file.

## Three-Layer Defense

Commonplace uses three complementary mechanisms:

```
┌─────────────────────────────────────────────────────────────┐
│                    Shadow Hardlinks                          │
│         Detect writes to old inodes after atomic rename      │
├─────────────────────────────────────────────────────────────┤
│                    Flock Protection                          │
│         Coordinate with agents via advisory file locks       │
├─────────────────────────────────────────────────────────────┤
│                   Ancestry Checking                          │
│         Ensure causal ordering of CRDT commits               │
└─────────────────────────────────────────────────────────────┘
```

## Shadow Hardlinks

Shadow hardlinks solve the atomic write problem by preserving old inodes.

### How It Works

When sync performs an atomic write:

1. **Before rename**: Create a hardlink from the current file's inode to a
   shadow directory (`.commonplace-shadow/`)
2. **Atomic rename**: Replace the file with new content (new inode)
3. **Track**: Record the old inode's commit ID for later CRDT merge

The hardlink points to the same data blocks on disk as the original file.
If an agent continues writing to the old inode, the writes go to those blocks.

### Shadow Write Detection

A shadow watcher task monitors the shadow directory for modifications:

```
Agent writes to old inode
        ↓
Shadow hardlink shows modification (same inode)
        ↓
shadow_watcher_task detects IN_MODIFY event
        ↓
Read content from shadow file
        ↓
Create CRDT update with OLD commit_id as parent
        ↓
POST /replace → server merges via CRDT
        ↓
Agent's edits preserved!
```

### Inode Tracking

The `InodeTracker` maintains state for each active inode:

```rust
struct InodeState {
    primary_path: PathBuf,      // Where this inode lives
    commit_id: Option<String>,  // Last known CID for this content
    shadow_path: Option<PathBuf>, // Hardlink in shadow directory
    last_shadow_write: Option<Instant>,
}
```

Shadow files are named by hex-encoded device + inode: `{dev}-{ino}`

### Garbage Collection

Shadow hardlinks are cleaned up when:
- Idle for > 1 hour (no writes detected)
- Age > 5 minutes (give agent time to finish)

## Flock Protection

Flock provides advisory locking to coordinate with cooperative agents.

### Lock Types

**Exclusive (LOCK_EX)**: Used by sync before writing server content
- If agent holds LOCK_EX, sync waits (up to 30 second timeout)
- Prevents overwriting while agent is actively editing

**Shared (LOCK_SH)**: Used by sync when reading file content
- Multiple readers allowed simultaneously
- Signals intent to read without blocking long

### Write Flow

```rust
// Before writing server content:
match try_flock_exclusive(&path, Some(FLOCK_TIMEOUT)).await {
    FlockResult::Acquired(guard) => {
        // Safe to write, agent not editing
        write_content(&path, content)?;
        // Lock released when guard drops
    }
    FlockResult::Timeout => {
        // Agent held lock for 30 seconds
        // Proceed anyway (agent's responsibility to handle)
        write_content(&path, content)?;
    }
    FlockResult::NotFound => {
        // File deleted, create new
        create_file(&path, content)?;
    }
}
```

### Agent Responsibilities

Agents that want flock protection must:
1. Open the file with `flock(fd, LOCK_EX)` before writing
2. Hold the lock for the duration of their edit session
3. Release the lock when done

If an agent doesn't use flock, sync will proceed after timeout.

## Ancestry Checking

Ancestry checking ensures CRDT commits are applied in causal order.

### The Problem

```
Time →
Local:    [edit A] ─────────────── upload A ──┐
                                              ↓
Server:   [has B from other client] ─────────────→ [merge A+B = C]
                                              ↑
SSE:      ←──── receive B ───────────────────┘

Without ancestry check:
  SSE receives B before A merges
  Writes B to disk, overwrites local edit
  Upload sends A, but local file now has B
  Lost: local edit A
```

### FlockSyncState

Tracks pending outbound commits per file:

```rust
struct PathState {
    pending_outbound: HashSet<String>,  // CIDs we uploaded but not confirmed
    pending_inbound: Option<InboundWrite>, // Queued server write
}
```

### The Check

When SSE receives a server edit:

1. Check if we have pending outbound commits for this file
2. If yes, call `/is-ancestor` API for each pending CID
3. If ALL pending commits are ancestors of server commit → safe to write
4. If NOT all ancestors → queue the write, wait for merge

```rust
async fn handle_server_edit_with_flock(...) {
    let pending = flock_state.get_pending_outbound(&path).await;

    if !pending.is_empty() {
        let all_confirmed = all_are_ancestors(
            client, server, doc_id, &pending, &server_cid
        ).await?;

        if !all_confirmed {
            // Our uploads not merged yet, queue this write
            flock_state.queue_inbound(&path, content, server_cid).await;
            return;
        }

        // All our commits are ancestors, confirm them
        flock_state.confirm_outbound(&path, &pending.iter().collect()).await;
    }

    // Safe to write
    write_inbound_with_checks(...).await;
}
```

### Processing Queued Writes

When a pending commit is confirmed (appears as ancestor in later SSE):

```rust
if let Some((content, cid)) = process_pending_inbound_after_confirm(&flock_state, &path).await {
    // We had a queued write, now safe to apply
    write_content(&path, &content)?;
}
```

## Complete Data Flow

### Outbound (Local → Server)

```
File modified on disk
        ↓
file_watcher_task detects change
        ↓ (acquire LOCK_SH to read)
Capture content, compute diff
        ↓
upload_task creates CRDT update
        ↓
POST /replace with parent CID
        ↓
record_outbound(path, new_cid)  ← Track pending
        ↓
Server confirms, SSE will receive merged commit
```

### Inbound (Server → Local)

```
SSE receives "edit" event
        ↓
Fetch HEAD for content + CID
        ↓
Check ancestry: all pending confirmed?
        ├─ NO → queue_inbound(), wait
        └─ YES → confirm_outbound(), continue
        ↓
Try acquire LOCK_EX
        ├─ Acquired → write (agent not editing)
        ├─ Timeout → write anyway (agent responsible)
        └─ NotFound → create file
        ↓
atomic_write_with_shadow()
        ↓ Create shadow hardlink for old inode
        ↓ Write temp file + fsync
        ↓ Atomic rename
        ↓ Track new inode
```

### Shadow Recovery

```
atomic_write replaces inode
        ↓
Agent still has old fd open, writes to old inode
        ↓
shadow_watcher detects write on hardlink
        ↓
handle_shadow_write()
        ↓ Look up old commit_id from InodeState
        ↓ Read content from shadow file
        ↓
POST /replace with old commit_id as parent
        ↓
Server merges via CRDT
        ↓
Agent's edits preserved!
```

## Configuration

### Timeouts

- **Flock timeout**: 30 seconds (`FLOCK_TIMEOUT`)
- **Flock retry interval**: 100ms (`FLOCK_RETRY_INTERVAL`)
- **Shadow GC idle threshold**: 1 hour
- **Shadow GC minimum age**: 5 minutes

### Shadow Directory

Located at `{sync_dir}/.commonplace-shadow/`

Files named: `{device_hex}-{inode_hex}`

### Enabling Flock Protection

Flock protection is enabled by using `spawn_file_sync_tasks_with_flock()`
instead of `spawn_file_sync_tasks()`. This is now the default in both
directory mode and exec (sandbox) mode.

## Debugging

### Check if Flock is Active

Look for log messages:
```
"recorded pending outbound commit" → FlockSyncState tracking uploads
"confirmed outbound commits" → Ancestry check passed
"Ancestry check failed, skipping server edit" → Write queued
"Flock timeout" → Agent held lock too long
```

### Check Shadow Activity

```
"Shadow hardlink created" → Old inode preserved
"Shadow write detected" → Agent wrote to old inode
"Merging shadow write" → CRDT merge in progress
```

### Common Issues

**"Ancestry check failed" repeatedly**:
- Upload may be failing silently
- Check server logs for commit errors

**No flock logs at all**:
- Verify `spawn_file_sync_tasks_with_flock` is being called
- Check that `FlockSyncState` is created and passed

**Data still lost despite protections**:
- Agent may not be using flock (only advisory)
- Check if agent opens file with O_APPEND or similar
- Verify shadow directory exists and is writable

## Related Files

- `src/sync/flock.rs` - Flock acquisition with retry
- `src/sync/flock_state.rs` - FlockSyncState, PathState, pending tracking
- `src/sync/sse.rs` - atomic_write_with_shadow, write_inbound_with_checks
- `src/sync/watcher.rs` - shadow_watcher_task, ShadowWriteEvent
- `src/sync/ancestry.rs` - all_are_ancestors, is_ancestor
- `src/sync/state.rs` - InodeTracker, InodeState
- `src/sync/file_sync.rs` - spawn_file_sync_tasks_with_flock
