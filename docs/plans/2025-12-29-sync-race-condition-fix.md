# CP-r2a: Fix Sync Race Condition

## Problem Statement

When a server edit arrives via SSE, there's a race condition where `upload_task` can send stale file content back to the server, overwriting the server's update.

### Root Cause

The file watcher can trigger from a previous/stale event after `handle_server_edit` updates state but before (or during) the file write. The current echo detection compares file content to `last_written_content`, but this fails when:

1. State is updated to NEW content
2. Stale watcher event fires
3. `upload_task` reads OLD file content
4. OLD != NEW, so it uploads OLD content back to server

### Current Code Locations

- `SyncState` struct: `src/bin/sync.rs:91`
- `file_watcher_task`: `src/bin/sync.rs:1608`
- `upload_task`: `src/bin/sync.rs:1674`
- `handle_server_edit`: `src/bin/sync.rs:1895`
- Echo detection: `src/bin/sync.rs:1704-1711`

## Design: Token-Based Write Barrier

### New State Fields

```rust
struct SyncState {
    // Existing fields
    last_written_cid: Option<String>,
    last_written_content: String,

    // NEW: Token-based barrier
    current_write_id: u64,  // Monotonic counter, incremented on each server write
    pending_write: Option<PendingWrite>,
}

struct PendingWrite {
    write_id: u64,           // Token to identify this specific write
    content: String,         // Content being written
    cid: Option<String>,     // CID of the commit being written
    started_at: Instant,     // For timeout detection
}
```

### handle_server_edit Changes

```rust
async fn handle_server_edit(...) {
    // 1. Read local file content
    let local_content = read_file(&file_path).await?;

    // 2. Fetch HEAD from server
    let head = fetch_head(&client, &server, &node_id).await?;

    // 3. Acquire WRITE lock and set barrier atomically
    {
        let mut s = state.write().await;

        // Check if there's already a pending write (concurrent SSE events)
        if let Some(pending) = &s.pending_write {
            if pending.started_at.elapsed() < Duration::from_secs(30) {
                // Another write in progress, skip this one
                // The SSE will fire again if needed
                debug!("Skipping server edit - another write in progress");
                return;
            }
            // Timeout - clear stale pending
            warn!("Clearing timed-out pending write");
        }

        // Re-check local content under lock (catches edits since step 1)
        let current_content = read_file(&file_path).await?;
        if current_content != local_content {
            debug!("Local file changed during HEAD fetch, aborting server write");
            return;
        }

        // Set barrier with new token
        s.current_write_id += 1;
        s.pending_write = Some(PendingWrite {
            write_id: s.current_write_id,
            content: head.content.clone(),
            cid: head.cid.clone(),
            started_at: Instant::now(),
        });
    }
    // Lock released before I/O

    // 4. Write file (inode-preserving direct write)
    if let Err(e) = tokio::fs::write(&file_path, &head.content).await {
        error!("Failed to write file: {}", e);
        // Clear barrier on failure
        let mut s = state.write().await;
        s.pending_write = None;
        return;
    }

    // 5. DO NOT clear barrier here - upload_task will do it
    // This ensures the watcher event is processed with barrier context

    info!("Wrote server content, waiting for watcher confirmation");
}
```

### upload_task Changes

```rust
async fn upload_task(...) {
    while let Some(_event) = rx.recv().await {
        // 1. Read file content
        let content = read_file(&file_path).await?;

        // 2. Acquire WRITE lock for entire decision+mutation
        let mut s = state.write().await;

        // 3. Check for pending write (barrier is up)
        if let Some(pending) = &s.pending_write {
            // Check for timeout
            if pending.started_at.elapsed() > Duration::from_secs(30) {
                warn!("Pending write timed out, clearing barrier");
                s.pending_write = None;
                s.last_written_content = content.clone();
                // Fall through to normal processing
            } else if content == pending.content {
                // Content matches what we wrote - this is our echo
                info!("Echo detected (content matches pending write)");
                s.last_written_cid = pending.cid.clone();
                s.last_written_content = pending.content.clone();
                s.pending_write = None;
                continue;  // Skip upload
            } else {
                // Content differs from pending - could be:
                // a) Partial write (we're mid-write)
                // b) User edit during our write

                // Release lock and retry after delay
                let pending_content = pending.content.clone();
                let retry_count = 3;
                drop(s);

                let mut is_stable = false;
                for i in 0..retry_count {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let reread = read_file(&file_path).await?;
                    if reread == pending_content {
                        // Content now matches - was partial write
                        is_stable = true;
                        break;
                    }
                    if reread == content {
                        // Content stable but different - user edit
                        break;
                    }
                    // Content still changing, keep waiting
                    content = reread;
                }

                // Re-acquire lock
                let mut s = state.write().await;

                if is_stable || content == s.pending_write.as_ref().map(|p| &p.content).unwrap_or(&String::new()) {
                    // Our write completed
                    if let Some(pending) = s.pending_write.take() {
                        s.last_written_cid = pending.cid;
                        s.last_written_content = pending.content;
                    }
                    continue;  // Skip upload
                }

                // User edited during our write
                info!("User edit detected during server write");
                s.pending_write = None;
                // DON'T update last_written_* - use old parent for CRDT merge
                // Fall through to upload
            }
        }

        // 4. Normal echo detection (no barrier)
        if content == s.last_written_content {
            debug!("Echo detected (content matches last written)");
            continue;
        }

        // 5. Upload with current parent_cid
        let parent_cid = s.last_written_cid.clone();
        drop(s);  // Release lock before network I/O

        // ... existing upload logic ...
        // On success, update last_written_cid and last_written_content
    }
}
```

## Key Design Decisions

### 1. Token-Based Barrier (write_id)
Each server write gets a unique monotonic ID. This prevents clobbering if multiple SSE events arrive - we can tell which write a watcher event corresponds to.

### 2. Retry on Content Mismatch
When content differs from pending during barrier, we don't immediately assume "user edit". We retry several times with delays to handle partial writes. Only if content stabilizes to something different do we treat it as a user edit.

### 3. Single Write Lock
Use write lock for entire decision+mutation to avoid ABA bugs from read→drop→write pattern.

### 4. 30-Second Timeout
If watcher never fires (or dies), timeout clears the stuck barrier and updates state.

### 5. CRDT Merge on Conflict
When user edits during server write, we upload with OLD parent_cid. Server's CRDT merge handles concurrent edits. User's changes are preserved in the merge.

### 6. Skip Concurrent Server Edits
If handle_server_edit is called while barrier is up (rapid SSE events), skip the new edit. The SSE stream will re-notify if needed.

## Edge Cases Handled

| Scenario | Handling |
|----------|----------|
| Stale watcher event during write | Barrier up, content differs, retry, detect stable |
| User edit during write | Retry stabilizes to user content, upload with old parent |
| Partial write detected | Retry until content matches pending |
| Watcher never fires | 30s timeout clears barrier |
| Rapid SSE events | Skip if barrier up, let stream re-notify |
| Write fails | Clear barrier, don't update state |

## Files to Modify

1. `src/bin/sync.rs`:
   - Add `PendingWrite` struct (~line 91)
   - Extend `SyncState` with new fields (~line 91)
   - Update `SyncState::new()` (~line 98)
   - Modify `handle_server_edit` (~line 1895)
   - Modify `upload_task` (~line 1674)
   - Update `FileSyncState` for directory mode (~line 470)

## Testing Strategy

1. **Unit test**: Mock file operations, verify barrier state transitions
2. **Integration test**: Start sync, make server change, verify local file updates
3. **Race test**: Rapid server changes, verify no stale uploads
4. **Manual test**: Edit file while server update in flight, verify merge

## Success Criteria

- [ ] Server→local sync works without uploading stale content
- [ ] User edits during sync are preserved (CRDT merged)
- [ ] Partial writes don't get uploaded
- [ ] No stuck barriers (timeout works)
- [ ] All existing tests pass
- [ ] Codex review passes with no P1 issues
