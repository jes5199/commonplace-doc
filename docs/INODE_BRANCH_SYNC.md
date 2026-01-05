# Inode-Branch Sync Proposal

## Summary

This proposal makes the sync layer resilient to concurrent local writers by
treating each inode as its own "branch head." When the sync layer replaces a
file via temp+rename, it keeps a hardlink to the old inode and continues to
observe it. Late writes to old file descriptors become mergeable commits rather
than silent data loss.

## Problem

The current file bridge is a two-way sync between a Unix file and a CRDT-backed
document. When both sides write without coordination:

- Local processes can write through long-lived file descriptors after the sync
  layer replaces the path, so their writes go to an unlinked inode.
- The sync layer no longer sees those writes, leading to lost edits and stale
  read-modify-write loops.

## Goals

- Preserve all local writes, including writes to old inodes.
- Allow server-to-local writes to use atomic rename.
- Convert local writes into CRDT commits that can merge cleanly.
- Keep file watchers reliable across replace events.

## Non-Goals

- Full prevention of conflicting human edits in a shared file.
- Cross-filesystem linking (hardlinks only work on the same filesystem).
- Perfect portability across OSes without fallback paths.

## Design Overview

### Inode tracking model

Each inode is treated as a logical "branch":

- `inode_id` = `(dev, ino)`
- `base_head_cid` = document head at the time this inode became current
- `last_hash` = hash of last known content
- `open_count` = inotify-derived reference count
- `last_write` = last observed write timestamp

Each tracked inode gets a stable hardlink:

```
.commonplace/.inodes/<dev>_<ino>
```

The hardlink keeps the inode addressable even after the main path is replaced.

### Server -> local write path

1. Fetch head content.
2. Write to a temp file.
3. Add inotify watch to the temp file inode.
4. Rename temp file into place.
5. Keep or create a hardlink for the old inode and continue watching it.

This keeps the new inode observable from the moment it exists and keeps the old
inode observable after rename.

### Local -> server write path

1. Watch each inode for `IN_MODIFY` and `IN_CLOSE_WRITE`.
2. When content changes, create a commit whose parent is that inode's
   `base_head_cid`.
3. Merge inode branches on the server using CRDT semantics.

Inode branches act like parallel commits in the Merkle tree, preserving edits
from stale descriptors.

## Inotify details

- Watches are inode-based; a watch added before rename follows the inode after
  rename.
- Use `IN_OPEN`, `IN_CLOSE_WRITE`, and `IN_CLOSE_NOWRITE` to maintain a best-
  effort `open_count`.
- On `IN_Q_OVERFLOW`, treat counts as unreliable and fall back to `/proc`
  scanning or conservative timeouts.

## Garbage Collection

Old inodes are removed once they are inactive:

- `open_count == 0`
- `now - last_write > grace_period`

If counts are unreliable (inotify overflow), confirm via `/proc/*/fd` scanning
before unlinking the hardlink. If `/proc` is unavailable or too expensive,
increase the grace period and rely on size/time heuristics.

## Merge Semantics

Commits from different inodes may conflict. The CRDT/Yjs model should merge
these updates; the server remains authoritative for resolution and produces a
canonical head.

## Failure Modes and Mitigations

- **Writes to old inode are missed:** ensure a hardlink exists and its inode
  is watched; keep a fallback scan for changes.
- **Inotify overflow:** degrade to slower inode scan or pause GC.
- **Long-lived open fds:** handled by keeping inode watchers and hardlinks.
- **Cross-filesystem files:** detect and disable inode-branch mode for those
  files (hardlinks cannot be created).

## Implementation Touchpoints

- `src/sync/sse.rs`: switch to temp+rename, attach inode watcher before rename.
- `src/sync/watcher.rs`: add per-inode watches and maintain open_count state.
- `src/sync/file_sync.rs`: track inode state and map local changes to branch
  commits.
- `src/sync/state.rs` and `src/sync/state_file.rs`: persist inode branch state.

## Open Questions

- What is the minimal metadata needed to map inode branches back to document
  heads for clean merges?
- Should inode-branch mode be enabled globally or opt-in per sync session?
- What grace period best balances safety and inode cleanup overhead?
