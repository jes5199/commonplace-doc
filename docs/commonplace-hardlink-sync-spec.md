# Hardlink Shadow Strategy for Non-Atomic Write Tolerance

## Problem

When syncing a file, Commonplace performs an atomic write (write to temp, rename to target). This replaces the inode at the path. If another process has an open write fd to the old inode and is doing a slow/non-atomic write, those writes are lost—they go to an orphaned inode that nothing is watching.

## Solution

Before replacing a file during server sync, hardlink the old inode to a shadow directory. Continue watching the shadow. Writes to the old inode become edits against an older commit, resolved via CRDT merge.

## Data Structures

```
shadow_dir: <sync-root>/.commonplace-shadow/  (default; user-configurable)

InodeKey: (DevId, InodeId)  # (st_dev, st_ino) — inode numbers are only unique per-device

inode_state: Map<InodeKey, {
  commit_id: CommitId,           # last-written commit for this inode
  shadow_path: Path,             # path to hardlink in shadow_dir ({devHex}-{inoHex})
  shadowed_at: Timestamp,        # when we moved this inode to shadow
  last_write: Timestamp,         # last IN_CLOSE_WRITE or IN_MODIFY seen
}>
```

## Operations

### On Server Update (file at path P needs new content)

1. `fd = open(P)` and `fstat(fd)` to get (st_dev, st_ino) as I_old
2. If I_old is tracked:
   - Link via `/proc/self/fd/{fd}` → `shadow_dir/{devHex}-{inoHex}` (TOCTOU-proof)
   - Update `inode_state[I_old].shadow_path`
   - Update `inode_state[I_old].shadowed_at = now()`
   - Ensure shadow_dir/ is watched; handle per-entry events
3. `close(fd)`
4. Atomic write: write temp file, `rename(temp, P)` — creates new inode I_new
5. `inode_state[I_new] = { commit_id: new_commit, shadow_path: null, ... }`

### On Write to Shadow (IN_MODIFY or IN_CLOSE_WRITE on shadow_dir/{devHex}-{inoHex})

1. Look up `inode_state[inode]`
2. Read content from shadow path
3. Create Yjs update as edit against `inode_state[inode].commit_id`
4. Apply update (CRDT merge into HEAD), producing new commit C
5. Update `inode_state[inode].commit_id = C`
6. Update `inode_state[inode].last_write = now()`

### On Write to Primary Path (existing logic, updated)

Detect atomic vs non-atomic writes:
- **Non-atomic write** (same inode): Diff content against `inode_state[inode].commit_id`, sync, update commit_id.
- **Atomic write** (inode changed via rename): Diff new content against `inode_state[old_inode].commit_id`, sync, create `inode_state[new_inode]` with new commit_id.

### Garbage Collection

Shadow inodes are eligible for cleanup when:
- `now() - last_write > IDLE_TIMEOUT` (e.g., 1 hour), AND
- `now() - shadowed_at > MIN_SHADOW_LIFETIME` (e.g., 5 minutes)

On cleanup:
- `unlink(shadow_path)`
- Remove from `inode_state`

If the inode has no other links and no open fds, it gets reclaimed by the filesystem.

## Inotify Events

| Location | Event | Action |
|----------|-------|--------|
| Watched dir | IN_CREATE | Check if atomic write completion (rename from temp) |
| Watched dir | IN_MOVED_TO | Atomic write or user rename |
| Watched dir | IN_DELETE | Path removed; shadow keeps inode alive if hardlinked |
| Primary path | IN_MODIFY | Non-atomic write in progress |
| Primary path | IN_CLOSE_WRITE | Non-atomic write complete, sync content |
| Shadow path | IN_MODIFY | Old inode being written |
| Shadow path | IN_CLOSE_WRITE | Old inode write complete, merge |
| Any | IN_Q_OVERFLOW | Queue overflowed, initiate full rescan |

## Constraints

**Same filesystem**: Hardlinks don't cross filesystem boundaries. The default shadow directory resolves to `<sync-root>/.commonplace-shadow/` so it shares the same filesystem as synced files. Startup fails if the shadow directory cannot be created.

**Inode identity**: Use `InodeKey = (DevId, InodeId)` as the map key, since inode numbers are only unique per-device.

**Inode reuse**: Inodes can be reused after the old file is fully deleted. The `inode_state` entry must be removed on GC to prevent misattribution. Since we hold a hardlink, the inode can't be reused until we release it.

**Shadow dir cleanup**: On startup, clear the entire shadow directory and reset `inode_state`. Stale hardlinks from previous runs are not useful (we don't have their commit_id mapping in memory). No persistence of inode→commit mapping across restarts.

**Directory watching**: Watch directories, not individual files. This is the primary mechanism for detecting creates, renames, and deletes.

**Inotify overflow**: On `IN_Q_OVERFLOW`, inotify's event queue has overflowed and events were dropped. Recovery requires a full rescan of all watched paths to reconcile state.

## Example Timeline

```
T0: File F has inode (dev=0x01, ino=0x3e8), commit C1
T1: Server update arrives with commit C2
T2: Open F, fstat, link via /proc/self/fd/X → shadow/1-3e8
T3: Atomic write: F now has inode (dev=0x01, ino=0x3e9), commit C2
T4: Slow writer (still has fd to ino 0x3e8) writes "hello"
T5: IN_CLOSE_WRITE on shadow/1-3e8
T6: Read shadow/1-3e8, create update against C1, merge into HEAD
T7: Result: HEAD contains both C2 changes and "hello", no conflict
```

## Open Questions

- Should we track open fd count heuristically (count IN_OPEN vs IN_CLOSE_WRITE)? Probably not worth it.
- What's the right idle timeout? 1 hour seems conservative. 10 minutes might be fine.
