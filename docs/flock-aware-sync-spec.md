# Flock-Aware Sync Spec

## Problem

Sync atomically replaces files while agents hold them open. This causes:
- Agents reading stale state (minor, CRDT handles)
- Agent edits getting lost or misattributed (major)
- Resurrection of deleted content (major)

## Solution

Sync honors flock. Delays incoming writes until local edits are included in incoming state.

## Invariant

**Never overwrite a file with content that doesn't include all local edits made to that file.**

## Sync State Per Path

```rust
struct PathState {
    current_inode: u64,
    pending_outbound: Option<CommitId>,  // edit uploaded, awaiting confirmation
    pending_inbound: Option<InboundWrite>,  // server update waiting to land
}

struct InboundWrite {
    content: Bytes,
    commit_id: CommitId,
}
```

## Flow: Outbound Edit

```
1. Sync detects file change on inode X
2. Sync reads content, gets commit ID for inode X
3. Sync uploads: {content, base_commit_id}
4. Sync sets pending_outbound = Some(new_commit_id)
5. Server confirms commit landed
6. Sync clears pending_outbound
```

## Flow: Inbound Update

```
1. Server sends update: {content, commit_id}
2. If pending_outbound is Some(local_commit):
   a. Check: is local_commit ancestor of incoming commit_id?
   b. If yes: safe to write (our edit is included)
   c. If no: queue as pending_inbound, wait
3. If no pending_outbound:
   a. Proceed to write
```

## Flow: Atomic Replace with Flock

```
1. Sync wants to write to path
2. Try flock(path, LOCK_EX | LOCK_NB)  // non-blocking
3. If locked:
   a. Someone is editing
   b. Queue as pending_inbound
   c. Subscribe to inotify on that inode
   d. When edit detected: upload it first
   e. Wait for merge result that includes local edit
   f. Then proceed to write
4. If lock acquired:
   a. Write content to temp file
   b. Atomic rename temp -> path
   c. Update inode tracking
   d. Release flock
```

## Flow: Processing Pending Inbound

```
1. Local edit uploaded, server confirms
2. Check pending_inbound:
   a. Is pending_inbound.commit_id descendant of our edit?
   b. If yes: write it now
   c. If no: still waiting for merged result from server
3. New server update arrives:
   a. Replace pending_inbound with newer update
   b. Re-check ancestry
```

## Flock Protocol Between Sync and Agents

```
Agent (e.g., Bartleby):
  flock(fd, LOCK_EX)      // I'm reading and writing
  ... read, process, write ...
  flock(fd, LOCK_UN)      // Done

Sync:
  flock(path, LOCK_EX)    // Wait for agent to finish
  ... atomic rename ...
  flock(path, LOCK_UN)    // Agents can proceed
```

## Edge Case: Agent Holds Lock Forever

```
Timeout after N seconds
Log warning
Proceed anyway (agent's problem)
Or: LOCK_NB + retry loop with backoff
```

## Edge Case: Rapid Edits

```
Agent makes edit A
Sync uploads A
Agent makes edit B before A confirmed
Sync uploads B
Server returns merge(A, B, server_state)
Sync writes result

pending_outbound could be a Set<CommitId> not Option
Wait until ALL pending are ancestors of incoming
```

## Edge Case: Path Deleted

```
If path doesn't exist during inbound:
  Create it (no flock needed, no contention)

If path deleted during outbound:
  Upload deletion
  Clear pending_outbound when confirmed
```

## Inode Tracking Integration

```
When atomic rename happens:
  old_inode = stat(path).st_ino before rename
  new_inode = stat(path).st_ino after rename
  
  hardlink .inodes/{old_inode} (shadow)
  watch old_inode for stragglers
  associate new_inode with new commit_id
  
When old_inode gets write:
  upload with old_inode's commit_id
  merge happens server-side
```

## Summary

```
Sync respects flock:        Serializes with agents
Sync tracks pending edits:  Knows what's in flight
Sync checks ancestry:       Won't overwrite with stale
Inode tracking:             Each inode knows its base commit

Result: Agents never see state that excludes their recent edits
```
