# Unify CID Tracking with Shadow Inode System

**Issue**: CP-bsjy
**Date**: 2025-01-11
**Status**: Approved

## Problem

Currently there are multiple places tracking commit IDs (CIDs):

1. `InodeState.commit_id` - runtime, per inode, lost on restart
2. `SyncState.last_written_cid` - runtime, per file task
3. `FileState.last_cid` - persisted, per path

This creates redundancy and means InodeTracker starts empty on restart, losing the inode→CID mapping.

## Design

### Core Change

Add `inode_key` to `FileState` in `state_file.rs`:

```rust
pub struct FileState {
    pub hash: String,
    pub last_modified: Option<String>,
    pub last_cid: Option<String>,
    pub inode_key: Option<String>,  // "dev-ino" hex format
}
```

### Responsibilities

**FileState.inode_key** (persisted):
- Tracks the *primary* inode for each path
- Used to initialize InodeTracker on startup
- Survives restarts

**InodeTracker** (runtime):
- Still handles shadow creation when atomic writes happen
- Tracks multiple inodes per path (current + shadows)
- Shadows remain ephemeral - not persisted

### Flow

```
Startup:
  1. Load SyncStateFile
  2. For each file, initialize InodeTracker with (path, inode_key, last_cid)

Runtime:
  3. Atomic write detected → InodeTracker shadows old inode (in-memory)
  4. Writes to shadow → merge using shadow's commit_id

Sync completion:
  5. Update FileState with new inode_key and last_cid
  6. InodeTracker.update_commit() for the new primary inode

Shutdown:
  7. Save SyncStateFile (shadows discarded)
```

### Why Shadows Aren't Persisted

The orchestrator takes down child processes on shutdown/crash. No orphaned file descriptors survive restart, so there's no value in persisting shadow inode state.

## Implementation Tasks

1. Add `inode_key: Option<String>` field to `FileState`
2. Add `update_file_with_inode()` method to `SyncStateFile`
3. Update `mark_synced()` to record current inode
4. Add `InodeTracker::init_from_state_file()` method
5. Call init on startup in `sync.rs`
6. Update integration test to verify inode persistence

## Non-Goals

- Persisting shadow inodes (crash recovery not needed)
- Changing the primary key from path to inode (paths needed for schema lookups)
