//! File and directory watcher submodule for sync.
//!
//! This module contains all file watching-related functionality:
//! - `watcher`: File and directory watcher tasks using `notify`
//! - `file_events`: Handlers for file creation, modification, and deletion events
//! - `flock`: Unix file locking utilities (Unix only)
//! - `flock_state`: Per-path state for flock-aware sync
//! - `state`: Sync state management including inode tracking

pub mod file_events;
#[cfg(unix)]
pub mod flock;
pub mod flock_state;
pub mod state;
pub mod watcher;

// Re-export from file_events
pub use file_events::{
    ensure_parent_directories_exist, find_owning_document, handle_file_deleted,
    handle_file_modified, OwningDocument,
};

// Re-export from flock (Unix only)
#[cfg(unix)]
pub use flock::{
    try_flock_exclusive, try_flock_shared, FlockGuard, FlockResult, FLOCK_RETRY_INTERVAL,
    FLOCK_TIMEOUT,
};

// Re-export from flock_state
pub use flock_state::FlockSyncState;

// Re-export from state
#[cfg(unix)]
pub use state::{hardlink_from_fd, hardlink_from_path};
pub use state::{InodeKey, InodeState, InodeTracker, PendingWrite, SyncState};

// Re-export from watcher
#[cfg(unix)]
pub use watcher::spawn_shadow_tasks;
pub use watcher::{
    directory_watcher_task, file_watcher_task, shadow_watcher_task, wait_for_file_stability,
    ShadowWriteEvent, FILE_DEBOUNCE_MS, STABILITY_CHECK_INTERVAL_MS, STABILITY_MAX_WAIT_MS,
};
