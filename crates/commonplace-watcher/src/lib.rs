//! Filesystem watching for commonplace sync.
//!
//! This crate provides file/directory watchers, file locking,
//! inode tracking, and sync state management.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(unix)]
pub mod flock;
pub mod flock_state;
pub mod state;
pub mod watcher;

/// Tracks schema files written by sync, for detecting user edits vs our writes.
///
/// Key: Canonical path to the `.commonplace.json` file
/// Value: Content we last wrote (as normalized JSON string)
pub type WrittenSchemas = Arc<RwLock<HashMap<PathBuf, String>>>;

// Re-exports
#[cfg(unix)]
pub use flock::{
    try_flock_exclusive, try_flock_shared, FlockGuard, FlockResult, FLOCK_RETRY_INTERVAL,
    FLOCK_TIMEOUT,
};
pub use flock_state::{
    process_pending_inbound_after_confirm, record_upload_result, FlockSyncState, InboundWrite,
    PathState,
};
#[cfg(unix)]
pub use state::{hardlink_from_fd, hardlink_from_path};
pub use state::{InodeKey, InodeState, InodeTracker, PendingWrite};
pub use state::{SHADOW_IDLE_TIMEOUT, SHADOW_MIN_LIFETIME};
