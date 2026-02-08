//! Filesystem watching for commonplace sync.
//!
//! This crate provides file/directory watchers, file locking,
//! inode tracking, and sync state management.

#[cfg(unix)]
pub mod flock;
pub mod flock_state;

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
