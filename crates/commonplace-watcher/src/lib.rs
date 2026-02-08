//! Filesystem watching for commonplace sync.
//!
//! This crate provides file/directory watchers, file locking,
//! inode tracking, and sync state management.

#[cfg(unix)]
pub mod flock;

// Re-exports
#[cfg(unix)]
pub use flock::{
    try_flock_exclusive, try_flock_shared, FlockGuard, FlockResult, FLOCK_RETRY_INTERVAL,
    FLOCK_TIMEOUT,
};
