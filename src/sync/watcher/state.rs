//! Sync state management for file synchronization.
//!
//! Re-exports inode tracking types from `commonplace-watcher` crate.
//! `SyncState` and the `InodeTrackerInit` extension trait remain here
//! because they depend on main-crate-only types (`SyncStateFile`, `SharedStateFile`).
//!
//! ## Deprecation Notice
//!
//! The echo suppression mechanisms in this module (PendingWrite, write barriers,
//! last_written_content tracking) are part of the legacy "mirror with echo suppression"
//! architecture. New code should use the CRDT peer modules instead:
//!
//! - `crdt_state.rs` - Per-directory Y.Doc state with CID tracking
//! - `crdt_publish.rs` - Local changes via Y.Doc diff and MQTT publish
//! - `crdt_merge.rs` - Remote changes with CID-based deduplication (no echo suppression needed)
//! - `crdt_new_file.rs` - Local UUID generation (no server round-trip)
//!
//! In the CRDT peer architecture, echo prevention happens via CID checks: if we receive
//! a commit we already know (by CID), we ignore it. This eliminates the need for
//! content-based or path-based echo detection.
//!
//! See: `docs/plans/2026-01-21-crdt-peer-sync-design.md`

// Re-export inode tracking types from commonplace-watcher
#[cfg(target_os = "linux")]
pub use commonplace_watcher::state::hardlink_from_fd;
#[cfg(unix)]
pub use commonplace_watcher::state::hardlink_from_path;
pub use commonplace_watcher::state::{
    InodeKey, InodeState, InodeTracker, PendingWrite, SHADOW_IDLE_TIMEOUT, SHADOW_MIN_LIFETIME,
};

use crate::sync::state_file::SyncStateFile;
use crate::sync::SharedStateFile;
use std::path::PathBuf;
use tracing::warn;

/// Extension trait for initializing `InodeTracker` from a `SyncStateFile`.
///
/// This is defined in the main crate because `SyncStateFile` is not available
/// in the `commonplace-watcher` crate.
pub trait InodeTrackerInit {
    /// Initialize tracker from persisted state file.
    ///
    /// On startup, this populates the tracker with inode->CID mappings from the
    /// persisted state file. This ensures InodeTracker starts with correct state
    /// rather than being empty.
    ///
    /// The `base_dir` is the directory containing the synced files (used to
    /// construct absolute paths from relative paths in the state file).
    fn init_from_state_file(&mut self, state_file: &SyncStateFile, base_dir: &std::path::Path);
}

impl InodeTrackerInit for InodeTracker {
    fn init_from_state_file(&mut self, state_file: &SyncStateFile, base_dir: &std::path::Path) {
        for (relative_path, file_state) in &state_file.files {
            // Skip files without inode or CID
            let (Some(inode_str), Some(cid)) = (&file_state.inode_key, &file_state.last_cid) else {
                continue;
            };

            // Parse the inode key
            let Some(inode_key) = InodeKey::from_shadow_filename(inode_str) else {
                warn!("Invalid inode key in state file: {}", inode_str);
                continue;
            };

            // Construct primary path
            let primary_path = base_dir.join(relative_path);

            // Track this inode
            self.track(inode_key, cid.clone(), primary_path);
        }
    }
}

/// Shared state between file watcher and SSE tasks.
///
/// This struct is protected by `Arc<RwLock<SyncState>>` and coordinates between:
/// - The upload task (watches local file, pushes to server)
/// - The SSE task (receives server edits, writes to local file)
///
/// Key mechanisms:
/// - **Echo detection**: Prevents re-uploading content we just received from server
/// - **Write barrier**: Tracks pending server writes to handle concurrent edits
/// - **State file**: Persists sync state for offline change detection
/// - **Upload serialization**: Prevents concurrent duplicate uploads
pub struct SyncState {
    /// CID of the commit we last wrote to the local file
    pub last_written_cid: Option<String>,
    /// Content we last wrote to the local file (for echo detection)
    pub last_written_content: String,
    /// Monotonic counter for write operations
    pub current_write_id: u64,
    /// Currently pending server write (barrier is "up" when Some)
    pub pending_write: Option<PendingWrite>,
    /// Flag indicating a server edit was skipped while barrier was up.
    /// upload_task should refresh HEAD after clearing barrier if this is true.
    pub needs_head_refresh: bool,
    /// Persistent state file for offline change detection (file mode only)
    pub state_file: Option<SyncStateFile>,
    /// Path to save the state file
    pub state_file_path: Option<PathBuf>,
    /// Shared state file for directory mode (multiple files share one state file)
    pub shared_state_file: Option<SharedStateFile>,
    /// Relative path for this file within the directory (for shared state file updates)
    pub relative_path: Option<String>,
    /// Flag indicating an upload is currently in progress.
    /// Other upload_tasks should wait for this to clear before proceeding.
    pub upload_in_progress: bool,
}

impl SyncState {
    /// Create a new SyncState with default values.
    pub fn new() -> Self {
        Self {
            last_written_cid: None,
            last_written_content: String::new(),
            current_write_id: 0,
            pending_write: None,
            needs_head_refresh: false,
            state_file: None,
            state_file_path: None,
            shared_state_file: None,
            relative_path: None,
            upload_in_progress: false,
        }
    }

    /// Create a SyncState with an initial CID.
    ///
    /// Used in directory mode when loading persisted per-file CIDs from a shared state file.
    pub fn with_cid(cid: Option<String>) -> Self {
        Self {
            last_written_cid: cid,
            last_written_content: String::new(),
            current_write_id: 0,
            pending_write: None,
            needs_head_refresh: false,
            state_file: None,
            state_file_path: None,
            shared_state_file: None,
            relative_path: None,
            upload_in_progress: false,
        }
    }

    /// Create a SyncState for directory mode with a shared state file.
    ///
    /// This enables per-file CID persistence in directory sync mode. The shared state file
    /// is updated whenever a sync completes, allowing CIDs to survive restart.
    pub fn for_directory_file(
        cid: Option<String>,
        shared_state_file: SharedStateFile,
        relative_path: String,
    ) -> Self {
        Self {
            last_written_cid: cid,
            last_written_content: String::new(),
            current_write_id: 0,
            pending_write: None,
            needs_head_refresh: false,
            state_file: None,
            state_file_path: None,
            shared_state_file: Some(shared_state_file),
            relative_path: Some(relative_path),
            upload_in_progress: false,
        }
    }

    /// Create a SyncState initialized from a persisted state file.
    ///
    /// This is used when resuming sync to detect offline changes.
    pub fn with_state_file(state_file: SyncStateFile, state_file_path: PathBuf) -> Self {
        Self {
            last_written_cid: state_file.last_synced_cid.clone(),
            last_written_content: String::new(),
            current_write_id: 0,
            pending_write: None,
            needs_head_refresh: false,
            state_file: Some(state_file),
            state_file_path: Some(state_file_path),
            shared_state_file: None,
            relative_path: None,
            upload_in_progress: false,
        }
    }

    /// Update state file after successful sync and save to disk.
    ///
    /// Called after a successful upload or download to record the new state.
    /// Works with both file mode (single state file) and directory mode (shared state file).
    ///
    /// The optional `inode_key` should be in "dev-ino" hex format (e.g., "fc00-1234abcd").
    /// Use `InodeKey::from_path()` to compute it from the file path.
    pub async fn mark_synced(
        &mut self,
        cid: &str,
        content_hash: &str,
        relative_path: &str,
        inode_key: Option<String>,
    ) {
        // File mode: update single-file state file
        if let Some(ref mut state_file) = self.state_file {
            state_file.mark_synced(cid.to_string());
            state_file.update_file_with_inode(
                relative_path,
                content_hash.to_string(),
                Some(cid.to_string()),
                inode_key.clone(),
            );

            // Save to disk
            if let Some(ref path) = self.state_file_path {
                if let Err(e) = state_file.save(path).await {
                    warn!("Failed to save state file: {}", e);
                }
            }
        }

        // Directory mode: update shared state file with per-file CID and inode
        if let Some(ref shared_sf) = self.shared_state_file {
            // Use our stored relative path for the key (not the filename parameter)
            let path_key = self.relative_path.as_deref().unwrap_or(relative_path);

            let mut sf = shared_sf.write().await;
            sf.update_file_with_inode(
                path_key,
                content_hash.to_string(),
                Some(cid.to_string()),
                inode_key,
            );
        }
    }

    /// Save the shared state file to disk.
    ///
    /// Called periodically or at shutdown to persist per-file CIDs in directory mode.
    pub async fn save_shared_state_file(&self, state_file_path: &std::path::Path) {
        if let Some(ref shared_sf) = self.shared_state_file {
            let sf = shared_sf.read().await;
            if let Err(e) = sf.save(state_file_path).await {
                warn!("Failed to save shared state file: {}", e);
            }
        }
    }
}

impl Default for SyncState {
    fn default() -> Self {
        Self::new()
    }
}
