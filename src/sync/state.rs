//! Sync state management for file synchronization.
//!
//! This module contains the `SyncState` struct that tracks synchronization state
//! between file watchers and SSE tasks, including echo detection and write barriers.
//!
//! ## Inode Tracking
//!
//! When syncing with atomic writes (temp+rename), the inode at the path changes.
//! If another process has an open write fd to the old inode, its writes go to an
//! orphaned inode. The inode tracking system creates hardlinks to shadow old inodes,
//! watches them for writes, and merges those writes via CRDT.

use crate::sync::state_file::SyncStateFile;
use crate::sync::SharedStateFile;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tracing::warn;

/// Key for inode tracking: (device_id, inode_number).
/// Inode numbers are only unique within a device, so we need both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InodeKey {
    pub dev: u64,
    pub ino: u64,
}

impl InodeKey {
    /// Create a new InodeKey from device and inode numbers.
    pub fn new(dev: u64, ino: u64) -> Self {
        Self { dev, ino }
    }

    /// Generate the shadow filename for this inode: {devHex}-{inoHex}
    pub fn shadow_filename(&self) -> String {
        format!("{:x}-{:x}", self.dev, self.ino)
    }

    /// Parse an InodeKey from a shadow filename.
    pub fn from_shadow_filename(name: &str) -> Option<Self> {
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() != 2 {
            return None;
        }
        let dev = u64::from_str_radix(parts[0], 16).ok()?;
        let ino = u64::from_str_radix(parts[1], 16).ok()?;
        Some(Self { dev, ino })
    }

    /// Get inode key from a file path using stat.
    #[cfg(unix)]
    pub fn from_path(path: &std::path::Path) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(path)?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    /// Get inode key from an open file descriptor using fstat.
    #[cfg(unix)]
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
}

/// Create a hardlink to an open file using linkat with AT_EMPTY_PATH.
/// This is TOCTOU-proof since we link the exact inode we have open.
///
/// Note: AT_EMPTY_PATH requires CAP_DAC_READ_SEARCH or Linux 5.12+ with
/// LOOKUP_LINKAT_EMPTY_PATH. Falls back to using /proc/self/fd path.
#[cfg(target_os = "linux")]
pub fn hardlink_from_fd(file: &std::fs::File, target: &std::path::Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let target_cstr = CString::new(target.as_os_str().as_bytes())?;

    // Try linkat with AT_EMPTY_PATH first (requires CAP_DAC_READ_SEARCH or recent kernel)
    // AT_EMPTY_PATH = 0x1000, AT_FDCWD = -100
    const AT_FDCWD: libc::c_int = -100;
    const AT_EMPTY_PATH: libc::c_int = 0x1000;

    let result = unsafe {
        libc::linkat(
            fd,
            c"".as_ptr(), // empty path
            AT_FDCWD,
            target_cstr.as_ptr(),
            AT_EMPTY_PATH,
        )
    };

    if result == 0 {
        return Ok(());
    }

    let err = std::io::Error::last_os_error();

    // If EPERM or ENOENT, the kernel doesn't support AT_EMPTY_PATH for unprivileged users.
    // Fall back to using /proc/self/fd (has small race window but usually works)
    if err.raw_os_error() == Some(libc::EPERM) || err.raw_os_error() == Some(libc::ENOENT) {
        let proc_path = format!("/proc/self/fd/{}", fd);
        let proc_cstr = CString::new(proc_path.as_bytes())?;

        // Use linkat with AT_SYMLINK_FOLLOW to follow the /proc symlink
        const AT_SYMLINK_FOLLOW: libc::c_int = 0x400;
        let result2 = unsafe {
            libc::linkat(
                AT_FDCWD,
                proc_cstr.as_ptr(),
                AT_FDCWD,
                target_cstr.as_ptr(),
                AT_SYMLINK_FOLLOW,
            )
        };

        if result2 == 0 {
            return Ok(());
        }

        return Err(std::io::Error::last_os_error());
    }

    Err(err)
}

/// Fallback for non-Linux: hardlink from path (has TOCTOU race).
#[cfg(all(unix, not(target_os = "linux")))]
pub fn hardlink_from_fd(_file: &std::fs::File, _target: &std::path::Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "hardlink_from_fd requires Linux",
    ))
}

/// Hardlink from path (fallback, has TOCTOU race).
#[cfg(unix)]
pub fn hardlink_from_path(
    source: &std::path::Path,
    target: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::hard_link(source, target)
}

/// State for a tracked inode (either primary path or shadowed).
#[derive(Debug, Clone)]
pub struct InodeState {
    /// CID of the commit this inode's content is based on.
    /// For writes to this inode, use this as the parent for CRDT merge.
    pub commit_id: String,
    /// Path to hardlink in shadow directory (if shadowed).
    /// None for the current primary inode.
    pub shadow_path: Option<PathBuf>,
    /// When this inode was moved to shadow (if shadowed).
    pub shadowed_at: Option<Instant>,
    /// Last write seen on this inode.
    pub last_write: Instant,
    /// Primary file path this inode is associated with.
    pub primary_path: PathBuf,
}

/// Default idle timeout before GC can remove a shadow link (1 hour).
pub const SHADOW_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

/// Minimum time a shadow must exist before GC can remove it (5 minutes).
pub const SHADOW_MIN_LIFETIME: std::time::Duration = std::time::Duration::from_secs(300);

/// Shared tracker for all inodes across the sync client.
///
/// This is shared across all file sync tasks and tracks:
/// - Which inodes are currently being synced
/// - Shadow hardlinks for old inodes that may still receive writes
#[derive(Debug)]
pub struct InodeTracker {
    /// Map of inode key to state.
    pub states: HashMap<InodeKey, InodeState>,
    /// Shadow directory where hardlinks are stored.
    pub shadow_dir: PathBuf,
}

impl InodeTracker {
    /// Create a new InodeTracker with the given shadow directory.
    pub fn new(shadow_dir: PathBuf) -> Self {
        Self {
            states: HashMap::new(),
            shadow_dir,
        }
    }

    /// Get the shadow path for an inode key.
    pub fn shadow_path(&self, key: &InodeKey) -> PathBuf {
        self.shadow_dir.join(key.shadow_filename())
    }

    /// Track a new inode at a primary path with a given commit.
    pub fn track(&mut self, key: InodeKey, commit_id: String, primary_path: PathBuf) {
        self.states.insert(
            key,
            InodeState {
                commit_id,
                shadow_path: None,
                shadowed_at: None,
                last_write: Instant::now(),
                primary_path,
            },
        );
    }

    /// Move an inode to shadow status.
    /// Returns the shadow path where the hardlink should be created.
    pub fn shadow(&mut self, key: InodeKey) -> Option<PathBuf> {
        // Compute shadow path before borrowing states mutably
        let shadow_path = self.shadow_dir.join(key.shadow_filename());
        if let Some(state) = self.states.get_mut(&key) {
            state.shadow_path = Some(shadow_path.clone());
            state.shadowed_at = Some(Instant::now());
            Some(shadow_path)
        } else {
            None
        }
    }

    /// Update the commit_id and last_write for an inode.
    pub fn update_commit(&mut self, key: &InodeKey, commit_id: String) {
        if let Some(state) = self.states.get_mut(key) {
            state.commit_id = commit_id;
            state.last_write = Instant::now();
        }
    }

    /// Mark that a write was observed on this inode.
    pub fn touch(&mut self, key: &InodeKey) {
        if let Some(state) = self.states.get_mut(key) {
            state.last_write = Instant::now();
        }
    }

    /// Get state for an inode.
    pub fn get(&self, key: &InodeKey) -> Option<&InodeState> {
        self.states.get(key)
    }

    /// Find inode state by shadow path.
    pub fn find_by_shadow_path(&self, path: &PathBuf) -> Option<(&InodeKey, &InodeState)> {
        self.states
            .iter()
            .find(|(_, state)| state.shadow_path.as_ref() == Some(path))
    }

    /// Garbage collect stale shadow links.
    /// Returns list of shadow paths that were cleaned up.
    pub fn gc(&mut self) -> Vec<PathBuf> {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (key, state) in &self.states {
            // Only GC shadowed inodes
            if let (Some(shadow_path), Some(shadowed_at)) = (&state.shadow_path, state.shadowed_at)
            {
                let idle_time = now.duration_since(state.last_write);
                let shadow_age = now.duration_since(shadowed_at);

                if idle_time > SHADOW_IDLE_TIMEOUT && shadow_age > SHADOW_MIN_LIFETIME {
                    to_remove.push((*key, shadow_path.clone()));
                }
            }
        }

        let mut cleaned = Vec::new();
        for (key, shadow_path) in to_remove {
            self.states.remove(&key);
            cleaned.push(shadow_path);
        }

        cleaned
    }

    /// Clear all state (called on startup).
    pub fn clear(&mut self) {
        self.states.clear();
    }
}

/// Pending write from server - used for barrier-based echo detection.
///
/// When the server sends an edit, we write it to the local file. The file watcher
/// will see this change and try to upload it back. The pending write barrier prevents
/// this "echo" by tracking what we just wrote.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    /// Unique token for this write operation
    pub write_id: u64,
    /// Content being written
    pub content: String,
    /// CID of the commit being written
    pub cid: Option<String>,
    /// When this write started (for timeout detection)
    pub started_at: std::time::Instant,
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
        }
    }

    /// Update state file after successful sync and save to disk.
    ///
    /// Called after a successful upload or download to record the new state.
    /// Works with both file mode (single state file) and directory mode (shared state file).
    pub async fn mark_synced(&mut self, cid: &str, content_hash: &str, relative_path: &str) {
        // File mode: update single-file state file
        if let Some(ref mut state_file) = self.state_file {
            state_file.mark_synced(cid.to_string());
            state_file.update_file(relative_path, content_hash.to_string());

            // Save to disk
            if let Some(ref path) = self.state_file_path {
                if let Err(e) = state_file.save(path).await {
                    warn!("Failed to save state file: {}", e);
                }
            }
        }

        // Directory mode: update shared state file with per-file CID
        if let Some(ref shared_sf) = self.shared_state_file {
            // Use our stored relative path for the key (not the filename parameter)
            let path_key = self.relative_path.as_deref().unwrap_or(relative_path);

            let mut sf = shared_sf.write().await;
            sf.update_file_with_cid(path_key, content_hash.to_string(), Some(cid.to_string()));
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
