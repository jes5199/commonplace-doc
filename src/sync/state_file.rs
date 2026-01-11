//! Persistent sync state file for offline change detection.
//!
//! The state file is stored beside the synced target (not inside) to avoid
//! syncing it back to the server. For a target "notes/", the state file
//! is ".notes.commonplace-sync.json" in the parent directory.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncReadExt;

/// Persistent state for a sync session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStateFile {
    /// The server URL being synced with
    pub server: String,
    /// The document/node ID being synced
    pub node_id: String,
    /// Commit ID of the last successful sync
    pub last_synced_cid: Option<String>,
    /// Timestamp of the last sync (RFC 3339)
    pub last_synced_at: Option<String>,
    /// Map of relative paths to file metadata
    pub files: HashMap<String, FileState>,
    /// Set of directories that have been synced/created locally.
    /// Used to distinguish "deleted locally" from "not yet synced".
    #[serde(default)]
    pub directories: std::collections::HashSet<String>,
}

/// State for a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    /// SHA-256 hash of the file content (hex-encoded)
    pub hash: String,
    /// Last modified time (RFC 3339)
    pub last_modified: Option<String>,
    /// Commit ID of the last successful sync for this file
    #[serde(default)]
    pub last_cid: Option<String>,
    /// Inode key (dev-ino hex format) of the file at last sync.
    /// Used to initialize InodeTracker on startup.
    #[serde(default)]
    pub inode_key: Option<String>,
}

impl SyncStateFile {
    /// Create a new empty state file.
    pub fn new(server: String, node_id: String) -> Self {
        Self {
            server,
            node_id,
            last_synced_cid: None,
            last_synced_at: None,
            files: HashMap::new(),
            directories: std::collections::HashSet::new(),
        }
    }

    /// Derive the state file path for a given sync target.
    ///
    /// For a target "path/to/notes/", returns "path/to/.notes.commonplace-sync.json".
    /// For a target "readme.txt", returns ".readme.txt.commonplace-sync.json".
    pub fn state_file_path(target: &Path) -> PathBuf {
        let file_name = target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("sync");
        let state_name = format!(".{}.commonplace-sync.json", file_name);

        if let Some(parent) = target.parent() {
            parent.join(&state_name)
        } else {
            PathBuf::from(&state_name)
        }
    }

    /// Load state from file, or return None if it doesn't exist.
    pub async fn load(path: &Path) -> io::Result<Option<Self>> {
        match fs::read_to_string(path).await {
            Ok(content) => {
                let state: Self = serde_json::from_str(&content)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Save state to file.
    pub async fn save(&self, path: &Path) -> io::Result<()> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(path, content).await
    }

    /// Load state for a sync target, or create a new one if none exists.
    pub async fn load_or_create(target: &Path, server: &str, node_id: &str) -> io::Result<Self> {
        let state_path = Self::state_file_path(target);
        match Self::load(&state_path).await? {
            Some(state) => Ok(state),
            None => Ok(Self::new(server.to_string(), node_id.to_string())),
        }
    }

    /// Update file state after a successful sync.
    pub fn update_file(&mut self, relative_path: &str, hash: String) {
        self.update_file_with_cid(relative_path, hash, None);
    }

    /// Update file state with CID after a successful sync.
    pub fn update_file_with_cid(&mut self, relative_path: &str, hash: String, cid: Option<String>) {
        self.update_file_full(relative_path, hash, cid, None);
    }

    /// Update file state with CID and inode after a successful sync.
    ///
    /// The inode_key should be in "dev-ino" hex format (e.g., "fc00-1234abcd").
    pub fn update_file_with_inode(
        &mut self,
        relative_path: &str,
        hash: String,
        cid: Option<String>,
        inode_key: Option<String>,
    ) {
        self.update_file_full(relative_path, hash, cid, inode_key);
    }

    /// Internal: update file state with all fields.
    fn update_file_full(
        &mut self,
        relative_path: &str,
        hash: String,
        cid: Option<String>,
        inode_key: Option<String>,
    ) {
        let now = chrono::Utc::now().to_rfc3339();
        self.files.insert(
            relative_path.to_string(),
            FileState {
                hash,
                last_modified: Some(now),
                last_cid: cid,
                inode_key,
            },
        );
    }

    /// Get the last synced CID for a file, if known.
    pub fn get_file_cid(&self, relative_path: &str) -> Option<String> {
        self.files
            .get(relative_path)
            .and_then(|f| f.last_cid.clone())
    }

    /// Get the inode key for a file, if known.
    pub fn get_file_inode(&self, relative_path: &str) -> Option<String> {
        self.files
            .get(relative_path)
            .and_then(|f| f.inode_key.clone())
    }

    /// Mark a sync as complete.
    pub fn mark_synced(&mut self, cid: String) {
        self.last_synced_cid = Some(cid);
        self.last_synced_at = Some(chrono::Utc::now().to_rfc3339());
    }

    /// Check if a file has changed compared to the saved state.
    pub fn has_file_changed(&self, relative_path: &str, current_hash: &str) -> bool {
        match self.files.get(relative_path) {
            Some(state) => state.hash != current_hash,
            None => true, // New file, not in saved state
        }
    }

    /// Record that a directory has been synced/created locally.
    pub fn add_directory(&mut self, relative_path: &str) {
        self.directories.insert(relative_path.to_string());
    }

    /// Remove a directory from the tracked set (when deleted from schema).
    pub fn remove_directory(&mut self, relative_path: &str) {
        self.directories.remove(relative_path);
    }

    /// Check if a directory was previously synced/created locally.
    ///
    /// Returns true if the directory is tracked, meaning if it's missing
    /// from disk, it was deleted by the user (not just never synced).
    pub fn has_directory(&self, relative_path: &str) -> bool {
        self.directories.contains(relative_path)
    }
}

/// State file for tracking synced directories.
/// Used to distinguish "deleted locally" from "not yet synced from server".
const SYNCED_DIRS_FILENAME: &str = ".commonplace-synced-dirs.json";

/// Load the set of synced directories from the state file.
pub async fn load_synced_directories(directory: &Path) -> std::collections::HashSet<String> {
    let state_path = directory.join(SYNCED_DIRS_FILENAME);
    match fs::read_to_string(&state_path).await {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => std::collections::HashSet::new(),
    }
}

/// Save the set of synced directories to the state file.
pub async fn save_synced_directories(
    directory: &Path,
    dirs: &std::collections::HashSet<String>,
) -> io::Result<()> {
    let state_path = directory.join(SYNCED_DIRS_FILENAME);
    let content = serde_json::to_string_pretty(dirs)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    fs::write(&state_path, content).await
}

/// Add a directory to the synced set.
pub async fn mark_directory_synced(directory: &Path, relative_path: &str) -> io::Result<()> {
    let mut dirs = load_synced_directories(directory).await;
    dirs.insert(relative_path.to_string());
    save_synced_directories(directory, &dirs).await
}

/// Remove a directory from the synced set.
pub async fn unmark_directory_synced(directory: &Path, relative_path: &str) -> io::Result<()> {
    let mut dirs = load_synced_directories(directory).await;
    dirs.remove(relative_path);
    save_synced_directories(directory, &dirs).await
}

/// Check if a directory was previously synced.
pub async fn is_directory_synced(directory: &Path, relative_path: &str) -> bool {
    let dirs = load_synced_directories(directory).await;
    dirs.contains(relative_path)
}

/// Compute SHA-256 hash of a file.
pub async fn compute_file_hash(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8192];

    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Compute SHA-256 hash of content bytes.
pub fn compute_content_hash(content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_state_file_path_directory() {
        let target = Path::new("/path/to/notes");
        let path = SyncStateFile::state_file_path(target);
        assert_eq!(path, Path::new("/path/to/.notes.commonplace-sync.json"));
    }

    #[test]
    fn test_state_file_path_file() {
        let target = Path::new("/path/to/readme.txt");
        let path = SyncStateFile::state_file_path(target);
        assert_eq!(
            path,
            Path::new("/path/to/.readme.txt.commonplace-sync.json")
        );
    }

    #[test]
    fn test_state_file_path_root() {
        let target = Path::new("notes");
        let path = SyncStateFile::state_file_path(target);
        assert_eq!(path, Path::new(".notes.commonplace-sync.json"));
    }

    #[test]
    fn test_new_state() {
        let state = SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());
        assert_eq!(state.server, "http://localhost:3000");
        assert_eq!(state.node_id, "doc-123");
        assert!(state.last_synced_cid.is_none());
        assert!(state.files.is_empty());
    }

    #[test]
    fn test_update_file() {
        let mut state =
            SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());
        state.update_file("notes/test.txt", "abc123".to_string());

        assert!(state.files.contains_key("notes/test.txt"));
        assert_eq!(state.files["notes/test.txt"].hash, "abc123");
    }

    #[test]
    fn test_has_file_changed() {
        let mut state =
            SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());
        state.update_file("test.txt", "abc123".to_string());

        // Same hash - not changed
        assert!(!state.has_file_changed("test.txt", "abc123"));

        // Different hash - changed
        assert!(state.has_file_changed("test.txt", "def456"));

        // New file - changed
        assert!(state.has_file_changed("new.txt", "xyz"));
    }

    #[test]
    fn test_mark_synced() {
        let mut state =
            SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());
        state.mark_synced("Qm123".to_string());

        assert_eq!(state.last_synced_cid, Some("Qm123".to_string()));
        assert!(state.last_synced_at.is_some());
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");

        let mut state =
            SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());
        state.update_file("test.txt", "abc123".to_string());
        state.mark_synced("Qm123".to_string());

        state.save(&path).await.unwrap();

        let loaded = SyncStateFile::load(&path).await.unwrap().unwrap();
        assert_eq!(loaded.server, "http://localhost:3000");
        assert_eq!(loaded.node_id, "doc-123");
        assert_eq!(loaded.last_synced_cid, Some("Qm123".to_string()));
        assert_eq!(loaded.files["test.txt"].hash, "abc123");
    }

    #[tokio::test]
    async fn test_load_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let result = SyncStateFile::load(&path).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_compute_file_hash() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");
        fs::write(&path, "hello world").await.unwrap();

        let hash = compute_file_hash(&path).await.unwrap();
        // SHA-256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_compute_content_hash() {
        let hash = compute_content_hash(b"hello world");
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_update_file_with_cid() {
        let mut state =
            SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());
        state.update_file_with_cid("test.txt", "abc123".to_string(), Some("cid456".to_string()));

        assert!(state.files.contains_key("test.txt"));
        assert_eq!(state.files["test.txt"].hash, "abc123");
        assert_eq!(state.files["test.txt"].last_cid, Some("cid456".to_string()));
    }

    #[test]
    fn test_get_file_cid() {
        let mut state =
            SyncStateFile::new("http://localhost:3000".to_string(), "doc-123".to_string());

        // No CID initially
        assert!(state.get_file_cid("test.txt").is_none());

        // Add file without CID
        state.update_file("test.txt", "abc123".to_string());
        assert!(state.get_file_cid("test.txt").is_none());

        // Update with CID
        state.update_file_with_cid("test.txt", "def456".to_string(), Some("cid789".to_string()));
        assert_eq!(state.get_file_cid("test.txt"), Some("cid789".to_string()));
    }
}
