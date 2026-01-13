//! Data types for sync client operations.
//!
//! This module contains request/response types for the sync client API.

use crate::sync::SyncState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Tracks the content we've written to schema files, for echo detection.
///
/// When the sync client writes a `.commonplace.json` file (e.g., after receiving
/// an update from the server), it records the content here. When the watcher
/// detects a schema file change, it compares against this map to distinguish
/// our writes from user edits.
///
/// Key: Canonical path to the `.commonplace.json` file
/// Value: Content we last wrote (as normalized JSON string)
pub type WrittenSchemas = Arc<RwLock<HashMap<PathBuf, String>>>;

/// Shared state file for directory sync mode.
///
/// This is loaded at startup and shared across all file sync states.
/// Each file's last CID is stored in the `files` map, enabling proper
/// ancestry checking after restart.
pub type SharedStateFile = Arc<RwLock<crate::sync::state_file::SyncStateFile>>;

/// Response from GET /docs/:id/head
#[derive(Debug, Deserialize)]
pub struct HeadResponse {
    pub cid: Option<String>,
    pub content: String,
    /// Yjs state bytes at HEAD (base64 encoded, for CRDT-compatible diffs)
    #[serde(default)]
    pub state: Option<String>,
}

/// Response from POST /docs/:id/replace
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ReplaceResponse {
    pub cid: String,
    pub edit_cid: String,
    pub summary: ReplaceSummary,
}

/// Summary of a replace operation
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ReplaceSummary {
    pub chars_inserted: usize,
    pub chars_deleted: usize,
    pub operations: usize,
}

/// SSE edit event data
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct EditEventData {
    pub source: String,
    pub commit: CommitData,
}

/// Commit data from SSE events
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct CommitData {
    pub update: String,
    pub parents: Vec<String>,
    pub timestamp: u64,
    pub author: String,
    pub message: Option<String>,
}

/// Request for POST /docs/:id/edit (initial commit)
#[derive(Debug, Serialize)]
pub struct EditRequest {
    pub update: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Response from POST /docs/:id/edit
#[derive(Debug, Deserialize)]
pub struct EditResponse {
    pub cid: String,
}

/// Response from POST /docs/:id/fork
#[derive(Debug, Deserialize)]
pub struct ForkResponse {
    pub id: String,
    pub head: String,
}

/// File watcher events
#[derive(Debug)]
pub enum FileEvent {
    /// File was modified. Contains the raw file content captured at notification time.
    /// Capturing content immediately prevents race conditions where SSE might overwrite
    /// the file before the upload task reads it.
    Modified(Vec<u8>),
}

/// Directory watcher events
#[derive(Debug)]
pub enum DirEvent {
    Created(std::path::PathBuf),
    Modified(std::path::PathBuf),
    Deleted(std::path::PathBuf),
    /// Schema file (.commonplace.json) was modified by user (not by sync client).
    /// Contains the path to the schema file and the new content.
    SchemaModified(std::path::PathBuf, String),
}

/// Sync state for a single file in directory mode.
///
/// Tracks the synchronization state and resources for an individual file
/// being synced as part of a directory.
pub struct FileSyncState {
    /// Relative path from directory root
    #[allow(dead_code)]
    pub relative_path: String,
    /// Identifier - either path (for /files/* API) or node ID (for /docs/* API)
    pub identifier: String,
    /// Sync state for this file
    pub state: Arc<RwLock<SyncState>>,
    /// Task handles for cleanup on deletion
    pub task_handles: Vec<JoinHandle<()>>,
    /// Whether to use path-based API (/files/*) or ID-based API (/docs/*)
    pub use_paths: bool,
    /// Content hash for fork detection (SHA-256 hex)
    pub content_hash: Option<String>,
}

/// Remove a file state from the map and abort its associated tasks.
///
/// This helper consolidates the common pattern of removing a file state
/// and cleaning up its task handles when a file is deleted or no longer tracked.
///
/// Returns the removed state if it existed.
pub async fn remove_file_state_and_abort(
    file_states: &tokio::sync::RwLock<std::collections::HashMap<String, FileSyncState>>,
    path: &str,
) -> Option<FileSyncState> {
    let mut states = file_states.write().await;
    if let Some(file_state) = states.remove(path) {
        for handle in &file_state.task_handles {
            handle.abort();
        }
        Some(file_state)
    } else {
        None
    }
}

/// Reverse map from UUID to file paths.
///
/// Used to look up which local file(s) to update when we receive an edit
/// for a file UUID. Multiple paths can map to the same UUID when files
/// are linked via `commonplace-link`.
///
/// Key: UUID (node_id)
/// Value: List of relative paths that have this UUID
pub type UuidToPathsMap = HashMap<String, Vec<String>>;

/// Build a reverse map (uuid -> paths) from a forward uuid_map (path -> uuid).
///
/// This allows looking up which files to update when we receive an edit for a UUID.
pub fn build_uuid_to_paths_map(uuid_map: &HashMap<String, String>) -> UuidToPathsMap {
    let mut reverse_map: UuidToPathsMap = HashMap::new();
    for (path, uuid) in uuid_map {
        reverse_map
            .entry(uuid.clone())
            .or_default()
            .push(path.clone());
    }
    reverse_map
}

/// Event published when initial sync completes.
///
/// Published to `{fs_root_id}/events/sync/initial-complete` via MQTT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialSyncComplete {
    /// The filesystem root document ID (workspace name)
    pub fs_root_id: String,
    /// Number of files synced during initial sync
    pub files_synced: usize,
    /// Sync strategy used: "local", "server", or "skip"
    pub strategy: String,
    /// Time taken for initial sync in milliseconds
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_sync_complete_serialization() {
        let event = InitialSyncComplete {
            fs_root_id: "workspace".to_string(),
            files_synced: 42,
            strategy: "local".to_string(),
            duration_ms: 1523,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"fs_root_id\":\"workspace\""));
        assert!(json.contains("\"files_synced\":42"));
        assert!(json.contains("\"strategy\":\"local\""));
        assert!(json.contains("\"duration_ms\":1523"));

        let parsed: InitialSyncComplete = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.fs_root_id, "workspace");
        assert_eq!(parsed.files_synced, 42);
        assert_eq!(parsed.strategy, "local");
        assert_eq!(parsed.duration_ms, 1523);
    }

    #[test]
    fn test_build_uuid_to_paths_map() {
        let mut uuid_map = HashMap::new();
        uuid_map.insert("file1.txt".to_string(), "uuid-a".to_string());
        uuid_map.insert("file2.txt".to_string(), "uuid-b".to_string());
        // Two paths share the same UUID (linked files)
        uuid_map.insert("linked/file3.txt".to_string(), "uuid-a".to_string());

        let reverse_map = build_uuid_to_paths_map(&uuid_map);

        // uuid-a should have two paths
        let paths_a = reverse_map.get("uuid-a").unwrap();
        assert_eq!(paths_a.len(), 2);
        assert!(paths_a.contains(&"file1.txt".to_string()));
        assert!(paths_a.contains(&"linked/file3.txt".to_string()));

        // uuid-b should have one path
        let paths_b = reverse_map.get("uuid-b").unwrap();
        assert_eq!(paths_b.len(), 1);
        assert!(paths_b.contains(&"file2.txt".to_string()));
    }
}
