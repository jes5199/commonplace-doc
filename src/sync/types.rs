//! Data types for sync client operations.
//!
//! This module contains request/response types for the sync client API.

use crate::sync::SyncState;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

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
