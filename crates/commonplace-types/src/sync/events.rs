//! Event types for file and directory watchers.
//!
//! These types are used by the watcher module to communicate filesystem
//! changes to the sync orchestrator.

use std::collections::HashMap;
use std::path::PathBuf;

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
    Created(PathBuf),
    Modified(PathBuf),
    Deleted(PathBuf),
    /// Schema file (.commonplace.json) was modified by user (not by sync client).
    /// Contains the path to the schema file and the new content.
    SchemaModified(PathBuf, String),
}

/// Options for directory scanning.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Include hidden files (starting with '.')
    pub include_hidden: bool,
    /// Custom ignore patterns (glob-style)
    pub ignore_patterns: Vec<String>,
    /// Pre-computed workspace UUID map (relative_path → node_id).
    /// When set, `load_workspace_existing_file_node_ids` skips rebuilding
    /// via `build_uuid_map_from_local_schemas`.
    pub workspace_uuid_map_cache: Option<HashMap<String, String>>,
}
