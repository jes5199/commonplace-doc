//! UUID map building and path resolution for directory sync.
//!
//! Pure functions for building UUID maps from local schemas and
//! collecting paths from schema entries. HTTP-dependent functions
//! remain in the main crate's sync::schema::uuid_map shim.

use crate::schema_io::SCHEMA_FILENAME;
use commonplace_types::fs::{Entry, FsSchema};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tracing::{debug, warn};

/// Collect all file paths and their node_ids from a schema entry.
///
/// This is a synchronous traversal that does not follow node-backed directories.
/// For async traversal that follows node-backed dirs, use `collect_paths_with_node_backed_dirs`.
pub fn collect_paths_from_entry(
    entry: &Entry,
    prefix: &str,
    paths: &mut Vec<(String, Option<String>)>,
) {
    match entry {
        Entry::Dir(dir) => {
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_paths_from_entry(child, &child_path, paths);
                }
            }
        }
        Entry::Doc(doc) => {
            paths.push((prefix.to_string(), doc.node_id.clone()));
        }
    }
}

/// Error type for UUID map readiness timeout.
#[derive(Debug, Clone)]
pub struct UuidMapTimeoutError {
    /// Paths that were still missing UUIDs when timeout occurred.
    pub missing_paths: Vec<String>,
    /// Total time waited before timeout.
    pub elapsed: Duration,
}

impl std::fmt::Display for UuidMapTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UUID map not ready after {:?}: {} paths still missing UUIDs",
            self.elapsed,
            self.missing_paths.len()
        )
    }
}

impl std::error::Error for UuidMapTimeoutError {}

/// Build a UUID map by reading local `.commonplace.json` schema files from the filesystem.
///
/// This is the local equivalent of `build_uuid_map_recursive` — it builds the same
/// path-to-UUID map but reads schema files from disk instead of fetching via HTTP.
/// Used after `sync_schema` has already ensured the local schema files are up to date.
pub fn build_uuid_map_from_local_schemas(
    directory: &Path,
    path_prefix: &str,
    uuid_map: &mut HashMap<String, String>,
) {
    let schema_path = directory.join(SCHEMA_FILENAME);
    let content = match std::fs::read_to_string(&schema_path) {
        Ok(c) => c,
        Err(e) => {
            debug!("No schema file at {}: {}", schema_path.display(), e);
            return;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse schema at {}: {}", schema_path.display(), e);
            return;
        }
    };

    if let Some(ref root) = schema.root {
        collect_paths_from_local_schema(root, directory, path_prefix, uuid_map);
    }
}

/// Recursively collect file paths and UUIDs from local schema entries.
///
/// For node-backed directories, reads the subdirectory's `.commonplace.json` and recurses.
fn collect_paths_from_local_schema(
    entry: &Entry,
    directory: &Path,
    prefix: &str,
    uuid_map: &mut HashMap<String, String>,
) {
    match entry {
        Entry::Dir(dir) => {
            // Node-backed directory: read its local schema and recurse
            if dir.node_id.is_some() {
                let subdir_path = directory.join(prefix);
                build_uuid_map_from_local_schemas(&subdir_path, prefix, uuid_map);
            }
            // Walk inline entries (root directory has entries)
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_paths_from_local_schema(child, directory, &child_path, uuid_map);
                }
            }
        }
        Entry::Doc(doc) => {
            if let Some(ref node_id) = doc.node_id {
                debug!("Local UUID: {} -> {}", prefix, node_id);
                uuid_map.insert(prefix.to_string(), node_id.clone());
            }
        }
    }
}
