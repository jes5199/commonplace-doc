//! UUID map building and path resolution for directory sync.
//!
//! This module handles building maps of relative paths to UUIDs by
//! fetching schemas from the server and traversing node-backed directories.

use super::schema_io::write_schema_file;
use crate::fs::{Entry, FsSchema};
use crate::sync::client::fetch_head;
use crate::sync::types::WrittenSchemas;
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

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

/// Fetch the node_id (UUID) for a file path from the server's schema.
///
/// After pushing a schema update, the server's reconciler creates documents with UUIDs.
/// This function fetches the updated schema and looks up the UUID for the given path.
/// Returns None if the path is not found or if the node_id is not set.
pub async fn fetch_node_id_from_schema(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    relative_path: &str,
) -> Option<String> {
    // Build the full UUID map recursively (follows node-backed directories)
    let uuid_map = build_uuid_map_recursive(client, server, fs_root_id).await;
    uuid_map.get(relative_path).cloned()
}

/// Fetch the node_id for a subdirectory from the parent schema.
///
/// Unlike `fetch_node_id_from_schema` which looks up files, this function
/// looks up the node_id of a node-backed subdirectory entry.
pub async fn fetch_subdir_node_id(
    client: &Client,
    server: &str,
    parent_doc_id: &str,
    subdir_name: &str,
) -> Option<String> {
    use crate::sync::client::{fetch_head, FetchHeadError};

    let head = match fetch_head(client, server, parent_doc_id, false).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            warn!("No HEAD for parent document {}", parent_doc_id);
            return None;
        }
        Err(FetchHeadError::Status(_, _)) => {
            warn!("Document {} not found", parent_doc_id);
            return None;
        }
        Err(e) => {
            warn!("Failed to fetch HEAD for {}: {:?}", parent_doc_id, e);
            return None;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Document {} failed to parse as schema ({})",
                parent_doc_id, e
            );
            return None;
        }
    };

    // Look up the subdirectory entry in the root entries
    if let Some(crate::fs::Entry::Dir(dir)) = schema.root.as_ref() {
        if let Some(crate::fs::Entry::Dir(subdir)) =
            dir.entries.as_ref().and_then(|e| e.get(subdir_name))
        {
            return subdir.node_id.clone();
        }
    }

    None
}

/// Recursively build a map of relative paths to UUIDs by fetching all schemas.
///
/// This function follows node-backed directories and fetches their schemas
/// to build a complete map of all file paths to their UUIDs.
pub async fn build_uuid_map_recursive(
    client: &Client,
    server: &str,
    doc_id: &str,
) -> HashMap<String, String> {
    let (uuid_map, _success) = build_uuid_map_recursive_with_status(client, server, doc_id).await;
    uuid_map
}

/// Recursively build a map of relative paths to UUIDs, also returning success status.
///
/// Returns (uuid_map, all_fetches_succeeded). If all_fetches_succeeded is false,
/// at least one network fetch failed during recursive traversal, and the uuid_map
/// may be incomplete. This helps distinguish between "legitimately empty" and
/// "fetch failed" scenarios.
pub async fn build_uuid_map_recursive_with_status(
    client: &Client,
    server: &str,
    doc_id: &str,
) -> (HashMap<String, String>, bool) {
    let mut uuid_map = HashMap::new();
    let mut all_succeeded = true;
    build_uuid_map_from_doc_with_status(
        client,
        server,
        doc_id,
        "",
        &mut uuid_map,
        &mut all_succeeded,
        None,
        None,
    )
    .await;
    (uuid_map, all_succeeded)
}

/// Build UUID map AND write nested schema files to local directory.
///
/// This is used during initial sync to ensure all nested node-backed directories
/// have their .commonplace.json files written locally. This is necessary for
/// `find_owning_document` to work correctly when the uuid_map lookup fails.
///
/// If `written_schemas` is provided, written schema content is recorded for
/// echo detection by the directory watcher.
///
/// Returns (uuid_map, all_fetches_succeeded).
pub async fn build_uuid_map_and_write_schemas(
    client: &Client,
    server: &str,
    doc_id: &str,
    local_directory: &Path,
    written_schemas: Option<&WrittenSchemas>,
) -> (HashMap<String, String>, bool) {
    let mut uuid_map = HashMap::new();
    let mut all_succeeded = true;
    build_uuid_map_from_doc_with_status(
        client,
        server,
        doc_id,
        "",
        &mut uuid_map,
        &mut all_succeeded,
        Some(local_directory),
        written_schemas,
    )
    .await;
    (uuid_map, all_succeeded)
}

/// Helper function to recursively build the UUID map from a document and its children.
#[async_recursion::async_recursion]
pub async fn build_uuid_map_from_doc(
    client: &Client,
    server: &str,
    doc_id: &str,
    path_prefix: &str,
    uuid_map: &mut HashMap<String, String>,
) {
    let mut success = true;
    build_uuid_map_from_doc_with_status(
        client,
        server,
        doc_id,
        path_prefix,
        uuid_map,
        &mut success,
        None,
        None,
    )
    .await;
}

/// Helper function to recursively build the UUID map, tracking fetch success.
///
/// Optionally writes schema files to the local directory if `local_directory` is provided.
/// This consolidates the previous separate functions for building UUID maps with and without
/// schema writing.
#[allow(clippy::too_many_arguments)]
#[async_recursion::async_recursion]
pub async fn build_uuid_map_from_doc_with_status(
    client: &Client,
    server: &str,
    doc_id: &str,
    path_prefix: &str,
    uuid_map: &mut HashMap<String, String>,
    all_succeeded: &mut bool,
    local_directory: Option<&Path>,
    written_schemas: Option<&WrittenSchemas>,
) {
    // Fetch the schema from this document
    let head = match fetch_head(client, server, doc_id, false).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            warn!("Document {} not found", doc_id);
            *all_succeeded = false;
            return;
        }
        Err(e) => {
            warn!("Failed to fetch schema for {}: {}", doc_id, e);
            *all_succeeded = false;
            return;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            // In the context of deletion cleanup, a node-backed directory that doesn't
            // parse as a schema is unexpected and could indicate corruption or truncation.
            // Treat this as a failure to prevent accidental file deletions.
            warn!("Document {} failed to parse as schema ({})", doc_id, e);
            *all_succeeded = false;
            return;
        }
    };

    // Optionally write schema file to local directory
    if let Some(local_dir) = local_directory {
        let schema_dir = if path_prefix.is_empty() {
            local_dir.to_path_buf()
        } else {
            local_dir.join(path_prefix)
        };

        // Create directory if it doesn't exist
        if let Err(e) = tokio::fs::create_dir_all(&schema_dir).await {
            warn!(
                "Failed to create directory {:?} for schema: {}",
                schema_dir, e
            );
        } else {
            // Write the schema file
            if let Err(e) = write_schema_file(&schema_dir, &head.content, written_schemas).await {
                warn!("Failed to write schema file to {:?}: {}", schema_dir, e);
            } else {
                debug!("Wrote schema file to {:?}", schema_dir);
            }
        }
    }

    // Traverse the schema and collect UUIDs
    if let Some(ref root) = schema.root {
        collect_paths_with_node_backed_dirs_with_status(
            client,
            server,
            root,
            path_prefix,
            uuid_map,
            all_succeeded,
            local_directory,
            written_schemas,
        )
        .await;
    }
}

/// Recursively collect paths from an entry, following node-backed directories.
#[async_recursion::async_recursion]
pub async fn collect_paths_with_node_backed_dirs(
    client: &Client,
    server: &str,
    entry: &Entry,
    prefix: &str,
    uuid_map: &mut HashMap<String, String>,
) {
    let mut all_succeeded = true;
    collect_paths_with_node_backed_dirs_with_status(
        client,
        server,
        entry,
        prefix,
        uuid_map,
        &mut all_succeeded,
        None,
        None,
    )
    .await;
}

/// Recursively collect paths from an entry, following node-backed directories, with status tracking.
///
/// Optionally writes schema files if `local_directory` is provided.
#[allow(clippy::too_many_arguments)]
#[async_recursion::async_recursion]
pub async fn collect_paths_with_node_backed_dirs_with_status(
    client: &Client,
    server: &str,
    entry: &Entry,
    prefix: &str,
    uuid_map: &mut HashMap<String, String>,
    all_succeeded: &mut bool,
    local_directory: Option<&Path>,
    written_schemas: Option<&WrittenSchemas>,
) {
    match entry {
        Entry::Dir(dir) => {
            // Node-backed directory: fetch its document and recurse
            if let Some(ref node_id) = dir.node_id {
                build_uuid_map_from_doc_with_status(
                    client,
                    server,
                    node_id,
                    prefix,
                    uuid_map,
                    all_succeeded,
                    local_directory,
                    written_schemas,
                )
                .await;
            }
            // Directory with inline entries (root or legacy): iterate over entries
            // Note: nested inline subdirectories are deprecated, but root entries are valid
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_paths_with_node_backed_dirs_with_status(
                        client,
                        server,
                        child,
                        &child_path,
                        uuid_map,
                        all_succeeded,
                        local_directory,
                        written_schemas,
                    )
                    .await;
                }
            }
        }
        Entry::Doc(doc) => {
            // This is a file - add it to the map if it has a node_id
            if let Some(ref node_id) = doc.node_id {
                debug!("Found UUID: {} -> {}", prefix, node_id);
                uuid_map.insert(prefix.to_string(), node_id.clone());
            }
        }
    }
}

/// Collect all node-backed directory IDs from a schema (recursively).
///
/// This returns a vec of (path_prefix, node_id) tuples for all directories
/// that have a node_id (i.e., node-backed directories).
#[async_recursion::async_recursion]
pub async fn collect_node_backed_dir_ids(
    client: &Client,
    server: &str,
    entry: &Entry,
    prefix: &str,
    result: &mut Vec<(String, String)>,
) {
    match entry {
        Entry::Dir(dir) => {
            // Node-backed directory: add to result and recurse into its content
            if let Some(ref node_id) = dir.node_id {
                // Add to result
                result.push((prefix.to_string(), node_id.clone()));

                // Also recursively check if this subdirectory has nested node-backed dirs
                if let Ok(Some(head)) = fetch_head(client, server, node_id, false).await {
                    if let Ok(schema) = serde_json::from_str::<FsSchema>(&head.content) {
                        if let Some(ref root) = schema.root {
                            collect_node_backed_dir_ids(client, server, root, prefix, result).await;
                        }
                    }
                }
            }
            // Directory with inline entries (root or legacy): iterate over entries
            // to find node-backed subdirectories
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_node_backed_dir_ids(client, server, child, &child_path, result).await;
                }
            }
        }
        Entry::Doc(_) => {
            // Documents are not directories, skip them
        }
    }
}

/// Collect all node-backed directory IDs from an fs-root document.
///
/// Fetches the schema and returns all (path, node_id) pairs for node-backed subdirectories.
pub async fn get_all_node_backed_dir_ids(
    client: &Client,
    server: &str,
    fs_root_id: &str,
) -> Vec<(String, String)> {
    let mut result = Vec::new();

    let head = match fetch_head(client, server, fs_root_id, false).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            warn!("fs-root document {} not found", fs_root_id);
            return result;
        }
        Err(e) => {
            warn!("Failed to fetch fs-root schema: {}", e);
            return result;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse fs-root schema: {}", e);
            return result;
        }
    };

    if let Some(ref root) = schema.root {
        collect_node_backed_dir_ids(client, server, root, "", &mut result).await;
    }

    result
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

/// Wait for the UUID map to be ready with all expected file paths having UUIDs.
///
/// This function polls the server with exponential backoff until all files
/// in the schema have UUIDs assigned by the reconciler.
///
/// # Arguments
/// * `client` - HTTP client for server requests
/// * `server` - Server URL
/// * `fs_root_id` - The fs-root document ID
/// * `expected_paths` - List of relative file paths that should have UUIDs
///
/// # Returns
/// * `Ok(HashMap<String, String>)` - Map of relative paths to UUIDs when all are ready
/// * `Err(UuidMapTimeoutError)` - If timeout (10s) is reached with missing UUIDs
///
/// # Backoff Strategy
/// Uses exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms...
/// with a maximum total wait time of 10 seconds.
pub async fn wait_for_uuid_map_ready(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    expected_paths: &[String],
) -> Result<HashMap<String, String>, UuidMapTimeoutError> {
    const INITIAL_DELAY_MS: u64 = 100;
    const MAX_DELAY_MS: u64 = 1600;
    const TIMEOUT_MS: u64 = 10_000;

    // If no paths expected, return early with an empty map
    if expected_paths.is_empty() {
        return Ok(HashMap::new());
    }

    let start = std::time::Instant::now();
    let mut delay_ms = INITIAL_DELAY_MS;
    let mut attempt = 0;

    loop {
        attempt += 1;

        // Fetch the UUID map
        let uuid_map = build_uuid_map_recursive(client, server, fs_root_id).await;

        // Check if all expected paths have UUIDs
        let missing: Vec<String> = expected_paths
            .iter()
            .filter(|path| !uuid_map.contains_key(*path))
            .cloned()
            .collect();

        if missing.is_empty() {
            info!(
                "UUID map ready after {} attempts ({:?}): {} UUIDs resolved",
                attempt,
                start.elapsed(),
                uuid_map.len()
            );
            return Ok(uuid_map);
        }

        // Check timeout
        let elapsed = start.elapsed();
        if elapsed.as_millis() >= TIMEOUT_MS as u128 {
            warn!(
                "UUID map timeout after {} attempts ({:?}): {} paths still missing UUIDs",
                attempt,
                elapsed,
                missing.len()
            );
            for path in &missing {
                debug!("  Missing UUID for: {}", path);
            }
            return Err(UuidMapTimeoutError {
                missing_paths: missing,
                elapsed,
            });
        }

        // Log progress
        debug!(
            "UUID map attempt {}: {}/{} paths resolved, waiting {}ms",
            attempt,
            expected_paths.len() - missing.len(),
            expected_paths.len(),
            delay_ms
        );

        // Wait with exponential backoff
        sleep(Duration::from_millis(delay_ms)).await;
        delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
    }
}
