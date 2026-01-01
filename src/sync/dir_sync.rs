//! Directory mode synchronization helpers.
//!
//! This module contains helper functions for syncing a directory
//! with a server document, including schema traversal and UUID mapping.

use crate::fs::{Entry, FsSchema};
use crate::sync::state_file::compute_content_hash;
use crate::sync::{
    build_head_url, detect_from_path, encode_node_id, is_allowed_extension, is_binary_content,
    spawn_file_sync_tasks, FileSyncState, HeadResponse, SyncState,
};
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Filename for the local schema JSON file in directory sync mode
pub const SCHEMA_FILENAME: &str = ".commonplace.json";

/// Write the schema JSON to the local .commonplace.json file
pub async fn write_schema_file(directory: &Path, schema_json: &str) -> Result<(), std::io::Error> {
    let schema_path = directory.join(SCHEMA_FILENAME);
    tokio::fs::write(&schema_path, schema_json).await?;
    info!("Wrote schema to {}", schema_path.display());
    Ok(())
}

/// Recursively collect file paths from an entry, including explicit node_id if present.
/// Returns Vec<(path, explicit_node_id)> where explicit_node_id is Some if DocEntry has node_id.
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

/// Recursively build a map of relative paths to UUIDs by fetching all schemas.
///
/// This function follows node-backed directories and fetches their schemas
/// to build a complete map of all file paths to their UUIDs.
pub async fn build_uuid_map_recursive(
    client: &Client,
    server: &str,
    doc_id: &str,
) -> HashMap<String, String> {
    let mut uuid_map = HashMap::new();
    build_uuid_map_from_doc(client, server, doc_id, "", &mut uuid_map).await;
    uuid_map
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
    // Fetch the schema from this document
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(doc_id));
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to fetch schema for {}: {}", doc_id, e);
            return;
        }
    };

    if !resp.status().is_success() {
        warn!(
            "Failed to fetch schema: {} (status {})",
            doc_id,
            resp.status()
        );
        return;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            warn!("Failed to parse schema response for {}: {}", doc_id, e);
            return;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            debug!("Document {} is not a schema ({}), skipping", doc_id, e);
            return;
        }
    };

    // Traverse the schema and collect UUIDs
    if let Some(ref root) = schema.root {
        collect_paths_with_node_backed_dirs(client, server, root, path_prefix, uuid_map).await;
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
    match entry {
        Entry::Dir(dir) => {
            // If this is a node-backed directory (entries: null, node_id: Some),
            // fetch its schema and continue recursively
            if dir.entries.is_none() {
                if let Some(ref node_id) = dir.node_id {
                    // This is a node-backed directory - fetch its schema
                    build_uuid_map_from_doc(client, server, node_id, prefix, uuid_map).await;
                }
            } else if let Some(ref entries) = dir.entries {
                // Inline directory - traverse its entries
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_paths_with_node_backed_dirs(
                        client,
                        server,
                        child,
                        &child_path,
                        uuid_map,
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

/// Handle a schema change from the server - create new local files.
///
/// When the server's fs-root schema changes (e.g., new files added by another client),
/// this function fetches the new schema, creates any missing local files, and optionally
/// spawns sync tasks for them.
pub async fn handle_schema_change(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    spawn_tasks: bool,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fetch current schema from server (fs-root schema always uses ID-based API)
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch fs-root HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;
    if head.content.is_empty() {
        return Ok(());
    }

    // Parse and validate schema BEFORE writing to disk
    // This prevents corrupting the local schema file with invalid/empty server content
    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse fs-root schema: {}", e);
            return Ok(());
        }
    };

    // Validate that schema has a populated root with entries - don't overwrite with empty/minimal schemas
    let has_valid_root = match &schema.root {
        Some(Entry::Dir(dir)) => dir.entries.is_some() || dir.node_id.is_some(),
        _ => false,
    };
    if !has_valid_root {
        warn!("Server returned schema without valid root (missing entries or node_id), not overwriting local schema");
        return Ok(());
    }

    // Only write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(directory, &head.content).await {
        warn!("Failed to write schema file: {}", e);
    }

    // Collect all paths from schema (with explicit node_id if present)
    let mut schema_paths: Vec<(String, Option<String>)> = Vec::new();
    if let Some(ref root) = schema.root {
        collect_paths_from_entry(root, "", &mut schema_paths);
    }

    // Check for new paths not in our state
    let known_paths: Vec<String> = {
        let states = file_states.read().await;
        states.keys().cloned().collect()
    };

    for (path, explicit_node_id) in &schema_paths {
        if !known_paths.contains(path) {
            // New file from server - create local file and fetch content
            let file_path = directory.join(path);

            // Skip files with disallowed extensions
            if !is_allowed_extension(&file_path) {
                debug!("Ignoring server file with disallowed extension: {}", path);
                continue;
            }

            // When use_paths=true, use path for /files/* API
            // When use_paths=false, use explicit node_id or derive as fs_root:path for /docs/* API
            let identifier = if use_paths {
                path.clone()
            } else {
                explicit_node_id
                    .clone()
                    .unwrap_or_else(|| format!("{}:{}", fs_root_id, path))
            };

            info!(
                "Server created new file: {} -> {}",
                path,
                file_path.display()
            );

            // Create parent directories if needed
            if let Some(parent) = file_path.parent() {
                if !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }

            // Fetch content from server
            let file_head_url = build_head_url(server, &identifier, use_paths);
            if let Ok(resp) = client.get(&file_head_url).send().await {
                if resp.status().is_success() {
                    if let Ok(file_head) = resp.json::<HeadResponse>().await {
                        // Detect if file is binary and decode base64 if needed
                        // Use both extension-based detection AND try decoding as base64
                        // to handle files that were uploaded as binary via content sniffing
                        // Track actual bytes written for consistent hash computation
                        use base64::{engine::general_purpose::STANDARD, Engine};
                        let content_info = detect_from_path(&file_path);
                        let bytes_written: Vec<u8> = if content_info.is_binary {
                            // Extension indicates binary - decode base64
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) => decoded,
                                Err(e) => {
                                    warn!("Failed to decode binary content: {}", e);
                                    file_head.content.as_bytes().to_vec()
                                }
                            }
                        } else {
                            // Extension says text, but try decoding as base64 in case
                            // this was a binary file detected by content sniffing
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) if is_binary_content(&decoded) => {
                                    // Successfully decoded and content is binary
                                    decoded
                                }
                                _ => {
                                    // Not base64 or not binary - write as text
                                    file_head.content.as_bytes().to_vec()
                                }
                            }
                        };
                        tokio::fs::write(&file_path, &bytes_written).await?;

                        // Hash the actual bytes written to disk for consistent fork detection
                        let content_hash = compute_content_hash(&bytes_written);

                        // Add to file states
                        let state = Arc::new(RwLock::new(SyncState {
                            last_written_cid: file_head.cid.clone(),
                            last_written_content: file_head.content,
                            current_write_id: 0,
                            pending_write: None,
                            needs_head_refresh: false,
                            state_file: None, // Directory mode doesn't use state files yet
                            state_file_path: None,
                        }));

                        info!("Created local file: {}", file_path.display());

                        // Spawn sync tasks for the new file (only if requested)
                        let task_handles = if spawn_tasks {
                            spawn_file_sync_tasks(
                                client.clone(),
                                server.to_string(),
                                identifier.clone(),
                                file_path.clone(),
                                state.clone(),
                                use_paths,
                            )
                        } else {
                            Vec::new()
                        };
                        let mut states = file_states.write().await;
                        states.insert(
                            path.clone(),
                            FileSyncState {
                                relative_path: path.clone(),
                                identifier,
                                state,
                                task_handles,
                                use_paths,
                                content_hash: Some(content_hash),
                            },
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
