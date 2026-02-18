//! File event handlers for directory sync mode.
//!
//! This module handles file creation, modification, and deletion events
//! during directory synchronization.

use crate::fs::{Entry, FsSchema};
use crate::mqtt::MqttRequestClient;
use crate::sync::crdt_new_file::add_directory_to_schema;
use crate::sync::directory::{scan_directory_to_json, ScanOptions};
use crate::sync::schema_io::{write_schema_file, SCHEMA_FILENAME};
use crate::sync::subscriptions::CrdtFileSyncContext;
use crate::sync::{normalize_path, remove_file_state_and_abort, FileSyncState};
use reqwest::Client;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, trace, warn};

/// Result of finding which document owns a given file path.
///
/// When files are in node-backed subdirectories, they belong to that subdirectory's
/// document rather than the root fs-root document.
pub struct OwningDocument {
    /// The document ID that owns this file (fs_root_id or a subdirectory's node_id)
    pub document_id: String,
    /// The path relative to the owning document's root
    pub relative_path: String,
    /// The directory on disk that corresponds to the owning document
    pub directory: PathBuf,
}

/// Find which document owns a file path, accounting for node-backed subdirectories.
///
/// When a file is in a node-backed subdirectory, the file belongs to that subdirectory's
/// document (identified by node_id) rather than the root fs-root document.
///
/// For example, if workspace/text-to-telegram is a node-backed directory:
/// - File: workspace/text-to-telegram/test.txt
/// - Returns: OwningDocument { document_id: "subdir-node-id", relative_path: "test.txt" }
///
/// If the file is not in a node-backed subdirectory:
/// - File: workspace/content.txt
/// - Returns: OwningDocument { document_id: fs_root_id, relative_path: "content.txt" }
pub fn find_owning_document(
    root_directory: &Path,
    fs_root_id: &str,
    relative_path: &str,
) -> OwningDocument {
    // Split path into components
    let components: Vec<&str> = relative_path.split('/').collect();
    trace!(
        "find_owning_document: path={}, components={:?}",
        relative_path,
        components
    );

    // Walk from root checking each directory for node-backed status
    let mut current_dir = root_directory.to_path_buf();
    let mut current_document_id = fs_root_id.to_string();
    let mut path_start_index = 0;

    // For each directory component (not the last one, which is the file)
    for (i, component) in components
        .iter()
        .take(components.len().saturating_sub(1))
        .enumerate()
    {
        trace!(
            "find_owning_document: checking component '{}' at index {}",
            component,
            i
        );

        // Check if parent has a .commonplace.json with this component as a node-backed entry
        let schema_path = current_dir.join(SCHEMA_FILENAME);
        if let Ok(content) = std::fs::read_to_string(&schema_path) {
            if let Ok(schema) = serde_json::from_str::<FsSchema>(&content) {
                if let Some(Entry::Dir(dir_entry)) = schema.root.as_ref() {
                    if let Some(ref entries) = dir_entry.entries {
                        trace!("find_owning_document: schema has {} entries", entries.len());
                        if let Some(entry) = entries.get(*component) {
                            trace!("find_owning_document: found entry for '{}'", component);
                            if let Entry::Dir(subdir) = entry {
                                trace!(
                                    "find_owning_document: entry is dir, node_id={:?}",
                                    subdir.node_id
                                );
                                if let Some(ref node_id) = subdir.node_id {
                                    // This is a node-backed directory!
                                    // The remaining path belongs to this document
                                    trace!(
                                        "find_owning_document: FOUND node-backed dir '{}' with id {}",
                                        component, node_id
                                    );
                                    current_document_id = node_id.clone();
                                    path_start_index = i + 1;
                                }
                            }
                        }
                    }
                }
            } else {
                debug!(
                    "find_owning_document: failed to parse schema from {:?}",
                    schema_path
                );
            }
        } else {
            trace!("find_owning_document: no schema at {:?}", schema_path);
        }

        // Move into this directory for next iteration
        current_dir = current_dir.join(component);
    }

    // Build the relative path within the owning document
    let remaining_path = if path_start_index < components.len() {
        components[path_start_index..].join("/")
    } else {
        String::new()
    };

    // Compute the directory for the owning document
    let owning_directory = if path_start_index == 0 {
        root_directory.to_path_buf()
    } else {
        let mut dir = root_directory.to_path_buf();
        for component in &components[0..path_start_index] {
            dir = dir.join(component);
        }
        dir
    };

    OwningDocument {
        document_id: current_document_id,
        relative_path: remaining_path,
        directory: owning_directory,
    }
}

/// Fetch a subdirectory's node_id from a parent schema via MQTT get-content.
async fn fetch_subdir_node_id_via_mqtt(
    mqtt_request: &MqttRequestClient,
    parent_doc_id: &str,
    subdir_name: &str,
) -> Option<String> {
    let response = match mqtt_request.get_content(parent_doc_id).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!(
                "Failed to fetch parent schema for {} via MQTT: {}",
                parent_doc_id, e
            );
            return None;
        }
    };

    if let Some(err) = response.error {
        warn!(
            "MQTT get-content returned error for parent {}: {}",
            parent_doc_id, err
        );
        return None;
    }

    let content = match response.content {
        Some(c) => c,
        None => {
            warn!(
                "MQTT get-content returned empty content for parent {}",
                parent_doc_id
            );
            return None;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&content) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Failed to parse parent schema {} from MQTT payload: {}",
                parent_doc_id, e
            );
            return None;
        }
    };

    if let Some(Entry::Dir(root_dir)) = schema.root.as_ref() {
        if let Some(entries) = &root_dir.entries {
            if let Some(Entry::Dir(subdir)) = entries.get(subdir_name) {
                return subdir.node_id.clone();
            }
        }
    }

    None
}

/// Ensure all parent directories of a file path exist as node-backed directories.
///
/// When a file is created in a new subdirectory (e.g., `newdir/file.txt`), we need to:
/// 1. Check if `newdir` exists in the parent's schema with a node_id
/// 2. If not, create a schema document via MQTT create-document
/// 3. Update the parent schema to include the directory with its node_id
///
/// In MQTT-only sync runtime, this creates missing directory documents via
/// MQTT create-document requests and publishes schema updates via CRDT edits.
///
/// `skip_http_schema_push` is retained for backward compatibility but HTTP
/// schema pushes are no longer performed from this watcher path.
#[allow(clippy::too_many_arguments)]
pub async fn ensure_parent_directories_exist(
    _client: &Client,
    _server: &str,
    fs_root_id: &str,
    root_directory: &Path,
    relative_file_path: &str,
    options: &ScanOptions,
    author: &str,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
    _skip_http_schema_push: bool,
    crdt_context: Option<&CrdtFileSyncContext>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "ensure_parent_directories_exist called for: {}",
        relative_file_path
    );
    let components: Vec<&str> = relative_file_path.split('/').collect();

    // If file is in root (no directory components), nothing to do
    if components.len() <= 1 {
        info!("File in root directory, no parent dirs needed");
        return Ok(());
    }

    // Walk through each directory component (not the file itself)
    let mut current_dir = root_directory.to_path_buf();
    let mut current_parent_id = fs_root_id.to_string();

    for dir_name in components.iter().take(components.len() - 1) {
        // Check if this directory has a node_id in the parent's schema
        let schema_path = current_dir.join(SCHEMA_FILENAME);
        let mut needs_creation = true;
        let mut existing_node_id: Option<String> = None;

        if let Ok(content) = std::fs::read_to_string(&schema_path) {
            if let Ok(schema) = serde_json::from_str::<FsSchema>(&content) {
                if let Some(Entry::Dir(dir_entry)) = schema.root.as_ref() {
                    if let Some(ref entries) = dir_entry.entries {
                        if let Some(Entry::Dir(subdir)) = entries.get(*dir_name) {
                            if let Some(ref node_id) = subdir.node_id {
                                needs_creation = false;
                                existing_node_id = Some(node_id.clone());
                                debug!("Directory '{}' already has node_id {}", dir_name, node_id);
                            }
                        }
                    }
                }
            }
        }

        if needs_creation {
            // Check server for existing directory (prevents duplicate UUIDs when local schema is stale)
            if let Some(ctx) = crdt_context {
                if let Some(server_node_id) =
                    fetch_subdir_node_id_via_mqtt(&ctx.mqtt_request, &current_parent_id, dir_name)
                        .await
                {
                    info!(
                        "Directory '{}' already exists on server with node_id {} (local schema was stale)",
                        dir_name, server_node_id
                    );
                    needs_creation = false;
                    existing_node_id = Some(server_node_id);
                }
            }
        }

        if needs_creation {
            let ctx = crdt_context.ok_or_else(|| {
                "CRDT context is required for parent directory creation in MQTT-only sync runtime"
            })?;
            info!(
                "Creating node-backed directory '{}' (parent: {})",
                dir_name, current_parent_id
            );

            let create_resp = ctx
                .mqtt_request
                .create_document("application/json", None)
                .await
                .map_err(|e| format!("Failed to create directory document via MQTT: {}", e))?;
            let new_node_id = create_resp.uuid.ok_or_else(|| {
                format!(
                    "MQTT create-document returned no uuid for '{}': {}",
                    dir_name,
                    create_resp
                        .error
                        .unwrap_or_else(|| "unknown error".to_string())
                )
            })?;

            info!(
                "Created document {} for directory '{}'",
                new_node_id, dir_name
            );

            // Update parent schema with the new directory entry
            if let Ok(json) = scan_directory_to_json(&current_dir, options) {
                let mut schema: serde_json::Value = serde_json::from_str(&json)?;
                if let Some(entries) = schema
                    .get_mut("root")
                    .and_then(|r| r.get_mut("entries"))
                    .and_then(|e| e.as_object_mut())
                {
                    if let Some(dir_entry) = entries.get_mut(*dir_name) {
                        dir_entry["node_id"] = serde_json::Value::String(new_node_id.clone());
                    }
                }

                let updated_json = serde_json::to_string(&schema)?;
                write_schema_file(&current_dir, &updated_json, written_schemas).await?;

                // Publish schema update via MQTT CRDT edit.
                let parent_uuid =
                    uuid::Uuid::parse_str(&current_parent_id).unwrap_or(uuid::Uuid::nil());
                let is_root = current_parent_id == fs_root_id;
                if is_root {
                    let mut state = ctx.crdt_state.write().await;
                    if let Err(e) = add_directory_to_schema(
                        &ctx.mqtt_client,
                        &ctx.workspace,
                        &mut state.schema,
                        dir_name,
                        &new_node_id,
                        author,
                    )
                    .await
                    {
                        warn!("Failed to publish directory schema via MQTT: {}", e);
                    }
                    if let Err(e) = state.save(root_directory).await {
                        warn!("Failed to save CRDT state after dir creation: {}", e);
                    }
                } else {
                    match ctx
                        .subdir_cache
                        .get_or_load(&current_dir, parent_uuid)
                        .await
                    {
                        Ok(subdir_state) => {
                            let mut state = subdir_state.write().await;
                            if let Err(e) = add_directory_to_schema(
                                &ctx.mqtt_client,
                                &ctx.workspace,
                                &mut state.schema,
                                dir_name,
                                &new_node_id,
                                author,
                            )
                            .await
                            {
                                warn!("Failed to publish subdirectory schema via MQTT: {}", e);
                            }
                            if let Err(e) = state.save(&current_dir).await {
                                warn!("Failed to save subdirectory CRDT state: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to load subdirectory state: {}", e);
                        }
                    }
                }
            }

            // Initialize the new directory's schema (empty) if it doesn't already exist
            let subdir_path = current_dir.join(dir_name);
            let subdir_schema_path = subdir_path.join(SCHEMA_FILENAME);
            let existing_schema = match std::fs::read_to_string(&subdir_schema_path) {
                Ok(content) => Some(content),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    return Err(format!(
                        "Failed to read schema at {:?}: {}",
                        subdir_schema_path, e
                    )
                    .into());
                }
            };

            if existing_schema.is_none() {
                let empty_schema = serde_json::json!({
                    "version": 1,
                    "root": {
                        "type": "dir",
                        "entries": {}
                    }
                });
                let schema_str = serde_json::to_string(&empty_schema)?;
                write_schema_file(&subdir_path, &schema_str, written_schemas).await?;
            } else {
                debug!(
                    "Subdirectory '{}' already has schema, skipping initial write/push",
                    dir_name
                );
            }

            // Wait briefly for peers to observe schema updates.
            sleep(Duration::from_millis(50)).await;

            existing_node_id = Some(new_node_id);
        }

        // Move to the next directory level
        current_dir = current_dir.join(dir_name);
        if let Some(node_id) = existing_node_id {
            current_parent_id = node_id;
        }

        debug!(
            "After processing '{}': current_parent_id = {}",
            dir_name, current_parent_id
        );
    }

    Ok(())
}

/// Handle a file modification event in directory sync mode.
///
/// Modified files are handled by per-file watchers, so this just updates
/// the schema in case metadata changed.
pub async fn handle_file_modified(
    _client: &Client,
    _server: &str,
    fs_root_id: &str,
    directory: &Path,
    path: &Path,
    options: &ScanOptions,
    _author: &str,
) {
    debug!("Directory event: file modified: {}", path.display());

    // Calculate relative path - canonicalize both paths first
    let canonical_dir = match directory.canonicalize() {
        Ok(d) => d,
        Err(_) => return,
    };
    let canonical_path = match path.canonicalize() {
        Ok(p) => p,
        Err(_) => return,
    };
    let relative_path = match canonical_path.strip_prefix(&canonical_dir) {
        Ok(rel) => normalize_path(&rel.to_string_lossy()),
        Err(_) => return,
    };

    // Find which document owns this file path
    let owning_doc = find_owning_document(directory, fs_root_id, &relative_path);

    let _ = scan_directory_to_json(&owning_doc.directory, options);
    debug!(
        "Skipping legacy schema push for modified file '{}' (MQTT-only runtime)",
        owning_doc.relative_path
    );
}

/// Handle a file deletion event in directory sync mode.
///
/// Stops sync tasks for the deleted file and updates the schema.
#[allow(clippy::too_many_arguments)]
pub async fn handle_file_deleted(
    _client: &Client,
    _server: &str,
    _fs_root_id: &str,
    directory: &Path,
    path: &Path,
    _options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    _author: &str,
) {
    debug!("Directory event: file deleted: {}", path.display());

    // Calculate relative path - canonicalize the directory, but the file may not exist
    // For deleted files, we need to strip the canonical dir prefix from the absolute path
    let canonical_dir = match directory.canonicalize() {
        Ok(d) => d,
        Err(_) => return,
    };
    // Try to make path absolute if it isn't already
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let relative_path = match absolute_path.strip_prefix(&canonical_dir) {
        Ok(rel) => normalize_path(&rel.to_string_lossy()),
        Err(_) => {
            warn!(
                "Could not strip prefix {} from {}",
                canonical_dir.display(),
                absolute_path.display()
            );
            return;
        }
    };

    // Stop sync tasks for this file and remove from file_states
    if remove_file_state_and_abort(file_states, &relative_path)
        .await
        .is_some()
    {
        info!("Stopping sync tasks for deleted file: {}", relative_path);
    }

    warn!(
        "Legacy delete handler invoked for '{}'; schema mutation is CRDT-only in MQTT runtime",
        relative_path
    );
}
