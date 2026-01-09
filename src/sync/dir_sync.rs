//! Directory mode synchronization helpers.
//!
//! This module contains helper functions for syncing a directory
//! with a server document, including schema traversal and UUID mapping.

use crate::fs::{Entry, FsSchema};
use crate::mqtt::{MqttClient, Topic};
use crate::sync::directory::{scan_directory, schema_to_json, ScanOptions};
use crate::sync::state_file::{
    compute_content_hash, load_synced_directories, mark_directory_synced, unmark_directory_synced,
};
use crate::sync::uuid_map::{
    build_uuid_map_and_write_schemas, build_uuid_map_recursive,
    build_uuid_map_recursive_with_status, fetch_node_id_from_schema,
};
use crate::sync::{
    build_head_url, delete_schema_entry, detect_from_path, encode_node_id, fork_node,
    is_allowed_extension, is_binary_content, looks_like_base64_binary, normalize_path,
    push_file_content, push_json_content, push_jsonl_content, push_schema_to_server,
    spawn_file_sync_tasks, FileSyncState, HeadResponse, SyncState,
};
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use reqwest_eventsource::{Error as SseError, Event as SseEvent, EventSource};
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Filename for the local schema JSON file in directory sync mode
pub const SCHEMA_FILENAME: &str = ".commonplace.json";

/// Write the schema JSON to the local .commonplace.json file.
///
/// This function compares the new schema with the existing local file
/// and skips the write if they are semantically equivalent, preventing
/// unnecessary file system events that could cause feedback loops.
pub async fn write_schema_file(directory: &Path, schema_json: &str) -> Result<(), std::io::Error> {
    let schema_path = directory.join(SCHEMA_FILENAME);

    // Check if existing schema is the same (prevents feedback loops)
    if schema_path.exists() {
        if let Ok(existing) = tokio::fs::read_to_string(&schema_path).await {
            // Compare as parsed JSON to ignore whitespace/formatting differences
            let existing_parsed: Result<serde_json::Value, _> = serde_json::from_str(&existing);
            let new_parsed: Result<serde_json::Value, _> = serde_json::from_str(schema_json);
            if let (Ok(existing_json), Ok(new_json)) = (existing_parsed, new_parsed) {
                if existing_json == new_json {
                    tracing::debug!(
                        "Schema unchanged, skipping write to {}",
                        schema_path.display()
                    );
                    return Ok(());
                }
            }
        }
    }

    tokio::fs::write(&schema_path, schema_json).await?;
    info!("Wrote schema to {}", schema_path.display());
    Ok(())
}

/// Clean up directories that exist on disk but not in the schema.
///
/// After deleting files that were removed from the schema, this function
/// removes any orphaned directories (directories that exist locally but
/// have no corresponding entry in the schema).
async fn cleanup_orphaned_directories(directory: &Path, schema: &FsSchema) {
    // Collect all directory names from the schema
    let schema_dirs: std::collections::HashSet<String> = collect_schema_directories(schema);

    // Read local directory entries
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(e) => e,
        Err(e) => {
            warn!("Failed to read directory {}: {}", directory.display(), e);
            return;
        }
    };

    let mut dirs_to_remove = Vec::new();

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };

        // Skip hidden directories and special directories
        if name.starts_with('.') {
            continue;
        }

        // If this directory isn't in the schema, mark it for removal
        if !schema_dirs.contains(&name) {
            dirs_to_remove.push((path, name));
        }
    }

    // Remove orphaned directories and unmark them from sync state
    for (dir_path, name) in dirs_to_remove {
        info!(
            "Removing orphaned directory (not in schema): {}",
            dir_path.display()
        );
        if let Err(e) = tokio::fs::remove_dir_all(&dir_path).await {
            warn!("Failed to remove directory {}: {}", dir_path.display(), e);
        }
        // Unmark this directory from synced state
        if let Err(e) = unmark_directory_synced(directory, &name).await {
            debug!("Failed to unmark directory synced: {}", e);
        }
    }
}

/// Handle a subdirectory schema change by fetching, validating, cleaning up, and
/// deleting files that were removed from the server.
///
/// This function handles SSE "edit" events for node-backed subdirectories by:
/// 1. Fetching the subdirectory schema from the server
/// 2. Validating and writing it to the local .commonplace.json file
/// 3. Deleting local files that no longer exist in the server schema
/// 4. Cleaning up orphaned directories not in the schema
///
/// File sync tasks for NEW files in subdirectories are tracked in CP-fs3x.
/// This function focuses on cleanup (deletions), not pulling new content.
pub async fn handle_subdir_schema_cleanup(
    client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    subdir_directory: &Path,
    root_directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fetch current schema from server
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(subdir_node_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch subdir HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;
    if head.content.is_empty() {
        return Ok(());
    }

    // Parse and validate schema BEFORE writing to disk
    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse subdir schema: {}", e);
            return Ok(());
        }
    };

    // Validate that schema has a populated root with entries
    let has_valid_root = match &schema.root {
        Some(Entry::Dir(dir)) => dir.entries.is_some() || dir.node_id.is_some(),
        _ => false,
    };
    if !has_valid_root {
        warn!("Server returned subdir schema without valid root, not overwriting local schema");
        return Ok(());
    }

    // Only write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(subdir_directory, &head.content).await {
        warn!("Failed to write subdir schema file: {}", e);
    }

    // Check if schema has any entries at all
    // If schema has entries but UUID map is empty, something is wrong (e.g., docs awaiting node_id)
    let schema_has_entries = if let Some(Entry::Dir(root_dir)) = &schema.root {
        if let Some(ref entries) = root_dir.entries {
            !entries.is_empty()
        } else {
            // All directories are node-backed - check if it has a node_id
            root_dir.node_id.is_some()
        }
    } else {
        false
    };

    // Build UUID map for the subdirectory (paths relative to subdir), tracking fetch success
    let (subdir_uuid_map, all_fetches_succeeded) =
        build_uuid_map_recursive_with_status(client, server, subdir_node_id).await;

    // Safety checks for deletion:
    // 1. If ANY fetch failed → map may be incomplete, skip deletions
    // 2. If schema has entries but UUID map is empty → entries may be waiting for node_id
    //    assignment by the reconciler, skip deletions during this race window
    let skip_deletions = if !all_fetches_succeeded {
        debug!(
            "Subdir {} had fetch failures during UUID map building - skipping file deletion to avoid data loss",
            subdir_path
        );
        true
    } else if schema_has_entries && subdir_uuid_map.is_empty() {
        debug!(
            "Subdir {} schema has entries but UUID map is empty - entries may be awaiting node_id, skipping deletions",
            subdir_path
        );
        true
    } else {
        false
    };

    if skip_deletions {
        // Don't proceed with deletions, just clean up orphaned directories
    } else {
        // Convert subdir-relative paths to root-relative paths for comparison with file_states
        let schema_paths: std::collections::HashSet<String> = subdir_uuid_map
            .keys()
            .map(|p| {
                if subdir_path.is_empty() {
                    p.clone()
                } else {
                    format!("{}/{}", subdir_path, p)
                }
            })
            .collect();

        // Find files in file_states that are under this subdir but no longer in schema
        let deleted_paths: Vec<String> = {
            let states = file_states.read().await;
            states
                .keys()
                .filter(|p| {
                    // Only consider files under this subdirectory
                    let is_in_subdir = if subdir_path.is_empty() {
                        true
                    } else {
                        p.starts_with(&format!("{}/", subdir_path))
                    };
                    is_in_subdir && !schema_paths.contains(*p)
                })
                .cloned()
                .collect()
        };

        // Delete files that were removed from server
        for path in &deleted_paths {
            info!(
                "Subdir {} removed file: {} - deleting local copy",
                subdir_path, path
            );
            let file_path = root_directory.join(path);

            // Stop sync tasks and remove from file_states
            {
                let mut states = file_states.write().await;
                if let Some(file_state) = states.remove(path) {
                    for handle in file_state.task_handles {
                        handle.abort();
                    }
                }
            }

            // Delete the file from disk
            if file_path.exists() && file_path.is_file() {
                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                    warn!("Failed to delete file {}: {}", file_path.display(), e);
                }
            }
        }
    }

    // Clean up directories that exist locally but not in the schema
    cleanup_orphaned_directories(subdir_directory, &schema).await;

    Ok(())
}

/// Handle syncing NEW files that appear in a subdirectory schema.
///
/// This is called from `subdir_sse_task` when a subdirectory schema changes.
/// It finds files in the server schema that don't exist locally and syncs them.
#[allow(clippy::too_many_arguments)]
pub async fn handle_subdir_new_files(
    client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    _subdir_directory: &Path,
    root_directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Don't pull if push_only mode
    if push_only {
        return Ok(());
    }

    // Fetch current schema from server
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(subdir_node_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch subdir HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;
    if head.content.is_empty() {
        return Ok(());
    }

    // Parse and validate schema (early return if invalid)
    let _schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse subdir schema: {}", e);
            return Ok(());
        }
    };

    // Build UUID map for the subdirectory
    let subdir_uuid_map = build_uuid_map_recursive(client, server, subdir_node_id).await;

    // Convert to path -> (subdir_relative_path, node_id) pairs
    // We need the subdir-relative path for API calls but root-relative path for file_states
    let schema_paths: Vec<(String, String, String)> = subdir_uuid_map
        .into_iter()
        .map(|(subdir_relative_path, node_id)| {
            let root_relative_path = if subdir_path.is_empty() {
                subdir_relative_path.clone()
            } else {
                format!("{}/{}", subdir_path, subdir_relative_path)
            };
            (root_relative_path, subdir_relative_path, node_id)
        })
        .collect();

    // Get known paths from file_states
    let known_paths: Vec<String> = {
        let states = file_states.read().await;
        states.keys().cloned().collect()
    };

    // Load synced directories from root_directory (where they're stored)
    // The synced_dirs contains root-relative paths like "bartleby/nested"
    let synced_dirs = load_synced_directories(root_directory).await;

    // Find and sync new files
    for (root_relative_path, _subdir_relative_path, node_id) in &schema_paths {
        if known_paths.contains(root_relative_path) {
            continue; // Already tracking this file
        }

        // Check if the file's parent directory was deleted locally
        // synced_dirs contains root-relative paths, so we check all ancestor directories
        let path_parts: Vec<&str> = root_relative_path.split('/').collect();
        let mut should_skip = false;
        for i in 1..path_parts.len() {
            // Check each parent directory (root-relative)
            let parent_dir: String = path_parts[..i].join("/");
            let parent_path = root_directory.join(&parent_dir);
            // Directory was synced but doesn't exist locally = user deleted it
            if synced_dirs.contains(&parent_dir) && !parent_path.exists() {
                debug!(
                    "Skipping file {} - parent directory '{}' was deleted locally",
                    root_relative_path, parent_dir
                );
                should_skip = true;
                break;
            }
        }
        if should_skip {
            continue;
        }

        // New file from server - create local file and fetch content
        let file_path = root_directory.join(root_relative_path);

        // Skip files with disallowed extensions
        if !is_allowed_extension(&file_path) {
            debug!(
                "Ignoring server file with disallowed extension: {}",
                root_relative_path
            );
            continue;
        }

        info!(
            "Subdir {} server created new file: {} -> {}",
            subdir_path,
            root_relative_path,
            file_path.display()
        );

        // Create parent directories if needed
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
                // Track the directories we created
                let mut current = parent;
                while let Ok(rel) = current.strip_prefix(root_directory) {
                    let rel_str = rel.to_string_lossy().to_string();
                    if !rel_str.is_empty() {
                        if let Err(e) = mark_directory_synced(root_directory, &rel_str).await {
                            debug!("Failed to mark directory synced: {}", e);
                        }
                    }
                    if let Some(p) = current.parent() {
                        current = p;
                    } else {
                        break;
                    }
                }
            }
        }

        // When use_paths=true, use path for /files/* API
        // When use_paths=false, use node_id for /docs/* API
        let identifier = if use_paths {
            root_relative_path.clone()
        } else {
            node_id.clone()
        };

        // Fetch content from server
        let file_head_url = build_head_url(server, &identifier, use_paths);
        if let Ok(resp) = client.get(&file_head_url).send().await {
            if resp.status().is_success() {
                if let Ok(file_head) = resp.json::<HeadResponse>().await {
                    // Detect if file is binary and decode base64 if needed
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
                    } else if looks_like_base64_binary(&file_head.content) {
                        // Extension says text, but content looks like base64 binary
                        match STANDARD.decode(&file_head.content) {
                            Ok(decoded) if is_binary_content(&decoded) => decoded,
                            _ => file_head.content.as_bytes().to_vec(),
                        }
                    } else {
                        file_head.content.as_bytes().to_vec()
                    };
                    tokio::fs::write(&file_path, &bytes_written).await?;

                    // Hash the actual bytes written to disk
                    let content_hash = compute_content_hash(&bytes_written);

                    // Add to file states
                    let state = Arc::new(RwLock::new(SyncState {
                        last_written_cid: file_head.cid.clone(),
                        last_written_content: file_head.content,
                        current_write_id: 0,
                        pending_write: None,
                        needs_head_refresh: false,
                        state_file: None,
                        state_file_path: None,
                    }));

                    info!("Created local file from subdir: {}", file_path.display());

                    // Spawn sync tasks for the new file
                    let task_handles = spawn_file_sync_tasks(
                        client.clone(),
                        server.to_string(),
                        identifier.clone(),
                        file_path.clone(),
                        state.clone(),
                        use_paths,
                        push_only,
                        pull_only,
                        false, // force_push: directory mode doesn't support force-push
                        #[cfg(unix)]
                        inode_tracker.clone(),
                    );

                    let mut states = file_states.write().await;
                    states.insert(
                        root_relative_path.clone(),
                        FileSyncState {
                            relative_path: root_relative_path.clone(),
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

    Ok(())
}

/// Detect directories that were synced but are now deleted locally.
///
/// This returns directories that:
/// 1. Exist in the schema
/// 2. Don't exist on disk
/// 3. Were previously synced (tracked in state file)
///
/// These directories should be removed from the schema because the user
/// intentionally deleted them locally.
pub async fn find_locally_deleted_directories(directory: &Path, schema: &FsSchema) -> Vec<String> {
    let schema_dirs = collect_schema_directories(schema);
    let synced_dirs = load_synced_directories(directory).await;

    let mut deleted = Vec::new();

    for dir_name in &schema_dirs {
        let dir_path = directory.join(dir_name);
        // Directory in schema but not on disk
        if !dir_path.exists() {
            // Check if it was previously synced
            if synced_dirs.contains(dir_name) {
                info!(
                    "Directory '{}' was synced but deleted locally - will remove from schema",
                    dir_name
                );
                deleted.push(dir_name.clone());
            }
        }
    }

    deleted
}

/// Collect all directory names from the schema's root entries.
fn collect_schema_directories(schema: &FsSchema) -> std::collections::HashSet<String> {
    let mut dirs = std::collections::HashSet::new();

    if let Some(Entry::Dir(root)) = &schema.root {
        if let Some(ref entries) = root.entries {
            for (name, entry) in entries {
                if matches!(entry, Entry::Dir(_)) {
                    dirs.insert(name.clone());
                }
            }
        }
    }

    dirs
}

/// Write nested .commonplace.json files for all node-backed directories.
///
/// When receiving a schema from the server that contains node-backed directories,
/// this function fetches each directory's content and writes it to a local
/// .commonplace.json file inside that subdirectory. This preserves the subdirectory
/// schemas locally so they can be restored if the server's database is cleared.
///
/// Uses content-based deduplication to prevent redundant writes and feedback loops.
pub async fn write_nested_schemas(
    client: &Client,
    server: &str,
    directory: &Path,
    schema: &FsSchema,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref root) = schema.root {
        // Track processed node_ids with their content hashes to prevent redundant fetches
        let mut processed_hashes: HashMap<String, String> = HashMap::new();
        write_nested_schemas_recursive(
            client,
            server,
            directory,
            root,
            directory,
            &mut processed_hashes,
        )
        .await?;
    }
    Ok(())
}

/// Recursively traverse the schema and write nested schemas for node-backed directories.
///
/// Uses a hash map to track already-processed node_ids and their content hashes,
/// preventing redundant fetches and writes within a single traversal.
#[async_recursion::async_recursion]
async fn write_nested_schemas_recursive(
    client: &Client,
    server: &str,
    _base_dir: &Path,
    entry: &Entry,
    current_dir: &Path,
    processed_hashes: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "write_nested_schemas_recursive: current_dir={:?}, entry_type={:?}",
        current_dir,
        match entry {
            Entry::Dir(_) => "dir",
            Entry::Doc(_) => "doc",
        }
    );

    if let Entry::Dir(dir) = entry {
        // If this directory has entries, iterate over them to find node-backed subdirectories
        if let Some(ref entries) = dir.entries {
            info!(
                "write_nested_schemas_recursive: processing {} entries in {:?}",
                entries.len(),
                current_dir
            );
            for (name, child_entry) in entries {
                if let Entry::Dir(child_dir) = child_entry {
                    if let Some(ref node_id) = child_dir.node_id {
                        info!(
                            "write_nested_schemas_recursive: found node-backed dir '{}' with node_id={}",
                            name, node_id
                        );

                        // Skip if we've already processed this node_id in this traversal
                        if processed_hashes.contains_key(node_id) {
                            debug!(
                                "Skipping already-processed subdirectory schema: {}",
                                node_id
                            );
                            continue;
                        }

                        // Calculate the subdirectory path
                        let subdir_path = current_dir.join(name);

                        // Fetch the directory's schema from server
                        let head_url = format!("{}/docs/{}/head", server, encode_node_id(node_id));
                        info!(
                            "write_nested_schemas_recursive: fetching schema from {}",
                            head_url
                        );
                        match client.get(&head_url).send().await {
                            Ok(resp) => {
                                if resp.status().is_success() {
                                    match resp.json::<HeadResponse>().await {
                                        Ok(head) => {
                                            // Compute content hash and check if content has changed
                                            let content_hash =
                                                compute_content_hash(head.content.as_bytes());

                                            // Mark as processed with current hash
                                            processed_hashes.insert(node_id.clone(), content_hash);

                                            // Create directory for node-backed subdirectory (even if schema is empty)
                                            if !subdir_path.exists() {
                                                info!(
                                                    "Creating directory for node-backed subdirectory: {:?}",
                                                    subdir_path
                                                );
                                                if let Err(e) =
                                                    tokio::fs::create_dir_all(&subdir_path).await
                                                {
                                                    warn!(
                                                        "Failed to create directory for nested schema {:?}: {}",
                                                        subdir_path, e
                                                    );
                                                }
                                            } else {
                                                debug!(
                                                    "Directory already exists: {:?}",
                                                    subdir_path
                                                );
                                            }

                                            // Only write schema if content is valid (not just "{}")
                                            if !head.content.is_empty() && head.content != "{}" {
                                                info!(
                                                    "write_nested_schemas_recursive: parsing schema for {} (content len: {})",
                                                    node_id, head.content.len()
                                                );
                                                match serde_json::from_str::<FsSchema>(
                                                    &head.content,
                                                ) {
                                                    Ok(sub_schema) => {
                                                        info!(
                                                            "write_nested_schemas_recursive: parsed schema, root={:?}",
                                                            sub_schema.root.is_some()
                                                        );
                                                        // Write the schema to the subdirectory (write_schema_file has its own dedup)
                                                        if subdir_path.exists() {
                                                            if let Err(e) = write_schema_file(
                                                                &subdir_path,
                                                                &head.content,
                                                            )
                                                            .await
                                                            {
                                                                warn!(
                                                                    "Failed to write nested schema to {:?}: {}",
                                                                    subdir_path, e
                                                                );
                                                            }
                                                        }
                                                        // Recursively handle any nested node-backed directories
                                                        if let Some(ref sub_root) = sub_schema.root
                                                        {
                                                            info!(
                                                                "write_nested_schemas_recursive: recursing into {:?}",
                                                                subdir_path
                                                            );
                                                            write_nested_schemas_recursive(
                                                                client,
                                                                server,
                                                                _base_dir,
                                                                sub_root,
                                                                &subdir_path,
                                                                processed_hashes,
                                                            )
                                                            .await?;
                                                        } else {
                                                            info!(
                                                                "write_nested_schemas_recursive: no root in schema for {}",
                                                                node_id
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        warn!(
                                                            "Failed to parse schema for {}: {} (content: {})",
                                                            node_id, e, &head.content[..100.min(head.content.len())]
                                                        );
                                                    }
                                                }
                                            } else {
                                                debug!(
                                                    "Schema for {} is empty, skipping recursion",
                                                    node_id
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to parse HEAD response for {}: {}",
                                                node_id, e
                                            );
                                        }
                                    }
                                } else {
                                    warn!(
                                        "Failed to fetch schema for {}: status {}",
                                        node_id,
                                        resp.status()
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Failed to fetch schema for {}: {}", node_id, e);
                            }
                        }
                    }
                }
            }
        } else {
            debug!(
                "write_nested_schemas_recursive: no entries in {:?}",
                current_dir
            );
        }
    }
    Ok(())
}

/// Create directories for node-backed subdirectories within a subdirectory.
///
/// This function fetches the schema for a subdirectory and creates local directories
/// for any node-backed child directories that don't exist locally yet.
/// It's called from the subdir SSE task when a subdirectory schema changes.
pub async fn create_subdir_nested_directories(
    client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fetch the subdirectory's schema from server
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(subdir_node_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch subdir schema: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;
    if head.content.is_empty() || head.content == "{}" {
        return Ok(());
    }

    let schema: FsSchema = serde_json::from_str(&head.content)?;

    if let Some(ref root) = schema.root {
        if let Entry::Dir(dir) = root {
            if let Some(ref entries) = dir.entries {
                for (name, child_entry) in entries {
                    if let Entry::Dir(child_dir) = child_entry {
                        if child_dir.node_id.is_some() {
                            // This is a node-backed directory - create it if it doesn't exist
                            let child_path = subdir_directory.join(name);
                            if !child_path.exists() {
                                info!(
                                    "Creating directory for node-backed subdirectory: {:?}",
                                    child_path
                                );
                                if let Err(e) = tokio::fs::create_dir_all(&child_path).await {
                                    warn!("Failed to create directory {:?}: {}", child_path, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Push nested .commonplace.json files from local subdirectories to their server documents.
///
/// This function walks the directory tree looking for subdirectories that have:
/// 1. A local .commonplace.json file
/// 2. A corresponding node_id in the parent's schema
///
/// For each match, it pushes the local nested schema to the server document.
/// This restores node-backed directory contents after a server database clear.
pub async fn push_nested_schemas(
    client: &Client,
    server: &str,
    directory: &Path,
    schema: &FsSchema,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut pushed_count = 0;
    if let Some(ref root) = schema.root {
        pushed_count =
            push_nested_schemas_recursive(client, server, directory, root, directory).await?;
    }
    if pushed_count > 0 {
        info!("Pushed {} nested schemas to server", pushed_count);
    }
    Ok(pushed_count)
}

/// Recursively push nested schemas for node-backed directories.
#[async_recursion::async_recursion]
async fn push_nested_schemas_recursive(
    client: &Client,
    server: &str,
    _base_dir: &Path,
    entry: &Entry,
    current_dir: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut pushed_count = 0;

    if let Entry::Dir(dir) = entry {
        // Handle node-backed directory - check for local schema and push
        if let Some(ref node_id) = dir.node_id {
            let nested_schema_path = current_dir.join(SCHEMA_FILENAME);
            if nested_schema_path.exists() {
                // Read the local nested schema
                if let Ok(local_schema) = tokio::fs::read_to_string(&nested_schema_path).await {
                    // Validate it's a proper schema
                    if let Ok(parsed_schema) = serde_json::from_str::<FsSchema>(&local_schema) {
                        // Check if server document is empty or has different content
                        let head_url = format!("{}/docs/{}/head", server, encode_node_id(node_id));
                        let should_push = if let Ok(resp) = client.get(&head_url).send().await {
                            if resp.status().is_success() {
                                if let Ok(head) = resp.json::<HeadResponse>().await {
                                    // Push if server is empty or has trivial content
                                    head.content.is_empty() || head.content == "{}"
                                } else {
                                    true
                                }
                            } else {
                                true
                            }
                        } else {
                            true
                        };

                        if should_push {
                            info!(
                                "Pushing nested schema from {:?} to document {}",
                                nested_schema_path, node_id
                            );
                            if let Err(e) =
                                push_schema_to_server(client, server, node_id, &local_schema).await
                            {
                                warn!(
                                    "Failed to push nested schema for {}: {}",
                                    current_dir.display(),
                                    e
                                );
                            } else {
                                pushed_count += 1;
                            }
                        }

                        // Recursively handle any nested node-backed directories within this schema
                        if let Some(ref sub_root) = parsed_schema.root {
                            pushed_count += push_nested_schemas_recursive(
                                client,
                                server,
                                _base_dir,
                                sub_root,
                                current_dir,
                            )
                            .await?;
                        }
                    }
                }
            }
        }

        // Note: inline subdirectories were deprecated - all directories are now node-backed
    }

    Ok(pushed_count)
}

/// Handle a schema change from the server - create new local files.
///
/// When the server's fs-root schema changes (e.g., new files added by another client),
/// this function fetches the new schema, creates any missing local files, and optionally
/// spawns sync tasks for them.
#[allow(clippy::too_many_arguments)]
pub async fn handle_schema_change(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    spawn_tasks: bool,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
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
    // Use async version that follows node-backed directories to get complete path->UUID map
    // AND writes nested schema files to local directory (required for find_owning_document fallback)
    let (uuid_map, _all_succeeded) =
        build_uuid_map_and_write_schemas(client, server, fs_root_id, directory).await;
    let schema_paths: Vec<(String, Option<String>)> = uuid_map
        .into_iter()
        .map(|(path, node_id)| (path, Some(node_id)))
        .collect();

    // Check for new paths not in our state
    let known_paths: Vec<String> = {
        let states = file_states.read().await;
        states.keys().cloned().collect()
    };

    // Find directories that were synced but are now deleted locally
    // These should not be recreated (user intentionally deleted them)
    let locally_deleted_dirs = find_locally_deleted_directories(directory, &schema).await;

    for (path, explicit_node_id) in &schema_paths {
        if !known_paths.contains(path) {
            // Check if the file's parent directory was deleted locally
            // If so, skip creating this file (it will be removed from schema)
            let path_parts: Vec<&str> = path.split('/').collect();
            if path_parts.len() > 1 {
                let parent_dir = path_parts[0];
                if locally_deleted_dirs.contains(&parent_dir.to_string()) {
                    debug!(
                        "Skipping file {} - parent directory '{}' was deleted locally",
                        path, parent_dir
                    );
                    continue;
                }
            }

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

            // Create parent directories if needed and track them
            if let Some(parent) = file_path.parent() {
                if !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                    // Track the directories we created
                    let mut current = parent;
                    while let Ok(rel) = current.strip_prefix(directory) {
                        let rel_str = rel.to_string_lossy().to_string();
                        if !rel_str.is_empty() {
                            if let Err(e) = mark_directory_synced(directory, &rel_str).await {
                                debug!("Failed to mark directory synced: {}", e);
                            }
                        }
                        if let Some(p) = current.parent() {
                            current = p;
                        } else {
                            break;
                        }
                    }
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
                        } else if looks_like_base64_binary(&file_head.content) {
                            // Extension says text, but content looks like base64 binary
                            // This handles files that were detected as binary on upload
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) if is_binary_content(&decoded) => {
                                    // Successfully decoded and content is binary
                                    decoded
                                }
                                _ => {
                                    // Decode failed or not binary - write as text
                                    file_head.content.as_bytes().to_vec()
                                }
                            }
                        } else {
                            // Extension says text, content doesn't look like base64 binary
                            file_head.content.as_bytes().to_vec()
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
                                push_only,
                                pull_only,
                                false, // force_push: directory mode doesn't support force-push
                                #[cfg(unix)]
                                inode_tracker.clone(),
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

    // Delete local files/directories that have been removed from server schema
    let schema_path_set: std::collections::HashSet<&String> =
        schema_paths.iter().map(|(p, _)| p).collect();

    // Find files that exist locally but not in schema
    let deleted_paths: Vec<String> = known_paths
        .iter()
        .filter(|p| !schema_path_set.contains(p))
        .cloned()
        .collect();

    for path in &deleted_paths {
        info!("Server removed file: {} - deleting local copy", path);
        let file_path = directory.join(path);

        // Stop sync tasks and remove from file_states
        {
            let mut states = file_states.write().await;
            if let Some(file_state) = states.remove(path) {
                for handle in file_state.task_handles {
                    handle.abort();
                }
            }
        }

        // Delete the file from disk
        if file_path.exists() && file_path.is_file() {
            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                warn!("Failed to delete file {}: {}", file_path.display(), e);
            }
        }
    }

    // Check for directories that should be removed (empty after file deletions,
    // or directories that exist on disk but have no corresponding entries in schema)
    cleanup_orphaned_directories(directory, &schema).await;

    // Unmark locally deleted directories from sync state so they don't keep being flagged
    for dir_name in &locally_deleted_dirs {
        if let Err(e) = unmark_directory_synced(directory, dir_name).await {
            debug!("Failed to unmark deleted directory: {}", e);
        }
    }

    // Write nested schemas for node-backed directories to local subdirectories.
    // This persists subdirectory schemas so they survive server restarts.
    // NOTE: This must be called AFTER the file sync loop above which creates directories.
    if let Err(e) = write_nested_schemas(client, server, directory, &schema).await {
        warn!("Failed to write nested schemas: {}", e);
    }

    Ok(())
}

/// SSE task for directory-level events (watching fs-root).
///
/// This task subscribes to the fs-root document's SSE stream and handles
/// schema change events, triggering handle_schema_change to sync new files.
///
/// When new node-backed subdirectories are discovered, this task dynamically
/// spawns `subdir_sse_task` for them to watch their schemas.
#[allow(clippy::too_many_arguments)]
pub async fn directory_sse_task(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
) {
    // fs-root schema subscription always uses ID-based API
    let sse_url = format!("{}/sse/docs/{}", server, encode_node_id(&fs_root_id));

    // Track last processed schema to prevent redundant processing
    let mut last_schema_hash: Option<String> = None;

    'reconnect: loop {
        info!("Connecting to fs-root SSE: {}", sse_url);

        let request_builder = client.get(&sse_url);
        let mut es = match EventSource::new(request_builder) {
            Ok(es) => es,
            Err(e) => {
                error!("Failed to create fs-root EventSource: {}", e);
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        while let Some(event) = es.next().await {
            match event {
                Ok(SseEvent::Open) => {
                    info!("fs-root SSE connection opened");
                }
                Ok(SseEvent::Message(msg)) => {
                    debug!("fs-root SSE event: {} - {}", msg.event, msg.data);

                    match msg.event.as_str() {
                        "connected" => {
                            info!("fs-root SSE connected");
                        }
                        "edit" => {
                            // Schema changed on server, sync new files to local
                            // Use content-based deduplication to prevent feedback loops
                            match handle_schema_change_with_dedup(
                                &client,
                                &server,
                                &fs_root_id,
                                &directory,
                                &file_states,
                                true, // spawn_tasks: true for runtime schema changes
                                use_paths,
                                &mut last_schema_hash,
                                push_only,
                                pull_only,
                                #[cfg(unix)]
                                inode_tracker.clone(),
                            )
                            .await
                            {
                                Ok(true) => {
                                    debug!("Schema change processed successfully");
                                }
                                Ok(false) => {
                                    debug!("Schema unchanged, skipped processing");
                                }
                                Err(e) => {
                                    warn!("Failed to handle schema change: {}", e);
                                }
                            }

                            // Check for newly discovered node-backed subdirs and spawn SSE tasks
                            // (Done outside the match to avoid holding non-Send error across await)
                            if !push_only {
                                let all_subdirs = crate::sync::get_all_node_backed_dir_ids(
                                    &client,
                                    &server,
                                    &fs_root_id,
                                )
                                .await;

                                let mut watched = watched_subdirs.write().await;
                                for (subdir_path, subdir_node_id) in all_subdirs {
                                    if !watched.contains(&subdir_node_id) {
                                        info!(
                                            "Spawning SSE task for newly discovered subdir: {} ({})",
                                            subdir_path, subdir_node_id
                                        );
                                        watched.insert(subdir_node_id.clone());
                                        tokio::spawn(subdir_sse_task(
                                            client.clone(),
                                            server.clone(),
                                            fs_root_id.clone(),
                                            subdir_path,
                                            subdir_node_id,
                                            directory.clone(),
                                            file_states.clone(),
                                            use_paths,
                                            push_only,
                                            pull_only,
                                            #[cfg(unix)]
                                            inode_tracker.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                        "closed" => {
                            warn!("fs-root SSE: Target node shut down");
                            break;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    // Handle 404 gracefully - document may not exist yet
                    if let SseError::InvalidStatusCode(status, _) = &e {
                        if *status == StatusCode::NOT_FOUND {
                            debug!("fs-root SSE: Document not found (404), retrying in 1s...");
                            sleep(Duration::from_secs(1)).await;
                            continue 'reconnect; // Skip outer 5s sleep
                        }
                    }
                    error!("fs-root SSE error: {}", e);
                    break;
                }
            }
        }

        debug!("fs-root SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// SSE task for a node-backed subdirectory.
///
/// This task subscribes to a subdirectory's SSE stream and handles schema changes.
/// When the subdirectory's schema changes, it:
/// 1. Fetches and writes the updated schema to the local .commonplace.json file
/// 2. Deletes local files that were removed from the server schema
/// 3. Cleans up orphaned directories that no longer exist in the schema
/// 4. Syncs NEW files that were added to the server schema
#[allow(clippy::too_many_arguments)]
pub async fn subdir_sse_task(
    client: Client,
    server: String,
    _fs_root_id: String,
    subdir_path: String,
    subdir_node_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) {
    let sse_url = format!("{}/sse/docs/{}", server, encode_node_id(&subdir_node_id));

    'reconnect: loop {
        info!(
            "Connecting to subdir SSE: {} (path: {})",
            sse_url, subdir_path
        );

        let request_builder = client.get(&sse_url);
        let mut es = match EventSource::new(request_builder) {
            Ok(es) => es,
            Err(e) => {
                error!(
                    "Failed to create subdir EventSource for {}: {}",
                    subdir_path, e
                );
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        while let Some(event) = es.next().await {
            match event {
                Ok(SseEvent::Open) => {
                    debug!("Subdir {} SSE connection opened", subdir_path);
                }
                Ok(SseEvent::Message(msg)) => {
                    debug!(
                        "Subdir {} SSE event: {} - {}",
                        subdir_path, msg.event, msg.data
                    );

                    match msg.event.as_str() {
                        "connected" => {
                            debug!("Subdir {} SSE connected", subdir_path);
                        }
                        "edit" => {
                            // Subdirectory schema changed - handle cleanup and sync new files
                            info!("Subdir {} schema changed, triggering sync", subdir_path);
                            let subdir_full_path = directory.join(&subdir_path);

                            // First, cleanup deleted files and orphaned directories
                            match handle_subdir_schema_cleanup(
                                &client,
                                &server,
                                &subdir_node_id,
                                &subdir_path,
                                &subdir_full_path,
                                &directory,
                                &file_states,
                            )
                            .await
                            {
                                Ok(()) => {
                                    debug!("Subdir {} schema cleanup completed", subdir_path);
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to handle subdir {} schema cleanup: {}",
                                        subdir_path, e
                                    );
                                }
                            }

                            // Then, sync NEW files from server
                            match handle_subdir_new_files(
                                &client,
                                &server,
                                &subdir_node_id,
                                &subdir_path,
                                &subdir_full_path,
                                &directory,
                                &file_states,
                                use_paths,
                                push_only,
                                pull_only,
                                #[cfg(unix)]
                                inode_tracker.clone(),
                            )
                            .await
                            {
                                Ok(()) => {
                                    debug!("Subdir {} new files sync completed", subdir_path);
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to sync new files for subdir {}: {}",
                                        subdir_path, e
                                    );
                                }
                            }

                            // Also create directories for any NEW node-backed subdirectories
                            // This ensures directories like 0-commonplace get created when added
                            if let Err(e) = create_subdir_nested_directories(
                                &client,
                                &server,
                                &subdir_node_id,
                                &subdir_full_path,
                            )
                            .await
                            {
                                warn!(
                                    "Failed to create nested directories for subdir {}: {}",
                                    subdir_path, e
                                );
                            }
                        }
                        "closed" => {
                            warn!("Subdir {} SSE: Target node shut down", subdir_path);
                            break;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    // Handle 404 gracefully - document may not exist yet
                    if let SseError::InvalidStatusCode(status, _) = &e {
                        if *status == StatusCode::NOT_FOUND {
                            debug!(
                                "Subdir {} SSE: Document not found (404), retrying in 1s...",
                                subdir_path
                            );
                            sleep(Duration::from_secs(1)).await;
                            continue 'reconnect; // Skip outer 5s sleep for 404
                        }
                    }
                    error!("Subdir {} SSE error: {}", subdir_path, e);
                    break;
                }
            }
        }

        debug!(
            "Subdir {} SSE connection closed, reconnecting in 5s...",
            subdir_path
        );
        sleep(Duration::from_secs(5)).await;
    }
}

/// MQTT task for directory-level events (watching fs-root via MQTT).
///
/// This task subscribes to the fs-root document's edits topic via MQTT and handles
/// schema change events, triggering handle_schema_change to sync new files.
///
/// When new node-backed subdirectories are discovered, this task dynamically
/// spawns tasks for them to watch their schemas.
///
/// This is the MQTT equivalent of `directory_sse_task`.
#[allow(clippy::too_many_arguments)]
pub async fn directory_mqtt_task(
    http_client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
) {
    // Subscribe to edits for the fs-root document
    let edits_topic = Topic::edits(&workspace, &fs_root_id);
    let topic_str = edits_topic.to_topic_string();

    info!(
        "Subscribing to MQTT edits for fs-root {} at topic: {}",
        fs_root_id, topic_str
    );

    if let Err(e) = mqtt_client.subscribe(&topic_str, QoS::AtLeastOnce).await {
        error!("Failed to subscribe to MQTT edits topic: {}", e);
        return;
    }

    // Get a receiver for incoming messages
    let mut message_rx = mqtt_client.subscribe_messages();

    // Track last processed schema to prevent redundant processing
    let mut last_schema_hash: Option<String> = None;

    // Process incoming MQTT messages
    loop {
        match message_rx.recv().await {
            Ok(msg) => {
                // Check if this message is for our topic
                if msg.topic == topic_str {
                    debug!(
                        "MQTT edit received for fs-root: {} bytes",
                        msg.payload.len()
                    );

                    // An edit message means the schema has changed
                    // Use content-based deduplication to prevent feedback loops
                    match handle_schema_change_with_dedup(
                        &http_client,
                        &server,
                        &fs_root_id,
                        &directory,
                        &file_states,
                        true, // spawn_tasks: true for runtime schema changes
                        use_paths,
                        &mut last_schema_hash,
                        push_only,
                        pull_only,
                        #[cfg(unix)]
                        inode_tracker.clone(),
                    )
                    .await
                    {
                        Ok(true) => {
                            debug!("MQTT: Schema change processed successfully");
                        }
                        Ok(false) => {
                            debug!("MQTT: Schema unchanged, skipped processing");
                        }
                        Err(e) => {
                            warn!("MQTT: Failed to handle schema change: {}", e);
                        }
                    }

                    // Check for newly discovered node-backed subdirs and spawn tasks
                    if !push_only {
                        let all_subdirs = crate::sync::get_all_node_backed_dir_ids(
                            &http_client,
                            &server,
                            &fs_root_id,
                        )
                        .await;

                        let mut watched = watched_subdirs.write().await;
                        for (subdir_path, subdir_node_id) in all_subdirs {
                            if !watched.contains(&subdir_node_id) {
                                info!(
                                    "Spawning MQTT task for newly discovered subdir: {} ({})",
                                    subdir_path, subdir_node_id
                                );
                                watched.insert(subdir_node_id.clone());
                                tokio::spawn(subdir_mqtt_task(
                                    http_client.clone(),
                                    server.clone(),
                                    fs_root_id.clone(),
                                    subdir_path,
                                    subdir_node_id,
                                    directory.clone(),
                                    file_states.clone(),
                                    use_paths,
                                    push_only,
                                    pull_only,
                                    #[cfg(unix)]
                                    inode_tracker.clone(),
                                    mqtt_client.clone(),
                                    workspace.clone(),
                                ));
                            }
                        }
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("MQTT message receiver lagged by {} messages", n);
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                info!("MQTT message channel closed");
                break;
            }
        }
    }
}

/// MQTT task for a node-backed subdirectory.
///
/// This task subscribes to a subdirectory's edits topic via MQTT and handles schema changes.
/// When the subdirectory's schema changes, it:
/// 1. Fetches and writes the updated schema to the local .commonplace.json file
/// 2. Deletes local files that were removed from the server schema
/// 3. Cleans up orphaned directories that no longer exist in the schema
/// 4. Syncs NEW files that were added to the server schema
///
/// This is the MQTT equivalent of `subdir_sse_task`.
#[allow(clippy::too_many_arguments)]
pub async fn subdir_mqtt_task(
    http_client: Client,
    server: String,
    _fs_root_id: String,
    subdir_path: String,
    subdir_node_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
) {
    // Subscribe to edits for the subdirectory document
    let edits_topic = Topic::edits(&workspace, &subdir_node_id);
    let topic_str = edits_topic.to_topic_string();

    info!(
        "Subscribing to MQTT edits for subdir {} at topic: {}",
        subdir_path, topic_str
    );

    if let Err(e) = mqtt_client.subscribe(&topic_str, QoS::AtLeastOnce).await {
        error!(
            "Failed to subscribe to MQTT edits topic for subdir {}: {}",
            subdir_path, e
        );
        return;
    }

    // Get a receiver for incoming messages
    let mut message_rx = mqtt_client.subscribe_messages();

    // Process incoming MQTT messages
    loop {
        match message_rx.recv().await {
            Ok(msg) => {
                // Check if this message is for our topic
                if msg.topic == topic_str {
                    debug!(
                        "MQTT edit received for subdir {}: {} bytes",
                        subdir_path,
                        msg.payload.len()
                    );

                    // Subdirectory schema changed - handle cleanup and sync new files
                    info!(
                        "MQTT: Subdir {} schema changed, triggering sync",
                        subdir_path
                    );
                    let subdir_full_path = directory.join(&subdir_path);

                    // First, cleanup deleted files and orphaned directories
                    match handle_subdir_schema_cleanup(
                        &http_client,
                        &server,
                        &subdir_node_id,
                        &subdir_path,
                        &subdir_full_path,
                        &directory,
                        &file_states,
                    )
                    .await
                    {
                        Ok(()) => {
                            debug!("MQTT: Subdir {} schema cleanup completed", subdir_path);
                        }
                        Err(e) => {
                            warn!(
                                "MQTT: Failed to handle subdir {} schema cleanup: {}",
                                subdir_path, e
                            );
                        }
                    }

                    // Then, sync NEW files from server
                    match handle_subdir_new_files(
                        &http_client,
                        &server,
                        &subdir_node_id,
                        &subdir_path,
                        &subdir_full_path,
                        &directory,
                        &file_states,
                        use_paths,
                        push_only,
                        pull_only,
                        #[cfg(unix)]
                        inode_tracker.clone(),
                    )
                    .await
                    {
                        Ok(()) => {
                            debug!("MQTT: Subdir {} new files sync completed", subdir_path);
                        }
                        Err(e) => {
                            warn!(
                                "MQTT: Failed to sync new files for subdir {}: {}",
                                subdir_path, e
                            );
                        }
                    }

                    // Also create directories for any NEW node-backed subdirectories
                    if let Err(e) = create_subdir_nested_directories(
                        &http_client,
                        &server,
                        &subdir_node_id,
                        &subdir_full_path,
                    )
                    .await
                    {
                        warn!(
                            "MQTT: Failed to create nested directories for subdir {}: {}",
                            subdir_path, e
                        );
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!(
                    "MQTT message receiver for subdir {} lagged by {} messages",
                    subdir_path, n
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                info!("MQTT message channel closed for subdir {}", subdir_path);
                break;
            }
        }
    }
}

/// Handle schema change with content-based deduplication.
///
/// Returns Ok(true) if schema was processed, Ok(false) if skipped due to no change.
#[allow(clippy::too_many_arguments)]
async fn handle_schema_change_with_dedup(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    spawn_tasks: bool,
    use_paths: bool,
    last_schema_hash: &mut Option<String>,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Fetch current schema from server
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch fs-root HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;
    if head.content.is_empty() {
        return Ok(false);
    }

    // Compute hash of schema content for deduplication
    let current_hash = compute_content_hash(head.content.as_bytes());

    // Skip if schema hasn't changed
    if let Some(ref last_hash) = last_schema_hash {
        if &current_hash == last_hash {
            return Ok(false);
        }
    }

    // Delegate to the regular handler
    // Note: hash is updated AFTER successful processing to ensure retries on failure
    handle_schema_change(
        client,
        server,
        fs_root_id,
        directory,
        file_states,
        spawn_tasks,
        use_paths,
        push_only,
        pull_only,
        #[cfg(unix)]
        inode_tracker,
    )
    .await?;

    // Update last hash only after successful processing
    *last_schema_hash = Some(current_hash);

    Ok(true)
}

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
    info!(
        "find_owning_document: path={}, components={:?}",
        relative_path, components
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
        info!(
            "find_owning_document: checking component '{}' at index {}",
            component, i
        );

        // Check if parent has a .commonplace.json with this component as a node-backed entry
        let schema_path = current_dir.join(SCHEMA_FILENAME);
        if let Ok(content) = std::fs::read_to_string(&schema_path) {
            if let Ok(schema) = serde_json::from_str::<FsSchema>(&content) {
                if let Some(Entry::Dir(dir_entry)) = schema.root.as_ref() {
                    if let Some(ref entries) = dir_entry.entries {
                        info!("find_owning_document: schema has {} entries", entries.len());
                        if let Some(entry) = entries.get(*component) {
                            info!("find_owning_document: found entry for '{}'", component);
                            if let Entry::Dir(subdir) = entry {
                                info!(
                                    "find_owning_document: entry is dir, node_id={:?}",
                                    subdir.node_id
                                );
                                if let Some(ref node_id) = subdir.node_id {
                                    // This is a node-backed directory!
                                    // The remaining path belongs to this document
                                    info!(
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
                info!(
                    "find_owning_document: failed to parse schema from {:?}",
                    schema_path
                );
            }
        } else {
            info!("find_owning_document: no schema at {:?}", schema_path);
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

/// Handle a file creation event in directory sync mode.
///
/// This function handles all the logic for syncing a newly created file:
/// - Checks ignore patterns and file filters
/// - Detects if content matches an existing file (for forking)
/// - Pushes content to server
/// - Spawns sync tasks for the new file
#[allow(clippy::too_many_arguments)]
pub async fn handle_file_created(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    path: &Path,
    options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) {
    debug!("Directory event: file created: {}", path.display());

    // Calculate relative path (normalized to forward slashes for cross-platform consistency)
    // First canonicalize both paths to handle absolute vs relative path mismatches
    let canonical_dir = match directory.canonicalize() {
        Ok(d) => d,
        Err(e) => {
            warn!(
                "Failed to canonicalize directory {}: {}",
                directory.display(),
                e
            );
            return;
        }
    };
    let canonical_path = match path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to canonicalize path {}: {}", path.display(), e);
            return;
        }
    };
    let relative_path = match canonical_path.strip_prefix(&canonical_dir) {
        Ok(rel) => normalize_path(&rel.to_string_lossy()),
        Err(e) => {
            warn!(
                "Failed to strip prefix {} from {}: {}",
                canonical_dir.display(),
                canonical_path.display(),
                e
            );
            return;
        }
    };

    // Find which document owns this file path (may be a node-backed subdirectory)
    let owning_doc = find_owning_document(directory, fs_root_id, &relative_path);
    debug!(
        "File {} owned by document {} (relative: {})",
        relative_path, owning_doc.document_id, owning_doc.relative_path
    );

    // Check if file matches ignore patterns
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let should_ignore = options.ignore_patterns.iter().any(|pattern| {
        if pattern == &file_name {
            true
        } else if pattern.contains('*') {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                file_name.starts_with(parts[0]) && file_name.ends_with(parts[1])
            } else {
                false
            }
        } else {
            false
        }
    });
    if should_ignore {
        debug!(
            "Ignoring new file (matches ignore pattern): {}",
            relative_path
        );
        return;
    }

    // Skip hidden files unless configured to include them
    if !options.include_hidden && file_name.starts_with('.') {
        debug!("Ignoring hidden file: {}", relative_path);
        return;
    }

    // Skip files with disallowed extensions
    if !is_allowed_extension(path) {
        debug!("Ignoring file with disallowed extension: {}", relative_path);
        return;
    }

    // Check if we already have sync tasks for this file
    let already_tracked = {
        let states = file_states.read().await;
        states.contains_key(&relative_path)
    };

    if !already_tracked && path.is_file() {
        // Read file content first (needed for both hash check and push)
        let raw_content = match tokio::fs::read(path).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to read new file {}: {}", path.display(), e);
                return;
            }
        };

        // Compute content hash and check for matching file (fork detection)
        let content_hash = compute_content_hash(&raw_content);
        let matching_source = {
            let states = file_states.read().await;
            states
                .iter()
                .find(|(_, state)| state.content_hash.as_ref() == Some(&content_hash))
                .map(|(_, state)| state.identifier.clone())
        };

        // Initial identifier - may be updated after schema push if not using paths
        // When using paths, always use the full fs-root relative path.
        let mut identifier = if use_paths {
            relative_path.clone()
        } else {
            format!("{}:{}", owning_doc.document_id, owning_doc.relative_path)
        };
        let state = Arc::new(RwLock::new(SyncState::new()));

        // If content matches an existing file, try to fork it
        let forked_successfully = if let Some(source_id) = matching_source {
            info!(
                "New file {} has identical content to {}, forking...",
                relative_path, source_id
            );

            // Convert Result to avoid holding Box<dyn Error> across await
            let fork_result = fork_node(client, server, &source_id, None)
                .await
                .map_err(|e| e.to_string());

            match fork_result {
                Ok(forked_id) => {
                    info!(
                        "Forked {} -> {} for new file {}",
                        source_id, forked_id, relative_path
                    );
                    // Update schema to point to forked node
                    // Use the owning document's directory and ID
                    if let Ok(schema) = scan_directory(&owning_doc.directory, options) {
                        if let Ok(json) = schema_to_json(&schema) {
                            if let Err(e) = push_schema_to_server(
                                client,
                                server,
                                &owning_doc.document_id,
                                &json,
                            )
                            .await
                            {
                                warn!("Failed to push updated schema: {}", e);
                            }
                        }
                    }
                    // Use the forked ID as the identifier
                    identifier = forked_id;
                    true
                }
                Err(e) => {
                    warn!(
                        "Fork failed for {}, falling back to new document: {}",
                        relative_path, e
                    );
                    false
                }
            }
        } else {
            false
        };

        // If fork didn't succeed (no match or fork failed), create document normally
        if !forked_successfully {
            // Push updated schema FIRST so server reconciler creates the node
            // Use the owning document's directory and ID
            if let Ok(schema) = scan_directory(&owning_doc.directory, options) {
                if let Ok(json) = schema_to_json(&schema) {
                    if let Err(e) =
                        push_schema_to_server(client, server, &owning_doc.document_id, &json).await
                    {
                        warn!("Failed to push updated schema: {}", e);
                    }
                }
            }

            // Wait briefly for server to reconcile and create the node
            sleep(Duration::from_millis(100)).await;

            // When not using paths, fetch the UUID from the updated schema
            // The reconciler assigns UUIDs to new entries, so we need to look them up
            // Use the owning document's relative path
            if !use_paths {
                if let Some(uuid) = fetch_node_id_from_schema(
                    client,
                    server,
                    &owning_doc.document_id,
                    &owning_doc.relative_path,
                )
                .await
                {
                    info!(
                        "Resolved UUID for {}: {} -> {}",
                        owning_doc.relative_path, identifier, uuid
                    );
                    identifier = uuid;
                } else {
                    warn!(
                        "Could not resolve UUID for {}, using derived ID: {}",
                        owning_doc.relative_path, identifier
                    );
                }
            }

            // Now push initial content (handle binary files)
            let content_info = detect_from_path(path);
            let is_binary = content_info.is_binary || is_binary_content(&raw_content);

            let content = if is_binary {
                use base64::{engine::general_purpose::STANDARD, Engine};
                STANDARD.encode(&raw_content)
            } else {
                String::from_utf8_lossy(&raw_content).to_string()
            };

            let is_json = !is_binary && content_info.mime_type == "application/json";
            let is_jsonl = !is_binary && content_info.mime_type == "application/x-ndjson";
            if let Err(e) = if is_json {
                push_json_content(client, server, &identifier, &content, &state, use_paths).await
            } else if is_jsonl {
                push_jsonl_content(client, server, &identifier, &content, &state, use_paths).await
            } else {
                push_file_content(client, server, &identifier, &content, &state, use_paths).await
            } {
                warn!("Failed to push new file content: {}", e);
            }
        }

        // Spawn sync tasks for the new file
        let task_handles = spawn_file_sync_tasks(
            client.clone(),
            server.to_string(),
            identifier.clone(),
            path.to_path_buf(),
            state.clone(),
            use_paths,
            push_only,
            pull_only,
            false, // force_push: directory mode doesn't support force-push
            #[cfg(unix)]
            inode_tracker.clone(),
        );

        // Add to file_states with task handles
        {
            let mut states = file_states.write().await;
            states.insert(
                relative_path.clone(),
                FileSyncState {
                    relative_path: relative_path.clone(),
                    identifier,
                    state,
                    task_handles,
                    use_paths,
                    content_hash: Some(content_hash),
                },
            );
        }

        info!("Started sync for new local file: {}", relative_path);
    }
}

/// Handle a file modification event in directory sync mode.
///
/// Modified files are handled by per-file watchers, so this just updates
/// the schema in case metadata changed.
pub async fn handle_file_modified(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    path: &Path,
    options: &ScanOptions,
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

    // Modified files are handled by per-file watchers
    // Just update schema in case metadata changed
    // Use the owning document's directory and ID
    if let Ok(schema) = scan_directory(&owning_doc.directory, options) {
        if let Ok(json) = schema_to_json(&schema) {
            if let Err(e) =
                push_schema_to_server(client, server, &owning_doc.document_id, &json).await
            {
                warn!("Failed to push updated schema: {}", e);
            }
        }
    }
}

/// Ensure fs-root document exists on the server, creating it if necessary.
///
/// Returns Ok(()) if the document exists or was created successfully.
pub async fn ensure_fs_root_exists(
    client: &Client,
    server: &str,
    fs_root_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let doc_url = format!("{}/docs/{}/info", server, encode_node_id(fs_root_id));
    let resp = client.get(&doc_url).send().await?;
    if !resp.status().is_success() {
        // Create the document
        info!("Creating fs-root document: {}", fs_root_id);
        let create_url = format!("{}/docs", server);
        let create_resp = client
            .post(&create_url)
            .json(&serde_json::json!({
                "type": "document",
                "id": fs_root_id,
                "content_type": "application/json"
            }))
            .send()
            .await?;
        if !create_resp.status().is_success() {
            let status = create_resp.status();
            let body = create_resp.text().await.unwrap_or_default();
            return Err(format!("Failed to create fs-root document: {} - {}", status, body).into());
        }
    }
    info!("Connected to fs-root document: {}", fs_root_id);
    Ok(())
}

/// Synchronize schema between local directory and server.
///
/// Based on the initial sync strategy:
/// - "local": Always push local schema
/// - "server": Only push if server is empty
/// - "skip": Only push if server is empty
///
/// Returns the schema JSON that was pushed or fetched.
pub async fn sync_schema(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    options: &ScanOptions,
    initial_sync_strategy: &str,
    server_has_content: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    // Scan directory and generate FS schema
    info!("Scanning directory...");
    let schema = scan_directory(directory, options).map_err(|e| format!("Scan error: {}", e))?;
    let schema_json = schema_to_json(&schema)?;
    info!(
        "Directory scanned: generated {} bytes of schema JSON",
        schema_json.len()
    );

    // Decide whether to push local schema based on strategy
    let should_push_schema = match initial_sync_strategy {
        "local" => true,
        "server" => !server_has_content,
        "skip" => !server_has_content,
        _ => !server_has_content,
    };

    if should_push_schema {
        // Push schema to fs-root node (with None UUIDs for new entries)
        info!("Pushing filesystem schema to server...");
        push_schema_to_server(client, server, fs_root_id, &schema_json).await?;
        info!("Schema pushed successfully");

        // Push nested schemas from local subdirectories to their server documents.
        // This restores node-backed directory contents after a server database clear.
        if let Err(e) = push_nested_schemas(client, server, directory, &schema).await {
            warn!("Failed to push nested schemas: {}", e);
        }

        // Give the server's reconciler a moment to generate UUIDs
        sleep(Duration::from_millis(100)).await;

        // Fetch the schema back from server (now with server-generated UUIDs)
        // and write to local .commonplace.json files.
        // This ensures all sync clients get the same server-assigned UUIDs.
        let mut final_schema_json = schema_json.clone();
        let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
        if let Ok(resp) = client.get(&head_url).send().await {
            if resp.status().is_success() {
                if let Ok(head) = resp.json::<HeadResponse>().await {
                    // Write server's schema (with UUIDs) to local file
                    final_schema_json = head.content.clone();
                    if let Err(e) = write_schema_file(directory, &head.content).await {
                        warn!("Failed to write schema file: {}", e);
                    }

                    // Write nested schemas from server
                    if let Ok(server_schema) = serde_json::from_str::<FsSchema>(&head.content) {
                        info!("Writing nested schemas from server...");
                        if let Err(e) =
                            write_nested_schemas(client, server, directory, &server_schema).await
                        {
                            warn!("Failed to write nested schemas from server: {}", e);
                        }
                    }
                }
            }
        }

        // Retry fetching nested schemas to handle async directory creation by other clients
        for attempt in 2..=3 {
            // Wait between retries to give other sync clients time to push schemas
            info!(
                "Waiting 3s before retry #{} of write_nested_schemas...",
                attempt
            );
            sleep(Duration::from_secs(3)).await;

            if let Ok(resp) = client.get(&head_url).send().await {
                if resp.status().is_success() {
                    if let Ok(head) = resp.json::<HeadResponse>().await {
                        if let Ok(server_schema) = serde_json::from_str::<FsSchema>(&head.content) {
                            info!(
                                "Writing nested schemas from server (attempt {})...",
                                attempt
                            );
                            if let Err(e) =
                                write_nested_schemas(client, server, directory, &server_schema)
                                    .await
                            {
                                warn!("Failed to write nested schemas from server: {}", e);
                            }
                        }
                    }
                }
            }
        }

        Ok(final_schema_json)
    } else {
        info!(
            "Server already has content, skipping schema push (strategy={})",
            initial_sync_strategy
        );

        // Check if local schema file already exists
        let local_schema_path = directory.join(SCHEMA_FILENAME);
        let local_schema_exists = local_schema_path.exists();

        // Fetch server schema
        let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
        if let Ok(resp) = client.get(&head_url).send().await {
            if resp.status().is_success() {
                if let Ok(head) = resp.json::<HeadResponse>().await {
                    // Parse the server schema for nested schema operations
                    let server_schema: FsSchema = serde_json::from_str(&head.content)
                        .map_err(|e| format!("Failed to parse server schema: {}", e))?;

                    if local_schema_exists {
                        // Don't overwrite local schema - it may have been modified
                        // by commonplace-link or manual edits. Use --initial-sync local
                        // to push local changes to server, or delete .commonplace.json
                        // to reset to server state.
                        debug!(
                            "Local schema exists, preserving it (use --initial-sync local to push changes)"
                        );
                    } else {
                        // No local schema exists, write server's schema
                        if let Err(e) = write_schema_file(directory, &head.content).await {
                            warn!("Failed to write schema file: {}", e);
                        }
                    }

                    // Write nested schemas for node-backed directories to local subdirectories.
                    // This persists subdirectory schemas so they survive server restarts.
                    if let Err(e) =
                        write_nested_schemas(client, server, directory, &server_schema).await
                    {
                        warn!("Failed to write nested schemas: {}", e);
                    }

                    return Ok(head.content);
                }
            }
        }
        Ok(schema_json)
    }
}

/// Check if the server has existing schema content.
///
/// Returns true if the server has non-empty, non-trivial content.
pub async fn check_server_has_content(client: &Client, server: &str, fs_root_id: &str) -> bool {
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    if let Ok(resp) = client.get(&head_url).send().await {
        if resp.status().is_success() {
            if let Ok(head) = resp.json::<HeadResponse>().await {
                return !head.content.is_empty() && head.content != "{}";
            }
        }
    }
    false
}

/// Handle a file deletion event in directory sync mode.
///
/// Stops sync tasks for the deleted file and updates the schema.
pub async fn handle_file_deleted(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    path: &Path,
    _options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
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
    {
        let mut states = file_states.write().await;
        if let Some(file_state) = states.remove(&relative_path) {
            info!("Stopping sync tasks for deleted file: {}", relative_path);
            for handle in file_state.task_handles {
                handle.abort();
            }
        }
    }

    // Delete from schema
    // For nested paths, check if the immediate subdirectory is node-backed (has its own sync)
    // Node-backed directories have their own .commonplace.json and sync process
    if relative_path.contains('/') {
        // Extract first path component (the immediate subdirectory)
        let first_component = relative_path.split('/').next().unwrap();
        let subdir_schema_path = directory.join(first_component).join(SCHEMA_FILENAME);

        if subdir_schema_path.exists() {
            // Node-backed directory has its own sync - skip deletion here
            debug!(
                "Skipping schema delete for {} - subdirectory {} has its own sync",
                relative_path, first_component
            );
            return;
        }
        // Non-node-backed subdirectory: fall through to delete from this schema
        debug!(
            "Deleting {} from parent schema (subdirectory {} is inline)",
            relative_path, first_component
        );
    }

    // Delete from schema (top-level files or files in non-node-backed subdirectories)
    if let Err(e) = delete_schema_entry(client, server, fs_root_id, &relative_path).await {
        warn!("Failed to delete schema entry {}: {}", relative_path, e);
    }
}
