//! File event handlers for directory sync mode.
//!
//! This module handles file creation, modification, and deletion events
//! during directory synchronization.

use crate::fs::{Entry, FsSchema};
use crate::sync::directory::{scan_directory_to_json, ScanOptions};
use crate::sync::schema_io::SCHEMA_FILENAME;
use crate::sync::state_file::compute_content_hash;
use crate::sync::uuid_map::{fetch_node_id_from_schema, fetch_subdir_node_id};
use crate::sync::{
    delete_schema_entry, detect_from_path, fork_node, is_allowed_extension, is_binary_content,
    normalize_path, push_content_by_type, push_schema_to_server, remove_file_state_and_abort,
    spawn_file_sync_tasks, FileSyncState, SyncState,
};
use reqwest::Client;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

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
    shared_state_file: Option<&crate::sync::SharedStateFile>,
    author: &str,
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
        // Create SyncState - use directory mode if we have a shared state file
        let state = if let Some(sf) = shared_state_file {
            Arc::new(RwLock::new(SyncState::for_directory_file(
                None, // No initial CID for new files
                sf.clone(),
                relative_path.clone(),
            )))
        } else {
            Arc::new(RwLock::new(SyncState::new()))
        };

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
                    if let Ok(json) = scan_directory_to_json(&owning_doc.directory, options) {
                        if let Err(e) = push_schema_to_server(
                            client,
                            server,
                            &owning_doc.document_id,
                            &json,
                            author,
                        )
                        .await
                        {
                            warn!("Failed to push updated schema: {}", e);
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
            if let Ok(json) = scan_directory_to_json(&owning_doc.directory, options) {
                if let Err(e) =
                    push_schema_to_server(client, server, &owning_doc.document_id, &json, author)
                        .await
                {
                    warn!("Failed to push updated schema: {}", e);
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

            if let Err(e) = push_content_by_type(
                client,
                server,
                &identifier,
                &content,
                &state,
                use_paths,
                is_binary,
                &content_info.mime_type,
                author,
            )
            .await
            {
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
            author.to_string(),
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
    author: &str,
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
    if let Ok(json) = scan_directory_to_json(&owning_doc.directory, options) {
        if let Err(e) =
            push_schema_to_server(client, server, &owning_doc.document_id, &json, author).await
        {
            warn!("Failed to push updated schema: {}", e);
        }
    }
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
    author: &str,
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

    // Delete from schema
    // For nested paths, check if the immediate subdirectory is node-backed
    // Node-backed directories have their own document on the server - we need to
    // delete the file entry from that document, not from the parent schema
    if relative_path.contains('/') {
        // Extract first path component (the immediate subdirectory)
        let first_component = relative_path.split('/').next().unwrap();
        let subdir_schema_path = directory.join(first_component).join(SCHEMA_FILENAME);

        if subdir_schema_path.exists() {
            // Node-backed directory - need to get its node_id and delete from that schema
            let file_in_subdir = relative_path
                .strip_prefix(first_component)
                .and_then(|s| s.strip_prefix('/'))
                .unwrap_or(&relative_path);

            // Get the subdirectory's node_id from the parent schema
            if let Some(subdir_node_id) =
                fetch_subdir_node_id(client, server, fs_root_id, first_component).await
            {
                info!(
                    "Deleting {} from node-backed subdirectory {} (node_id: {})",
                    file_in_subdir, first_component, subdir_node_id
                );
                if let Err(e) =
                    delete_schema_entry(client, server, &subdir_node_id, file_in_subdir, author)
                        .await
                {
                    warn!(
                        "Failed to delete schema entry {} from subdirectory {}: {}",
                        file_in_subdir, first_component, e
                    );
                }
            } else {
                warn!(
                    "Could not find node_id for subdirectory {} - skipping deletion of {}",
                    first_component, file_in_subdir
                );
            }
            return;
        }
        // Non-node-backed subdirectory: fall through to delete from this schema
        debug!(
            "Deleting {} from parent schema (subdirectory {} is inline)",
            relative_path, first_component
        );
    }

    // Delete from schema (top-level files or files in non-node-backed subdirectories)
    if let Err(e) = delete_schema_entry(client, server, fs_root_id, &relative_path, author).await {
        warn!("Failed to delete schema entry {}: {}", relative_path, e);
    }
}
