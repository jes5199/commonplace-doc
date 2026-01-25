//! File event handlers for directory sync mode.
//!
//! This module handles file creation, modification, and deletion events
//! during directory synchronization.

use crate::fs::{Entry, FsSchema};
use crate::sync::directory::{scan_directory_to_json, ScanOptions};
use crate::sync::schema_io::{write_schema_file, SCHEMA_FILENAME};
use crate::sync::state_file::compute_content_hash;
use crate::sync::uuid_map::fetch_subdir_node_id;
use crate::sync::{
    delete_schema_entry, detect_from_path, fork_node, is_allowed_extension, is_binary_content,
    normalize_path, push_content_by_type, push_schema_to_server, remove_file_state_and_abort,
    spawn_file_sync_tasks, wait_for_file_stability, FileSyncState, SyncState,
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

/// Ensure all parent directories of a file path exist as node-backed directories.
///
/// When a file is created in a new subdirectory (e.g., `newdir/file.txt`), we need to:
/// 1. Check if `newdir` exists in the parent's schema with a node_id
/// 2. If not, create a document on the server for it
/// 3. Update the parent schema to include the directory with its node_id
///
/// This function walks from the file's directory up to the root, ensuring each
/// directory has a node_id assigned. Without this, `find_owning_document` would
/// fail to find the correct owning document for files in new subdirectories.
///
/// Returns the deepest directory's node_id (the one that will own the file),
/// or the fs_root_id if the file is in the root directory.
///
/// When `skip_http_schema_push` is true, schema updates are written locally
/// but NOT pushed to the server via HTTP. This is used when CRDT/MQTT sync
/// is enabled, as the MQTT path handles schema propagation and HTTP pushes
/// would cause conflicting updates from the server.
#[allow(clippy::too_many_arguments)]
pub async fn ensure_parent_directories_exist(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    root_directory: &Path,
    relative_file_path: &str,
    options: &ScanOptions,
    author: &str,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
    skip_http_schema_push: bool,
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
                                // Directory already has a node_id
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
            info!(
                "Creating node-backed directory '{}' (parent: {})",
                dir_name, current_parent_id
            );

            // Create a new document on the server for this directory
            let create_url = format!("{}/docs", server);
            let create_resp = client
                .post(&create_url)
                .json(&serde_json::json!({
                    "content_type": "application/json"
                }))
                .send()
                .await?;

            if !create_resp.status().is_success() {
                let status = create_resp.status();
                let body = create_resp.text().await.unwrap_or_default();
                return Err(format!(
                    "Failed to create directory document for '{}': {} - {}",
                    dir_name, status, body
                )
                .into());
            }

            let resp_body: serde_json::Value = create_resp.json().await?;
            let new_node_id = resp_body["id"]
                .as_str()
                .ok_or("No id in document creation response")?
                .to_string();

            info!(
                "Created document {} for directory '{}'",
                new_node_id, dir_name
            );

            // Update parent schema to include this directory with its node_id
            // First, scan the parent directory to get the current schema
            if let Ok(json) = scan_directory_to_json(&current_dir, options) {
                // Parse and modify the schema to add node_id
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

                // Write locally
                if let Err(e) =
                    write_schema_file(&current_dir, &updated_json, written_schemas).await
                {
                    warn!("Failed to write local schema for '{}': {}", dir_name, e);
                }

                // Push to server (skip when CRDT/MQTT sync handles schema propagation)
                if !skip_http_schema_push {
                    if let Err(e) = push_schema_to_server(
                        client,
                        server,
                        &current_parent_id,
                        &updated_json,
                        author,
                    )
                    .await
                    {
                        warn!("Failed to push parent schema for '{}': {}", dir_name, e);
                    }
                }
            }

            // Also initialize the new directory's schema (empty) if it doesn't already exist
            // We only write an empty schema if there's no existing schema to avoid
            // overwriting a schema that already has file entries from scan_directory
            let subdir_path = current_dir.join(dir_name);
            let subdir_schema_path = subdir_path.join(SCHEMA_FILENAME);
            let existing_schema = std::fs::read_to_string(&subdir_schema_path).ok();

            // Only write and push schema if no schema exists yet
            // If a schema exists, it may already have file entries from scan_directory
            // and will be properly updated and pushed by the later code in handle_file_created
            if existing_schema.is_none() {
                let empty_schema = serde_json::json!({
                    "version": 1,
                    "root": {
                        "type": "dir",
                        "entries": {}
                    }
                });
                let schema_str = serde_json::to_string(&empty_schema)?;

                // Write local schema for the new directory
                if let Err(e) = write_schema_file(&subdir_path, &schema_str, written_schemas).await
                {
                    warn!("Failed to write subdir schema for '{}': {}", dir_name, e);
                }

                // Push empty schema to server for the new directory (skip when CRDT/MQTT sync handles this)
                if !skip_http_schema_push {
                    if let Err(e) =
                        push_schema_to_server(client, server, &new_node_id, &schema_str, author)
                            .await
                    {
                        warn!("Failed to push subdir schema for '{}': {}", dir_name, e);
                    }
                }
            } else {
                debug!(
                    "Subdirectory '{}' already has schema, skipping initial write/push",
                    dir_name
                );
            }

            // Wait briefly for server to process
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
    written_schemas: Option<&crate::sync::WrittenSchemas>,
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

    // Ensure parent directories exist as node-backed directories before finding owner
    // This handles the case where a file is created in a new subdirectory
    if let Err(e) = ensure_parent_directories_exist(
        client,
        server,
        fs_root_id,
        directory,
        &relative_path,
        options,
        author,
        written_schemas,
        false, // skip_http_schema_push: HTTP path needs to push schema
    )
    .await
    {
        warn!(
            "Failed to ensure parent directories for {}: {}",
            relative_path, e
        );
        // Continue anyway - find_owning_document may still work if some directories exist
    }

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
        // Wait for file content to stabilize before reading.
        // This ensures we don't read partial content from atomic writes or
        // multi-step editor saves that trigger notify events before completion.
        let path_buf = path.to_path_buf();
        if let Err(e) = wait_for_file_stability(&path_buf).await {
            debug!(
                "File stability check failed for {}: {}, skipping",
                path.display(),
                e
            );
            return;
        }

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
                        // Write schema locally and track for echo detection
                        if let Err(e) =
                            write_schema_file(&owning_doc.directory, &json, written_schemas).await
                        {
                            warn!("Failed to write local schema: {}", e);
                        }
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
            // Create the document directly on the server (don't rely on reconciler)
            // This is the same approach used for directories in ensure_parent_directories_exist
            if !use_paths {
                let content_info = detect_from_path(path);
                let content_type = if content_info.is_binary {
                    "application/octet-stream"
                } else {
                    &content_info.mime_type
                };

                let create_url = format!("{}/docs", server);
                match client
                    .post(&create_url)
                    .json(&serde_json::json!({
                        "content_type": content_type
                    }))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(body) = resp.json::<serde_json::Value>().await {
                            if let Some(new_id) = body["id"].as_str() {
                                info!(
                                    "Created document {} for file '{}'",
                                    new_id, owning_doc.relative_path
                                );
                                identifier = new_id.to_string();
                            }
                        }
                    }
                    Ok(resp) => {
                        warn!(
                            "Failed to create document for '{}': {}",
                            owning_doc.relative_path,
                            resp.status()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create document for '{}': {}",
                            owning_doc.relative_path, e
                        );
                    }
                }
            }

            // Now scan and push the schema with the node_id we created
            if let Ok(json) = scan_directory_to_json(&owning_doc.directory, options) {
                // If we created a document, update the schema to include the node_id
                let updated_json = if !use_paths && !identifier.contains(':') {
                    // identifier is a UUID, update the schema entry with it
                    if let Ok(mut schema) = serde_json::from_str::<serde_json::Value>(&json) {
                        if let Some(entries) = schema
                            .get_mut("root")
                            .and_then(|r| r.get_mut("entries"))
                            .and_then(|e| e.as_object_mut())
                        {
                            // Get the filename from the relative path
                            let filename = owning_doc
                                .relative_path
                                .split('/')
                                .next_back()
                                .unwrap_or(&owning_doc.relative_path);
                            if let Some(entry) = entries.get_mut(filename) {
                                entry["node_id"] = serde_json::Value::String(identifier.clone());
                            }
                        }
                        serde_json::to_string(&schema).unwrap_or(json)
                    } else {
                        json
                    }
                } else {
                    json
                };

                // Write schema locally and track for echo detection
                // This prevents handle_schema_change from deleting the file we just created
                // if it receives a stale schema update from another client
                if let Err(e) =
                    write_schema_file(&owning_doc.directory, &updated_json, written_schemas).await
                {
                    warn!("Failed to write local schema: {}", e);
                }
                if let Err(e) = push_schema_to_server(
                    client,
                    server,
                    &owning_doc.document_id,
                    &updated_json,
                    author,
                )
                .await
                {
                    warn!("Failed to push updated schema: {}", e);
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

        // Add to file_states with task handles - but check for race condition first
        {
            let mut states = file_states.write().await;
            if states.contains_key(&relative_path) {
                // Another task already registered this file - abort our tasks to avoid duplicates
                warn!(
                    "Race detected: file {} already registered, aborting duplicate sync tasks",
                    relative_path
                );
                for handle in task_handles {
                    handle.abort();
                }
                return;
            }
            states.insert(
                relative_path.clone(),
                FileSyncState {
                    relative_path: relative_path.clone(),
                    identifier,
                    state,
                    task_handles,
                    use_paths,
                    content_hash: Some(content_hash),
                    crdt_last_content: None,
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
#[allow(clippy::too_many_arguments)]
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
        // Note: split('/').next() always returns Some for non-empty strings,
        // and we're inside a contains('/') check, but we use unwrap_or for safety
        let first_component = relative_path.split('/').next().unwrap_or("");
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
