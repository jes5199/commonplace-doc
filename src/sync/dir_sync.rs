//! Directory mode synchronization helpers.
//!
//! This module contains helper functions for syncing a directory
//! with a server document, including schema traversal and UUID mapping.

use crate::fs::{Entry, FsSchema};
use crate::sync::client::fetch_head;
use crate::sync::directory::{scan_directory, schema_to_json, ScanOptions};
use crate::sync::schema_io::{
    fetch_and_validate_schema, write_nested_schemas, write_schema_file, SCHEMA_FILENAME,
};
use crate::sync::state_file::{
    compute_content_hash, load_synced_directories, mark_directory_synced, unmark_directory_synced,
};
use crate::sync::uuid_map::{
    build_uuid_map_and_write_schemas, build_uuid_map_recursive,
    build_uuid_map_recursive_with_status,
};
use crate::sync::{
    ancestry::determine_sync_direction, build_info_url, detect_from_path, is_allowed_extension,
    is_binary_content, looks_like_base64_binary, push_schema_to_server,
    remove_file_state_and_abort, spawn_file_sync_tasks, FileSyncState, SyncState,
};
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Check if a file path exists in any recently written schema.
///
/// This is used for echo detection during file deletion - we should not delete
/// a file if we recently pushed a schema update containing it, as the server
/// may not have processed our update yet.
///
/// `base_directory` is the directory being synced (the root of the sync), used to
/// compute relative paths when checking nested schemas.
async fn path_in_written_schemas(
    path: &str,
    base_directory: &Path,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
) -> bool {
    let Some(ws) = written_schemas else {
        return false;
    };

    // Wait for read lock - this is safe because schema writes are brief
    let ws_guard = ws.read().await;

    for (schema_path, schema_content) in ws_guard.iter() {
        if let Ok(schema) = serde_json::from_str::<FsSchema>(schema_content) {
            // The schema_path is the canonical path to the .commonplace.json file.
            // We need to compute the path relative to that schema's directory.
            //
            // Example: if we're checking "bartleby/prompts.txt" and the schema is at
            // "/workspace/bartleby/.commonplace.json", we need to strip "bartleby/"
            // from the path to get "prompts.txt".
            let schema_dir = schema_path.parent().unwrap_or(schema_path.as_path());

            // Get the relative path from base_directory to the schema's directory
            let relative_schema_dir = schema_dir
                .strip_prefix(base_directory)
                .unwrap_or(Path::new(""));

            // If the path starts with the schema's relative directory, strip it
            let relative_path = if relative_schema_dir.as_os_str().is_empty() {
                // Schema is at base directory, use full path
                path.to_string()
            } else {
                let schema_prefix = relative_schema_dir.to_string_lossy();
                if let Some(stripped) = path.strip_prefix(&*schema_prefix) {
                    stripped.strip_prefix('/').unwrap_or(stripped).to_string()
                } else {
                    // Path doesn't start with this schema's directory, skip
                    continue;
                }
            };

            if schema_has_path(&schema, &relative_path) {
                return true;
            }
        }
    }

    false
}

/// Check if a schema contains an entry for the given path.
///
/// This only handles single-level nesting because inline subdirectories are deprecated.
/// All directories are now node-backed, meaning each subdirectory has its own schema
/// document stored in `written_schemas` with its own canonical path. Multi-level paths
/// like "a/b/file.txt" are handled by checking the nested schema at "a/b/.commonplace.json".
fn schema_has_path(schema: &FsSchema, path: &str) -> bool {
    let Some(Entry::Dir(ref root)) = schema.root else {
        return false;
    };

    let Some(ref entries) = root.entries else {
        return false;
    };

    // Handle simple single-level paths
    if !path.contains('/') {
        return entries.contains_key(path);
    }

    // Handle nested paths (e.g., "bartleby/prompts.txt")
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() != 2 {
        return entries.contains_key(path);
    }

    let (first, rest) = (parts[0], parts[1]);
    if let Some(Entry::Dir(subdir)) = entries.get(first) {
        // For node-backed directories, the files are defined in a separate schema
        // document, not in the root schema. We should NOT return true just because
        // the directory exists - that would prevent all deletions under that directory.
        // Instead, return false here and let the function continue to check other
        // written schemas (the nested schema should also be in written_schemas if
        // we wrote it).
        if subdir.node_id.is_some() {
            // Node-backed directory - can't determine from this schema alone
            // Continue checking other schemas in written_schemas
            return false;
        }
        // Inline directory entries
        if let Some(ref sub_entries) = subdir.entries {
            return sub_entries.contains_key(rest);
        }
    }

    false
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Fetch, parse, and validate schema from server
    let fetched = match fetch_and_validate_schema(client, server, subdir_node_id, true).await {
        Some(f) => f,
        None => return Ok(()), // Logged inside fetch_and_validate_schema
    };
    let schema = fetched.schema;

    // Write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(subdir_directory, &fetched.content, None).await {
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
            remove_file_state_and_abort(file_states, path).await;

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
    shared_state_file: Option<&crate::sync::SharedStateFile>,
    author: &str,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Don't pull if push_only mode
    if push_only {
        return Ok(());
    }

    // Fetch and parse schema from server (validation only, no root check needed)
    if fetch_and_validate_schema(client, server, subdir_node_id, false)
        .await
        .is_none()
    {
        return Ok(()); // Logged inside fetch_and_validate_schema
    }

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
        if let Ok(Some(file_head)) = fetch_head(client, server, &identifier, use_paths).await {
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

            // Add to file states - use directory mode if shared state file available
            let state = if let Some(sf) = shared_state_file {
                Arc::new(RwLock::new(SyncState::for_directory_file(
                    file_head.cid.clone(),
                    sf.clone(),
                    root_relative_path.clone(),
                )))
            } else {
                Arc::new(RwLock::new(SyncState::with_cid(file_head.cid.clone())))
            };
            // Update with content for echo detection
            {
                let mut s = state.write().await;
                s.last_written_content = file_head.content;
            }

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
                author.to_string(),
                #[cfg(unix)]
                inode_tracker.clone(),
            );

            let mut states = file_states.write().await;
            // Check for race condition - another task might have registered this file
            if states.contains_key(root_relative_path) {
                warn!(
                    "Race detected: file {} already registered during subdir sync, aborting duplicate tasks",
                    root_relative_path
                );
                for handle in task_handles {
                    handle.abort();
                }
                continue;
            }
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Fetch the subdirectory's schema from server
    let head = match fetch_head(client, server, subdir_node_id, false).await {
        Ok(Some(h)) => h,
        Ok(None) => return Err(format!("Subdir {} not found", subdir_node_id).into()),
        Err(e) => return Err(format!("Failed to fetch subdir schema: {}", e).into()),
    };
    if head.content.is_empty() || head.content == "{}" {
        return Ok(());
    }

    let schema: FsSchema = serde_json::from_str(&head.content)?;

    if let Some(Entry::Dir(dir)) = schema.root.as_ref() {
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
    author: &str,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut pushed_count = 0;
    if let Some(ref root) = schema.root {
        pushed_count =
            push_nested_schemas_recursive(client, server, directory, root, directory, author)
                .await?;
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
    author: &str,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
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
                        let should_push = match fetch_head(client, server, node_id, false).await {
                            Ok(Some(head)) => {
                                // Push if server is empty or has trivial content
                                head.content.is_empty() || head.content == "{}"
                            }
                            // Push if document not found or error
                            Ok(None) | Err(_) => true,
                        };

                        if should_push {
                            info!(
                                "Pushing nested schema from {:?} to document {}",
                                nested_schema_path, node_id
                            );
                            if let Err(e) = push_schema_to_server(
                                client,
                                server,
                                node_id,
                                &local_schema,
                                author,
                            )
                            .await
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
                        if let Some(Entry::Dir(ref sub_dir)) = parsed_schema.root {
                            if let Some(ref entries) = sub_dir.entries {
                                for (entry_name, child_entry) in entries {
                                    if let Entry::Dir(ref child_dir) = child_entry {
                                        if child_dir.node_id.is_some() {
                                            let child_path = current_dir.join(entry_name);
                                            pushed_count += push_nested_schemas_recursive(
                                                client,
                                                server,
                                                _base_dir,
                                                child_entry,
                                                &child_path,
                                                author,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Non-node-backed directory: iterate over inline entries (deprecated but handle for safety)
            if let Some(ref entries) = dir.entries {
                for (entry_name, child_entry) in entries {
                    if let Entry::Dir(ref child_dir) = child_entry {
                        if child_dir.node_id.is_some() {
                            let child_path = current_dir.join(entry_name);
                            pushed_count += push_nested_schemas_recursive(
                                client,
                                server,
                                _base_dir,
                                child_entry,
                                &child_path,
                                author,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }

    Ok(pushed_count)
}

/// Handle a schema change from the server - create new local files.
///
/// When the server's fs-root schema changes (e.g., new files added by another client),
/// this function fetches the new schema, creates any missing local files, and optionally
/// spawns sync tasks for them.
///
/// If `written_schemas` is provided, written schema content is recorded for
/// echo detection by the directory watcher.
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
    author: &str,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
    shared_state_file: Option<&crate::sync::SharedStateFile>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Fetch, parse, and validate schema from server
    let fetched = match fetch_and_validate_schema(client, server, fs_root_id, true).await {
        Some(f) => f,
        None => return Ok(()), // Logged inside fetch_and_validate_schema
    };
    let schema = fetched.schema;

    // Write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(directory, &fetched.content, written_schemas).await {
        warn!("Failed to write schema file: {}", e);
    }

    // Collect all paths from schema (with explicit node_id if present)
    // Use async version that follows node-backed directories to get complete path->UUID map
    // AND writes nested schema files to local directory (required for find_owning_document fallback)
    let (uuid_map, _all_succeeded) =
        build_uuid_map_and_write_schemas(client, server, fs_root_id, directory, written_schemas)
            .await;
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
            if let Ok(Some(file_head)) = fetch_head(client, server, &identifier, use_paths).await {
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
                // Use CID from server response, or fall back to persisted CID if available
                let initial_cid = if file_head.cid.is_some() {
                    file_head.cid.clone()
                } else if let Some(sf) = shared_state_file {
                    sf.read().await.get_file_cid(path)
                } else {
                    None
                };
                // Create SyncState - use directory mode if we have a shared state file
                let state = if let Some(sf) = shared_state_file {
                    Arc::new(RwLock::new(SyncState::for_directory_file(
                        initial_cid,
                        sf.clone(),
                        path.clone(),
                    )))
                } else {
                    Arc::new(RwLock::new(SyncState::with_cid(initial_cid)))
                };
                // Update state with content for echo detection
                {
                    let mut s = state.write().await;
                    s.last_written_content = file_head.content;
                }

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
                        author.to_string(),
                        #[cfg(unix)]
                        inode_tracker.clone(),
                    )
                } else {
                    Vec::new()
                };
                let mut states = file_states.write().await;
                // Check for race condition - another task might have registered this file
                if states.contains_key(path) {
                    warn!(
                        "Race detected: file {} already registered during sync, aborting duplicate tasks",
                        path
                    );
                    for handle in task_handles {
                        handle.abort();
                    }
                    continue;
                }
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
        // Check if we recently wrote a schema containing this file
        // This prevents race conditions where we create a file, push schema,
        // but receive a stale SSE update before the server processes our push
        if path_in_written_schemas(path, directory, written_schemas).await {
            debug!(
                "Skipping deletion of {} - found in recently written schema (echo detection)",
                path
            );
            continue;
        }

        info!("Server removed file: {} - deleting local copy", path);
        let file_path = directory.join(path);

        // Stop sync tasks and remove from file_states
        remove_file_state_and_abort(file_states, path).await;

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
    if let Err(e) = write_nested_schemas(client, server, directory, &schema, written_schemas).await
    {
        warn!("Failed to write nested schemas: {}", e);
    }

    Ok(())
}

/// Handle schema change with content-based deduplication.
///
/// Returns Ok(true) if schema was processed, Ok(false) if skipped due to no change.
#[allow(clippy::too_many_arguments)]
pub async fn handle_schema_change_with_dedup(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    spawn_tasks: bool,
    use_paths: bool,
    last_schema_hash: &mut Option<String>,
    last_schema_cid: &mut Option<String>,
    push_only: bool,
    pull_only: bool,
    author: &str,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
    shared_state_file: Option<&crate::sync::SharedStateFile>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // Fetch current schema from server
    let head = match fetch_head(client, server, fs_root_id, false).await {
        Ok(Some(h)) => h,
        Ok(None) => return Ok(false), // Document not found, nothing to poll
        Err(e) => return Err(format!("Failed to fetch fs-root HEAD: {}", e).into()),
    };
    if head.content.is_empty() {
        return Ok(false);
    }

    // Use CRDT ancestry to determine if we should apply server schema.
    // Only pull if server is ahead of us (our CID is ancestor of server's CID).
    let server_cid = head.cid.as_deref();
    let direction = match determine_sync_direction(
        client,
        server,
        fs_root_id,
        last_schema_cid.as_deref(),
        server_cid,
    )
    .await
    {
        Ok(d) => d,
        Err(e) => {
            warn!("Ancestry check failed for schema sync: {}", e);
            // On error, fall through to content-based checks as fallback
            crate::sync::ancestry::SyncDirection::Pull
        }
    };

    if !direction.should_pull() {
        debug!(
            "Ancestry check for schema: {:?}, skipping server schema",
            direction
        );
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
        author,
        #[cfg(unix)]
        inode_tracker,
        written_schemas,
        shared_state_file,
    )
    .await?;

    // Update last hash and CID only after successful processing
    *last_schema_hash = Some(current_hash);
    *last_schema_cid = head.cid.clone();

    Ok(true)
}

/// Ensure fs-root document exists on the server, creating it if necessary.
///
/// Returns Ok(()) if the document exists or was created successfully.
pub async fn ensure_fs_root_exists(
    client: &Client,
    server: &str,
    fs_root_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let doc_url = build_info_url(server, fs_root_id);
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
/// Returns (schema_json, initial_cid) where initial_cid is the CID of the schema
/// after initial sync. This CID should be passed to subscription tasks to prevent
/// them from pulling stale server content that predates our push.
#[allow(clippy::too_many_arguments)]
pub async fn sync_schema(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &Path,
    options: &ScanOptions,
    initial_sync_strategy: &str,
    server_has_content: bool,
    author: &str,
) -> Result<(String, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
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
        push_schema_to_server(client, server, fs_root_id, &schema_json, author).await?;
        info!("Schema pushed successfully");

        // Push nested schemas from local subdirectories to their server documents.
        // This restores node-backed directory contents after a server database clear.
        if let Err(e) = push_nested_schemas(client, server, directory, &schema, author).await {
            warn!("Failed to push nested schemas: {}", e);
        }

        // Give the server's reconciler a moment to generate UUIDs
        sleep(Duration::from_millis(100)).await;

        // Fetch the schema back from server (now with server-generated UUIDs)
        // and write to local .commonplace.json files.
        // This ensures all sync clients get the same server-assigned UUIDs.
        let mut final_schema_json = schema_json.clone();
        let mut final_cid: Option<String> = None;
        if let Ok(Some(head)) = fetch_head(client, server, fs_root_id, false).await {
            // Write server's schema (with UUIDs) to local file
            final_schema_json = head.content.clone();
            final_cid = head.cid.clone();
            if let Err(e) = write_schema_file(directory, &head.content, None).await {
                warn!("Failed to write schema file: {}", e);
            }

            // Write nested schemas from server
            if let Ok(server_schema) = serde_json::from_str::<FsSchema>(&head.content) {
                info!("Writing nested schemas from server...");
                if let Err(e) =
                    write_nested_schemas(client, server, directory, &server_schema, None).await
                {
                    warn!("Failed to write nested schemas from server: {}", e);
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

            if let Ok(Some(head)) = fetch_head(client, server, fs_root_id, false).await {
                // Update final_cid with the latest CID from retry
                final_cid = head.cid.clone();
                if let Ok(server_schema) = serde_json::from_str::<FsSchema>(&head.content) {
                    info!(
                        "Writing nested schemas from server (attempt {})...",
                        attempt
                    );
                    if let Err(e) =
                        write_nested_schemas(client, server, directory, &server_schema, None).await
                    {
                        warn!("Failed to write nested schemas from server: {}", e);
                    }
                }
            }
        }

        Ok((final_schema_json, final_cid))
    } else {
        info!(
            "Server already has content, skipping schema push (strategy={})",
            initial_sync_strategy
        );

        // Check if local schema file already exists
        let local_schema_path = directory.join(SCHEMA_FILENAME);
        let local_schema_exists = local_schema_path.exists();

        // Fetch server schema
        if let Ok(Some(head)) = fetch_head(client, server, fs_root_id, false).await {
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
                if let Err(e) = write_schema_file(directory, &head.content, None).await {
                    warn!("Failed to write schema file: {}", e);
                }
            }

            // Write nested schemas for node-backed directories to local subdirectories.
            // This persists subdirectory schemas so they survive server restarts.
            if let Err(e) =
                write_nested_schemas(client, server, directory, &server_schema, None).await
            {
                warn!("Failed to write nested schemas: {}", e);
            }

            return Ok((head.content, head.cid));
        }
        Ok((schema_json, None))
    }
}

/// Check if the server has existing schema content.
///
/// Returns true if the server has non-empty, non-trivial content.
pub async fn check_server_has_content(client: &Client, server: &str, fs_root_id: &str) -> bool {
    if let Ok(Some(head)) = fetch_head(client, server, fs_root_id, false).await {
        return !head.content.is_empty() && head.content != "{}";
    }
    false
}

/// Handle a user edit to a .commonplace.json schema file.
///
/// When the user edits a schema file locally (e.g., to change a node_id for linking),
/// this function pushes the changes to the server.
///
/// # Arguments
/// * `client` - HTTP client
/// * `server` - Server URL
/// * `fs_root_id` - The root document ID for this sync directory
/// * `base_directory` - The base sync directory path
/// * `schema_path` - Path to the modified .commonplace.json file
/// * `content` - New content of the schema file
/// * `author` - The author string for the commit
pub async fn handle_schema_modified(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    base_directory: &Path,
    schema_path: &Path,
    content: &str,
    author: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Validate the schema content
    let _schema: FsSchema =
        serde_json::from_str(content).map_err(|e| format!("Invalid schema JSON: {}", e))?;

    // Determine which document owns this schema
    // If schema_path is in base_directory (root schema), push to fs_root_id
    // If schema_path is in a subdirectory, find the node_id from parent schema

    let schema_dir = schema_path
        .parent()
        .ok_or("Schema file has no parent directory")?;

    // Check if this is the root schema
    if schema_dir == base_directory {
        info!("Pushing root schema to server (fs_root_id: {})", fs_root_id);
        push_schema_to_server(client, server, fs_root_id, content, author).await?;
        return Ok(());
    }

    // This is a nested schema - find the owning document
    // Get the relative path from base_directory to schema_dir
    let relative_dir = schema_dir.strip_prefix(base_directory).map_err(|_| {
        format!(
            "Schema path {} is not under base directory {}",
            schema_path.display(),
            base_directory.display()
        )
    })?;

    // Find the node_id for this directory by reading parent schemas
    // Start from base_directory and walk down
    let mut current_dir = base_directory.to_path_buf();
    let mut current_node_id = fs_root_id.to_string();

    for component in relative_dir.iter() {
        let component_str = component.to_string_lossy();

        // Read the schema at current_dir
        let parent_schema_path = current_dir.join(SCHEMA_FILENAME);
        let parent_schema_content = tokio::fs::read_to_string(&parent_schema_path)
            .await
            .map_err(|e| {
                format!(
                    "Failed to read parent schema {}: {}",
                    parent_schema_path.display(),
                    e
                )
            })?;

        let parent_schema: FsSchema = serde_json::from_str(&parent_schema_content)
            .map_err(|e| format!("Invalid parent schema: {}", e))?;

        // Find the entry for this component
        let entry = parent_schema
            .root
            .as_ref()
            .and_then(|root| {
                if let Entry::Dir(dir) = root {
                    dir.entries.as_ref()?.get(component_str.as_ref())
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("Directory {} not found in parent schema", component_str))?;

        // Get the node_id if this is a node-backed directory
        if let Entry::Dir(dir) = entry {
            if let Some(ref node_id) = dir.node_id {
                current_node_id = node_id.clone();
            } else {
                return Err(format!(
                    "Directory {} is not node-backed (no node_id)",
                    component_str
                )
                .into());
            }
        } else {
            return Err(format!("{} is not a directory", component_str).into());
        }

        current_dir = current_dir.join(component);
    }

    info!(
        "Pushing nested schema {} to server (node_id: {})",
        schema_path.display(),
        current_node_id
    );
    push_schema_to_server(client, server, &current_node_id, content, author).await?;

    Ok(())
}
