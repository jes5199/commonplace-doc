//! Directory mode synchronization helpers.
//!
//! This module contains helper functions for syncing a directory
//! with a server document, including schema traversal and UUID mapping.

use crate::commit::Commit;
use crate::fs::{Entry, FsSchema};
use crate::sync::client::fetch_head;
use crate::sync::crdt_merge::parse_edit_message;
use crate::sync::directory::{scan_directory, schema_to_json, ScanOptions};
use crate::sync::file_events::find_owning_document;
use crate::sync::schema_io::{
    fetch_and_validate_schema, write_nested_schemas, write_schema_file, SCHEMA_FILENAME,
};
use crate::sync::state_file::{
    compute_content_hash, load_synced_directories, mark_directory_synced, unmark_directory_synced,
};
use crate::sync::subscriptions::CrdtFileSyncContext;
use crate::sync::uuid_map::{
    build_uuid_map_and_write_schemas, build_uuid_map_recursive,
    build_uuid_map_recursive_with_status,
};
use crate::sync::ymap_schema;
use crate::sync::{
    ancestry::determine_sync_direction, build_info_url, detect_from_path, is_allowed_extension,
    is_binary_content, looks_like_base64_binary, push_schema_to_server,
    remove_file_state_and_abort, spawn_file_sync_tasks_crdt, FileSyncState, SharedLastContent,
    SyncState,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::{Doc, Transact, Update};

/// Decode an MQTT schema update payload into an FsSchema (stateless fallback).
///
/// **WARNING: This function cannot correctly handle DELETE operations!**
///
/// This function creates a fresh empty Y.Doc and applies the delta update to it.
/// DELETE operations in the delta have no effect because the content being deleted
/// doesn't exist in the empty doc. This causes deleted files to reappear locally.
///
/// **Prefer using `apply_schema_update_to_state()` instead**, which loads the existing
/// Y.Doc state before applying the delta, allowing DELETE operations to work correctly.
///
/// This function should only be used as a last-resort fallback when:
/// 1. CRDT context is not available, AND
/// 2. Loading state from disk fails, AND
/// 3. The caller can tolerate DELETE operations being ignored (will fall back to HTTP)
///
/// See bug CP-hwg2 for details on this limitation.
///
/// Returns None if decoding fails (caller should fall back to HTTP fetch).
pub fn decode_schema_from_mqtt_payload(payload: &[u8]) -> Option<(FsSchema, String)> {
    // Parse the EditMessage JSON
    let edit_msg = match parse_edit_message(payload) {
        Ok(m) => m,
        Err(e) => {
            debug!("Failed to parse MQTT schema payload: {}", e);
            return None;
        }
    };

    // Decode base64 Yrs update
    let update_bytes = match STANDARD.decode(&edit_msg.update) {
        Ok(b) => b,
        Err(e) => {
            debug!("Failed to decode base64 schema update: {}", e);
            return None;
        }
    };

    // Create a fresh Y.Doc and apply the update
    let doc = Doc::new();
    {
        let mut txn = doc.transact_mut();
        match Update::decode_v1(&update_bytes) {
            Ok(update) => {
                txn.apply_update(update);
            }
            Err(e) => {
                debug!("Failed to decode Yrs update: {:?}", e);
                return None;
            }
        }
    }

    // Convert YMap format to FsSchema
    let schema = ymap_schema::to_fs_schema(&doc);

    // Also serialize back to JSON for writing to local schema file
    let schema_json = match serde_json::to_string_pretty(&schema) {
        Ok(j) => j,
        Err(e) => {
            debug!("Failed to serialize schema to JSON: {}", e);
            return None;
        }
    };

    Some((schema, schema_json))
}

/// Result of applying a schema CRDT update, including explicit deletion tracking.
/// See CP-seha: deletions are detected by before/after YMap comparison rather than
/// schema-diff against disk.
pub struct SchemaUpdateResult {
    pub schema: FsSchema,
    pub schema_json: String,
    /// Entry names that were explicitly removed by this CRDT update.
    /// Empty when state was uninitialized (fresh Y.Doc has no "before" entries).
    pub deleted_entries: std::collections::HashSet<String>,
}

/// Apply an MQTT schema update to persistent CRDT state.
///
/// Unlike `decode_schema_from_mqtt_payload` which creates a fresh Y.Doc,
/// this function loads the existing Y.Doc state, applies the delta, and
/// saves the updated state back. This is critical for DELETE operations
/// where the delta removes entries - applying a delete to an empty doc
/// has no effect, but applying it to a doc with the entries works correctly.
///
/// # Arguments
/// * `state` - The persistent CRDT state for this schema document
/// * `payload` - The raw MQTT payload containing the EditMessage
///
/// # Returns
/// * `Some(SchemaUpdateResult)` - The decoded schema, JSON, and deletion tracking
/// * `None` - If decoding fails (caller should fall back to HTTP fetch)
pub fn apply_schema_update_to_state(
    state: &mut crate::sync::CrdtPeerState,
    payload: &[u8],
) -> Option<SchemaUpdateResult> {
    // Parse the EditMessage JSON
    let edit_msg = match parse_edit_message(payload) {
        Ok(m) => m,
        Err(e) => {
            debug!("Failed to parse MQTT schema payload: {}", e);
            return None;
        }
    };

    // Decode base64 Yrs update
    let update_bytes = match STANDARD.decode(&edit_msg.update) {
        Ok(b) => b,
        Err(e) => {
            debug!("Failed to decode base64 schema update: {}", e);
            return None;
        }
    };

    // Load existing Y.Doc from state, or create fresh if none exists.
    let doc = match state.to_doc() {
        Ok(d) => {
            let before_count = ymap_schema::list_entry_names(&d).len();
            debug!(
                "[CRDT-DEBUG] Loaded existing Y.Doc with {} entries",
                before_count
            );
            d
        }
        Err(e) => {
            info!("[CRDT-DEBUG] Failed to load Y.Doc: {:?}, creating fresh", e);
            Doc::new()
        }
    };

    // Capture entry names BEFORE applying update for deletion detection (CP-seha)
    let before_entries = ymap_schema::list_entry_names(&doc);

    // Apply the incoming update
    {
        let mut txn = doc.transact_mut();
        match Update::decode_v1(&update_bytes) {
            Ok(update) => {
                txn.apply_update(update);
            }
            Err(e) => {
                debug!("Failed to decode Yrs update: {:?}", e);
                return None;
            }
        }
    }

    // Detect explicit deletions: entries present before but absent after (CP-seha)
    let after_entry_names = ymap_schema::list_entry_names(&doc);
    let deleted_entries: std::collections::HashSet<String> = before_entries
        .difference(&after_entry_names)
        .cloned()
        .collect();

    if !deleted_entries.is_empty() {
        info!(
            "[CRDT-DELETE] Detected {} explicit deletions: {:?}",
            deleted_entries.len(),
            deleted_entries
        );
    }

    let after_count = after_entry_names.len();
    debug!(
        "[CRDT-DEBUG] After applying update: {} entries",
        after_count
    );

    // Save the updated doc back to state
    state.update_from_doc(&doc);

    // Compute the CID for this edit and update state tracking.
    // This mirrors the logic in crdt_merge::process_received_edit to ensure
    // ancestry checks work correctly for CRDT-based schema processing.
    let commit = Commit::with_timestamp(
        edit_msg.parents.clone(),
        edit_msg.update.clone(),
        edit_msg.author.clone(),
        edit_msg.message.clone(),
        edit_msg.timestamp,
    );
    let received_cid = commit.calculate_cid();

    // Update head_cid to track what we've received from the server.
    // Also update local_head_cid if we don't have local changes ahead of this.
    // This is a simplified fast-forward since schema changes from MQTT are
    // typically server-authoritative.
    state.head_cid = Some(received_cid.clone());
    if state.local_head_cid.is_none() || state.local_head_cid == state.head_cid {
        state.local_head_cid = Some(received_cid.clone());
    }
    debug!(
        "[CRDT-DEBUG] Updated schema CID tracking: head_cid={}",
        received_cid
    );

    // Convert YMap format to FsSchema
    let schema = ymap_schema::to_fs_schema(&doc);

    // Also serialize back to JSON for writing to local schema file
    let schema_json = match serde_json::to_string_pretty(&schema) {
        Ok(j) => j,
        Err(e) => {
            debug!("Failed to serialize schema to JSON: {}", e);
            return None;
        }
    };

    Some(SchemaUpdateResult {
        schema,
        schema_json,
        deleted_entries,
    })
}

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

            if schema.has_path(&relative_path) {
                return true;
            }
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
    // Guard: If schema is empty (root=None or 0 entries), treat it as "unknown"
    // rather than "everything deleted". An empty schema can result from applying
    // a delta to uninitialized CRDT state, and deleting all directories would
    // cause data loss. Explicit CRDT deletions (via apply_explicit_deletions)
    // handle real deletions without relying on schema-diff.
    let entry_count = schema
        .root
        .as_ref()
        .and_then(|e| match e {
            crate::fs::Entry::Dir(dir) => dir.entries.as_ref().map(|e| e.len()),
            _ => None,
        })
        .unwrap_or(0);
    if entry_count == 0 {
        info!(
            "[CRDT-GUARD] Skipping orphaned directory cleanup: schema has 0 entries \
             (likely uninitialized CRDT state). Directory: {}",
            directory.display()
        );
        return;
    }

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
    for (dir_path, name) in &dirs_to_remove {
        info!(
            "Removing orphaned directory (not in schema): {}",
            dir_path.display()
        );
        if let Err(e) = tokio::fs::remove_dir_all(dir_path).await {
            warn!("Failed to remove directory {}: {}", dir_path.display(), e);
        }
        // Unmark this directory from synced state
        if let Err(e) = unmark_directory_synced(directory, name).await {
            debug!("Failed to unmark directory synced: {}", e);
        }
    }
}

/// Handle a subdirectory schema change by fetching, validating, cleaning up, and
/// deleting files that were removed from the server.
///
/// This function handles SSE "edit" events for node-backed subdirectories by:
/// 1. Fetching the subdirectory schema from the server (or using provided schema from MQTT)
/// 2. Validating and writing it to the local .commonplace.json file
/// 3. Deleting local files that no longer exist in the server schema
/// 4. Cleaning up orphaned directories not in the schema
///
/// If `mqtt_schema` is provided, it will be used directly instead of fetching from HTTP.
/// This avoids race conditions where the server hasn't processed the MQTT edit yet.
///
/// File sync tasks for NEW files in subdirectories are tracked in CP-fs3x.
/// This function focuses on cleanup (deletions), not pulling new content.
#[allow(clippy::too_many_arguments)]
pub async fn handle_subdir_schema_cleanup(
    client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    subdir_directory: &Path,
    root_directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    mqtt_schema: Option<(FsSchema, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Track whether we're using MQTT schema (authoritative) vs HTTP (may be stale)
    let using_mqtt_schema = mqtt_schema.is_some();

    // Use provided schema or fetch from server.
    // When using persistent CRDT state (via apply_schema_update_to_state), DELETE operations
    // are handled correctly because the delta is applied to the existing doc.
    let (schema, schema_json) = if let Some((s, json)) = mqtt_schema {
        debug!(
            "Using MQTT-decoded schema for subdir {} cleanup (avoids HTTP race)",
            subdir_path
        );
        (s, json)
    } else {
        // Fetch, parse, and validate schema from server
        let fetched = match fetch_and_validate_schema(client, server, subdir_node_id, true).await {
            Some(f) => f,
            None => return Ok(()), // Logged inside fetch_and_validate_schema
        };
        (fetched.schema, fetched.content)
    };

    // Write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(subdir_directory, &schema_json, None).await {
        warn!("Failed to write subdir schema file: {}", e);
    }

    // Extract filenames from the MQTT/local schema for deletion checks.
    // This is critical: we must use the schema we received (which reflects the delete)
    // rather than fetching from HTTP which may still have the old state due to race conditions.
    let schema_entry_names: std::collections::HashSet<String> =
        if let Some(Entry::Dir(root_dir)) = &schema.root {
            if let Some(ref entries) = root_dir.entries {
                entries.keys().cloned().collect()
            } else {
                std::collections::HashSet::new()
            }
        } else {
            std::collections::HashSet::new()
        };

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

    // Safety check: if ANY fetch failed → map may be incomplete, skip deletions
    let skip_deletions = if !all_fetches_succeeded {
        debug!(
            "Subdir {} had fetch failures during UUID map building - skipping file deletion to avoid data loss",
            subdir_path
        );
        true
    } else if schema_has_entries && subdir_uuid_map.is_empty() && schema_entry_names.is_empty() {
        // Only skip if both sources are empty - this indicates a potential race condition
        // where entries exist but couldn't be extracted from either source
        debug!(
            "Subdir {} schema has entries but both UUID map and entry names are empty - potential race, skipping deletions",
            subdir_path
        );
        true
    } else {
        false
    };

    // Guard: If schema has 0 entries, treat it as unknown rather than empty.
    // An empty schema can result from applying a delta to uninitialized CRDT state.
    // Real deletions are handled by apply_explicit_deletions via before/after comparison.
    if schema_entry_names.is_empty() {
        info!(
            "[CRDT-GUARD] Skipping schema-diff deletions for '{}': schema has 0 entries \
             (likely uninitialized CRDT state). Real deletions handled by CRDT before/after comparison.",
            subdir_path
        );
    } else if skip_deletions {
        // Don't proceed with deletions, just clean up orphaned directories
    } else {
        // Build the set of paths that legitimately exist in the schema.
        // We need to combine:
        // 1. Top-level entries from schema_entry_names (MQTT-derived, authoritative for deletes)
        // 2. Recursive paths from subdir_uuid_map (for files in nested node-backed directories)
        //
        // For top-level files, schema_entry_names is authoritative (avoids HTTP race conditions).
        // But for node-backed subdirectories, the entries in schema_entry_names are just directory
        // names - the actual files live in nested schemas and are captured by subdir_uuid_map.
        let mut schema_paths: std::collections::HashSet<String> = schema_entry_names
            .iter()
            .map(|name| {
                if subdir_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", subdir_path, name)
                }
            })
            .collect();

        // Add ONLY nested paths from subdir_uuid_map (files in node-backed subdirectories).
        // We skip top-level files because:
        // 1. They're already covered by schema_entry_names (MQTT-authoritative)
        // 2. HTTP may be stale after an MQTT delete, so using HTTP paths for top-level
        //    files would prevent deletions from working correctly
        // Nested paths (containing "/") are safe because they exist in nested schemas
        // that schema_entry_names doesn't capture.
        for rel_path in subdir_uuid_map.keys() {
            // Only include paths that are nested (contain a "/" separator)
            if rel_path.contains('/') {
                let root_relative = if subdir_path.is_empty() {
                    rel_path.clone()
                } else {
                    format!("{}/{}", subdir_path, rel_path)
                };
                schema_paths.insert(root_relative);
            }
        }

        // Find files in file_states that are under this subdir but no longer in schema.
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

        // ALSO scan the actual disk directory for files not in schema.
        // This is critical for sandbox sync: files may exist on disk (written via MQTT)
        // but not be tracked in file_states. Without this disk scan, such files would
        // never be deleted when removed from the schema.
        //
        // IMPORTANT: Use schema_entry_names (derived from the MQTT/local schema we received)
        // rather than schema_filenames (derived from HTTP UUID map). The MQTT schema reflects
        // the actual delete, while HTTP may still have stale data due to race conditions.
        let disk_deleted_paths: Vec<String> = {
            let mut paths = Vec::new();
            if let Ok(mut entries) = tokio::fs::read_dir(subdir_directory).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let entry_path = entry.path();
                    if !entry_path.is_file() {
                        continue;
                    }
                    let filename = match entry.file_name().into_string() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };
                    // Skip hidden files and schema files
                    if filename.starts_with('.') {
                        continue;
                    }
                    // If this file is not in the MQTT schema, mark for deletion
                    // Use schema_entry_names from the MQTT payload, not HTTP-fetched schema_filenames
                    if !schema_entry_names.contains(&filename) {
                        let full_path = if subdir_path.is_empty() {
                            filename.clone()
                        } else {
                            format!("{}/{}", subdir_path, filename)
                        };
                        // Only add if not already tracked in file_states (avoid double-processing)
                        if !deleted_paths.contains(&full_path) {
                            paths.push(full_path);
                        }
                    }
                }
            }
            paths
        };

        // Combine paths from file_states and disk scan
        let all_deleted_paths: Vec<String> = deleted_paths
            .into_iter()
            .chain(disk_deleted_paths.into_iter())
            .collect();

        // Delete files that were removed from server
        for path in &all_deleted_paths {
            let file_path = root_directory.join(path);

            // Safety check: don't delete files that were modified very recently (within 10 seconds)
            // ONLY when using HTTP-fetched schema. When using MQTT schema, the delete is
            // authoritative and should proceed regardless of local modification time.
            //
            // This protects against race conditions where we just created a file via CRDT but
            // the server schema hasn't been updated yet. Without this check, the cleanup would
            // see the file isn't in the server schema and delete it prematurely.
            //
            // When using_mqtt_schema is true, we trust the MQTT-delivered schema as authoritative
            // because MQTT messages are delivered in causal order within a topic.
            if !using_mqtt_schema && file_path.exists() && file_path.is_file() {
                if let Ok(metadata) = file_path.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(elapsed) = modified.elapsed() {
                            if elapsed.as_secs() < 10 {
                                debug!(
                                    "Subdir {} skipping deletion of recently modified file: {} (modified {}s ago)",
                                    subdir_path, path, elapsed.as_secs()
                                );
                                continue;
                            }
                        }
                    }
                }
            }

            info!(
                "Subdir {} removed file: {} - deleting local copy",
                subdir_path, path
            );

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

/// Apply explicit file deletions detected from CRDT before/after comparison.
///
/// Unlike handle_subdir_schema_cleanup which infers deletions by comparing schema
/// against disk (fragile when schema is incomplete), this function only deletes
/// files that were explicitly removed from the CRDT YMap. See CP-seha.
///
/// Handles both file entries (deletes the file) and directory entries (deletes
/// the directory tree and unmarks from sync state).
pub async fn apply_explicit_deletions(
    subdir_path: &str,
    subdir_directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    deleted_entries: &std::collections::HashSet<String>,
) {
    if deleted_entries.is_empty() {
        return;
    }

    info!(
        "[CRDT-DELETE] Applying {} explicit deletions for '{}': {:?}",
        deleted_entries.len(),
        subdir_path,
        deleted_entries
    );

    for entry_name in deleted_entries {
        let entry_path = subdir_directory.join(entry_name);

        // Build the relative path key used in file_states
        let relative_key = if subdir_path.is_empty() {
            entry_name.clone()
        } else {
            format!("{}/{}", subdir_path, entry_name)
        };

        if entry_path.is_dir() {
            // Directory entry was removed - delete the directory tree
            info!(
                "[CRDT-DELETE] Removing directory '{}' (explicitly deleted from schema)",
                entry_path.display()
            );
            if let Err(e) = tokio::fs::remove_dir_all(&entry_path).await {
                warn!("Failed to remove directory {}: {}", entry_path.display(), e);
            }
            // Unmark from synced directories state
            if let Err(e) = unmark_directory_synced(subdir_directory, entry_name).await {
                debug!("Failed to unmark directory synced: {}", e);
            }
        } else if entry_path.is_file() {
            // File entry was removed - delete the file
            info!(
                "[CRDT-DELETE] Removing file '{}' (explicitly deleted from schema)",
                entry_path.display()
            );

            // Remove from file_states and abort sync tasks
            remove_file_state_and_abort(file_states, &relative_key).await;

            if let Err(e) = tokio::fs::remove_file(&entry_path).await {
                warn!("Failed to delete file {}: {}", entry_path.display(), e);
            }
        } else {
            // Entry doesn't exist on disk - nothing to delete
            debug!(
                "[CRDT-DELETE] Entry '{}' not found on disk, skipping",
                entry_path.display()
            );
        }
    }
}

/// Handle syncing NEW files that appear in a subdirectory schema.
///
/// This is called from `subdir_mqtt_task` when a subdirectory schema changes.
/// It finds files in the server schema that don't exist locally and syncs them.
///
/// When `crdt_context` is provided, CRDT sync tasks are spawned instead of HTTP sync tasks.
/// This enables file edits to be published via MQTT instead of HTTP.
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
    crdt_context: Option<&CrdtFileSyncContext>,
    mqtt_schema: Option<(crate::fs::FsSchema, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Don't pull if push_only mode
    if push_only {
        return Ok(());
    }

    #[cfg(unix)]
    let inode_tracker = inode_tracker;
    #[cfg(not(unix))]
    let inode_tracker = None;

    if crdt_context
        .as_ref()
        .map(|ctx| ctx.mqtt_only_config.mqtt_only)
        .unwrap_or(false)
    {
        warn!(
            "Skipping subdir new file sync for '{}' (MQTT-only mode, HTTP disabled)",
            subdir_path
        );
        return Ok(());
    }

    // Use MQTT schema if provided, otherwise fetch from HTTP
    let schema = if let Some((schema, _content)) = mqtt_schema {
        schema
    } else {
        // Fetch and parse schema from server (validation only, no root check needed)
        match fetch_and_validate_schema(client, server, subdir_node_id, false).await {
            Some(fetched) => fetched.schema,
            None => return Ok(()), // Logged inside fetch_and_validate_schema
        }
    };

    // Build UUID map for the subdirectory (for files with explicit node_id)
    let subdir_uuid_map = build_uuid_map_recursive(client, server, subdir_node_id).await;

    // Convert to path -> (root_relative_path, subdir_relative_path, Option<node_id>) tuples
    // We need the subdir-relative path for API calls but root-relative path for file_states
    let mut schema_paths: Vec<(String, String, Option<String>)> = subdir_uuid_map
        .into_iter()
        .map(|(subdir_relative_path, node_id)| {
            let root_relative_path = if subdir_path.is_empty() {
                subdir_relative_path.clone()
            } else {
                format!("{}/{}", subdir_path, subdir_relative_path)
            };
            (root_relative_path, subdir_relative_path, Some(node_id))
        })
        .collect();

    // Also add files from schema.root.entries that aren't in the UUID map yet.
    // This handles two cases:
    // 1. Files without node_id (haven't been assigned UUIDs yet)
    // 2. Files with node_id that were added via MQTT but not yet visible via HTTP
    //    (race condition where HTTP returns stale data)
    if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
        if let Some(ref entries) = root.entries {
            for (name, entry) in entries {
                if name.starts_with('.') {
                    continue; // Skip hidden files/schema
                }
                if let crate::fs::Entry::Doc(doc) = entry {
                    let root_relative_path = if subdir_path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", subdir_path, name)
                    };
                    // Only add if not already in schema_paths (from HTTP uuid_map)
                    if !schema_paths
                        .iter()
                        .any(|(p, _, _)| p == &root_relative_path)
                    {
                        // Use node_id from schema if available, otherwise None
                        schema_paths.push((root_relative_path, name.clone(), doc.node_id.clone()));
                    }
                }
            }
        }
    }

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
        // When use_paths=false, use node_id for /docs/* API, or derive from fs_root:path
        let identifier = if use_paths {
            root_relative_path.clone()
        } else {
            node_id
                .clone()
                .unwrap_or_else(|| format!("{}:{}", subdir_node_id, root_relative_path))
        };

        // READINESS FIX: If file already exists locally, skip HTTP fetch.
        // This handles the race where handle_file_created_crdt is processing a new file
        // while directory_mqtt_task receives the schema update and calls this function.
        // See: CP-1ual (MQTT_FALLBACK race condition)
        if file_path.exists() {
            debug!(
                "File {} already exists locally, skipping HTTP fetch for subdir sync",
                root_relative_path
            );

            // Read local content and start sync tasks
            if let Some(ctx) = crdt_context {
                if let Ok(node_uuid) = Uuid::parse_str(&identifier) {
                    // Check if already registered
                    {
                        let states = file_states.read().await;
                        if states.contains_key(root_relative_path) {
                            debug!("File {} already registered, skipping", root_relative_path);
                            continue;
                        }
                    }

                    let filename = file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(&identifier)
                        .to_string();

                    let local_content = tokio::fs::read_to_string(&file_path).await.ok();
                    let shared_last_content: SharedLastContent =
                        Arc::new(RwLock::new(local_content.clone()));
                    let content_hash = compute_content_hash(
                        local_content.as_ref().map(|s| s.as_bytes()).unwrap_or(b""),
                    );

                    // Create state
                    let state = if let Some(sf) = shared_state_file {
                        Arc::new(RwLock::new(SyncState::for_directory_file(
                            None, // CID will be updated by CRDT sync
                            sf.clone(),
                            root_relative_path.clone(),
                        )))
                    } else {
                        Arc::new(RwLock::new(SyncState::new()))
                    };

                    // Register file state BEFORE spawning tasks
                    {
                        let mut states = file_states.write().await;
                        if states.contains_key(root_relative_path) {
                            debug!(
                                "Race detected: file {} already registered, skipping",
                                root_relative_path
                            );
                            continue;
                        }
                        states.insert(
                            root_relative_path.clone(),
                            FileSyncState {
                                relative_path: root_relative_path.clone(),
                                identifier: identifier.clone(),
                                state: state.clone(),
                                task_handles: Vec::new(),
                                use_paths,
                                content_hash: Some(content_hash),
                                crdt_last_content: Some(shared_last_content.clone()),
                            },
                        );
                    }

                    // Load subdirectory state from cache
                    let subdir_node_uuid = match Uuid::parse_str(subdir_node_id) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!("Invalid subdir_node_id '{}': {}", subdir_node_id, e);
                            continue;
                        }
                    };
                    let subdir_directory = root_directory.join(subdir_path);
                    let subdir_state = match ctx
                        .subdir_cache
                        .get_or_load(&subdir_directory, subdir_node_uuid)
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(
                                "Failed to load CRDT state for subdir {}: {}",
                                subdir_path, e
                            );
                            continue;
                        }
                    };

                    // Spawn CRDT sync tasks
                    let handles = spawn_file_sync_tasks_crdt(
                        ctx.mqtt_client.clone(),
                        client.clone(),
                        server.to_string(),
                        ctx.workspace.clone(),
                        node_uuid,
                        file_path.clone(),
                        subdir_state,
                        filename,
                        shared_last_content.clone(),
                        pull_only,
                        author.to_string(),
                        ctx.mqtt_only_config,
                        inode_tracker.clone(),
                        None,
                        None,
                    );

                    // Update task handles
                    {
                        let mut states = file_states.write().await;
                        if let Some(file_state) = states.get_mut(root_relative_path) {
                            file_state.task_handles = handles;
                        }
                    }

                    continue; // Skip the rest of the loop iteration
                }
            }
            // If no CRDT context or invalid UUID, fall through to existing logic
        }

        // Fetch content from server
        let fetch_result = fetch_head(client, server, &identifier, use_paths).await;

        // Track if we have a valid UUID from MQTT schema for fallback when HTTP returns 404
        let has_mqtt_uuid = node_id.is_some() && crdt_context.is_some();

        if let Ok(Some(file_head)) = fetch_result {
            // Note: We must NOT skip when content is empty. If we skip, the file
            // won't be registered in file_states, and subsequent MQTT messages
            // for the file's UUID will be ignored. Instead, create an empty file
            // and start sync tasks - the MQTT receive task will handle content updates.
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
                s.last_written_content = file_head.content.clone();
            }

            info!("Created local file from subdir: {}", file_path.display());

            // RACE CONDITION FIX: Register file state BEFORE spawning tasks.
            // This ensures any messages that arrive immediately after task spawn
            // can find the registered state. We register with empty task_handles
            // initially, then update after spawning.
            {
                let mut states = file_states.write().await;
                // Check for race condition - another task might have registered this file
                if states.contains_key(root_relative_path) {
                    warn!(
                        "Race detected: file {} already registered during subdir sync, skipping",
                        root_relative_path
                    );
                    continue;
                }
                // Register state with empty task_handles - will be updated after spawn
                states.insert(
                    root_relative_path.clone(),
                    FileSyncState {
                        relative_path: root_relative_path.clone(),
                        identifier: identifier.clone(),
                        state: state.clone(),
                        task_handles: Vec::new(), // Empty initially, updated after spawn
                        use_paths,
                        content_hash: Some(content_hash.clone()),
                        crdt_last_content: None, // Updated for CRDT path after spawn
                    },
                );
            }

            // Spawn sync tasks for the new file
            // Use CRDT tasks if context is provided and identifier is a valid UUID
            let (task_handles, crdt_last_content) = if let Some(ctx) = crdt_context {
                // Try to parse identifier as UUID for CRDT sync
                if let Ok(node_uuid) = Uuid::parse_str(&identifier) {
                    // Initialize CRDT state from server
                    let filename = file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(&identifier)
                        .to_string();

                    // Create shared_last_content BEFORE init so we can update it during init
                    // This prevents the file watcher from detecting init writes as local changes
                    let initial_content = tokio::fs::read_to_string(&file_path)
                        .await
                        .ok()
                        .filter(|s| !s.is_empty());
                    let shared_last_content: SharedLastContent =
                        Arc::new(RwLock::new(initial_content));

                    // CRITICAL FIX: Use subdirectory's state from cache, not root's state.
                    // See comment in MQTT fallback path above for full explanation.
                    let subdir_node_uuid = match Uuid::parse_str(subdir_node_id) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(
                                "Invalid subdir_node_id '{}' for HTTP path: {}",
                                subdir_node_id, e
                            );
                            continue;
                        }
                    };
                    let subdir_directory = root_directory.join(subdir_path);
                    let subdir_state = match ctx
                        .subdir_cache
                        .get_or_load(&subdir_directory, subdir_node_uuid)
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(
                                "Failed to load CRDT state for subdirectory {} (HTTP path): {}",
                                subdir_path, e
                            );
                            continue;
                        }
                    };

                    if let Err(e) =
                        crate::sync::file_sync::initialize_crdt_state_from_server_with_pending(
                            client,
                            server,
                            node_uuid,
                            &subdir_state,
                            &filename,
                            &file_path,
                            Some(&ctx.mqtt_client),
                            Some(&ctx.workspace),
                            Some(author),
                            Some(&shared_last_content),
                            inode_tracker.as_ref(),
                            ctx.mqtt_only_config,
                        )
                        .await
                    {
                        warn!(
                            "Failed to initialize CRDT state for {}: {}",
                            root_relative_path, e
                        );
                    }

                    // State is already registered, spawn tasks using subdirectory state
                    // Pass None for file_states since we already registered
                    let handles = spawn_file_sync_tasks_crdt(
                        ctx.mqtt_client.clone(),
                        client.clone(),
                        server.to_string(),
                        ctx.workspace.clone(),
                        node_uuid,
                        file_path.clone(),
                        subdir_state,
                        filename,
                        shared_last_content.clone(),
                        pull_only,
                        author.to_string(),
                        ctx.mqtt_only_config,
                        inode_tracker.clone(),
                        None, // Already registered, no need to check
                        None,
                    );
                    (handles, Some(shared_last_content))
                } else {
                    // Non-UUID identifier - CRDT sync requires a valid UUID
                    warn!(
                        "Subdir {} file {} has non-UUID identifier '{}', skipping sync",
                        subdir_path, root_relative_path, identifier
                    );
                    continue;
                }
            } else {
                // No CRDT context available - CRDT is required for sync
                warn!(
                    "No CRDT context for subdir {} file {}, skipping sync",
                    subdir_path, root_relative_path
                );
                continue;
            };

            // Update the registered state with task handles and crdt_last_content
            {
                let mut states = file_states.write().await;
                if let Some(file_state) = states.get_mut(root_relative_path) {
                    file_state.task_handles = task_handles;
                    file_state.crdt_last_content = crdt_last_content;
                }
            }
        } else if has_mqtt_uuid && matches!(fetch_result, Ok(None)) {
            // MQTT-only fallback: HTTP returned 404 but we have a UUID from MQTT schema.
            // This happens when a new file is created via MQTT but the server hasn't
            // processed it yet. Create an empty file and start CRDT tasks - the retained
            // MQTT content message will be received by the CRDT receive task.
            //
            // See: CP-1ual (flaky sync test due to HTTP/MQTT race condition)
            // Trace log for debugging
            {
                use std::io::Write;
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/sandbox-trace.log")
                {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis())
                        .unwrap_or(0);
                    let pid = std::process::id();
                    let _ = writeln!(
                        file,
                        "[{} pid={}] MQTT_FALLBACK create: path={}, uuid={}",
                        timestamp, pid, root_relative_path, identifier
                    );
                }
            }

            info!(
                "MQTT fallback: HTTP 404 for {} but have UUID {} from schema, creating empty file for CRDT sync",
                root_relative_path, identifier
            );

            // Create empty file
            tokio::fs::write(&file_path, "").await?;
            let content_hash = compute_content_hash(b"");

            // Create state with no CID (will be updated when CRDT content arrives)
            let state = if let Some(sf) = shared_state_file {
                Arc::new(RwLock::new(SyncState::for_directory_file(
                    None, // No CID yet
                    sf.clone(),
                    root_relative_path.clone(),
                )))
            } else {
                Arc::new(RwLock::new(SyncState::new()))
            };

            // Register file state before spawning tasks
            {
                let mut states = file_states.write().await;
                if states.contains_key(root_relative_path) {
                    warn!(
                        "Race detected: file {} already registered during MQTT fallback, skipping",
                        root_relative_path
                    );
                    continue;
                }
                states.insert(
                    root_relative_path.clone(),
                    FileSyncState {
                        relative_path: root_relative_path.clone(),
                        identifier: identifier.clone(),
                        state: state.clone(),
                        task_handles: Vec::new(),
                        use_paths,
                        content_hash: Some(content_hash.clone()),
                        crdt_last_content: None,
                    },
                );
            }

            // Start CRDT sync tasks - we have crdt_context and valid UUID guaranteed by has_mqtt_uuid
            if let Some(ctx) = crdt_context {
                if let Ok(node_uuid) = Uuid::parse_str(&identifier) {
                    let filename = file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(&identifier)
                        .to_string();

                    // Create shared_last_content with empty content
                    let shared_last_content: SharedLastContent =
                        Arc::new(RwLock::new(Some(String::new())));

                    // CRITICAL FIX: Use subdirectory's state from cache, not root's state.
                    // The root crdt_state has a nil UUID (because fs_root_id="workspace" isn't a UUID).
                    // When CRDT tasks save to file_path.parent() (the subdirectory), they would
                    // overwrite the subdirectory's correct state with the root's nil UUID state.
                    // This breaks delete operations because the Y.js schema gets cleared on load.
                    let subdir_node_uuid = match Uuid::parse_str(subdir_node_id) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(
                                "Invalid subdir_node_id '{}' for MQTT fallback: {}",
                                subdir_node_id, e
                            );
                            continue;
                        }
                    };
                    let subdir_directory = root_directory.join(subdir_path);
                    let subdir_state = match ctx
                        .subdir_cache
                        .get_or_load(&subdir_directory, subdir_node_uuid)
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(
                                "Failed to load CRDT state for subdirectory {} (MQTT fallback): {}",
                                subdir_path, e
                            );
                            continue;
                        }
                    };

                    // Initialize CRDT state from server (will handle empty/404 case)
                    if let Err(e) =
                        crate::sync::file_sync::initialize_crdt_state_from_server_with_pending(
                            client,
                            server,
                            node_uuid,
                            &subdir_state,
                            &filename,
                            &file_path,
                            Some(&ctx.mqtt_client),
                            Some(&ctx.workspace),
                            Some(author),
                            Some(&shared_last_content),
                            inode_tracker.as_ref(),
                            ctx.mqtt_only_config,
                        )
                        .await
                    {
                        warn!(
                            "Failed to initialize CRDT state for {} (MQTT fallback): {}",
                            root_relative_path, e
                        );
                    }

                    // Spawn CRDT tasks using subdirectory state
                    // Pass file_states and relative_path to enable deduplication check
                    let states_snapshot = file_states.read().await;
                    let handles = spawn_file_sync_tasks_crdt(
                        ctx.mqtt_client.clone(),
                        client.clone(),
                        server.to_string(),
                        ctx.workspace.clone(),
                        node_uuid,
                        file_path.clone(),
                        subdir_state,
                        filename,
                        shared_last_content.clone(),
                        pull_only,
                        author.to_string(),
                        ctx.mqtt_only_config,
                        inode_tracker.clone(),
                        Some(&*states_snapshot),
                        Some(root_relative_path),
                    );
                    drop(states_snapshot);

                    // Update file state with task handles
                    {
                        let mut states = file_states.write().await;
                        if let Some(file_state) = states.get_mut(root_relative_path) {
                            file_state.task_handles = handles;
                            file_state.crdt_last_content = Some(shared_last_content);
                        }
                    }
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
    _push_only: bool,
    pull_only: bool,
    author: &str,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
    shared_state_file: Option<&crate::sync::SharedStateFile>,
    crdt_context: Option<&CrdtFileSyncContext>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(unix)]
    let inode_tracker = inode_tracker;
    #[cfg(not(unix))]
    let inode_tracker = None;
    // Fetch, parse, and validate schema from server
    let fetched = match fetch_and_validate_schema(client, server, fs_root_id, true).await {
        Some(f) => f,
        None => return Ok(()), // Logged inside fetch_and_validate_schema
    };
    let schema = fetched.schema;

    // Log schema entries for debugging
    let entry_count = if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
        root.entries.as_ref().map(|e| e.len()).unwrap_or(0)
    } else {
        0
    };
    debug!(
        "handle_schema_change: fs_root={} entry_count={} dir={}",
        fs_root_id,
        entry_count,
        directory.display()
    );

    // Write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(directory, &fetched.content, written_schemas).await {
        warn!("Failed to write schema file: {}", e);
    }

    // Collect all paths from schema (with explicit node_id if present)
    // Use async version that follows node-backed directories to get complete path->UUID map
    // AND writes nested schema files to local directory (required for find_owning_document fallback)
    let (uuid_map, all_uuids_succeeded) =
        build_uuid_map_and_write_schemas(client, server, fs_root_id, directory, written_schemas)
            .await;
    let mut schema_paths: Vec<(String, Option<String>)> = uuid_map
        .into_iter()
        .map(|(path, node_id)| (path, Some(node_id)))
        .collect();

    // Also add files from schema.root.entries that don't have node_id
    // These files exist in the schema but haven't been assigned UUIDs yet (e.g., newly created)
    // We need to include them so they get created on disk using path-based fetch
    if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
        if let Some(ref entries) = root.entries {
            for (name, entry) in entries {
                if name.starts_with('.') {
                    continue; // Skip hidden files/schema
                }
                if let crate::fs::Entry::Doc(doc) = entry {
                    if doc.node_id.is_none() {
                        // File without node_id - add with None if not already present
                        if !schema_paths.iter().any(|(p, _)| p == name) {
                            schema_paths.push((name.clone(), None));
                        }
                    }
                }
            }
        }
    }

    // Reconcile UUIDs after schema fetch to fix any drift
    if let Some(ctx) = crdt_context {
        let mut state = ctx.crdt_state.write().await;
        if let Err(e) = state.reconcile_with_schema(directory).await {
            warn!("Failed to reconcile UUIDs after schema change: {}", e);
        }
    }

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
                // Skip writing if content is empty - the MQTT retained message
                // will provide the actual content. Writing empty would clobber
                // content that arrives via MQTT.
                if file_head.content.is_empty() {
                    debug!(
                        "Server returned empty content for {}, skipping write (MQTT will handle)",
                        path
                    );
                    continue;
                }
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
                // Use CRDT tasks if context is provided and identifier is a valid UUID
                let (task_handles, crdt_last_content) = if spawn_tasks {
                    if let Some(ctx) = crdt_context {
                        // Try to parse identifier as UUID for CRDT sync
                        if let Ok(node_uuid) = Uuid::parse_str(&identifier) {
                            // Initialize CRDT state from server
                            let filename = file_path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or(&identifier)
                                .to_string();

                            // Create shared_last_content BEFORE init so we can update it during init
                            // This prevents the file watcher from detecting init writes as local changes
                            let initial_content = tokio::fs::read_to_string(&file_path)
                                .await
                                .ok()
                                .filter(|s| !s.is_empty());
                            let shared_last_content: SharedLastContent =
                                Arc::new(RwLock::new(initial_content));

                            // CRITICAL FIX: Use the correct directory's state from cache, not root's state.
                            // For files in subdirectories, we need to load the subdirectory's state.
                            // See comment in handle_subdir_new_files for full explanation.
                            let owning_doc = find_owning_document(directory, fs_root_id, path);
                            let dir_state = if owning_doc.document_id != fs_root_id {
                                // File is in a subdirectory - load that subdirectory's state from cache
                                let subdir_node_uuid = match Uuid::parse_str(
                                    &owning_doc.document_id,
                                ) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        warn!(
                                            "Invalid subdir document_id '{}' for handle_schema_change: {}",
                                            owning_doc.document_id, e
                                        );
                                        continue;
                                    }
                                };
                                match ctx
                                    .subdir_cache
                                    .get_or_load(&owning_doc.directory, subdir_node_uuid)
                                    .await
                                {
                                    Ok(s) => s,
                                    Err(e) => {
                                        warn!(
                                            "Failed to load CRDT state for subdirectory {} in handle_schema_change: {}",
                                            owning_doc.directory.display(), e
                                        );
                                        continue;
                                    }
                                }
                            } else {
                                // File is in root directory - use root's state
                                ctx.crdt_state.clone()
                            };

                            if let Err(e) =
                                crate::sync::file_sync::initialize_crdt_state_from_server_with_pending(
                                    client,
                                    server,
                                    node_uuid,
                                    &dir_state,
                                    &filename,
                                    &file_path,
                                    Some(&ctx.mqtt_client),
                                    Some(&ctx.workspace),
                                    Some(author),
                                    Some(&shared_last_content),
                                    inode_tracker.as_ref(),
                                    ctx.mqtt_only_config,
                                )
                                .await
                            {
                                warn!(
                                    "Failed to initialize CRDT state for {}: {}",
                                    path, e
                                );
                            }

                            // Check for existing tasks before spawning (prevents duplicates)
                            let states_snapshot = file_states.read().await;
                            let handles = spawn_file_sync_tasks_crdt(
                                ctx.mqtt_client.clone(),
                                client.clone(),
                                server.to_string(),
                                ctx.workspace.clone(),
                                node_uuid,
                                file_path.clone(),
                                dir_state,
                                filename,
                                shared_last_content.clone(),
                                pull_only,
                                author.to_string(),
                                ctx.mqtt_only_config,
                                inode_tracker.clone(),
                                Some(&*states_snapshot),
                                Some(path),
                            );
                            drop(states_snapshot);
                            (handles, Some(shared_last_content))
                        } else {
                            // Non-UUID identifier - CRDT sync requires a valid UUID
                            warn!(
                                "File {} has non-UUID identifier '{}', skipping sync",
                                path, identifier
                            );
                            continue;
                        }
                    } else {
                        // No CRDT context available - CRDT is required for sync
                        warn!("No CRDT context for file {}, skipping sync", path);
                        continue;
                    }
                } else {
                    (Vec::new(), None)
                };

                // Skip if spawn was prevented due to existing tasks
                if task_handles.is_empty() && crdt_last_content.is_some() {
                    // CRDT spawn was attempted but prevented - skip this file
                    continue;
                }

                let mut states = file_states.write().await;
                // Check for race condition - another task might have registered this file
                // (this can still happen in the window between read lock release and write lock acquire)
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
                        crdt_last_content,
                    },
                );
            }
        }
    }

    // Guard: If UUID map is incomplete (some subdirectory schemas failed to fetch),
    // skip all deletions. The UUID map only contains files from successfully fetched
    // subdirectories, so deleting files "not in the map" would delete files from
    // subdirectories whose schemas are empty/unreachable on the server.
    // Real deletions are handled by apply_explicit_deletions via CRDT before/after comparison.
    let skip_deletions = !all_uuids_succeeded;
    if skip_deletions {
        info!(
            "[CRDT-GUARD] Skipping schema-diff deletions in handle_schema_change: \
             UUID map is incomplete (some subdirectory schemas failed to fetch). \
             Real deletions handled by CRDT before/after comparison."
        );
    }

    // Delete local files/directories that have been removed from server schema
    let schema_path_set: std::collections::HashSet<&String> =
        schema_paths.iter().map(|(p, _)| p).collect();

    // Build a set of schema filenames for disk scanning (for root directory files)
    // IMPORTANT: Derive from schema.root.entries directly, NOT from uuid_map/schema_paths.
    // The uuid_map only contains files with explicit node_id, but files without node_id
    // (using path-based identification) are valid and should not be deleted.
    let schema_filenames: std::collections::HashSet<String> =
        if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
            if let Some(ref entries) = root.entries {
                entries
                    .keys()
                    .filter(|k| !k.starts_with('.')) // Skip hidden files/schema
                    .cloned()
                    .collect()
            } else {
                std::collections::HashSet::new()
            }
        } else {
            std::collections::HashSet::new()
        };

    if !skip_deletions {
        // Find files that exist in file_states but not in schema
        // IMPORTANT: Check both schema_path_set (for files with node_id) AND schema_filenames
        // (for root-level files without node_id). A file should only be deleted if it's not
        // in the schema at all.
        let deleted_paths: Vec<String> = known_paths
            .iter()
            .filter(|p| {
                // Not in uuid_map (schema_path_set)
                if schema_path_set.contains(p) {
                    return false;
                }
                // For root-level files, also check schema_filenames
                // (which includes files without explicit node_id)
                if !p.contains('/') && schema_filenames.contains(p.as_str()) {
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        // ALSO scan the actual disk directory for files not in schema.
        // This is critical for sandbox sync: files may exist on disk (written via MQTT)
        // but not be tracked in file_states. Without this disk scan, such files would
        // never be deleted when removed from the schema.
        let disk_deleted_paths: Vec<String> = {
            let mut paths = Vec::new();
            if let Ok(mut entries) = tokio::fs::read_dir(directory).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let entry_path = entry.path();
                    if !entry_path.is_file() {
                        continue;
                    }
                    let filename = match entry.file_name().into_string() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };
                    // Skip hidden files and schema files
                    if filename.starts_with('.') {
                        continue;
                    }
                    // If this file is not in the schema, mark for deletion
                    if !schema_filenames.contains(&filename) {
                        // Only add if not already tracked in file_states (avoid double-processing)
                        if !deleted_paths.contains(&filename) {
                            paths.push(filename);
                        }
                    }
                }
            }
            paths
        };

        debug!(
            "delete check: schema_filenames={:?} disk_scan={:?} deleted_paths={:?}",
            schema_filenames, disk_deleted_paths, deleted_paths
        );

        // Combine paths from file_states and disk scan
        let all_deleted_paths: Vec<String> = deleted_paths
            .into_iter()
            .chain(disk_deleted_paths.into_iter())
            .collect();

        for path in &all_deleted_paths {
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

        // Clean up directories that exist locally but not in the schema
        cleanup_orphaned_directories(directory, &schema).await;
    }

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
    crdt_context: Option<&CrdtFileSyncContext>,
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
        crdt_context,
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

/// Merge existing node_ids from server schema into a local schema.
///
/// When the local .commonplace.json is corrupted or incomplete, scan_directory()
/// returns entries with node_id=None even though the server already has UUIDs for
/// those entries. This function copies server node_ids into the local schema,
/// preventing create_documents_for_null_entries() from creating duplicate UUIDs.
/// See CP-7hmh.
fn merge_server_node_ids(local: &mut FsSchema, server: &FsSchema) -> usize {
    let mut merged = 0;

    let (local_entries, server_entries) = match (&mut local.root, &server.root) {
        (Some(Entry::Dir(local_dir)), Some(Entry::Dir(server_dir))) => {
            match (&mut local_dir.entries, &server_dir.entries) {
                (Some(local_e), Some(server_e)) => (local_e, server_e),
                _ => return 0,
            }
        }
        _ => return 0,
    };

    for (name, local_entry) in local_entries.iter_mut() {
        let server_entry = match server_entries.get(name) {
            Some(e) => e,
            None => continue,
        };

        match (local_entry, server_entry) {
            (Entry::Dir(local_dir), Entry::Dir(server_dir)) => {
                if local_dir.node_id.is_none() {
                    if let Some(ref server_id) = server_dir.node_id {
                        info!(
                            "Using existing server UUID {} for directory '{}' (CP-7hmh)",
                            server_id, name
                        );
                        local_dir.node_id = Some(server_id.clone());
                        merged += 1;
                    }
                }
            }
            (Entry::Doc(local_doc), Entry::Doc(server_doc)) => {
                if local_doc.node_id.is_none() {
                    if let Some(ref server_id) = server_doc.node_id {
                        debug!(
                            "Using existing server UUID {} for file '{}' (CP-7hmh)",
                            server_id, name
                        );
                        local_doc.node_id = Some(server_id.clone());
                        merged += 1;
                    }
                }
            }
            _ => {}
        }
    }

    merged
}

/// Create documents on the server for all schema entries with null node_ids.
///
/// Per RECURSIVE_SYNC_THEORY.md: schemas pushed to server must have valid UUIDs.
/// This function creates documents for any entry missing a node_id and returns
/// an updated schema with the assigned UUIDs.
///
/// This ensures we never push schemas with `node_id: null` to the server.
///
/// Also processes nested schemas in subdirectories recursively.
async fn create_documents_for_null_entries(
    client: &Client,
    server: &str,
    schema: &FsSchema,
    directory: &Path,
) -> Result<FsSchema, Box<dyn std::error::Error + Send + Sync>> {
    let mut updated_schema = schema.clone();

    if let Some(Entry::Dir(ref mut root_dir)) = updated_schema.root {
        if let Some(ref mut entries) = root_dir.entries {
            for (name, entry) in entries.iter_mut() {
                match entry {
                    Entry::Doc(doc) if doc.node_id.is_none() => {
                        // Create document for file
                        let content_type = doc
                            .content_type
                            .as_deref()
                            .unwrap_or("application/octet-stream");
                        let create_url = format!("{}/docs", server);
                        let resp = client
                            .post(&create_url)
                            .json(&serde_json::json!({ "content_type": content_type }))
                            .send()
                            .await?;

                        if resp.status().is_success() {
                            if let Ok(body) = resp.json::<serde_json::Value>().await {
                                if let Some(id) = body["id"].as_str() {
                                    info!("Created document {} for file '{}'", id, name);
                                    doc.node_id = Some(id.to_string());
                                }
                            }
                        } else {
                            warn!(
                                "Failed to create document for '{}': {}",
                                name,
                                resp.status()
                            );
                        }
                    }
                    Entry::Dir(dir) => {
                        // Create document for directory if needed
                        if dir.node_id.is_none() {
                            let create_url = format!("{}/docs", server);
                            let resp = client
                                .post(&create_url)
                                .json(&serde_json::json!({ "content_type": "application/json" }))
                                .send()
                                .await?;

                            if resp.status().is_success() {
                                if let Ok(body) = resp.json::<serde_json::Value>().await {
                                    if let Some(id) = body["id"].as_str() {
                                        info!("Created document {} for directory '{}'", id, name);
                                        dir.node_id = Some(id.to_string());
                                    }
                                }
                            } else {
                                warn!(
                                    "Failed to create document for directory '{}': {}",
                                    name,
                                    resp.status()
                                );
                            }
                        }

                        // Recursively process subdirectory schema if it has a node_id
                        if let Some(ref node_id) = dir.node_id {
                            let subdir_path = directory.join(name);
                            if subdir_path.is_dir() {
                                // Scan the subdirectory
                                let options = ScanOptions::default();
                                if let Ok(sub_schema) = scan_directory(&subdir_path, &options) {
                                    // Recursively create documents for null entries in subdirectory
                                    let updated_sub_schema =
                                        Box::pin(create_documents_for_null_entries(
                                            client,
                                            server,
                                            &sub_schema,
                                            &subdir_path,
                                        ))
                                        .await?;

                                    // Write the updated subdirectory schema to disk
                                    let sub_schema_json = schema_to_json(&updated_sub_schema)?;
                                    if let Err(e) =
                                        write_schema_file(&subdir_path, &sub_schema_json, None)
                                            .await
                                    {
                                        warn!(
                                            "Failed to write subdirectory schema for '{}': {}",
                                            name, e
                                        );
                                    }

                                    // Push the subdirectory schema to server
                                    if let Err(e) = push_schema_to_server(
                                        client,
                                        server,
                                        node_id,
                                        &sub_schema_json,
                                        "sync-client",
                                    )
                                    .await
                                    {
                                        warn!(
                                            "Failed to push subdirectory schema for '{}': {}",
                                            name, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        // Entry already has node_id
                    }
                }
            }
        }
    }

    Ok(updated_schema)
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

    // Decide whether to push local schema based on strategy.
    // Guard against pushing a stripped-down local schema over a richer server schema.
    // This prevents data loss when --initial-sync local runs with a stale/minimal local
    // .commonplace.json (e.g., after sandbox readiness timeout). See CP-z7a8.
    let should_push_schema = match initial_sync_strategy {
        "local" => {
            if server_has_content {
                // Check if server has MORE entries than local — don't overwrite richer schema.
                // Count entries from the local scanned schema.
                let local_entry_count = schema
                    .root
                    .as_ref()
                    .and_then(|e| match e {
                        Entry::Dir(d) => d.entries.as_ref().map(|entries| entries.len()),
                        _ => None,
                    })
                    .unwrap_or(0);
                let server_entry_count = match fetch_head(client, server, fs_root_id, false).await {
                    Ok(Some(head)) => serde_json::from_str::<FsSchema>(&head.content)
                        .ok()
                        .and_then(|s| s.root)
                        .and_then(|e| match e {
                            Entry::Dir(d) => d.entries.map(|entries| entries.len()),
                            _ => None,
                        })
                        .unwrap_or(0),
                    _ => 0,
                };
                if server_entry_count > local_entry_count {
                    warn!(
                        "Skipping initial-sync local schema push: server has {} entries but local has only {} (CP-z7a8)",
                        server_entry_count, local_entry_count
                    );
                    false
                } else {
                    true
                }
            } else {
                true
            }
        }
        "server" => !server_has_content,
        "skip" => !server_has_content,
        _ => !server_has_content,
    };

    if should_push_schema {
        // Before creating new documents, check if the server already has UUIDs for
        // entries that appear as null locally (e.g., due to corrupted .commonplace.json).
        // This prevents creating duplicate UUIDs for directories that already exist
        // on the server. See CP-7hmh.
        let mut schema = schema;
        if let Ok(Some(head)) = fetch_head(client, server, fs_root_id, false).await {
            if let Ok(server_schema) = serde_json::from_str::<FsSchema>(&head.content) {
                let merged = merge_server_node_ids(&mut schema, &server_schema);
                if merged > 0 {
                    info!(
                        "Merged {} existing node_ids from server schema (CP-7hmh)",
                        merged
                    );
                }
            }
        }

        // Per RECURSIVE_SYNC_THEORY.md: Create documents for null entries BEFORE pushing schema.
        // This ensures we never push schemas with node_id: null to the server.
        info!("Creating documents for new entries...");
        let schema_with_uuids =
            create_documents_for_null_entries(client, server, &schema, directory).await?;
        let schema_json_with_uuids = schema_to_json(&schema_with_uuids)?;

        // Push schema with UUIDs (no more nulls)
        info!("Pushing filesystem schema to server...");
        push_schema_to_server(client, server, fs_root_id, &schema_json_with_uuids, author).await?;
        info!("Schema pushed successfully");

        // Write the schema with UUIDs to local file
        if let Err(e) = write_schema_file(directory, &schema_json_with_uuids, None).await {
            warn!("Failed to write local schema file: {}", e);
        }

        // No need to wait for reconciler - we already created documents directly

        // Fetch the schema back from server (now with server-assigned UUIDs)
        // and write to local .commonplace.json files.
        // This ensures all sync clients get the same server-assigned UUIDs.
        //
        // IMPORTANT: push_nested_schemas must use the SERVER schema (fetched below),
        // not the local schema. When deep_merge preserves existing server node_ids
        // (CP-7hmh), the server's UUIDs may differ from local. Pushing nested schemas
        // to the server's UUIDs ensures content reaches the right documents.
        let mut final_schema_json = schema_json.clone();
        let mut final_cid: Option<String> = None;
        if let Ok(Some(head)) = fetch_head(client, server, fs_root_id, false).await {
            // Write server's schema (with UUIDs) to local file
            final_schema_json = head.content.clone();
            final_cid = head.cid.clone();
            if let Err(e) = write_schema_file(directory, &head.content, None).await {
                warn!("Failed to write schema file: {}", e);
            }

            if let Ok(server_schema) = serde_json::from_str::<FsSchema>(&head.content) {
                // Push nested schemas using server UUIDs (CP-7hmh).
                // This restores node-backed directory contents after a server database
                // clear, and ensures content is pushed to the correct documents when
                // local and server UUIDs differ.
                if let Err(e) =
                    push_nested_schemas(client, server, directory, &server_schema, author).await
                {
                    warn!("Failed to push nested schemas: {}", e);
                }

                // Write nested schemas (create directories) from server
                info!("Writing nested schemas from server...");
                if let Err(e) =
                    write_nested_schemas(client, server, directory, &server_schema, None).await
                {
                    warn!("Failed to write nested schemas from server: {}", e);
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
#[allow(clippy::too_many_arguments)]
pub async fn handle_schema_modified(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    base_directory: &Path,
    schema_path: &Path,
    content: &str,
    author: &str,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check for echo - if we recently wrote to this path, skip pushing
    // This prevents feedback loops where intermediate writes (e.g., from scan_directory_to_json)
    // get pushed back to the server and overwrite our correct content.
    // The key insight: if we wrote to this file recently, we already pushed the correct content.
    // Any SchemaModified event is likely a stale echo of an intermediate write.
    if let Some(ws) = written_schemas {
        let canonical = schema_path
            .canonicalize()
            .unwrap_or(schema_path.to_path_buf());
        let written = ws.read().await;
        if written.contains_key(&canonical) {
            // We recently wrote to this path - skip this event entirely
            // Our write already pushed the correct content to the server
            tracing::debug!(
                "Skipping schema push for {} - we recently wrote to this file",
                schema_path.display()
            );
            return Ok(());
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::ymap_schema;
    use yrs::{Doc, ReadTxn, Transact};

    /// Helper to create a valid MQTT payload with a schema containing one file
    fn create_test_payload(filename: &str, node_id: &str) -> Vec<u8> {
        // Create a Y.Doc with schema content
        let doc = Doc::new();
        ymap_schema::add_file(&doc, filename, node_id);

        // Get the state as an update
        let txn = doc.transact();
        let update = txn.encode_state_as_update_v1(&yrs::StateVector::default());
        let update_b64 = STANDARD.encode(&update);

        // Create EditMessage JSON
        let edit_msg = serde_json::json!({
            "update": update_b64,
            "parents": [],
            "author": "test",
            "timestamp": 1234567890000_i64
        });

        serde_json::to_vec(&edit_msg).unwrap()
    }

    #[test]
    fn test_decode_schema_from_mqtt_payload_valid() {
        let payload = create_test_payload("test.txt", "uuid-123");
        let result = decode_schema_from_mqtt_payload(&payload);

        assert!(result.is_some());
        let (schema, json) = result.unwrap();

        // Check schema has the file
        assert!(schema.has_path("test.txt"));
        assert!(!json.is_empty());
    }

    #[test]
    fn test_decode_schema_from_mqtt_payload_multiple_files() {
        // Create schema with multiple files
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "file1.txt", "uuid-1");
        ymap_schema::add_file(&doc, "file2.json", "uuid-2");

        let txn = doc.transact();
        let update = txn.encode_state_as_update_v1(&yrs::StateVector::default());
        let update_b64 = STANDARD.encode(&update);

        let edit_msg = serde_json::json!({
            "update": update_b64,
            "parents": ["parent-cid"],
            "author": "test",
            "timestamp": 1234567890000_u64
        });

        let payload = serde_json::to_vec(&edit_msg).unwrap();
        let result = decode_schema_from_mqtt_payload(&payload);

        assert!(result.is_some());
        let (schema, _) = result.unwrap();
        assert!(schema.has_path("file1.txt"));
        assert!(schema.has_path("file2.json"));
    }

    #[test]
    fn test_decode_schema_from_mqtt_payload_invalid_json() {
        let payload = b"not valid json";
        let result = decode_schema_from_mqtt_payload(payload);
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_schema_from_mqtt_payload_invalid_base64() {
        let edit_msg = serde_json::json!({
            "update": "not-valid-base64!!!",
            "parents": [],
            "author": "test"
        });
        let payload = serde_json::to_vec(&edit_msg).unwrap();

        let result = decode_schema_from_mqtt_payload(&payload);
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_schema_from_mqtt_payload_invalid_yrs_update() {
        // Valid base64 but not a valid Yrs update
        let edit_msg = serde_json::json!({
            "update": STANDARD.encode(b"not a valid yrs update"),
            "parents": [],
            "author": "test"
        });
        let payload = serde_json::to_vec(&edit_msg).unwrap();

        let result = decode_schema_from_mqtt_payload(&payload);
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_schema_from_mqtt_payload_empty_schema() {
        // Valid update but empty schema (no files added)
        let doc = Doc::new();
        // Just get the empty state - ymap_schema::to_fs_schema handles empty docs

        let txn = doc.transact();
        let update = txn.encode_state_as_update_v1(&yrs::StateVector::default());
        let update_b64 = STANDARD.encode(&update);

        let edit_msg = serde_json::json!({
            "update": update_b64,
            "parents": [],
            "author": "test",
            "timestamp": 1234567890000_u64
        });
        let payload = serde_json::to_vec(&edit_msg).unwrap();

        let result = decode_schema_from_mqtt_payload(&payload);
        assert!(result.is_some());
        let (schema, _) = result.unwrap();
        // Empty schema should not have any paths
        assert!(!schema.has_path("anything"));
    }

    // =========================================================================
    // Tests for explicit CRDT deletion detection (CP-seha)
    // =========================================================================

    /// Helper: create a valid EditMessage payload from a Y.Doc's full state.
    fn make_edit_payload(doc: &Doc) -> Vec<u8> {
        let txn = doc.transact();
        let update = txn.encode_state_as_update_v1(&yrs::StateVector::default());
        drop(txn);

        let edit_msg = serde_json::json!({
            "update": STANDARD.encode(&update),
            "parents": [],
            "author": "test",
            "message": "test",
            "timestamp": 0_u64
        });
        serde_json::to_vec(&edit_msg).unwrap()
    }

    #[test]
    fn test_uninitialized_state_produces_no_deletions() {
        // Fresh state (no yjs_state) + full schema update = no deletions.
        // This is the CP-l8d2 scenario: fresh Y.Doc receives full state.
        // Before-entries is empty, so deleted_entries must be empty.
        let mut state = crate::sync::CrdtPeerState::new(uuid::Uuid::new_v4());
        assert!(state.needs_server_init());

        // Create a "full state" MQTT payload with 3 files
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "file1.txt", &uuid::Uuid::new_v4().to_string());
        ymap_schema::add_file(&doc, "file2.txt", &uuid::Uuid::new_v4().to_string());
        ymap_schema::add_file(&doc, "file3.txt", &uuid::Uuid::new_v4().to_string());

        let payload = make_edit_payload(&doc);

        let result = apply_schema_update_to_state(&mut state, &payload);
        let r = result.expect("should produce a result");
        assert!(
            r.deleted_entries.is_empty(),
            "Fresh state must produce no deletions"
        );
        assert_eq!(
            r.schema
                .root
                .as_ref()
                .and_then(|e| match e {
                    crate::fs::Entry::Dir(d) => d.entries.as_ref().map(|e| e.len()),
                    _ => None,
                })
                .unwrap_or(0),
            3,
            "Schema should have 3 entries"
        );
    }

    #[test]
    fn test_explicit_deletion_detected() {
        // State has 3 files. Update removes 1 file. deleted_entries should contain it.
        let node1 = uuid::Uuid::new_v4().to_string();
        let node2 = uuid::Uuid::new_v4().to_string();
        let node3 = uuid::Uuid::new_v4().to_string();

        // Create initial state with 3 files
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "file1.txt", &node1);
        ymap_schema::add_file(&doc, "file2.txt", &node2);
        ymap_schema::add_file(&doc, "file3.txt", &node3);

        let mut state = crate::sync::CrdtPeerState::new(uuid::Uuid::new_v4());
        state.update_from_doc(&doc);
        assert!(!state.needs_server_init());

        // Now remove file2.txt from the doc
        ymap_schema::remove_entry(&doc, "file2.txt");

        // Encode the full state (which now has file2.txt tombstoned)
        let payload = make_edit_payload(&doc);

        let result = apply_schema_update_to_state(&mut state, &payload);
        let r = result.expect("should produce a result");
        assert!(
            r.deleted_entries.contains("file2.txt"),
            "file2.txt should be in deleted_entries"
        );
        assert_eq!(r.deleted_entries.len(), 1, "Only 1 deletion expected");
    }

    #[test]
    fn test_idempotent_deletion_on_replay() {
        // After applying deletion once and persisting state, re-applying the same
        // update should produce no new deletions (before and after are identical).
        let node1 = uuid::Uuid::new_v4().to_string();
        let node2 = uuid::Uuid::new_v4().to_string();

        // Create initial state with 2 files
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "file1.txt", &node1);
        ymap_schema::add_file(&doc, "file2.txt", &node2);

        let mut state = crate::sync::CrdtPeerState::new(uuid::Uuid::new_v4());
        state.update_from_doc(&doc);

        // Remove file2.txt
        ymap_schema::remove_entry(&doc, "file2.txt");
        let payload = make_edit_payload(&doc);

        // First application: should detect deletion
        let r1 =
            apply_schema_update_to_state(&mut state, &payload).expect("first apply should succeed");
        assert!(r1.deleted_entries.contains("file2.txt"));

        // Second application (simulates MQTT retained message replay):
        // State already has file2.txt removed, so before and after are the same.
        let r2 = apply_schema_update_to_state(&mut state, &payload)
            .expect("second apply should succeed");
        assert!(
            r2.deleted_entries.is_empty(),
            "Replay must produce no deletions"
        );
    }

    #[test]
    fn test_addition_produces_no_deletions() {
        // Adding a new file to existing state should not produce any deletions.
        let node1 = uuid::Uuid::new_v4().to_string();

        let doc = Doc::new();
        ymap_schema::add_file(&doc, "file1.txt", &node1);

        let mut state = crate::sync::CrdtPeerState::new(uuid::Uuid::new_v4());
        state.update_from_doc(&doc);

        // Add a second file
        let node2 = uuid::Uuid::new_v4().to_string();
        ymap_schema::add_file(&doc, "file2.txt", &node2);

        let payload = make_edit_payload(&doc);

        let r = apply_schema_update_to_state(&mut state, &payload).expect("should succeed");
        assert!(
            r.deleted_entries.is_empty(),
            "Adding file should not produce deletions"
        );
    }

    #[test]
    fn test_merge_server_node_ids_fills_missing() {
        use crate::fs::{DirEntry, DocEntry};

        let mut local = FsSchema {
            version: 1,
            root: Some(Entry::Dir(DirEntry {
                entries: Some(HashMap::from([
                    (
                        "bartleby".to_string(),
                        Entry::Dir(DirEntry {
                            entries: None,
                            node_id: None, // Local doesn't know the UUID
                            content_type: Some("application/json".to_string()),
                        }),
                    ),
                    (
                        "file.txt".to_string(),
                        Entry::Doc(DocEntry {
                            node_id: None, // Local doesn't know the UUID
                            content_type: Some("text/plain".to_string()),
                        }),
                    ),
                ])),
                node_id: None,
                content_type: None,
            })),
        };

        let server = FsSchema {
            version: 1,
            root: Some(Entry::Dir(DirEntry {
                entries: Some(HashMap::from([
                    (
                        "bartleby".to_string(),
                        Entry::Dir(DirEntry {
                            entries: None,
                            node_id: Some("server-dir-uuid".to_string()),
                            content_type: Some("application/json".to_string()),
                        }),
                    ),
                    (
                        "file.txt".to_string(),
                        Entry::Doc(DocEntry {
                            node_id: Some("server-file-uuid".to_string()),
                            content_type: Some("text/plain".to_string()),
                        }),
                    ),
                ])),
                node_id: None,
                content_type: None,
            })),
        };

        let merged = merge_server_node_ids(&mut local, &server);
        assert_eq!(merged, 2, "Should merge 2 node_ids from server");

        if let Some(Entry::Dir(ref root)) = local.root {
            let entries = root.entries.as_ref().unwrap();
            if let Entry::Dir(ref dir) = entries["bartleby"] {
                assert_eq!(
                    dir.node_id.as_deref(),
                    Some("server-dir-uuid"),
                    "Directory node_id should be filled from server"
                );
            }
            if let Entry::Doc(ref doc) = entries["file.txt"] {
                assert_eq!(
                    doc.node_id.as_deref(),
                    Some("server-file-uuid"),
                    "File node_id should be filled from server"
                );
            }
        }
    }

    #[test]
    fn test_merge_server_node_ids_preserves_existing_local() {
        use crate::fs::DirEntry;

        let mut local = FsSchema {
            version: 1,
            root: Some(Entry::Dir(DirEntry {
                entries: Some(HashMap::from([(
                    "bartleby".to_string(),
                    Entry::Dir(DirEntry {
                        entries: None,
                        node_id: Some("local-uuid".to_string()), // Already has UUID
                        content_type: Some("application/json".to_string()),
                    }),
                )])),
                node_id: None,
                content_type: None,
            })),
        };

        let server = FsSchema {
            version: 1,
            root: Some(Entry::Dir(DirEntry {
                entries: Some(HashMap::from([(
                    "bartleby".to_string(),
                    Entry::Dir(DirEntry {
                        entries: None,
                        node_id: Some("server-uuid".to_string()),
                        content_type: Some("application/json".to_string()),
                    }),
                )])),
                node_id: None,
                content_type: None,
            })),
        };

        let merged = merge_server_node_ids(&mut local, &server);
        assert_eq!(merged, 0, "Should not merge when local already has node_id");

        if let Some(Entry::Dir(ref root)) = local.root {
            let entries = root.entries.as_ref().unwrap();
            if let Entry::Dir(ref dir) = entries["bartleby"] {
                assert_eq!(
                    dir.node_id.as_deref(),
                    Some("local-uuid"),
                    "Local node_id should be preserved when it already exists"
                );
            }
        }
    }
}
