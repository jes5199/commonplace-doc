//! Commonplace Sync - Local file synchronization with a Commonplace server
//!
//! This binary syncs a local file or directory with a server-side document node.
//! It watches both directions: local changes push to server,
//! server changes update local files.

use clap::Parser;
use commonplace_doc::events::recv_broadcast;
use commonplace_doc::mqtt::MqttRequestClient;
use commonplace_doc::mqtt::{MqttClient, MqttConfig, Topic};
use commonplace_doc::store::CommitStore;
use commonplace_doc::sync::crdt_state::DirectorySyncState;
#[cfg(unix)]
use commonplace_doc::sync::spawn_shadow_tasks;
use commonplace_doc::sync::state_file::compute_content_hash;
use commonplace_doc::sync::subdir_spawn::{
    spawn_subdir_watchers, SubdirSpawnParams, SubdirTransport,
};
use commonplace_doc::sync::types::InitialSyncComplete;
use commonplace_doc::sync::{
    acquire_sync_lock, add_file_to_schema, build_uuid_map_from_local_schemas, create_new_file,
    detect_from_path, directory_mqtt_task, directory_watcher_task, ensure_fs_root_exists_mqtt,
    ensure_parent_directories_exist, find_owning_document, handle_file_deleted,
    handle_file_modified, handle_schema_modified, push_local_if_differs, remove_file_from_schema,
    remove_file_state_and_abort, rename_file_in_schema, resync_crdt_state_via_cyan_with_pending,
    schema_to_json, set_sync_http_disabled, set_sync_schema_mqtt_request_client,
    spawn_command_listener, spawn_file_sync_tasks_crdt, trace_timeline, wait_for_file_stability,
    write_schema_file, ymap_schema, CrdtFileSyncContext, DirEvent, FileSyncState, InodeKey,
    InodeTracker, InodeTrackerInit, MqttOnlySyncConfig, ScanOptions, SubdirStateCache, SyncError,
    SyncState, TimelineMilestone, SCHEMA_FILENAME,
};
use commonplace_doc::workspace::is_process_running;
use commonplace_doc::{DEFAULT_SERVER_URL, DEFAULT_WORKSPACE};
use reqwest::Client;
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use commonplace_doc::sync::WrittenSchemas;
use tokio::task::JoinHandle;

/// Default shadow dir when running with inode tracking enabled.
const DEFAULT_SHADOW_DIR: &str = "/tmp/commonplace-sync/hardlinks";

fn resolve_shadow_dir(shadow_dir: &str, base_dir: &Path) -> String {
    if shadow_dir.is_empty() {
        return String::new();
    }
    if shadow_dir == DEFAULT_SHADOW_DIR {
        return base_dir
            .join(".commonplace-shadow")
            .to_string_lossy()
            .to_string();
    }
    shadow_dir.to_string()
}

/// Publish initial sync complete event via MQTT.
async fn publish_initial_sync_complete(
    mqtt_client: &Arc<MqttClient>,
    fs_root_id: &str,
    files_synced: usize,
    strategy: &str,
    duration_ms: u64,
) {
    let event = InitialSyncComplete {
        fs_root_id: fs_root_id.to_string(),
        files_synced,
        strategy: strategy.to_string(),
        duration_ms,
    };
    let topic = format!("{}/events/sync/initial-complete", fs_root_id);
    match serde_json::to_string(&event) {
        Ok(payload) => {
            if let Err(e) = mqtt_client
                .publish(&topic, payload.as_bytes(), QoS::AtLeastOnce)
                .await
            {
                warn!("Failed to publish initial-sync-complete event: {}", e);
            } else {
                info!("Published initial-sync-complete event to {}", topic);
            }
        }
        Err(e) => {
            warn!("Failed to serialize initial-sync-complete event: {}", e);
        }
    }
}

/// Resources created by setup_directory_watchers().
///
/// This struct holds the watcher task handle, event receiver, and shared state
/// needed for directory synchronization.
struct WatcherSetup {
    /// Receiver for directory events from the watcher task.
    dir_rx: mpsc::Receiver<DirEvent>,
    /// Handle to the watcher task (None if pull-only mode).
    watcher_handle: Option<JoinHandle<()>>,
    /// Tracks which subdirectories have subscription tasks.
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
}

/// Set up directory watchers and shared state for sync.
///
/// This consolidates the repeated setup logic used in both
/// `run_directory_mode` and `run_exec_mode`.
fn setup_directory_watchers(
    pull_only: bool,
    directory: PathBuf,
    options: ScanOptions,
    written_schemas: WrittenSchemas,
) -> WatcherSetup {
    let (dir_tx, dir_rx) = mpsc::channel::<DirEvent>(100);
    let watcher_handle = if !pull_only {
        Some(tokio::spawn(directory_watcher_task(
            directory,
            dir_tx,
            options,
            Some(written_schemas),
        )))
    } else {
        info!("Pull-only mode: skipping directory watcher");
        None
    };

    WatcherSetup {
        dir_rx,
        watcher_handle,
        watched_subdirs: Arc::new(RwLock::new(HashSet::new())),
    }
}

/// Optional CRDT parameters for handling directory events via MQTT instead of HTTP.
struct CrdtEventParams {
    mqtt_client: Arc<MqttClient>,
    mqtt_request: Arc<MqttRequestClient>,
    workspace: String,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    subdir_cache: Arc<SubdirStateCache>,
    mqtt_only_config: MqttOnlySyncConfig,
    inode_tracker: Option<Arc<RwLock<InodeTracker>>>,
}

/// Handle a single directory event by dispatching to the appropriate handler.
///
/// This consolidates the repeated DirEvent match blocks used in both
/// `run_directory_mode` and `run_exec_mode`.
///
/// When `crdt_params` is provided, file creation uses MQTT/CRDT path instead of HTTP.
#[allow(clippy::too_many_arguments)]
async fn handle_dir_event(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &std::path::Path,
    options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    pull_only: bool,
    author: &str,
    written_schemas: Option<&WrittenSchemas>,
    crdt_params: Option<&CrdtEventParams>,
    event: DirEvent,
) {
    match event {
        DirEvent::Created(path) => {
            if let Some(crdt) = crdt_params {
                handle_file_created_crdt(
                    client,
                    server,
                    &path,
                    directory,
                    fs_root_id,
                    options,
                    file_states,
                    author,
                    crdt,
                    pull_only,
                    written_schemas,
                )
                .await;
            } else {
                warn!(
                    "No CRDT context for file creation event: {}, skipping",
                    path.display()
                );
            }
        }
        DirEvent::Modified(path) => {
            // Skip HTTP schema push when CRDT is enabled - MQTT handles schema propagation
            // The per-file CRDT tasks handle content updates, not the directory-level handler
            if crdt_params.is_none() {
                handle_file_modified(
                    client, server, fs_root_id, directory, &path, options, author,
                )
                .await;
            }
            // When CRDT is enabled, file content changes are handled by the per-file CRDT sync tasks
            // spawned in handle_file_created_crdt. Schema updates are not needed for modifications
            // since the file entry already exists in the schema.
        }
        DirEvent::Deleted(path) => {
            // Use CRDT path if available
            if let Some(crdt) = crdt_params {
                handle_file_deleted_crdt(
                    &path,
                    directory,
                    fs_root_id,
                    options,
                    file_states,
                    author,
                    crdt,
                )
                .await;
            } else {
                handle_file_deleted(
                    client,
                    server,
                    fs_root_id,
                    directory,
                    &path,
                    options,
                    file_states,
                    author,
                )
                .await;
            }
        }
        DirEvent::SchemaModified(path, content) => {
            info!("User edited schema file: {}", path.display());
            // Build CrdtFileSyncContext from CrdtEventParams if available
            let crdt_ctx = crdt_params.map(|p| CrdtFileSyncContext {
                mqtt_client: p.mqtt_client.clone(),
                mqtt_request: p.mqtt_request.clone(),
                workspace: p.workspace.clone(),
                crdt_state: p.crdt_state.clone(),
                subdir_cache: p.subdir_cache.clone(),
                mqtt_only_config: p.mqtt_only_config,
            });
            if let Err(e) = handle_schema_modified(
                client,
                server,
                fs_root_id,
                directory,
                &path,
                &content,
                author,
                written_schemas,
                crdt_ctx.as_ref(),
            )
            .await
            {
                warn!("Failed to push schema change: {}", e);
            }
        }
    }
}

/// Handle file creation via CRDT/MQTT path.
///
/// This function properly handles files in node-backed subdirectories by:
/// 1. Ensuring parent directories exist as node-backed directories
/// 2. Finding the owning document (could be root or a subdirectory)
/// 3. Loading the appropriate DirectorySyncState for that directory
/// 4. Creating the file in the correct schema
#[allow(clippy::too_many_arguments)]
async fn handle_file_created_crdt(
    client: &Client,
    server: &str,
    path: &std::path::Path,
    directory: &std::path::Path,
    fs_root_id: &str,
    options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    author: &str,
    crdt: &CrdtEventParams,
    pull_only: bool,
    written_schemas: Option<&WrittenSchemas>,
) {
    // Skip directories - they will be created via ensure_parent_directories_exist
    // when a file inside them is created
    if path.is_dir() {
        debug!(
            "Skipping directory in handle_file_created_crdt: {}",
            path.display()
        );
        return;
    }
    // Get filename from path
    let filename = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_string(),
        None => {
            warn!("Could not get filename from path: {}", path.display());
            return;
        }
    };

    // Check ignore patterns
    let should_ignore = options.ignore_patterns.iter().any(|pattern| {
        if pattern == &filename {
            true
        } else if pattern.contains('*') {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                filename.starts_with(parts[0]) && filename.ends_with(parts[1])
            } else {
                false
            }
        } else {
            false
        }
    });
    if should_ignore {
        debug!("Ignoring new file (matches ignore pattern): {}", filename);
        return;
    }

    // Skip hidden files unless configured
    if !options.include_hidden && filename.starts_with('.') {
        debug!("Ignoring hidden file: {}", filename);
        return;
    }

    // Skip if pull-only mode
    if pull_only {
        debug!("Skipping file creation in pull-only mode: {}", filename);
        return;
    }

    // Calculate relative path from root directory
    // Canonicalize both paths to ensure prefix stripping works correctly
    // when path is absolute and directory is relative (from --directory arg)
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let canonical_directory = directory
        .canonicalize()
        .unwrap_or_else(|_| directory.to_path_buf());

    let relative_path = match canonical_path.strip_prefix(&canonical_directory) {
        Ok(rel) => rel.to_string_lossy().to_string().replace('\\', "/"),
        Err(_) => {
            warn!(
                "Could not strip directory prefix: path={}, directory={}",
                canonical_path.display(),
                canonical_directory.display()
            );
            filename.clone()
        }
    };

    // Check for rename via inode tracking (same-directory renames only).
    // When a file is renamed (mv foo.txt bar.txt), the watcher sees Delete(foo) + Create(bar).
    // The inode stays the same, so we can detect this by checking if the inode is tracked
    // under a different path.
    #[cfg(unix)]
    if let Some(ref tracker) = crdt.inode_tracker {
        if let Ok(inode_key) = InodeKey::from_path(&canonical_path) {
            let rename_info = {
                let tracker_guard = tracker.read().await;
                tracker_guard.check_rename(&inode_key, &canonical_path)
            };
            if let Some((old_path, stashed_node_id)) = rename_info {
                // Only handle same-directory renames. Cross-directory moves
                // need special handling (commonplace-mv) because the destination
                // directory may not be node-backed yet.
                if old_path.parent() != canonical_path.parent() {
                    info!(
                        "Cross-directory move detected ({} -> {}), treating as new file",
                        old_path.display(),
                        canonical_path.display()
                    );
                } else {
                    // Rename detected! Get node_id from stash (if Deleted ran first)
                    // or from schema (if Created ran first, schema still has old entry)
                    let node_id = if let Some(id) = stashed_node_id {
                        Some(id)
                    } else {
                        // Try reading from schema - old entry may still exist.
                        // Use relative path (not basename) and look up in the correct
                        // schema (root or subdirectory) based on owning document.
                        let old_rel = old_path
                            .strip_prefix(&canonical_directory)
                            .map(|r| r.to_string_lossy().to_string().replace('\\', "/"))
                            .ok();
                        if let Some(old_rel) = old_rel {
                            let old_owning =
                                find_owning_document(&canonical_directory, fs_root_id, &old_rel);
                            if old_owning.document_id != fs_root_id {
                                // Subdirectory-owned file: look up in subdir schema
                                let subdir_node_id =
                                    uuid::Uuid::parse_str(&old_owning.document_id).ok();
                                if let Some(subdir_uuid) = subdir_node_id {
                                    if let Ok(subdir_state_arc) = crdt
                                        .subdir_cache
                                        .get_or_load(&old_owning.directory, subdir_uuid)
                                        .await
                                    {
                                        let subdir_state = subdir_state_arc.read().await;
                                        subdir_state
                                            .schema
                                            .to_doc()
                                            .ok()
                                            .and_then(|doc| {
                                                ymap_schema::get_entry(
                                                    &doc,
                                                    &old_owning.relative_path,
                                                )
                                            })
                                            .and_then(|e| e.node_id)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                // Root-owned file: look up in root schema
                                let state = crdt.crdt_state.read().await;
                                state
                                    .schema
                                    .to_doc()
                                    .ok()
                                    .and_then(|doc| ymap_schema::get_entry(&doc, &old_rel))
                                    .and_then(|e| e.node_id)
                            }
                        } else {
                            None
                        }
                    };

                    if let Some(node_id) = node_id {
                        info!(
                            "Detected rename to {} (preserving node_id: {})",
                            filename, node_id
                        );
                        let rename_ok = handle_file_renamed(
                            path,
                            &old_path,
                            &node_id,
                            directory,
                            fs_root_id,
                            options,
                            file_states,
                            author,
                            crdt,
                            pull_only,
                            client,
                            server,
                            written_schemas,
                        )
                        .await;
                        if rename_ok {
                            return;
                        }
                        // Rename failed — fall through to normal file creation
                        warn!(
                            "Rename handling failed for {}, falling back to create",
                            filename
                        );
                    }
                } // else (same-directory rename)
            }
        }
    }

    // Ensure parent directories exist as node-backed directories via MQTT/CRDT.
    let crdt_ctx = Some(CrdtFileSyncContext {
        mqtt_client: crdt.mqtt_client.clone(),
        mqtt_request: crdt.mqtt_request.clone(),
        workspace: crdt.workspace.clone(),
        crdt_state: crdt.crdt_state.clone(),
        subdir_cache: crdt.subdir_cache.clone(),
        mqtt_only_config: crdt.mqtt_only_config,
    });
    if let Err(e) = ensure_parent_directories_exist(
        client,
        server,
        fs_root_id,
        directory,
        &relative_path,
        options,
        author,
        written_schemas,
        true, // skip_http_schema_push: CRDT context handles schema propagation
        crdt_ctx.as_ref(),
    )
    .await
    {
        warn!(
            "Failed to ensure parent directories for {}: {}",
            relative_path, e
        );
        // Continue anyway - find_owning_document may still work if some directories exist
    }

    // Find which document owns this file (may be a node-backed subdirectory)
    let owning_doc = find_owning_document(&canonical_directory, fs_root_id, &relative_path);
    info!(
        "CRDT file created: {} owned by document {} (relative: {})",
        relative_path, owning_doc.document_id, owning_doc.relative_path
    );

    // Check if file is already being tracked - don't create duplicate UUIDs.
    {
        let states = file_states.read().await;
        if states.contains_key(&relative_path) {
            debug!(
                "File '{}' already in file_states, skipping create_new_file",
                relative_path
            );
            return;
        }
    }

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

    // Read file content
    let content = match tokio::fs::read_to_string(path).await {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to read new file {}: {}", path.display(), e);
            return;
        }
    };

    // Determine which DirectorySyncState to use
    // If the file is in a subdirectory, we need to load that subdirectory's state
    let is_subdirectory = owning_doc.document_id != fs_root_id;

    if is_subdirectory {
        // Load or create state for the subdirectory from cache
        let subdir_node_id = match uuid::Uuid::parse_str(&owning_doc.document_id) {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    "Invalid subdirectory node_id '{}': {}",
                    owning_doc.document_id, e
                );
                return;
            }
        };

        let subdir_state_arc = match crdt
            .subdir_cache
            .get_or_load(&owning_doc.directory, subdir_node_id)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "Failed to load state for subdirectory {}: {}",
                    owning_doc.directory.display(),
                    e
                );
                return;
            }
        };

        // Check if already tracked in subdirectory state
        {
            let subdir_state = subdir_state_arc.read().await;
            if subdir_state.files.contains_key(&owning_doc.relative_path) {
                debug!(
                    "File '{}' already in subdirectory CRDT state, skipping",
                    owning_doc.relative_path
                );
                return;
            }
        }

        // Create file via CRDT using subdirectory state
        let result = {
            let mut subdir_state = subdir_state_arc.write().await;
            // Ensure schema Y.Doc has existing entries before adding new file.
            // Without this, initial HTTP-only sync leaves yjs_state=None and
            // the published schema would only contain the new file, causing
            // peers to delete all previously existing files.
            subdir_state
                .ensure_schema_initialized(&owning_doc.directory)
                .await;
            create_new_file(
                &crdt.mqtt_client,
                &crdt.workspace,
                &mut subdir_state,
                &owning_doc.relative_path,
                &content,
                author,
            )
            .await
        };

        match result {
            Ok(new_file) => {
                info!(
                    "Created file '{}' via CRDT in subdir {}: uuid={}, schema_cid={}, file_cid={}",
                    owning_doc.relative_path,
                    owning_doc.document_id,
                    new_file.uuid,
                    new_file.schema_cid,
                    new_file.file_cid
                );

                // Save the subdirectory state and convert schema (requires read lock)
                {
                    let subdir_state = subdir_state_arc.read().await;

                    // Save the subdirectory state (.commonplace-sync.json)
                    if let Err(e) = subdir_state.save(&owning_doc.directory).await {
                        warn!(
                            "Failed to save subdirectory state for {}: {}",
                            owning_doc.directory.display(),
                            e
                        );
                    }

                    // Also write the local schema file (.commonplace.json) so it reflects the new file
                    // Convert the Y.Doc schema to FsSchema and write to disk
                    info!(
                        "Converting Y.Doc schema for subdirectory {} (dir: {}), yjs_state len: {}",
                        owning_doc.document_id,
                        owning_doc.directory.display(),
                        subdir_state
                            .schema
                            .yjs_state
                            .as_ref()
                            .map(|s| s.len())
                            .unwrap_or(0)
                    );
                    match subdir_state.schema.to_doc() {
                        Ok(doc) => {
                            let fs_schema = ymap_schema::to_fs_schema(&doc);
                            let entry_count = fs_schema
                                .root
                                .as_ref()
                                .and_then(|e| match e {
                                    commonplace_doc::fs::Entry::Dir(d) => {
                                        d.entries.as_ref().map(|e| e.len())
                                    }
                                    _ => None,
                                })
                                .unwrap_or(0);
                            info!(
                                "Converted Y.Doc to FsSchema for {}: {} entries",
                                owning_doc.directory.display(),
                                entry_count
                            );
                            match schema_to_json(&fs_schema) {
                                Ok(schema_json) => {
                                    info!(
                                        "Serialized schema JSON ({} bytes) for {}",
                                        schema_json.len(),
                                        owning_doc.directory.display()
                                    );
                                    match write_schema_file(
                                        &owning_doc.directory,
                                        &schema_json,
                                        None,
                                    )
                                    .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "Successfully wrote local schema for {}",
                                                owning_doc.directory.display()
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to write local schema for {}: {}",
                                                owning_doc.directory.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to serialize schema for {}: {}",
                                        owning_doc.directory.display(),
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to convert schema state to Y.Doc for {}: {}",
                                owning_doc.directory.display(),
                                e
                            );
                        }
                    }
                }

                // Spawn CRDT sync tasks for the new file using the cached state Arc
                let file_path = path.to_path_buf();
                // Check for existing tasks before spawning (prevents duplicates)
                let states_snapshot = file_states.read().await;
                let handles = spawn_file_sync_tasks_crdt(
                    crdt.mqtt_client.clone(),
                    client.clone(),
                    server.to_string(),
                    crdt.workspace.clone(),
                    new_file.uuid,
                    file_path,
                    subdir_state_arc,
                    owning_doc.relative_path.clone(),
                    false, // pull_only = false for new files
                    author.to_string(),
                    crdt.mqtt_only_config,
                    crdt.inode_tracker.clone(),
                    Some(&*states_snapshot),
                    Some(&relative_path),
                );
                drop(states_snapshot);

                // Add to file_states
                let mut states = file_states.write().await;
                let state = Arc::new(RwLock::new(SyncState::new()));
                let content_hash = compute_content_hash(content.as_bytes());

                states.insert(
                    relative_path.clone(),
                    FileSyncState {
                        relative_path,
                        identifier: new_file.uuid.to_string(),
                        state,
                        task_handles: handles,
                        use_paths: false,
                        content_hash: Some(content_hash),
                        crdt_tasks_spawned: true,
                    },
                );
            }
            Err(e) => {
                warn!(
                    "Failed to create file '{}' via CRDT in subdir: {}",
                    owning_doc.relative_path, e
                );
            }
        }
    } else {
        // File is in the root directory - use the existing crdt_state
        // Check if already tracked in root CRDT state
        {
            let state = crdt.crdt_state.read().await;
            if state.files.contains_key(&filename) {
                debug!(
                    "File '{}' already in root CRDT state, skipping create_new_file",
                    filename
                );
                return;
            }
        }

        // Create file via CRDT using root state
        let result = {
            let mut state = crdt.crdt_state.write().await;
            state.ensure_schema_initialized(directory).await;
            create_new_file(
                &crdt.mqtt_client,
                &crdt.workspace,
                &mut state,
                &filename,
                &content,
                author,
            )
            .await
        };

        match result {
            Ok(new_file) => {
                info!(
                    "Created file '{}' via CRDT: uuid={}, schema_cid={}, file_cid={}",
                    filename, new_file.uuid, new_file.schema_cid, new_file.file_cid
                );

                // Spawn CRDT sync tasks for the new file
                let file_path = path.to_path_buf();
                // Check for existing tasks before spawning (prevents duplicates)
                let states_snapshot = file_states.read().await;
                let handles = spawn_file_sync_tasks_crdt(
                    crdt.mqtt_client.clone(),
                    client.clone(),
                    server.to_string(),
                    crdt.workspace.clone(),
                    new_file.uuid,
                    file_path,
                    crdt.crdt_state.clone(),
                    filename.clone(),
                    false, // pull_only = false for new files
                    author.to_string(),
                    crdt.mqtt_only_config,
                    crdt.inode_tracker.clone(),
                    Some(&*states_snapshot),
                    Some(&filename),
                );
                drop(states_snapshot);

                // Add to file_states
                let mut states = file_states.write().await;
                let state = Arc::new(RwLock::new(SyncState::new()));
                let content_hash = compute_content_hash(content.as_bytes());

                states.insert(
                    relative_path.clone(),
                    FileSyncState {
                        relative_path,
                        identifier: new_file.uuid.to_string(),
                        state,
                        task_handles: handles,
                        use_paths: false,
                        content_hash: Some(content_hash),
                        crdt_tasks_spawned: true,
                    },
                );
            }
            Err(e) => {
                warn!("Failed to create file '{}' via CRDT: {}", filename, e);
            }
        }
    }
}

/// Handle the "creation" side of a detected file rename.
///
/// When inode tracking detects that a newly-created file has the same inode as
/// a previously-tracked file at a different path, this is a rename. This function:
/// 1. Updates the schema to reflect the new filename (preserving node_id)
/// 2. Updates InodeTracker primary_path
/// 3. Migrates file_states from old path to new path
/// 4. Writes updated .commonplace.json
/// 5. Spawns sync tasks for the file under its new name
///
/// Only handles same-directory renames. Cross-directory renames are deferred.
#[allow(clippy::too_many_arguments)]
async fn handle_file_renamed(
    new_path: &std::path::Path,
    old_path: &std::path::Path,
    node_id: &str,
    directory: &std::path::Path,
    fs_root_id: &str,
    _options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    author: &str,
    crdt: &CrdtEventParams,
    pull_only: bool,
    client: &Client,
    server: &str,
    _written_schemas: Option<&WrittenSchemas>,
) -> bool {
    let new_filename = match new_path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_string(),
        None => {
            warn!(
                "Could not get filename from new path: {}",
                new_path.display()
            );
            return false;
        }
    };
    let old_filename = match old_path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_string(),
        None => {
            warn!(
                "Could not get filename from old path: {}",
                old_path.display()
            );
            return false;
        }
    };

    let canonical_path = new_path
        .canonicalize()
        .unwrap_or_else(|_| new_path.to_path_buf());
    let canonical_directory = directory
        .canonicalize()
        .unwrap_or_else(|_| directory.to_path_buf());

    let relative_path = match canonical_path.strip_prefix(&canonical_directory) {
        Ok(rel) => rel.to_string_lossy().to_string().replace('\\', "/"),
        Err(_) => new_filename.clone(),
    };

    let canonical_old_path = old_path
        .canonicalize()
        .unwrap_or_else(|_| old_path.to_path_buf());
    let old_relative_path = match canonical_old_path.strip_prefix(&canonical_directory) {
        Ok(rel) => rel.to_string_lossy().to_string().replace('\\', "/"),
        Err(_) => old_filename.clone(),
    };

    // Find which document owns this file
    let owning_doc = find_owning_document(&canonical_directory, fs_root_id, &relative_path);
    let is_subdirectory = owning_doc.document_id != fs_root_id;

    // Compute old relative path within the owning document's directory.
    // Schema keys use paths relative to the owning document, not basenames.
    let old_relative_in_owner = if is_subdirectory {
        match old_path.strip_prefix(&owning_doc.directory) {
            Ok(rel) => rel.to_string_lossy().to_string().replace('\\', "/"),
            Err(_) => old_filename.clone(),
        }
    } else {
        old_relative_path.clone()
    };

    // Try atomic rename in schema first (works if old entry still exists).
    // If that fails (old entry already removed by Deleted handler), fall back to
    // adding a new entry with the preserved node_id.
    let schema_updated = if is_subdirectory {
        let subdir_node_id = match uuid::Uuid::parse_str(&owning_doc.document_id) {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    "Invalid subdirectory node_id '{}': {}",
                    owning_doc.document_id, e
                );
                return false;
            }
        };

        let subdir_state_arc = match crdt
            .subdir_cache
            .get_or_load(&owning_doc.directory, subdir_node_id)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "Failed to load state for subdirectory {}: {}",
                    owning_doc.directory.display(),
                    e
                );
                return false;
            }
        };

        let result = {
            let mut subdir_state = subdir_state_arc.write().await;
            // Try rename first
            let rename_result = rename_file_in_schema(
                &crdt.mqtt_client,
                &crdt.workspace,
                &mut subdir_state,
                &old_relative_in_owner,
                &owning_doc.relative_path,
                author,
            )
            .await;

            match rename_result {
                Ok(_nid) => true,
                Err(SyncError::NotFound(_)) => {
                    // Old entry already removed, add new entry with preserved node_id
                    // and publish via MQTT so remote peers see the update
                    debug!(
                        "Old entry '{}' already removed, adding '{}' with preserved node_id",
                        old_relative_in_owner, owning_doc.relative_path
                    );
                    subdir_state
                        .ensure_schema_initialized(&owning_doc.directory)
                        .await;
                    match add_file_to_schema(
                        &crdt.mqtt_client,
                        &crdt.workspace,
                        &mut subdir_state,
                        &owning_doc.relative_path,
                        node_id,
                        author,
                    )
                    .await
                    {
                        Ok(_cid) => true,
                        Err(e) => {
                            warn!("Failed to add file to schema in rename fallback: {}", e);
                            false
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to rename '{}' -> '{}' in schema: {}",
                        old_relative_in_owner, owning_doc.relative_path, e
                    );
                    false
                }
            }
        };

        // Save state and write .commonplace.json
        if result {
            let subdir_state = subdir_state_arc.read().await;
            if let Err(e) = subdir_state.save(&owning_doc.directory).await {
                warn!("Failed to save subdirectory state after rename: {}", e);
            }
            if let Ok(doc) = subdir_state.schema.to_doc() {
                let fs_schema = ymap_schema::to_fs_schema(&doc);
                if let Ok(schema_json) = schema_to_json(&fs_schema) {
                    if let Err(e) =
                        write_schema_file(&owning_doc.directory, &schema_json, None).await
                    {
                        warn!("Failed to write schema after rename: {}", e);
                    }
                }
            }
        }

        result
    } else {
        // Root directory
        let result = {
            let mut state = crdt.crdt_state.write().await;
            let rename_result = rename_file_in_schema(
                &crdt.mqtt_client,
                &crdt.workspace,
                &mut state,
                &old_relative_in_owner,
                &relative_path,
                author,
            )
            .await;

            match rename_result {
                Ok(_nid) => true,
                Err(SyncError::NotFound(_)) => {
                    // Old entry already removed, add new entry with preserved node_id
                    // and publish via MQTT so remote peers see the update
                    debug!(
                        "Old entry '{}' already removed, adding '{}' with preserved node_id",
                        old_relative_in_owner, relative_path
                    );
                    state.ensure_schema_initialized(directory).await;
                    match add_file_to_schema(
                        &crdt.mqtt_client,
                        &crdt.workspace,
                        &mut state,
                        &relative_path,
                        node_id,
                        author,
                    )
                    .await
                    {
                        Ok(_cid) => true,
                        Err(e) => {
                            warn!("Failed to add file to schema in rename fallback: {}", e);
                            false
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to rename '{}' -> '{}' in schema: {}",
                        old_relative_in_owner, relative_path, e
                    );
                    false
                }
            }
        };

        // Write .commonplace.json for root directory
        if result {
            let state = crdt.crdt_state.read().await;
            if let Ok(doc) = state.schema.to_doc() {
                let fs_schema = ymap_schema::to_fs_schema(&doc);
                if let Ok(schema_json) = schema_to_json(&fs_schema) {
                    if let Err(e) = write_schema_file(directory, &schema_json, None).await {
                        warn!("Failed to write schema after rename: {}", e);
                    }
                }
            }
        }

        result
    };

    if !schema_updated {
        warn!(
            "Failed to update schema for rename: {} -> {}",
            old_filename, new_filename
        );
        return false;
    }

    // Update InodeTracker primary_path
    #[cfg(unix)]
    if let Some(ref tracker) = crdt.inode_tracker {
        if let Ok(inode_key) = InodeKey::from_path(&canonical_path) {
            let mut t = tracker.write().await;
            t.update_primary_path(&inode_key, canonical_path.clone());
        }
    }

    // Migrate file_states: remove old entry, keep task handles for abort
    let old_entry = {
        let mut states = file_states.write().await;
        states.remove(&old_relative_path)
    };

    // Abort old sync tasks (they reference the old filename)
    if let Some(old_state) = &old_entry {
        for handle in &old_state.task_handles {
            handle.abort();
        }
    }

    // Read file content for the new entry
    let content = match tokio::fs::read_to_string(new_path).await {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to read renamed file {}: {}", new_path.display(), e);
            return false;
        }
    };

    // Parse the preserved node_id as UUID for spawning tasks
    let file_uuid = match uuid::Uuid::parse_str(node_id) {
        Ok(u) => u,
        Err(e) => {
            warn!("Invalid node_id UUID '{}': {}", node_id, e);
            return false;
        }
    };

    // Spawn new sync tasks for the renamed file
    let file_path = new_path.to_path_buf();
    let dir_state_arc = if is_subdirectory {
        let subdir_node_id = match uuid::Uuid::parse_str(&owning_doc.document_id) {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid subdir node_id for spawning tasks: {}", e);
                return false;
            }
        };
        match crdt
            .subdir_cache
            .get_or_load(&owning_doc.directory, subdir_node_id)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to load subdir state for spawning tasks: {}", e);
                return false;
            }
        }
    } else {
        crdt.crdt_state.clone()
    };

    let file_key = if is_subdirectory {
        owning_doc.relative_path.clone()
    } else {
        relative_path.clone()
    };

    let states_snapshot = file_states.read().await;
    let handles = spawn_file_sync_tasks_crdt(
        crdt.mqtt_client.clone(),
        client.clone(),
        server.to_string(),
        crdt.workspace.clone(),
        file_uuid,
        file_path,
        dir_state_arc,
        file_key.clone(),
        pull_only,
        author.to_string(),
        crdt.mqtt_only_config,
        crdt.inode_tracker.clone(),
        Some(&*states_snapshot),
        Some(&relative_path),
    );
    drop(states_snapshot);

    // Add to file_states under new path
    let mut states = file_states.write().await;
    let state = Arc::new(RwLock::new(SyncState::new()));
    let content_hash = compute_content_hash(content.as_bytes());

    states.insert(
        relative_path.clone(),
        FileSyncState {
            relative_path,
            identifier: node_id.to_string(),
            state,
            task_handles: handles,
            use_paths: false,
            content_hash: Some(content_hash),
            crdt_tasks_spawned: true,
        },
    );

    info!(
        "Rename complete: {} -> {} (node_id: {})",
        old_filename, new_filename, node_id
    );
    true
}

/// Handle file deletion via CRDT/MQTT path.
///
/// This function properly handles files in node-backed subdirectories by:
/// 1. Finding the owning document (could be root or a subdirectory)
/// 2. Loading the appropriate DirectorySyncState for that directory
/// 3. Removing the file from the correct schema
async fn handle_file_deleted_crdt(
    path: &std::path::Path,
    directory: &std::path::Path,
    fs_root_id: &str,
    options: &ScanOptions,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    author: &str,
    crdt: &CrdtEventParams,
) {
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
                "[{} pid={}] handle_file_deleted_crdt called: path={}",
                timestamp,
                pid,
                path.display()
            );
        }
    }

    // Get filename from path
    let filename = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_string(),
        None => {
            warn!("Could not get filename from path: {}", path.display());
            return;
        }
    };

    // Check ignore patterns (same as creation)
    let should_ignore = options.ignore_patterns.iter().any(|pattern| {
        if pattern == &filename {
            true
        } else if pattern.contains('*') {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                filename.starts_with(parts[0]) && filename.ends_with(parts[1])
            } else {
                false
            }
        } else {
            false
        }
    });
    if should_ignore {
        debug!(
            "Ignoring deleted file (matches ignore pattern): {}",
            filename
        );
        return;
    }

    // Skip hidden files unless configured
    if !options.include_hidden && filename.starts_with('.') {
        debug!("Ignoring hidden file deletion: {}", filename);
        return;
    }

    // Calculate relative path from root directory
    // Canonicalize both paths to ensure prefix stripping works correctly
    // when path is absolute and directory is relative (from --directory arg)
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let canonical_directory = directory
        .canonicalize()
        .unwrap_or_else(|_| directory.to_path_buf());

    let relative_path = match canonical_path.strip_prefix(&canonical_directory) {
        Ok(rel) => rel.to_string_lossy().to_string().replace('\\', "/"),
        Err(_) => {
            warn!(
                "Could not strip directory prefix (delete): path={}, directory={}",
                canonical_path.display(),
                canonical_directory.display()
            );
            filename.clone()
        }
    };

    // Stop sync tasks and clear runtime tracking so recreate events can
    // re-enter the normal create/publish path for this same relative path.
    if remove_file_state_and_abort(file_states, &relative_path)
        .await
        .is_some()
    {
        info!("Stopping sync tasks for deleted file: {}", relative_path);
    }

    // Find which document owns this file (may be a node-backed subdirectory)
    let owning_doc = find_owning_document(&canonical_directory, fs_root_id, &relative_path);
    info!(
        "CRDT file deleted: {} owned by document {} (relative: {})",
        relative_path, owning_doc.document_id, owning_doc.relative_path
    );

    // Determine which DirectorySyncState to use
    let is_subdirectory = owning_doc.document_id != fs_root_id;

    if is_subdirectory {
        // Load state for the subdirectory from cache
        let subdir_node_id = match uuid::Uuid::parse_str(&owning_doc.document_id) {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    "Invalid subdirectory node_id '{}': {}",
                    owning_doc.document_id, e
                );
                return;
            }
        };

        let subdir_state_arc = match crdt
            .subdir_cache
            .get_or_load(&owning_doc.directory, subdir_node_id)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "Failed to load state for subdirectory {}: {}",
                    owning_doc.directory.display(),
                    e
                );
                return;
            }
        };

        // Trace log the loaded state
        {
            let subdir_state = subdir_state_arc.read().await;
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
                let yjs_state_len = subdir_state
                    .schema
                    .yjs_state
                    .as_ref()
                    .map(|s| s.len())
                    .unwrap_or(0);
                let files_count = subdir_state.files.len();
                let _ = writeln!(
                    file,
                    "[{} pid={}] Loaded subdir_state for delete: dir={}, yjs_state_len={}, files_count={}",
                    timestamp, pid, owning_doc.directory.display(), yjs_state_len, files_count
                );
            }
        }

        // Stash node_id in InodeTracker before removing from schema (for rename detection)
        #[cfg(unix)]
        if let Some(ref tracker) = crdt.inode_tracker {
            let subdir_state = subdir_state_arc.read().await;
            if let Ok(doc) = subdir_state.schema.to_doc() {
                if let Some(entry) = ymap_schema::get_entry(&doc, &owning_doc.relative_path) {
                    if let Some(ref node_id) = entry.node_id {
                        let mut t = tracker.write().await;
                        t.stash_node_id_by_path(&canonical_path, node_id.clone());
                    }
                }
            }
        }

        // Remove file from subdirectory schema via CRDT
        let result = {
            let mut subdir_state = subdir_state_arc.write().await;
            remove_file_from_schema(
                &crdt.mqtt_client,
                &crdt.workspace,
                &mut subdir_state,
                &owning_doc.relative_path,
                author,
            )
            .await
        };

        // Trace log the result
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
                let result_str = match &result {
                    Ok(cid) => format!("OK: cid={}", cid),
                    Err(e) => format!("ERR: {}", e),
                };
                let _ = writeln!(
                    file,
                    "[{} pid={}] remove_file_from_schema result: file={}, doc={}, result={}",
                    timestamp, pid, owning_doc.relative_path, owning_doc.document_id, result_str
                );
            }
        }

        match result {
            Ok(cid) => {
                info!(
                    "Removed file '{}' from subdirectory {} schema via CRDT: cid={}",
                    owning_doc.relative_path, owning_doc.document_id, cid
                );

                // Save the subdirectory state
                {
                    let subdir_state = subdir_state_arc.read().await;
                    if let Err(e) = subdir_state.save(&owning_doc.directory).await {
                        warn!(
                            "Failed to save subdirectory state for {}: {}",
                            owning_doc.directory.display(),
                            e
                        );
                    }
                }
            }
            Err(e) => {
                // File might not be in schema (e.g., temp file, already deleted)
                debug!(
                    "Could not remove file '{}' from subdirectory schema: {}",
                    owning_doc.relative_path, e
                );
            }
        }
    } else {
        // File is in the root directory - use the existing crdt_state
        // Stash node_id in InodeTracker before removing from schema (for rename detection)
        #[cfg(unix)]
        if let Some(ref tracker) = crdt.inode_tracker {
            let state = crdt.crdt_state.read().await;
            if let Ok(doc) = state.schema.to_doc() {
                if let Some(entry) = ymap_schema::get_entry(&doc, &filename) {
                    if let Some(ref node_id) = entry.node_id {
                        let mut t = tracker.write().await;
                        t.stash_node_id_by_path(&canonical_path, node_id.clone());
                    }
                }
            }
        }

        let result = {
            let mut state = crdt.crdt_state.write().await;
            remove_file_from_schema(
                &crdt.mqtt_client,
                &crdt.workspace,
                &mut state,
                &filename,
                author,
            )
            .await
        };

        match result {
            Ok(cid) => {
                info!(
                    "Removed file '{}' from schema via CRDT: cid={}",
                    filename, cid
                );
            }
            Err(e) => {
                // File might not be in schema (e.g., temp file, already deleted)
                debug!("Could not remove file '{}' from schema: {}", filename, e);
            }
        }
    }
}

/// Commonplace Sync - Keep a local file or directory in sync with a server document
#[derive(Parser, Debug)]
#[command(name = "commonplace-sync")]
#[command(about = "Sync a local file or directory with a Commonplace document node")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Server URL (also reads from COMMONPLACE_SERVER env var)
    #[arg(
        short,
        long,
        default_value = DEFAULT_SERVER_URL,
        env = "COMMONPLACE_SERVER"
    )]
    server: String,

    /// Node ID (UUID) to sync with (reads from COMMONPLACE_NODE env var; optional if --path or --fork-from is provided)
    #[arg(short, long, env = "COMMONPLACE_NODE")]
    node: Option<String>,

    /// Path relative to fs-root to sync with (reads from COMMONPLACE_PATH env var; resolved to UUID)
    /// Example: "bartleby" or "workspace/bartleby"
    #[arg(short, long, env = "COMMONPLACE_PATH", conflicts_with = "node")]
    path: Option<String>,

    /// Local file path to sync (mutually exclusive with --directory)
    #[arg(short, long, conflicts_with = "directory")]
    file: Option<PathBuf>,

    /// Local directory path to sync (mutually exclusive with --file)
    #[arg(short, long, conflicts_with = "file")]
    directory: Option<PathBuf>,

    /// Fork from this node before syncing (also reads from COMMONPLACE_FORK_FROM env var; creates a new node)
    #[arg(long, env = "COMMONPLACE_FORK_FROM")]
    fork_from: Option<String>,

    /// When forking, use this commit instead of HEAD
    #[arg(long, requires = "fork_from")]
    at_commit: Option<String>,

    /// Include hidden files when syncing directories
    #[arg(long, default_value = "false")]
    include_hidden: bool,

    /// Glob patterns to ignore (can be specified multiple times)
    #[arg(long)]
    ignore: Vec<String>,

    /// Initial sync strategy when both sides have content
    #[arg(long, default_value = "skip", value_parser = ["local", "server", "skip"], env = "COMMONPLACE_INITIAL_SYNC")]
    initial_sync: String,

    /// Use path-based API endpoints (/files/*path) instead of ID-based endpoints
    /// This requires the server to have --fs-root configured
    #[arg(long, default_value = "false")]
    use_paths: bool,

    /// Run a command in the synced directory context.
    /// Sync will continue running while the command executes.
    /// When the command exits, sync shuts down and propagates the exit code.
    /// Use `--` to separate sync args from command args.
    #[arg(long, value_name = "COMMAND")]
    exec: Option<String>,

    /// Additional arguments to pass to the exec command (after --)
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    exec_args: Vec<String>,

    /// Run in sandbox mode: creates a temporary directory, syncs content there,
    /// runs the command in isolation, then cleans up on exit.
    /// Requires either --exec or --log-listener. Conflicts with --directory.
    #[arg(long, conflicts_with = "directory")]
    sandbox: bool,

    /// Process name for log file naming in sandbox mode.
    /// Log files will be named __<name>.stdout.txt and __<name>.stderr.txt
    /// If not specified, defaults to extracting from the exec command.
    #[arg(long)]
    name: Option<String>,

    /// Push-only mode: watch local files and push changes to server.
    /// Ignores server-side updates (no SSE subscription).
    /// Use case: source-of-truth files like .beads/issues.jsonl
    #[arg(long, conflicts_with = "pull_only")]
    push_only: bool,

    /// Pull-only mode: subscribe to server updates and write to local files.
    /// Ignores local file changes (no file watcher).
    /// Use case: read-only mirrors, generated content
    #[arg(long, conflicts_with = "push_only")]
    pull_only: bool,

    /// Force-push mode: local file content replaces server entirely.
    /// On each local change, fetches current HEAD and replaces content.
    /// No CRDT merge - local always wins unconditionally.
    /// Use case: source-of-truth files, recovery scenarios
    #[arg(long)]
    force_push: bool,

    /// Shadow directory for inode tracking hardlinks.
    /// When syncing with atomic writes, old inodes are hardlinked here so
    /// slow writers to old inodes can be detected and merged.
    /// Must be on the same filesystem as the synced directory.
    /// Set to empty string to disable inode tracking.
    #[arg(
        long,
        default_value = DEFAULT_SHADOW_DIR,
        env = "COMMONPLACE_SHADOW_DIR"
    )]
    shadow_dir: String,

    /// MQTT broker URL for pub/sub (required, also reads from COMMONPLACE_MQTT env var)
    #[arg(long, env = "COMMONPLACE_MQTT")]
    mqtt_broker: String,

    /// MQTT workspace name for topic namespacing (also reads from COMMONPLACE_WORKSPACE env var)
    #[arg(long, default_value = DEFAULT_WORKSPACE, env = "COMMONPLACE_WORKSPACE")]
    workspace: String,

    /// Path to listen for stdout/stderr events from another process.
    /// When set, this sync process subscribes to events at the given path
    /// and writes them to a log file. Requires --sandbox mode.
    /// Events appear on {workspace}/events/{path}/stdout and /stderr topics.
    #[arg(long, requires = "sandbox")]
    log_listener: Option<String>,

    /// Timeout in seconds to wait for CRDT readiness before starting exec.
    /// In sandbox mode, exec waits for CRDT tasks to be ready for all files,
    /// or until this timeout expires, whichever comes first.
    /// Default: 10 seconds.
    #[arg(long, default_value = "10")]
    sandbox_timeout: u64,

    /// Path to a local redb commit store for persisting commits locally.
    /// When enabled, the sync client stores all received and created commits
    /// to this database, enabling commit rebroadcast and restart resilience.
    /// Example: ~/.commonplace/sync-commits.redb
    #[arg(long, env = "COMMONPLACE_COMMIT_STORE")]
    commit_store: Option<PathBuf>,

    /// Enable MQTT-only sync mode (HTTP disabled).
    /// When enabled, HTTP calls for sync operations are rejected and skipped.
    /// State initialization and resync must use MQTT/cyan sync.
    /// Default: false (HTTP fallback enabled)
    #[arg(long, default_value = "false", env = "COMMONPLACE_MQTT_ONLY_SYNC")]
    mqtt_only_sync: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    // Validate that either --file, --directory, or --sandbox is provided
    if args.file.is_none() && args.directory.is_none() && !args.sandbox {
        error!("Either --file, --directory, or --sandbox must be provided");
        return ExitCode::from(1);
    }

    // Exec mode requires --directory or --sandbox (doesn't make sense with single file)
    if args.exec.is_some() && args.directory.is_none() && !args.sandbox {
        error!(
            "--exec requires --directory or --sandbox (exec mode doesn't support single file sync)"
        );
        return ExitCode::from(1);
    }

    // Sandbox mode requires either --exec or --log-listener
    if args.sandbox && args.exec.is_none() && args.log_listener.is_none() {
        error!("--sandbox requires either --exec or --log-listener");
        return ExitCode::from(1);
    }

    // Hard-disable HTTP transport for sync runtime.
    // Any remaining HTTP call paths should fail fast so they can be replaced by MQTT/cyan.
    set_sync_http_disabled(true);
    info!("HTTP sync transport disabled (strict MQTT-only runtime)");

    // Create HTTP client
    let client = Client::new();

    // Initialize MQTT client (required)
    info!("Initializing MQTT client for broker: {}", args.mqtt_broker);
    let mqtt_config = MqttConfig {
        broker_url: args.mqtt_broker.clone(),
        client_id: format!("sync-{}", uuid::Uuid::new_v4()),
        workspace: args.workspace.clone(),
        ..Default::default()
    };
    let mqtt_client = match MqttClient::connect(mqtt_config).await {
        Ok(mqtt) => {
            info!("Connected to MQTT broker, workspace: {}", args.workspace);
            Arc::new(mqtt)
        }
        Err(e) => {
            error!("Failed to connect to MQTT broker: {}", e);
            return ExitCode::from(1);
        }
    };

    // Initialize local commit store if path provided
    // TODO: Wire commit_store through to sync tasks (receive_task_crdt, etc.)
    // For now, the store is initialized but not yet passed to CRDT processing.
    let _commit_store: Option<Arc<CommitStore>> = if let Some(ref path) = args.commit_store {
        info!("Initializing local commit store at: {}", path.display());
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    error!("Failed to create commit store directory: {}", e);
                    return ExitCode::from(1);
                }
            }
        }
        match CommitStore::new(path) {
            Ok(store) => {
                info!("Local commit store initialized");
                Some(Arc::new(store))
            }
            Err(e) => {
                error!("Failed to create commit store: {}", e);
                return ExitCode::from(1);
            }
        }
    } else {
        None
    };

    // Initialize MQTT request/response client for startup operations (path resolution, fork).
    let mqtt_request =
        match MqttRequestClient::new(mqtt_client.clone(), args.workspace.clone()).await {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to create MQTT request client: {}", e);
                return ExitCode::from(1);
            }
        };

    // Determine the node ID to sync with
    // Priority: --node > --path > --fork-from > --use-paths discovery
    let node_id = if let Some(ref node) = args.node {
        // UUID values are used directly.
        if uuid::Uuid::parse_str(node).is_ok() {
            node.clone()
        } else {
            // Non-UUID --node is treated as an fs-root-relative path.
            info!(
                "Resolving --node '{}' as fs-root-relative path via MQTT...",
                node
            );
            let fs_root_id = match mqtt_request.discover_fs_root().await {
                Ok(id) => id,
                Err(e) => {
                    error!(
                        "Failed to discover fs-root for --node path resolution: {}",
                        e
                    );
                    error!("Ensure server was started with --fs-root, or pass --node <uuid>");
                    return ExitCode::from(1);
                }
            };
            match mqtt_request.resolve_path_to_uuid(&fs_root_id, node).await {
                Ok(id) => {
                    info!("Resolved --node path '{}' -> {}", node, id);
                    id
                }
                Err(e) => {
                    error!("Failed to resolve --node path '{}' via MQTT: {}", node, e);
                    return ExitCode::from(1);
                }
            }
        }
    } else if let Some(ref path) = args.path {
        info!("Resolving --path '{}' via MQTT...", path);
        let fs_root_id = match mqtt_request.discover_fs_root().await {
            Ok(id) => id,
            Err(e) => {
                error!("Failed to discover fs-root for --path resolution: {}", e);
                error!("Either specify --node <uuid>, or ensure server was started with --fs-root");
                return ExitCode::from(1);
            }
        };
        match mqtt_request.resolve_path_to_uuid(&fs_root_id, path).await {
            Ok(id) => {
                info!("Resolved --path '{}' -> {}", path, id);
                id
            }
            Err(e) => {
                error!("Failed to resolve --path '{}' via MQTT: {}", path, e);
                return ExitCode::from(1);
            }
        }
    } else if let Some(ref source) = args.fork_from {
        // Fork from another node via MQTT
        info!("Forking from node {} via MQTT...", source);
        match mqtt_request
            .fork_document(source, args.at_commit.as_deref())
            .await
        {
            Ok(resp) => match resp.id {
                Some(id) => {
                    info!(
                        "Forked node {} -> {} (at commit {})",
                        source,
                        id,
                        resp.head.as_deref().unwrap_or("HEAD")
                    );
                    id
                }
                None => {
                    error!(
                        "Fork failed: {}",
                        resp.error.unwrap_or_else(|| "unknown error".to_string())
                    );
                    return ExitCode::from(1);
                }
            },
            Err(e) => {
                error!("Fork failed: {}", e);
                return ExitCode::from(1);
            }
        }
    } else if args.use_paths {
        // No node specified - discover fs-root via MQTT retained message
        info!("Discovering fs-root via MQTT...");
        match mqtt_request.discover_fs_root().await {
            Ok(id) => {
                info!("Discovered fs-root: {}", id);
                id
            }
            Err(e) => {
                error!("Failed to discover fs-root: {}", e);
                error!(
                    "Either specify --node, --path, or ensure server was started with --fs-root"
                );
                return ExitCode::from(1);
            }
        }
    } else {
        error!("Either --node, --path, or --fork-from must be provided");
        return ExitCode::from(1);
    };

    // Route to appropriate mode
    let result = if args.sandbox {
        // Sandbox mode: create temp directory, sync there, run command, clean up
        // Clean up stale sandbox directories from previous runs (killed by SIGKILL)
        cleanup_stale_sandboxes();

        // Create sandbox with our prefix for easy identification during cleanup
        let sandbox_dir =
            std::env::temp_dir().join(format!("commonplace-sandbox-{}", uuid::Uuid::new_v4()));

        if let Err(e) = std::fs::create_dir_all(&sandbox_dir) {
            error!("Failed to create sandbox directory: {}", e);
            return ExitCode::from(1);
        }
        info!("Creating sandbox directory: {}", sandbox_dir.display());

        // Write PID file to mark this sandbox as active
        let pid_file = sandbox_dir.join(".pid");
        if let Err(e) = std::fs::write(&pid_file, std::process::id().to_string()) {
            warn!("Failed to write PID file: {}", e);
        }

        // Always ignore the schema file and PID file when scanning
        let mut ignore_patterns = args.ignore;
        ignore_patterns.push(SCHEMA_FILENAME.to_string());
        ignore_patterns.push(".pid".to_string());
        ignore_patterns.push(".commonplace-synced-dirs.json".to_string());

        let scan_options = ScanOptions {
            include_hidden: args.include_hidden,
            ignore_patterns,
        };

        // Sandbox mode defaults to pulling server content (since local sandbox is empty)
        // User can still override with explicit --initial-sync if needed
        let initial_sync = if args.initial_sync == "skip" {
            "server".to_string()
        } else {
            args.initial_sync
        };

        let exec_result = if let Some(ref listen_path) = args.log_listener {
            // Log-listener mode: subscribe to events at another path and write to log file
            run_log_listener_mode(
                client,
                args.server,
                node_id,
                sandbox_dir.clone(),
                scan_options,
                initial_sync,
                args.use_paths,
                args.push_only,
                args.pull_only,
                args.shadow_dir,
                args.name,
                mqtt_client,
                args.workspace,
                listen_path.clone(),
            )
            .await
        } else {
            // Normal exec mode
            let exec_cmd = args
                .exec
                .expect("--sandbox requires --exec or --log-listener");
            run_exec_mode(
                client,
                args.server.clone(),
                node_id,
                sandbox_dir.clone(),
                scan_options,
                initial_sync,
                args.use_paths,
                exec_cmd,
                args.exec_args,
                true, // sandbox mode
                args.push_only,
                args.pull_only,
                args.shadow_dir,
                args.name,
                mqtt_client,
                args.workspace,
                args.path.clone(),
                args.sandbox_timeout,
                args.mqtt_only_sync,
            )
            .await
        };

        // Clean up sandbox directory only if the command completed successfully (exit code 0)
        // If we received a signal (SIGTERM/SIGINT), preserve the sandbox for debugging
        match &exec_result {
            Ok(0) => {
                info!("Cleaning up sandbox directory: {}", sandbox_dir.display());
                if let Err(e) = std::fs::remove_dir_all(&sandbox_dir) {
                    warn!("Failed to clean up sandbox directory: {}", e);
                }
            }
            Ok(code) => {
                info!(
                    "Preserving sandbox directory (exit code {}): {}",
                    code,
                    sandbox_dir.display()
                );
            }
            Err(_) => {
                info!(
                    "Preserving sandbox directory (error exit): {}",
                    sandbox_dir.display()
                );
            }
        }

        exec_result
    } else if let Some(directory) = args.directory {
        // Acquire sync lock for this directory (non-sandbox mode)
        let _sync_lock = match acquire_sync_lock(&directory) {
            Ok(lock) => lock,
            Err(e) => {
                error!("Failed to acquire sync lock: {}", e);
                return ExitCode::from(1);
            }
        };

        // Always ignore the schema file (.commonplace.json) when scanning
        let mut ignore_patterns = args.ignore;
        ignore_patterns.push(SCHEMA_FILENAME.to_string());
        ignore_patterns.push(".commonplace-sync.lock".to_string()); // Ignore lock file
        ignore_patterns.push(".commonplace-synced-dirs.json".to_string()); // Ignore synced dirs state

        let scan_options = ScanOptions {
            include_hidden: args.include_hidden,
            ignore_patterns,
        };

        if let Some(exec_cmd) = args.exec {
            // Exec mode: sync directory, run command, exit when command exits
            run_exec_mode(
                client,
                args.server,
                node_id,
                directory,
                scan_options,
                args.initial_sync,
                args.use_paths,
                exec_cmd,
                args.exec_args,
                false, // not sandbox mode
                args.push_only,
                args.pull_only,
                args.shadow_dir,
                args.name,
                mqtt_client,
                args.workspace,
                args.path.clone(),
                args.sandbox_timeout,
                args.mqtt_only_sync,
            )
            .await
        } else {
            // Normal directory sync mode
            run_directory_mode(
                client,
                args.server,
                node_id,
                directory,
                scan_options,
                args.initial_sync,
                args.use_paths,
                args.push_only,
                args.pull_only,
                args.shadow_dir,
                mqtt_client,
                args.workspace,
                args.name.clone(),
                args.mqtt_only_sync,
            )
            .await
            .map(|_| 0u8)
        }
    } else if let Some(file) = args.file {
        run_file_mode(
            client,
            args.server,
            node_id,
            file,
            args.push_only,
            args.pull_only,
            args.shadow_dir,
            args.name.clone(),
            mqtt_client,
            args.workspace,
        )
        .await
        .map(|_| 0u8)
    } else {
        unreachable!("Validated above")
    };

    match result {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            error!("Error: {}", e);
            ExitCode::from(1)
        }
    }
}

/// Run single-file sync mode using CRDT/MQTT infrastructure.
///
/// This is a thin wrapper around the same CRDT infrastructure that directory mode uses:
/// - Initializes CRDT state via cyan sync (MQTT)
/// - Pushes local content if it differs from server
/// - Spawns upload_task_crdt + receive_task_crdt for ongoing sync
#[allow(clippy::too_many_arguments)]
async fn run_file_mode(
    client: Client,
    server: String,
    node_id: String,
    file: PathBuf,
    push_only: bool,
    pull_only: bool,
    shadow_dir: String,
    name: Option<String>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let author = name.unwrap_or_else(|| "sync-client".to_string());

    let mode = if push_only {
        "push-only"
    } else if pull_only {
        "pull-only"
    } else {
        "bidirectional"
    };
    info!(
        "Starting commonplace-sync (file mode CRDT, {}): node={}, file={}",
        mode,
        node_id,
        file.display()
    );

    // Parse node_id as UUID (required for CRDT)
    let node_id_uuid = uuid::Uuid::parse_str(&node_id)
        .map_err(|e| format!("Invalid node UUID {}: {}", node_id, e))?;

    let parent_dir = file.parent().unwrap_or(Path::new("."));
    let filename = file
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("Invalid filename")?
        .to_string();

    // Create CRDT state (use Uuid::nil() for schema since file mode has no schema document)
    let crdt_state = Arc::new(RwLock::new(
        DirectorySyncState::load_or_create(parent_dir, uuid::Uuid::nil())
            .await
            .map_err(|e| format!("Failed to load CRDT state: {}", e))?,
    ));

    // Start MQTT event loop
    let mqtt_for_loop = mqtt_client.clone();
    tokio::spawn(async move {
        if let Err(e) = mqtt_for_loop.run_event_loop().await {
            error!("MQTT event loop error: {}", e);
        }
    });

    // Initialize CRDT state via cyan sync (MQTT).
    // Don't write to disk yet — we need to check if local content should take precedence.
    let cyan_ok = resync_crdt_state_via_cyan_with_pending(
        &mqtt_client,
        &workspace,
        node_id_uuid,
        &crdt_state,
        &filename,
        &file,
        &author,
        None,  // inode_tracker not needed during init
        false, // write_to_disk=false: defer disk write until we compare local vs server
        true,  // process_pending_edits
    )
    .await
    .is_ok();

    if !cyan_ok {
        warn!(
            "Failed to init CRDT state for {} — initializing empty",
            filename
        );
        let mut state = crdt_state.write().await;
        let fs = state.get_or_create_file(&filename, node_id_uuid);
        fs.initialize_empty();
    }

    // Reconcile local file vs server CRDT state:
    // - If local file has non-default content → push local to server
    // - If local file is missing/empty/default → write server content to disk
    let content_type_info = commonplace_doc::sync::detect_from_path(&file);
    let local_content = tokio::fs::read_to_string(&file).await.ok();
    let local_has_content = local_content
        .as_ref()
        .map(|c| !c.is_empty() && !commonplace_doc::sync::is_default_content(c, &content_type_info))
        .unwrap_or(false);

    if local_has_content {
        // Local file has meaningful content — push it to server if different
        if let Err(e) = push_local_if_differs(
            &mqtt_client,
            &workspace,
            &crdt_state,
            &filename,
            node_id_uuid,
            &file,
            &author,
        )
        .await
        {
            warn!("Failed to push local content for {}: {}", filename, e);
        }
    } else if cyan_ok {
        // No local content — write server content to disk
        let content_to_write = {
            let state = crdt_state.read().await;
            state.get_file(&filename).and_then(|fs| {
                // Try YText first (get_text_content), then try all types (get_doc_text_content)
                let text_content =
                    commonplace_doc::sync::crdt::crdt_publish::get_text_content(fs).ok();
                if text_content.as_ref().is_none_or(|s| s.is_empty()) {
                    let doc = fs.to_doc().ok()?;
                    let c = commonplace_doc::sync::crdt_merge::get_doc_text_content(&doc);
                    if c.is_empty() {
                        None
                    } else {
                        Some(c)
                    }
                } else {
                    text_content
                }
            })
        };
        if let Some(content) = content_to_write {
            if !content.is_empty()
                && !commonplace_doc::sync::is_default_content(&content, &content_type_info)
            {
                let content_with_newline = commonplace_doc::sync::ensure_trailing_newline(&content);
                if let Err(e) = tokio::fs::write(&file, &content_with_newline).await {
                    warn!(
                        "Failed to write server content to {}: {}",
                        file.display(),
                        e
                    );
                } else {
                    info!(
                        "Wrote server content to {} ({} bytes)",
                        file.display(),
                        content_with_newline.len()
                    );
                }
            }
        }
    }

    // Save CRDT state after initialization so it persists across restarts
    {
        let state = crdt_state.read().await;
        if let Err(e) = state.save(parent_dir).await {
            warn!("Failed to save initial CRDT state: {}", e);
        }
    }

    // Set up inode tracking if shadow_dir is configured
    let shadow_dir = resolve_shadow_dir(&shadow_dir, parent_dir);
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = if !shadow_dir.is_empty() {
        let shadow_path = PathBuf::from(&shadow_dir);
        tokio::fs::create_dir_all(&shadow_path)
            .await
            .map_err(|e| format!("Failed to create shadow directory {}: {}", shadow_dir, e))?;
        let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_path)));
        info!("Inode tracking enabled with shadow dir: {}", shadow_dir);
        Some(tracker)
    } else {
        None
    };

    // Spawn CRDT sync tasks (watcher + upload + receive)
    let mqtt_only_config = MqttOnlySyncConfig::mqtt_only();
    let task_handles = spawn_file_sync_tasks_crdt(
        mqtt_client.clone(),
        client.clone(),
        server.clone(),
        workspace.clone(),
        node_id_uuid,
        file.clone(),
        crdt_state.clone(),
        filename,
        pull_only,
        author.clone(),
        mqtt_only_config,
        inode_tracker,
        None, // file_states: no deduplication needed
        None, // relative_path
    );

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    for handle in task_handles {
        handle.abort();
    }

    info!("Goodbye!");
    Ok(())
}

/// Run directory sync mode
#[allow(clippy::too_many_arguments)]
async fn run_directory_mode(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    options: ScanOptions,
    initial_sync_strategy: String,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shadow_dir: String,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    name: Option<String>,
    mqtt_only_sync: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Derive author from name parameter, defaulting to "sync-client"
    let author = name.unwrap_or_else(|| "sync-client".to_string());

    // Track timing for initial sync event
    let sync_start = Instant::now();

    let mode = if push_only {
        "push-only"
    } else if pull_only {
        "pull-only"
    } else {
        "bidirectional"
    };
    info!(
        "Starting commonplace-sync (directory mode, {}): server={}, fs-root={}, directory={}, use_paths={}",
        mode,
        server,
        fs_root_id,
        directory.display(),
        use_paths
    );

    // CRDT mode requires UUID-based identifiers, not path-based
    if use_paths {
        return Err("CRDT sync mode requires UUID-based identifiers. Path-based sync (--use-paths) is no longer supported.".into());
    }

    // Verify directory exists
    if !directory.is_dir() {
        error!("Not a directory: {}", directory.display());
        return Err(format!("Not a directory: {}", directory.display()).into());
    }
    let shadow_dir = resolve_shadow_dir(&shadow_dir, &directory);
    let shadow_dir = resolve_shadow_dir(&shadow_dir, &directory);

    // Set up inode tracking if shadow_dir is configured (unix only)
    #[cfg(unix)]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = if !shadow_dir.is_empty() {
        let shadow_path = PathBuf::from(&shadow_dir);

        tokio::fs::create_dir_all(&shadow_path)
            .await
            .map_err(|e| format!("Failed to create shadow directory {}: {}", shadow_dir, e))?;

        let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_path)));
        info!("Inode tracking enabled with shadow dir: {}", shadow_dir);
        Some(tracker)
    } else {
        debug!("Inode tracking disabled (no shadow_dir configured)");
        None
    };
    #[cfg(not(unix))]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = None;

    // Start MQTT event loop before making any MQTT request/response calls.
    let mqtt_for_loop = mqtt_client.clone();
    tokio::spawn(async move {
        if let Err(e) = mqtt_for_loop.run_event_loop().await {
            error!("MQTT event loop error: {}", e);
        }
    });

    // Verify fs-root document exists (or create it) via MQTT
    let mqtt_request = Arc::new(
        MqttRequestClient::new(mqtt_client.clone(), workspace.clone())
            .await
            .map_err(|e| format!("Failed to create MQTT request client: {}", e))?,
    );
    set_sync_schema_mqtt_request_client(Some(mqtt_request.clone()));
    ensure_fs_root_exists_mqtt(&mqtt_request, &fs_root_id).await?;

    // Load or create state file for persisting per-file CIDs
    let state_file_path =
        commonplace_doc::sync::state_file::SyncStateFile::state_file_path(&directory);
    let state_file = commonplace_doc::sync::state_file::SyncStateFile::load_or_create(
        &directory,
        &server,
        &fs_root_id,
    )
    .await
    .unwrap_or_else(|e| {
        warn!("Failed to load state file: {} - creating new one", e);
        commonplace_doc::sync::state_file::SyncStateFile::new(server.clone(), fs_root_id.clone())
    });
    let shared_state_file: commonplace_doc::sync::SharedStateFile =
        Arc::new(RwLock::new(state_file));
    info!(
        "Loaded state file from {} with {} tracked files",
        state_file_path.display(),
        shared_state_file.read().await.files.len()
    );

    // Initialize inode tracker from persisted state
    if let Some(ref tracker) = inode_tracker {
        let sf = shared_state_file.read().await;
        let mut t = tracker.write().await;
        t.init_from_state_file(&sf, &directory);
        if !t.states.is_empty() {
            info!(
                "Initialized inode tracker with {} persisted entries",
                t.states.len()
            );
        }
    }

    // Initialize per-file sync state map before watcher/subscription task startup.
    let file_states: Arc<RwLock<HashMap<String, FileSyncState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Track schema writes emitted by this process to avoid feedback loops.
    let written_schemas: commonplace_doc::sync::WrittenSchemas =
        Arc::new(RwLock::new(std::collections::HashMap::new()));

    if initial_sync_strategy != "skip" {
        warn!(
            "Ignoring --initial-sync={} during HTTP-off startup; MQTT/cyan bootstrap owns remote reconciliation",
            initial_sync_strategy
        );
    }

    // HTTP bootstrap is disabled in strict MQTT runtime.
    // Schema/state reconciliation is handled by MQTT subscription tasks.
    let initial_schema_cid: Option<String> = None;

    // Build UUID map from local schema files.
    // Remote reconciliation is deferred to MQTT subscription tasks.
    let mut uuid_map = HashMap::new();
    build_uuid_map_from_local_schemas(&directory, "", &mut uuid_map);
    let file_count = uuid_map.len();
    info!("Built UUID map from local schemas: {} files", file_count);

    // Populate file_states from UUID map (only for files that exist on disk).
    // Content sync (push/pull) is handled later by CRDT init + push_local_if_differs.
    {
        let mut states = file_states.write().await;
        for (relative_path, uuid) in &uuid_map {
            let file_path = directory.join(relative_path);
            if !file_path.exists() {
                debug!("Skipping {} — not on disk yet", relative_path);
                continue;
            }

            trace_timeline(TimelineMilestone::UuidReady, relative_path, Some(uuid));

            let initial_cid = shared_state_file.read().await.get_file_cid(relative_path);
            let state = Arc::new(RwLock::new(SyncState::for_directory_file(
                initial_cid,
                shared_state_file.clone(),
                relative_path.clone(),
            )));

            states.insert(
                relative_path.clone(),
                FileSyncState {
                    relative_path: relative_path.clone(),
                    identifier: uuid.clone(),
                    state,
                    task_handles: Vec::new(),
                    use_paths,
                    content_hash: None,
                    crdt_tasks_spawned: false,
                },
            );
        }
    }

    // Publish initial-sync-complete event via MQTT
    publish_initial_sync_complete(
        &mqtt_client,
        &fs_root_id,
        file_count,
        &initial_sync_strategy,
        sync_start.elapsed().as_millis() as u64,
    )
    .await;

    // Start directory watcher and create shared state
    let WatcherSetup {
        mut dir_rx,
        watcher_handle,
        watched_subdirs,
    } = setup_directory_watchers(
        pull_only,
        directory.clone(),
        options.clone(),
        written_schemas.clone(),
    );

    // Load/create CRDT state early so subscription tasks can use it.
    // Note: If fs_root_id is not a UUID (e.g., "workspace"), use a nil UUID for the schema.
    let schema_node_id = uuid::Uuid::parse_str(&fs_root_id).unwrap_or_else(|_| {
        warn!(
            "fs_root_id '{}' is not a UUID, using nil UUID for CRDT schema state",
            fs_root_id
        );
        uuid::Uuid::nil()
    });
    let crdt_state = Arc::new(RwLock::new(
        DirectorySyncState::load_or_create(&directory, schema_node_id)
            .await
            .map_err(|e| format!("Failed to load CRDT state: {}", e))?,
    ));

    // Reconcile sync state UUIDs with schema to detect and fix drift
    {
        let mut state = crdt_state.write().await;
        if let Err(e) = state.reconcile_with_schema(&directory).await {
            warn!("Failed to reconcile UUIDs with schema: {}", e);
        }
    }

    let subdir_cache = Arc::new(SubdirStateCache::new());
    let mqtt_only_config = if mqtt_only_sync {
        MqttOnlySyncConfig::mqtt_only()
    } else {
        MqttOnlySyncConfig::with_http_fallback()
    };
    let crdt_context = Some(CrdtFileSyncContext {
        mqtt_client: mqtt_client.clone(),
        mqtt_request: mqtt_request.clone(),
        workspace: workspace.clone(),
        crdt_state: crdt_state.clone(),
        subdir_cache: subdir_cache.clone(),
        mqtt_only_config,
    });

    // Start subscription task for fs-root (skip if push-only)
    let subscription_handle = if !push_only {
        let initial_uuid_map: HashMap<String, String> = uuid_map.clone();

        info!(
            "Using MQTT for directory sync subscriptions ({} file UUIDs)",
            initial_uuid_map.len()
        );
        Some(tokio::spawn(directory_mqtt_task(
            client.clone(),
            server.clone(),
            fs_root_id.clone(),
            directory.clone(),
            file_states.clone(),
            use_paths,
            push_only,
            pull_only,
            author.clone(),
            #[cfg(unix)]
            inode_tracker.clone(),
            watched_subdirs.clone(),
            mqtt_client.clone(),
            workspace.clone(),
            Some(written_schemas.clone()),
            initial_schema_cid.clone(),
            Some(shared_state_file.clone()),
            initial_uuid_map,
            crdt_context.clone(),
        )))
    } else {
        info!("Push-only mode: skipping subscription");
        None
    };

    // Start tasks for all node-backed subdirectories (skip if push-only)
    // This allows files created in subdirectories to propagate to other sync clients
    if !push_only {
        let params = SubdirSpawnParams {
            client: client.clone(),
            server: server.clone(),
            fs_root_id: fs_root_id.clone(),
            directory: directory.clone(),
            file_states: file_states.clone(),
            use_paths,
            push_only,
            pull_only,
            shared_state_file: Some(shared_state_file.clone()),
            author: author.clone(),
            #[cfg(unix)]
            inode_tracker: inode_tracker.clone(),
            watched_subdirs: watched_subdirs.clone(),
            crdt_context: crdt_context.clone(),
        };
        let transport = SubdirTransport::Mqtt {
            client: mqtt_client.clone(),
            workspace: workspace.clone(),
        };
        let count = spawn_subdir_watchers(&params, transport).await;
        info!("Spawned {} node-backed subdirectory watcher(s)", count);
    }

    // Start file sync tasks for each file and store handles in FileSyncState

    {
        let mut states = file_states.write().await;
        for (relative_path, file_state) in states.iter_mut() {
            let file_path = directory.join(relative_path);

            // Parse doc_id from identifier (UUID required for CRDT)
            let node_id = uuid::Uuid::parse_str(&file_state.identifier)
                .map_err(|e| format!("Invalid file UUID {}: {}", file_state.identifier, e))?;

            // Determine which state owns this file (root or subdirectory)
            let owning_doc = find_owning_document(&directory, &fs_root_id, relative_path);
            let is_subdirectory = owning_doc.document_id != fs_root_id;

            let (file_crdt_state, filename) = if is_subdirectory {
                let subdir_node_id =
                    uuid::Uuid::parse_str(&owning_doc.document_id).map_err(|e| {
                        format!("Invalid subdir node_id '{}': {}", owning_doc.document_id, e)
                    })?;
                let subdir_state = subdir_cache
                    .get_or_load(&owning_doc.directory, subdir_node_id)
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to load subdir state for {}: {}",
                            owning_doc.directory.display(),
                            e
                        )
                    })?;
                (subdir_state, owning_doc.relative_path.clone())
            } else {
                let filename = std::path::Path::new(relative_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(relative_path)
                    .to_string();
                (crdt_state.clone(), filename)
            };

            // Initialize CRDT state via cyan sync (MQTT) before spawning tasks.
            // This ensures all sync clients share the same Yjs operation history,
            // which is critical for merges to work correctly (especially deletions).
            // Don't write to disk during init — we push local content afterward,
            // and write_to_disk=true would overwrite local content with (possibly empty)
            // server content before we get a chance to push.
            if let Err(e) = resync_crdt_state_via_cyan_with_pending(
                &mqtt_client,
                &workspace,
                node_id,
                &file_crdt_state,
                &filename,
                &file_path,
                &author,
                inode_tracker.as_ref(),
                false,
                true,
            )
            .await
            {
                warn!(
                    "Failed to initialize CRDT state for {}: {} — initializing empty",
                    relative_path, e
                );
                // Initialize empty so should_queue_edits() doesn't return NeedsServerInit
                // and MQTT edits can still be applied
                {
                    let mut state = file_crdt_state.write().await;
                    let fs = state.get_or_create_file(&filename, node_id);
                    fs.initialize_empty();
                }
            } else {
                // Trace CRDT_INIT_COMPLETE milestone (directory mode)
                trace_timeline(
                    TimelineMilestone::CrdtInitComplete,
                    relative_path,
                    Some(&file_state.identifier),
                );
            }

            // Push local content if it differs from server (replaces sync_single_file push).
            // This runs after BOTH success and failure of CRDT init: when cyan sync fails
            // and we initialize empty, we still need to push local content to create the
            // first commit on the server.
            if !pull_only {
                if let Err(e) = push_local_if_differs(
                    &mqtt_client,
                    &workspace,
                    &file_crdt_state,
                    &filename,
                    node_id,
                    &file_path,
                    &author,
                )
                .await
                {
                    warn!("Failed to push local content for {}: {}", relative_path, e);
                }
            }

            // Pull case: if local file is empty/default but server has content,
            // write server content to disk so the file reflects the CRDT state.
            {
                let local_content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                let content_info = detect_from_path(&file_path);
                if local_content.is_empty()
                    || commonplace_doc::sync::is_default_content(&local_content, &content_info)
                {
                    let crdt_content = {
                        let state = file_crdt_state.read().await;
                        state.get_file(&filename).and_then(|fs| {
                            let doc = fs.to_doc().ok()?;
                            let c = commonplace_doc::sync::crdt_merge::get_doc_text_content(&doc);
                            if c.is_empty() {
                                None
                            } else {
                                Some(c)
                            }
                        })
                    };
                    if let Some(ref content) = crdt_content {
                        if !content.is_empty() {
                            info!(
                                "Writing CRDT content to {} ({} bytes, pull case)",
                                file_path.display(),
                                content.len()
                            );
                            if let Err(e) = tokio::fs::write(&file_path, content).await {
                                warn!(
                                    "Failed to write CRDT content to {}: {}",
                                    file_path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
            // Trace TASK_SPAWN milestone before spawning tasks (directory mode)
            trace_timeline(
                TimelineMilestone::TaskSpawn,
                relative_path,
                Some(&file_state.identifier),
            );
            file_state.task_handles = spawn_file_sync_tasks_crdt(
                mqtt_client.clone(),
                client.clone(),
                server.clone(),
                workspace.clone(),
                node_id,
                file_path,
                file_crdt_state.clone(),
                filename,
                pull_only,
                author.clone(),
                mqtt_only_config,
                inode_tracker.clone(),
                None, // file_states - initial setup, not needed
                None, // relative_path
            );
        }
    }

    // Start shadow watcher, handler, and GC if inode tracking is enabled (unix only)
    #[cfg(unix)]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle) =
        if let Some(ref tracker) = inode_tracker {
            let shadow_path = {
                let t = tracker.read().await;
                t.shadow_dir.clone()
            };

            let (watcher, handler, gc) = spawn_shadow_tasks(
                shadow_path,
                client.clone(),
                server.clone(),
                tracker.clone(),
                use_paths,
                author.clone(),
            );

            (Some(watcher), Some(handler), Some(gc))
        } else {
            (None, None, None)
        };
    #[cfg(not(unix))]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle): (
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = (None, None, None);

    // Handle directory-level events (file creation/deletion)
    let dir_event_handle = tokio::spawn({
        let client = client.clone();
        let server = server.clone();
        let fs_root_id = fs_root_id.clone();
        let directory = directory.clone();
        let options = options.clone();
        let file_states = file_states.clone();
        let written_schemas = written_schemas.clone();
        let author = author.clone();
        // Create CRDT params for the event handler
        let crdt_params = CrdtEventParams {
            mqtt_client: mqtt_client.clone(),
            mqtt_request: mqtt_request.clone(),
            workspace: workspace.clone(),
            crdt_state: crdt_state.clone(),
            subdir_cache: subdir_cache.clone(),
            mqtt_only_config,
            inode_tracker: inode_tracker.clone(),
        };
        async move {
            while let Some(event) = dir_rx.recv().await {
                handle_dir_event(
                    &client,
                    &server,
                    &fs_root_id,
                    &directory,
                    &options,
                    &file_states,
                    pull_only,
                    &author,
                    Some(&written_schemas),
                    Some(&crdt_params),
                    event,
                )
                .await;
            }
        }
    });

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    // Cancel all tasks
    if let Some(handle) = watcher_handle {
        handle.abort();
    }
    if let Some(handle) = subscription_handle {
        handle.abort();
    }
    dir_event_handle.abort();

    // Abort shadow tasks
    if let Some(handle) = shadow_watcher_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_handler_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_gc_handle {
        handle.abort();
    }

    // Abort all per-file sync tasks
    {
        let states = file_states.read().await;
        for file_state in states.values() {
            for handle in &file_state.task_handles {
                handle.abort();
            }
        }
    }

    // Save state file on shutdown
    info!("Saving state file...");
    {
        let sf = shared_state_file.read().await;
        if let Err(e) = sf.save(&state_file_path).await {
            warn!("Failed to save state file on shutdown: {}", e);
        } else {
            info!("Saved state file with {} tracked files", sf.files.len());
        }
    }

    info!("Goodbye!");
    Ok(())
}

/// Run exec mode: sync directory, run command, exit when command exits
///
/// This mode is designed for workflows where a user wants to work on synced files
/// with their preferred editor/tool, and have everything tear down cleanly when done.
#[allow(clippy::too_many_arguments)]
async fn run_exec_mode(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    options: ScanOptions,
    initial_sync_strategy: String,
    use_paths: bool,
    exec_cmd: String,
    exec_args: Vec<String>,
    sandbox: bool,
    push_only: bool,
    pull_only: bool,
    shadow_dir: String,
    process_name: Option<String>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    command_path: Option<String>,
    sandbox_timeout_secs: u64,
    mqtt_only_sync: bool,
) -> Result<u8, Box<dyn std::error::Error + Send + Sync>> {
    // Derive author from process_name, defaulting to "sync-client"
    let author = process_name
        .clone()
        .unwrap_or_else(|| "sync-client".to_string());

    let mode = if push_only {
        "push-only"
    } else if pull_only {
        "pull-only"
    } else {
        "bidirectional"
    };
    info!(
        "Starting commonplace-sync (exec mode, {}): server={}, fs-root={}, directory={}, exec={}",
        mode,
        server,
        fs_root_id,
        directory.display(),
        exec_cmd
    );

    // CRDT mode requires UUID-based identifiers, not path-based
    if use_paths {
        return Err("CRDT sync mode requires UUID-based identifiers. Path-based sync (--use-paths) is no longer supported.".into());
    }

    // Verify directory exists or create it
    if !directory.exists() {
        info!("Creating directory: {}", directory.display());
        tokio::fs::create_dir_all(&directory).await?;
    }
    if !directory.is_dir() {
        error!("Not a directory: {}", directory.display());
        return Err(format!("Not a directory: {}", directory.display()).into());
    }

    // Set up inode tracking if shadow_dir is configured (unix only)
    #[cfg(unix)]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = if !shadow_dir.is_empty() {
        let shadow_path = PathBuf::from(&shadow_dir);

        tokio::fs::create_dir_all(&shadow_path)
            .await
            .map_err(|e| format!("Failed to create shadow directory {}: {}", shadow_dir, e))?;

        let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_path)));
        info!("Inode tracking enabled with shadow dir: {}", shadow_dir);
        Some(tracker)
    } else {
        debug!("Inode tracking disabled (no shadow_dir configured)");
        None
    };
    #[cfg(not(unix))]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = None;

    // Start MQTT event loop before making any MQTT request/response calls.
    let mqtt_for_loop = mqtt_client.clone();
    tokio::spawn(async move {
        if let Err(e) = mqtt_for_loop.run_event_loop().await {
            error!("MQTT event loop error: {}", e);
        }
    });

    // Verify fs-root document exists (or create it) via MQTT
    let mqtt_request = Arc::new(
        MqttRequestClient::new(mqtt_client.clone(), workspace.clone())
            .await
            .map_err(|e| format!("Failed to create MQTT request client: {}", e))?,
    );
    set_sync_schema_mqtt_request_client(Some(mqtt_request.clone()));
    ensure_fs_root_exists_mqtt(&mqtt_request, &fs_root_id).await?;

    // Load or create state file for persisting per-file CIDs (sandbox mode)
    let state_file_path =
        commonplace_doc::sync::state_file::SyncStateFile::state_file_path(&directory);
    let state_file = commonplace_doc::sync::state_file::SyncStateFile::load_or_create(
        &directory,
        &server,
        &fs_root_id,
    )
    .await
    .unwrap_or_else(|e| {
        warn!("Failed to load state file: {} - creating new one", e);
        commonplace_doc::sync::state_file::SyncStateFile::new(server.clone(), fs_root_id.clone())
    });
    let shared_state_file: commonplace_doc::sync::SharedStateFile =
        Arc::new(RwLock::new(state_file));
    info!(
        "Loaded state file from {} with {} tracked files",
        state_file_path.display(),
        shared_state_file.read().await.files.len()
    );

    // Initialize inode tracker from persisted state (sandbox mode)
    if let Some(ref tracker) = inode_tracker {
        let sf = shared_state_file.read().await;
        let mut t = tracker.write().await;
        t.init_from_state_file(&sf, &directory);
        if !t.states.is_empty() {
            info!(
                "Initialized inode tracker with {} persisted entries",
                t.states.len()
            );
        }
    }

    // Initialize per-file sync state map before watcher/subscription task startup.
    let file_states: Arc<RwLock<HashMap<String, FileSyncState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Track schema writes emitted by this process to avoid feedback loops.
    let written_schemas: commonplace_doc::sync::WrittenSchemas =
        Arc::new(RwLock::new(std::collections::HashMap::new()));

    if initial_sync_strategy != "skip" {
        warn!(
            "Ignoring --initial-sync={} during HTTP-off startup; MQTT/cyan bootstrap owns remote reconciliation",
            initial_sync_strategy
        );
    }

    // HTTP bootstrap is disabled in strict MQTT runtime.
    // Schema/state reconciliation is handled by MQTT subscription tasks.
    let initial_schema_cid: Option<String> = None;

    let sync_start = Instant::now();

    // Build UUID map from local schema files.
    // Remote reconciliation is deferred to MQTT subscription tasks.
    let mut uuid_map = HashMap::new();
    build_uuid_map_from_local_schemas(&directory, "", &mut uuid_map);
    let file_count = uuid_map.len();
    info!("Built UUID map from local schemas: {} files", file_count);

    // Populate file_states from UUID map (only for files that exist on disk).
    // Content sync (push/pull) is handled later by CRDT init + push_local_if_differs.
    {
        let mut states = file_states.write().await;
        for (relative_path, uuid) in &uuid_map {
            let file_path = directory.join(relative_path);
            if !file_path.exists() {
                debug!("Skipping {} — not on disk yet", relative_path);
                continue;
            }

            trace_timeline(TimelineMilestone::UuidReady, relative_path, Some(uuid));

            let initial_cid = shared_state_file.read().await.get_file_cid(relative_path);
            let state = Arc::new(RwLock::new(SyncState::for_directory_file(
                initial_cid,
                shared_state_file.clone(),
                relative_path.clone(),
            )));

            states.insert(
                relative_path.clone(),
                FileSyncState {
                    relative_path: relative_path.clone(),
                    identifier: uuid.clone(),
                    state,
                    task_handles: Vec::new(),
                    use_paths,
                    content_hash: None,
                    crdt_tasks_spawned: false,
                },
            );
        }
    }

    // Publish initial-sync-complete event via MQTT
    publish_initial_sync_complete(
        &mqtt_client,
        &fs_root_id,
        file_count,
        &initial_sync_strategy,
        sync_start.elapsed().as_millis() as u64,
    )
    .await;

    // Start directory watcher and create shared state
    let WatcherSetup {
        mut dir_rx,
        watcher_handle,
        watched_subdirs,
    } = setup_directory_watchers(
        pull_only,
        directory.clone(),
        options.clone(),
        written_schemas.clone(),
    );

    // Load/create CRDT state early so subscription tasks can use it.
    // Note: If fs_root_id is not a UUID (e.g., "workspace"), use a nil UUID for the schema.
    let schema_node_id = uuid::Uuid::parse_str(&fs_root_id).unwrap_or_else(|_| {
        warn!(
            "fs_root_id '{}' is not a UUID, using nil UUID for CRDT schema state",
            fs_root_id
        );
        uuid::Uuid::nil()
    });
    let crdt_state = Arc::new(RwLock::new(
        DirectorySyncState::load_or_create(&directory, schema_node_id)
            .await
            .map_err(|e| format!("Failed to load CRDT state: {}", e))?,
    ));

    // Reconcile sync state UUIDs with schema to detect and fix drift
    {
        let mut state = crdt_state.write().await;
        if let Err(e) = state.reconcile_with_schema(&directory).await {
            warn!("Failed to reconcile UUIDs with schema: {}", e);
        }
    }

    let subdir_cache = Arc::new(SubdirStateCache::new());
    let mqtt_only_config = if mqtt_only_sync {
        MqttOnlySyncConfig::mqtt_only()
    } else {
        MqttOnlySyncConfig::with_http_fallback()
    };
    let crdt_context = Some(CrdtFileSyncContext {
        mqtt_client: mqtt_client.clone(),
        mqtt_request: mqtt_request.clone(),
        workspace: workspace.clone(),
        crdt_state: crdt_state.clone(),
        subdir_cache: subdir_cache.clone(),
        mqtt_only_config,
    });

    // Start subscription task for fs-root (skip if push-only)
    let subscription_handle = if !push_only {
        let initial_uuid_map: HashMap<String, String> = uuid_map.clone();
        info!(
            "Using MQTT for exec mode subscriptions ({} file UUIDs)",
            initial_uuid_map.len()
        );
        Some(tokio::spawn(directory_mqtt_task(
            client.clone(),
            server.clone(),
            fs_root_id.clone(),
            directory.clone(),
            file_states.clone(),
            use_paths,
            push_only,
            pull_only,
            author.clone(),
            #[cfg(unix)]
            inode_tracker.clone(),
            watched_subdirs.clone(),
            mqtt_client.clone(),
            workspace.clone(),
            Some(written_schemas.clone()),
            initial_schema_cid.clone(),
            Some(shared_state_file.clone()),
            initial_uuid_map,
            crdt_context.clone(),
        )))
    } else {
        info!("Push-only mode: skipping subscription");
        None
    };

    // Start tasks for all node-backed subdirectories (skip if push-only)
    if !push_only {
        let params = SubdirSpawnParams {
            client: client.clone(),
            server: server.clone(),
            fs_root_id: fs_root_id.clone(),
            directory: directory.clone(),
            file_states: file_states.clone(),
            use_paths,
            push_only,
            pull_only,
            shared_state_file: Some(shared_state_file.clone()),
            author: author.clone(),
            #[cfg(unix)]
            inode_tracker: inode_tracker.clone(),
            watched_subdirs: watched_subdirs.clone(),
            crdt_context: crdt_context.clone(),
        };
        let transport = SubdirTransport::Mqtt {
            client: mqtt_client.clone(),
            workspace: workspace.clone(),
        };
        let count = spawn_subdir_watchers(&params, transport).await;
        info!("Spawned {} node-backed subdirectory watcher(s)", count);
    }

    // Start file sync tasks for each file using CRDT mode

    {
        let mut states = file_states.write().await;
        for (relative_path, file_state) in states.iter_mut() {
            let file_path = directory.join(relative_path);

            // Parse doc_id from identifier - UUID required for CRDT mode
            let node_id = uuid::Uuid::parse_str(&file_state.identifier)
                .map_err(|e| format!("UUID required for CRDT mode: {}", e))?;

            // Determine which state owns this file (root or subdirectory)
            let owning_doc = find_owning_document(&directory, &fs_root_id, relative_path);
            let is_subdirectory = owning_doc.document_id != fs_root_id;

            let (file_crdt_state, filename) = if is_subdirectory {
                let subdir_node_id =
                    uuid::Uuid::parse_str(&owning_doc.document_id).map_err(|e| {
                        format!("Invalid subdir node_id '{}': {}", owning_doc.document_id, e)
                    })?;
                let subdir_state = subdir_cache
                    .get_or_load(&owning_doc.directory, subdir_node_id)
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to load subdir state for {}: {}",
                            owning_doc.directory.display(),
                            e
                        )
                    })?;
                (subdir_state, owning_doc.relative_path.clone())
            } else {
                let filename = std::path::Path::new(relative_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(relative_path)
                    .to_string();
                (crdt_state.clone(), filename)
            };

            // Don't write to disk during init — we push local content afterward,
            // and write_to_disk=true would overwrite local content with (possibly empty)
            // server content before we get a chance to push.
            if let Err(e) = resync_crdt_state_via_cyan_with_pending(
                &mqtt_client,
                &workspace,
                node_id,
                &file_crdt_state,
                &filename,
                &file_path,
                &author,
                inode_tracker.as_ref(),
                false,
                true,
            )
            .await
            {
                warn!(
                    "Failed to initialize CRDT state for {}: {} — initializing empty",
                    relative_path, e
                );
                // Initialize empty so should_queue_edits() doesn't return NeedsServerInit
                // and MQTT edits can still be applied
                {
                    let mut state = file_crdt_state.write().await;
                    let fs = state.get_or_create_file(&filename, node_id);
                    fs.initialize_empty();
                }
            } else {
                // Trace CRDT_INIT_COMPLETE milestone
                trace_timeline(
                    TimelineMilestone::CrdtInitComplete,
                    relative_path,
                    Some(&file_state.identifier),
                );
            }

            // Push local content if it differs from server (replaces sync_single_file push).
            // This runs after BOTH success and failure of CRDT init.
            if !pull_only {
                if let Err(e) = push_local_if_differs(
                    &mqtt_client,
                    &workspace,
                    &file_crdt_state,
                    &filename,
                    node_id,
                    &file_path,
                    &author,
                )
                .await
                {
                    warn!("Failed to push local content for {}: {}", relative_path, e);
                }
            }

            // Pull case: if local file is empty/default but server has content,
            // write server content to disk so the file reflects the CRDT state.
            {
                let local_content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                let content_info = detect_from_path(&file_path);
                if local_content.is_empty()
                    || commonplace_doc::sync::is_default_content(&local_content, &content_info)
                {
                    let crdt_content = {
                        let state = file_crdt_state.read().await;
                        state.get_file(&filename).and_then(|fs| {
                            let doc = fs.to_doc().ok()?;
                            let c = commonplace_doc::sync::crdt_merge::get_doc_text_content(&doc);
                            if c.is_empty() {
                                None
                            } else {
                                Some(c)
                            }
                        })
                    };
                    if let Some(ref content) = crdt_content {
                        if !content.is_empty() {
                            info!(
                                "Writing CRDT content to {} ({} bytes, pull case)",
                                file_path.display(),
                                content.len()
                            );
                            if let Err(e) = tokio::fs::write(&file_path, content).await {
                                warn!(
                                    "Failed to write CRDT content to {}: {}",
                                    file_path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }

            // Trace TASK_SPAWN milestone before spawning tasks
            trace_timeline(
                TimelineMilestone::TaskSpawn,
                relative_path,
                Some(&file_state.identifier),
            );
            file_state.task_handles = spawn_file_sync_tasks_crdt(
                mqtt_client.clone(),
                client.clone(),
                server.clone(),
                workspace.clone(),
                node_id,
                file_path,
                file_crdt_state.clone(),
                filename,
                pull_only,
                author.clone(),
                mqtt_only_config,
                inode_tracker.clone(),
                None, // file_states - initial setup, not needed
                None, // relative_path
            );
        }
    }

    // Start shadow watcher, handler, and GC if inode tracking is enabled (unix only)
    #[cfg(unix)]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle) =
        if let Some(ref tracker) = inode_tracker {
            let shadow_path = {
                let t = tracker.read().await;
                t.shadow_dir.clone()
            };

            let (watcher, handler, gc) = spawn_shadow_tasks(
                shadow_path,
                client.clone(),
                server.clone(),
                tracker.clone(),
                use_paths,
                author.clone(),
            );

            (Some(watcher), Some(handler), Some(gc))
        } else {
            (None, None, None)
        };
    #[cfg(not(unix))]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle): (
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = (None, None, None);

    // Start directory event handler (same logic as run_directory_mode)
    let dir_event_handle = tokio::spawn({
        let client = client.clone();
        let server = server.clone();
        let fs_root_id = fs_root_id.clone();
        let directory = directory.clone();
        let options = options.clone();
        let file_states = file_states.clone();
        let written_schemas = written_schemas.clone();
        let author = author.clone();
        // Create CRDT params for the event handler
        let crdt_params = CrdtEventParams {
            mqtt_client: mqtt_client.clone(),
            mqtt_request: mqtt_request.clone(),
            workspace: workspace.clone(),
            crdt_state: crdt_state.clone(),
            subdir_cache: subdir_cache.clone(),
            mqtt_only_config,
            inode_tracker: inode_tracker.clone(),
        };
        async move {
            while let Some(event) = dir_rx.recv().await {
                debug!("RECEIVED DIR EVENT (exec mode): {:?}", event);
                handle_dir_event(
                    &client,
                    &server,
                    &fs_root_id,
                    &directory,
                    &options,
                    &file_states,
                    pull_only,
                    &author,
                    Some(&written_schemas),
                    Some(&crdt_params),
                    event,
                )
                .await;
            }
        }
    });

    // Wait for CRDT readiness before starting exec (in sandbox mode)
    // This ensures all file sync tasks are ready to capture writes from the exec process
    if sandbox {
        let timeout = Duration::from_secs(sandbox_timeout_secs);
        let start = Instant::now();
        let check_interval = Duration::from_millis(100);

        loop {
            // Check if all files have CRDT state initialized AND receive tasks ready.
            // We use should_queue_edits() to check both conditions:
            // 1. yjs_state is set (CRDT initialized from server)
            // 2. receive_task_ready is true (receive task is subscribed and ready)
            let state = crdt_state.read().await;
            let unready_files: Vec<(String, &str)> = state
                .files
                .iter()
                .filter_map(|(name, file_state)| {
                    file_state
                        .should_queue_edits()
                        .map(|reason| (name.clone(), reason.as_str()))
                })
                .collect();
            drop(state);

            if unready_files.is_empty() {
                info!(
                    "CRDT readiness achieved: all {} files ready, starting exec ({}ms elapsed)",
                    file_count,
                    start.elapsed().as_millis()
                );
                break;
            }

            if start.elapsed() >= timeout {
                warn!(
                    "CRDT readiness timeout after {}s: {} files still not ready ({:?}), proceeding to exec",
                    sandbox_timeout_secs,
                    unready_files.len(),
                    unready_files
                );
                break;
            }

            // Brief sleep before next check
            sleep(check_interval).await;
        }
    }

    // Build the command to execute
    // Parse exec_cmd - if it contains spaces and no exec_args, treat as shell command
    let (program, args) = if exec_args.is_empty() && exec_cmd.contains(' ') {
        // Treat as shell command
        if cfg!(target_os = "windows") {
            ("cmd".to_string(), vec!["/C".to_string(), exec_cmd])
        } else {
            ("sh".to_string(), vec!["-c".to_string(), exec_cmd])
        }
    } else {
        // Direct command with optional args
        (exec_cmd, exec_args)
    };

    // Calculate exec_name early - needed for log file schema entries
    // Use provided process_name if available, otherwise extract from program path
    let exec_name = process_name.unwrap_or_else(|| {
        std::path::Path::new(&program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("exec")
            .to_string()
    });

    // Note: stdout/stderr are now emitted as MQTT events instead of being saved to files
    // Subscribe to {workspace}/events/{path}/stdout and {workspace}/events/{path}/stderr
    // to receive output lines

    info!("Launching: {} {:?}", program, args);

    // Build the command
    let mut cmd = tokio::process::Command::new(&program);
    cmd.args(&args)
        .current_dir(&directory)
        .stdin(std::process::Stdio::inherit())
        // Pass through key environment variables
        .env("COMMONPLACE_SERVER", &server)
        .env("COMMONPLACE_NODE", &fs_root_id);

    // For sandbox mode, capture stdout/stderr to write to log files
    // For non-sandbox mode, inherit stdout/stderr for interactive programs
    if sandbox {
        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Force unbuffered output for Python processes (and similar)
            .env("PYTHONUNBUFFERED", "1");
    } else {
        cmd.stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit());
    }

    // Make child a process group leader and set death signal on Linux
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            // Put child in its own process group so we can kill all descendants
            libc::setpgid(0, 0);
            // Request SIGTERM if parent dies (Linux-only, covers SIGKILL of parent)
            #[cfg(target_os = "linux")]
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }

    // In sandbox mode, add commonplace binaries to PATH
    if sandbox {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(bin_dir) = exe_path.parent() {
                let current_path = std::env::var_os("PATH").unwrap_or_default();
                if let Ok(new_path) = std::env::join_paths(
                    std::iter::once(bin_dir.to_path_buf())
                        .chain(std::env::split_paths(&current_path)),
                ) {
                    cmd.env("PATH", &new_path);
                    info!("Added {} to PATH for sandbox", bin_dir.display());
                }
            }
        }
    }

    // Spawn the child process in the synced directory
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn command '{}': {}", program, e))?;

    // Trace EXEC_START milestone - exec process has been spawned
    trace_timeline(TimelineMilestone::ExecStart, &exec_name, None);

    // In sandbox mode, emit stdout/stderr as MQTT events and spawn command listener
    if sandbox {
        use tokio::io::{AsyncBufReadExt, BufReader};

        // Use command_path for event topics (path like "/stdio-test"), falling back to UUID
        let event_path = command_path
            .clone()
            .unwrap_or_else(|| fs_root_id.clone())
            .trim_start_matches('/')
            .to_string();

        // Stdout event emission
        if let Some(stdout) = child.stdout.take() {
            let exec_name_clone = exec_name.clone();
            let mqtt_clone = mqtt_client.clone();
            let workspace_clone = workspace.clone();
            let path_clone = event_path.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // Print to console (like non-sandbox mode would)
                    println!("[{}] {}", exec_name_clone, line);
                    // Emit as MQTT event
                    let topic = Topic::events(&workspace_clone, &path_clone, "stdout");
                    if let Err(e) = mqtt_clone
                        .publish(&topic.to_topic_string(), line.as_bytes(), QoS::AtMostOnce)
                        .await
                    {
                        tracing::debug!("Failed to publish stdout event: {}", e);
                    }
                }
            });
        }

        // Stderr event emission
        if let Some(stderr) = child.stderr.take() {
            let exec_name_clone = exec_name.clone();
            let mqtt_clone = mqtt_client.clone();
            let workspace_clone = workspace.clone();
            let path_clone = event_path.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // Print to console (like non-sandbox mode would)
                    eprintln!("[{}] {}", exec_name_clone, line);
                    // Emit as MQTT event
                    let topic = Topic::events(&workspace_clone, &path_clone, "stderr");
                    if let Err(e) = mqtt_clone
                        .publish(&topic.to_topic_string(), line.as_bytes(), QoS::AtMostOnce)
                        .await
                    {
                        tracing::debug!("Failed to publish stderr event: {}", e);
                    }
                }
            });
        }

        // Spawn command listener for this sandbox process
        // Commands sent to {workspace}/commands/{path}/# will be written to __commands.jsonl
        // Use event_path (e.g., "echo") rather than UUID for topic matching
        info!(
            "Starting command listener for sandbox process at path: {}",
            event_path
        );
        spawn_command_listener(
            mqtt_client.clone(),
            workspace.clone(),
            event_path,
            directory.clone(),
        );
    }

    // Wait for child to exit OR signal
    let exit_code = tokio::select! {
        status = child.wait() => {
            match status {
                Ok(s) => {
                    let code = s.code().unwrap_or(1) as u8;
                    info!("Command exited with code: {}", code);
                    code
                }
                Err(e) => {
                    error!("Failed to wait for command: {}", e);
                    1
                }
            }
        }
        _ = async {
            // Handle both SIGINT (Ctrl+C) and SIGTERM
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
                let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");
                tokio::select! {
                    _ = sigterm.recv() => info!("Received SIGTERM"),
                    _ = sigint.recv() => info!("Received SIGINT"),
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c().await.ok();
            }
        } => {
            info!("Terminating child process...");
            // Try to kill the child and its descendants gracefully
            #[cfg(unix)]
            {
                // Send SIGTERM to entire process group (negative PID)
                if let Some(pid) = child.id() {
                    info!("Sending SIGTERM to process group {}", pid);
                    unsafe {
                        // Negative PID kills the entire process group
                        libc::kill(-(pid as i32), libc::SIGTERM);
                    }
                }
                // Wait briefly for graceful shutdown
                tokio::select! {
                    _ = child.wait() => {
                        info!("Child process exited gracefully");
                    }
                    _ = sleep(Duration::from_secs(5)) => {
                        // Force kill the process group after timeout
                        if let Some(pid) = child.id() {
                            info!("Force killing process group {}", pid);
                            unsafe {
                                libc::kill(-(pid as i32), libc::SIGKILL);
                            }
                        }
                        let _ = child.kill().await;
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = child.kill().await;
            }
            130 // Standard exit code for Ctrl+C
        }
    };

    info!("Shutting down sync...");

    // Cancel all tasks
    if let Some(handle) = watcher_handle {
        handle.abort();
    }
    if let Some(handle) = subscription_handle {
        handle.abort();
    }
    dir_event_handle.abort();

    // Abort shadow tracking tasks
    #[cfg(unix)]
    {
        if let Some(handle) = shadow_watcher_handle {
            handle.abort();
        }
        if let Some(handle) = shadow_handler_handle {
            handle.abort();
        }
        if let Some(handle) = shadow_gc_handle {
            handle.abort();
        }
    }

    // Abort all per-file sync tasks
    {
        let states = file_states.read().await;
        for file_state in states.values() {
            for handle in &file_state.task_handles {
                handle.abort();
            }
        }
    }

    // Save state file on shutdown
    info!("Saving state file...");
    {
        let sf = shared_state_file.read().await;
        if let Err(e) = sf.save(&state_file_path).await {
            warn!("Failed to save state file on shutdown: {}", e);
        } else {
            info!("Saved state file with {} tracked files", sf.files.len());
        }
    }

    info!("Goodbye!");
    Ok(exit_code)
}

/// Clean up stale sandbox directories from previous runs.
/// This handles directories left behind when a process was killed with SIGKILL
/// (which doesn't allow cleanup code to run).
fn cleanup_stale_sandboxes() {
    let temp_dir = std::env::temp_dir();

    // Look for directories matching our sandbox pattern
    let entries = match std::fs::read_dir(&temp_dir) {
        Ok(entries) => entries,
        Err(e) => {
            warn!(
                "Failed to read temp directory for stale sandbox cleanup: {}",
                e
            );
            return;
        }
    };

    let mut cleaned = 0;

    for entry in entries.flatten() {
        let path = entry.path();

        // Only process directories with our prefix
        if !path.is_dir() {
            continue;
        }

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        if !name.starts_with("commonplace-sandbox-") {
            continue;
        }

        // Check if this sandbox is still active by reading its PID file
        // Only clean up if we can confirm the owning process is dead
        let pid_file = path.join(".pid");
        let can_cleanup = if pid_file.exists() {
            // PID file exists - check if process is still running
            match std::fs::read_to_string(&pid_file) {
                Ok(pid_str) => match pid_str.trim().parse::<u32>() {
                    Ok(pid) => !is_process_running(pid), // Clean up only if process is dead
                    Err(_) => false,                     // Invalid PID, don't clean up to be safe
                },
                Err(_) => false, // Can't read PID file, don't clean up to be safe
            }
        } else {
            // No PID file - this could be a legacy sandbox or one where PID write failed
            // Be conservative: don't delete directories without PID files
            // They'll be cleaned up manually or when someone adds a PID file
            false
        };

        if !can_cleanup {
            continue;
        }

        // PID file exists and process is confirmed dead - safe to clean up
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {
                info!("Cleaned up stale sandbox: {}", path.display());
                cleaned += 1;
            }
            Err(e) => {
                // Directory might be in use by another process, that's fine
                debug!("Could not remove stale sandbox {}: {}", path.display(), e);
            }
        }
    }

    if cleaned > 0 {
        info!("Cleaned up {} stale sandbox directories", cleaned);
    }
}

/// Run in log-listener mode: subscribe to stdout/stderr events at another path
/// and write them to a log file in this sandbox.
#[allow(clippy::too_many_arguments)]
async fn run_log_listener_mode(
    _client: Client,
    _server: String,
    _fs_root_id: String,
    directory: PathBuf,
    _options: ScanOptions,
    _initial_sync_strategy: String,
    _use_paths: bool,
    _push_only: bool,
    _pull_only: bool,
    _shadow_dir: String,
    process_name: Option<String>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    listen_path: String,
) -> Result<u8, Box<dyn std::error::Error + Send + Sync>> {
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;

    // Derive process name from listen_path if not provided
    let exec_name = process_name.unwrap_or_else(|| {
        // Use the last component of the listen path
        listen_path
            .split('/')
            .rfind(|s| !s.is_empty())
            .unwrap_or("log-listener")
            .to_string()
    });

    // Normalize listen_path by stripping leading slash (topic paths don't use leading slashes)
    let normalized_path = listen_path.trim_start_matches('/').to_string();

    info!(
        "Starting log-listener mode: listening to {} in sandbox {}",
        normalized_path,
        directory.display()
    );

    // Set up the log file path
    let log_file_path = directory.join(format!("{}.log", exec_name));

    // Subscribe to stdout and stderr events at the listened path
    let stdout_topic = Topic::events(&workspace, &normalized_path, "stdout");
    let stderr_topic = Topic::events(&workspace, &normalized_path, "stderr");

    info!(
        "Subscribing to events: {} and {}",
        stdout_topic.to_topic_string(),
        stderr_topic.to_topic_string()
    );

    // Subscribe to both topics
    if let Err(e) = mqtt_client
        .subscribe(&stdout_topic.to_topic_string(), QoS::AtLeastOnce)
        .await
    {
        error!("Failed to subscribe to stdout events: {}", e);
        return Err(format!("Failed to subscribe: {}", e).into());
    }
    if let Err(e) = mqtt_client
        .subscribe(&stderr_topic.to_topic_string(), QoS::AtLeastOnce)
        .await
    {
        error!("Failed to subscribe to stderr events: {}", e);
        return Err(format!("Failed to subscribe: {}", e).into());
    }

    // Spawn the MQTT event loop
    let mqtt_for_loop = mqtt_client.clone();
    tokio::spawn(async move {
        let _ = mqtt_for_loop.run_event_loop().await;
    });

    // Get a receiver for incoming messages
    let mut message_rx = mqtt_client.subscribe_messages();

    // Open the log file for appending
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .await?;

    info!("Log file: {}", log_file_path.display());

    // Topic prefixes for matching
    let stdout_prefix = stdout_topic.to_topic_string();
    let stderr_prefix = stderr_topic.to_topic_string();

    // Process events until signaled to stop
    let exit_code = tokio::select! {
        _ = async {
            while let Some(msg) = recv_broadcast(&mut message_rx, "log-listener", None::<fn(u64)>).await {
                // Determine if this is stdout or stderr
                let stream_type = if msg.topic == stdout_prefix {
                    "stdout"
                } else if msg.topic == stderr_prefix {
                    "stderr"
                } else {
                    continue;
                };

                // Parse the line from payload
                let line = match String::from_utf8(msg.payload.clone()) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                // Write to log file with timestamp and stream type
                let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                let log_line = format!("[{}] [{}] {}\n", timestamp, stream_type, line);

                if let Err(e) = log_file.write_all(log_line.as_bytes()).await {
                    warn!("Failed to write to log file: {}", e);
                }
                if let Err(e) = log_file.flush().await {
                    warn!("Failed to flush log file: {}", e);
                }

                // Also print to console for debugging
                println!("[{}] [{}] {}", listen_path, stream_type, line);
            }
        } => {
            info!("Message stream ended");
            0
        }
        _ = async {
            // Handle both SIGINT (Ctrl+C) and SIGTERM
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
                let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");
                tokio::select! {
                    _ = sigterm.recv() => info!("Received SIGTERM"),
                    _ = sigint.recv() => info!("Received SIGINT"),
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c().await.ok();
            }
        } => {
            info!("Shutting down log-listener...");
            0
        }
    };

    info!("Log-listener stopped");
    Ok(exit_code)
}
