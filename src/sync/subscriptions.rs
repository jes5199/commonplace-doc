//! SSE and MQTT subscription tasks for directory sync.
//!
//! This module contains the task functions that subscribe to SSE and MQTT
//! events for directory-level synchronization.

use crate::events::recv_broadcast;
use crate::mqtt::{MqttClient, Topic};
use crate::sync::dir_sync::{
    create_subdir_nested_directories, handle_schema_change_with_dedup, handle_subdir_new_files,
    handle_subdir_schema_cleanup,
};
use crate::sync::subdir_spawn::{spawn_subdir_watchers, SubdirSpawnParams, SubdirTransport};
use crate::sync::{encode_node_id, FileSyncState};
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

// ============================================================================
// Shared Helpers
// ============================================================================

/// Handle a subdirectory schema edit event (shared by SSE and MQTT paths).
///
/// This performs the common operations when a subdirectory schema changes:
/// 1. Cleanup deleted files and orphaned directories
/// 2. Sync NEW files from server
/// 3. Create directories for new node-backed subdirectories
#[allow(clippy::too_many_arguments)]
pub async fn handle_subdir_edit(
    client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    subdir_full_path: &Path,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shared_state_file: Option<&crate::sync::SharedStateFile>,
    author: &str,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    log_prefix: &str,
) {
    // First, cleanup deleted files and orphaned directories
    match handle_subdir_schema_cleanup(
        client,
        server,
        subdir_node_id,
        subdir_path,
        subdir_full_path,
        directory,
        file_states,
    )
    .await
    {
        Ok(()) => {
            debug!(
                "{}: Subdir {} schema cleanup completed",
                log_prefix, subdir_path
            );
        }
        Err(e) => {
            warn!(
                "{}: Failed to handle subdir {} schema cleanup: {}",
                log_prefix, subdir_path, e
            );
        }
    }

    // Then, sync NEW files from server
    match handle_subdir_new_files(
        client,
        server,
        subdir_node_id,
        subdir_path,
        subdir_full_path,
        directory,
        file_states,
        use_paths,
        push_only,
        pull_only,
        shared_state_file,
        author,
        #[cfg(unix)]
        inode_tracker,
    )
    .await
    {
        Ok(()) => {
            debug!(
                "{}: Subdir {} new files sync completed",
                log_prefix, subdir_path
            );
        }
        Err(e) => {
            warn!(
                "{}: Failed to sync new files for subdir {}: {}",
                log_prefix, subdir_path, e
            );
        }
    }

    // Also create directories for any NEW node-backed subdirectories
    if let Err(e) =
        create_subdir_nested_directories(client, server, subdir_node_id, subdir_full_path).await
    {
        warn!(
            "{}: Failed to create nested directories for subdir {}: {}",
            log_prefix, subdir_path, e
        );
    }
}

/// Collect newly discovered subdirectories that aren't already being watched.
///
/// Returns a list of (path, node_id) tuples for subdirs that should be spawned.
/// Updates the watched_subdirs set with the new entries.
pub async fn collect_new_subdirs(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    watched_subdirs: &Arc<RwLock<HashSet<String>>>,
) -> Vec<(String, String)> {
    let all_subdirs = crate::sync::get_all_node_backed_dir_ids(client, server, fs_root_id).await;

    let mut watched = watched_subdirs.write().await;
    all_subdirs
        .into_iter()
        .filter(|(_, node_id)| {
            if watched.contains(node_id) {
                false
            } else {
                watched.insert(node_id.clone());
                true
            }
        })
        .collect()
}

// ============================================================================
// SSE Tasks
// ============================================================================

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
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
    written_schemas: Option<crate::sync::WrittenSchemas>,
    initial_schema_cid: Option<String>,
    shared_state_file: Option<crate::sync::SharedStateFile>,
) {
    // fs-root schema subscription always uses ID-based API
    let sse_url = format!("{}/sse/docs/{}", server, encode_node_id(&fs_root_id));

    // Track last processed schema to prevent redundant processing
    let mut last_schema_hash: Option<String> = None;
    // Track last applied schema CID for ancestry checking.
    // Initialize with the CID from initial sync to prevent pulling stale server content.
    let mut last_schema_cid: Option<String> = initial_schema_cid;

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
                            // Use content-based deduplication and ancestry checking
                            match handle_schema_change_with_dedup(
                                &client,
                                &server,
                                &fs_root_id,
                                &directory,
                                &file_states,
                                true, // spawn_tasks: true for runtime schema changes
                                use_paths,
                                &mut last_schema_hash,
                                &mut last_schema_cid,
                                push_only,
                                pull_only,
                                &author,
                                #[cfg(unix)]
                                inode_tracker.clone(),
                                written_schemas.as_ref(),
                                shared_state_file.as_ref(),
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
                                    shared_state_file: shared_state_file.clone(),
                                    author: author.clone(),
                                    #[cfg(unix)]
                                    inode_tracker: inode_tracker.clone(),
                                    watched_subdirs: watched_subdirs.clone(),
                                };
                                spawn_subdir_watchers(&params, SubdirTransport::Sse).await;
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

/// Helper to spawn SSE tasks for subdirectories.
/// This is separate from subdir_sse_task to avoid recursive type issues with tokio::spawn.
#[allow(clippy::too_many_arguments)]
pub fn spawn_subdir_sse_task(
    client: Client,
    server: String,
    fs_root_id: String,
    subdir_path: String,
    subdir_node_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shared_state_file: Option<crate::sync::SharedStateFile>,
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
) {
    tokio::spawn(subdir_sse_task(
        client,
        server,
        fs_root_id,
        subdir_path,
        subdir_node_id,
        directory,
        file_states,
        use_paths,
        push_only,
        pull_only,
        shared_state_file,
        author,
        #[cfg(unix)]
        inode_tracker,
        watched_subdirs,
    ));
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
    fs_root_id: String,
    subdir_path: String,
    subdir_node_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shared_state_file: Option<crate::sync::SharedStateFile>,
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
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

                            // Use shared helper for edit handling
                            handle_subdir_edit(
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
                                shared_state_file.as_ref(),
                                &author,
                                #[cfg(unix)]
                                inode_tracker.clone(),
                                "SSE",
                            )
                            .await;

                            // Check for newly discovered nested node-backed subdirs and spawn SSE tasks
                            if !push_only {
                                let subdirs_to_spawn = collect_new_subdirs(
                                    &client,
                                    &server,
                                    &fs_root_id,
                                    &watched_subdirs,
                                )
                                .await;

                                for (nested_subdir_path, nested_subdir_node_id) in subdirs_to_spawn
                                {
                                    info!(
                                        "Spawning SSE task for newly discovered nested subdir: {} ({})",
                                        nested_subdir_path, nested_subdir_node_id
                                    );
                                    spawn_subdir_sse_task(
                                        client.clone(),
                                        server.clone(),
                                        fs_root_id.clone(),
                                        nested_subdir_path,
                                        nested_subdir_node_id,
                                        directory.clone(),
                                        file_states.clone(),
                                        use_paths,
                                        push_only,
                                        pull_only,
                                        shared_state_file.clone(),
                                        author.clone(),
                                        #[cfg(unix)]
                                        inode_tracker.clone(),
                                        watched_subdirs.clone(),
                                    );
                                }
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
/// This task subscribes to:
/// 1. The fs-root document's edits topic (for schema changes)
/// 2. All file UUIDs in the schema (for file content changes)
///
/// When the schema changes, subscriptions are updated to match the new set of UUIDs.
/// This enables real-time sync of file content, including linked files that share UUIDs.
///
/// When new node-backed subdirectories are discovered, this task dynamically
/// spawns tasks for them to watch their schemas.
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
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    written_schemas: Option<crate::sync::WrittenSchemas>,
    initial_schema_cid: Option<String>,
    shared_state_file: Option<crate::sync::SharedStateFile>,
    initial_uuid_map: HashMap<String, String>,
) {
    // Subscribe to edits for the fs-root document (schema changes)
    let fs_root_topic = Topic::edits(&workspace, &fs_root_id).to_topic_string();

    info!(
        "Subscribing to MQTT edits for fs-root {} at topic: {}",
        fs_root_id, fs_root_topic
    );

    if let Err(e) = mqtt_client
        .subscribe(&fs_root_topic, QoS::AtLeastOnce)
        .await
    {
        error!("Failed to subscribe to MQTT edits topic: {}", e);
        return;
    }

    // Build reverse map (uuid -> paths) and subscribe to all file UUIDs
    let mut uuid_to_paths = crate::sync::build_uuid_to_paths_map(&initial_uuid_map);
    let mut subscribed_uuids: HashSet<String> = HashSet::new();

    // Subscribe to all file UUIDs from the initial schema
    for uuid in uuid_to_paths.keys() {
        let topic = Topic::edits(&workspace, uuid).to_topic_string();
        if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
            warn!("Failed to subscribe to file UUID {}: {}", uuid, e);
        } else {
            subscribed_uuids.insert(uuid.clone());
            debug!("Subscribed to file UUID: {}", uuid);
        }
    }

    info!(
        "Subscribed to {} file UUIDs for content sync",
        subscribed_uuids.len()
    );

    // Get a receiver for incoming messages
    let mut message_rx = mqtt_client.subscribe_messages();

    // Track last processed schema to prevent redundant processing
    let mut last_schema_hash: Option<String> = None;
    // Track last applied schema CID for ancestry checking.
    // Initialize with the CID from initial sync to prevent pulling stale server content.
    let mut last_schema_cid: Option<String> = initial_schema_cid;

    // Process incoming MQTT messages
    while let Some(msg) = recv_broadcast(&mut message_rx, "MQTT directory receiver").await {
        // Parse the topic to extract the document ID
        let doc_id = match Topic::parse(&msg.topic, &workspace) {
            Ok(parsed) => parsed.path,
            Err(_) => {
                debug!("Ignoring message with unparseable topic: {}", msg.topic);
                continue;
            }
        };

        if doc_id == fs_root_id {
            // This is a schema change for the fs-root
            debug!(
                "MQTT edit received for fs-root: {} bytes",
                msg.payload.len()
            );

            // Use content-based deduplication and ancestry checking
            match handle_schema_change_with_dedup(
                &http_client,
                &server,
                &fs_root_id,
                &directory,
                &file_states,
                true, // spawn_tasks: true for runtime schema changes
                use_paths,
                &mut last_schema_hash,
                &mut last_schema_cid,
                push_only,
                pull_only,
                &author,
                #[cfg(unix)]
                inode_tracker.clone(),
                written_schemas.as_ref(),
                shared_state_file.as_ref(),
            )
            .await
            {
                Ok(true) => {
                    debug!("MQTT: Schema change processed successfully");

                    // Schema changed - update UUID subscriptions
                    sync_uuid_subscriptions(
                        &http_client,
                        &server,
                        &fs_root_id,
                        &mqtt_client,
                        &workspace,
                        &mut subscribed_uuids,
                        &mut uuid_to_paths,
                    )
                    .await;
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
                let params = SubdirSpawnParams {
                    client: http_client.clone(),
                    server: server.clone(),
                    fs_root_id: fs_root_id.clone(),
                    directory: directory.clone(),
                    file_states: file_states.clone(),
                    use_paths,
                    push_only,
                    pull_only,
                    shared_state_file: shared_state_file.clone(),
                    author: author.clone(),
                    #[cfg(unix)]
                    inode_tracker: inode_tracker.clone(),
                    watched_subdirs: watched_subdirs.clone(),
                };
                spawn_subdir_watchers(
                    &params,
                    SubdirTransport::Mqtt {
                        client: mqtt_client.clone(),
                        workspace: workspace.clone(),
                    },
                )
                .await;
            }
        } else if let Some(paths) = uuid_to_paths.get(&doc_id) {
            // This is a file content edit - pull and write to local file(s)
            //
            // IMPORTANT: We only process MQTT file edits for linked files (multiple paths
            // sharing the same UUID). For single-path files, the existing per-file SSE tasks
            // handle updates with proper sync state and ancestry checking. Processing MQTT
            // edits for single-path files would bypass those safeguards and risk data loss.
            //
            // Linked files don't have individual SSE tasks (they share a UUID), so MQTT is
            // the only way for changes to propagate between them.
            let is_linked_file = paths.len() > 1;
            if is_linked_file && (pull_only || !push_only) {
                debug!(
                    "MQTT edit received for linked file UUID {}: {} bytes, {} local path(s)",
                    doc_id,
                    msg.payload.len(),
                    paths.len()
                );

                // Fetch latest content from server and write to all linked local files
                if let Err(e) = handle_file_uuid_edit(
                    &http_client,
                    &server,
                    &doc_id,
                    paths,
                    &directory,
                    use_paths,
                )
                .await
                {
                    warn!(
                        "Failed to handle linked file UUID edit for {}: {}",
                        doc_id, e
                    );
                }
            } else if !is_linked_file {
                // Single-path file - let the per-file SSE task handle it
                debug!(
                    "Ignoring MQTT edit for single-path file UUID {} (handled by SSE)",
                    doc_id
                );
            }
        } else {
            // Unknown document ID - might be a subdir schema, ignore
            debug!("Ignoring edit for unknown document: {}", doc_id);
        }
    }

    info!("MQTT directory message channel closed");
}

/// Sync UUID subscriptions after a schema change.
///
/// Compares the current subscribed UUIDs with the new UUID map from the schema,
/// subscribing to new UUIDs and unsubscribing from removed ones.
///
/// If the schema fetch fails, subscriptions are left unchanged to avoid
/// accidentally unsubscribing from all file topics due to transient errors.
async fn sync_uuid_subscriptions(
    http_client: &Client,
    server: &str,
    fs_root_id: &str,
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    subscribed_uuids: &mut HashSet<String>,
    uuid_to_paths: &mut crate::sync::UuidToPathsMap,
) {
    // Fetch the current UUID map from the schema, with status to detect failures
    let (new_uuid_map, fetch_succeeded) =
        crate::sync::uuid_map::build_uuid_map_recursive_with_status(
            http_client,
            server,
            fs_root_id,
        )
        .await;

    // If fetch failed, don't modify subscriptions - we might accidentally unsubscribe from all topics
    if !fetch_succeeded {
        warn!("Schema fetch failed during subscription sync - keeping existing subscriptions");
        return;
    }

    // Build new reverse map
    let new_uuid_to_paths = crate::sync::build_uuid_to_paths_map(&new_uuid_map);
    let new_uuids: HashSet<String> = new_uuid_to_paths.keys().cloned().collect();

    // Find UUIDs to add and remove
    let to_add: Vec<String> = new_uuids.difference(subscribed_uuids).cloned().collect();
    let to_remove: Vec<String> = subscribed_uuids.difference(&new_uuids).cloned().collect();

    // Subscribe to new UUIDs
    for uuid in &to_add {
        let topic = Topic::edits(workspace, uuid).to_topic_string();
        if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
            warn!("Failed to subscribe to new file UUID {}: {}", uuid, e);
        } else {
            subscribed_uuids.insert(uuid.clone());
            debug!("Subscribed to new file UUID: {}", uuid);
        }
    }

    // Unsubscribe from removed UUIDs
    for uuid in &to_remove {
        let topic = Topic::edits(workspace, uuid).to_topic_string();
        if let Err(e) = mqtt_client.unsubscribe(&topic).await {
            warn!("Failed to unsubscribe from file UUID {}: {}", uuid, e);
        } else {
            subscribed_uuids.remove(uuid);
            debug!("Unsubscribed from removed file UUID: {}", uuid);
        }
    }

    // Update the reverse map
    *uuid_to_paths = new_uuid_to_paths;

    if !to_add.is_empty() || !to_remove.is_empty() {
        info!(
            "UUID subscriptions updated: +{} -{} (total: {})",
            to_add.len(),
            to_remove.len(),
            subscribed_uuids.len()
        );
    }
}

/// Handle a file UUID edit by fetching content from server and writing to local files.
async fn handle_file_uuid_edit(
    http_client: &Client,
    server: &str,
    uuid: &str,
    paths: &[String],
    directory: &Path,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::sync::fetch_head;

    // Fetch the latest content from the server
    let head = fetch_head(http_client, server, uuid, use_paths)
        .await?
        .ok_or_else(|| format!("Document {} not found", uuid))?;

    // Write content to all local paths that share this UUID
    for rel_path in paths {
        let file_path = directory.join(rel_path);

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Decode if binary (base64)
        let content_bytes = if crate::sync::looks_like_base64_binary(&head.content) {
            use base64::{engine::general_purpose::STANDARD, Engine};
            match STANDARD.decode(&head.content) {
                Ok(decoded) if crate::sync::is_binary_content(&decoded) => decoded,
                _ => head.content.as_bytes().to_vec(),
            }
        } else {
            head.content.as_bytes().to_vec()
        };

        tokio::fs::write(&file_path, &content_bytes).await?;
        debug!("Wrote {} bytes to {}", content_bytes.len(), rel_path);
    }

    Ok(())
}

/// Helper to spawn MQTT tasks for subdirectories.
/// This is separate from subdir_mqtt_task to avoid recursive type issues with tokio::spawn.
#[allow(clippy::too_many_arguments)]
pub fn spawn_subdir_mqtt_task(
    http_client: Client,
    server: String,
    fs_root_id: String,
    subdir_path: String,
    subdir_node_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shared_state_file: Option<crate::sync::SharedStateFile>,
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
) {
    tokio::spawn(subdir_mqtt_task(
        http_client,
        server,
        fs_root_id,
        subdir_path,
        subdir_node_id,
        directory,
        file_states,
        use_paths,
        push_only,
        pull_only,
        shared_state_file,
        author,
        #[cfg(unix)]
        inode_tracker,
        mqtt_client,
        workspace,
        watched_subdirs,
    ));
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
    fs_root_id: String,
    subdir_path: String,
    subdir_node_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shared_state_file: Option<crate::sync::SharedStateFile>,
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    watched_subdirs: Arc<RwLock<HashSet<String>>>,
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
    let context = format!("MQTT subdir {} receiver", subdir_path);
    while let Some(msg) = recv_broadcast(&mut message_rx, &context).await {
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

            // Use shared helper for edit handling
            handle_subdir_edit(
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
                shared_state_file.as_ref(),
                &author,
                #[cfg(unix)]
                inode_tracker.clone(),
                "MQTT",
            )
            .await;

            // Check for newly discovered nested node-backed subdirs and spawn MQTT tasks
            if !push_only {
                let subdirs_to_spawn =
                    collect_new_subdirs(&http_client, &server, &fs_root_id, &watched_subdirs).await;

                for (nested_subdir_path, nested_subdir_node_id) in subdirs_to_spawn {
                    info!(
                        "MQTT: Spawning task for newly discovered nested subdir: {} ({})",
                        nested_subdir_path, nested_subdir_node_id
                    );
                    spawn_subdir_mqtt_task(
                        http_client.clone(),
                        server.clone(),
                        fs_root_id.clone(),
                        nested_subdir_path,
                        nested_subdir_node_id,
                        directory.clone(),
                        file_states.clone(),
                        use_paths,
                        push_only,
                        pull_only,
                        shared_state_file.clone(),
                        author.clone(),
                        #[cfg(unix)]
                        inode_tracker.clone(),
                        mqtt_client.clone(),
                        workspace.clone(),
                        watched_subdirs.clone(),
                    );
                }
            }
        }
    }

    info!("MQTT message channel closed for subdir {}", subdir_path);
}
