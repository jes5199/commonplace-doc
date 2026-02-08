//! Subdirectory MQTT subscription task.
//!
//! Contains `subdir_mqtt_task` which watches a node-backed subdirectory
//! for schema changes and all file UUIDs for content changes.

use super::recovery::{
    resync_subdir_schema_from_server, resync_subscribed_files, sync_schema_via_cyan,
};
use super::{collect_new_subdirs, handle_subdir_edit, CrdtFileSyncContext};
use crate::events::{recv_broadcast_with_lag, BroadcastRecvResult};
use crate::mqtt::{MqttClient, Topic};
use crate::sync::dir_sync::{apply_schema_update_to_state, decode_schema_from_mqtt_payload};
use crate::sync::FileSyncState;
use reqwest::Client;
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
    crdt_context: Option<CrdtFileSyncContext>,
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
        crdt_context,
    ));
}

/// MQTT task for a node-backed subdirectory.
///
/// This task subscribes to:
/// 1. The subdirectory's edits topic via MQTT (for schema changes)
/// 2. All file UUIDs in the subdirectory's schema (for file content changes)
///
/// When the subdirectory's schema changes, it:
/// 1. Fetches and writes the updated schema to the local .commonplace.json file
/// 2. Deletes local files that were removed from the server schema
/// 3. Cleans up orphaned directories that no longer exist in the schema
/// 4. Syncs NEW files that were added to the server schema
/// 5. Updates file UUID subscriptions to match the new schema
///
/// When `crdt_context` is provided, CRDT sync tasks are spawned for new files.
///
/// This is the primary subdir sync task using MQTT.
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
    crdt_context: Option<CrdtFileSyncContext>,
) {
    // Subscribe to edits for the subdirectory document (schema changes)
    let edits_topic = Topic::edits(&workspace, &subdir_node_id);
    let schema_topic_str = edits_topic.to_topic_string();
    #[cfg(unix)]
    let inode_tracker_for_crdt = inode_tracker.clone();
    #[cfg(not(unix))]
    let inode_tracker_for_crdt = None;

    // CRITICAL: Create the broadcast receiver BEFORE subscribing.
    // This ensures we receive retained messages.
    let mut message_rx = mqtt_client.subscribe_messages();

    info!(
        "Subscribing to MQTT edits for subdir {} at topic: {}",
        subdir_path, schema_topic_str
    );

    if let Err(e) = mqtt_client
        .subscribe(&schema_topic_str, QoS::AtLeastOnce)
        .await
    {
        error!(
            "Failed to subscribe to MQTT edits topic for subdir {}: {}",
            subdir_path, e
        );
        return;
    }

    // Initialize subdirectory schema CRDT state via cyan sync (CP-uals)
    if let Some(ref ctx) = crdt_context {
        let subdir_full_path_init = directory.join(&subdir_path);
        if let Ok(subdir_uuid) = Uuid::parse_str(&subdir_node_id) {
            match ctx
                .subdir_cache
                .get_or_load(&subdir_full_path_init, subdir_uuid)
                .await
            {
                Ok(subdir_state) => {
                    let needs_init = {
                        let state = subdir_state.read().await;
                        state.schema.needs_server_init()
                    };

                    if needs_init {
                        let sync_client_id = Uuid::new_v4().to_string();
                        let mut state = subdir_state.write().await;
                        let initialized = sync_schema_via_cyan(
                            &mqtt_client,
                            &workspace,
                            &subdir_node_id,
                            &sync_client_id,
                            &mut state.schema,
                        )
                        .await;

                        if initialized {
                            debug!(
                                "[CYAN-SYNC] Subdir schema CRDT initialized for {} ({})",
                                subdir_path, subdir_node_id
                            );
                        } else {
                            warn!(
                                "[CYAN-SYNC] Failed to init subdir schema for {} — will use MQTT retained messages",
                                subdir_path
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "[CYAN-SYNC] Failed to load subdir state for {}: {}",
                        subdir_path, e
                    );
                }
            }
        }
    }

    // Build initial UUID map from the subdirectory's schema AND write schemas locally.
    // This ensures local .commonplace.json files exist with correct UUIDs from server.
    // The subdir_path is the prefix for paths within this subdirectory
    let subdir_full_path = directory.join(&subdir_path);
    let (initial_uuid_map, _) = crate::sync::uuid_map::build_uuid_map_and_write_schemas(
        &http_client,
        &server,
        &subdir_node_id,
        &subdir_full_path,
        None,
    )
    .await;

    // Build reverse map (uuid -> paths) and subscribe to all file UUIDs
    // Paths need to be prefixed with subdir_path since they're relative to the subdir
    let prefixed_uuid_map: HashMap<String, String> = initial_uuid_map
        .into_iter()
        .map(|(path, uuid)| {
            let full_path = if subdir_path.is_empty() {
                path
            } else {
                format!("{}/{}", subdir_path, path)
            };
            (full_path, uuid)
        })
        .collect();

    let mut uuid_to_paths = crate::sync::build_uuid_to_paths_map(&prefixed_uuid_map);
    let mut subscribed_uuids: HashSet<String> = HashSet::new();

    // Subscribe to all file UUIDs from the initial schema
    for uuid in uuid_to_paths.keys() {
        let topic = Topic::edits(&workspace, uuid).to_topic_string();
        if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
            warn!(
                "Subdir {}: Failed to subscribe to file UUID {}: {}",
                subdir_path, uuid, e
            );
        } else {
            subscribed_uuids.insert(uuid.clone());
            debug!("Subdir {}: Subscribed to file UUID: {}", subdir_path, uuid);
        }
    }

    info!(
        "Subdir {}: Subscribed to {} file UUIDs for content sync",
        subdir_path,
        subscribed_uuids.len()
    );

    // Process incoming MQTT messages with lag detection
    let context = format!("MQTT subdir {} receiver", subdir_path);
    info!(
        "[SANDBOX-TRACE] subdir_mqtt_task READY for subdir={} node_id={} subscribed_uuids={}",
        subdir_path,
        subdir_node_id,
        subscribed_uuids.len()
    );
    loop {
        let msg = match recv_broadcast_with_lag(&mut message_rx, &context).await {
            BroadcastRecvResult::Message(m) => m,
            BroadcastRecvResult::Lagged {
                missed_count,
                next_message,
            } => {
                // For subdir watchers, resync by triggering a schema refresh and resyncing files
                debug!(
                    "MQTT subdir {} receiver lagged by {} messages, triggering resync",
                    subdir_path, missed_count
                );
                // Force a schema refresh by calling handle_subdir_edit
                // Resync fetches from HTTP (no MQTT payload available)
                let subdir_full_path = directory.join(&subdir_path);
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
                    "MQTT-resync",
                    crdt_context.as_ref(),
                    None, // No MQTT schema during resync, fetch from HTTP
                    std::collections::HashSet::new(), // No CRDT deletions during HTTP resync
                )
                .await;

                // Also resync subscribed files to recover missed edits
                resync_subscribed_files(
                    &http_client,
                    &server,
                    &subscribed_uuids,
                    &uuid_to_paths,
                    &directory,
                    use_paths,
                    &file_states,
                    None, // No CRDT context for subdirs currently
                    inode_tracker_for_crdt.clone(),
                    &author,
                )
                .await;

                // Resync subdirectory schema CRDT state to recover missed schema updates.
                // Without this, the local subdirectory Y.Doc can drift from server state.
                if let Some(ref ctx) = crdt_context {
                    resync_subdir_schema_from_server(
                        &subdir_node_id,
                        &subdir_path,
                        ctx,
                        &directory,
                    )
                    .await;
                }

                match next_message {
                    Some(m) => m,
                    None => break, // Channel closed
                }
            }
            BroadcastRecvResult::Closed => break,
        };

        // Parse the topic to extract the document ID
        let doc_id = match Topic::parse(&msg.topic, &workspace) {
            Ok(parsed) => parsed.path,
            Err(_) => {
                debug!(
                    "Subdir {}: Ignoring message with unparseable topic: {}",
                    subdir_path, msg.topic
                );
                continue;
            }
        };

        if doc_id == subdir_node_id {
            // This is a schema change for the subdirectory
            info!(
                "[SANDBOX-TRACE] subdir_mqtt_task RECV schema_change subdir={} doc_id={} payload_len={}",
                subdir_path,
                doc_id,
                msg.payload.len()
            );
            debug!(
                "MQTT edit received for subdir {} schema: {} bytes",
                subdir_path,
                msg.payload.len()
            );

            // Subdirectory schema changed - handle cleanup and sync new files
            info!(
                "MQTT: Subdir {} schema changed, triggering sync",
                subdir_path
            );
            let subdir_full_path = directory.join(&subdir_path);

            // Apply MQTT schema update to persistent CRDT state.
            // This properly handles both ADD and DELETE operations by maintaining
            // the Y.Doc state across updates. DELETE operations now work correctly
            // because the delta is applied to the existing doc (which has entries)
            // rather than a fresh empty doc.
            //
            // IMPORTANT: We must use the subdirectory's cached state, NOT the root
            // crdt_state. Using the root state would corrupt it by merging subdir
            // schema edits into the root document.
            let mqtt_schema = if let Some(ref ctx) = crdt_context {
                // Parse subdir_node_id as UUID for cache lookup
                let subdir_uuid = match Uuid::parse_str(&subdir_node_id) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(
                            "Failed to parse subdir_node_id {} as UUID: {}, falling back to HTTP",
                            subdir_node_id, e
                        );
                        // Fall through to None result
                        Uuid::nil()
                    }
                };

                if subdir_uuid.is_nil() {
                    None
                } else {
                    // Get the subdirectory's cached state (NOT the root state!)
                    match ctx
                        .subdir_cache
                        .get_or_load(&subdir_full_path, subdir_uuid)
                        .await
                    {
                        Ok(subdir_state) => {
                            let mut state = subdir_state.write().await;
                            let result =
                                apply_schema_update_to_state(&mut state.schema, &msg.payload);
                            if let Some(ref r) = result {
                                let entry_count =
                                    if let Some(crate::fs::Entry::Dir(ref root)) = r.schema.root {
                                        root.entries.as_ref().map(|e| e.len()).unwrap_or(0)
                                    } else {
                                        0
                                    };
                                info!(
                                    "[SANDBOX-TRACE] Applied schema update to subdir cached state for subdir={} entry_count={}",
                                    subdir_path, entry_count
                                );

                                // Persist the updated CRDT state to disk so it survives restarts
                                if let Err(e) = state.save(&subdir_full_path).await {
                                    warn!(
                                        "Failed to save subdir CRDT state for {}: {}",
                                        subdir_path, e
                                    );
                                } else {
                                    debug!(
                                        "[SANDBOX-TRACE] Persisted schema CRDT state for subdir={}",
                                        subdir_path
                                    );
                                }
                            } else {
                                debug!(
                                    "Failed to apply schema update for subdir={}, will fetch from HTTP",
                                    subdir_path
                                );
                            }
                            result.map(|r| (r.schema, r.schema_json, r.deleted_entries))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load subdir state for {}: {}, falling back to HTTP",
                                subdir_path, e
                            );
                            None
                        }
                    }
                }
            } else {
                // No CRDT context - try loading state from disk to properly handle DELETEs.
                // The stateless decode (Doc::new()) breaks DELETE operations because the
                // fresh Y.Doc doesn't have the server's operation history - deletions
                // reference specific client IDs that don't exist in the fresh doc.
                // See CP-hwg2 for details.
                let subdir_uuid = Uuid::parse_str(&subdir_node_id).ok();

                let result = if let Some(uuid) = subdir_uuid {
                    // Try to load state from disk
                    match crate::sync::DirectorySyncState::load_or_create(&subdir_full_path, uuid)
                        .await
                    {
                        Ok(mut state) => {
                            // Use the correct pattern: load state first, then apply update
                            let schema_result =
                                apply_schema_update_to_state(&mut state.schema, &msg.payload);
                            if let Some(ref r) = schema_result {
                                let entry_count =
                                    if let Some(crate::fs::Entry::Dir(ref root)) = r.schema.root {
                                        root.entries.as_ref().map(|e| e.len()).unwrap_or(0)
                                    } else {
                                        0
                                    };
                                info!(
                                    "[SANDBOX-TRACE] Applied schema update to disk-loaded state for subdir={} entry_count={}",
                                    subdir_path, entry_count
                                );

                                // Persist the updated state back to disk
                                if let Err(e) = state.save(&subdir_full_path).await {
                                    warn!(
                                        "Failed to save subdir CRDT state for {}: {}",
                                        subdir_path, e
                                    );
                                }
                            }
                            schema_result.map(|r| (r.schema, r.schema_json, r.deleted_entries))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load subdir state from disk for {}: {}, using stateless decode (DELETE operations may fail - see CP-hwg2)",
                                subdir_path, e
                            );
                            // Fall through to stateless decode.
                            // WARNING: decode_schema_from_mqtt_payload cannot handle DELETE
                            // operations correctly because it uses a fresh Y.Doc. Deletions
                            // will be ignored and must be handled via HTTP fallback.
                            decode_schema_from_mqtt_payload(&msg.payload)
                                .map(|(s, j)| (s, j, std::collections::HashSet::new()))
                        }
                    }
                } else {
                    warn!(
                        "Failed to parse subdir_node_id {} as UUID, using stateless decode (DELETE operations may fail - see CP-hwg2)",
                        subdir_node_id
                    );
                    // WARNING: decode_schema_from_mqtt_payload cannot handle DELETE
                    // operations correctly because it uses a fresh Y.Doc. Deletions
                    // will be ignored and must be handled via HTTP fallback.
                    decode_schema_from_mqtt_payload(&msg.payload)
                        .map(|(s, j)| (s, j, std::collections::HashSet::new()))
                };

                if result.is_none() {
                    debug!(
                        "Failed to decode schema from MQTT payload for subdir={}, will fetch from HTTP",
                        subdir_path
                    );
                }
                result
            };

            // Extract deleted_entries from schema result (CP-seha)
            let (mqtt_schema, crdt_deleted_entries) = match mqtt_schema {
                Some((s, j, deleted)) => (Some((s, j)), deleted),
                None => (None, std::collections::HashSet::new()),
            };

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
                crdt_context.as_ref(),
                mqtt_schema,          // Use MQTT-decoded schema to avoid HTTP race
                crdt_deleted_entries, // Explicit CRDT deletions (CP-seha)
            )
            .await;

            // Schema changed - update UUID subscriptions for this subdirectory
            // IMPORTANT: Skip sync_subdir_uuid_subscriptions when CRDT context is present.
            // In CRDT mode, each file's receive_task_crdt handles its own subscription.
            // The HTTP-based sync_subdir_uuid_subscriptions can race with the server's
            // reconciler, causing UUIDs to be overwritten and subscriptions lost.
            // See CP-1ual for details on this race condition.
            if crdt_context.is_none() {
                sync_subdir_uuid_subscriptions(
                    &http_client,
                    &server,
                    &subdir_node_id,
                    &subdir_path,
                    &mqtt_client,
                    &workspace,
                    &mut subscribed_uuids,
                    &mut uuid_to_paths,
                    &directory,
                    use_paths,
                    &file_states,
                )
                .await;
            }

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
                        crdt_context.clone(),
                    );
                }
            }
        } else if let Some(paths) = uuid_to_paths.get(&doc_id) {
            // This is a file content edit - pull and write to local file(s)
            if pull_only || !push_only {
                info!(
                    "[SANDBOX-TRACE] subdir_mqtt_task RECV file_edit subdir={} uuid={} payload_len={} paths={:?}",
                    subdir_path,
                    doc_id,
                    msg.payload.len(),
                    paths
                );
                debug!(
                    "Subdir {}: MQTT edit received for file UUID {}: {} bytes, {} path(s)",
                    subdir_path,
                    doc_id,
                    msg.payload.len(),
                    paths.len()
                );

                // Try CRDT handling first, fall back to server fetch if needed
                let crdt_handled = if let Some(ref ctx) = crdt_context {
                    if let Ok(edit_msg) = crate::sync::crdt_merge::parse_edit_message(&msg.payload)
                    {
                        // Apply to CRDT state and get content
                        let node_id = match uuid::Uuid::parse_str(&doc_id) {
                            Ok(id) => id,
                            Err(_) => {
                                debug!(
                                    "Subdir {}: Non-UUID document {}, falling back to server fetch",
                                    subdir_path, doc_id
                                );
                                uuid::Uuid::nil() // Skip CRDT handling
                            }
                        };

                        if !node_id.is_nil() {
                            // Use bare filename as key since per-subdir state
                            // scopes correctly (no cross-directory collisions).
                            let file_key = paths
                                .first()
                                .and_then(|p| std::path::Path::new(p.as_str()).file_name())
                                .and_then(|n| n.to_str())
                                .unwrap_or(&doc_id)
                                .to_string();

                            // Load per-subdirectory state instead of root state
                            let subdir_full_path = directory.join(&subdir_path);
                            let subdir_state_arc = match Uuid::parse_str(&subdir_node_id) {
                                Ok(subdir_uuid) => {
                                    match ctx
                                        .subdir_cache
                                        .get_or_load(&subdir_full_path, subdir_uuid)
                                        .await
                                    {
                                        Ok(s) => Some(s),
                                        Err(e) => {
                                            warn!(
                                                "Subdir {}: Failed to load subdir state: {}, falling back to server fetch",
                                                subdir_path, e
                                            );
                                            None
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Subdir {}: Invalid subdir_node_id '{}': {}, falling back to server fetch",
                                        subdir_path, subdir_node_id, e
                                    );
                                    None
                                }
                            };

                            if let Some(subdir_state_arc) = subdir_state_arc {
                                let mut state_guard = subdir_state_arc.write().await;
                                let file_state = state_guard.get_or_create_file(&file_key, node_id);

                                // Check if CRDT state needs initialization.
                                // If so, queue the edit for later processing instead of
                                // trying to merge it now (which would fail or produce
                                // incorrect results without shared history).
                                if file_state.needs_server_init() {
                                    file_state.queue_pending_edit(msg.payload.clone());
                                    info!(
                                    "[SANDBOX-TRACE] subdir_mqtt_task CRDT_QUEUED subdir={} file={} queue_size={}",
                                    subdir_path,
                                    file_key,
                                    file_state.pending_edits.len()
                                );
                                    debug!(
                                    "Subdir {}: CRDT needs init for {}, queued edit (queue size: {})",
                                    subdir_path,
                                    file_key,
                                    file_state.pending_edits.len()
                                );
                                    drop(state_guard);
                                    // Don't fall back to server fetch - we'll apply after init
                                    true
                                } else {
                                    match crate::sync::crdt_merge::process_received_edit(
                                        Some(&ctx.mqtt_client),
                                        &ctx.workspace,
                                        &doc_id,
                                        file_state,
                                        &edit_msg,
                                        &author,
                                        None::<&Arc<crate::store::CommitStore>>, // No local commit store in subscription context
                                    )
                                    .await
                                    {
                                        Ok((result, maybe_content)) => {
                                            info!(
                                            "[SANDBOX-TRACE] subdir_mqtt_task CRDT_MERGE subdir={} file={} result={:?} has_content={}",
                                            subdir_path,
                                            file_key,
                                            result,
                                            maybe_content.is_some()
                                        );
                                            drop(state_guard); // Release lock before file I/O

                                            if let Some(content) = maybe_content {
                                                // Write to all paths
                                                for rel_path in paths {
                                                    let file_path = directory.join(rel_path);

                                                    if let Some(parent) = file_path.parent() {
                                                        let _ =
                                                            tokio::fs::create_dir_all(parent).await;
                                                    }
                                                    if let Err(e) =
                                                        tokio::fs::write(&file_path, &content).await
                                                    {
                                                        warn!(
                                                            "Subdir {}: Failed to write {}: {}",
                                                            subdir_path, rel_path, e
                                                        );
                                                    } else {
                                                        info!(
                                                        "[SANDBOX-TRACE] subdir_mqtt_task DISK_WRITE subdir={} file={} content_len={}",
                                                        subdir_path,
                                                        rel_path,
                                                        content.len()
                                                    );
                                                        debug!(
                                                        "Subdir {}: CRDT edit applied to {} ({} bytes, {:?})",
                                                        subdir_path,
                                                        rel_path,
                                                        content.len(),
                                                        result
                                                    );
                                                    }
                                                }
                                                true // Handled via CRDT
                                            } else {
                                                info!(
                                                "[SANDBOX-TRACE] subdir_mqtt_task CRDT_NO_CONTENT subdir={} file={} (already known)",
                                                subdir_path,
                                                file_key
                                            );
                                                debug!(
                                                "Subdir {}: CRDT merge returned no content (already known)",
                                                subdir_path
                                            );
                                                true // Handled (no-op)
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Subdir {}: CRDT merge failed for {}: {}",
                                                subdir_path, doc_id, e
                                            );
                                            false // Fall back to server fetch
                                        }
                                    }
                                }
                            } else {
                                false // Failed to load subdir state
                            }
                        } else {
                            false
                        }
                    } else {
                        debug!(
                            "Subdir {}: Failed to parse MQTT message for {}",
                            subdir_path, doc_id
                        );
                        false // Fall back to server fetch
                    }
                } else {
                    false // No CRDT context, use server fetch
                };

                // If CRDT handling failed, log and continue — next MQTT message will retry
                if !crdt_handled {
                    warn!(
                        "Subdir {}: CRDT handling failed for {} — will retry on next MQTT message",
                        subdir_path, doc_id
                    );
                }
            }
        } else {
            // Unknown document ID - might be another subdir's schema, ignore
            debug!(
                "Subdir {}: Ignoring edit for unrelated document: {}",
                subdir_path, doc_id
            );
        }
    }

    info!("MQTT message channel closed for subdir {}", subdir_path);
}

/// Sync UUID subscriptions for a subdirectory after its schema changes.
///
/// Similar to `sync_uuid_subscriptions` but scoped to a single subdirectory.
/// Compares the current subscribed UUIDs with the new UUID map from the subdir's schema,
/// subscribing to new UUIDs and unsubscribing from removed ones.
#[allow(clippy::too_many_arguments)]
async fn sync_subdir_uuid_subscriptions(
    http_client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    subscribed_uuids: &mut HashSet<String>,
    uuid_to_paths: &mut crate::sync::UuidToPathsMap,
    _directory: &std::path::Path,
    _use_paths: bool,
    _file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
) {
    // Fetch the current UUID map from the subdirectory's schema
    let (new_uuid_map, fetch_succeeded) =
        crate::sync::uuid_map::build_uuid_map_recursive_with_status(
            http_client,
            server,
            subdir_node_id,
        )
        .await;

    // If fetch failed, don't modify subscriptions
    if !fetch_succeeded {
        warn!(
            "Subdir {}: Schema fetch failed during subscription sync - keeping existing subscriptions",
            subdir_path
        );
        return;
    }

    // Prefix paths with subdir_path
    let prefixed_uuid_map: HashMap<String, String> = new_uuid_map
        .into_iter()
        .map(|(path, uuid)| {
            let full_path = if subdir_path.is_empty() {
                path
            } else {
                format!("{}/{}", subdir_path, path)
            };
            (full_path, uuid)
        })
        .collect();

    // Build new reverse map
    let new_uuid_to_paths = crate::sync::build_uuid_to_paths_map(&prefixed_uuid_map);
    let new_uuids: HashSet<String> = new_uuid_to_paths.keys().cloned().collect();

    // Find UUIDs to add and remove
    let to_add: Vec<String> = new_uuids.difference(subscribed_uuids).cloned().collect();
    let to_remove: Vec<String> = subscribed_uuids.difference(&new_uuids).cloned().collect();

    // Subscribe to new UUIDs
    for uuid in &to_add {
        let topic = Topic::edits(workspace, uuid).to_topic_string();
        if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
            warn!(
                "Subdir {}: Failed to subscribe to new file UUID {}: {}",
                subdir_path, uuid, e
            );
        } else {
            subscribed_uuids.insert(uuid.clone());
            debug!(
                "Subdir {}: Subscribed to new file UUID: {}",
                subdir_path, uuid
            );
        }
    }

    // Unsubscribe from removed UUIDs
    for uuid in &to_remove {
        let topic = Topic::edits(workspace, uuid).to_topic_string();
        if let Err(e) = mqtt_client.unsubscribe(&topic).await {
            warn!(
                "Subdir {}: Failed to unsubscribe from file UUID {}: {}",
                subdir_path, uuid, e
            );
        } else {
            subscribed_uuids.remove(uuid);
            debug!(
                "Subdir {}: Unsubscribed from removed file UUID: {}",
                subdir_path, uuid
            );
        }
    }

    // Update the reverse map BEFORE fetching content
    *uuid_to_paths = new_uuid_to_paths;

    // Brief delay to allow MQTT event loop to process retained messages
    if !to_add.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Initial content for newly subscribed UUIDs arrives via MQTT retained messages
    // and is handled by the CRDT receive path. No HTTP fetch needed.

    if !to_add.is_empty() || !to_remove.is_empty() {
        info!(
            "Subdir {}: UUID subscriptions updated: +{} -{} (total: {})",
            subdir_path,
            to_add.len(),
            to_remove.len(),
            subscribed_uuids.len()
        );
    }
}
