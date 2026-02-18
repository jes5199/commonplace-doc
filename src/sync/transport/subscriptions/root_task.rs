//! Root directory MQTT subscription task.
//!
//! Contains `directory_mqtt_task` which watches the fs-root document
//! for schema changes and all file UUIDs for content changes.

use super::recovery::{resync_schema_from_server, resync_subscribed_files, sync_schema_via_cyan};
use super::{is_http_recovery_disabled, trace_log, CrdtFileSyncContext};
use crate::commit::Commit;
use crate::events::{recv_broadcast_with_lag, BroadcastRecvResult};
use crate::mqtt::{MqttClient, Topic};
use crate::sync::dir_sync::{
    apply_explicit_deletions, apply_schema_update_to_state, handle_schema_change_with_dedup,
    handle_subdir_new_files,
};
use crate::sync::error::SyncResult;
#[cfg(unix)]
use crate::sync::flock::{try_flock_exclusive, FlockResult};
use crate::sync::subdir_spawn::{spawn_subdir_watchers, SubdirSpawnParams, SubdirTransport};
use crate::sync::{spawn_file_sync_tasks_crdt, FileSyncState};
use reqwest::Client;
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

async fn load_root_schema_from_state(
    ctx: &CrdtFileSyncContext,
) -> Option<(crate::fs::FsSchema, String, Option<String>)> {
    let state = ctx.crdt_state.read().await;
    let doc = match state.schema.to_doc() {
        Ok(doc) => doc,
        Err(e) => {
            warn!("Failed to convert root schema state to doc: {}", e);
            return None;
        }
    };

    let schema = crate::sync::ymap_schema::to_fs_schema(&doc);
    let schema_json = match crate::sync::schema_to_json(&schema) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Failed to serialize root schema snapshot after cyan resync: {}",
                e
            );
            return None;
        }
    };

    Some((schema, schema_json, state.schema.head_cid.clone()))
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
    crdt_context: Option<CrdtFileSyncContext>,
) {
    // Ensure ancestry checks in this process use MQTT/cyan instead of HTTP.
    crate::sync::set_sync_ancestry_mqtt_context(Some(crate::sync::SyncAncestryMqttContext::new(
        mqtt_client.clone(),
        workspace.clone(),
    )));

    // CRITICAL: Create the broadcast receiver BEFORE any MQTT subscriptions.
    // This ensures we receive retained messages that the broker sends immediately
    // after we subscribe. If we create the receiver after subscribing, retained
    // messages may be missed (they're broadcast before our receiver exists).
    let mut message_rx = mqtt_client.subscribe_messages();
    #[cfg(unix)]
    let inode_tracker_for_crdt = inode_tracker.clone();
    #[cfg(not(unix))]
    let inode_tracker_for_crdt = None;
    let http_recovery_disabled = is_http_recovery_disabled(
        crdt_context
            .as_ref()
            .map(|ctx| ctx.mqtt_only_config.mqtt_only)
            .unwrap_or(false),
    );

    // Subscribe to edits for the fs-root document (schema changes)
    let fs_root_topic = Topic::edits(&workspace, &fs_root_id).to_topic_string();

    info!(
        "Subscribing to MQTT edits for fs-root {} at topic: {}",
        fs_root_id, fs_root_topic
    );

    trace_log(&format!(
        "directory_mqtt_task starting: fs_root={}, topic={}",
        fs_root_id, fs_root_topic
    ));

    if let Err(e) = mqtt_client
        .subscribe(&fs_root_topic, QoS::AtLeastOnce)
        .await
    {
        error!("Failed to subscribe to MQTT edits topic: {}", e);
        return;
    }

    trace_log(&format!(
        "directory_mqtt_task subscribed to topic: {}",
        fs_root_topic
    ));

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

    // Write a readiness marker to indicate MQTT subscriptions are established.
    // This allows tests to wait for the sandbox to be fully ready to receive messages.
    let ready_marker = directory.join(".mqtt-ready");
    if let Err(e) = std::fs::write(
        &ready_marker,
        format!("ready at {:?}", std::time::SystemTime::now()),
    ) {
        warn!("Failed to write MQTT readiness marker: {}", e);
    } else {
        debug!("Wrote MQTT readiness marker to {}", ready_marker.display());
    }

    // Bootstrap schema CRDT state via cyan sync channel before entering MQTT loop.
    // This replaces HTTP bootstrap — see CP-uals, design: cyan-schema-sync-design.md
    if let Some(ref ctx) = crdt_context {
        if !push_only {
            let needs_init = {
                let state = ctx.crdt_state.read().await;
                state.schema.needs_server_init()
            };

            if needs_init {
                let sync_client_id = Uuid::new_v4().to_string();

                let initialized = {
                    let mut state = ctx.crdt_state.write().await;
                    sync_schema_via_cyan(
                        &mqtt_client,
                        &workspace,
                        &fs_root_id,
                        &sync_client_id,
                        &mut state.schema,
                    )
                    .await
                };

                if initialized {
                    debug!("Schema CRDT initialized via cyan sync for {}", fs_root_id);
                } else {
                    warn!(
                        "Cyan sync failed for {} — no HTTP bootstrap fallback in MQTT runtime",
                        fs_root_id
                    );
                }
            }

            // Bootstrap files from the schema (whether initialized via cyan or already present)
            if http_recovery_disabled {
                warn!(
                    "Skipping HTTP file bootstrap for {} (HTTP recovery disabled)",
                    fs_root_id
                );
            } else if let Err(e) = handle_subdir_new_files(
                &http_client,
                &server,
                &fs_root_id,
                "", // root directory = empty subdir_path
                &directory,
                &directory,
                &file_states,
                use_paths,
                push_only,
                pull_only,
                shared_state_file.as_ref(),
                &author,
                #[cfg(unix)]
                inode_tracker.clone(),
                Some(ctx),
                None, // Schema already initialized — HTTP fetch will populate files
            )
            .await
            {
                warn!("Failed to bootstrap files after schema init: {}", e);
            }
        }
    }

    // Track last processed schema to prevent redundant processing
    let mut last_schema_hash: Option<String> = None;
    // Track last applied schema CID for ancestry checking.
    // Initialize with the CID from initial sync to prevent pulling stale server content.
    let mut last_schema_cid: Option<String> = initial_schema_cid;

    // Process incoming MQTT messages with lag detection
    loop {
        let msg = match recv_broadcast_with_lag(&mut message_rx, "MQTT directory receiver").await {
            BroadcastRecvResult::Message(m) => m,
            BroadcastRecvResult::Lagged {
                missed_count,
                next_message,
            } => {
                // Resync all subscribed files to recover missed edits
                debug!(
                    "MQTT directory receiver lagged by {} messages, triggering resync",
                    missed_count
                );
                resync_subscribed_files(
                    &http_client,
                    &server,
                    &subscribed_uuids,
                    &uuid_to_paths,
                    &directory,
                    use_paths,
                    &file_states,
                    crdt_context.as_ref(),
                    inode_tracker_for_crdt.clone(),
                    &author,
                )
                .await;

                // Also resync schema CRDT state to recover missed schema updates.
                // Without this, the local schema Y.Doc can drift from server state,
                // causing issues with subsequent deltas that reference missing items.
                if let Some(ref ctx) = crdt_context {
                    resync_schema_from_server(&fs_root_id, ctx).await;
                }

                // Process the next message if available
                match next_message {
                    Some(m) => m,
                    None => break, // Channel closed after lag
                }
            }
            BroadcastRecvResult::Closed => break,
        };

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
            trace_log(&format!(
                "MQTT schema edit received for fs-root {}: {} bytes",
                fs_root_id,
                msg.payload.len()
            ));
            info!(
                "[SANDBOX-TRACE] MQTT schema edit received for fs-root {}: {} bytes",
                fs_root_id,
                msg.payload.len()
            );

            // When CRDT context is available, apply the MQTT delta to persistent state
            // and use it DIRECTLY for file creation and deletion. This avoids race
            // conditions where HTTP may return stale data.
            //
            // When CRDT context is NOT available, fall back to HTTP-based processing.
            if let Some(ref ctx) = crdt_context {
                // Apply MQTT delta to persistent CRDT state.
                let mut schema_recovered_from_cyan = false;
                let mut updated_cid: Option<String> = None;
                let mut root_deleted_entries = std::collections::HashSet::new();

                let mut mqtt_schema = {
                    let mut state = ctx.crdt_state.write().await;
                    let schema_result =
                        apply_schema_update_to_state(&mut state.schema, &msg.payload)
                            .map(|r| (r.schema, r.schema_json, r.deleted_entries));

                    if let Some((_, _, deleted_entries)) = &schema_result {
                        updated_cid = state.schema.head_cid.clone();
                        root_deleted_entries = deleted_entries.clone();

                        if let Err(e) = state.save(&directory).await {
                            warn!("Failed to save root CRDT state: {}", e);
                        } else {
                            debug!(
                                "[SANDBOX-TRACE] Persisted schema CRDT state for root directory"
                            );
                        }
                    }

                    schema_result.map(|(schema, schema_json, _)| (schema, schema_json))
                };

                if mqtt_schema.is_none() {
                    trace_log(
                        "MQTT: Failed to decode schema from CRDT state; triggering cyan resync",
                    );
                    debug!("MQTT: Failed to decode schema from CRDT state; triggering cyan resync");
                    resync_schema_from_server(&fs_root_id, ctx).await;
                    mqtt_schema = load_root_schema_from_state(ctx).await.map(
                        |(schema, schema_json, recovered_cid)| {
                            schema_recovered_from_cyan = true;
                            updated_cid = recovered_cid;
                            (schema, schema_json)
                        },
                    );
                }

                if let Some((ref schema, ref schema_json)) = mqtt_schema {
                    let entry_count = if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
                        root.entries.as_ref().map(|e| e.len()).unwrap_or(0)
                    } else {
                        0
                    };
                    // Log entry names for debugging
                    let entry_names: Vec<String> =
                        if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
                            root.entries
                                .as_ref()
                                .map(|e| e.keys().cloned().collect())
                                .unwrap_or_default()
                        } else {
                            vec![]
                        };
                    trace_log(&format!(
                        "MQTT schema decoded: entry_count={}, entries={:?}",
                        entry_count, entry_names
                    ));
                    info!(
                        "[SANDBOX-TRACE] MQTT schema decoded: entry_count={}, entries={:?}",
                        entry_count, entry_names
                    );

                    {
                        // Compute hash for deduplication
                        let current_hash =
                            crate::sync::state_file::compute_content_hash(schema_json.as_bytes());
                        let is_duplicate = last_schema_hash.as_ref() == Some(&current_hash);

                        // Full snapshot recovery after decode failure still needs reconciliation
                        // even if schema hash is unchanged.
                        if !is_duplicate || schema_recovered_from_cyan {
                            // Write the schema to disk
                            if let Err(e) = crate::sync::write_schema_file(
                                &directory,
                                schema_json,
                                written_schemas.as_ref(),
                            )
                            .await
                            {
                                warn!("MQTT: Failed to write schema file: {}", e);
                            }

                            // Handle new files using MQTT schema
                            let schema_tuple = (schema.clone(), schema_json.clone());
                            if !push_only {
                                if http_recovery_disabled {
                                    warn!(
                                        "MQTT: Skipping new file sync for {} (HTTP recovery disabled)",
                                        fs_root_id
                                    );
                                } else if let Err(e) = handle_subdir_new_files(
                                    &http_client,
                                    &server,
                                    &fs_root_id,
                                    "", // root directory = empty subdir_path
                                    &directory,
                                    &directory,
                                    &file_states,
                                    use_paths,
                                    push_only,
                                    pull_only,
                                    shared_state_file.as_ref(),
                                    &author,
                                    #[cfg(unix)]
                                    inode_tracker.clone(),
                                    crdt_context.as_ref(),
                                    Some(schema_tuple.clone()),
                                )
                                .await
                                {
                                    warn!("MQTT: Failed to sync new files: {}", e);
                                }

                                // Reconcile UUIDs after schema update to fix any drift
                                let mut state = ctx.crdt_state.write().await;
                                if let Err(e) = state.reconcile_with_schema(&directory).await {
                                    warn!(
                                        "MQTT: Failed to reconcile UUIDs after schema update: {}",
                                        e
                                    );
                                }
                            }

                            // Handle deletions: use ONLY explicit CRDT deletions (CP-seha).
                            // When CRDT path is active, empty deleted_entries means "no deletions"
                            // — NOT "fall back to schema-diff". Schema-diff is unreliable when
                            // CRDT state is partial/uninitialized and would cause mass deletions.
                            if !root_deleted_entries.is_empty() {
                                apply_explicit_deletions(
                                    "", // root directory = empty subdir_path
                                    &directory,
                                    &file_states,
                                    &root_deleted_entries,
                                )
                                .await;
                            } else if schema_recovered_from_cyan {
                                debug!(
                                    "MQTT: Root schema recovered from cyan snapshot with no explicit deletions; skipping deletion sweep"
                                );
                            }

                            // Ensure CRDT tasks exist for all files
                            if let Err(e) = ensure_crdt_tasks_for_files(
                                &http_client,
                                &server,
                                &directory,
                                &file_states,
                                pull_only,
                                &author,
                                inode_tracker_for_crdt.clone(),
                                ctx,
                            )
                            .await
                            {
                                warn!("MQTT: Failed to ensure CRDT tasks: {}", e);
                            }

                            // Update UUID subscriptions in CRDT mode too —
                            // new files need MQTT topic subscriptions and initial content fetch
                            sync_uuid_subscriptions(
                                &http_client,
                                &server,
                                &fs_root_id,
                                &mqtt_client,
                                &workspace,
                                &mut subscribed_uuids,
                                &mut uuid_to_paths,
                                &directory,
                                use_paths,
                                &file_states,
                            )
                            .await;

                            // Update dedup state (hash and CID for ancestry tracking)
                            last_schema_hash = Some(current_hash);
                            // Sync last_schema_cid from CRDT state to maintain ancestry checks
                            if updated_cid.is_some() {
                                last_schema_cid = updated_cid.clone();
                            }

                            if schema_recovered_from_cyan && !push_only {
                                // Materialize current file state immediately from the recovered
                                // snapshot so convergence does not wait for a later edit.
                                resync_subscribed_files(
                                    &http_client,
                                    &server,
                                    &subscribed_uuids,
                                    &uuid_to_paths,
                                    &directory,
                                    use_paths,
                                    &file_states,
                                    crdt_context.as_ref(),
                                    inode_tracker_for_crdt.clone(),
                                    &author,
                                )
                                .await;
                            }
                        } else {
                            debug!("MQTT: Schema unchanged (same hash), skipped processing");
                        }
                    }
                } else if schema_recovered_from_cyan {
                    warn!(
                        "MQTT: Cyan resync completed for {}, but no schema snapshot could be loaded",
                        fs_root_id
                    );
                }
            } else {
                if http_recovery_disabled {
                    warn!(
                        "MQTT: Received schema change for {} but no CRDT context is available and HTTP recovery is disabled",
                        fs_root_id
                    );
                } else {
                    // No CRDT context - use HTTP-based processing (legacy mode)
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
                        crdt_context.as_ref(),
                    )
                    .await
                    {
                        Ok(true) => {
                            debug!("MQTT: Schema change processed successfully (HTTP mode)");
                            // Update UUID subscriptions in legacy mode
                            sync_uuid_subscriptions(
                                &http_client,
                                &server,
                                &fs_root_id,
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
                        Ok(false) => {
                            debug!("MQTT: Schema unchanged, skipped processing (HTTP mode)");
                        }
                        Err(e) => {
                            warn!("MQTT: Failed to handle schema change: {}", e);
                        }
                    }
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
                    crdt_context: crdt_context.clone(),
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
            // We process ALL file edits here for ALL paths, regardless of CRDT status.
            // This is necessary because CRDT receive_task may miss messages due to
            // broadcast channel timing when tasks are spawned.
            //
            // For CRDT-managed files, we apply the MQTT message directly using CRDT merge.
            // For non-CRDT files, we fetch from server as a fallback.
            if pull_only || !push_only {
                debug!(
                    "MQTT edit received for file UUID {}: {} bytes, {} path(s)",
                    doc_id,
                    msg.payload.len(),
                    paths.len()
                );

                // Try to apply the edit directly via CRDT merge if we have CRDT context.
                // Yjs updates use encode_state_as_update_v1(&StateVector::default()) which
                // produces full-state updates that can safely be applied to an empty doc.
                // This means we can apply updates even before fetching initial state from server.
                let crdt_handled = if let Some(ref ctx) = crdt_context {
                    if let Ok(edit_msg) = crate::sync::crdt_merge::parse_edit_message(&msg.payload)
                    {
                        // Apply to CRDT state and get content
                        let node_id = match uuid::Uuid::parse_str(&doc_id) {
                            Ok(id) => id,
                            Err(_) => {
                                debug!(
                                    "Non-UUID document {}, falling back to server fetch",
                                    doc_id
                                );
                                uuid::Uuid::nil() // Skip CRDT handling
                            }
                        };

                        if !node_id.is_nil() {
                            // Get the filename from the first path
                            let filename = paths
                                .first()
                                .and_then(|p| std::path::Path::new(p).file_name())
                                .and_then(|n| n.to_str())
                                .unwrap_or(&doc_id)
                                .to_string();

                            let received_cid = Commit::with_timestamp(
                                edit_msg.parents.clone(),
                                edit_msg.update.clone(),
                                edit_msg.author.clone(),
                                edit_msg.message.clone(),
                                edit_msg.timestamp,
                            )
                            .calculate_cid();

                            let mut state_guard = ctx.crdt_state.write().await;
                            let file_state = state_guard.get_or_create_file(&filename, node_id);

                            // Check if CRDT state needs initialization or receive task isn't ready.
                            if let Some(reason) = file_state.should_queue_edits() {
                                file_state.queue_pending_edit(msg.payload.clone());
                                debug!(
                                    "CRDT not ready for {}, queued edit (reason: {:?}, queue size: {})",
                                    filename,
                                    reason,
                                    file_state.pending_edits.len()
                                );
                                drop(state_guard);
                                // Don't fall back to server fetch - we'll apply after init
                                true
                            } else if file_state.is_cid_known(&received_cid) {
                                drop(state_guard);
                                true
                            } else {
                                drop(state_guard);

                                let mut deferred = false;
                                #[cfg(unix)]
                                let mut _flock_guard = None;
                                #[cfg(unix)]
                                {
                                    let lock_path = directory
                                        .join(paths.first().map(|p| p.as_str()).unwrap_or(""));
                                    match try_flock_exclusive(
                                        &lock_path,
                                        Some(crate::sync::file_sync::CRDT_FLOCK_TIMEOUT),
                                    )
                                    .await
                                    {
                                        Ok(FlockResult::Acquired(guard)) => {
                                            _flock_guard = Some(guard);
                                        }
                                        Ok(FlockResult::Timeout) => {
                                            let mut state_guard = ctx.crdt_state.write().await;
                                            let file_state =
                                                state_guard.get_or_create_file(&filename, node_id);
                                            file_state.queue_pending_edit(msg.payload.clone());
                                            warn!(
                                                "CRDT edit deferred for {} due to flock timeout",
                                                filename
                                            );
                                            drop(state_guard);
                                            deferred = true;
                                        }
                                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                                        Err(e) => {
                                            let mut state_guard = ctx.crdt_state.write().await;
                                            let file_state =
                                                state_guard.get_or_create_file(&filename, node_id);
                                            file_state.queue_pending_edit(msg.payload.clone());
                                            warn!(
                                                "CRDT edit deferred for {} (flock error: {})",
                                                filename, e
                                            );
                                            drop(state_guard);
                                            deferred = true;
                                        }
                                    }
                                }

                                if deferred {
                                    true
                                } else {
                                    let mut state_guard = ctx.crdt_state.write().await;
                                    let file_state =
                                        state_guard.get_or_create_file(&filename, node_id);
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
                                                            "Failed to write {}: {}",
                                                            rel_path, e
                                                        );
                                                    } else {
                                                        debug!(
                                                        "CRDT edit applied to {} ({} bytes, {:?})",
                                                        rel_path,
                                                        content.len(),
                                                        result
                                                    );
                                                    }
                                                }
                                                true // Handled via CRDT
                                            } else {
                                                debug!(
                                                    "CRDT merge returned no content (already known)"
                                                );
                                                true // Handled (no-op)
                                            }
                                        }
                                        Err(e) => {
                                            warn!("CRDT merge failed for {}: {}", doc_id, e);
                                            false // Fall back to server fetch
                                        }
                                    }
                                }
                            }
                        } else {
                            false
                        }
                    } else {
                        debug!("Failed to parse MQTT message for {}", doc_id);
                        false // Fall back to server fetch
                    }
                } else {
                    false // No CRDT context, use server fetch
                };

                // If CRDT handling failed, log and continue — next MQTT message will retry
                if !crdt_handled {
                    warn!(
                        "CRDT handling failed for {} — will retry on next MQTT message",
                        doc_id
                    );
                }
            }
        } else {
            // Unknown document ID - might be a subdir schema, ignore
            debug!("Ignoring edit for unknown document: {}", doc_id);
        }
    }

    info!("MQTT directory message channel closed");
}

/// Ensure files discovered via schema changes are upgraded to CRDT tasks.
#[allow(clippy::too_many_arguments)]
async fn ensure_crdt_tasks_for_files(
    http_client: &Client,
    server: &str,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    pull_only: bool,
    author: &str,
    inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    context: &CrdtFileSyncContext,
) -> SyncResult<()> {
    let pending: Vec<(String, String)> = {
        let states = file_states.read().await;
        states
            .iter()
            .filter(|(_, state)| !state.crdt_tasks_spawned && !state.use_paths)
            .map(|(path, state)| (path.clone(), state.identifier.clone()))
            .collect()
    };

    for (relative_path, identifier) in pending {
        let node_id = match Uuid::parse_str(&identifier) {
            Ok(id) => id,
            Err(_) => {
                debug!(
                    "Skipping CRDT task spawn for {} (non-UUID identifier {})",
                    relative_path, identifier
                );
                continue;
            }
        };

        let file_path = directory.join(&relative_path);
        let filename = Path::new(&relative_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&relative_path)
            .to_string();

        if let Err(e) = crate::sync::file_sync::resync_crdt_state_via_cyan_with_pending(
            &context.mqtt_client,
            &context.workspace,
            node_id,
            &context.crdt_state,
            &filename,
            &file_path,
            author,
            inode_tracker.as_ref(),
            true,
            true,
        )
        .await
        {
            warn!(
                "Failed to initialize CRDT state for {}: {} — initializing empty",
                relative_path, e
            );
            // Initialize empty so MQTT edits can still be applied
            let mut state = context.crdt_state.write().await;
            let fs = state.get_or_create_file(&filename, node_id);
            fs.initialize_empty();
        }

        let old_handles = {
            let mut states = file_states.write().await;
            if let Some(state) = states.get_mut(&relative_path) {
                std::mem::take(&mut state.task_handles)
            } else {
                Vec::new()
            }
        };

        for handle in old_handles {
            handle.abort();
        }

        // Note: We intentionally pass None for file_states here because this code path
        // already explicitly aborts old tasks before spawning new ones (see lines above).
        // The deduplication check is for preventing concurrent spawns from different code paths.
        let handles = spawn_file_sync_tasks_crdt(
            context.mqtt_client.clone(),
            http_client.clone(),
            server.to_string(),
            context.workspace.clone(),
            node_id,
            file_path,
            context.crdt_state.clone(),
            filename,
            pull_only,
            author.to_string(),
            context.mqtt_only_config,
            inode_tracker.clone(),
            None, // file_states - not needed, old tasks already aborted above
            None, // relative_path
        );

        let mut states = file_states.write().await;
        if let Some(state) = states.get_mut(&relative_path) {
            state.task_handles = handles;
            state.crdt_tasks_spawned = true;
        }
    }

    Ok(())
}

/// Sync UUID subscriptions after a schema change.
///
/// Compares the current subscribed UUIDs with the new UUID map from the schema,
/// subscribing to new UUIDs and unsubscribing from removed ones.
///
/// If the schema fetch fails, subscriptions are left unchanged to avoid
/// accidentally unsubscribing from all file topics due to transient errors.
#[allow(clippy::too_many_arguments)]
async fn sync_uuid_subscriptions(
    http_client: &Client,
    server: &str,
    fs_root_id: &str,
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    subscribed_uuids: &mut HashSet<String>,
    uuid_to_paths: &mut crate::sync::UuidToPathsMap,
    _directory: &std::path::Path,
    _use_paths: bool,
    _file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
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

    // Update the reverse map BEFORE fetching content (so we have path info)
    *uuid_to_paths = new_uuid_to_paths;

    // Brief delay to allow the MQTT event loop to process retained messages
    // that arrive after subscription. The main content delivery happens via
    // server fetch with retries below.
    if !to_add.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Initial content for newly subscribed UUIDs arrives via MQTT retained messages
    // and is handled by the CRDT receive path. No HTTP fetch needed.

    if !to_add.is_empty() || !to_remove.is_empty() {
        info!(
            "UUID subscriptions updated: +{} -{} (total: {})",
            to_add.len(),
            to_remove.len(),
            subscribed_uuids.len()
        );
    }
}
