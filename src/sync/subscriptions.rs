//! SSE and MQTT subscription tasks for directory sync.
//!
//! This module contains the task functions that subscribe to SSE and MQTT
//! events for directory-level synchronization.

use std::io::Write;

/// Write a trace message to /tmp/sandbox-trace.log for debugging
fn trace_log(msg: &str) {
    use std::fs::OpenOptions;
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/sandbox-trace.log")
    {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let pid = std::process::id();
        let _ = writeln!(file, "[{} pid={}] {}", timestamp, pid, msg);
    }
}

/// Timeline milestones for sandbox sync readiness tracing.
///
/// These milestones track the ordering of key events during sandbox sync startup.
/// The expected ordering is:
/// 1. UUID_READY - when file UUID is known (from schema)
/// 2. CRDT_INIT_COMPLETE - when CRDT state is initialized from server
/// 3. TASK_SPAWN - when sync tasks are spawned for a file
/// 4. EXEC_START - when sandbox exec process starts
/// 5. FIRST_WRITE - when first file write occurs (optional, only if process writes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineMilestone {
    /// File UUID has been resolved from schema
    UuidReady,
    /// CRDT state has been initialized from server
    CrdtInitComplete,
    /// Sync tasks have been spawned for this file
    TaskSpawn,
    /// Sandbox exec process has started
    ExecStart,
    /// First write to file has occurred
    FirstWrite,
}

impl std::fmt::Display for TimelineMilestone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimelineMilestone::UuidReady => write!(f, "UUID_READY"),
            TimelineMilestone::CrdtInitComplete => write!(f, "CRDT_INIT_COMPLETE"),
            TimelineMilestone::TaskSpawn => write!(f, "TASK_SPAWN"),
            TimelineMilestone::ExecStart => write!(f, "EXEC_START"),
            TimelineMilestone::FirstWrite => write!(f, "FIRST_WRITE"),
        }
    }
}

/// Emit a structured timeline trace event.
///
/// Format: `[TIMELINE] milestone=<MILESTONE> path=<path> uuid=<uuid> timestamp=<millis>`
///
/// These events can be captured in tests to verify ordering of sandbox sync milestones.
pub fn trace_timeline(milestone: TimelineMilestone, path: &str, uuid: Option<&str>) {
    use std::fs::OpenOptions;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let pid = std::process::id();

    let uuid_str = uuid.unwrap_or("-");

    // Log to tracing for normal observation
    tracing::info!(
        "[TIMELINE] milestone={} path={} uuid={} timestamp={} pid={}",
        milestone,
        path,
        uuid_str,
        timestamp,
        pid
    );

    // Also write to trace file for test capture
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/sandbox-trace.log")
    {
        let _ = writeln!(
            file,
            "[TIMELINE] milestone={} path={} uuid={} timestamp={} pid={}",
            milestone, path, uuid_str, timestamp, pid
        );
    }
}

use crate::events::{recv_broadcast_with_lag, BroadcastRecvResult};
use crate::fs::FsSchema;
use crate::mqtt::{MqttClient, Topic};
use crate::sync::dir_sync::{
    apply_schema_update_to_state, create_subdir_nested_directories,
    decode_schema_from_mqtt_payload, handle_schema_change_with_dedup, handle_subdir_new_files,
    handle_subdir_schema_cleanup,
};
use crate::sync::error::SyncResult;
use crate::sync::subdir_spawn::{spawn_subdir_watchers, SubdirSpawnParams, SubdirTransport};
use crate::sync::{
    encode_node_id, fetch_head, spawn_file_sync_tasks_crdt, FileSyncState, SharedLastContent,
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
use uuid::Uuid;

/// Context for spawning CRDT file sync tasks from MQTT schema updates.
#[derive(Clone)]
pub struct CrdtFileSyncContext {
    pub mqtt_client: Arc<MqttClient>,
    pub workspace: String,
    pub crdt_state: Arc<RwLock<crate::sync::DirectorySyncState>>,
    pub subdir_cache: Arc<crate::sync::SubdirStateCache>,
}

// ============================================================================
// Shared Helpers
// ============================================================================

/// Handle a subdirectory schema edit event (shared by SSE and MQTT paths).
///
/// This performs the common operations when a subdirectory schema changes:
/// 1. Cleanup deleted files and orphaned directories
/// 2. Sync NEW files from server
/// 3. Create directories for new node-backed subdirectories
///
/// When `crdt_context` is provided, CRDT sync tasks are spawned instead of HTTP sync tasks.
///
/// When `mqtt_schema` is provided (decoded from MQTT payload), it's used directly for cleanup
/// instead of fetching from HTTP. This avoids race conditions where the server hasn't
/// processed the MQTT edit yet.
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
    crdt_context: Option<&CrdtFileSyncContext>,
    mqtt_schema: Option<(FsSchema, String)>,
) {
    // Clone mqtt_schema to pass to new files sync
    let mqtt_schema_for_new_files = mqtt_schema.clone();

    // First, cleanup deleted files and orphaned directories
    match handle_subdir_schema_cleanup(
        client,
        server,
        subdir_node_id,
        subdir_path,
        subdir_full_path,
        directory,
        file_states,
        mqtt_schema,
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
        crdt_context,
        mqtt_schema_for_new_files,
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
                                None, // SSE path doesn't have CRDT context
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
                                    crdt_context: None, // SSE path doesn't have CRDT context
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
                            // SSE path doesn't have CRDT context, use None
                            // SSE path doesn't have MQTT payload, fetch schema from HTTP
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
                                None, // No CRDT context for SSE
                                None, // No MQTT schema for SSE, fetch from HTTP
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
    crdt_context: Option<CrdtFileSyncContext>,
) {
    // CRITICAL: Create the broadcast receiver BEFORE any MQTT subscriptions.
    // This ensures we receive retained messages that the broker sends immediately
    // after we subscribe. If we create the receiver after subscribing, retained
    // messages may be missed (they're broadcast before our receiver exists).
    let mut message_rx = mqtt_client.subscribe_messages();

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
                info!(
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
                    &author,
                )
                .await;

                // Also resync schema CRDT state to recover missed schema updates.
                // Without this, the local schema Y.Doc can drift from server state,
                // causing issues with subsequent deltas that reference missing items.
                if let Some(ref ctx) = crdt_context {
                    resync_schema_from_server(
                        &http_client,
                        &server,
                        &fs_root_id,
                        ctx,
                        &directory,
                        written_schemas.as_ref(),
                    )
                    .await;
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
                // Apply MQTT delta to persistent CRDT state
                let mut state = ctx.crdt_state.write().await;
                let mqtt_schema = apply_schema_update_to_state(&mut state.schema, &msg.payload);
                // Extract the updated CID before releasing the lock (for ancestry tracking)
                let updated_cid = state.schema.head_cid.clone();
                drop(state); // Release lock before doing I/O

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

                    // Compute hash for deduplication
                    let current_hash =
                        crate::sync::state_file::compute_content_hash(schema_json.as_bytes());
                    let is_duplicate = last_schema_hash.as_ref() == Some(&current_hash);

                    if !is_duplicate {
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
                            if let Err(e) = handle_subdir_new_files(
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
                        }

                        // Handle deletions using MQTT schema
                        if let Err(e) = handle_subdir_schema_cleanup(
                            &http_client,
                            &server,
                            &fs_root_id,
                            "", // root directory = empty subdir_path
                            &directory,
                            &directory,
                            &file_states,
                            Some(schema_tuple),
                        )
                        .await
                        {
                            warn!("MQTT: Failed to cleanup deleted files: {}", e);
                        }

                        // Ensure CRDT tasks exist for all files
                        if let Err(e) = ensure_crdt_tasks_for_files(
                            &http_client,
                            &server,
                            &directory,
                            &file_states,
                            pull_only,
                            &author,
                            ctx,
                        )
                        .await
                        {
                            warn!("MQTT: Failed to ensure CRDT tasks: {}", e);
                        }

                        // Update dedup state (hash and CID for ancestry tracking)
                        last_schema_hash = Some(current_hash);
                        // Sync last_schema_cid from CRDT state to maintain ancestry checks
                        if updated_cid.is_some() {
                            last_schema_cid = updated_cid.clone();
                        }
                    } else {
                        debug!("MQTT: Schema unchanged (same hash), skipped processing");
                    }
                } else {
                    trace_log(
                        "MQTT: Failed to decode schema from CRDT state, falling back to HTTP",
                    );
                    debug!("MQTT: Failed to decode schema from CRDT state, falling back to HTTP");
                    // Fall through to HTTP-based processing
                    if let Err(e) = handle_schema_change_with_dedup(
                        &http_client,
                        &server,
                        &fs_root_id,
                        &directory,
                        &file_states,
                        true,
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
                        warn!("MQTT: Failed to handle schema change: {}", e);
                    }
                }
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

                            let mut state_guard = ctx.crdt_state.write().await;
                            let file_state = state_guard.get_or_create_file(&filename, node_id);

                            // Check if CRDT state needs initialization.
                            // If so, queue the edit for later processing instead of
                            // trying to merge it now (which would fail or produce
                            // incorrect results without shared history).
                            if file_state.needs_server_init() {
                                file_state.queue_pending_edit(msg.payload.clone());
                                debug!(
                                    "CRDT needs init for {}, queued edit (queue size: {})",
                                    filename,
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
                                )
                                .await
                                {
                                    Ok((result, maybe_content)) => {
                                        drop(state_guard); // Release lock before file I/O

                                        if let Some(content) = maybe_content {
                                            // Write to all paths
                                            for rel_path in paths {
                                                let file_path = directory.join(rel_path);

                                                // Update shared_last_content before writing to prevent echo
                                                // If we can't update it, log a warning since this may cause
                                                // echo loops when the file watcher detects our write
                                                {
                                                    let states = file_states.read().await;
                                                    if let Some(state) = states.get(rel_path) {
                                                        if let Some(ref slc) =
                                                            state.crdt_last_content
                                                        {
                                                            let mut shared = slc.write().await;
                                                            *shared = Some(content.clone());
                                                        } else {
                                                            warn!(
                                                                "No crdt_last_content for {} - echo suppression may fail",
                                                                rel_path
                                                            );
                                                        }
                                                    } else {
                                                        warn!(
                                                            "No file state for {} - echo suppression may fail",
                                                            rel_path
                                                        );
                                                    }
                                                }

                                                if let Some(parent) = file_path.parent() {
                                                    let _ = tokio::fs::create_dir_all(parent).await;
                                                }
                                                if let Err(e) =
                                                    tokio::fs::write(&file_path, &content).await
                                                {
                                                    warn!("Failed to write {}: {}", rel_path, e);
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

                // Fallback: fetch from server for non-CRDT paths or if CRDT handling failed
                if !crdt_handled {
                    if let Err(e) = handle_file_uuid_edit(
                        &http_client,
                        &server,
                        &doc_id,
                        paths,
                        &directory,
                        use_paths,
                        &file_states,
                    )
                    .await
                    {
                        warn!("Failed to handle file UUID edit for {}: {}", doc_id, e);
                    }
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
async fn ensure_crdt_tasks_for_files(
    http_client: &Client,
    server: &str,
    directory: &Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    pull_only: bool,
    author: &str,
    context: &CrdtFileSyncContext,
) -> SyncResult<()> {
    let pending: Vec<(String, String)> = {
        let states = file_states.read().await;
        states
            .iter()
            .filter(|(_, state)| state.crdt_last_content.is_none() && !state.use_paths)
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

        // Create shared_last_content BEFORE init so we can update it during init
        // This prevents the file watcher from detecting init writes as local changes
        let initial_content = match tokio::fs::read_to_string(&file_path).await {
            Ok(s) if !s.is_empty() => Some(s),
            Ok(_) => None, // Empty file treated as no initial content
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                error!(
                    "Failed to read initial content from {}: {}",
                    file_path.display(),
                    e
                );
                None
            }
        };
        let shared_last_content: SharedLastContent = Arc::new(RwLock::new(initial_content));

        if let Err(e) = crate::sync::file_sync::initialize_crdt_state_from_server_with_pending(
            http_client,
            server,
            node_id,
            &context.crdt_state,
            &filename,
            &file_path,
            Some(&context.mqtt_client),
            Some(&context.workspace),
            Some(author),
            Some(&shared_last_content),
        )
        .await
        {
            // Log as error and skip spawning sync tasks - continuing without proper
            // initialization risks data divergence or loss
            error!(
                "Failed to initialize CRDT state for {}: {} - skipping sync task spawn",
                relative_path, e
            );
            continue;
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
            shared_last_content.clone(),
            pull_only,
            author.to_string(),
            None, // file_states - not needed, old tasks already aborted above
            None, // relative_path
        );

        let mut states = file_states.write().await;
        if let Some(state) = states.get_mut(&relative_path) {
            state.task_handles = handles;
            state.crdt_last_content = Some(shared_last_content);
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
    directory: &std::path::Path,
    use_paths: bool,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
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

    // Fetch and write initial content for newly added UUIDs.
    // This is a fallback for when MQTT messages are missed (e.g., published before
    // we subscribed and not retained). The MQTT retained messages should normally
    // handle this, but server fetch provides a safety net.
    if !to_add.is_empty() {
        info!(
            "Fetching initial content for {} newly subscribed UUIDs",
            to_add.len()
        );
        for uuid in &to_add {
            if let Some(paths) = uuid_to_paths.get(uuid) {
                if let Err(e) = handle_file_uuid_edit(
                    http_client,
                    server,
                    uuid,
                    paths,
                    directory,
                    use_paths,
                    file_states,
                )
                .await
                {
                    warn!("Failed to fetch initial content for UUID {}: {}", uuid, e);
                } else {
                    debug!("Fetched initial content for UUID {}", uuid);
                }
            }
        }
    }

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
///
/// Also updates shared_last_content for CRDT-managed files to prevent echo publishes
/// when the file watcher detects the write.
///
/// Retries with backoff if server returns empty content, as the server may not have
/// committed the MQTT edit yet (server and sync clients receive MQTT simultaneously).
async fn handle_file_uuid_edit(
    http_client: &Client,
    server: &str,
    uuid: &str,
    paths: &[String],
    directory: &Path,
    use_paths: bool,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::sync::fetch_head;
    use std::time::Duration;
    use tokio::time::sleep;

    const MAX_RETRIES: u32 = 15;
    const INITIAL_DELAY_MS: u64 = 100;

    // Retry fetching with exponential backoff if content is empty.
    // The server may not have committed the MQTT edit yet.
    // With 15 retries and exponential backoff (100, 200, 400, 800, 1600, ...),
    // we wait up to about 3 seconds total for server to have content.
    let mut head = None;
    let mut delay_ms = INITIAL_DELAY_MS;
    let max_delay_ms = 500; // Cap at 500ms per retry

    for attempt in 0..MAX_RETRIES {
        match fetch_head(http_client, server, uuid, use_paths).await? {
            Some(h) if !h.content.is_empty() => {
                head = Some(h);
                break;
            }
            Some(h) => {
                // Got a response but content is empty - server may still be processing
                if attempt < MAX_RETRIES - 1 {
                    debug!(
                        "Server returned empty content for {}, retrying in {}ms (attempt {}/{})",
                        uuid,
                        delay_ms,
                        attempt + 1,
                        MAX_RETRIES
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(max_delay_ms); // Exponential backoff, capped
                } else {
                    // Last attempt, content still empty - use it, will write empty file
                    // This is better than not creating the file at all
                    head = Some(h);
                }
            }
            None => {
                if attempt < MAX_RETRIES - 1 {
                    debug!(
                        "Document {} not found, retrying in {}ms (attempt {}/{})",
                        uuid,
                        delay_ms,
                        attempt + 1,
                        MAX_RETRIES
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(max_delay_ms);
                }
            }
        }
    }

    let head =
        head.ok_or_else(|| format!("Document {} not found after {} retries", uuid, MAX_RETRIES))?;

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

    let content_string = String::from_utf8_lossy(&content_bytes).to_string();

    // Write content to all local paths that share this UUID
    for rel_path in paths {
        let file_path = directory.join(rel_path);

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Update shared_last_content BEFORE writing to prevent echo publishes
        // This is critical for CRDT-managed files where upload_task_crdt is watching
        {
            let states = file_states.read().await;
            if let Some(state) = states.get(rel_path) {
                if let Some(ref shared_last_content) = state.crdt_last_content {
                    let mut shared = shared_last_content.write().await;
                    *shared = Some(content_string.clone());
                    debug!("Updated shared_last_content for {} before write", rel_path);
                }
            }
        }

        tokio::fs::write(&file_path, &content_bytes).await?;
        debug!("Wrote {} bytes to {}", content_bytes.len(), rel_path);
    }

    Ok(())
}

/// Resync all subscribed file UUIDs from server after broadcast lag.
///
/// When the broadcast channel lags, we may have missed edit messages for files.
/// This function fetches the current HEAD state from server for all subscribed
/// file UUIDs and applies any changes to local files.
///
/// For CRDT-managed files, it re-initializes the CRDT state from server.
/// For non-CRDT files, it fetches and writes the content directly.
#[allow(clippy::too_many_arguments)]
async fn resync_subscribed_files(
    http_client: &Client,
    server: &str,
    subscribed_uuids: &HashSet<String>,
    uuid_to_paths: &crate::sync::UuidToPathsMap,
    directory: &Path,
    use_paths: bool,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    crdt_context: Option<&CrdtFileSyncContext>,
    author: &str,
) {
    info!(
        "Resyncing {} subscribed file UUIDs after broadcast lag",
        subscribed_uuids.len()
    );

    for uuid in subscribed_uuids {
        // Get paths for this UUID
        let paths = match uuid_to_paths.get(uuid) {
            Some(p) => p,
            None => continue,
        };

        // For CRDT-managed files, re-initialize from server
        if let Some(ctx) = crdt_context {
            if let Ok(node_id) = Uuid::parse_str(uuid) {
                // Get the filename from the first path
                let filename = paths
                    .first()
                    .and_then(|p| std::path::Path::new(p).file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or(uuid)
                    .to_string();

                let file_path = paths
                    .first()
                    .map(|p| directory.join(p))
                    .unwrap_or_else(|| directory.join(&filename));

                // Mark state as needing re-init so initialize_crdt_state_from_server_with_pending
                // will actually fetch fresh state
                {
                    let mut state_guard = ctx.crdt_state.write().await;
                    if let Some(file_state) = state_guard.get_file_mut(&filename) {
                        file_state.mark_needs_resync();
                    }
                }

                // Look up existing shared_last_content from file_states
                let relative_path = paths.first().cloned().unwrap_or_else(|| filename.clone());
                let shared_last_content = {
                    let states = file_states.read().await;
                    states
                        .get(&relative_path)
                        .and_then(|s| s.crdt_last_content.clone())
                };

                if let Err(e) =
                    crate::sync::file_sync::initialize_crdt_state_from_server_with_pending(
                        http_client,
                        server,
                        node_id,
                        &ctx.crdt_state,
                        &filename,
                        &file_path,
                        Some(&ctx.mqtt_client),
                        Some(&ctx.workspace),
                        Some(author),
                        shared_last_content.as_ref(),
                    )
                    .await
                {
                    warn!(
                        "Resync: Failed to re-initialize CRDT state for {}: {}",
                        uuid, e
                    );
                } else {
                    debug!("Resync: Re-initialized CRDT state for {}", uuid);
                }
                continue;
            }
        }

        // Fallback for non-CRDT files: fetch from server
        if let Err(e) = handle_file_uuid_edit(
            http_client,
            server,
            uuid,
            paths,
            directory,
            use_paths,
            file_states,
        )
        .await
        {
            warn!("Resync: Failed to fetch UUID {}: {}", uuid, e);
        } else {
            debug!("Resync: Fetched UUID {} from server", uuid);
        }
    }

    info!("Resync completed for subscribed file UUIDs");
}

/// Resync schema CRDT state from server after broadcast lag.
///
/// When the broadcast channel lags, we may have missed schema CRDT updates.
/// Without resyncing, the local schema Y.Doc state can drift from the server's
/// authoritative state, causing issues with subsequent deltas (e.g., delete
/// operations that reference items we don't have).
///
/// This function:
/// 1. Marks the schema state as needing resync (clears yjs_state)
/// 2. Fetches the authoritative schema HEAD from server (including Yjs state)
/// 3. Re-initializes the local CRDT state with the server's state
///
/// # Arguments
/// * `http_client` - HTTP client for server requests
/// * `server` - Server base URL
/// * `fs_root_id` - The fs-root document ID (schema document)
/// * `crdt_context` - CRDT context containing the schema state to resync
/// * `directory` - Local directory path (for writing schema file)
/// * `written_schemas` - Schema write tracking for deduplication
async fn resync_schema_from_server(
    http_client: &Client,
    server: &str,
    fs_root_id: &str,
    crdt_context: &CrdtFileSyncContext,
    directory: &Path,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
) {
    warn!(
        "Resyncing schema CRDT state from server after broadcast lag for {}",
        fs_root_id
    );

    // Step 1: Mark schema state as needing resync
    {
        let mut state_guard = crdt_context.crdt_state.write().await;
        state_guard.schema.mark_needs_resync();
        debug!(
            "Marked schema {} as needing resync - cleared CRDT state",
            fs_root_id
        );
    }

    // Step 2: Fetch authoritative schema HEAD from server
    let head = match fetch_head(http_client, server, &encode_node_id(fs_root_id), false).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            warn!(
                "Schema resync: Document {} not found on server, skipping",
                fs_root_id
            );
            return;
        }
        Err(e) => {
            warn!(
                "Schema resync: Failed to fetch HEAD for {}: {}",
                fs_root_id, e
            );
            return;
        }
    };

    // Step 3: Re-initialize CRDT state with server's Yjs state
    if let Some(ref server_state) = head.state {
        let cid = head.cid.as_deref().unwrap_or("unknown");
        {
            let mut state_guard = crdt_context.crdt_state.write().await;
            state_guard.schema.initialize_from_server(server_state, cid);
            info!(
                "Schema resync: Re-initialized CRDT state from server for {} at cid={}",
                fs_root_id, cid
            );
        }
    } else {
        // Server has no Yjs state - initialize as empty
        {
            let mut state_guard = crdt_context.crdt_state.write().await;
            state_guard.schema.initialize_empty();
            info!(
                "Schema resync: Initialized empty CRDT state for {} (server has no state)",
                fs_root_id
            );
        }
    }

    // Step 4: Also write the schema content to disk to ensure consistency
    if !head.content.is_empty() {
        if let Err(e) =
            crate::sync::write_schema_file(directory, &head.content, written_schemas).await
        {
            warn!(
                "Schema resync: Failed to write schema file for {}: {}",
                fs_root_id, e
            );
        } else {
            debug!(
                "Schema resync: Wrote schema file to {}",
                directory.display()
            );
        }
    }

    info!("Schema resync completed for {}", fs_root_id);
}

/// Resync subdirectory schema CRDT state from server after broadcast lag.
///
/// Similar to `resync_schema_from_server` but operates on the subdirectory's
/// cached CRDT state (via `subdir_cache`) instead of the root schema state.
///
/// # Arguments
/// * `http_client` - HTTP client for server requests
/// * `server` - Server base URL
/// * `subdir_node_id` - The subdirectory's document ID
/// * `subdir_path` - Relative path to subdirectory
/// * `crdt_context` - CRDT context containing the subdir state cache
/// * `directory` - Root local directory path
async fn resync_subdir_schema_from_server(
    http_client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    crdt_context: &CrdtFileSyncContext,
    directory: &Path,
) {
    warn!(
        "Resyncing subdirectory schema CRDT state from server after broadcast lag for {} ({})",
        subdir_path, subdir_node_id
    );

    // Parse subdir_node_id as UUID for cache lookup
    let subdir_uuid = match Uuid::parse_str(subdir_node_id) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "Subdir schema resync: Failed to parse subdir_node_id {} as UUID: {}, skipping",
                subdir_node_id, e
            );
            return;
        }
    };

    let subdir_full_path = directory.join(subdir_path);

    // Step 1: Get or load the subdirectory's cached state and mark as needing resync
    {
        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                state.schema.mark_needs_resync();
                debug!(
                    "Marked subdir schema {} as needing resync - cleared CRDT state",
                    subdir_node_id
                );
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to load cached state for {}: {}",
                    subdir_path, e
                );
                return;
            }
        }
    }

    // Step 2: Fetch authoritative schema HEAD from server
    let head = match fetch_head(http_client, server, &encode_node_id(subdir_node_id), false).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            warn!(
                "Subdir schema resync: Document {} not found on server, skipping",
                subdir_node_id
            );
            return;
        }
        Err(e) => {
            warn!(
                "Subdir schema resync: Failed to fetch HEAD for {}: {}",
                subdir_node_id, e
            );
            return;
        }
    };

    // Step 3: Re-initialize CRDT state with server's Yjs state
    {
        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                if let Some(ref server_state) = head.state {
                    let cid = head.cid.as_deref().unwrap_or("unknown");
                    state.schema.initialize_from_server(server_state, cid);
                    info!(
                        "Subdir schema resync: Re-initialized CRDT state from server for {} at cid={}",
                        subdir_path, cid
                    );
                } else {
                    state.schema.initialize_empty();
                    info!(
                        "Subdir schema resync: Initialized empty CRDT state for {} (server has no state)",
                        subdir_path
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to reload cached state for {}: {}",
                    subdir_path, e
                );
                return;
            }
        }
    }

    // Step 4: Write the schema content to disk
    if !head.content.is_empty() {
        if let Err(e) = crate::sync::write_schema_file(&subdir_full_path, &head.content, None).await
        {
            warn!(
                "Subdir schema resync: Failed to write schema file for {}: {}",
                subdir_path, e
            );
        } else {
            debug!(
                "Subdir schema resync: Wrote schema file to {}",
                subdir_full_path.display()
            );
        }
    }

    info!("Subdir schema resync completed for {}", subdir_path);
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
    crdt_context: Option<CrdtFileSyncContext>,
) {
    // Subscribe to edits for the subdirectory document (schema changes)
    let edits_topic = Topic::edits(&workspace, &subdir_node_id);
    let schema_topic_str = edits_topic.to_topic_string();

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

    // Build initial UUID map from the subdirectory's schema
    // The subdir_path is the prefix for paths within this subdirectory
    let initial_uuid_map =
        crate::sync::uuid_map::build_uuid_map_recursive(&http_client, &server, &subdir_node_id)
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
                info!(
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
                    &author,
                )
                .await;

                // Resync subdirectory schema CRDT state to recover missed schema updates.
                // Without this, the local subdirectory Y.Doc can drift from server state.
                if let Some(ref ctx) = crdt_context {
                    resync_subdir_schema_from_server(
                        &http_client,
                        &server,
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
                            if let Some((ref schema, _)) = result {
                                let entry_count =
                                    if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
                                        root.entries.as_ref().map(|e| e.len()).unwrap_or(0)
                                    } else {
                                        0
                                    };
                                info!(
                                    "[SANDBOX-TRACE] Applied schema update to subdir cached state for subdir={} entry_count={}",
                                    subdir_path, entry_count
                                );
                            } else {
                                debug!(
                                    "Failed to apply schema update for subdir={}, will fetch from HTTP",
                                    subdir_path
                                );
                            }
                            result
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
                // No CRDT context - use stateless decode (may not work for DELETE)
                let result = decode_schema_from_mqtt_payload(&msg.payload);
                if let Some((ref schema, _)) = result {
                    let entry_count = if let Some(crate::fs::Entry::Dir(ref root)) = schema.root {
                        root.entries.as_ref().map(|e| e.len()).unwrap_or(0)
                    } else {
                        0
                    };
                    info!(
                        "[SANDBOX-TRACE] Decoded schema from MQTT payload for subdir={} entry_count={} (stateless)",
                        subdir_path, entry_count
                    );
                } else {
                    debug!(
                        "Failed to decode schema from MQTT payload for subdir={}, will fetch from HTTP",
                        subdir_path
                    );
                }
                result
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
                mqtt_schema, // Use MQTT-decoded schema to avoid HTTP race
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
                            // Get the filename from the first path
                            let filename = paths
                                .first()
                                .and_then(|p| std::path::Path::new(p).file_name())
                                .and_then(|n| n.to_str())
                                .unwrap_or(&doc_id)
                                .to_string();

                            let mut state_guard = ctx.crdt_state.write().await;
                            let file_state = state_guard.get_or_create_file(&filename, node_id);

                            // Check if CRDT state needs initialization.
                            // If so, queue the edit for later processing instead of
                            // trying to merge it now (which would fail or produce
                            // incorrect results without shared history).
                            if file_state.needs_server_init() {
                                file_state.queue_pending_edit(msg.payload.clone());
                                info!(
                                    "[SANDBOX-TRACE] subdir_mqtt_task CRDT_QUEUED subdir={} file={} queue_size={}",
                                    subdir_path,
                                    filename,
                                    file_state.pending_edits.len()
                                );
                                debug!(
                                    "Subdir {}: CRDT needs init for {}, queued edit (queue size: {})",
                                    subdir_path,
                                    filename,
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
                                )
                                .await
                                {
                                    Ok((result, maybe_content)) => {
                                        info!(
                                            "[SANDBOX-TRACE] subdir_mqtt_task CRDT_MERGE subdir={} file={} result={:?} has_content={}",
                                            subdir_path,
                                            filename,
                                            result,
                                            maybe_content.is_some()
                                        );
                                        drop(state_guard); // Release lock before file I/O

                                        if let Some(content) = maybe_content {
                                            // Write to all paths
                                            for rel_path in paths {
                                                let file_path = directory.join(rel_path);

                                                // Update shared_last_content before writing to prevent echo
                                                // If we can't update it, log a warning since this may cause
                                                // echo loops when the file watcher detects our write
                                                {
                                                    let states = file_states.read().await;
                                                    if let Some(state) = states.get(rel_path) {
                                                        if let Some(ref slc) =
                                                            state.crdt_last_content
                                                        {
                                                            let mut shared = slc.write().await;
                                                            *shared = Some(content.clone());
                                                        } else {
                                                            warn!(
                                                                "Subdir {}: No crdt_last_content for {} - echo suppression may fail",
                                                                subdir_path, rel_path
                                                            );
                                                        }
                                                    } else {
                                                        warn!(
                                                            "Subdir {}: No file state for {} - echo suppression may fail",
                                                            subdir_path, rel_path
                                                        );
                                                    }
                                                }

                                                if let Some(parent) = file_path.parent() {
                                                    let _ = tokio::fs::create_dir_all(parent).await;
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
                                                filename
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

                // Fallback: fetch from server for non-CRDT paths or if CRDT handling failed
                if !crdt_handled {
                    if let Err(e) = handle_file_uuid_edit(
                        &http_client,
                        &server,
                        &doc_id,
                        paths,
                        &directory,
                        use_paths,
                        &file_states,
                    )
                    .await
                    {
                        warn!(
                            "Subdir {}: Failed to handle file UUID edit for {}: {}",
                            subdir_path, doc_id, e
                        );
                    }
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
    directory: &std::path::Path,
    use_paths: bool,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
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

    // Fetch and write initial content for newly added UUIDs
    if !to_add.is_empty() {
        info!(
            "Subdir {}: Fetching initial content for {} newly subscribed UUIDs",
            subdir_path,
            to_add.len()
        );
        for uuid in &to_add {
            if let Some(paths) = uuid_to_paths.get(uuid) {
                if let Err(e) = handle_file_uuid_edit(
                    http_client,
                    server,
                    uuid,
                    paths,
                    directory,
                    use_paths,
                    file_states,
                )
                .await
                {
                    warn!(
                        "Subdir {}: Failed to fetch initial content for UUID {}: {}",
                        subdir_path, uuid, e
                    );
                } else {
                    debug!(
                        "Subdir {}: Fetched initial content for UUID {}",
                        subdir_path, uuid
                    );
                }
            }
        }
    }

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
