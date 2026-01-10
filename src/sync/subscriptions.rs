//! SSE and MQTT subscription tasks for directory sync.
//!
//! This module contains the task functions that subscribe to SSE and MQTT
//! events for directory-level synchronization.

use crate::mqtt::{MqttClient, Topic};
use crate::sync::dir_sync::{
    create_subdir_nested_directories, handle_schema_change_with_dedup, handle_subdir_new_files,
    handle_subdir_schema_cleanup,
};
use crate::sync::{encode_node_id, FileSyncState};
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use reqwest_eventsource::{Error as SseError, Event as SseEvent, EventSource};
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

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
    written_schemas: Option<crate::sync::WrittenSchemas>,
) {
    // fs-root schema subscription always uses ID-based API
    let sse_url = format!("{}/sse/docs/{}", server, encode_node_id(&fs_root_id));

    // Track last processed schema to prevent redundant processing
    let mut last_schema_hash: Option<String> = None;
    // Track last applied schema CID for ancestry checking
    let mut last_schema_cid: Option<String> = None;

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
                                #[cfg(unix)]
                                inode_tracker.clone(),
                                written_schemas.as_ref(),
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
                                            watched_subdirs.clone(),
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

                            // Check for newly discovered nested node-backed subdirs and spawn SSE tasks
                            // This handles cases like tmux/0 being created inside tmux
                            if !push_only {
                                let all_subdirs = crate::sync::get_all_node_backed_dir_ids(
                                    &client,
                                    &server,
                                    &fs_root_id,
                                )
                                .await;

                                // Collect new subdirs to spawn (must drop lock before spawning to avoid Send issues)
                                let subdirs_to_spawn: Vec<(String, String)> = {
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
                                };

                                // Spawn tasks after releasing the lock
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
    written_schemas: Option<crate::sync::WrittenSchemas>,
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
    // Track last applied schema CID for ancestry checking
    let mut last_schema_cid: Option<String> = None;

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
                        #[cfg(unix)]
                        inode_tracker.clone(),
                        written_schemas.as_ref(),
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
                                    watched_subdirs.clone(),
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

                    // Check for newly discovered nested node-backed subdirs and spawn MQTT tasks
                    if !push_only {
                        let all_subdirs = crate::sync::get_all_node_backed_dir_ids(
                            &http_client,
                            &server,
                            &fs_root_id,
                        )
                        .await;

                        // Collect new subdirs to spawn (must drop lock before spawning to avoid Send issues)
                        let subdirs_to_spawn: Vec<(String, String)> = {
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
                        };

                        // Spawn tasks after releasing the lock
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
