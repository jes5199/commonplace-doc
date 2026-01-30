//! Shared helpers for spawning node-backed subdirectory sync tasks.
//!
//! This module centralizes the logic for:
//! 1. Discovering node-backed subdirectories via get_all_node_backed_dir_ids
//! 2. Filtering for newly discovered subdirs (not already watched)
//! 3. Spawning MQTT tasks for each new subdir
//!
//! Used by both `subscriptions.rs` and `bin/sync.rs` to avoid duplication.

use crate::mqtt::MqttClient;
use crate::sync::subscriptions::{subdir_mqtt_task, CrdtFileSyncContext};
use crate::sync::uuid_map::get_all_node_backed_dir_ids;
use crate::sync::FileSyncState;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Transport mode for subdir sync tasks.
#[derive(Clone)]
pub enum SubdirTransport {
    /// Use MQTT for subscriptions.
    Mqtt {
        client: Arc<MqttClient>,
        workspace: String,
    },
}

/// Parameters common to all subdir spawn operations.
#[derive(Clone)]
pub struct SubdirSpawnParams {
    pub client: Client,
    pub server: String,
    pub fs_root_id: String,
    pub directory: PathBuf,
    pub file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    pub use_paths: bool,
    pub push_only: bool,
    pub pull_only: bool,
    pub shared_state_file: Option<crate::sync::SharedStateFile>,
    pub author: String,
    #[cfg(unix)]
    pub inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    pub watched_subdirs: Arc<RwLock<HashSet<String>>>,
    /// CRDT context for spawning CRDT sync tasks instead of HTTP sync tasks.
    /// When provided, subdirectory file syncs will use MQTT instead of HTTP.
    pub crdt_context: Option<CrdtFileSyncContext>,
}

/// Discover and spawn tasks for all node-backed subdirectories.
///
/// This function:
/// 1. Fetches all node-backed subdirs from the server
/// 2. Filters for subdirs not already in watched_subdirs
/// 3. Spawns MQTT tasks for each new subdir
/// 4. Updates watched_subdirs with newly spawned subdirs
///
/// Returns the number of new subdirs spawned.
pub async fn spawn_subdir_watchers(
    params: &SubdirSpawnParams,
    transport: SubdirTransport,
) -> usize {
    let node_backed_subdirs =
        get_all_node_backed_dir_ids(&params.client, &params.server, &params.fs_root_id).await;

    let mut watched = params.watched_subdirs.write().await;
    let mut spawned_count = 0;

    for (subdir_path, subdir_node_id) in node_backed_subdirs {
        if watched.contains(&subdir_node_id) {
            continue;
        }

        watched.insert(subdir_node_id.clone());
        spawned_count += 1;

        match &transport {
            SubdirTransport::Mqtt { client, workspace } => {
                info!(
                    "Spawning MQTT task for node-backed subdir: {} ({})",
                    subdir_path, subdir_node_id
                );
                tokio::spawn(subdir_mqtt_task(
                    params.client.clone(),
                    params.server.clone(),
                    params.fs_root_id.clone(),
                    subdir_path,
                    subdir_node_id,
                    params.directory.clone(),
                    params.file_states.clone(),
                    params.use_paths,
                    params.push_only,
                    params.pull_only,
                    params.shared_state_file.clone(),
                    params.author.clone(),
                    #[cfg(unix)]
                    params.inode_tracker.clone(),
                    client.clone(),
                    workspace.clone(),
                    params.watched_subdirs.clone(),
                    params.crdt_context.clone(),
                ));
            }
        }
    }

    spawned_count
}

/// Discover and spawn tasks for newly discovered subdirs (convenience wrapper).
///
/// This is a convenience function that logs the discovery count and returns
/// the number of new subdirs spawned.
pub async fn discover_and_spawn_subdirs(
    params: &SubdirSpawnParams,
    transport: SubdirTransport,
    context: &str,
) -> usize {
    let count = spawn_subdir_watchers(params, transport).await;
    if count > 0 {
        info!("{}: Spawned {} new subdir watcher(s)", context, count);
    }
    count
}
