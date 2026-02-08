//! MQTT subscription tasks for directory sync.
//!
//! This module contains the task functions that subscribe to MQTT
//! events for directory-level synchronization.

pub(crate) mod recovery;
pub mod root_task;
pub mod subdir_task;

// Re-export public API (preserves existing import paths)
pub use root_task::directory_mqtt_task;
pub use subdir_task::{spawn_subdir_mqtt_task, subdir_mqtt_task};

use std::io::Write;

use crate::fs::FsSchema;
use crate::mqtt::MqttClient;
use crate::sync::dir_sync::{
    apply_explicit_deletions, create_subdir_nested_directories, handle_subdir_new_files,
};
use crate::sync::FileSyncState;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Write a trace message to /tmp/sandbox-trace.log for debugging
pub(super) fn trace_log(msg: &str) {
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

/// Context for spawning CRDT file sync tasks from MQTT schema updates.
#[derive(Clone)]
pub struct CrdtFileSyncContext {
    pub mqtt_client: Arc<MqttClient>,
    pub workspace: String,
    pub crdt_state: Arc<RwLock<crate::sync::DirectorySyncState>>,
    pub subdir_cache: Arc<crate::sync::SubdirStateCache>,
    /// Configuration for MQTT-only sync mode.
    /// When enabled, HTTP calls are deprecated and state is initialized from MQTT.
    pub mqtt_only_config: crate::sync::MqttOnlySyncConfig,
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
    deleted_entries: std::collections::HashSet<String>,
) {
    // Cleanup deleted files using ONLY explicit CRDT deletions (CP-seha).
    // When CRDT path is active, empty deleted_entries means "no deletions"
    // — NOT "fall back to schema-diff". Schema-diff is unreliable when
    // CRDT state is partial/uninitialized and would cause mass deletions.
    if !deleted_entries.is_empty() {
        apply_explicit_deletions(subdir_path, subdir_full_path, file_states, &deleted_entries)
            .await;
        debug!(
            "{}: Subdir {} explicit CRDT deletions applied",
            log_prefix, subdir_path
        );
    }

    // Then, sync NEW files from server (skip in MQTT-only mode to avoid HTTP calls)
    if crdt_context
        .as_ref()
        .map(|ctx| ctx.mqtt_only_config.mqtt_only)
        .unwrap_or(false)
    {
        warn!(
            "{}: Skipping new file sync for {} (MQTT-only mode, HTTP disabled)",
            log_prefix, subdir_path
        );
    } else {
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
            mqtt_schema,
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
