//! Sync utilities for directory synchronization.
//!
//! This module is organized into focused submodules:
//! - `crdt/` - CRDT state management, merge, publish operations
//! - `watcher/` - File watching, shadow files, stability checks
//! - `schema/` - Schema I/O, directory scanning, UUID mapping
//! - `transport/` - HTTP client, SSE, MQTT communication

use fs2::FileExt;
use std::fs::File;
use std::io;
use std::path::Path;
use tracing::{error, info};

// =============================================================================
// Submodules (organized by concern)
// =============================================================================

/// CRDT operations: state management, merge, publish, Yjs utilities
pub mod crdt;

/// File watching: watchers, file events, flock, sync state
pub mod watcher;

/// Schema operations: I/O, directory scanning, UUID mapping, content types
pub mod schema;

/// Transport: HTTP client, SSE, MQTT, URL utilities
pub mod transport;

// =============================================================================
// Root-level modules (orchestration and common types)
// =============================================================================

/// Directory sync orchestration
pub mod dir_sync;

/// Error types for sync operations
pub mod error;

/// File sync orchestration
pub mod file_sync;

/// State file persistence
pub mod state_file;

/// Subdirectory spawning utilities
pub mod subdir_spawn;

/// Common types (requests, responses, events)
pub mod types;

// =============================================================================
// Sync lock
// =============================================================================

/// Lock file name for sync process
const SYNC_LOCK_FILENAME: &str = ".commonplace-sync.lock";

/// Acquires an exclusive lock on the sync directory to prevent multiple sync instances.
/// Returns the lock file handle which must be kept alive for the duration of the sync.
/// If the lock cannot be acquired, returns an error.
pub fn acquire_sync_lock(directory: &Path) -> io::Result<File> {
    let lock_path = directory.join(SYNC_LOCK_FILENAME);
    let lock_file = File::create(&lock_path)?;

    match lock_file.try_lock_exclusive() {
        Ok(()) => {
            info!("Acquired sync lock for directory: {}", directory.display());
            Ok(lock_file)
        }
        Err(e) => {
            error!(
                "Another sync process is already running for directory {}: {}",
                directory.display(),
                e
            );
            error!("Only one sync instance can run per checkout");
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "Another sync is already running for {}",
                    directory.display()
                ),
            ))
        }
    }
}

// =============================================================================
// Module re-exports for backward compatibility
// =============================================================================

// Re-export submodule modules so `crate::sync::yjs` etc. still work
pub use crdt::ack_handler;
pub use crdt::crdt_merge;
pub use crdt::crdt_new_file;
pub use crdt::crdt_publish;
pub use crdt::crdt_state;
pub use crdt::sync_state_machine;
pub use crdt::yjs;

pub use watcher::file_events;
#[cfg(unix)]
pub use watcher::flock;
pub use watcher::state;
// Note: watcher::watcher is accessed as watcher::watcher, not re-exported at root
// to avoid name conflict with the watcher submodule itself

pub use schema::content_type;
pub use schema::directory;
pub use schema::schema_io;
pub use schema::uuid_map;

pub use transport::ancestry;
pub use transport::client;
pub use transport::commands;
pub use transport::missing_parent;
pub use transport::peer_fallback;
pub use transport::shadow;
pub use transport::sse;
pub use transport::subscriptions;
pub use transport::urls;

// =============================================================================
// Item re-exports for backward compatibility
// =============================================================================

// --- From crdt/ ---
pub use crdt::ack_handler::{
    handle_ack_message, process_ack, AckResult, PendingCommit, PendingCommitTracker, MAX_RETRIES,
    PENDING_COMMIT_TIMEOUT,
};
pub use crdt::crdt_merge::{
    apply_commits_to_doc, create_and_publish_merge_commit, create_merge_commit,
    find_common_ancestor, parse_edit_message, process_received_edit, MergeCommitInput,
    MergeCommitResult, MergeResult,
};
pub use crdt::crdt_new_file::{
    create_new_file, generate_file_uuid, remove_file_from_schema, NewFileResult,
};
pub use crdt::crdt_publish::{
    apply_received_commit, get_text_content, is_commit_known, publish_text_change,
    publish_yjs_update, PublishResult,
};
pub use crdt::crdt_state::{
    load_or_migrate, migrate_from_old_state, CrdtPeerState, DirectorySyncState, MqttOnlySyncConfig,
    PendingEdit, QueueReason, SubdirStateCache, SyncBufferedEdit, SyncGuardrails,
    CRDT_STATE_FILENAME, MAX_PENDING_EDITS,
};
pub use crdt::sync_state_machine::{
    transition, ClientSyncState, InvalidTransition, SyncEvent, SyncStateMachine,
};
pub use crdt::yjs::{
    base64_decode, base64_encode, create_yjs_json_delete_key, create_yjs_json_merge,
    create_yjs_json_update, create_yjs_jsonl_update, create_yjs_structured_update,
    create_yjs_text_diff_update, create_yjs_text_update, json_value_to_any, TEXT_ROOT_NAME,
};
pub use crdt::ymap_schema;

// --- From watcher/ ---
pub use watcher::file_events::{
    ensure_parent_directories_exist, find_owning_document, handle_file_deleted,
    handle_file_modified, OwningDocument,
};
#[cfg(unix)]
pub use watcher::flock::{
    try_flock_exclusive, try_flock_shared, FlockGuard, FlockResult, FLOCK_RETRY_INTERVAL,
    FLOCK_TIMEOUT,
};
pub use watcher::flock_state;
pub use watcher::flock_state::FlockSyncState;
pub use watcher::state::{InodeKey, InodeState, InodeTracker, PendingWrite, SyncState};
#[cfg(unix)]
pub use watcher::watcher::spawn_shadow_tasks;
pub use watcher::watcher::{
    directory_watcher_task, file_watcher_task, shadow_watcher_task, wait_for_file_stability,
    ShadowWriteEvent, FILE_DEBOUNCE_MS, STABILITY_CHECK_INTERVAL_MS, STABILITY_MAX_WAIT_MS,
};

// --- From schema/ ---
pub use schema::content_type::{
    detect_from_path, ensure_trailing_newline, is_allowed_extension, is_binary_content,
    is_default_content, is_default_content_for_mime, looks_like_base64_binary, ContentTypeInfo,
    DEFAULT_XML_CONTENT,
};
pub use schema::directory::{
    scan_directory, scan_directory_to_json, scan_directory_with_contents, schema_to_json,
    ScanError, ScanOptions, ScannedFile,
};
pub use schema::schema_io::{
    fetch_and_validate_schema, write_nested_schemas, write_schema_file, FetchedSchema,
    SCHEMA_FILENAME,
};
pub use schema::uuid_map::{
    build_uuid_map_and_write_schemas, build_uuid_map_from_doc, build_uuid_map_recursive,
    collect_node_backed_dir_ids, collect_paths_from_entry, collect_paths_with_node_backed_dirs,
    fetch_node_id_from_schema, get_all_node_backed_dir_ids, wait_for_uuid_map_ready,
    UuidMapTimeoutError,
};

// --- From transport/ ---
pub use transport::ancestry::{determine_sync_direction, is_ancestor, SyncDirection};
pub use transport::client::{
    delete_schema_entry, discover_fs_root, fetch_head, fork_node, push_content_by_type,
    push_file_content, push_json_content, push_jsonl_content, push_schema_to_server,
    refresh_from_head, resolve_path_to_uuid_http, DiscoverFsRootError, FetchHeadError,
};
pub use transport::commands::{spawn_command_listener, CommandEntry};
pub use transport::missing_parent::{
    check_and_alert_missing_parents, handle_missing_parent_alert, new_rate_limiter,
    publish_missing_parent_alert, subscribe_to_missing_parent_alerts, RebroadcastRateLimiter,
    SharedRateLimiter, REBROADCAST_MAX_JITTER_MS, REBROADCAST_MIN_INTERVAL,
};
pub use transport::peer_fallback::{
    handle_peer_sync_request, new_peer_fallback_handler, subscribe_for_peer_fallback,
    PeerFallbackHandler, PeerFallbackStats, PendingRequest, SharedPeerFallbackHandler,
};
#[cfg(unix)]
pub use transport::shadow::{
    atomic_write_with_shadow, handle_shadow_write, shadow_write_handler_task,
    write_inbound_with_checks, write_inbound_with_checks_atomic, InboundWriteError,
    InboundWriteResult,
};
pub use transport::sse::{
    handle_server_edit, handle_server_edit_with_flock, sse_task, sse_task_with_flock,
};
#[cfg(unix)]
pub use transport::sse::{handle_server_edit_with_tracker, sse_task_with_tracker};
pub use transport::subscriptions::{
    directory_mqtt_task, spawn_subdir_mqtt_task, subdir_mqtt_task, trace_timeline,
    CrdtFileSyncContext, TimelineMilestone,
};
pub use transport::urls::{
    build_commits_url, build_edit_url, build_fork_url, build_head_at_commit_url, build_head_url,
    build_health_url, build_info_url, build_replace_url, build_sse_url, encode_node_id,
    encode_path, normalize_path,
};

// --- From root-level modules ---
pub use dir_sync::{
    apply_explicit_deletions, apply_schema_update_to_state, check_server_has_content,
    decode_schema_from_mqtt_payload, ensure_fs_root_exists, handle_schema_change,
    handle_schema_change_with_dedup, handle_schema_modified, push_nested_schemas, sync_schema,
    SchemaUpdateResult,
};
pub use error::{SyncError, SyncResult};
pub use file_sync::{
    initial_sync, prepare_content_for_upload, resync_crdt_state_via_cyan_with_pending,
    spawn_file_sync_tasks_crdt, sync_single_file, upload_task, upload_task_crdt, PreparedContent,
    PENDING_WRITE_TIMEOUT,
};
pub use types::{
    build_uuid_to_paths_map, remove_file_state_and_abort, CommitData, DirEvent, EditEventData,
    EditRequest, EditResponse, FileEvent, FileSyncState, ForkResponse, HeadResponse,
    InitialSyncComplete, ReplaceResponse, ReplaceSummary, SharedLastContent, SharedStateFile,
    UuidToPathsMap, WrittenSchemas,
};
