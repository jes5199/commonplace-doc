//! Sync utilities for directory synchronization.

use fs2::FileExt;
use std::fs::File;
use std::io;
use std::path::Path;
use tracing::{error, info};

pub mod ancestry;
pub mod client;
pub mod commands;
pub mod content_type;
pub mod crdt_merge;
pub mod crdt_new_file;
pub mod crdt_publish;
pub mod crdt_state;
pub mod dir_sync;
pub mod directory;
pub mod file_events;
pub mod file_sync;
#[cfg(unix)]
pub mod flock;
pub mod flock_state;
pub mod schema_io;
pub mod sse;
pub mod state;
pub mod state_file;
pub mod subdir_spawn;
pub mod subscriptions;
pub mod types;
pub mod urls;
pub mod uuid_map;
pub mod watcher;
pub mod yjs;

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

pub use client::{
    delete_schema_entry, discover_fs_root, fetch_head, fork_node, push_content_by_type,
    push_file_content, push_json_content, push_jsonl_content, push_schema_to_server,
    resolve_path_to_uuid_http, DiscoverFsRootError, FetchHeadError,
};
pub use dir_sync::{
    check_server_has_content, ensure_fs_root_exists, handle_schema_change, handle_schema_modified,
    push_nested_schemas, sync_schema,
};
pub use file_events::{
    ensure_parent_directories_exist, find_owning_document, handle_file_created,
    handle_file_deleted, handle_file_modified, OwningDocument,
};
pub use file_sync::{
    initial_sync, spawn_file_sync_tasks, spawn_file_sync_tasks_crdt,
    spawn_file_sync_tasks_with_flock, sync_single_file, upload_task, upload_task_crdt,
    upload_task_with_flock,
};
pub use schema_io::{
    fetch_and_validate_schema, write_nested_schemas, write_schema_file, FetchedSchema,
    SCHEMA_FILENAME,
};
pub use subscriptions::{
    directory_mqtt_task, directory_sse_task, spawn_subdir_mqtt_task, spawn_subdir_sse_task,
    subdir_mqtt_task, subdir_sse_task,
};
pub use uuid_map::{
    build_uuid_map_and_write_schemas, build_uuid_map_from_doc, build_uuid_map_recursive,
    collect_node_backed_dir_ids, collect_paths_from_entry, collect_paths_with_node_backed_dirs,
    fetch_node_id_from_schema, get_all_node_backed_dir_ids,
};

pub use ancestry::{determine_sync_direction, is_ancestor, SyncDirection};

pub use content_type::{
    detect_from_path, is_allowed_extension, is_binary_content, looks_like_base64_binary,
    ContentTypeInfo,
};
pub use directory::{
    scan_directory, scan_directory_to_json, scan_directory_with_contents, schema_to_json,
    ScanError, ScanOptions, ScannedFile,
};
#[cfg(unix)]
pub use flock::{
    try_flock_exclusive, try_flock_shared, FlockGuard, FlockResult, FLOCK_RETRY_INTERVAL,
    FLOCK_TIMEOUT,
};
pub use flock_state::FlockSyncState;
// Note: process_pending_inbound_after_confirm, record_upload_result, InboundWrite, PathState
// are internal to sync module
#[cfg(unix)]
pub use sse::{
    atomic_write_with_shadow, handle_server_edit_with_tracker, handle_shadow_write,
    shadow_write_handler_task, sse_task_with_tracker, write_inbound_with_checks,
    write_inbound_with_checks_atomic, InboundWriteError, InboundWriteResult,
};
pub use sse::{
    handle_server_edit, handle_server_edit_with_flock, refresh_from_head, sse_task,
    sse_task_with_flock, PENDING_WRITE_TIMEOUT,
};
// Note: hardlink_from_fd, hardlink_from_path, SHADOW_IDLE_TIMEOUT, SHADOW_MIN_LIFETIME
// are internal to sync module
pub use state::{InodeKey, InodeState, InodeTracker, PendingWrite, SyncState};
pub use types::{
    build_uuid_to_paths_map, remove_file_state_and_abort, CommitData, DirEvent, EditEventData,
    EditRequest, EditResponse, FileEvent, FileSyncState, ForkResponse, HeadResponse,
    InitialSyncComplete, ReplaceResponse, ReplaceSummary, SharedStateFile, UuidToPathsMap,
    WrittenSchemas,
};
pub use urls::{
    build_commits_url, build_edit_url, build_fork_url, build_head_at_commit_url, build_head_url,
    build_health_url, build_info_url, build_replace_url, build_sse_url, encode_node_id,
    encode_path, normalize_path,
};
#[cfg(unix)]
pub use watcher::spawn_shadow_tasks;
pub use watcher::{directory_watcher_task, file_watcher_task, ShadowWriteEvent};
// Note: shadow_gc_task, shadow_watcher_task, SHADOW_GC_INTERVAL are internal
pub use yjs::{
    base64_decode, base64_encode, create_yjs_json_delete_key, create_yjs_json_merge,
    create_yjs_json_update, create_yjs_jsonl_update, create_yjs_structured_update,
    create_yjs_text_diff_update, create_yjs_text_update, json_value_to_any, TEXT_ROOT_NAME,
};

pub use commands::{spawn_command_listener, CommandEntry};
pub use crdt_merge::{parse_edit_message, process_received_edit, MergeResult};
pub use crdt_new_file::{
    create_new_file, generate_file_uuid, remove_file_from_schema, NewFileResult,
};
pub use crdt_publish::{
    apply_received_commit, get_text_content, is_commit_known, publish_text_change,
    publish_yjs_update, PublishResult,
};
pub use crdt_state::{
    load_or_migrate, migrate_from_old_state, CrdtPeerState, DirectorySyncState, CRDT_STATE_FILENAME,
};
