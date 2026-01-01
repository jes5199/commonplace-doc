//! Sync utilities for directory synchronization.

pub mod client;
pub mod content_type;
pub mod dir_sync;
pub mod directory;
pub mod file_sync;
pub mod sse;
pub mod state;
pub mod state_file;
pub mod types;
pub mod urls;
pub mod watcher;
pub mod yjs;

pub use client::{fork_node, push_file_content, push_json_content, push_schema_to_server};
pub use dir_sync::{
    build_uuid_map_from_doc, build_uuid_map_recursive, check_server_has_content,
    collect_paths_from_entry, collect_paths_with_node_backed_dirs, directory_sse_task,
    ensure_fs_root_exists, fetch_node_id_from_schema, handle_file_created, handle_file_deleted,
    handle_file_modified, handle_schema_change, sync_schema, write_schema_file, SCHEMA_FILENAME,
};
pub use file_sync::{
    initial_sync, spawn_file_sync_tasks, sync_single_file, upload_task, BARRIER_RETRY_COUNT,
    BARRIER_RETRY_DELAY,
};

pub use content_type::{
    detect_from_path, is_allowed_extension, is_binary_content, ContentTypeInfo,
};
pub use directory::{
    scan_directory, scan_directory_with_contents, schema_to_json, ScanError, ScanOptions,
    ScannedFile,
};
pub use sse::{handle_server_edit, refresh_from_head, sse_task, PENDING_WRITE_TIMEOUT};
pub use state::{PendingWrite, SyncState};
pub use types::{
    CommitData, DirEvent, EditEventData, EditRequest, EditResponse, FileEvent, FileSyncState,
    ForkResponse, HeadResponse, ReplaceResponse, ReplaceSummary,
};
pub use urls::{
    build_edit_url, build_fork_url, build_head_url, build_replace_url, build_sse_url,
    encode_node_id, encode_path, normalize_path,
};
pub use watcher::{directory_watcher_task, file_watcher_task};
pub use yjs::{
    base64_decode, base64_encode, create_yjs_json_update, create_yjs_text_diff_update,
    create_yjs_text_update, json_value_to_any, TEXT_ROOT_NAME,
};
