//! CRDT-related modules for peer sync.
//!
//! This submodule contains all CRDT-specific functionality for the sync client:
//! - State management (crdt_state)
//! - Merge logic (crdt_merge)
//! - Publishing changes (crdt_publish)
//! - New file creation (crdt_new_file)
//! - Yjs utilities (yjs)
//! - YMap schema operations (ymap_schema)
//! - Sync state machine (sync_state_machine)
//! - Ack handling (ack_handler)

pub mod ack_handler;
pub mod crdt_merge;
pub mod crdt_new_file;
pub mod crdt_publish;
pub mod crdt_state;
pub mod sync_state_machine;
pub mod yjs;
pub mod ymap_schema;

// Re-export public items for convenience
pub use ack_handler::{
    handle_ack_message, process_ack, AckResult, PendingCommit, PendingCommitTracker, MAX_RETRIES,
    PENDING_COMMIT_TIMEOUT,
};
pub use crdt_merge::{
    apply_commits_to_doc, create_and_publish_merge_commit, create_merge_commit,
    find_common_ancestor, parse_edit_message, process_received_edit, MergeCommitInput,
    MergeCommitResult, MergeResult,
};
pub use crdt_new_file::{
    create_new_file, generate_file_uuid, remove_file_from_schema, NewFileResult,
};
pub use crdt_publish::{
    apply_received_commit, get_text_content, is_commit_known, publish_text_change,
    publish_yjs_update, PublishResult,
};
pub use crdt_state::{
    load_or_migrate, migrate_from_old_state, CrdtPeerState, DirectorySyncState, MqttOnlySyncConfig,
    PendingEdit, QueueReason, SubdirStateCache, SyncBufferedEdit, SyncGuardrails,
    CRDT_STATE_FILENAME, MAX_PENDING_EDITS,
};
pub use sync_state_machine::{
    transition, ClientSyncState, InvalidTransition, SyncEvent, SyncStateMachine,
};
pub use yjs::{
    any_to_json_value, base64_decode, base64_encode, create_yjs_json_delete_key,
    create_yjs_json_merge, create_yjs_json_update, create_yjs_jsonl_update,
    create_yjs_structured_update, create_yjs_text_diff_update, create_yjs_text_update,
    json_value_to_any, yjs_array_to_jsonl, TEXT_ROOT_NAME,
};
pub use ymap_schema::{
    add_directory, add_file, from_fs_schema, get_entry, is_json_text_format, is_ymap_format,
    list_entries, migrate_from_json_text, remove_entry, to_fs_schema, SchemaEntry, SchemaEntryType,
};
