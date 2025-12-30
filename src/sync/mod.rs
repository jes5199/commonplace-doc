//! Sync utilities for directory synchronization.

pub mod client;
pub mod content_type;
pub mod dir_sync;
pub mod directory;
pub mod file_sync;
pub mod sse;
pub mod state_file;
pub mod types;
pub mod urls;
pub mod yjs;

pub use content_type::{
    detect_from_path, is_allowed_extension, is_binary_content, ContentTypeInfo,
};
pub use directory::{
    scan_directory, scan_directory_with_contents, schema_to_json, ScanError, ScanOptions,
    ScannedFile,
};
pub use types::{
    CommitData, DirEvent, EditEventData, EditRequest, EditResponse, FileEvent, ForkResponse,
    HeadResponse, ReplaceResponse, ReplaceSummary,
};
pub use urls::{
    build_edit_url, build_fork_url, build_head_url, build_replace_url, build_sse_url,
    encode_node_id, encode_path, normalize_path,
};
pub use yjs::{
    base64_decode, base64_encode, create_yjs_json_update, create_yjs_text_diff_update,
    create_yjs_text_update, json_value_to_any, TEXT_ROOT_NAME,
};
