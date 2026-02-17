//! Schema-related utilities for directory sync.
//!
//! This module contains functionality for:
//! - Schema I/O (reading/writing .commonplace.json files)
//! - Directory scanning and FS JSON generation
//! - UUID map building and path resolution
//! - Content type detection

pub mod content_type;
pub mod directory;
pub mod schema_io;
pub mod uuid_map;

// Re-export all public items from submodules to maintain backward compatibility.

// From content_type
pub use content_type::{
    detect_from_path, ensure_trailing_newline, is_allowed_extension, is_binary_content,
    is_default_content, is_default_content_for_mime, looks_like_base64_binary, ContentTypeInfo,
    ALLOWED_EXTENSIONS, DEFAULT_XML_CONTENT,
};

// From directory
pub use directory::{
    scan_directory, scan_directory_to_json, scan_directory_with_contents, schema_to_json,
    ScanError, ScanOptions, ScannedFile,
};

// From schema_io
pub use schema_io::{
    fetch_and_validate_schema, set_sync_schema_mqtt_request_client, write_nested_schemas,
    write_schema_file, FetchedSchema, SCHEMA_FILENAME,
};

// From uuid_map
pub use uuid_map::{
    build_uuid_map_and_write_schemas, build_uuid_map_from_doc, build_uuid_map_from_doc_with_status,
    build_uuid_map_recursive, build_uuid_map_recursive_with_status, collect_node_backed_dir_ids,
    collect_paths_from_entry, collect_paths_with_node_backed_dirs,
    collect_paths_with_node_backed_dirs_with_status, fetch_node_id_from_schema,
    fetch_subdir_node_id, get_all_node_backed_dir_ids, wait_for_uuid_map_ready,
    UuidMapTimeoutError,
};
