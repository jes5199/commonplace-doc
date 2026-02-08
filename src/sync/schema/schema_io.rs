//! Schema I/O operations for reading and writing .commonplace.json files.
//!
//! Pure functions are re-exported from the `commonplace-schema` crate.
//! HTTP-dependent functions (fetch_and_validate_schema) remain here.

pub use commonplace_schema::schema_io::{
    create_nested_directories, write_schema_file, FetchedSchema, SCHEMA_FILENAME,
};

use crate::fs::{Entry, FsSchema};
use crate::sync::client::fetch_head;
use crate::sync::types::WrittenSchemas;
use crate::sync::urls::encode_node_id;
use reqwest::Client;
use std::path::Path;
use tracing::{debug, warn};

/// Create directories for node-backed subdirectories.
///
/// Backward-compatible wrapper that accepts unused client/server params.
pub async fn write_nested_schemas(
    _client: &Client,
    _server: &str,
    directory: &Path,
    schema: &FsSchema,
    _written_schemas: Option<&WrittenSchemas>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    commonplace_schema::schema_io::write_nested_schemas(directory, schema).await
}

/// Fetch a schema from the server, parse it, and optionally validate it.
///
/// Returns `None` if:
/// - The fetch fails
/// - The content is empty
/// - Parsing fails
/// - `require_valid_root` is true and schema doesn't have a valid root
///
/// A valid root is a directory with either entries or a node_id.
pub async fn fetch_and_validate_schema(
    client: &Client,
    server: &str,
    node_id: &str,
    require_valid_root: bool,
) -> Option<FetchedSchema> {
    let head = match fetch_head(client, server, &encode_node_id(node_id), false).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            debug!("Schema not found for {}", node_id);
            return None;
        }
        Err(e) => {
            warn!("Failed to fetch schema for {}: {:?}", node_id, e);
            return None;
        }
    };

    if head.content.is_empty() {
        debug!("Schema content is empty for {}", node_id);
        return None;
    }

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse schema for {}: {}", node_id, e);
            return None;
        }
    };

    if require_valid_root {
        let has_valid_root = match &schema.root {
            Some(Entry::Dir(dir)) => dir.entries.is_some() || dir.node_id.is_some(),
            _ => false,
        };
        if !has_valid_root {
            warn!(
                "Schema for {} has no valid root (missing entries or node_id)",
                node_id
            );
            return None;
        }
    }

    Some(FetchedSchema {
        schema,
        content: head.content,
        cid: head.cid,
    })
}
