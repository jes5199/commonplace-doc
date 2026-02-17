//! Schema I/O operations for reading and writing .commonplace.json files.
//!
//! Pure functions are re-exported from the `commonplace-schema` crate.
//! Runtime fetch helpers prefer MQTT request/response when available.

pub use commonplace_schema::schema_io::{
    create_nested_directories, write_schema_file, FetchedSchema, SCHEMA_FILENAME,
};

use crate::fs::{Entry, FsSchema};
use crate::mqtt::MqttRequestClient;
use crate::sync::client::fetch_head;
use crate::sync::types::WrittenSchemas;
use crate::sync::urls::encode_node_id;
use reqwest::Client;
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use tracing::{debug, warn};

/// Process-local MQTT request client used by schema fetch helpers in sync runtime.
///
/// This is set by `commonplace-sync` startup. When unset, helpers fall back to
/// HTTP so non-sync binaries and tests keep existing behavior.
static SYNC_SCHEMA_MQTT_REQUEST_CLIENT: OnceLock<RwLock<Option<Arc<MqttRequestClient>>>> =
    OnceLock::new();

fn mqtt_request_slot() -> &'static RwLock<Option<Arc<MqttRequestClient>>> {
    SYNC_SCHEMA_MQTT_REQUEST_CLIENT.get_or_init(|| RwLock::new(None))
}

fn get_sync_schema_mqtt_request_client() -> Option<Arc<MqttRequestClient>> {
    let guard = mqtt_request_slot()
        .read()
        .expect("schema mqtt request client lock poisoned");
    guard.clone()
}

/// Register or clear the MQTT request client used by schema fetch helpers.
pub fn set_sync_schema_mqtt_request_client(client: Option<Arc<MqttRequestClient>>) {
    let mut guard = mqtt_request_slot()
        .write()
        .expect("schema mqtt request client lock poisoned");
    *guard = client;
}

/// Fetch schema content, preferring MQTT when available and falling back to HTTP.
pub(crate) async fn fetch_schema_content_with_optional_cid(
    client: &Client,
    server: &str,
    node_id: &str,
) -> Result<Option<(String, Option<String>)>, String> {
    if let Some(mqtt_request) = get_sync_schema_mqtt_request_client() {
        match mqtt_request.get_content(node_id).await {
            Ok(resp) => {
                if let Some(err) = resp.error {
                    if err.to_ascii_lowercase().contains("not found") {
                        return Ok(None);
                    }
                    return Err(format!("MQTT get-content failed for {}: {}", node_id, err));
                }
                if let Some(content) = resp.content {
                    // get-content does not currently include commit metadata.
                    return Ok(Some((content, None)));
                }
                return Err(format!(
                    "MQTT get-content returned no content for {}",
                    node_id
                ));
            }
            Err(e) => {
                return Err(format!(
                    "MQTT get-content request failed for {}: {}",
                    node_id, e
                ));
            }
        }
    }

    match fetch_head(client, server, &encode_node_id(node_id), false).await {
        Ok(Some(h)) => Ok(Some((h.content, h.cid))),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("HTTP HEAD fetch failed for {}: {}", node_id, e)),
    }
}

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
    let (content, cid) = match fetch_schema_content_with_optional_cid(client, server, node_id).await
    {
        Ok(Some((content, cid))) => (content, cid),
        Ok(None) => {
            debug!("Schema not found for {}", node_id);
            return None;
        }
        Err(e) => {
            warn!("Failed to fetch schema for {}: {}", node_id, e);
            return None;
        }
    };

    if content.is_empty() {
        debug!("Schema content is empty for {}", node_id);
        return None;
    }

    let schema: FsSchema = match serde_json::from_str(&content) {
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
        content,
        cid,
    })
}
