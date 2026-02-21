//! HTTP client operations for the sync client.
//!
//! This module contains functions for interacting with the Commonplace server
//! via HTTP: forking nodes and syncing schemas.

use super::urls::{build_edit_url, build_fork_url, build_head_url};
use crate::sync::{
    create_yjs_json_delete_key, create_yjs_json_merge, EditRequest, ForkResponse, HeadResponse,
};
use reqwest::Client;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{debug, info};

/// Global policy toggle used by `commonplace-sync` to hard-disable HTTP paths.
///
/// This is process-local and defaults to `false` to avoid impacting non-sync binaries.
static HTTP_DISABLED_IN_SYNC_RUNTIME: AtomicBool = AtomicBool::new(false);

/// Enable/disable HTTP operations used by sync transport helpers.
///
/// When enabled, helper functions in this module fail fast with a clear error so
/// remaining HTTP dependencies are surfaced immediately.
pub fn set_sync_http_disabled(disabled: bool) {
    HTTP_DISABLED_IN_SYNC_RUNTIME.store(disabled, Ordering::Relaxed);
}

/// Returns whether HTTP sync helpers are disabled for this process.
pub fn is_sync_http_disabled() -> bool {
    HTTP_DISABLED_IN_SYNC_RUNTIME.load(Ordering::Relaxed)
}

fn http_block_message(operation: &str) -> String {
    format!(
        "HTTP disabled in sync runtime; blocked operation: {} (use MQTT/cyan equivalent)",
        operation
    )
}

fn ensure_http_allowed(operation: &str) -> Result<(), io::Error> {
    if HTTP_DISABLED_IN_SYNC_RUNTIME.load(Ordering::Relaxed) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            http_block_message(operation),
        ));
    }
    Ok(())
}

/// Error from fetching HEAD
#[derive(Debug)]
pub enum FetchHeadError {
    /// HTTP request failed
    Request(reqwest::Error),
    /// JSON parsing failed
    Parse(reqwest::Error),
    /// Server returned error status (not 404)
    Status(reqwest::StatusCode, String),
}

impl std::fmt::Display for FetchHeadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(e) => write!(f, "HEAD request failed: {}", e),
            Self::Parse(e) => write!(f, "Failed to parse HEAD response: {}", e),
            Self::Status(code, body) => write!(f, "HEAD failed with {}: {}", code, body),
        }
    }
}

impl std::error::Error for FetchHeadError {}

/// Fetch HEAD for a document.
///
/// Returns:
/// - `Ok(Some(head))` if the document exists and HEAD was fetched successfully
/// - `Ok(None)` if the document doesn't exist (404)
/// - `Err(_)` if there was a network error or parse error
pub async fn fetch_head(
    client: &Client,
    server: &str,
    identifier: &str,
    use_paths: bool,
) -> Result<Option<HeadResponse>, FetchHeadError> {
    if let Err(e) = ensure_http_allowed("fetch_head") {
        return Err(FetchHeadError::Status(
            reqwest::StatusCode::FORBIDDEN,
            e.to_string(),
        ));
    }

    let head_url = build_head_url(server, identifier, use_paths);
    let resp = client
        .get(&head_url)
        .send()
        .await
        .map_err(FetchHeadError::Request)?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(FetchHeadError::Status(status, body));
    }

    let head = resp.json().await.map_err(FetchHeadError::Parse)?;
    Ok(Some(head))
}

/// Fork a node on the server, optionally at a specific commit.
pub async fn fork_node(
    client: &Client,
    server: &str,
    source_node: &str,
    at_commit: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    ensure_http_allowed("fork_node")?;

    let fork_url = build_fork_url(server, source_node, at_commit);

    let resp = client.post(&fork_url).send().await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Fork failed: {} - {}", status, body).into());
    }

    let fork_response: ForkResponse = resp.json().await?;
    info!(
        "Forked node {} -> {} (at commit {})",
        source_node,
        fork_response.id,
        &fork_response.head[..8.min(fork_response.head.len())]
    );

    Ok(fork_response.id)
}

/// Push a schema JSON document to the fs-root node on the server.
///
/// This function compares the new schema with the current server content
/// and skips the update if they are semantically equivalent, preventing
/// unnecessary SSE events that could cause feedback loops.
///
/// If the document doesn't exist on the server, it will be created first.
pub async fn push_schema_to_server(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    schema_json: &str,
    author: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ensure_http_allowed("push_schema_to_server")?;

    let edit_url = build_edit_url(server, fs_root_id, false);

    // First fetch current server content and state
    let (old_content, base_state) = match fetch_head(client, server, fs_root_id, false).await {
        Ok(Some(head)) => (Some(head.content), head.state),
        Ok(None) => {
            // Document doesn't exist (404), create it first
            info!("Creating document {} before pushing schema", fs_root_id);
            let create_url = format!("{}/docs", server);
            let create_resp = client
                .post(&create_url)
                .json(&serde_json::json!({
                    "type": "document",
                    "id": fs_root_id,
                    "content_type": "application/json"
                }))
                .send()
                .await?;
            if !create_resp.status().is_success() {
                let status = create_resp.status();
                let body = create_resp.text().await.unwrap_or_default();
                return Err(format!(
                    "Failed to create document {}: {} - {}",
                    fs_root_id, status, body
                )
                .into());
            }
            (None, None)
        }
        Err(FetchHeadError::Request(e)) => return Err(e.into()),
        Err(FetchHeadError::Parse(e)) => return Err(e.into()),
        Err(FetchHeadError::Status(code, body)) => {
            return Err(format!(
                "Failed to fetch HEAD for {}: {} - {}",
                fs_root_id, code, body
            )
            .into());
        }
    };

    // Skip update if schema hasn't changed (prevents feedback loops)
    if json_content_equal(old_content.as_deref(), schema_json) {
        debug!("Schema unchanged, skipping push to server");
        return Ok(());
    }

    // Create an additive merge (don't remove entries that other sync clients may have added)
    let update = create_yjs_json_merge(schema_json, base_state.as_deref())
        .map_err(|e| format!("Failed to create JSON update: {}", e))?;
    let edit_req = EditRequest {
        update,
        author: Some(author.to_string()),
        message: Some("Update filesystem schema".to_string()),
    };

    let resp = client.post(&edit_url).json(&edit_req).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to push schema: {} - {}", status, body).into());
    }

    Ok(())
}

/// Check if two JSON strings are semantically equal (ignoring whitespace/formatting).
fn json_content_equal(old: Option<&str>, new: &str) -> bool {
    if let Some(old) = old {
        let old_parsed: Result<serde_json::Value, _> = serde_json::from_str(old);
        let new_parsed: Result<serde_json::Value, _> = serde_json::from_str(new);
        if let (Ok(old_json), Ok(new_json)) = (old_parsed, new_parsed) {
            return old_json == new_json;
        }
    }
    false
}

/// Delete a specific entry from a schema on the server.
///
/// This function removes a single entry without affecting other entries,
/// allowing file deletions to propagate while preserving entries from other clients.
pub async fn delete_schema_entry(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    entry_name: &str,
    author: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ensure_http_allowed("delete_schema_entry")?;

    let edit_url = build_edit_url(server, fs_root_id, false);

    // Fetch current server state
    let base_state = match fetch_head(client, server, fs_root_id, false).await {
        Ok(Some(head)) => head.state,
        Ok(None) => None, // Document doesn't exist (404), proceed with None state
        Err(FetchHeadError::Request(e)) => return Err(e.into()),
        Err(FetchHeadError::Parse(e)) => return Err(e.into()),
        Err(FetchHeadError::Status(code, body)) => {
            return Err(format!(
                "Failed to fetch HEAD for {}: {} - {}",
                fs_root_id, code, body
            )
            .into());
        }
    };

    // Create an update that deletes just this entry
    let key_path = format!("root.entries.{}", entry_name);
    let update = create_yjs_json_delete_key(&key_path, base_state.as_deref())
        .map_err(|e| format!("Failed to create delete update: {}", e))?;

    let edit_req = EditRequest {
        update,
        author: Some(author.to_string()),
        message: Some(format!("Delete schema entry: {}", entry_name)),
    };

    let resp = client.post(&edit_url).json(&edit_req).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to delete schema entry: {} - {}", status, body).into());
    }

    info!("Deleted schema entry: {}", entry_name);
    Ok(())
}

/// Resolve a path to a UUID by traversing the fs-root schema hierarchy.
///
/// Fetches schemas from the server and follows node_id references for nested paths.
/// For example, "bartleby/script.js" would:
/// 1. Fetch fs-root schema, find bartleby's node_id
/// 2. Fetch bartleby's schema, find script.js's node_id
pub async fn resolve_path_to_uuid_http(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    path: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    ensure_http_allowed("resolve_path_to_uuid_http")?;

    use serde::Deserialize;
    use std::collections::HashMap;

    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return Ok(fs_root_id.to_string());
    }

    let mut current_id = fs_root_id.to_string();

    for (i, segment) in segments.iter().enumerate() {
        let head = match fetch_head(client, server, &current_id, false).await {
            Ok(Some(h)) => h,
            Ok(None) => {
                return Err(format!(
                    "Failed to fetch schema for '{}': document not found",
                    segments[..=i].join("/")
                )
                .into());
            }
            Err(e) => {
                return Err(format!(
                    "Failed to fetch schema for '{}': {}",
                    segments[..=i].join("/"),
                    e
                )
                .into());
            }
        };

        #[derive(Deserialize, Default)]
        struct Schema {
            #[serde(default)]
            root: SchemaRoot,
        }

        #[derive(Deserialize, Default)]
        struct SchemaRoot {
            entries: Option<HashMap<String, SchemaEntry>>,
        }

        #[derive(Deserialize)]
        struct SchemaEntry {
            node_id: Option<String>,
        }

        let schema: Schema = if head.content.trim() == "{}" {
            Schema::default()
        } else {
            serde_json::from_str(&head.content)?
        };

        let entries = schema.root.entries.ok_or_else(|| {
            format!(
                "Path '{}' not found: '{}' has no entries",
                path,
                if i == 0 {
                    "fs-root".to_string()
                } else {
                    segments[..i].join("/")
                }
            )
        })?;

        let entry = entries.get(*segment).ok_or_else(|| {
            let available: Vec<_> = entries.keys().collect();
            format!(
                "Path '{}' not found: no entry '{}' in '{}'. Available: {:?}",
                path,
                segment,
                if i == 0 {
                    "fs-root".to_string()
                } else {
                    segments[..i].join("/")
                },
                available
            )
        })?;

        let node_id = entry.node_id.clone().ok_or_else(|| {
            format!(
                "Path '{}' not found: entry '{}' has no node_id",
                path, segment
            )
        })?;

        current_id = node_id;
    }

    Ok(current_id)
}

/// Error from discovering fs-root
#[derive(Debug)]
pub enum DiscoverFsRootError {
    /// HTTP request failed
    Request(reqwest::Error),
    /// JSON parsing failed
    Parse(reqwest::Error),
    /// Server not configured with --fs-root (503)
    NotConfigured,
    /// Server returned error status
    Status(reqwest::StatusCode, String),
}

impl std::fmt::Display for DiscoverFsRootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(e) => write!(f, "fs-root request failed: {}", e),
            Self::Parse(e) => write!(f, "Failed to parse fs-root response: {}", e),
            Self::NotConfigured => write!(f, "Server was not started with --fs-root"),
            Self::Status(code, body) => write!(f, "fs-root failed with {}: {}", code, body),
        }
    }
}

impl std::error::Error for DiscoverFsRootError {}

/// Response from GET /fs-root endpoint.
#[derive(Debug, serde::Deserialize)]
struct FsRootResponse {
    id: String,
}

/// Discover the fs-root document ID from the server.
///
/// Queries the GET /fs-root endpoint to get the fs-root ID.
/// This allows sync to work with --use-paths without requiring --node.
///
/// Returns:
/// - `Ok(id)` if the fs-root was discovered successfully
/// - `Err(NotConfigured)` if the server wasn't started with --fs-root (503)
/// - `Err(_)` if there was a network error or parse error
pub async fn discover_fs_root(
    client: &Client,
    server: &str,
) -> Result<String, DiscoverFsRootError> {
    if let Err(e) = ensure_http_allowed("discover_fs_root") {
        return Err(DiscoverFsRootError::Status(
            reqwest::StatusCode::FORBIDDEN,
            e.to_string(),
        ));
    }

    let url = format!("{}/fs-root", server);
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(DiscoverFsRootError::Request)?;

    if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Err(DiscoverFsRootError::NotConfigured);
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(DiscoverFsRootError::Status(status, body));
    }

    let response: FsRootResponse = resp.json().await.map_err(DiscoverFsRootError::Parse)?;
    Ok(response.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn http_flag_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("http flag test lock poisoned")
    }

    #[tokio::test]
    async fn fetch_head_fails_fast_when_http_disabled() {
        let _guard = http_flag_test_lock();
        set_sync_http_disabled(true);
        let client = Client::new();

        let result = fetch_head(&client, "http://127.0.0.1:1", "any-id", false).await;
        set_sync_http_disabled(false);

        match result {
            Err(FetchHeadError::Status(code, body)) => {
                assert_eq!(code, reqwest::StatusCode::FORBIDDEN);
                assert!(body.contains("HTTP disabled in sync runtime"));
            }
            other => panic!("expected forbidden status error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn discover_fs_root_fails_fast_when_http_disabled() {
        let _guard = http_flag_test_lock();
        set_sync_http_disabled(true);
        let client = Client::new();

        let result = discover_fs_root(&client, "http://127.0.0.1:1").await;
        set_sync_http_disabled(false);

        match result {
            Err(DiscoverFsRootError::Status(code, body)) => {
                assert_eq!(code, reqwest::StatusCode::FORBIDDEN);
                assert!(body.contains("HTTP disabled in sync runtime"));
            }
            other => panic!("expected forbidden status error, got {:?}", other),
        }
    }
}
