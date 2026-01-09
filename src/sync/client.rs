//! HTTP client operations for the sync client.
//!
//! This module contains functions for interacting with the Commonplace server
//! via HTTP: forking nodes, pushing content, and syncing schemas.

use crate::sync::{
    build_edit_url, build_fork_url, build_head_url, build_replace_url, create_yjs_json_delete_key,
    create_yjs_json_merge, create_yjs_json_update, create_yjs_jsonl_update, create_yjs_text_update,
    encode_node_id, EditRequest, EditResponse, ForkResponse, HeadResponse, ReplaceResponse,
    SyncState,
};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Fork a node on the server, optionally at a specific commit.
pub async fn fork_node(
    client: &Client,
    server: &str,
    source_node: &str,
    at_commit: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
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
) -> Result<(), Box<dyn std::error::Error>> {
    // First fetch current server content and state to detect deletions
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    let head_resp = client.get(&head_url).send().await?;
    let (old_content, base_state) = if head_resp.status().is_success() {
        let head: HeadResponse = head_resp.json().await?;
        (Some(head.content), head.state)
    } else {
        // Document doesn't exist, create it first
        tracing::info!("Creating document {} before pushing schema", fs_root_id);
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
    };

    // Skip update if schema hasn't changed (prevents feedback loops)
    // Compare as parsed JSON to ignore whitespace/formatting differences
    if let Some(ref old) = old_content {
        let old_parsed: Result<serde_json::Value, _> = serde_json::from_str(old);
        let new_parsed: Result<serde_json::Value, _> = serde_json::from_str(schema_json);
        if let (Ok(old_json), Ok(new_json)) = (old_parsed, new_parsed) {
            if old_json == new_json {
                tracing::debug!("Schema unchanged, skipping push to server");
                return Ok(());
            }
        }
    }

    // Create an additive merge (don't remove entries that other sync clients may have added)
    // This allows multiple sync clients to each contribute their own schema entries
    let update = create_yjs_json_merge(schema_json, base_state.as_deref())
        .map_err(|e| format!("Failed to create JSON update: {}", e))?;
    let edit_url = format!("{}/docs/{}/edit", server, encode_node_id(fs_root_id));
    let edit_req = EditRequest {
        update,
        author: Some("sync-client".to_string()),
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

/// Delete a specific entry from a schema on the server.
///
/// This function removes a single entry without affecting other entries,
/// allowing file deletions to propagate while preserving entries from other clients.
pub async fn delete_schema_entry(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    entry_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fetch current server state
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    let head_resp = client.get(&head_url).send().await?;
    let base_state = if head_resp.status().is_success() {
        let head: HeadResponse = head_resp.json().await?;
        head.state
    } else {
        None
    };

    // Create an update that deletes just this entry
    let key_path = format!("root.entries.{}", entry_name);
    let update = create_yjs_json_delete_key(&key_path, base_state.as_deref())
        .map_err(|e| format!("Failed to create delete update: {}", e))?;

    let edit_url = format!("{}/docs/{}/edit", server, encode_node_id(fs_root_id));
    let edit_req = EditRequest {
        update,
        author: Some("sync-client".to_string()),
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

/// Push JSON content to a node using Y.Map/Y.Array updates.
pub async fn push_json_content(
    client: &Client,
    server: &str,
    identifier: &str,
    content: &str,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let head_url = build_head_url(server, identifier, use_paths);
    let edit_url = build_edit_url(server, identifier, use_paths);
    let mut attempts = 0;
    let max_attempts = 30; // 3 seconds max wait

    loop {
        let head_resp = client.get(&head_url).send().await?;
        if head_resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let base_state = if head_resp.status().is_success() {
            let head: HeadResponse = head_resp.json().await?;
            head.state
        } else {
            None
        };

        let update = create_yjs_json_update(content, base_state.as_deref())?;
        let edit_req = EditRequest {
            update,
            author: Some("sync-client".to_string()),
            message: Some("Sync JSON content".to_string()),
        };

        let resp = client.post(&edit_url).json(&edit_req).send().await?;
        if resp.status().is_success() {
            let result: EditResponse = resp.json().await?;
            let mut s = state.write().await;
            s.last_written_cid = Some(result.cid);
            s.last_written_content = content.to_string();
            return Ok(());
        }

        if resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to push JSON content: {}", body).into());
    }
}

/// Push JSONL content to a node using Y.Array updates.
pub async fn push_jsonl_content(
    client: &Client,
    server: &str,
    identifier: &str,
    content: &str,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let head_url = build_head_url(server, identifier, use_paths);
    let edit_url = build_edit_url(server, identifier, use_paths);
    let mut attempts = 0;
    let max_attempts = 30; // 3 seconds max wait

    loop {
        let head_resp = client.get(&head_url).send().await?;
        if head_resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let base_state = if head_resp.status().is_success() {
            let head: HeadResponse = head_resp.json().await?;
            head.state
        } else {
            None
        };

        let update = create_yjs_jsonl_update(content, base_state.as_deref())?;
        let edit_req = EditRequest {
            update,
            author: Some("sync-client".to_string()),
            message: Some("Sync JSONL content".to_string()),
        };

        let resp = client.post(&edit_url).json(&edit_req).send().await?;
        if resp.status().is_success() {
            let result: EditResponse = resp.json().await?;
            let mut s = state.write().await;
            s.last_written_cid = Some(result.cid);
            s.last_written_content = content.to_string();
            return Ok(());
        }

        if resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to push JSONL content: {}", body).into());
    }
}

/// Push file content to a node (text files use replace/edit endpoints).
pub async fn push_file_content(
    client: &Client,
    server: &str,
    identifier: &str,
    content: &str,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // First check if there's existing content
    let head_url = build_head_url(server, identifier, use_paths);
    let head_resp = client.get(&head_url).send().await;

    match head_resp {
        Ok(resp) if resp.status().is_success() => {
            let head: HeadResponse = resp.json().await?;
            if let Some(parent_cid) = head.cid {
                // Use replace endpoint
                let replace_url = build_replace_url(server, identifier, &parent_cid, use_paths);
                let resp = client
                    .post(&replace_url)
                    .header("content-type", "text/plain")
                    .body(content.to_string())
                    .send()
                    .await?;
                if resp.status().is_success() {
                    let result: ReplaceResponse = resp.json().await?;
                    let mut s = state.write().await;
                    s.last_written_cid = Some(result.cid);
                    s.last_written_content = content.to_string();
                }
                return Ok(());
            }
        }
        _ => {}
    }

    // No existing content, use edit endpoint with retry for node creation
    debug!("Using edit endpoint for initial content: {}", identifier);
    let update = create_yjs_text_update(content);
    let edit_url = build_edit_url(server, identifier, use_paths);
    let edit_req = EditRequest {
        update,
        author: Some("sync-client".to_string()),
        message: Some("Initial file content".to_string()),
    };

    // Retry loop: wait for node to be created by reconciler
    // The reconciler processes schema changes asynchronously, so we need to wait
    let mut attempts = 0;
    let max_attempts = 30; // 3 seconds max wait
    loop {
        let resp = client.post(&edit_url).json(&edit_req).send().await?;
        if resp.status().is_success() {
            let result: EditResponse = resp.json().await?;
            let mut s = state.write().await;
            s.last_written_cid = Some(result.cid);
            s.last_written_content = content.to_string();
            info!("Successfully pushed content for: {}", identifier);
            return Ok(());
        } else if resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            // Node not created yet by reconciler, wait and retry
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
        } else {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to push content for {}: {}", identifier, body);
            return Ok(());
        }
    }
}
