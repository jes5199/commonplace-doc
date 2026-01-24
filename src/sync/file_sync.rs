//! File mode synchronization.
//!
//! This module contains functions for syncing a single file with a server document.

use crate::events::recv_broadcast;
use crate::mqtt::{MqttClient, Topic};
use crate::sync::client::fetch_head;
use crate::sync::crdt_merge::{parse_edit_message, process_received_edit};
use crate::sync::crdt_publish::publish_text_change;
use crate::sync::crdt_state::DirectorySyncState;
use crate::sync::directory::{scan_directory_to_json, ScanOptions};
use crate::sync::file_events::find_owning_document;
use crate::sync::state::InodeKey;
use crate::sync::state_file::compute_content_hash;
use crate::sync::uuid_map::fetch_node_id_from_schema;
use crate::sync::{
    build_edit_url, build_replace_url, create_yjs_text_update, detect_from_path, error::SyncResult,
    file_watcher_task, flock_state::record_upload_result, is_binary_content,
    looks_like_base64_binary, push_json_content, push_jsonl_content, push_schema_to_server,
    refresh_from_head, sse_task, EditRequest, EditResponse, FileEvent, FlockSyncState,
    ReplaceResponse, SharedLastContent, SyncState, PENDING_WRITE_TIMEOUT,
};
use reqwest::Client;
use rumqttc::QoS;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Number of retries when content differs during a pending write (handles partial writes)
pub const BARRIER_RETRY_COUNT: u32 = 5;
/// Delay between retries when checking for stable content
pub const BARRIER_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Handle refresh logic after an upload attempt.
///
/// This helper encapsulates the common pattern of refreshing from HEAD after upload,
/// handling both success and failure cases consistently. If refresh is needed and
/// upload succeeded, it attempts to refresh from HEAD. If either upload or refresh
/// fails, it sets the needs_head_refresh flag so refresh is retried later.
///
/// # Arguments
/// * `should_refresh` - Whether a refresh was requested (e.g., due to skipped SSE events)
/// * `upload_succeeded` - Whether the upload operation succeeded
/// * Other args are passed through to `refresh_from_head`
#[allow(clippy::too_many_arguments)]
async fn handle_upload_refresh(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
    should_refresh: bool,
    upload_succeeded: bool,
) {
    if should_refresh {
        if upload_succeeded {
            let refresh_succeeded =
                refresh_from_head(client, server, identifier, file_path, state, use_paths).await;
            if !refresh_succeeded {
                let mut s = state.write().await;
                s.needs_head_refresh = true;
            }
        } else {
            // Upload failed - re-set the flag so we try again next time
            let mut s = state.write().await;
            s.needs_head_refresh = true;
        }
    }
}

/// Ensures text content ends with a trailing newline.
/// This is important for text files (especially JSON) to maintain proper formatting.
fn ensure_trailing_newline(content: &str) -> String {
    if content.ends_with('\n') {
        content.to_string()
    } else {
        format!("{}\n", content)
    }
}

/// Check if content equals the default for its content type.
///
/// This is used to detect newly created documents that haven't been edited yet.
/// Default content indicates the document was created by the reconciler but not
/// yet populated with real content - local content should take precedence.
fn is_default_content(content: &str, mime_type: &str) -> bool {
    let trimmed = content.trim();
    match mime_type {
        "application/json" => trimmed == "{}" || trimmed.is_empty(),
        "application/x-ndjson" => trimmed.is_empty(),
        "text/plain" => trimmed.is_empty(),
        "application/xml" => {
            trimmed.is_empty() || trimmed == r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#
        }
        _ => trimmed.is_empty(),
    }
}

/// Task that handles file changes and uploads to server
#[allow(clippy::too_many_arguments)]
pub async fn upload_task(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    mut rx: mpsc::Receiver<FileEvent>,
    use_paths: bool,
    force_push: bool,
    author: String,
) {
    while let Some(event) = rx.recv().await {
        // Extract captured content from the event
        // The watcher captures content at notification time to prevent race conditions
        // where SSE might overwrite the file between event dispatch and us reading it.
        let FileEvent::Modified(raw_content) = event;

        // Detect if file is binary and convert accordingly
        let content_info = detect_from_path(&file_path);
        let is_binary = content_info.is_binary || is_binary_content(&raw_content);
        let is_json = !is_binary && content_info.mime_type == "application/json";
        let is_jsonl = !is_binary && content_info.mime_type == "application/x-ndjson";

        let mut content = if is_binary {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.encode(&raw_content)
        } else {
            String::from_utf8_lossy(&raw_content).to_string()
        };

        // Log event received for debugging
        debug!(
            "upload_task event: identifier={}, content={:?} (len={})",
            identifier,
            &content.chars().take(50).collect::<String>(),
            content.len()
        );

        // Check for pending write barrier and handle echo detection
        // Track whether we detected an echo and need to refresh from HEAD
        let mut echo_detected = false;
        let mut should_refresh = false;

        {
            let mut s = state.write().await;
            debug!(
                "upload_task state: last_written_content={:?}, last_written_cid={:?}, has_barrier={}",
                &s.last_written_content.chars().take(50).collect::<String>(),
                s.last_written_cid.as_ref().map(|c| &c[..8.min(c.len())]),
                s.pending_write.is_some()
            );

            // Check for pending write (barrier is up)
            if let Some(pending) = s.pending_write.take() {
                // Check for timeout
                if pending.started_at.elapsed() > PENDING_WRITE_TIMEOUT {
                    warn!(
                        "Pending write timed out (id={}), clearing barrier",
                        pending.write_id
                    );
                    // The pending write timed out - we don't know if the file contains
                    // the server content (write succeeded but watcher was slow) or
                    // stale content (write failed or was interrupted).
                    //
                    // If content matches the pending write, treat as delayed echo.
                    // Otherwise, upload with old parent_cid for CRDT merge.
                    if content == pending.content {
                        debug!(
                            "Timed-out pending matches current content, treating as delayed echo"
                        );
                        s.last_written_cid = pending.cid;
                        s.last_written_content = pending.content;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                        echo_detected = true;
                    } else {
                        // Content differs - this is a user edit that slipped through,
                        // or the write failed. Upload with old parent for merge.
                        debug!(
                            "Timed-out pending differs from current content, uploading as user edit"
                        );
                        // DON'T update last_written_* - use old parent for CRDT merge
                    }
                } else if content == pending.content {
                    // Content matches what we wrote - this is our echo
                    debug!(
                        "Echo detected: content matches pending write (id={})",
                        pending.write_id
                    );
                    s.last_written_cid = pending.cid;
                    s.last_written_content = pending.content;
                    // Barrier cleared (we took it with .take())
                    should_refresh = s.needs_head_refresh;
                    s.needs_head_refresh = false;
                    echo_detected = true;
                } else {
                    debug!(
                        "Barrier present but content mismatch (id={}) - file: {:?} (len={}), pending: {:?} (len={})",
                        pending.write_id,
                        &content.chars().take(50).collect::<String>(),
                        content.len(),
                        &pending.content.chars().take(50).collect::<String>(),
                        pending.content.len()
                    );
                    // Content differs from pending - could be:
                    // a) Partial write (we're mid-write or just finished)
                    // b) User edit during our write
                    //
                    // Retry a few times to handle partial writes
                    let pending_content = pending.content.clone();
                    let pending_cid = pending.cid.clone();
                    let pending_write_id = pending.write_id;

                    // Put the pending back while we retry
                    s.pending_write = Some(pending);
                    drop(s); // Release lock during retries

                    let mut is_echo = false;
                    for i in 0..BARRIER_RETRY_COUNT {
                        sleep(BARRIER_RETRY_DELAY).await;

                        // Re-read file
                        let raw = match tokio::fs::read(&file_path).await {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Failed to re-read file during retry: {}", e);
                                break;
                            }
                        };

                        let reread = if is_binary {
                            use base64::{engine::general_purpose::STANDARD, Engine};
                            STANDARD.encode(&raw)
                        } else {
                            String::from_utf8_lossy(&raw).to_string()
                        };

                        if reread == pending_content {
                            // Content now matches - was partial write, now complete
                            debug!(
                                "Retry {}: content now matches pending write (id={})",
                                i + 1,
                                pending_write_id
                            );
                            content = reread;
                            is_echo = true;
                            break;
                        }

                        if reread != content {
                            // Content still changing, update and keep retrying
                            debug!("Retry {}: content still changing", i + 1);
                            content = reread;
                        }
                        // If reread == content and != pending, content is stable but different (user edit)
                    }

                    // Re-acquire lock and finalize
                    let mut s = state.write().await;

                    if is_echo {
                        // Our write completed after retries
                        debug!("Echo confirmed after retries (id={})", pending_write_id);
                        s.last_written_cid = pending_cid;
                        s.last_written_content = content.clone();
                        s.pending_write = None;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                        echo_detected = true;
                    } else {
                        // User edited during our write
                        info!(
                            "User edit detected during server write (id={})",
                            pending_write_id
                        );
                        s.pending_write = None; // Clear barrier
                                                // DON'T update last_written_* - use old parent for CRDT merge
                                                // Fall through to upload with old parent_cid

                        // IMPORTANT: Also check needs_head_refresh here!
                        // If server edits were skipped while barrier was up, we need to
                        // refresh after uploading to get the merged state.
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                    }
                }
            } else {
                // No barrier - normal echo detection
                // Also check needs_head_refresh in case server edit was skipped
                // due to local changes being pending
                should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;

                // Use trimmed comparison to avoid whitespace differences
                let content_trimmed = content.trim();
                let last_written_trimmed = s.last_written_content.trim();

                if content_trimmed == last_written_trimmed {
                    debug!("Ignoring echo: content matches last written (trimmed)");
                    echo_detected = true;
                } else {
                    debug!(
                        "No barrier, content mismatch - file: {:?} (len={}), last_written: {:?} (len={})",
                        &content.chars().take(50).collect::<String>(),
                        content.len(),
                        &s.last_written_content.chars().take(50).collect::<String>(),
                        s.last_written_content.len()
                    );
                }
            }
        }

        // If echo detected, optionally refresh from HEAD then skip upload
        if echo_detected {
            if should_refresh {
                let refresh_succeeded =
                    refresh_from_head(&client, &server, &identifier, &file_path, &state, use_paths)
                        .await;
                if !refresh_succeeded {
                    // Re-set the flag so we try again next time
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            }
            continue;
        }

        if is_json {
            let json_upload_succeeded = match push_json_content(
                &client,
                &server,
                &identifier,
                &content,
                &state,
                use_paths,
                &author,
            )
            .await
            {
                Ok(_) => true,
                Err(e) => {
                    error!("JSON upload failed: {}", e);
                    false
                }
            };
            handle_upload_refresh(
                &client,
                &server,
                &identifier,
                &file_path,
                &state,
                use_paths,
                should_refresh,
                json_upload_succeeded,
            )
            .await;
            continue;
        }

        if is_jsonl {
            let jsonl_upload_succeeded = match push_jsonl_content(
                &client,
                &server,
                &identifier,
                &content,
                &state,
                use_paths,
                &author,
            )
            .await
            {
                Ok(_) => true,
                Err(e) => {
                    error!("JSONL upload failed: {}", e);
                    false
                }
            };
            handle_upload_refresh(
                &client,
                &server,
                &identifier,
                &file_path,
                &state,
                use_paths,
                should_refresh,
                jsonl_upload_succeeded,
            )
            .await;
            continue;
        }

        // Get parent CID to decide which endpoint to use
        // CRDT safety: if we don't know the parent, fetch HEAD from server first
        // This prevents blind overwrites when server has content we don't know about
        let parent_cid = if force_push {
            // Force-push: fetch HEAD's cid to ensure we replace current content
            match fetch_head(&client, &server, &identifier, use_paths).await {
                Ok(Some(head)) => head.cid,
                Ok(None) => {
                    error!("Force-push: document not found on server");
                    continue;
                }
                Err(e) => {
                    error!("Force-push: HEAD request failed: {}", e);
                    continue;
                }
            }
        } else {
            let known_parent = {
                let s = state.read().await;
                s.last_written_cid.clone()
            };

            // If we don't know the parent, fetch HEAD to check if server has content
            // This prevents blind overwrites when syncing a file the server already has
            if known_parent.is_none() {
                match fetch_head(&client, &server, &identifier, use_paths).await {
                    Ok(Some(head)) => {
                        if let Some(cid) = head.cid {
                            info!(
                                "Server has existing content (cid: {}), syncing from server first",
                                &cid[..8.min(cid.len())]
                            );
                            // Fetch and apply server content before uploading
                            let refresh_succeeded = refresh_from_head(
                                &client,
                                &server,
                                &identifier,
                                &file_path,
                                &state,
                                use_paths,
                            )
                            .await;
                            if refresh_succeeded {
                                // Now we have the server content, get the new parent
                                let s = state.read().await;
                                s.last_written_cid.clone()
                            } else {
                                // Refresh failed, skip this upload
                                error!("Failed to sync from server, skipping upload");
                                continue;
                            }
                        } else {
                            // Server has no content, proceed with initial commit
                            None
                        }
                    }
                    // Document not found or error - proceed with initial commit
                    Ok(None) | Err(_) => None,
                }
            } else {
                known_parent
            }
        };

        // Track upload success - only refresh if upload succeeded
        let mut upload_succeeded = false;

        // Final safety check: if our content matches server HEAD, skip upload entirely.
        // This prevents feedback loops when echo detection fails due to race conditions.
        // The cost is one extra HEAD fetch, but it's worth it to prevent CRDT duplication.
        // Compare with trailing whitespace normalized to handle newline inconsistencies.
        // CRITICAL: Also update parent_cid from HEAD to prevent stale parent race conditions.
        // If we determined parent_cid earlier but the server has since moved forward, using
        // the stale parent would create duplicate CRDT operations.
        let mut parent_cid = parent_cid; // Make mutable for HEAD update
        if let Ok(Some(head)) = fetch_head(&client, &server, &identifier, use_paths).await {
            let head_trimmed = head.content.trim_end();
            let content_trimmed = content.trim_end();
            if head_trimmed == content_trimmed {
                debug!(
                    "Content matches server HEAD (after normalization), skipping redundant upload (cid: {:?})",
                    head.cid.as_ref().map(|c| &c[..8.min(c.len())])
                );
                // Update state to reflect server's current CID
                let mut s = state.write().await;
                s.last_written_cid = head.cid;
                s.last_written_content = content;
                continue;
            }
            // CRITICAL: Always use HEAD's CID as parent to prevent duplicate operations.
            // If our cached parent_cid is stale (server moved forward due to SSE), uploading
            // with the old parent would create a duplicate CRDT operation.
            if head.cid.is_some() && head.cid != parent_cid {
                warn!(
                    "PARENT UPDATE: using HEAD cid {:?} instead of stale parent {:?}",
                    head.cid.as_ref().map(|c| &c[..8.min(c.len())]),
                    parent_cid.as_ref().map(|c| &c[..8.min(c.len())])
                );
                parent_cid = head.cid;
            }
        }

        match parent_cid {
            Some(mut parent) => {
                // CRITICAL: Serialize uploads to prevent duplicate CRDT operations.
                // Wait if another upload is in progress, then re-check HEAD.
                let content_trimmed = content.trim_end();

                // Wait for any concurrent upload to complete (max 5 seconds)
                for wait_round in 0..50 {
                    let in_progress = {
                        let s = state.read().await;
                        s.upload_in_progress
                    };
                    if !in_progress {
                        break;
                    }
                    if wait_round == 0 {
                        debug!("Waiting for concurrent upload to complete...");
                    }
                    sleep(Duration::from_millis(100)).await;
                }

                // Set upload_in_progress flag and check HEAD atomically
                {
                    let mut s = state.write().await;
                    // Double-check: if still in progress, someone else is uploading
                    if s.upload_in_progress {
                        warn!("Upload still in progress after waiting, skipping to avoid race");
                        continue;
                    }
                    s.upload_in_progress = true;
                }

                // Now we hold the upload lock - check HEAD multiple times before uploading.
                // This handles the race condition where another client's upload is in-flight
                // and we might see stale HEAD. By retrying, we give time for in-flight
                // commits to complete and be reflected in HEAD.
                const PRE_UPLOAD_CHECK_COUNT: usize = 5;
                const PRE_UPLOAD_CHECK_DELAY_MS: u64 = 100;
                let mut should_skip = false;

                for check_round in 0..PRE_UPLOAD_CHECK_COUNT {
                    if check_round > 0 {
                        sleep(Duration::from_millis(PRE_UPLOAD_CHECK_DELAY_MS)).await;
                    }

                    if let Ok(Some(head)) =
                        fetch_head(&client, &server, &identifier, use_paths).await
                    {
                        let head_trimmed = head.content.trim_end();
                        if head_trimmed == content_trimmed {
                            warn!(
                                "PRE-UPLOAD SKIP (round {}): HEAD matches content, skipping duplicate (head_cid: {:?})",
                                check_round + 1,
                                head.cid.as_ref().map(|c| &c[..8.min(c.len())])
                            );
                            // Update state and release lock
                            let mut s = state.write().await;
                            s.last_written_cid = head.cid;
                            s.last_written_content = content.clone();
                            s.upload_in_progress = false;
                            should_skip = true;
                            break;
                        }
                        // Update parent if HEAD moved
                        if let Some(head_cid) = &head.cid {
                            if head_cid != &parent {
                                warn!(
                                    "PRE-UPLOAD PARENT UPDATE (round {}): HEAD moved from {} to {}",
                                    check_round + 1,
                                    &parent[..8.min(parent.len())],
                                    &head_cid[..8.min(head_cid.len())]
                                );
                                parent = head_cid.clone();
                            }
                        }
                    }
                }

                if should_skip {
                    continue;
                }

                // FINAL STATE CHECK: Re-check state right before upload.
                // This catches the race where SSE received and wrote content while
                // we were doing HEAD checks. Without this, we'd upload a redundant
                // diff that duplicates what workspace already uploaded.
                {
                    let s = state.read().await;
                    let last_trimmed = s.last_written_content.trim_end();
                    if last_trimmed == content_trimmed {
                        warn!(
                            "FINAL STATE CHECK: last_written_content matches, skipping redundant upload"
                        );
                        // Clear upload_in_progress and skip
                        drop(s);
                        let mut s = state.write().await;
                        s.upload_in_progress = false;
                        continue;
                    }
                    // Also check pending_write
                    if let Some(ref pending) = s.pending_write {
                        let pending_trimmed = pending.content.trim_end();
                        if pending_trimmed == content_trimmed {
                            warn!(
                                "FINAL STATE CHECK: pending_write matches, skipping redundant upload"
                            );
                            drop(s);
                            let mut s = state.write().await;
                            s.upload_in_progress = false;
                            continue;
                        }
                    }
                }

                // Normal case: use replace endpoint
                debug!(
                    "upload: identifier={}, parent_cid={}, content={:?} (len={})",
                    identifier,
                    &parent[..8.min(parent.len())],
                    &content.chars().take(50).collect::<String>(),
                    content.len()
                );

                let replace_url =
                    build_replace_url(&server, &identifier, &parent, use_paths, &author);

                match client
                    .post(&replace_url)
                    .header("content-type", "text/plain")
                    .body(content.clone())
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<ReplaceResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Uploaded: {} chars inserted, {} deleted (cid: {})",
                                        result.summary.chars_inserted,
                                        result.summary.chars_deleted,
                                        &result.cid[..8.min(result.cid.len())]
                                    );

                                    // Update state and persist to state file
                                    // Hash the raw file bytes, not the (possibly base64) content
                                    let cid = result.cid.clone();
                                    let file_bytes = tokio::fs::read(&file_path).await.ok();
                                    let content_hash = file_bytes
                                        .as_ref()
                                        .map(|b| compute_content_hash(b))
                                        .unwrap_or_default();
                                    let file_name = file_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "file".to_string());
                                    let inode_key = InodeKey::from_path(&file_path)
                                        .ok()
                                        .map(|k| k.shadow_filename());

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name, inode_key)
                                        .await;
                                    upload_succeeded = true;
                                }
                                Err(e) => {
                                    error!("Failed to parse replace response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            error!("Upload failed: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        error!("Upload request failed: {}", e);
                    }
                }

                // Clear upload_in_progress flag
                {
                    let mut s = state.write().await;
                    s.upload_in_progress = false;
                }
            }
            None => {
                // First commit: use edit endpoint with generated Yjs update
                info!("Creating initial commit...");
                let update = create_yjs_text_update(&content);
                let edit_url = build_edit_url(&server, &identifier, use_paths);
                let edit_req = EditRequest {
                    update,
                    author: Some(author.to_string()),
                    message: Some("Initial sync".to_string()),
                };

                match client.post(&edit_url).json(&edit_req).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<EditResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Created initial commit: {} bytes (cid: {})",
                                        content.len(),
                                        &result.cid[..8.min(result.cid.len())]
                                    );

                                    // Update state and persist to state file
                                    // Hash the raw file bytes, not the (possibly base64) content
                                    let cid = result.cid.clone();
                                    let file_bytes = tokio::fs::read(&file_path).await.ok();
                                    let content_hash = file_bytes
                                        .as_ref()
                                        .map(|b| compute_content_hash(b))
                                        .unwrap_or_default();
                                    let file_name = file_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "file".to_string());
                                    let inode_key = InodeKey::from_path(&file_path)
                                        .ok()
                                        .map(|k| k.shadow_filename());

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name, inode_key)
                                        .await;
                                    upload_succeeded = true;
                                }
                                Err(e) => {
                                    error!("Failed to parse edit response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            error!("Initial commit failed: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        error!("Initial commit request failed: {}", e);
                    }
                }
            }
        }

        // After successful upload, refresh from HEAD if server edits were skipped
        // IMPORTANT: Re-check needs_head_refresh here since SSE events might have
        // arrived DURING our upload (race condition fix for CP-f20).
        // Only refresh after successful upload to avoid overwriting local edits.
        let needs_refresh = {
            let mut s = state.write().await;
            let needs = should_refresh || s.needs_head_refresh;
            s.needs_head_refresh = false;
            needs
        };

        if needs_refresh {
            if upload_succeeded {
                let refresh_succeeded =
                    refresh_from_head(&client, &server, &identifier, &file_path, &state, use_paths)
                        .await;
                if !refresh_succeeded {
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            } else {
                // Upload failed - re-set the flag so we try again next time
                let mut s = state.write().await;
                s.needs_head_refresh = true;
            }
        }
    }
}

/// Task that handles file changes and uploads to server with flock-aware tracking.
///
/// This is an enhanced version of `upload_task` that tracks pending outbound commits
/// for flock-aware synchronization. After each successful upload, the commit_id is
/// recorded in the FlockSyncState so that SSE can verify ancestry before writing
/// inbound updates.
#[allow(clippy::too_many_arguments)]
pub async fn upload_task_with_flock(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    mut rx: mpsc::Receiver<FileEvent>,
    use_paths: bool,
    force_push: bool,
    flock_state: FlockSyncState,
    author: String,
) {
    while let Some(event) = rx.recv().await {
        // Small delay to allow any concurrent SSE state updates to complete.
        sleep(Duration::from_millis(50)).await;

        // Extract captured content from the event
        let FileEvent::Modified(raw_content) = event;

        // Detect if file is binary and convert accordingly
        let content_info = detect_from_path(&file_path);
        let is_binary = content_info.is_binary || is_binary_content(&raw_content);
        let is_json = !is_binary && content_info.mime_type == "application/json";
        let is_jsonl = !is_binary && content_info.mime_type == "application/x-ndjson";

        let mut content = if is_binary {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.encode(&raw_content)
        } else {
            String::from_utf8_lossy(&raw_content).to_string()
        };

        // Check for pending write barrier and handle echo detection
        let mut echo_detected = false;
        let mut should_refresh = false;

        {
            let mut s = state.write().await;

            if let Some(pending) = s.pending_write.take() {
                if pending.started_at.elapsed() > PENDING_WRITE_TIMEOUT {
                    warn!(
                        "Pending write timed out (id={}), clearing barrier",
                        pending.write_id
                    );
                    if content == pending.content {
                        debug!(
                            "Timed-out pending matches current content, treating as delayed echo"
                        );
                        s.last_written_cid = pending.cid;
                        s.last_written_content = pending.content;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                        echo_detected = true;
                    } else {
                        debug!(
                            "Timed-out pending differs from current content, uploading as user edit"
                        );
                    }
                } else if content == pending.content {
                    debug!(
                        "Echo detected: content matches pending write (id={})",
                        pending.write_id
                    );
                    s.last_written_cid = pending.cid;
                    s.last_written_content = pending.content;
                    should_refresh = s.needs_head_refresh;
                    s.needs_head_refresh = false;
                    echo_detected = true;
                } else {
                    let pending_content = pending.content.clone();
                    let pending_cid = pending.cid.clone();
                    let pending_write_id = pending.write_id;

                    s.pending_write = Some(pending);
                    drop(s);

                    let mut is_echo = false;
                    for i in 0..BARRIER_RETRY_COUNT {
                        sleep(BARRIER_RETRY_DELAY).await;

                        let raw = match tokio::fs::read(&file_path).await {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Failed to re-read file during retry: {}", e);
                                break;
                            }
                        };

                        let reread = if is_binary {
                            use base64::{engine::general_purpose::STANDARD, Engine};
                            STANDARD.encode(&raw)
                        } else {
                            String::from_utf8_lossy(&raw).to_string()
                        };

                        if reread == pending_content {
                            debug!(
                                "Retry {}: content now matches pending write (id={})",
                                i + 1,
                                pending_write_id
                            );
                            content = reread;
                            is_echo = true;
                            break;
                        }

                        if reread != content {
                            debug!("Retry {}: content still changing", i + 1);
                            content = reread;
                        }
                    }

                    let mut s = state.write().await;

                    if is_echo {
                        debug!("Echo confirmed after retries (id={})", pending_write_id);
                        s.last_written_cid = pending_cid;
                        s.last_written_content = content.clone();
                        s.pending_write = None;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                        echo_detected = true;
                    } else {
                        info!(
                            "User edit detected during server write (id={})",
                            pending_write_id
                        );
                        s.pending_write = None;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                    }
                }
            } else {
                should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;

                // Use trimmed comparison to avoid whitespace differences
                let content_trimmed = content.trim();
                let last_written_trimmed = s.last_written_content.trim();

                if content_trimmed == last_written_trimmed {
                    debug!("Ignoring echo: content matches last written (trimmed)");
                    echo_detected = true;
                }
            }
        }

        if echo_detected {
            if should_refresh {
                let refresh_succeeded =
                    refresh_from_head(&client, &server, &identifier, &file_path, &state, use_paths)
                        .await;
                if !refresh_succeeded {
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            }
            continue;
        }

        // Handle JSON/JSONL files separately
        if is_json {
            let json_upload_succeeded = push_json_content(
                &client,
                &server,
                &identifier,
                &content,
                &state,
                use_paths,
                &author,
            )
            .await
            .is_ok();
            if json_upload_succeeded {
                // Record successful upload in flock state
                // Note: push_json_content updates state.last_written_cid
                let cid = state.read().await.last_written_cid.clone();
                if let Some(cid) = cid {
                    record_upload_result(Some(&flock_state), &file_path, &cid).await;
                }
            } else {
                error!("JSON upload failed");
            }
            handle_upload_refresh(
                &client,
                &server,
                &identifier,
                &file_path,
                &state,
                use_paths,
                should_refresh,
                json_upload_succeeded,
            )
            .await;
            continue;
        }

        if is_jsonl {
            let jsonl_upload_succeeded = push_jsonl_content(
                &client,
                &server,
                &identifier,
                &content,
                &state,
                use_paths,
                &author,
            )
            .await
            .is_ok();
            if jsonl_upload_succeeded {
                // Record successful upload in flock state
                let cid = state.read().await.last_written_cid.clone();
                if let Some(cid) = cid {
                    record_upload_result(Some(&flock_state), &file_path, &cid).await;
                }
            } else {
                error!("JSONL upload failed");
            }
            handle_upload_refresh(
                &client,
                &server,
                &identifier,
                &file_path,
                &state,
                use_paths,
                should_refresh,
                jsonl_upload_succeeded,
            )
            .await;
            continue;
        }

        // Get parent CID for text file upload
        // CRDT safety: if we don't know the parent, fetch HEAD from server first
        let parent_cid = if force_push {
            match fetch_head(&client, &server, &identifier, use_paths).await {
                Ok(Some(head)) => head.cid,
                Ok(None) => {
                    error!("Force-push: document not found on server");
                    continue;
                }
                Err(e) => {
                    error!("Force-push: HEAD request failed: {}", e);
                    continue;
                }
            }
        } else {
            let known_parent = {
                let s = state.read().await;
                s.last_written_cid.clone()
            };

            // If we don't know the parent, fetch HEAD to check if server has content
            if known_parent.is_none() {
                match fetch_head(&client, &server, &identifier, use_paths).await {
                    Ok(Some(head)) => {
                        if let Some(cid) = head.cid {
                            info!(
                                "Server has existing content (cid: {}), syncing from server first",
                                &cid[..8.min(cid.len())]
                            );
                            let refresh_succeeded = refresh_from_head(
                                &client,
                                &server,
                                &identifier,
                                &file_path,
                                &state,
                                use_paths,
                            )
                            .await;
                            if refresh_succeeded {
                                let s = state.read().await;
                                s.last_written_cid.clone()
                            } else {
                                error!("Failed to sync from server, skipping upload");
                                continue;
                            }
                        } else {
                            None
                        }
                    }
                    Ok(None) | Err(_) => None,
                }
            } else {
                known_parent
            }
        };

        let mut upload_succeeded = false;

        // Final safety check: if our content matches server HEAD, skip upload entirely.
        // This prevents feedback loops when echo detection fails due to race conditions.
        // Compare with trailing whitespace normalized to handle newline inconsistencies.
        // CRITICAL: Also update parent_cid from HEAD to prevent stale parent race conditions.
        let mut parent_cid = parent_cid; // Make mutable for HEAD update
        if let Ok(Some(head)) = fetch_head(&client, &server, &identifier, use_paths).await {
            let head_trimmed = head.content.trim_end();
            let content_trimmed = content.trim_end();
            if head_trimmed == content_trimmed {
                debug!(
                    "Content matches server HEAD (after normalization), skipping redundant upload (cid: {:?})",
                    head.cid.as_ref().map(|c| &c[..8.min(c.len())])
                );
                let mut s = state.write().await;
                s.last_written_cid = head.cid;
                s.last_written_content = content;
                continue;
            }
            // CRITICAL: Always use HEAD's CID as parent to prevent duplicate operations.
            if head.cid.is_some() && head.cid != parent_cid {
                warn!(
                    "PARENT UPDATE (flock): using HEAD cid {:?} instead of stale parent {:?}",
                    head.cid.as_ref().map(|c| &c[..8.min(c.len())]),
                    parent_cid.as_ref().map(|c| &c[..8.min(c.len())])
                );
                parent_cid = head.cid;
            }
        }

        match parent_cid {
            Some(mut parent) => {
                // CRITICAL: Serialize uploads to prevent duplicate CRDT operations.
                let content_trimmed = content.trim_end();

                // Wait for any concurrent upload to complete
                for wait_round in 0..50 {
                    let in_progress = {
                        let s = state.read().await;
                        s.upload_in_progress
                    };
                    if !in_progress {
                        break;
                    }
                    if wait_round == 0 {
                        debug!("Waiting for concurrent upload to complete (flock)...");
                    }
                    sleep(Duration::from_millis(100)).await;
                }

                // Set upload_in_progress flag
                {
                    let mut s = state.write().await;
                    if s.upload_in_progress {
                        warn!("Upload still in progress after waiting (flock), skipping");
                        continue;
                    }
                    s.upload_in_progress = true;
                }

                // Check HEAD multiple times before uploading (same as upload_task).
                // This handles the race condition where another client's upload is in-flight.
                const PRE_UPLOAD_CHECK_COUNT: usize = 5;
                const PRE_UPLOAD_CHECK_DELAY_MS: u64 = 100;
                let mut should_skip = false;

                for check_round in 0..PRE_UPLOAD_CHECK_COUNT {
                    if check_round > 0 {
                        sleep(Duration::from_millis(PRE_UPLOAD_CHECK_DELAY_MS)).await;
                    }

                    if let Ok(Some(head)) =
                        fetch_head(&client, &server, &identifier, use_paths).await
                    {
                        let head_trimmed = head.content.trim_end();
                        if head_trimmed == content_trimmed {
                            warn!(
                                "PRE-UPLOAD SKIP (flock round {}): HEAD matches content, skipping duplicate (head_cid: {:?})",
                                check_round + 1,
                                head.cid.as_ref().map(|c| &c[..8.min(c.len())])
                            );
                            let mut s = state.write().await;
                            s.last_written_cid = head.cid;
                            s.last_written_content = content.clone();
                            s.upload_in_progress = false;
                            should_skip = true;
                            break;
                        }
                        // Update parent if HEAD moved
                        if let Some(head_cid) = &head.cid {
                            if head_cid != &parent {
                                warn!(
                                    "PRE-UPLOAD PARENT UPDATE (flock round {}): HEAD moved from {} to {}",
                                    check_round + 1,
                                    &parent[..8.min(parent.len())],
                                    &head_cid[..8.min(head_cid.len())]
                                );
                                parent = head_cid.clone();
                            }
                        }
                    }
                }

                if should_skip {
                    continue;
                }

                // FINAL STATE CHECK: Re-check state right before upload.
                // This catches the race where SSE received and wrote content while
                // we were doing HEAD checks. Without this, we'd upload a redundant
                // diff that duplicates what workspace already uploaded.
                {
                    let s = state.read().await;
                    let last_trimmed = s.last_written_content.trim_end();
                    if last_trimmed == content_trimmed {
                        warn!(
                            "FINAL STATE CHECK (flock): last_written_content matches, skipping redundant upload"
                        );
                        drop(s);
                        let mut s = state.write().await;
                        s.upload_in_progress = false;
                        continue;
                    }
                    if let Some(ref pending) = s.pending_write {
                        let pending_trimmed = pending.content.trim_end();
                        if pending_trimmed == content_trimmed {
                            warn!(
                                "FINAL STATE CHECK (flock): pending_write matches, skipping redundant upload"
                            );
                            drop(s);
                            let mut s = state.write().await;
                            s.upload_in_progress = false;
                            continue;
                        }
                    }
                }

                let replace_url =
                    build_replace_url(&server, &identifier, &parent, use_paths, &author);

                match client
                    .post(&replace_url)
                    .header("content-type", "text/plain")
                    .body(content.clone())
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<ReplaceResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Uploaded: {} chars inserted, {} deleted (cid: {})",
                                        result.summary.chars_inserted,
                                        result.summary.chars_deleted,
                                        &result.cid[..8.min(result.cid.len())]
                                    );

                                    let cid = result.cid.clone();
                                    let file_bytes = tokio::fs::read(&file_path).await.ok();
                                    let content_hash = file_bytes
                                        .as_ref()
                                        .map(|b| compute_content_hash(b))
                                        .unwrap_or_default();
                                    let file_name = file_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "file".to_string());
                                    let inode_key = InodeKey::from_path(&file_path)
                                        .ok()
                                        .map(|k| k.shadow_filename());

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name, inode_key)
                                        .await;
                                    upload_succeeded = true;

                                    // Record successful upload in flock state
                                    record_upload_result(Some(&flock_state), &file_path, &cid)
                                        .await;
                                }
                                Err(e) => {
                                    error!("Failed to parse replace response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            error!("Upload failed: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        error!("Upload request failed: {}", e);
                    }
                }

                // Clear upload_in_progress flag
                {
                    let mut s = state.write().await;
                    s.upload_in_progress = false;
                }
            }
            None => {
                info!("Creating initial commit...");
                let update = create_yjs_text_update(&content);
                let edit_url = build_edit_url(&server, &identifier, use_paths);
                let edit_req = EditRequest {
                    update,
                    author: Some(author.to_string()),
                    message: Some("Initial sync".to_string()),
                };

                match client.post(&edit_url).json(&edit_req).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<EditResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Created initial commit: {} bytes (cid: {})",
                                        content.len(),
                                        &result.cid[..8.min(result.cid.len())]
                                    );

                                    let cid = result.cid.clone();
                                    let file_bytes = tokio::fs::read(&file_path).await.ok();
                                    let content_hash = file_bytes
                                        .as_ref()
                                        .map(|b| compute_content_hash(b))
                                        .unwrap_or_default();
                                    let file_name = file_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "file".to_string());
                                    let inode_key = InodeKey::from_path(&file_path)
                                        .ok()
                                        .map(|k| k.shadow_filename());

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name, inode_key)
                                        .await;
                                    upload_succeeded = true;

                                    // Record successful upload in flock state
                                    record_upload_result(Some(&flock_state), &file_path, &cid)
                                        .await;
                                }
                                Err(e) => {
                                    error!("Failed to parse edit response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            error!("Initial commit failed: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        error!("Initial commit request failed: {}", e);
                    }
                }
            }
        }

        // After successful upload, refresh from HEAD if needed
        let needs_refresh = {
            let mut s = state.write().await;
            let needs = should_refresh || s.needs_head_refresh;
            s.needs_head_refresh = false;
            needs
        };

        if needs_refresh {
            if upload_succeeded {
                let refresh_succeeded =
                    refresh_from_head(&client, &server, &identifier, &file_path, &state, use_paths)
                        .await;
                if !refresh_succeeded {
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            } else {
                let mut s = state.write().await;
                s.needs_head_refresh = true;
            }
        }
    }
}

/// Perform initial sync: fetch HEAD and write to local file
pub async fn initial_sync(
    client: &Client,
    server: &str,
    node_id: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let head = match fetch_head(client, server, node_id, false).await {
        Ok(Some(h)) => h,
        Ok(None) => return Err(format!("Document {} not found", node_id).into()),
        Err(e) => return Err(format!("Failed to get HEAD: {}", e).into()),
    };

    // Write content to file, handling binary content (base64-encoded on server)
    // Track the bytes we actually write to disk for proper hash computation
    use base64::{engine::general_purpose::STANDARD, Engine};
    let content_info = detect_from_path(file_path);
    let bytes_written: Vec<u8> = if content_info.is_binary {
        // Extension indicates binary - decode base64
        match STANDARD.decode(&head.content) {
            Ok(decoded) => {
                tokio::fs::write(file_path, &decoded).await?;
                decoded
            }
            Err(e) => {
                warn!("Failed to decode binary content: {}", e);
                let bytes = head.content.as_bytes().to_vec();
                tokio::fs::write(file_path, &bytes).await?;
                bytes
            }
        }
    } else if looks_like_base64_binary(&head.content) {
        // Extension says text, but content looks like base64-encoded binary
        // This handles files that were detected as binary on upload
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                tokio::fs::write(file_path, &decoded).await?;
                decoded
            }
            _ => {
                let bytes = head.content.as_bytes().to_vec();
                tokio::fs::write(file_path, &bytes).await?;
                bytes
            }
        }
    } else {
        // Extension says text, content doesn't look like base64 binary
        // Ensure text files end with a trailing newline
        let content_with_newline = ensure_trailing_newline(&head.content);
        let bytes = content_with_newline.as_bytes().to_vec();
        tokio::fs::write(file_path, &bytes).await?;
        bytes
    };

    // Update state and persist to state file
    // Use actual written content for echo detection (important for text files with trailing newlines)
    let written_content_str = String::from_utf8_lossy(&bytes_written).to_string();
    {
        let mut s = state.write().await;
        s.last_written_cid = head.cid.clone();
        s.last_written_content = written_content_str;

        // Save to state file for offline change detection
        // Use the actual bytes written to disk, not the server response
        if let Some(ref cid) = head.cid {
            let content_hash = compute_content_hash(&bytes_written);
            let file_name = file_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            let inode_key = InodeKey::from_path(file_path)
                .ok()
                .map(|k| k.shadow_filename());
            s.mark_synced(cid, &content_hash, &file_name, inode_key)
                .await;
        }
    }

    match &head.cid {
        Some(cid) => info!(
            "Initial sync complete: {} bytes at {}",
            head.content.len(),
            cid
        ),
        None => info!("Initial sync complete: empty document (no commits yet)"),
    }

    Ok(())
}

/// Sync a single file during initial directory sync.
///
/// Handles determining the identifier (path or UUID), checking server content,
/// and pushing/pulling content based on strategy.
///
/// Returns the final identifier used and the content hash.
#[allow(clippy::too_many_arguments)]
pub async fn sync_single_file(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    root_directory: &Path,
    file: &crate::sync::directory::ScannedFile,
    file_path: &std::path::PathBuf,
    uuid_map: &std::collections::HashMap<String, String>,
    initial_sync_strategy: &str,
    file_states: &std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<String, crate::sync::FileSyncState>>,
    >,
    use_paths: bool,
    author: &str,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    eprintln!(
        "=== SYNC_SINGLE_FILE: {} use_paths={} ===",
        file.relative_path, use_paths
    );
    use base64::{engine::general_purpose::STANDARD, Engine};

    // Determine identifier
    let identifier = if use_paths {
        file.relative_path.clone()
    } else if let Some(uuid) = uuid_map.get(&file.relative_path) {
        info!("Using UUID for {}: {}", file.relative_path, uuid);
        uuid.clone()
    } else {
        // Use find_owning_document to determine correct parent document
        // (handles files in node-backed subdirectories)
        let owning = find_owning_document(root_directory, fs_root_id, &file.relative_path);
        let derived = format!("{}:{}", owning.document_id, owning.relative_path);
        info!(
            "No UUID found for {}, using derived ID: {} (owning doc: {})",
            file.relative_path, derived, owning.document_id
        );

        // If the owning document is different from fs_root (node-backed subdirectory),
        // push the subdirectory's schema first so the server reconciler can create the document
        let mut final_identifier = derived.clone();
        if owning.document_id != fs_root_id {
            let options = ScanOptions {
                include_hidden: false,
                ignore_patterns: vec![],
            };
            if let Ok(json) = scan_directory_to_json(&owning.directory, &options) {
                info!(
                    "Pushing subdirectory schema for {} (document {})",
                    owning.directory.display(),
                    owning.document_id
                );
                if let Err(e) =
                    push_schema_to_server(client, server, &owning.document_id, &json, author).await
                {
                    warn!("Failed to push subdirectory schema: {}", e);
                } else {
                    // Wait for server to reconcile and create the document
                    sleep(Duration::from_millis(200)).await;

                    // Fetch the UUID assigned by the reconciler
                    if let Some(uuid) = fetch_node_id_from_schema(
                        client,
                        server,
                        &owning.document_id,
                        &owning.relative_path,
                    )
                    .await
                    {
                        info!(
                            "Resolved UUID for {}: {} -> {}",
                            owning.relative_path, derived, uuid
                        );
                        final_identifier = uuid;
                    }
                }
            }
        }

        final_identifier
    };

    // Reuse existing state if handle_schema_change already created one
    let state = {
        let states = file_states.read().await;
        if let Some(existing) = states.get(&file.relative_path) {
            existing.state.clone()
        } else {
            std::sync::Arc::new(tokio::sync::RwLock::new(crate::sync::SyncState::new()))
        }
    };

    // Check server state
    let head_result = fetch_head(client, server, &identifier, use_paths).await;

    match &head_result {
        Ok(Some(head)) => {
            if head.content.is_empty() || initial_sync_strategy == "local" {
                // Push local content
                info!(
                    "Pushing initial content for: {} ({} bytes)",
                    identifier,
                    file.content.len()
                );
                crate::sync::push_content_by_type(
                    client,
                    server,
                    &identifier,
                    &file.content,
                    &state,
                    use_paths,
                    file.is_binary,
                    &file.content_type,
                    author,
                )
                .await?;
            } else {
                // Server has content - use CRDT ancestry to determine sync direction
                if initial_sync_strategy == "server" {
                    // Check if server has only default content (e.g., {} for JSON)
                    // Default content indicates newly created doc that should be overwritten
                    let server_has_default = is_default_content(&head.content, &file.content_type);

                    // Get local CID from state (if we've synced before)
                    let local_cid = {
                        let s = state.read().await;
                        s.last_written_cid.clone()
                    };

                    // Determine sync direction using CRDT ancestry
                    let should_push = if server_has_default {
                        // Server has default content - always push local
                        info!(
                            "Server has default content for {}, pushing local",
                            file.relative_path
                        );
                        true
                    } else if local_cid.is_none() {
                        // No local CID - first sync
                        // Check if local content is default/empty - if so, prefer server content
                        // This fixes a bug where sandbox restarts would push empty files to server
                        let local_is_default =
                            is_default_content(&file.content, &file.content_type);
                        if file.content != head.content && !local_is_default {
                            // Local has different non-default content but no CID - push local
                            // (This handles the case where local was edited offline before first sync)
                            info!(
                                "Local has different non-default content but no CID for {}, pushing local",
                                file.relative_path
                            );
                            true
                        } else {
                            // Either content matches, or local is default - pull from server
                            if local_is_default
                                && !is_default_content(&head.content, &file.content_type)
                            {
                                info!(
                                    "Local has default content but server has data for {}, pulling server",
                                    file.relative_path
                                );
                            }
                            false
                        }
                    } else {
                        // Both have CIDs - use ancestry check
                        match crate::sync::determine_sync_direction(
                            client,
                            server,
                            &identifier,
                            local_cid.as_deref(),
                            head.cid.as_deref(),
                        )
                        .await
                        {
                            Ok(crate::sync::SyncDirection::Push) => {
                                info!("Ancestry check: local is ahead for {}", file.relative_path);
                                true
                            }
                            Ok(crate::sync::SyncDirection::Pull) => {
                                info!("Ancestry check: server is ahead for {}", file.relative_path);
                                false
                            }
                            Ok(crate::sync::SyncDirection::InSync) => {
                                // Already in sync, but check content anyway
                                if file.content != head.content {
                                    info!(
                                        "CIDs match but content differs for {}, pushing local",
                                        file.relative_path
                                    );
                                    true
                                } else {
                                    false
                                }
                            }
                            Ok(crate::sync::SyncDirection::Diverged) => {
                                // Diverged - prefer local content (merge not implemented yet)
                                warn!(
                                    "Diverged history for {}, preferring local content",
                                    file.relative_path
                                );
                                true
                            }
                            Err(e) => {
                                // Ancestry check failed - prefer local to avoid data loss
                                warn!(
                                    "Ancestry check failed for {}: {}, preferring local",
                                    file.relative_path, e
                                );
                                true
                            }
                        }
                    };

                    if should_push {
                        // Push local content to server
                        crate::sync::push_content_by_type(
                            client,
                            server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                            file.is_binary,
                            &file.content_type,
                            author,
                        )
                        .await?;
                    } else {
                        // Pull server content to local
                        // Track the actual content written to file for echo detection
                        let written_content = if file.is_binary {
                            if let Ok(decoded) = STANDARD.decode(&head.content) {
                                tokio::fs::write(file_path, &decoded).await?;
                                head.content.clone() // Binary: store base64
                            } else {
                                head.content.clone()
                            }
                        } else {
                            // Ensure text files end with a trailing newline
                            let content_with_newline = ensure_trailing_newline(&head.content);
                            tokio::fs::write(file_path, &content_with_newline).await?;
                            content_with_newline // Store content WITH newline for echo detection
                        };
                        // Seed SyncState with ACTUAL written content for echo detection
                        let mut s = state.write().await;
                        s.last_written_cid = head.cid.clone();
                        s.last_written_content = written_content;
                    }
                } else if initial_sync_strategy == "skip" && file.content != head.content {
                    // Offline edits detected - push local changes to server
                    // Note: push_content_by_type updates state with new CID internally
                    info!(
                        "Detected offline edits for: {} - pushing to server",
                        identifier
                    );
                    crate::sync::push_content_by_type(
                        client,
                        server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                        file.is_binary,
                        &file.content_type,
                        author,
                    )
                    .await?;
                    // Push functions already updated state with new CID, just set content
                    let mut s = state.write().await;
                    s.last_written_content = file.content.clone();
                } else {
                    // No offline edits (skip strategy, content matches) - seed SyncState
                    let mut s = state.write().await;
                    s.last_written_cid = head.cid.clone();
                    s.last_written_content = file.content.clone();
                }
            }
        }
        Ok(None) | Err(_) => {
            // Node doesn't exist yet (404) or request failed - push content
            crate::sync::push_content_by_type(
                client,
                server,
                &identifier,
                &file.content,
                &state,
                use_paths,
                file.is_binary,
                &file.content_type,
                author,
            )
            .await?;
        }
    }

    // Compute content hash
    let content_hash = if file.is_binary {
        match STANDARD.decode(&file.content) {
            Ok(raw_bytes) => compute_content_hash(&raw_bytes),
            Err(_) => compute_content_hash(file.content.as_bytes()),
        }
    } else {
        compute_content_hash(file.content.as_bytes())
    };

    // Store state for this file
    {
        let mut states = file_states.write().await;
        states.insert(
            file.relative_path.clone(),
            crate::sync::FileSyncState {
                relative_path: file.relative_path.clone(),
                identifier: identifier.clone(),
                state,
                task_handles: Vec::new(),
                use_paths,
                content_hash: Some(content_hash.clone()),
                crdt_last_content: None,
            },
        );
    }

    Ok((identifier, content_hash))
}

/// Spawn sync tasks (watcher, upload, SSE) for a single file.
/// Returns the task handles so they can be aborted on file deletion.
///
/// - push_only: Skip SSE subscription (only push local changes)
/// - pull_only: Skip file watcher (only pull server changes)
/// - force_push: Always fetch HEAD before upload to ensure local replaces server
#[allow(clippy::too_many_arguments)]
pub fn spawn_file_sync_tasks(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    force_push: bool,
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) -> Vec<JoinHandle<()>> {
    let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);
    let mut handles = Vec::new();

    // File watcher and upload tasks (skip if pull-only)
    if !pull_only {
        handles.push(tokio::spawn(file_watcher_task(file_path.clone(), file_tx)));
        handles.push(tokio::spawn(upload_task(
            client.clone(),
            server.clone(),
            identifier.clone(),
            file_path.clone(),
            state.clone(),
            file_rx,
            use_paths,
            force_push,
            author,
        )));
    }

    // SSE task (skip if push-only)
    // Use tracker variant for atomic writes when inode tracker is available
    if !push_only {
        #[cfg(unix)]
        if let Some(tracker) = inode_tracker {
            handles.push(tokio::spawn(crate::sync::sse_task_with_tracker(
                client, server, identifier, file_path, state, use_paths, tracker,
            )));
        } else {
            handles.push(tokio::spawn(sse_task(
                client, server, identifier, file_path, state, use_paths,
            )));
        }
        #[cfg(not(unix))]
        handles.push(tokio::spawn(sse_task(
            client, server, identifier, file_path, state, use_paths,
        )));
    }

    handles
}

/// Spawn sync tasks (watcher, upload, SSE) for a single file with flock-aware tracking.
///
/// This is an enhanced version of `spawn_file_sync_tasks` that includes FlockSyncState
/// for tracking pending outbound commits. This enables flock-aware synchronization
/// where SSE can verify that pending uploads are included in server responses before
/// writing inbound updates.
///
/// - push_only: Skip SSE subscription (only push local changes)
/// - pull_only: Skip file watcher (only pull server changes)
/// - force_push: Always fetch HEAD before upload to ensure local replaces server
/// - flock_state: Shared state for tracking pending outbound commits
/// - doc_id: Optional UUID for the document (needed for ancestry checking)
#[allow(clippy::too_many_arguments)]
pub fn spawn_file_sync_tasks_with_flock(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    force_push: bool,
    flock_state: FlockSyncState,
    doc_id: Option<uuid::Uuid>,
    author: String,
    #[cfg(unix)] inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
) -> Vec<JoinHandle<()>> {
    let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);
    let mut handles = Vec::new();

    // File watcher and flock-aware upload tasks (skip if pull-only)
    if !pull_only {
        handles.push(tokio::spawn(file_watcher_task(file_path.clone(), file_tx)));
        handles.push(tokio::spawn(upload_task_with_flock(
            client.clone(),
            server.clone(),
            identifier.clone(),
            file_path.clone(),
            state.clone(),
            file_rx,
            use_paths,
            force_push,
            flock_state.clone(),
            author,
        )));
    }

    // SSE task (skip if push-only)
    // Use flock-aware SSE task for ancestry checking
    if !push_only {
        // Note: inode_tracker is only used on unix for atomic writes with shadow hardlinks
        // For flock-aware sync, we use sse_task_with_flock which handles ancestry checking
        #[cfg(unix)]
        let _ = inode_tracker; // Acknowledge unused parameter on unix

        handles.push(tokio::spawn(crate::sync::sse_task_with_flock(
            client,
            server,
            identifier,
            file_path,
            state,
            use_paths,
            flock_state,
            doc_id,
        )));
    }

    handles
}

/// CRDT-aware upload task that publishes local changes via MQTT.
///
/// This is the replacement for `upload_task` when using CRDT peer sync.
/// Instead of HTTP POST /replace (which causes character-level diffs),
/// this publishes proper Yjs updates via MQTT that merge correctly.
///
/// # Arguments
/// * `mqtt_client` - MQTT client for publishing
/// * `workspace` - Workspace name for MQTT topics
/// * `node_id` - Document UUID
/// * `file_path` - Local file path
/// * `crdt_state` - Shared CRDT state for this directory
/// * `filename` - Filename within the directory (for state lookup)
/// * `rx` - Channel for file modification events
/// * `author` - Author name for commits
#[allow(clippy::too_many_arguments)]
pub async fn upload_task_crdt(
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    node_id: Uuid,
    file_path: PathBuf,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    filename: String,
    shared_last_content: SharedLastContent,
    mut rx: mpsc::Receiver<FileEvent>,
    author: String,
) {
    // Initialize shared last_content from current file content (if unset).
    // This prevents the first diff from being computed against empty content.
    {
        let mut shared = shared_last_content.write().await;
        if shared.is_none() {
            *shared = tokio::fs::read_to_string(&file_path)
                .await
                .ok()
                .filter(|s| !s.is_empty());
            if shared.is_some() {
                debug!(
                    "CRDT upload_task initialized with existing content for {}",
                    file_path.display()
                );
            }
        }
    }

    while let Some(event) = rx.recv().await {
        let FileEvent::Modified(raw_content) = event;

        // Detect if file is binary
        let content_info = detect_from_path(&file_path);
        let is_binary = content_info.is_binary || is_binary_content(&raw_content);

        if is_binary {
            // CRDT sync doesn't support binary files well (yet)
            // For now, skip and log a warning
            warn!(
                "CRDT upload_task skipping binary file: {}",
                file_path.display()
            );
            continue;
        }

        let new_content = String::from_utf8_lossy(&raw_content).to_string();

        // Get the old content for diff computation
        let old_content = {
            let shared = shared_last_content.read().await;
            shared.clone().unwrap_or_default()
        };

        // Skip if content unchanged
        if old_content == new_content {
            debug!(
                "CRDT upload_task: content unchanged for {}",
                file_path.display()
            );
            continue;
        }

        // Skip if new content is empty but old content exists.
        // This is almost always a transient state during a non-atomic file write
        // (file truncated before content is written). We don't want to publish
        // a DELETE operation for transient states.
        if new_content.is_empty() && !old_content.is_empty() {
            debug!(
                "CRDT upload_task: skipping transient empty state for {} (old_len={})",
                file_path.display(),
                old_content.len()
            );
            continue;
        }

        // DEBUG: Log the diff being computed
        info!(
            "CRDT upload_task: computing diff for {} - old_len={}, new_len={}, old_preview={:?}, new_preview={:?}",
            file_path.display(),
            old_content.len(),
            new_content.len(),
            old_content.chars().take(50).collect::<String>(),
            new_content.chars().take(50).collect::<String>()
        );

        // Get or create the CRDT state for this file
        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(&filename, node_id);

        // Publish the change via MQTT
        match publish_text_change(
            &mqtt_client,
            &workspace,
            &node_id.to_string(),
            file_state,
            &old_content,
            &new_content,
            &author,
        )
        .await
        {
            Ok(result) => {
                info!(
                    "CRDT upload: published commit {} for {} ({} bytes)",
                    result.cid,
                    file_path.display(),
                    result.update_bytes.len()
                );
                // Update our last known content
                let mut shared = shared_last_content.write().await;
                *shared = Some(new_content);

                // Save the state to persist CID tracking
                drop(state_guard);
                let state_guard = crdt_state.read().await;
                if let Err(e) = state_guard
                    .save(file_path.parent().unwrap_or(&file_path))
                    .await
                {
                    warn!("Failed to save CRDT state: {}", e);
                }
            }
            Err(e) => {
                // ContentUnchanged is not really an error
                if !matches!(e, crate::sync::error::SyncError::ContentUnchanged) {
                    error!("CRDT upload failed for {}: {}", file_path.display(), e);
                }
            }
        }
    }

    info!(
        "CRDT upload_task shutting down for: {}",
        file_path.display()
    );
}

/// Initialize CRDT state for a file from the server's HEAD.
///
/// This is critical for CRDT sync to work correctly. Without initializing from
/// the server's Yjs state, each sync client would create its own independent
/// operation history. This causes merge failures where:
/// - Client A deletes text (delete operation references A's operations)
/// - Client B has different history (different client ID, different operation IDs)
/// - B receives the delete but can't find operations to delete (different origin)
/// - B's content is restored, triggering a loop
///
/// By fetching the server's Yjs state and using it to initialize our local state,
/// all clients share the same operation history and merges work correctly.
///
/// # Arguments
/// * `client` - HTTP client for fetching from server
/// * `server` - Server URL
/// * `node_id` - UUID of the document
/// * `crdt_state` - Shared CRDT state to initialize
/// * `filename` - Filename within the directory (for state lookup)
pub async fn initialize_crdt_state_from_server(
    client: &Client,
    server: &str,
    node_id: Uuid,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    file_path: &Path,
) -> SyncResult<()> {
    initialize_crdt_state_from_server_with_pending(
        client, server, node_id, crdt_state, filename, file_path, None, None, None,
    )
    .await
}

/// Initialize CRDT state from server and process any pending edits.
///
/// This extended version accepts optional MQTT client, workspace, and author
/// to process pending edits that arrived before initialization completed.
///
/// # Arguments
/// * `client` - HTTP client for fetching from server
/// * `server` - Server URL
/// * `node_id` - UUID of the document
/// * `crdt_state` - Shared CRDT state to initialize
/// * `filename` - Filename within the directory (for state lookup)
/// * `file_path` - Path to the local file
/// * `mqtt_client` - Optional MQTT client for processing pending edits
/// * `workspace` - Optional workspace name for MQTT topics
/// * `author` - Optional author for merge commits
#[allow(clippy::too_many_arguments)]
pub async fn initialize_crdt_state_from_server_with_pending(
    client: &Client,
    server: &str,
    node_id: Uuid,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    file_path: &Path,
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: Option<&str>,
    author: Option<&str>,
) -> SyncResult<()> {
    // Check if we need initialization
    {
        let state = crdt_state.read().await;
        if let Some(file_state) = state.get_file(filename) {
            if !file_state.needs_server_init() {
                debug!(
                    "CRDT state already initialized for {}, skipping server fetch",
                    filename
                );
                return Ok(());
            }
        }
    }

    // Fetch HEAD from server
    let identifier = node_id.to_string();
    match fetch_head(client, server, &identifier, false).await {
        Ok(Some(head)) => {
            if let (Some(ref state_b64), Some(ref cid)) = (&head.state, &head.cid) {
                // Initialize CRDT state and collect pending edits
                let pending_edits = {
                    let mut state = crdt_state.write().await;
                    let file_state = state.get_or_create_file(filename, node_id);
                    file_state.initialize_from_server(state_b64, cid);

                    // Take any pending edits that arrived before init
                    file_state.take_pending_edits()
                };

                // CRITICAL: Also write the server's content to the local file.
                // This prevents the upload task from computing a diff against
                // empty/stale local content and publishing deletes that wipe
                // out the server state.
                if !head.content.is_empty() {
                    // Read current local content
                    let local_content = tokio::fs::read_to_string(file_path)
                        .await
                        .unwrap_or_default();

                    // Only write if local differs from server
                    if local_content != head.content {
                        if let Err(e) = tokio::fs::write(file_path, &head.content).await {
                            warn!(
                                "Failed to write server content to {}: {}",
                                file_path.display(),
                                e
                            );
                        } else {
                            info!(
                                "Wrote server content to {} ({} bytes)",
                                file_path.display(),
                                head.content.len()
                            );
                        }
                    }
                }

                info!(
                    "Initialized CRDT state for {} from server HEAD {}",
                    filename, cid
                );

                // Process any pending edits that arrived before initialization
                if !pending_edits.is_empty() {
                    info!(
                        "Processing {} pending edits for {} after CRDT init",
                        pending_edits.len(),
                        filename
                    );

                    for pending in pending_edits {
                        if let Ok(edit_msg) = parse_edit_message(&pending.payload) {
                            let mut state = crdt_state.write().await;
                            let file_state = state.get_or_create_file(filename, node_id);

                            match process_received_edit(
                                mqtt_client,
                                workspace.unwrap_or(""),
                                &identifier,
                                file_state,
                                &edit_msg,
                                author.unwrap_or(""),
                            )
                            .await
                            {
                                Ok((result, maybe_content)) => {
                                    drop(state); // Release lock before file I/O

                                    if let Some(content) = maybe_content {
                                        if let Err(e) = tokio::fs::write(file_path, &content).await
                                        {
                                            warn!(
                                                "Failed to write pending edit to {}: {}",
                                                file_path.display(),
                                                e
                                            );
                                        } else {
                                            info!(
                                                "Applied pending edit to {} ({} bytes, {:?})",
                                                file_path.display(),
                                                content.len(),
                                                result
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to process pending edit for {}: {}", filename, e);
                                }
                            }
                        } else {
                            warn!("Failed to parse pending edit for {}, skipping", filename);
                        }
                    }

                    // Save state after processing pending edits
                    let state = crdt_state.read().await;
                    if let Err(e) = state.save(file_path.parent().unwrap_or(file_path)).await {
                        warn!("Failed to save CRDT state after pending edits: {}", e);
                    }
                }

                Ok(())
            } else {
                // Server has no state yet (empty document) - that's fine
                // Still need to process pending edits as they may initialize the doc
                let pending_edits = {
                    let mut state = crdt_state.write().await;
                    let file_state = state.get_or_create_file(filename, node_id);
                    file_state.take_pending_edits()
                };

                if !pending_edits.is_empty() {
                    info!(
                        "Server has no state yet, processing {} pending edits for {}",
                        pending_edits.len(),
                        filename
                    );

                    for pending in pending_edits {
                        if let Ok(edit_msg) = parse_edit_message(&pending.payload) {
                            let mut state = crdt_state.write().await;
                            let file_state = state.get_or_create_file(filename, node_id);

                            match process_received_edit(
                                mqtt_client,
                                workspace.unwrap_or(""),
                                &identifier,
                                file_state,
                                &edit_msg,
                                author.unwrap_or(""),
                            )
                            .await
                            {
                                Ok((result, maybe_content)) => {
                                    drop(state);

                                    if let Some(content) = maybe_content {
                                        if let Err(e) = tokio::fs::write(file_path, &content).await
                                        {
                                            warn!(
                                                "Failed to write pending edit to {}: {}",
                                                file_path.display(),
                                                e
                                            );
                                        } else {
                                            info!(
                                                "Applied pending edit to {} ({} bytes, {:?})",
                                                file_path.display(),
                                                content.len(),
                                                result
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to process pending edit for {}: {}", filename, e);
                                }
                            }
                        }
                    }

                    // Save state after processing pending edits
                    let state = crdt_state.read().await;
                    if let Err(e) = state.save(file_path.parent().unwrap_or(file_path)).await {
                        warn!("Failed to save CRDT state after pending edits: {}", e);
                    }
                }

                debug!(
                    "Server has no Yjs state for {} yet, starting fresh",
                    filename
                );
                Ok(())
            }
        }
        Ok(None) => {
            // Document doesn't exist on server yet - that's fine
            debug!("Document {} doesn't exist on server yet", identifier);
            Ok(())
        }
        Err(e) => {
            warn!(
                "Failed to fetch HEAD for CRDT initialization of {}: {:?}",
                filename, e
            );
            // Don't fail - we can still try to sync, just might have issues
            Ok(())
        }
    }
}

/// Spawn CRDT-aware sync tasks for a single file.
///
/// This is the CRDT equivalent of `spawn_file_sync_tasks`.
/// Uses MQTT publish for local changes instead of HTTP /replace.
#[allow(clippy::too_many_arguments)]
pub fn spawn_file_sync_tasks_crdt(
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    node_id: Uuid,
    file_path: PathBuf,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    filename: String,
    shared_last_content: SharedLastContent,
    pull_only: bool,
    author: String,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    if !pull_only {
        let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);

        // Spawn file watcher
        handles.push(tokio::spawn(file_watcher_task(file_path.clone(), file_tx)));

        // Spawn CRDT upload task
        handles.push(tokio::spawn(upload_task_crdt(
            mqtt_client.clone(),
            workspace.clone(),
            node_id,
            file_path.clone(),
            crdt_state.clone(),
            filename.clone(),
            shared_last_content.clone(),
            file_rx,
            author.clone(),
        )));
    }

    // Spawn CRDT receive task for incoming changes
    handles.push(tokio::spawn(receive_task_crdt(
        mqtt_client,
        workspace,
        node_id,
        file_path,
        crdt_state,
        filename,
        shared_last_content,
        author,
    )));

    handles
}

/// CRDT receive task that subscribes to MQTT edits and applies them locally.
///
/// This handles the "receive" side of CRDT peer sync:
/// - Subscribe to MQTT edits for this file's UUID
/// - When an edit arrives, parse and apply via process_received_edit
/// - If the merge produces content to write, update the local file
#[allow(clippy::too_many_arguments)]
pub async fn receive_task_crdt(
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    node_id: Uuid,
    file_path: PathBuf,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    filename: String,
    shared_last_content: SharedLastContent,
    author: String,
) {
    let node_id_str = node_id.to_string();

    // CRITICAL: Create the broadcast receiver BEFORE subscribing.
    // This ensures we receive retained messages that the broker sends immediately
    // after we subscribe.
    let mut message_rx = mqtt_client.subscribe_messages();

    // Subscribe to edits for this file's UUID
    let topic = Topic::edits(&workspace, &node_id_str).to_topic_string();

    info!(
        "CRDT receive_task: subscribing to edits for {} at topic: {}",
        file_path.display(),
        topic
    );

    if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
        error!("CRDT receive_task: failed to subscribe to {}: {}", topic, e);
        return;
    }
    let context = format!("CRDT receive {}", file_path.display());

    while let Some(msg) = recv_broadcast(&mut message_rx, &context, None::<fn(u64)>).await {
        // Check if this message is for our topic
        if msg.topic != topic {
            continue;
        }

        debug!(
            "CRDT receive_task: got edit for {} ({} bytes)",
            file_path.display(),
            msg.payload.len()
        );

        // Parse the edit message
        let edit_msg = match parse_edit_message(&msg.payload) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "CRDT receive_task: failed to parse edit message for {}: {}",
                    file_path.display(),
                    e
                );
                continue;
            }
        };

        // Get or create the CRDT state for this file
        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(&filename, node_id);

        // Check if CRDT state needs initialization.
        // If so, queue the edit for later processing instead of trying to merge
        // it now (which would fail or produce incorrect results without shared history).
        if file_state.needs_server_init() {
            file_state.queue_pending_edit(msg.payload.clone());
            debug!(
                "CRDT receive_task: needs init for {}, queued edit (queue size: {})",
                file_path.display(),
                file_state.pending_edits.len()
            );
            drop(state_guard);
            continue; // Skip to next message
        }

        // Process the received edit
        match process_received_edit(
            Some(&mqtt_client),
            &workspace,
            &node_id_str,
            file_state,
            &edit_msg,
            &author,
        )
        .await
        {
            Ok((result, maybe_content)) => {
                use crate::sync::crdt_merge::MergeResult;

                match result {
                    MergeResult::AlreadyKnown => {
                        debug!(
                            "CRDT receive_task: commit already known for {}",
                            file_path.display()
                        );
                    }
                    MergeResult::FastForward { new_head } => {
                        info!(
                            "CRDT receive_task: fast-forward to {} for {}",
                            new_head,
                            file_path.display()
                        );
                    }
                    MergeResult::Merged {
                        merge_cid,
                        remote_cid,
                    } => {
                        info!(
                            "CRDT receive_task: merged {} into {} for {}",
                            remote_cid,
                            merge_cid,
                            file_path.display()
                        );
                    }
                    MergeResult::LocalAhead => {
                        debug!(
                            "CRDT receive_task: local is ahead for {}",
                            file_path.display()
                        );
                    }
                    MergeResult::NeedsMerge { .. } => {
                        // NeedsMerge is only used internally by determine_merge_strategy
                        // and is converted to Merged by process_received_edit
                        unreachable!("process_received_edit should not return NeedsMerge")
                    }
                }

                // Write content to file if merge produced new content
                if let Some(content) = maybe_content {
                    // Check for rollback writes - don't write if it would truncate existing content
                    // This enforces the "no rollback writes" invariant from the CRDT design spec
                    let existing_content = tokio::fs::read_to_string(&file_path)
                        .await
                        .unwrap_or_default();

                    info!(
                        "CRDT receive_task: merge produced content for {} - existing={} bytes ({:?}), new={} bytes ({:?})",
                        file_path.display(),
                        existing_content.len(),
                        existing_content.chars().take(40).collect::<String>(),
                        content.len(),
                        content.chars().take(40).collect::<String>()
                    );

                    // Determine if this is a rollback write that would lose data
                    // We use a threshold to distinguish legitimate edits from merge conflicts:
                    // - Legitimate edits may shorten content (deleting text)
                    // - Merge conflicts often produce drastically shorter content (50%+ reduction)
                    let is_rollback = if content.is_empty() && !existing_content.is_empty() {
                        // Writing empty content when file has content is a rollback
                        true
                    } else if !content.is_empty()
                        && !existing_content.is_empty()
                        && content.len() < existing_content.len() / 2
                    {
                        // Content reduced by more than 50% - likely a merge conflict, not an edit.
                        // This prevents sync loops where clients flip-flop between versions
                        // while still allowing legitimate edits that shorten content.
                        true
                    } else {
                        false
                    };

                    if is_rollback {
                        warn!(
                            "CRDT receive_task: refusing rollback write for {} (existing {} bytes, new {} bytes)",
                            file_path.display(),
                            existing_content.len(),
                            content.len()
                        );
                        // Don't write - continue processing other messages
                    } else {
                        // Update shared last_content BEFORE writing to avoid echo publishes.
                        let previous_content = {
                            let mut shared = shared_last_content.write().await;
                            let prev = shared.clone();
                            *shared = Some(content.clone());
                            prev
                        };

                        match tokio::fs::write(&file_path, &content).await {
                            Ok(()) => {
                                info!(
                                    "CRDT receive_task: wrote {} bytes to {}",
                                    content.len(),
                                    file_path.display()
                                );
                            }
                            Err(e) => {
                                // Restore previous content marker on failure.
                                let mut shared = shared_last_content.write().await;
                                *shared = previous_content;
                                error!(
                                    "CRDT receive_task: failed to write to {}: {}",
                                    file_path.display(),
                                    e
                                );
                            }
                        }
                    }
                }

                // Save the updated state
                drop(state_guard);
                let state_guard = crdt_state.read().await;
                if let Err(e) = state_guard
                    .save(file_path.parent().unwrap_or(&file_path))
                    .await
                {
                    warn!("CRDT receive_task: failed to save state: {}", e);
                }
            }
            Err(e) => {
                warn!(
                    "CRDT receive_task: failed to process edit for {}: {}",
                    file_path.display(),
                    e
                );
            }
        }
    }

    info!(
        "CRDT receive_task: shutting down for {}",
        file_path.display()
    );
}
