//! File mode synchronization.
//!
//! This module contains functions for syncing a single file with a server document.

use crate::sync::directory::{scan_directory, schema_to_json, ScanOptions};
use crate::sync::file_events::find_owning_document;
use crate::sync::state_file::compute_content_hash;
use crate::sync::uuid_map::fetch_node_id_from_schema;
use crate::sync::{
    build_edit_url, build_head_url, build_replace_url, create_yjs_text_update, detect_from_path,
    encode_node_id, file_watcher_task, is_binary_content, looks_like_base64_binary,
    push_json_content, push_jsonl_content, push_schema_to_server, record_upload_result,
    refresh_from_head, sse_task, EditRequest, EditResponse, FileEvent, FlockSyncState,
    HeadResponse, ReplaceResponse, SyncState, PENDING_WRITE_TIMEOUT,
};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Number of retries when content differs during a pending write (handles partial writes)
pub const BARRIER_RETRY_COUNT: u32 = 5;
/// Delay between retries when checking for stable content
pub const BARRIER_RETRY_DELAY: Duration = Duration::from_millis(50);

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

        // Check for pending write barrier and handle echo detection
        // Track whether we detected an echo and need to refresh from HEAD
        let mut echo_detected = false;
        let mut should_refresh = false;

        {
            let mut s = state.write().await;

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

                if content == s.last_written_content {
                    debug!("Ignoring echo: content matches last written");
                    echo_detected = true;
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
            let json_upload_succeeded =
                match push_json_content(&client, &server, &identifier, &content, &state, use_paths)
                    .await
                {
                    Ok(_) => true,
                    Err(e) => {
                        error!("JSON upload failed: {}", e);
                        false
                    }
                };
            // Refresh from HEAD if server edits were skipped AND upload succeeded
            // IMPORTANT: Don't refresh after failed upload to avoid overwriting local edits
            if should_refresh {
                if json_upload_succeeded {
                    let refresh_succeeded = refresh_from_head(
                        &client,
                        &server,
                        &identifier,
                        &file_path,
                        &state,
                        use_paths,
                    )
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
            )
            .await
            {
                Ok(_) => true,
                Err(e) => {
                    error!("JSONL upload failed: {}", e);
                    false
                }
            };
            if should_refresh {
                if jsonl_upload_succeeded {
                    let refresh_succeeded = refresh_from_head(
                        &client,
                        &server,
                        &identifier,
                        &file_path,
                        &state,
                        use_paths,
                    )
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
            continue;
        }

        // Get parent CID to decide which endpoint to use
        // In force-push mode, always fetch HEAD to ensure we replace current content
        let parent_cid = if force_push {
            // Force-push: fetch HEAD's cid to ensure we replace current content
            let head_url = build_head_url(&server, &identifier, use_paths);
            match client.get(&head_url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.json::<HeadResponse>().await {
                    Ok(head) => head.cid,
                    Err(e) => {
                        error!("Force-push: failed to parse HEAD response: {}", e);
                        continue;
                    }
                },
                Ok(resp) => {
                    error!("Force-push: HEAD request failed with {}", resp.status());
                    continue;
                }
                Err(e) => {
                    error!("Force-push: HEAD request failed: {}", e);
                    continue;
                }
            }
        } else {
            let s = state.read().await;
            s.last_written_cid.clone()
        };

        // Track upload success - only refresh if upload succeeded
        let mut upload_succeeded = false;

        match parent_cid {
            Some(parent) => {
                // Normal case: use replace endpoint
                // For force-push, parent is HEAD's cid so this is a simple replace
                let replace_url = build_replace_url(&server, &identifier, &parent, use_paths);

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

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name).await;
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
            }
            None => {
                // First commit: use edit endpoint with generated Yjs update
                info!("Creating initial commit...");
                let update = create_yjs_text_update(&content);
                let edit_url = build_edit_url(&server, &identifier, use_paths);
                let edit_req = EditRequest {
                    update,
                    author: Some("sync-client".to_string()),
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

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name).await;
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
) {
    while let Some(event) = rx.recv().await {
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

                if content == s.last_written_content {
                    debug!("Ignoring echo: content matches last written");
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
            let json_upload_succeeded =
                push_json_content(&client, &server, &identifier, &content, &state, use_paths)
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
            if should_refresh {
                if json_upload_succeeded {
                    let refresh_succeeded = refresh_from_head(
                        &client,
                        &server,
                        &identifier,
                        &file_path,
                        &state,
                        use_paths,
                    )
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
            continue;
        }

        if is_jsonl {
            let jsonl_upload_succeeded =
                push_jsonl_content(&client, &server, &identifier, &content, &state, use_paths)
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
            if should_refresh {
                if jsonl_upload_succeeded {
                    let refresh_succeeded = refresh_from_head(
                        &client,
                        &server,
                        &identifier,
                        &file_path,
                        &state,
                        use_paths,
                    )
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
            continue;
        }

        // Get parent CID for text file upload
        let parent_cid = if force_push {
            let head_url = build_head_url(&server, &identifier, use_paths);
            match client.get(&head_url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.json::<HeadResponse>().await {
                    Ok(head) => head.cid,
                    Err(e) => {
                        error!("Force-push: failed to parse HEAD response: {}", e);
                        continue;
                    }
                },
                Ok(resp) => {
                    error!("Force-push: HEAD request failed with {}", resp.status());
                    continue;
                }
                Err(e) => {
                    error!("Force-push: HEAD request failed: {}", e);
                    continue;
                }
            }
        } else {
            let s = state.read().await;
            s.last_written_cid.clone()
        };

        let mut upload_succeeded = false;

        match parent_cid {
            Some(parent) => {
                let replace_url = build_replace_url(&server, &identifier, &parent, use_paths);

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

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name).await;
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
            }
            None => {
                info!("Creating initial commit...");
                let update = create_yjs_text_update(&content);
                let edit_url = build_edit_url(&server, &identifier, use_paths);
                let edit_req = EditRequest {
                    update,
                    author: Some("sync-client".to_string()),
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

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name).await;
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
) -> Result<(), Box<dyn std::error::Error>> {
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(node_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to get HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;

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
    {
        let mut s = state.write().await;
        s.last_written_cid = head.cid.clone();
        s.last_written_content = head.content.clone();

        // Save to state file for offline change detection
        // Use the actual bytes written to disk, not the server response
        if let Some(ref cid) = head.cid {
            let content_hash = compute_content_hash(&bytes_written);
            let file_name = file_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            s.mark_synced(cid, &content_hash, &file_name).await;
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
) -> Result<(String, String), Box<dyn std::error::Error>> {
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
            if let Ok(schema) = scan_directory(&owning.directory, &options) {
                if let Ok(json) = schema_to_json(&schema) {
                    info!(
                        "Pushing subdirectory schema for {} (document {})",
                        owning.directory.display(),
                        owning.document_id
                    );
                    if let Err(e) =
                        push_schema_to_server(client, server, &owning.document_id, &json).await
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
        }

        final_identifier
    };
    info!("Syncing file: {} -> {}", file.relative_path, identifier);

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
    let file_head_url = crate::sync::build_head_url(server, &identifier, use_paths);
    let file_head_resp = client.get(&file_head_url).send().await;

    if let Ok(resp) = file_head_resp {
        debug!(
            "File head response status: {} for {}",
            resp.status(),
            identifier
        );
        if resp.status().is_success() {
            let head: crate::sync::HeadResponse = resp.json().await?;
            info!(
                "File head content empty: {}, strategy: {}",
                head.content.is_empty(),
                initial_sync_strategy
            );
            if head.content.is_empty() || initial_sync_strategy == "local" {
                // Push local content
                info!(
                    "Pushing initial content for: {} ({} bytes)",
                    identifier,
                    file.content.len()
                );
                let is_json = !file.is_binary && file.content_type.starts_with("application/json");
                let is_jsonl = !file.is_binary && file.content_type == "application/x-ndjson";
                if is_json {
                    crate::sync::push_json_content(
                        client,
                        server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                } else if is_jsonl {
                    crate::sync::push_jsonl_content(
                        client,
                        server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                } else {
                    crate::sync::push_file_content(
                        client,
                        server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                }
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
                        // No local CID - first sync, trust server unless content differs
                        if file.content != head.content {
                            // Local has different content but no CID - push local
                            // (This handles the case where local was edited offline before first sync)
                            info!(
                                "Local has different content but no CID for {}, pushing local",
                                file.relative_path
                            );
                            true
                        } else {
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
                        let is_json =
                            !file.is_binary && file.content_type.starts_with("application/json");
                        let is_jsonl =
                            !file.is_binary && file.content_type == "application/x-ndjson";
                        if is_json {
                            crate::sync::push_json_content(
                                client,
                                server,
                                &identifier,
                                &file.content,
                                &state,
                                use_paths,
                            )
                            .await?;
                        } else if is_jsonl {
                            crate::sync::push_jsonl_content(
                                client,
                                server,
                                &identifier,
                                &file.content,
                                &state,
                                use_paths,
                            )
                            .await?;
                        } else {
                            crate::sync::push_file_content(
                                client,
                                server,
                                &identifier,
                                &file.content,
                                &state,
                                use_paths,
                            )
                            .await?;
                        }
                    } else {
                        // Pull server content to local
                        if file.is_binary {
                            if let Ok(decoded) = STANDARD.decode(&head.content) {
                                tokio::fs::write(file_path, &decoded).await?;
                            }
                        } else {
                            // Ensure text files end with a trailing newline
                            let content_with_newline = ensure_trailing_newline(&head.content);
                            tokio::fs::write(file_path, content_with_newline).await?;
                        }
                        // Seed SyncState with server content after pull
                        let mut s = state.write().await;
                        s.last_written_cid = head.cid.clone();
                        s.last_written_content = head.content.clone();
                    }
                } else if initial_sync_strategy == "skip" && file.content != head.content {
                    // Offline edits detected - push local changes to server
                    // Note: push_json_content/push_file_content update state with new CID internally
                    info!(
                        "Detected offline edits for: {} - pushing to server",
                        identifier
                    );
                    let is_json =
                        !file.is_binary && file.content_type.starts_with("application/json");
                    let is_jsonl = !file.is_binary && file.content_type == "application/x-ndjson";
                    if is_json {
                        crate::sync::push_json_content(
                            client,
                            server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    } else if is_jsonl {
                        crate::sync::push_jsonl_content(
                            client,
                            server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    } else {
                        crate::sync::push_file_content(
                            client,
                            server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    }
                    // Push functions already updated state with new CID, just set content
                    let mut s = state.write().await;
                    s.last_written_content = file.content.clone();
                } else {
                    // No offline edits (skip strategy, content matches) - seed SyncState
                    let mut s = state.write().await;
                    s.last_written_cid = head.cid;
                    s.last_written_content = file.content.clone();
                }
            }
        } else {
            // Node doesn't exist yet - push content
            info!("Node not ready, will push with retries for: {}", identifier);
            let is_json = !file.is_binary && file.content_type.starts_with("application/json");
            let is_jsonl = !file.is_binary && file.content_type == "application/x-ndjson";
            if is_json {
                crate::sync::push_json_content(
                    client,
                    server,
                    &identifier,
                    &file.content,
                    &state,
                    use_paths,
                )
                .await?;
            } else if is_jsonl {
                crate::sync::push_jsonl_content(
                    client,
                    server,
                    &identifier,
                    &file.content,
                    &state,
                    use_paths,
                )
                .await?;
            } else {
                crate::sync::push_file_content(
                    client,
                    server,
                    &identifier,
                    &file.content,
                    &state,
                    use_paths,
                )
                .await?;
            }
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
