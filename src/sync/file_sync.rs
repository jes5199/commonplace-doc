//! File mode synchronization.
//!
//! This module contains functions for syncing a single file with a server document.

use crate::sync::state_file::compute_content_hash;
use crate::sync::{
    build_edit_url, build_replace_url, create_yjs_text_update, detect_from_path, encode_node_id,
    is_binary_content, push_json_content, refresh_from_head, EditRequest, EditResponse, FileEvent,
    HeadResponse, ReplaceResponse, SyncState, PENDING_WRITE_TIMEOUT,
};
use reqwest::Client;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Number of retries when content differs during a pending write (handles partial writes)
pub const BARRIER_RETRY_COUNT: u32 = 5;
/// Delay between retries when checking for stable content
pub const BARRIER_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Task that handles file changes and uploads to server
pub async fn upload_task(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    mut rx: mpsc::Receiver<FileEvent>,
    use_paths: bool,
) {
    while let Some(_event) = rx.recv().await {
        // Read current file content as bytes
        let raw_content = match tokio::fs::read(&file_path).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read file: {}", e);
                continue;
            }
        };

        // Detect if file is binary and convert accordingly
        let content_info = detect_from_path(&file_path);
        let is_binary = content_info.is_binary || is_binary_content(&raw_content);
        let is_json = !is_binary && content_info.mime_type == "application/json";

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

        // Get parent CID to decide which endpoint to use
        let parent_cid = {
            let s = state.read().await;
            s.last_written_cid.clone()
        };

        // Track upload success - only refresh if upload succeeded
        let mut upload_succeeded = false;

        match parent_cid {
            Some(parent) => {
                // Normal case: use replace endpoint
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
        // IMPORTANT: Only refresh after successful upload to avoid overwriting
        // local edits when upload fails
        if should_refresh {
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
    } else {
        // Extension says text, but try decoding as base64 in case
        // this was a binary file detected by content sniffing
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
