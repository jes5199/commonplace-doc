//! File mode synchronization.
//!
//! This module contains functions for syncing a single file with a server document.

// Note: recv_broadcast_with_lag was replaced with direct tokio::select! in receive_task_crdt
use crate::mqtt::{IncomingMessage, MqttClient, Topic};
use crate::sync::client::fetch_head;
use crate::sync::crdt_merge::{parse_edit_message, process_received_edit};
use crate::sync::crdt_publish::{publish_text_change, publish_yjs_update};
use crate::sync::crdt_state::DirectorySyncState;
use crate::sync::directory::{scan_directory_to_json, ScanOptions};
use crate::sync::file_events::find_owning_document;
use crate::sync::state::InodeKey;
use crate::sync::state_file::compute_content_hash;
use crate::sync::uuid_map::fetch_node_id_from_schema;
use crate::sync::{
    build_edit_url, build_replace_url, create_yjs_text_update, detect_from_path,
    ensure_trailing_newline, error::SyncResult, file_watcher_task, is_binary_content,
    is_default_content_for_mime, looks_like_base64_binary, push_json_content, push_jsonl_content,
    push_schema_to_server, refresh_from_head, trace_timeline, EditRequest, EditResponse, FileEvent,
    ReplaceResponse, SharedLastContent, SyncState, TimelineMilestone,
};
use reqwest::Client;
use rumqttc::QoS;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use yrs::{GetString, ReadTxn, Transact};

/// Timeout for pending write barrier (30 seconds)
pub const PENDING_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Number of retries when content differs during a pending write (handles partial writes)
pub const BARRIER_RETRY_COUNT: u32 = 5;
/// Delay between retries when checking for stable content
pub const BARRIER_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Result of preparing file content for upload.
///
/// This struct consolidates the content detection and encoding logic that's
/// shared across upload_task variants.
#[derive(Debug)]
pub struct PreparedContent {
    /// The content to upload (text or base64-encoded binary)
    pub content: String,
    /// Whether the content is binary (base64-encoded)
    pub is_binary: bool,
    /// Whether this is a JSON file (application/json)
    pub is_json: bool,
    /// Whether this is a JSONL file (application/x-ndjson)
    pub is_jsonl: bool,
}

/// Prepare raw file content for upload.
///
/// Detects content type from file path, checks for binary content,
/// and encodes appropriately (base64 for binary, UTF-8 for text).
///
/// # Arguments
/// * `raw_content` - The raw bytes read from the file
/// * `file_path` - Path to the file (used for MIME type detection)
///
/// # Returns
/// A `PreparedContent` struct with the processed content and type flags.
pub fn prepare_content_for_upload(raw_content: &[u8], file_path: &Path) -> PreparedContent {
    use base64::{engine::general_purpose::STANDARD, Engine};

    let content_info = detect_from_path(file_path);
    let is_binary = content_info.is_binary || is_binary_content(raw_content);
    let is_json = !is_binary && content_info.mime_type == "application/json";
    let is_jsonl = !is_binary && content_info.mime_type == "application/x-ndjson";

    let content = if is_binary {
        STANDARD.encode(raw_content)
    } else {
        String::from_utf8_lossy(raw_content).to_string()
    };

    PreparedContent {
        content,
        is_binary,
        is_json,
        is_jsonl,
    }
}

/// Result of echo detection from pending write barrier.
///
/// This struct captures the outcome of checking whether a file change notification
/// is an echo of our own write (which should be ignored) or a user edit (which
/// should be uploaded).
#[derive(Debug, Clone)]
pub struct EchoDetectionResult {
    /// Whether an echo was detected (content matches pending write or last written)
    pub echo_detected: bool,
    /// Whether we need to refresh from HEAD (server edits were skipped while barrier was up)
    pub should_refresh: bool,
    /// Updated content after retry loop (may differ from input if file was still being written)
    pub updated_content: Option<String>,
}

impl EchoDetectionResult {
    /// Create a result indicating an echo was detected.
    pub fn echo(should_refresh: bool) -> Self {
        Self {
            echo_detected: true,
            should_refresh,
            updated_content: None,
        }
    }

    /// Create a result indicating no echo (user edit detected).
    pub fn no_echo(should_refresh: bool, updated_content: Option<String>) -> Self {
        Self {
            echo_detected: false,
            should_refresh,
            updated_content,
        }
    }
}

/// Detect if a file change is an echo of our own write or a user edit.
///
/// This function checks the SyncState's pending write barrier and determines:
/// 1. If there's a pending write that matches - it's our echo, skip upload
/// 2. If the pending write timed out - handle based on content match
/// 3. If content doesn't match but is still changing - retry to wait for write completion
/// 4. If no barrier - compare against last written content (trimmed)
///
/// # Arguments
/// * `state` - The shared sync state (write lock will be acquired)
/// * `content` - The current file content (may be updated by retry loop)
/// * `is_binary` - Whether the content is binary (affects how we re-read file)
/// * `file_path` - Path to the file (for re-reading during retry)
///
/// # Returns
/// An `EchoDetectionResult` containing:
/// - `echo_detected`: true if this is our echo and should skip upload
/// - `should_refresh`: true if we need to refresh from HEAD after
/// - `updated_content`: Some(content) if the retry loop updated the content
///
/// # Note
/// This function is async because the retry loop may need to re-read the file
/// and sleep between retries. The state lock is released during the retry loop.
pub async fn detect_echo_from_pending_write(
    state: &Arc<RwLock<SyncState>>,
    content: &str,
    is_binary: bool,
    file_path: &Path,
) -> EchoDetectionResult {
    let mut s = state.write().await;

    debug!(
        "detect_echo: last_written_content={:?}, last_written_cid={:?}, has_barrier={}",
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
                debug!("Timed-out pending matches current content, treating as delayed echo");
                s.last_written_cid = pending.cid;
                s.last_written_content = pending.content;
                let should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;
                return EchoDetectionResult::echo(should_refresh);
            } else {
                // Content differs - this is a user edit that slipped through,
                // or the write failed. Upload with old parent for merge.
                debug!("Timed-out pending differs from current content, uploading as user edit");
                let should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;
                // DON'T update last_written_* - use old parent for CRDT merge
                return EchoDetectionResult::no_echo(should_refresh, None);
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
            let should_refresh = s.needs_head_refresh;
            s.needs_head_refresh = false;
            return EchoDetectionResult::echo(should_refresh);
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

            let mut current_content = content.to_string();
            let mut is_echo = false;
            for i in 0..BARRIER_RETRY_COUNT {
                sleep(BARRIER_RETRY_DELAY).await;

                // Re-read file
                let raw = match tokio::fs::read(file_path).await {
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
                    current_content = reread;
                    is_echo = true;
                    break;
                }

                if reread != current_content {
                    // Content still changing, update and keep retrying
                    debug!("Retry {}: content still changing", i + 1);
                    current_content = reread;
                }
                // If reread == current_content and != pending, content is stable but different (user edit)
            }

            // Re-acquire lock and finalize
            let mut s = state.write().await;

            if is_echo {
                // Our write completed after retries
                debug!("Echo confirmed after retries (id={})", pending_write_id);
                s.last_written_cid = pending_cid;
                s.last_written_content = current_content.clone();
                s.pending_write = None;
                let should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;
                return EchoDetectionResult::echo(should_refresh);
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
                let should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;
                return EchoDetectionResult::no_echo(should_refresh, Some(current_content));
            }
        }
    }

    // No barrier - normal echo detection
    // Also check needs_head_refresh in case server edit was skipped
    // due to local changes being pending
    let should_refresh = s.needs_head_refresh;
    s.needs_head_refresh = false;

    // Use trimmed comparison to avoid whitespace differences
    let content_trimmed = content.trim();
    let last_written_trimmed = s.last_written_content.trim();

    if content_trimmed == last_written_trimmed {
        debug!("Ignoring echo: content matches last written (trimmed)");
        return EchoDetectionResult::echo(should_refresh);
    }

    debug!(
        "No barrier, content mismatch - file: {:?} (len={}), last_written: {:?} (len={})",
        &content.chars().take(50).collect::<String>(),
        content.len(),
        &s.last_written_content.chars().take(50).collect::<String>(),
        s.last_written_content.len()
    );

    EchoDetectionResult::no_echo(should_refresh, None)
}

/// Interval between periodic divergence checks (seconds).
/// This ensures sync clients detect when they've diverged from the server
/// even if MQTT messages are missed.
pub const DIVERGENCE_CHECK_INTERVAL_SECS: u64 = 30;

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

        // Detect content type and encode appropriately
        let prepared = prepare_content_for_upload(&raw_content, &file_path);
        let mut content = prepared.content;
        let is_binary = prepared.is_binary;
        let is_json = prepared.is_json;
        let is_jsonl = prepared.is_jsonl;

        // Log event received for debugging
        debug!(
            "upload_task event: identifier={}, content={:?} (len={})",
            identifier,
            &content.chars().take(50).collect::<String>(),
            content.len()
        );

        // Check for pending write barrier and handle echo detection
        let echo_result =
            detect_echo_from_pending_write(&state, &content, is_binary, &file_path).await;
        let echo_detected = echo_result.echo_detected;
        let should_refresh = echo_result.should_refresh;

        // Update content if the retry loop produced a new value
        if let Some(updated) = echo_result.updated_content {
            content = updated;
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
                                    let content_hash = match tokio::fs::read(&file_path).await {
                                        Ok(bytes) => compute_content_hash(&bytes),
                                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                            // File was deleted between upload and hash
                                            warn!(
                                                "File {} deleted after upload, using empty hash",
                                                file_path.display()
                                            );
                                            String::new()
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to read {} for hash: {}",
                                                file_path.display(),
                                                e
                                            );
                                            String::new()
                                        }
                                    };
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
                                    let content_hash = match tokio::fs::read(&file_path).await {
                                        Ok(bytes) => compute_content_hash(&bytes),
                                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                            // File was deleted between commit and hash
                                            warn!(
                                                "File {} deleted after commit, using empty hash",
                                                file_path.display()
                                            );
                                            String::new()
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to read {} for hash: {}",
                                                file_path.display(),
                                                e
                                            );
                                            String::new()
                                        }
                                    };
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
                    let server_has_default =
                        is_default_content_for_mime(&head.content, &file.content_type);

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
                            is_default_content_for_mime(&file.content, &file.content_type);
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
                                && !is_default_content_for_mime(&head.content, &file.content_type)
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
            *shared = match tokio::fs::read_to_string(&file_path).await {
                Ok(s) if !s.is_empty() => {
                    debug!(
                        "CRDT upload_task initialized with existing content for {}",
                        file_path.display()
                    );
                    Some(s)
                }
                Ok(_) => None, // Empty file
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    error!(
                        "Failed to read initial content for {}: {}",
                        file_path.display(),
                        e
                    );
                    None
                }
            };
        }
    }

    while let Some(event) = rx.recv().await {
        let FileEvent::Modified(raw_content) = event;

        // Detect content type
        let prepared = prepare_content_for_upload(&raw_content, &file_path);

        if prepared.is_binary {
            // CRDT sync doesn't support binary files well (yet)
            // For now, skip and log a warning
            warn!(
                "CRDT upload_task skipping binary file: {}",
                file_path.display()
            );
            continue;
        }

        let new_content = prepared.content;

        // Get the old content for diff computation
        let old_content = {
            let shared = shared_last_content.read().await;
            shared.clone().unwrap_or_default()
        };

        trace!(
            "[UPLOAD-TRACE] upload_task_crdt recv for {} - old_len={} old={:?} new_len={} new={:?}",
            file_path.display(),
            old_content.len(),
            old_content.chars().take(30).collect::<String>(),
            new_content.len(),
            new_content.chars().take(30).collect::<String>()
        );

        // Skip if content unchanged
        if old_content == new_content {
            trace!(
                "[UPLOAD-TRACE] upload_task_crdt SKIPPING unchanged content for {}",
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

        // Compute content hashes for tracing
        let old_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            old_content.hash(&mut h);
            h.finish()
        };
        let new_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            new_content.hash(&mut h);
            h.finish()
        };
        info!(
            "[SANDBOX-TRACE] upload_task_crdt CHANGE_DETECTED file={} uuid={} old_len={} old_hash={:016x} new_len={} new_hash={:016x}",
            file_path.display(),
            node_id,
            old_content.len(),
            old_hash,
            new_content.len(),
            new_hash
        );
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

        // Publish the change via MQTT.
        // For JSON files, create a YMap update to match the server's document type.
        // For text files, use character-level YText diffs (more efficient).
        let publish_result = if prepared.is_json || prepared.is_jsonl {
            // JSON/JSONL files: create structured update (YMap/YArray)
            let content_type = if prepared.is_jsonl {
                crate::content_type::ContentType::Jsonl
            } else {
                crate::content_type::ContentType::Json
            };
            let base_state = file_state.yjs_state.as_deref();
            match crate::sync::crdt::yjs::create_yjs_structured_update(
                content_type,
                &new_content,
                base_state,
            ) {
                Ok(update_b64) => match crate::sync::crdt::yjs::base64_decode(&update_b64) {
                    Ok(update_bytes) => {
                        publish_yjs_update(
                            &mqtt_client,
                            &workspace,
                            &node_id.to_string(),
                            file_state,
                            update_bytes,
                            &author,
                        )
                        .await
                    }
                    Err(e) => Err(crate::sync::error::SyncError::other(format!(
                        "Failed to decode structured update: {}",
                        e
                    ))),
                },
                Err(e) => {
                    // Fall back to text change if structured update fails
                    // (e.g., content isn't valid JSON)
                    warn!(
                        "Structured update failed for {}, falling back to text: {}",
                        file_path.display(),
                        e
                    );
                    publish_text_change(
                        &mqtt_client,
                        &workspace,
                        &node_id.to_string(),
                        file_state,
                        &old_content,
                        &new_content,
                        &author,
                    )
                    .await
                }
            }
        } else {
            // Text files: use efficient character-level diffs
            publish_text_change(
                &mqtt_client,
                &workspace,
                &node_id.to_string(),
                file_state,
                &old_content,
                &new_content,
                &author,
            )
            .await
        };
        match publish_result {
            Ok(result) => {
                info!(
                    "[SANDBOX-TRACE] upload_task_crdt PUBLISHED file={} uuid={} cid={} update_bytes={}",
                    file_path.display(),
                    node_id,
                    result.cid,
                    result.update_bytes.len()
                );
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
        client, server, node_id, crdt_state, filename, file_path, None, None, None, None,
    )
    .await
}

/// Initialize CRDT state from server and process any pending edits.
///
/// This extended version accepts optional MQTT client, workspace, and author
/// to process pending edits that arrived before initialization completed.
///
/// **Note**: This function uses HTTP to fetch HEAD from server, which can
/// race with MQTT messages causing stale data issues. Consider using
/// MQTT retained messages for initialization in MQTT-only mode.
/// See CP-cpcu for the deprecation plan.
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
    shared_last_content: Option<&SharedLastContent>,
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
    // NOTE: This HTTP call can race with MQTT messages. See MqttOnlySyncConfig
    // for deprecation tracking. In the future, initial state should come from
    // MQTT retained messages.
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
                        // Update shared_last_content BEFORE writing to prevent echo detection
                        if let Some(slc) = shared_last_content {
                            let mut shared = slc.write().await;
                            *shared = Some(head.content.clone());
                        }
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
                } else {
                    // Server returned empty content but has CRDT state - extract from state.
                    // This happens when content was only written via CRDT operations
                    // (not full content replacement), leaving the content field empty.
                    let extracted_content = {
                        let state = crdt_state.read().await;
                        if let Some(file_state) = state.get_file(filename) {
                            match file_state.to_doc() {
                                Ok(doc) => {
                                    let txn = doc.transact();
                                    let content = txn
                                        .get_text("content")
                                        .map(|text| text.get_string(&txn))
                                        .unwrap_or_default();
                                    if content.is_empty() {
                                        None
                                    } else {
                                        info!(
                                            "Extracted {} bytes from CRDT state for {} (server content was empty)",
                                            content.len(),
                                            filename
                                        );
                                        Some(content)
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to convert file_state to doc for {}: {}",
                                        filename, e
                                    );
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    }; // state lock and transaction dropped here

                    if let Some(content) = extracted_content {
                        // Read current local content
                        let local_content = tokio::fs::read_to_string(file_path)
                            .await
                            .unwrap_or_default();

                        // Only write if local differs from extracted content
                        if local_content != content {
                            // Update shared_last_content BEFORE writing to prevent echo
                            if let Some(slc) = shared_last_content {
                                let mut shared = slc.write().await;
                                *shared = Some(content.clone());
                            }
                            if let Err(e) = tokio::fs::write(file_path, &content).await {
                                warn!(
                                    "Failed to write CRDT-extracted content to {}: {}",
                                    file_path.display(),
                                    e
                                );
                            } else {
                                info!(
                                    "Wrote CRDT-extracted content to {} ({} bytes)",
                                    file_path.display(),
                                    content.len()
                                );
                            }
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
                                None, // commit_store not available in file_sync init context
                            )
                            .await
                            {
                                Ok((result, maybe_content)) => {
                                    drop(state); // Release lock before file I/O

                                    if let Some(content) = maybe_content {
                                        // Update shared_last_content BEFORE writing to prevent echo
                                        if let Some(slc) = shared_last_content {
                                            let mut shared = slc.write().await;
                                            *shared = Some(content.clone());
                                        }
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
                // CRITICAL: Initialize to empty state so should_queue_edits() doesn't
                // return NeedsServerInit. Without this, incoming MQTT edits would be
                // queued forever because yjs_state remains None.
                {
                    let mut state = crdt_state.write().await;
                    let file_state = state.get_or_create_file(filename, node_id);
                    file_state.initialize_empty();
                }

                // Still need to process pending edits as they may have arrived during init
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
                                None, // commit_store not available in file_sync empty-state context
                            )
                            .await
                            {
                                Ok((result, maybe_content)) => {
                                    drop(state);

                                    if let Some(content) = maybe_content {
                                        // Update shared_last_content BEFORE writing to prevent echo
                                        if let Some(slc) = shared_last_content {
                                            let mut shared = slc.write().await;
                                            *shared = Some(content.clone());
                                        }
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
            // CRITICAL: Initialize to empty state so should_queue_edits() doesn't
            // return NeedsServerInit. Without this, incoming MQTT edits would be
            // queued forever because yjs_state remains None.
            {
                let mut state = crdt_state.write().await;
                let file_state = state.get_or_create_file(filename, node_id);
                file_state.initialize_empty();
            }
            debug!(
                "Document {} doesn't exist on server yet, initialized empty",
                identifier
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                "Failed to fetch HEAD for CRDT initialization of {}: {:?}",
                filename, e
            );
            // CRITICAL: Initialize to empty state so should_queue_edits() doesn't
            // return NeedsServerInit. Without this, incoming MQTT edits would be
            // queued forever because yjs_state remains None.
            {
                let mut state = crdt_state.write().await;
                let file_state = state.get_or_create_file(filename, node_id);
                file_state.initialize_empty();
            }
            // Don't fail - we can still try to sync via MQTT
            Ok(())
        }
    }
}

/// Spawn CRDT-aware sync tasks for a single file.
///
/// This is the CRDT equivalent of `spawn_file_sync_tasks`.
/// Uses MQTT publish for local changes instead of HTTP /replace.
///
/// If `file_states` and `relative_path` are provided, checks for existing active
/// tasks before spawning. Returns empty Vec if tasks are already running to prevent
/// duplicate publish/receive loops.
///
/// Uses global `SyncGuardrails` to track and warn about duplicate spawn attempts.
#[allow(clippy::too_many_arguments)]
pub fn spawn_file_sync_tasks_crdt(
    mqtt_client: Arc<MqttClient>,
    http_client: Client,
    server: String,
    workspace: String,
    node_id: Uuid,
    file_path: PathBuf,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    filename: String,
    shared_last_content: SharedLastContent,
    pull_only: bool,
    author: String,
    file_states: Option<&std::collections::HashMap<String, crate::sync::FileSyncState>>,
    relative_path: Option<&str>,
) -> Vec<JoinHandle<()>> {
    use crate::sync::crdt_state::SyncGuardrails;

    // Check for existing active tasks to prevent duplicates using file_states
    if let (Some(states), Some(path)) = (file_states, relative_path) {
        if let Some(existing_state) = states.get(path) {
            if !existing_state.task_handles.is_empty() {
                // Record in guardrails metrics
                SyncGuardrails::global().check_and_record_spawn(&node_id, path);
                warn!(
                    "Skipping CRDT task spawn for {} (node_id={}): {} tasks already running",
                    path,
                    node_id,
                    existing_state.task_handles.len()
                );
                return Vec::new();
            }
        }
    }

    // Also check global guardrails for cross-path duplicates (same node_id)
    if let Some(path) = relative_path {
        if SyncGuardrails::global().check_and_record_spawn(&node_id, path) {
            // Duplicate detected via guardrails (different code path may have registered it)
            return Vec::new();
        }
    }

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

    // CRITICAL: Subscribe to MQTT broadcast channel BEFORE spawning the receive task.
    // This fixes a race condition where messages broadcast before the task starts
    // would be lost if the receiver was created inside the spawned task.
    let message_rx = mqtt_client.subscribe_messages();

    // Spawn CRDT receive task for incoming changes
    handles.push(tokio::spawn(receive_task_crdt(
        mqtt_client,
        http_client,
        server,
        workspace,
        node_id,
        file_path,
        crdt_state,
        filename,
        shared_last_content,
        author,
        message_rx,
    )));

    handles
}

/// CRDT receive task that subscribes to MQTT edits and applies them locally.
///
/// This handles the "receive" side of CRDT peer sync:
/// - Subscribe to MQTT edits for this file's UUID
/// - When an edit arrives, parse and apply via process_received_edit
/// - If the merge produces content to write, update the local file
/// - On broadcast lag, resync from server to recover missed edits
#[allow(clippy::too_many_arguments)]
pub async fn receive_task_crdt(
    mqtt_client: Arc<MqttClient>,
    http_client: Client,
    server: String,
    workspace: String,
    node_id: Uuid,
    file_path: PathBuf,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    filename: String,
    shared_last_content: SharedLastContent,
    author: String,
    message_rx: broadcast::Receiver<IncomingMessage>,
) {
    let node_id_str = node_id.to_string();

    // The message_rx is passed in from spawn_file_sync_tasks_crdt, which subscribes
    // BEFORE spawning this task to avoid a race condition where messages could be
    // lost if the receiver was created here (after the task starts).
    let mut message_rx = message_rx;

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

    info!(
        "[SANDBOX-TRACE] receive_task_crdt SUBSCRIBED file={} uuid={} topic={}",
        file_path.display(),
        node_id,
        topic
    );

    // Mark the receive task as ready and drain any pending edits that arrived
    // between CRDT init and now.
    let pending_edits = {
        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(&filename, node_id);
        file_state.mark_receive_task_ready()
    };

    // Process any pending edits that were queued before we were ready
    if !pending_edits.is_empty() {
        info!(
            "CRDT receive_task: processing {} pending edits for {} after becoming ready",
            pending_edits.len(),
            file_path.display()
        );

        for pending in pending_edits {
            if let Ok(edit_msg) = parse_edit_message(&pending.payload) {
                let mut state_guard = crdt_state.write().await;
                let file_state = state_guard.get_or_create_file(&filename, node_id);

                match process_received_edit(
                    Some(&mqtt_client),
                    &workspace,
                    &node_id_str,
                    file_state,
                    &edit_msg,
                    &author,
                    None, // commit_store not available in receive_task_crdt
                )
                .await
                {
                    Ok((result, maybe_content)) => {
                        debug!(
                            "CRDT receive_task: processed pending edit for {}: {:?}",
                            file_path.display(),
                            result
                        );

                        // Write content if merge produced new content
                        if let Some(content) = maybe_content {
                            // Update shared_last_content BEFORE writing
                            {
                                let mut shared = shared_last_content.write().await;
                                *shared = Some(content.clone());
                            }
                            if let Err(e) = tokio::fs::write(&file_path, &content).await {
                                error!(
                                    "CRDT receive_task: failed to write pending edit content to {}: {}",
                                    file_path.display(),
                                    e
                                );
                            } else {
                                info!(
                                    "CRDT receive_task: wrote {} bytes from pending edit to {}",
                                    content.len(),
                                    file_path.display()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "CRDT receive_task: failed to process pending edit for {}: {}",
                            file_path.display(),
                            e
                        );
                    }
                }
            }
        }

        // Save state after processing pending edits
        let state_guard = crdt_state.read().await;
        if let Err(e) = state_guard
            .save(file_path.parent().unwrap_or(&file_path))
            .await
        {
            warn!(
                "CRDT receive_task: failed to save state after pending edits: {}",
                e
            );
        }
    }

    // RACE CONDITION FIX: After becoming ready, check if server HEAD has advanced
    // since our initial fetch. This catches edits that arrived between init and
    // subscription. We compare current server HEAD with our head_cid.
    {
        let current_head_cid = {
            let state_guard = crdt_state.read().await;
            state_guard
                .get_file(&filename)
                .and_then(|f| f.head_cid.clone())
        };

        // Fetch current server HEAD
        match crate::sync::client::fetch_head(&http_client, &server, &node_id_str, false).await {
            Ok(Some(head)) => {
                if let Some(ref server_cid) = head.cid {
                    let needs_resync = match &current_head_cid {
                        Some(local_cid) => local_cid != server_cid,
                        None => true,
                    };

                    if needs_resync {
                        info!(
                            "CRDT receive_task: server HEAD advanced during init for {} (local: {:?}, server: {}), resyncing",
                            file_path.display(),
                            current_head_cid,
                            server_cid
                        );

                        // Mark state as needing re-init
                        {
                            let mut state_guard = crdt_state.write().await;
                            if let Some(file_state) = state_guard.get_file_mut(&filename) {
                                file_state.mark_needs_resync();
                            }
                        }

                        // Resync from server
                        if let Err(e) = initialize_crdt_state_from_server_with_pending(
                            &http_client,
                            &server,
                            node_id,
                            &crdt_state,
                            &filename,
                            &file_path,
                            Some(&mqtt_client),
                            Some(&workspace),
                            Some(&author),
                            Some(&shared_last_content),
                        )
                        .await
                        {
                            warn!(
                                "CRDT receive_task: failed to resync {} after init race: {}",
                                file_path.display(),
                                e
                            );
                        } else {
                            info!(
                                "CRDT receive_task: resynced {} from server after init race",
                                file_path.display()
                            );
                        }
                    }
                }
            }
            Ok(None) => {
                debug!(
                    "CRDT receive_task: no server HEAD for {} during init race check",
                    file_path.display()
                );
            }
            Err(e) => {
                warn!(
                    "CRDT receive_task: failed to check server HEAD for {} during init race check: {}",
                    file_path.display(),
                    e
                );
            }
        }
    }

    let context = format!("CRDT receive {}", file_path.display());

    info!(
        "[SANDBOX-TRACE] receive_task_crdt ENTERING_LOOP file={} uuid={}",
        file_path.display(),
        node_id
    );

    // Set up periodic divergence check timer
    let mut divergence_check_interval =
        tokio::time::interval(Duration::from_secs(DIVERGENCE_CHECK_INTERVAL_SECS));
    divergence_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first tick which fires immediately
    divergence_check_interval.tick().await;

    loop {
        // Use select! to either receive a message or trigger the periodic divergence check
        let msg = tokio::select! {
            // Periodic divergence check
            _ = divergence_check_interval.tick() => {
                debug!(
                    "CRDT receive_task: periodic divergence check for {}",
                    file_path.display()
                );
                let diverged = check_and_resolve_divergence(
                    &http_client,
                    &server,
                    node_id,
                    &file_path,
                    &crdt_state,
                    &filename,
                    Some(&mqtt_client),
                    Some(&workspace),
                    Some(&author),
                    Some(&shared_last_content),
                )
                .await;
                if diverged {
                    info!(
                        "CRDT receive_task: divergence detected and resolved for {}",
                        file_path.display()
                    );
                }
                continue; // Go back to waiting for messages
            }
            // Receive MQTT message
            recv_result = message_rx.recv() => {
                match recv_result {
                    Ok(m) => m,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Handle lag - same logic as recv_broadcast_with_lag
                        tracing::warn!("{} lagged by {} messages", context, n);
                        // Resync from server to recover missed edits
                        info!(
                            "CRDT receive_task: {} lagged by {} messages, triggering resync from server",
                            file_path.display(),
                            n
                        );

                        // Mark state as needing re-init so the fetch actually happens
                        {
                            let mut state_guard = crdt_state.write().await;
                            if let Some(file_state) = state_guard.get_file_mut(&filename) {
                                file_state.mark_needs_resync();
                            }
                        }

                        // Re-initialize from server to get the latest state
                        if let Err(e) = initialize_crdt_state_from_server_with_pending(
                            &http_client,
                            &server,
                            node_id,
                            &crdt_state,
                            &filename,
                            &file_path,
                            Some(&mqtt_client),
                            Some(&workspace),
                            Some(&author),
                            Some(&shared_last_content),
                        )
                        .await
                        {
                            warn!(
                                "CRDT receive_task: failed to resync {} from server after lag: {}",
                                file_path.display(),
                                e
                            );
                        } else {
                            info!(
                                "CRDT receive_task: successfully resynced {} from server after lag",
                                file_path.display()
                            );
                        }

                        // Try to get the next message
                        match message_rx.recv().await {
                            Ok(m) => m,
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };

        // Check if this message is for our topic
        if msg.topic != topic {
            continue;
        }

        info!(
            "[SANDBOX-TRACE] receive_task_crdt RECV file={} uuid={} payload_len={}",
            file_path.display(),
            node_id,
            msg.payload.len()
        );
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

        // Check if edits should be queued (CRDT not initialized OR receive task not ready).
        // Queue the edit for later processing instead of trying to merge it now.
        if let Some(reason) = file_state.should_queue_edits() {
            file_state.queue_pending_edit(msg.payload.clone());
            info!(
                "[SANDBOX-TRACE] receive_task_crdt QUEUED file={} uuid={} queue_size={} reason={:?}",
                file_path.display(),
                node_id,
                file_state.pending_edits.len(),
                reason
            );
            debug!(
                "CRDT receive_task: queuing edit for {} (reason: {:?}, queue size: {})",
                file_path.display(),
                reason,
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
            None, // commit_store not available in receive_task_crdt main loop
        )
        .await
        {
            Ok((result, maybe_content)) => {
                info!(
                    "[SANDBOX-TRACE] receive_task_crdt CRDT_MERGE file={} uuid={} result={:?} has_content={}",
                    file_path.display(),
                    node_id,
                    result,
                    maybe_content.is_some()
                );
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
                    MergeResult::MissingHistory {
                        received_cid,
                        current_head,
                    } => {
                        // Intermediate commits are missing - trigger a resync from server
                        warn!(
                            "CRDT receive_task: missing history for {} (head: {}, received: {}). Triggering resync.",
                            file_path.display(),
                            current_head,
                            received_cid
                        );

                        // Drop state_guard to release the write lock before resyncing.
                        // The resync function will acquire its own lock.
                        drop(state_guard);

                        // Mark state as needing resync
                        {
                            let mut resync_guard = crdt_state.write().await;
                            if let Some(file_state) = resync_guard.get_file_mut(&filename) {
                                file_state.mark_needs_resync();
                            }
                        }

                        // Re-fetch state from server
                        if let Err(e) = initialize_crdt_state_from_server_with_pending(
                            &http_client,
                            &server,
                            node_id,
                            &crdt_state,
                            &filename,
                            &file_path,
                            Some(&mqtt_client),
                            Some(&workspace),
                            Some(&author),
                            Some(&shared_last_content),
                        )
                        .await
                        {
                            warn!(
                                "CRDT receive_task: failed to resync {} from server after missing history: {}",
                                file_path.display(),
                                e
                            );
                        } else {
                            info!(
                                "CRDT receive_task: successfully resynced {} from server after missing history",
                                file_path.display()
                            );
                        }

                        // Continue to next message - state has been reset
                        continue;
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
                    // - Merge conflicts often produce drastically shorter content (90%+ reduction)
                    // Note: 50% was too aggressive - legitimate large edits exist
                    let is_rollback = if content.is_empty() && !existing_content.is_empty() {
                        // Writing empty content when file has content - could be legitimate delete
                        // Log at debug level since this can happen legitimately
                        true
                    } else if !content.is_empty()
                        && !existing_content.is_empty()
                        && content.len() < existing_content.len() / 10
                    {
                        // Content reduced by more than 90% - likely a merge conflict, not an edit.
                        // This prevents sync loops where clients flip-flop between versions
                        // while still allowing legitimate edits that shorten content.
                        true
                    } else {
                        false
                    };

                    if is_rollback {
                        debug!(
                            "CRDT receive_task: refusing rollback write for {} (existing {} bytes, new {} bytes)",
                            file_path.display(),
                            existing_content.len(),
                            content.len()
                        );
                        // Don't write AND don't save state - skip to next message
                        // This prevents divergence between file and CRDT state
                        continue;
                    } else {
                        // Update shared last_content BEFORE writing to avoid echo publishes.
                        let previous_content = {
                            let mut shared = shared_last_content.write().await;
                            let prev = shared.clone();
                            info!(
                                "[RECEIVE-TRACE] receive_task_crdt updating shared_last_content for {} from {:?} to {:?}",
                                file_path.display(),
                                prev.as_ref().map(|s| s.chars().take(30).collect::<String>()),
                                content.chars().take(30).collect::<String>()
                            );
                            *shared = Some(content.clone());
                            prev
                        };

                        match tokio::fs::write(&file_path, &content).await {
                            Ok(()) => {
                                // Trace FIRST_WRITE milestone for sync readiness timeline
                                // Note: This traces every write for debugging; the "first" is
                                // the one that appears first in chronological order
                                trace_timeline(
                                    TimelineMilestone::FirstWrite,
                                    &file_path.display().to_string(),
                                    Some(&node_id_str),
                                );
                                info!(
                                    "[SANDBOX-TRACE] receive_task_crdt DISK_WRITE file={} uuid={} content_len={}",
                                    file_path.display(),
                                    node_id,
                                    content.len()
                                );
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
                                // Don't save CRDT state - prevents divergence between file and state.
                                // On restart, we'll re-receive the edit and try again.
                                continue;
                            }
                        }
                    }
                }

                // Save the updated state - only reached if:
                // 1. No content to write (AlreadyKnown, LocalAhead), or
                // 2. File write succeeded
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

/// Check for divergence between local and server state, and resolve if needed.
///
/// This is called periodically to detect when MQTT messages were missed and
/// the local state has diverged from the server. It handles three cases:
/// 1. Server ahead: Pull and merge server state
/// 2. Local ahead: Re-publish local commits
/// 3. True divergence: Perform CRDT merge
///
/// Returns true if divergence was detected and handled, false otherwise.
#[allow(clippy::too_many_arguments)]
async fn check_and_resolve_divergence(
    http_client: &Client,
    server: &str,
    node_id: Uuid,
    file_path: &Path,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: Option<&str>,
    author: Option<&str>,
    shared_last_content: Option<&SharedLastContent>,
) -> bool {
    let node_id_str = node_id.to_string();

    // Get our current state
    let (local_head_cid, server_head_cid_known) = {
        let state_guard = crdt_state.read().await;
        let file_state = state_guard.get_file(filename);
        match file_state {
            Some(f) => (f.head_cid.clone(), f.local_head_cid.clone()),
            None => return false, // File not tracked
        }
    };

    // Fetch server HEAD
    let server_head = match fetch_head(http_client, server, &node_id_str, false).await {
        Ok(Some(head)) => head,
        Ok(None) => {
            debug!(
                "Divergence check: no server HEAD for {}",
                file_path.display()
            );
            return false;
        }
        Err(e) => {
            warn!(
                "Divergence check: failed to fetch server HEAD for {}: {}",
                file_path.display(),
                e
            );
            return false;
        }
    };

    let server_cid = match &server_head.cid {
        Some(cid) => cid.clone(),
        None => return false, // No server commit yet
    };

    // Check if we're in sync
    let local_cid = match &local_head_cid {
        Some(cid) => cid,
        None => {
            // We have no local head, server has content - need to pull
            info!(
                "Divergence check: no local HEAD for {}, server has {}, pulling",
                file_path.display(),
                server_cid
            );
            // Mark as needing resync and pull
            {
                let mut state_guard = crdt_state.write().await;
                if let Some(file_state) = state_guard.get_file_mut(filename) {
                    file_state.mark_needs_resync();
                }
            }
            if let Err(e) = initialize_crdt_state_from_server_with_pending(
                http_client,
                server,
                node_id,
                crdt_state,
                filename,
                file_path,
                mqtt_client,
                workspace,
                author,
                shared_last_content,
            )
            .await
            {
                warn!(
                    "Divergence check: failed to pull server state for {}: {}",
                    file_path.display(),
                    e
                );
            }
            return true;
        }
    };

    if local_cid == &server_cid {
        // In sync - nothing to do
        return false;
    }

    // Divergence detected!
    info!(
        "Divergence check: {} has local HEAD {} but server HEAD {}",
        file_path.display(),
        local_cid,
        server_cid
    );

    // Check if we have local changes that haven't been synced
    let has_local_changes = server_head_cid_known.as_ref() != Some(&server_cid);

    if has_local_changes {
        // We have local changes AND server has different content
        // This is true divergence - need to merge
        info!(
            "Divergence check: true divergence for {} (local_head_cid={:?}), triggering resync with merge",
            file_path.display(),
            server_head_cid_known
        );
    } else {
        // Server is ahead and we don't have local changes - just pull
        info!(
            "Divergence check: server ahead for {}, pulling",
            file_path.display()
        );
    }

    // Mark as needing resync
    {
        let mut state_guard = crdt_state.write().await;
        if let Some(file_state) = state_guard.get_file_mut(filename) {
            file_state.mark_needs_resync();
        }
    }

    // Resync from server - the merge logic in initialize_crdt_state_from_server_with_pending
    // will handle merging local changes with server state
    if let Err(e) = initialize_crdt_state_from_server_with_pending(
        http_client,
        server,
        node_id,
        crdt_state,
        filename,
        file_path,
        mqtt_client,
        workspace,
        author,
        shared_last_content,
    )
    .await
    {
        warn!(
            "Divergence check: failed to resync {} from server: {}",
            file_path.display(),
            e
        );
    } else {
        info!(
            "Divergence check: successfully resynced {} from server",
            file_path.display()
        );
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_prepare_content_for_upload_text() {
        let content = b"Hello, world!";
        let path = Path::new("test.txt");

        let prepared = prepare_content_for_upload(content, path);

        assert!(!prepared.is_binary);
        assert!(!prepared.is_json);
        assert!(!prepared.is_jsonl);
        assert_eq!(prepared.content, "Hello, world!");
    }

    #[test]
    fn test_prepare_content_for_upload_json() {
        let content = br#"{"key": "value"}"#;
        let path = Path::new("data.json");

        let prepared = prepare_content_for_upload(content, path);

        assert!(!prepared.is_binary);
        assert!(prepared.is_json);
        assert!(!prepared.is_jsonl);
        assert_eq!(prepared.content, r#"{"key": "value"}"#);
    }

    #[test]
    fn test_prepare_content_for_upload_jsonl() {
        let content = b"{}\n{}\n";
        let path = Path::new("data.jsonl");

        let prepared = prepare_content_for_upload(content, path);

        assert!(!prepared.is_binary);
        assert!(!prepared.is_json);
        assert!(prepared.is_jsonl);
        assert_eq!(prepared.content, "{}\n{}\n");
    }

    #[test]
    fn test_prepare_content_for_upload_binary_by_extension() {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let content = b"PNG data here";
        let path = Path::new("image.png");

        let prepared = prepare_content_for_upload(content, path);

        assert!(prepared.is_binary);
        assert!(!prepared.is_json);
        assert!(!prepared.is_jsonl);
        // Binary content is base64 encoded
        assert_eq!(prepared.content, STANDARD.encode(content));
    }

    #[test]
    fn test_prepare_content_for_upload_binary_by_content() {
        use base64::{engine::general_purpose::STANDARD, Engine};

        // Content with null bytes is binary even if extension is .txt
        let content = b"text\x00with\x00nulls";
        let path = Path::new("weird.txt");

        let prepared = prepare_content_for_upload(content, path);

        assert!(prepared.is_binary);
        assert_eq!(prepared.content, STANDARD.encode(content));
    }

    #[test]
    fn test_prepare_content_for_upload_empty() {
        let content = b"";
        let path = Path::new("empty.txt");

        let prepared = prepare_content_for_upload(content, path);

        assert!(!prepared.is_binary);
        assert_eq!(prepared.content, "");
    }

    #[test]
    fn test_prepare_content_for_upload_markdown() {
        let content = b"# Heading\n\nParagraph text.";
        let path = Path::new("README.md");

        let prepared = prepare_content_for_upload(content, path);

        assert!(!prepared.is_binary);
        assert!(!prepared.is_json);
        assert!(!prepared.is_jsonl);
        assert_eq!(prepared.content, "# Heading\n\nParagraph text.");
    }

    // Tests for EchoDetectionResult
    #[test]
    fn test_echo_detection_result_echo() {
        let result = EchoDetectionResult::echo(true);
        assert!(result.echo_detected);
        assert!(result.should_refresh);
        assert!(result.updated_content.is_none());

        let result = EchoDetectionResult::echo(false);
        assert!(result.echo_detected);
        assert!(!result.should_refresh);
        assert!(result.updated_content.is_none());
    }

    #[test]
    fn test_echo_detection_result_no_echo() {
        let result = EchoDetectionResult::no_echo(true, None);
        assert!(!result.echo_detected);
        assert!(result.should_refresh);
        assert!(result.updated_content.is_none());

        let result = EchoDetectionResult::no_echo(false, Some("updated".to_string()));
        assert!(!result.echo_detected);
        assert!(!result.should_refresh);
        assert_eq!(result.updated_content, Some("updated".to_string()));
    }

    // Async tests for detect_echo_from_pending_write
    mod echo_detection {
        use super::*;
        use crate::sync::state::PendingWrite;
        use std::time::Instant;
        use tokio::sync::RwLock;

        fn create_test_state() -> Arc<RwLock<SyncState>> {
            Arc::new(RwLock::new(SyncState::new()))
        }

        #[tokio::test]
        async fn test_no_barrier_content_matches_trimmed() {
            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.last_written_content = "hello world\n".to_string();
            }

            let result = detect_echo_from_pending_write(
                &state,
                "hello world",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            assert!(result.echo_detected);
            assert!(!result.should_refresh);
            assert!(result.updated_content.is_none());
        }

        #[tokio::test]
        async fn test_no_barrier_content_differs() {
            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.last_written_content = "hello world".to_string();
            }

            let result = detect_echo_from_pending_write(
                &state,
                "different content",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            assert!(!result.echo_detected);
            assert!(!result.should_refresh);
            assert!(result.updated_content.is_none());
        }

        #[tokio::test]
        async fn test_no_barrier_needs_refresh_flag_cleared() {
            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.last_written_content = "different".to_string();
                s.needs_head_refresh = true;
            }

            let result = detect_echo_from_pending_write(
                &state,
                "content",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            // should_refresh should be true (was needs_head_refresh)
            assert!(!result.echo_detected);
            assert!(result.should_refresh);

            // Flag should be cleared in state
            let s = state.read().await;
            assert!(!s.needs_head_refresh);
        }

        #[tokio::test]
        async fn test_pending_write_matches_content() {
            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.pending_write = Some(PendingWrite {
                    write_id: 1,
                    content: "server content".to_string(),
                    cid: Some("cid123".to_string()),
                    started_at: Instant::now(),
                });
            }

            let result = detect_echo_from_pending_write(
                &state,
                "server content",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            assert!(result.echo_detected);
            assert!(!result.should_refresh);
            assert!(result.updated_content.is_none());

            // Verify state was updated
            let s = state.read().await;
            assert!(s.pending_write.is_none()); // Barrier cleared
            assert_eq!(s.last_written_cid, Some("cid123".to_string()));
            assert_eq!(s.last_written_content, "server content");
        }

        #[tokio::test]
        async fn test_pending_write_with_needs_refresh() {
            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.needs_head_refresh = true;
                s.pending_write = Some(PendingWrite {
                    write_id: 2,
                    content: "server content".to_string(),
                    cid: Some("cid456".to_string()),
                    started_at: Instant::now(),
                });
            }

            let result = detect_echo_from_pending_write(
                &state,
                "server content",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            assert!(result.echo_detected);
            assert!(result.should_refresh); // Should be true
            assert!(result.updated_content.is_none());

            // Flag should be cleared
            let s = state.read().await;
            assert!(!s.needs_head_refresh);
        }

        #[tokio::test]
        async fn test_pending_write_timed_out_matches() {
            use crate::sync::PENDING_WRITE_TIMEOUT;
            use std::time::Duration;

            let state = create_test_state();
            {
                let mut s = state.write().await;
                // Set started_at to beyond the timeout
                s.pending_write = Some(PendingWrite {
                    write_id: 3,
                    content: "timed out content".to_string(),
                    cid: Some("timeout_cid".to_string()),
                    started_at: Instant::now() - PENDING_WRITE_TIMEOUT - Duration::from_secs(1),
                });
            }

            let result = detect_echo_from_pending_write(
                &state,
                "timed out content",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            // Timed out but content matches - treat as delayed echo
            assert!(result.echo_detected);
            assert!(!result.should_refresh);

            // Verify state was updated
            let s = state.read().await;
            assert!(s.pending_write.is_none());
            assert_eq!(s.last_written_cid, Some("timeout_cid".to_string()));
            assert_eq!(s.last_written_content, "timed out content");
        }

        #[tokio::test]
        async fn test_pending_write_timed_out_differs() {
            use crate::sync::PENDING_WRITE_TIMEOUT;
            use std::time::Duration;

            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.pending_write = Some(PendingWrite {
                    write_id: 4,
                    content: "pending content".to_string(),
                    cid: Some("stale_cid".to_string()),
                    started_at: Instant::now() - PENDING_WRITE_TIMEOUT - Duration::from_secs(1),
                });
            }

            let result = detect_echo_from_pending_write(
                &state,
                "user edit content",
                false,
                Path::new("/tmp/test.txt"),
            )
            .await;

            // Timed out and content differs - treat as user edit
            assert!(!result.echo_detected);
            assert!(!result.should_refresh);
            assert!(result.updated_content.is_none());

            // State should NOT update last_written_* (old parent for CRDT merge)
            let s = state.read().await;
            assert!(s.pending_write.is_none());
        }

        #[tokio::test]
        async fn test_empty_content_trimmed_comparison() {
            let state = create_test_state();
            {
                let mut s = state.write().await;
                s.last_written_content = "   \n\t".to_string(); // Whitespace only
            }

            let result =
                detect_echo_from_pending_write(&state, "\n  ", false, Path::new("/tmp/test.txt"))
                    .await;

            // Both trim to empty, so should match
            assert!(result.echo_detected);
        }

        #[tokio::test]
        async fn test_binary_content_no_barrier() {
            use base64::{engine::general_purpose::STANDARD, Engine};

            let state = create_test_state();
            let binary_b64 = STANDARD.encode(b"\x00\x01\x02");
            {
                let mut s = state.write().await;
                s.last_written_content = binary_b64.clone();
            }

            let result = detect_echo_from_pending_write(
                &state,
                &binary_b64,
                true, // is_binary
                Path::new("/tmp/test.bin"),
            )
            .await;

            assert!(result.echo_detected);
        }
    }
}
