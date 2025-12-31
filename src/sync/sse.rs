//! Server-Sent Events (SSE) handling for the sync client.
//!
//! This module handles SSE connections to the server, processing incoming
//! edit events and managing reconnection logic.

use crate::sync::{
    build_head_url, build_sse_url, detect_from_path, is_binary_content, EditEventData,
    HeadResponse, PendingWrite, SyncState,
};
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Timeout for pending write barrier (30 seconds)
pub const PENDING_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Task that subscribes to SSE and handles server changes for a single file.
///
/// This maintains a persistent connection to the server's SSE endpoint,
/// automatically reconnecting on disconnection. When edit events arrive,
/// it fetches the new content and updates the local file.
pub async fn sse_task(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
) {
    let sse_url = build_sse_url(&server, &identifier, use_paths);

    loop {
        info!("Connecting to SSE: {}", sse_url);

        let request_builder = client.get(&sse_url);

        let mut es = match EventSource::new(request_builder) {
            Ok(es) => es,
            Err(e) => {
                error!("Failed to create EventSource: {}", e);
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        while let Some(event) = es.next().await {
            match event {
                Ok(SseEvent::Open) => {
                    info!("SSE connection opened");
                }
                Ok(SseEvent::Message(msg)) => {
                    debug!("SSE event: {} - {}", msg.event, msg.data);

                    match msg.event.as_str() {
                        "connected" => {
                            info!("SSE connected to node");
                        }
                        "edit" => {
                            // Parse edit event
                            match serde_json::from_str::<EditEventData>(&msg.data) {
                                Ok(edit) => {
                                    handle_server_edit(
                                        &client,
                                        &server,
                                        &identifier,
                                        &file_path,
                                        &state,
                                        &edit,
                                        use_paths,
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    warn!("Failed to parse edit event: {}", e);
                                }
                            }
                        }
                        "closed" => {
                            warn!("SSE: Target node shut down");
                            break;
                        }
                        "warning" => {
                            warn!("SSE warning: {}", msg.data);
                        }
                        _ => {
                            debug!("Unknown SSE event type: {}", msg.event);
                        }
                    }
                }
                Err(e) => {
                    error!("SSE error: {}", e);
                    break;
                }
            }
        }

        warn!("SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Refresh local file from server HEAD if needed.
///
/// Called by upload_task after clearing barrier when needs_head_refresh was set.
/// Returns true on success, false on failure (caller should re-set needs_head_refresh).
pub async fn refresh_from_head(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> bool {
    debug!("Refreshing from HEAD due to skipped server edit");

    // Fetch HEAD
    let head_url = build_head_url(server, identifier, use_paths);
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to fetch HEAD for refresh: {}", e);
            return false;
        }
    };

    if !resp.status().is_success() {
        error!("HEAD fetch failed for refresh: {}", resp.status());
        return false;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to parse HEAD response for refresh: {}", e);
            return false;
        }
    };

    // Read current local file to check for pending changes
    use base64::{engine::general_purpose::STANDARD, Engine};
    let content_info = detect_from_path(file_path);
    let local_content = match std::fs::read(file_path) {
        Ok(bytes) => {
            if content_info.is_binary || is_binary_content(&bytes) {
                Some(STANDARD.encode(&bytes))
            } else {
                Some(String::from_utf8_lossy(&bytes).to_string())
            }
        }
        Err(e) => {
            // File missing or unreadable - treat as no pending changes so we can
            // recreate it from server HEAD
            debug!(
                "Local file not readable for refresh check ({}), will recreate from HEAD",
                e
            );
            None
        }
    };

    // Check for pending local changes and if HEAD differs from our state
    {
        let mut s = state.write().await;

        // If local file exists and differs from what we last wrote, there are pending changes.
        // Don't overwrite them - let the next upload cycle handle merging.
        // If local file is missing (None), proceed with refresh to recreate it.
        if let Some(ref local) = local_content {
            if local != &s.last_written_content {
                debug!(
                    "Skipping refresh - local file has pending changes (local {} bytes != last_written {} bytes)",
                    local.len(),
                    s.last_written_content.len()
                );
                return false; // Caller will re-set needs_head_refresh
            }
        }

        // Only skip write if local file exists AND matches what we expect.
        // If local file is missing, we must write to recreate it.
        if local_content.is_some() && head.content == s.last_written_content {
            // Content matches, but still update CID in case server advanced to new commit
            // with identical content (e.g., concurrent edits that merge to same text)
            debug!("HEAD matches last_written_content, updating CID only");
            s.last_written_cid = head.cid.clone();
            return true; // Success - content already matches
        }

        // No pending local changes and HEAD differs - safe to write
        // Update state BEFORE writing file - this way if watcher fires after write,
        // echo detection will see matching content and skip upload
        s.last_written_cid = head.cid.clone();
        s.last_written_content = head.content.clone();
    }

    // Content differs - we need to write the new content
    // Use similar logic to handle_server_edit for binary detection
    // (content_info and STANDARD already available from above)
    let write_result = if content_info.is_binary {
        match STANDARD.decode(&head.content) {
            Ok(decoded) => tokio::fs::write(file_path, &decoded).await,
            Err(e) => {
                // Decode failed - fall back to writing as text (like initial sync does)
                warn!(
                    "Failed to decode base64 content for refresh, writing as text: {}",
                    e
                );
                tokio::fs::write(file_path, &head.content).await
            }
        }
    } else {
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                tokio::fs::write(file_path, &decoded).await
            }
            _ => tokio::fs::write(file_path, &head.content).await,
        }
    };

    if let Err(e) = write_result {
        error!("Failed to write file for refresh: {}", e);
        // Revert state on failure
        let mut s = state.write().await;
        s.last_written_cid = None;
        s.last_written_content = String::new();
        return false;
    }

    info!(
        "Refreshed local file from HEAD: {} bytes",
        head.content.len()
    );
    true
}

/// Handle a server edit event.
///
/// This is called when the SSE connection receives an "edit" event.
/// It fetches the new content from the server and writes it to the local file,
/// coordinating with the upload_task via the pending_write barrier.
pub async fn handle_server_edit(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    _edit: &EditEventData,
    use_paths: bool,
) {
    // Detect if this file is binary (use both extension and content-based detection)
    let content_info = detect_from_path(file_path);

    // Fetch new content from server first
    let head_url = build_head_url(server, identifier, use_paths);
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to fetch HEAD: {}", e);
            return;
        }
    };

    if !resp.status().is_success() {
        error!("Failed to fetch HEAD: {}", resp.status());
        return;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to parse HEAD response: {}", e);
            return;
        }
    };

    // Acquire write lock and set up barrier atomically
    let write_id = {
        let mut s = state.write().await;

        // Check if there's already a pending write (concurrent SSE events)
        if let Some(pending) = &s.pending_write {
            if pending.started_at.elapsed() < PENDING_WRITE_TIMEOUT {
                // Another write in progress, we can't process this SSE event now.
                // Set a flag so upload_task knows to refresh HEAD after clearing barrier.
                // This prevents data loss when multiple server edits arrive quickly.
                debug!(
                    "Skipping server edit - another write in progress (id={}), \
                     setting needs_head_refresh flag",
                    pending.write_id
                );
                s.needs_head_refresh = true;
                return;
            }
            // Timeout - clear stale pending and continue
            warn!("Clearing timed-out pending write (id={})", pending.write_id);
        }

        // Read local file content to check for pending local changes
        let raw_content = match std::fs::read(file_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read local file: {}", e);
                return;
            }
        };

        let is_binary = content_info.is_binary || is_binary_content(&raw_content);
        let local_content = if is_binary {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.encode(&raw_content)
        } else {
            String::from_utf8_lossy(&raw_content).to_string()
        };

        // Check if local content differs from what we last wrote
        if local_content != s.last_written_content {
            // Local changes pending - don't overwrite, let upload_task handle
            // Set needs_head_refresh so upload_task will fetch HEAD after uploading
            debug!("Skipping server update - local changes pending, setting needs_head_refresh");
            s.needs_head_refresh = true;
            return;
        }

        // Set barrier with new token BEFORE writing
        s.current_write_id += 1;
        let write_id = s.current_write_id;
        s.pending_write = Some(PendingWrite {
            write_id,
            content: head.content.clone(),
            cid: head.cid.clone(),
            started_at: std::time::Instant::now(),
        });

        write_id
    };
    // Lock released before I/O

    // Write directly to local file (not atomic)
    // We avoid temp+rename because it changes the inode, which breaks
    // inotify file watchers on Linux. Since the server is authoritative,
    // partial writes on crash are recoverable via re-sync.
    //
    // For binary detection, use both extension-based AND content-based detection:
    // - If extension suggests binary, decode base64
    // - If extension says text, still try decoding as base64 in case
    //   the file was detected as binary by content sniffing on upload
    use base64::{engine::general_purpose::STANDARD, Engine};
    let write_result = if content_info.is_binary {
        // Extension says binary - decode base64
        match STANDARD.decode(&head.content) {
            Ok(decoded) => tokio::fs::write(file_path, &decoded).await,
            Err(e) => {
                error!("Failed to decode base64 content: {}", e);
                // Clear barrier on failure
                let mut s = state.write().await;
                if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
                    s.pending_write = None;
                }
                return;
            }
        }
    } else {
        // Extension says text, but try decoding as base64 in case
        // this was a binary file detected by content sniffing on upload
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                // Successfully decoded and content is binary - write decoded bytes
                tokio::fs::write(file_path, &decoded).await
            }
            _ => {
                // Not base64 or not binary - write as text
                tokio::fs::write(file_path, &head.content).await
            }
        }
    };

    if let Err(e) = write_result {
        error!("Failed to write file: {}", e);
        // Clear barrier on failure
        let mut s = state.write().await;
        if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
            s.pending_write = None;
        }
        return;
    }

    // DO NOT clear barrier or update last_written_* here!
    // upload_task will do it when it sees the matching content from file watcher.
    // This ensures proper echo detection even with stale watcher events.

    match &head.cid {
        Some(cid) => info!(
            "Wrote server content: {} bytes at {} (write_id={})",
            head.content.len(),
            &cid[..8.min(cid.len())],
            write_id
        ),
        None => info!(
            "Wrote server content: empty document (write_id={})",
            write_id
        ),
    }
}
