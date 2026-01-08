//! Server-Sent Events (SSE) handling for the sync client.
//!
//! This module handles SSE connections to the server, processing incoming
//! edit events and managing reconnection logic.

#[cfg(unix)]
use crate::sync::{
    ancestry::all_are_ancestors,
    flock::{try_flock_exclusive, FlockResult},
    flock_state::PathState,
};
use crate::sync::{
    build_head_url, build_sse_url, detect_from_path, is_binary_content, looks_like_base64_binary,
    process_pending_inbound_after_confirm, EditEventData, FlockSyncState, HeadResponse,
    PendingWrite, SyncState,
};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest_eventsource::{Error as SseError, Event as SseEvent, EventSource};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Timeout for pending write barrier (30 seconds)
pub const PENDING_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Error type for inbound write operations
#[derive(Debug, Error)]
pub enum InboundWriteError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("HTTP error checking ancestry: {0}")]
    Http(#[from] reqwest::Error),
}

/// Result of an inbound write attempt
#[derive(Debug)]
pub enum InboundWriteResult {
    /// Write succeeded
    Written,
    /// Write was queued because pending outbound commits aren't in server state yet
    Queued,
    /// Write was skipped (e.g., file doesn't exist and we're checking ancestry)
    Skipped,
}

/// Write an inbound update with flock and ancestry checking.
///
/// This function coordinates inbound writes from the server with:
/// 1. **Ancestry checking**: If there are pending outbound commits, verifies they
///    are ancestors of the incoming commit before writing. If not, queues the
///    write for later to avoid overwriting local edits with stale server state.
/// 2. **Flock coordination**: Attempts to acquire an exclusive flock before
///    writing to coordinate with agents that hold files open during editing.
///    Proceeds anyway after timeout.
///
/// Returns:
/// - `Ok(InboundWriteResult::Written)` - Content was written to disk
/// - `Ok(InboundWriteResult::Queued)` - Content was queued (pending outbound not merged)
/// - `Ok(InboundWriteResult::Skipped)` - Write was skipped (file doesn't exist for ancestry check)
/// - `Err(_)` - IO or HTTP error occurred
#[cfg(unix)]
pub async fn write_inbound_with_checks(
    path: &Path,
    content: &Bytes,
    commit_id: &str,
    doc_id: Option<&Uuid>,
    path_state: &mut PathState,
    client: &Client,
    server_url: &str,
) -> Result<InboundWriteResult, InboundWriteError> {
    // If file doesn't exist, just create it
    if !path.exists() {
        tokio::fs::write(path, content).await?;
        tracing::debug!(?path, commit_id, "created new file from inbound update");
        return Ok(InboundWriteResult::Written);
    }

    // Check ancestry if we have pending outbound commits and a doc_id
    if path_state.has_pending_outbound() {
        if let Some(doc_id) = doc_id {
            let all_included = all_are_ancestors(
                client,
                server_url,
                doc_id,
                &path_state.pending_outbound,
                commit_id,
            )
            .await?;

            if !all_included {
                // Queue for later - our edits aren't in this update yet
                path_state.queue_inbound(content.clone(), commit_id.to_string());
                tracing::debug!(
                    ?path,
                    commit_id,
                    pending = ?path_state.pending_outbound,
                    "queuing inbound - pending outbound not yet merged"
                );
                return Ok(InboundWriteResult::Queued);
            }

            // All pending outbound are ancestors - clear them
            let confirmed: Vec<String> = path_state.pending_outbound.iter().cloned().collect();
            path_state.confirm_outbound(&confirmed);
            tracing::debug!(
                ?path,
                commit_id,
                "cleared pending_outbound - all are ancestors of incoming commit"
            );
        } else {
            // No doc_id available - can't check ancestry, queue to be safe
            path_state.queue_inbound(content.clone(), commit_id.to_string());
            tracing::debug!(
                ?path,
                commit_id,
                "queuing inbound - no doc_id for ancestry check"
            );
            return Ok(InboundWriteResult::Queued);
        }
    }

    // Try to acquire flock before writing
    match try_flock_exclusive(path, None).await {
        Ok(FlockResult::Acquired(_guard)) => {
            // Lock acquired - write content
            // Note: guard is held until end of this block
            tokio::fs::write(path, content).await?;
            tracing::debug!(?path, commit_id, "wrote inbound update with flock");
        }
        Ok(FlockResult::Timeout) => {
            // Timeout - write anyway (agent's problem if they lose data)
            tokio::fs::write(path, content).await?;
            tracing::warn!(?path, commit_id, "wrote inbound update after flock timeout");
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // File was deleted between our exists check and flock attempt
            // Just create it
            tokio::fs::write(path, content).await?;
            tracing::debug!(
                ?path,
                commit_id,
                "file deleted during flock, created new file"
            );
        }
        Err(e) => return Err(InboundWriteError::Io(e)),
    }

    Ok(InboundWriteResult::Written)
}

/// Write an inbound update using atomic write with shadow hardlink for inode tracking.
///
/// Like `write_inbound_with_checks` but uses `atomic_write_with_shadow` for the
/// actual write, enabling detection of slow writers to old inodes.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
pub async fn write_inbound_with_checks_atomic(
    path: &Path,
    content: &Bytes,
    commit_id: Option<String>,
    doc_id: Option<&Uuid>,
    path_state: &mut PathState,
    client: &Client,
    server_url: &str,
    inode_tracker: &Arc<tokio::sync::RwLock<crate::sync::InodeTracker>>,
) -> Result<InboundWriteResult, InboundWriteError> {
    // If file doesn't exist, use atomic write to create it
    if !path.exists() {
        let path_buf = path.to_path_buf();
        atomic_write_with_shadow(&path_buf, content, commit_id, inode_tracker).await?;
        tracing::debug!(?path, "created new file from inbound update (atomic)");
        return Ok(InboundWriteResult::Written);
    }

    // Check ancestry if we have pending outbound commits and a doc_id
    if path_state.has_pending_outbound() {
        if let Some(doc_id) = doc_id {
            let cid = commit_id.as_deref().unwrap_or("");
            let all_included = all_are_ancestors(
                client,
                server_url,
                doc_id,
                &path_state.pending_outbound,
                cid,
            )
            .await?;

            if !all_included {
                // Queue for later - our edits aren't in this update yet
                path_state.queue_inbound(content.clone(), cid.to_string());
                tracing::debug!(
                    ?path,
                    commit_id = cid,
                    pending = ?path_state.pending_outbound,
                    "queuing inbound - pending outbound not yet merged"
                );
                return Ok(InboundWriteResult::Queued);
            }

            // All pending outbound are ancestors - clear them
            let confirmed: Vec<String> = path_state.pending_outbound.iter().cloned().collect();
            path_state.confirm_outbound(&confirmed);
            tracing::debug!(
                ?path,
                commit_id = cid,
                "cleared pending_outbound - all are ancestors of incoming commit"
            );
        } else {
            // No doc_id available - can't check ancestry, queue to be safe
            let cid = commit_id.as_deref().unwrap_or("").to_string();
            path_state.queue_inbound(content.clone(), cid.clone());
            tracing::debug!(
                ?path,
                commit_id = cid,
                "queuing inbound - no doc_id for ancestry check"
            );
            return Ok(InboundWriteResult::Queued);
        }
    }

    // Try to acquire flock before writing
    let path_buf = path.to_path_buf();
    match try_flock_exclusive(path, None).await {
        Ok(FlockResult::Acquired(_guard)) => {
            // Lock acquired - do atomic write
            // Note: guard is held until end of this block
            atomic_write_with_shadow(&path_buf, content, commit_id.clone(), inode_tracker).await?;
            tracing::debug!(
                ?path,
                ?commit_id,
                "wrote inbound update with flock (atomic)"
            );
        }
        Ok(FlockResult::Timeout) => {
            // Timeout - write anyway (agent's problem if they lose data)
            atomic_write_with_shadow(&path_buf, content, commit_id.clone(), inode_tracker).await?;
            tracing::warn!(
                ?path,
                ?commit_id,
                "wrote inbound update after flock timeout (atomic)"
            );
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // File was deleted between our exists check and flock attempt
            // Just create it with atomic write
            atomic_write_with_shadow(&path_buf, content, commit_id.clone(), inode_tracker).await?;
            tracing::debug!(
                ?path,
                ?commit_id,
                "file deleted during flock, created new file (atomic)"
            );
        }
        Err(e) => return Err(InboundWriteError::Io(e)),
    }

    Ok(InboundWriteResult::Written)
}

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

    'reconnect: loop {
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
                    // Handle 404 gracefully - document may not exist yet
                    if let SseError::InvalidStatusCode(status, _) = &e {
                        if *status == StatusCode::NOT_FOUND {
                            debug!("SSE: Document not found (404), retrying in 1s...");
                            sleep(Duration::from_secs(1)).await;
                            continue 'reconnect; // Skip outer 5s sleep
                        }
                    }
                    error!("SSE error: {}", e);
                    break;
                }
            }
        }

        debug!("SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Task that subscribes to SSE and handles server changes with flock-aware tracking.
///
/// This is an enhanced version of `sse_task` that uses `FlockSyncState` for tracking
/// pending outbound commits. When an edit event arrives, it verifies that any pending
/// uploads are ancestors of the incoming commit before writing to the local file.
///
/// This prevents the race condition where:
/// 1. Local edit is uploaded
/// 2. SSE receives an older commit that doesn't include our upload
/// 3. Writing that older commit would overwrite our local changes
///
/// The doc_id parameter is needed for ancestry checking via the server API.
#[allow(clippy::too_many_arguments)]
pub async fn sse_task_with_flock(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
    flock_state: FlockSyncState,
    doc_id: Option<Uuid>,
) {
    let sse_url = build_sse_url(&server, &identifier, use_paths);

    'reconnect: loop {
        info!("Connecting to SSE (flock-aware): {}", sse_url);

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
                                    handle_server_edit_with_flock(
                                        &client,
                                        &server,
                                        &identifier,
                                        &file_path,
                                        &state,
                                        &edit,
                                        use_paths,
                                        &flock_state,
                                        doc_id.as_ref(),
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
                    // Handle 404 gracefully - document may not exist yet
                    if let SseError::InvalidStatusCode(status, _) = &e {
                        if *status == StatusCode::NOT_FOUND {
                            debug!("SSE: Document not found (404), retrying in 1s...");
                            sleep(Duration::from_secs(1)).await;
                            continue 'reconnect;
                        }
                    }
                    error!("SSE error: {}", e);
                    break;
                }
            }
        }

        debug!("SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Handle a server edit event with flock-aware ancestry checking.
///
/// This is an enhanced version of `handle_server_edit` that checks pending
/// outbound commits before writing. If our pending uploads aren't yet
/// included in the server's response, the write is queued for later.
#[allow(clippy::too_many_arguments)]
pub async fn handle_server_edit_with_flock(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    _edit: &EditEventData,
    use_paths: bool,
    flock_state: &FlockSyncState,
    doc_id: Option<&Uuid>,
) {
    // Detect if this file is binary
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

    // Get commit_id from HEAD response
    let commit_id = match &head.cid {
        Some(cid) => cid.clone(),
        None => {
            // No commit yet - just write
            debug!("No commit_id in HEAD response, writing directly");
            handle_server_edit(
                client, server, identifier, file_path, state, _edit, use_paths,
            )
            .await;
            return;
        }
    };

    // Check if we have pending outbound commits
    if flock_state.has_pending_outbound(file_path).await {
        let pending = flock_state.get_pending_outbound(file_path).await;

        // Check ancestry if we have a doc_id
        if let Some(doc_id) = doc_id {
            match all_are_ancestors(client, server, doc_id, &pending, &commit_id).await {
                Ok(true) => {
                    // All pending are ancestors - confirm them
                    let confirmed: Vec<String> = pending.iter().cloned().collect();
                    flock_state.confirm_outbound(file_path, &confirmed).await;
                    debug!(
                        ?file_path,
                        commit_id,
                        "confirmed {} pending outbound commits",
                        confirmed.len()
                    );

                    // Check for any queued inbound write and process it if present
                    if let Some((content, cid)) =
                        process_pending_inbound_after_confirm(flock_state, file_path).await
                    {
                        debug!(?file_path, cid, "writing previously queued inbound content");
                        // Use flock protection for the write - ancestry already confirmed above
                        match try_flock_exclusive(file_path, None).await {
                            Ok(FlockResult::Acquired(_guard)) => {
                                if let Err(e) = tokio::fs::write(file_path, &content).await {
                                    error!(
                                        ?file_path,
                                        ?e,
                                        "failed to write queued inbound content"
                                    );
                                } else {
                                    debug!(?file_path, cid, "wrote queued inbound with flock");
                                }
                            }
                            Ok(FlockResult::Timeout) => {
                                // Timeout - write anyway (agent's problem if they lose data)
                                if let Err(e) = tokio::fs::write(file_path, &content).await {
                                    error!(
                                        ?file_path,
                                        ?e,
                                        "failed to write queued inbound content"
                                    );
                                } else {
                                    warn!(
                                        ?file_path,
                                        cid, "wrote queued inbound after flock timeout"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    ?file_path,
                                    ?e,
                                    "failed to acquire flock for queued inbound"
                                );
                            }
                        }
                    }
                }
                Ok(false) => {
                    // Not all ancestors - queue the write
                    use base64::{engine::general_purpose::STANDARD, Engine};
                    let content_bytes = if content_info.is_binary {
                        match STANDARD.decode(&head.content) {
                            Ok(decoded) => decoded,
                            Err(_) => head.content.as_bytes().to_vec(),
                        }
                    } else if looks_like_base64_binary(&head.content) {
                        match STANDARD.decode(&head.content) {
                            Ok(decoded) if is_binary_content(&decoded) => decoded,
                            _ => head.content.as_bytes().to_vec(),
                        }
                    } else {
                        head.content.as_bytes().to_vec()
                    };

                    flock_state
                        .queue_inbound(file_path, Bytes::from(content_bytes), commit_id.clone())
                        .await;
                    info!(
                        ?file_path,
                        commit_id,
                        pending_count = pending.len(),
                        "queued inbound write - pending outbound not yet merged"
                    );
                    return;
                }
                Err(e) => {
                    // Ancestry check failed - queue to be safe
                    warn!(
                        ?file_path,
                        commit_id, "ancestry check failed, queuing write: {}", e
                    );
                    use base64::{engine::general_purpose::STANDARD, Engine};
                    let content_bytes = if content_info.is_binary {
                        match STANDARD.decode(&head.content) {
                            Ok(decoded) => decoded,
                            Err(_) => head.content.as_bytes().to_vec(),
                        }
                    } else {
                        head.content.as_bytes().to_vec()
                    };

                    flock_state
                        .queue_inbound(file_path, Bytes::from(content_bytes), commit_id)
                        .await;
                    return;
                }
            }
        } else {
            // No doc_id - can't check ancestry, queue to be safe
            debug!(
                ?file_path,
                "no doc_id for ancestry check, queuing inbound write"
            );
            use base64::{engine::general_purpose::STANDARD, Engine};
            let content_bytes = if content_info.is_binary {
                match STANDARD.decode(&head.content) {
                    Ok(decoded) => decoded,
                    Err(_) => head.content.as_bytes().to_vec(),
                }
            } else {
                head.content.as_bytes().to_vec()
            };

            flock_state
                .queue_inbound(file_path, Bytes::from(content_bytes), commit_id)
                .await;
            return;
        }
    }

    // No pending outbound or all confirmed - proceed with normal write
    handle_server_edit(
        client, server, identifier, file_path, state, _edit, use_paths,
    )
    .await;
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
    } else if looks_like_base64_binary(&head.content) {
        // Content looks like base64-encoded binary
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                tokio::fs::write(file_path, &decoded).await
            }
            _ => tokio::fs::write(file_path, &head.content).await,
        }
    } else {
        // Extension says text, content doesn't look like base64 binary
        tokio::fs::write(file_path, &head.content).await
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
    } else if looks_like_base64_binary(&head.content) {
        // Extension says text, but content looks like base64-encoded binary
        // This handles files that were detected as binary on upload
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                // Successfully decoded and content is binary - write decoded bytes
                tokio::fs::write(file_path, &decoded).await
            }
            _ => {
                // Decode failed or not binary - write as text
                tokio::fs::write(file_path, &head.content).await
            }
        }
    } else {
        // Extension says text, content doesn't look like base64 binary
        tokio::fs::write(file_path, &head.content).await
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

/// SSE task with inode tracking for shadow hardlinks.
///
/// Like `sse_task`, but uses atomic writes with shadow hardlinks when
/// an InodeTracker is provided. This enables detection of slow writers
/// to old inodes after atomic file replacements.
#[cfg(unix)]
pub async fn sse_task_with_tracker(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
    inode_tracker: Arc<tokio::sync::RwLock<crate::sync::InodeTracker>>,
) {
    let sse_url = build_sse_url(&server, &identifier, use_paths);

    'reconnect: loop {
        info!("Connecting to SSE (with inode tracking): {}", sse_url);

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
                                    handle_server_edit_with_tracker(
                                        &client,
                                        &server,
                                        &identifier,
                                        &file_path,
                                        &state,
                                        &edit,
                                        use_paths,
                                        &inode_tracker,
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
                    // Handle 404 gracefully - document may not exist yet
                    if let SseError::InvalidStatusCode(status, _) = &e {
                        if *status == StatusCode::NOT_FOUND {
                            debug!("SSE: Document not found (404), retrying in 1s...");
                            sleep(Duration::from_secs(1)).await;
                            continue 'reconnect; // Skip outer 5s sleep
                        }
                    }
                    error!("SSE error: {}", e);
                    break;
                }
            }
        }

        debug!("SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Handle a server edit event with inode tracking.
///
/// Like `handle_server_edit`, but uses atomic writes with shadow hardlinks.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_server_edit_with_tracker(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    _edit: &EditEventData,
    use_paths: bool,
    inode_tracker: &Arc<tokio::sync::RwLock<crate::sync::InodeTracker>>,
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
                debug!(
                    "Skipping server edit - another write in progress (id={}), \
                     setting needs_head_refresh flag",
                    pending.write_id
                );
                s.needs_head_refresh = true;
                return;
            }
            warn!("Clearing timed-out pending write (id={})", pending.write_id);
        }

        // Read local file content to check for pending local changes
        let raw_content = match std::fs::read(file_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File doesn't exist yet - that's OK, we'll create it
                Vec::new()
            }
            Err(e) => {
                error!("Failed to read local file: {}", e);
                return;
            }
        };

        if !raw_content.is_empty() {
            let is_binary = content_info.is_binary || is_binary_content(&raw_content);
            let local_content = if is_binary {
                use base64::{engine::general_purpose::STANDARD, Engine};
                STANDARD.encode(&raw_content)
            } else {
                String::from_utf8_lossy(&raw_content).to_string()
            };

            // Check if local content differs from what we last wrote
            if local_content != s.last_written_content {
                debug!(
                    "Skipping server update - local changes pending, setting needs_head_refresh"
                );
                s.needs_head_refresh = true;
                return;
            }
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

    // Prepare content bytes for writing
    use base64::{engine::general_purpose::STANDARD, Engine};
    let content_bytes: Vec<u8> = if content_info.is_binary {
        match STANDARD.decode(&head.content) {
            Ok(decoded) => decoded,
            Err(e) => {
                error!("Failed to decode base64 content: {}", e);
                let mut s = state.write().await;
                if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
                    s.pending_write = None;
                }
                return;
            }
        }
    } else if looks_like_base64_binary(&head.content) {
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => decoded,
            _ => head.content.as_bytes().to_vec(),
        }
    } else {
        head.content.as_bytes().to_vec()
    };

    // Use atomic write with shadow hardlinking
    let write_result =
        atomic_write_with_shadow(file_path, &content_bytes, head.cid.clone(), inode_tracker).await;

    if let Err(e) = write_result {
        error!("Failed to write file: {}", e);
        let mut s = state.write().await;
        if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
            s.pending_write = None;
        }
        return;
    }

    match &head.cid {
        Some(cid) => info!(
            "Wrote server content (atomic): {} bytes at {} (write_id={})",
            head.content.len(),
            &cid[..8.min(cid.len())],
            write_id
        ),
        None => info!(
            "Wrote server content (atomic): empty document (write_id={})",
            write_id
        ),
    }
}

/// Handle a write event from a shadowed inode.
///
/// When a slow writer continues writing to an old inode (now shadowed),
/// this function creates a CRDT update against the old commit and pushes
/// it to the server for merging.
#[cfg(unix)]
pub async fn handle_shadow_write(
    client: &Client,
    server: &str,
    event: &crate::sync::ShadowWriteEvent,
    inode_tracker: &Arc<tokio::sync::RwLock<crate::sync::InodeTracker>>,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Look up inode state to get commit_id and primary_path
    let (commit_id, identifier, is_binary) = {
        let tracker = inode_tracker.read().await;
        let state = tracker.get(&event.inode_key).ok_or_else(|| {
            format!(
                "No inode state for {:x}-{:x}",
                event.inode_key.dev, event.inode_key.ino
            )
        })?;

        let content_info = detect_from_path(&state.primary_path);
        let is_binary = content_info.is_binary || is_binary_content(&event.content);

        (
            state.commit_id.clone(),
            state.primary_path.to_string_lossy().to_string(),
            is_binary,
        )
    };

    // Convert content to string (or base64 for binary)
    let content_str = if is_binary {
        use base64::{engine::general_purpose::STANDARD, Engine};
        STANDARD.encode(&event.content)
    } else {
        String::from_utf8_lossy(&event.content).to_string()
    };

    // Push update using replace endpoint with old commit as parent
    // This creates a CRDT update that merges with HEAD
    let replace_url = crate::sync::build_replace_url(server, &identifier, &commit_id, use_paths);

    info!(
        "Pushing shadow write for inode {:x}-{:x} (parent: {}, {} bytes)",
        event.inode_key.dev,
        event.inode_key.ino,
        &commit_id[..8.min(commit_id.len())],
        event.content.len()
    );

    let resp = client
        .post(&replace_url)
        .header("content-type", "text/plain")
        .body(content_str)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Shadow write push failed: {} - {}", status, body).into());
    }

    let result: crate::sync::ReplaceResponse = resp.json().await?;

    info!(
        "Merged shadow write: {} chars inserted, {} deleted (new cid: {})",
        result.summary.chars_inserted,
        result.summary.chars_deleted,
        &result.cid[..8.min(result.cid.len())]
    );

    // Update inode state with new commit_id
    {
        let mut tracker = inode_tracker.write().await;
        tracker.update_commit(&event.inode_key, result.cid);
    }

    Ok(())
}

/// Task that processes shadow write events and pushes them to the server.
#[cfg(unix)]
pub async fn shadow_write_handler_task(
    client: Client,
    server: String,
    mut rx: tokio::sync::mpsc::Receiver<crate::sync::ShadowWriteEvent>,
    inode_tracker: Arc<tokio::sync::RwLock<crate::sync::InodeTracker>>,
    use_paths: bool,
) {
    info!("Shadow write handler started");

    while let Some(event) = rx.recv().await {
        debug!(
            "Processing shadow write for inode {:x}-{:x}",
            event.inode_key.dev, event.inode_key.ino
        );

        if let Err(e) =
            handle_shadow_write(&client, &server, &event, &inode_tracker, use_paths).await
        {
            error!("Failed to handle shadow write: {}", e);
        }
    }

    info!("Shadow write handler stopped");
}

/// Write content to a file using atomic temp+rename, with shadow hardlinking
/// for inode tracking. This allows slow writers to old inodes to be detected.
///
/// Returns the new inode key if successful.
#[cfg(unix)]
pub async fn atomic_write_with_shadow(
    file_path: &PathBuf,
    content: &[u8],
    commit_id: Option<String>,
    inode_tracker: &Arc<tokio::sync::RwLock<crate::sync::InodeTracker>>,
) -> std::io::Result<crate::sync::InodeKey> {
    use crate::sync::{hardlink_from_fd, InodeKey};
    use std::fs::File;
    use std::io::Write;

    // Step 1: Open the current file and get its inode
    let old_inode = if file_path.exists() {
        let file = File::open(file_path)?;
        let old_key = InodeKey::from_file(&file)?;

        // Step 2: If this inode is tracked, create a shadow hardlink
        let shadow_path = {
            let tracker = inode_tracker.read().await;
            if tracker.get(&old_key).is_some() {
                Some(tracker.shadow_path(&old_key))
            } else {
                None
            }
        };

        if let Some(shadow_path) = shadow_path {
            // Ensure shadow directory exists
            if let Some(parent) = shadow_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            // Create hardlink via /proc/self/fd for TOCTOU safety
            if let Err(e) = hardlink_from_fd(&file, &shadow_path) {
                // Non-fatal: log and continue without shadow
                debug!(
                    "Failed to create shadow hardlink: {} (continuing without shadow)",
                    e
                );
            } else {
                // Update tracker to mark inode as shadowed
                let mut tracker = inode_tracker.write().await;
                if tracker.shadow(old_key).is_none() {
                    // Race: inode was removed between read lock and write lock (e.g., by GC)
                    // Delete the orphaned hardlink we just created
                    warn!(
                        "Inode {:x}-{:x} was removed from tracker after hardlink creation - removing orphaned shadow",
                        old_key.dev, old_key.ino
                    );
                    if let Err(e) = std::fs::remove_file(&shadow_path) {
                        warn!("Failed to remove orphaned shadow hardlink: {}", e);
                    }
                } else {
                    debug!(
                        "Created shadow hardlink for inode {:x}-{:x} at {}",
                        old_key.dev,
                        old_key.ino,
                        shadow_path.display()
                    );
                }
            }
        }

        Some(old_key)
    } else {
        None
    };

    // Step 3: Write to temp file
    let parent = file_path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "file has no parent directory",
        )
    })?;
    let temp_path = parent.join(format!(
        ".{}.tmp.{}",
        file_path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));

    // Write synchronously to ensure atomicity
    {
        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(content)?;
        temp_file.sync_all()?;
    }

    // Step 4: Atomic rename
    std::fs::rename(&temp_path, file_path)?;

    // Step 5: Get the new inode and track it
    let new_key = InodeKey::from_path(file_path)?;

    {
        let mut tracker = inode_tracker.write().await;
        // Track with commit_id or empty string for genesis (empty document)
        let cid = commit_id.unwrap_or_default();
        tracker.track(new_key, cid, file_path.clone());
    }

    debug!(
        "Atomic write complete: {:?} -> inode {:x}-{:x}",
        file_path, new_key.dev, new_key.ino
    );

    // Log if inode changed
    if let Some(old) = old_inode {
        if old != new_key {
            debug!(
                "Inode changed: {:x}-{:x} -> {:x}-{:x}",
                old.dev, old.ino, new_key.dev, new_key.ino
            );
        }
    }

    Ok(new_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::flock_state::PathState;
    use bytes::Bytes;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_write_inbound_creates_new_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("new_file.txt");
        let content = Bytes::from("hello world");
        let mut path_state = PathState::new(0);
        let client = Client::new();

        let result = write_inbound_with_checks(
            &file_path,
            &content,
            "commit123",
            None,
            &mut path_state,
            &client,
            "http://localhost:3000",
        )
        .await
        .unwrap();

        assert!(matches!(result, InboundWriteResult::Written));
        assert!(file_path.exists());
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "hello world");
    }

    #[tokio::test]
    async fn test_write_inbound_overwrites_existing_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"old content").unwrap();

        let content = Bytes::from("new content");
        let mut path_state = PathState::new(0);
        let client = Client::new();

        let result = write_inbound_with_checks(
            temp_file.path(),
            &content,
            "commit123",
            None,
            &mut path_state,
            &client,
            "http://localhost:3000",
        )
        .await
        .unwrap();

        assert!(matches!(result, InboundWriteResult::Written));
        assert_eq!(
            std::fs::read_to_string(temp_file.path()).unwrap(),
            "new content"
        );
    }

    #[tokio::test]
    async fn test_write_inbound_queues_when_pending_outbound_no_doc_id() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"content").unwrap();

        let content = Bytes::from("new content");
        let mut path_state = PathState::new(0);
        // Add pending outbound commit
        path_state.add_pending_outbound("pending_cid".to_string());
        let client = Client::new();

        // Without doc_id, should queue
        let result = write_inbound_with_checks(
            temp_file.path(),
            &content,
            "commit123",
            None,
            &mut path_state,
            &client,
            "http://localhost:3000",
        )
        .await
        .unwrap();

        assert!(matches!(result, InboundWriteResult::Queued));
        // Original content should be preserved
        assert_eq!(
            std::fs::read_to_string(temp_file.path()).unwrap(),
            "content"
        );
        // Should have queued inbound
        assert!(path_state.pending_inbound.is_some());
        assert_eq!(
            path_state.pending_inbound.as_ref().unwrap().commit_id,
            "commit123"
        );
    }

    #[tokio::test]
    async fn test_inbound_write_error_display() {
        let io_err =
            InboundWriteError::Io(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        assert!(io_err.to_string().contains("IO error"));
    }

    #[tokio::test]
    async fn test_inbound_write_result_variants() {
        // Just verify the enum variants exist and can be pattern matched
        let written = InboundWriteResult::Written;
        let queued = InboundWriteResult::Queued;
        let skipped = InboundWriteResult::Skipped;

        assert!(matches!(written, InboundWriteResult::Written));
        assert!(matches!(queued, InboundWriteResult::Queued));
        assert!(matches!(skipped, InboundWriteResult::Skipped));
    }
}
