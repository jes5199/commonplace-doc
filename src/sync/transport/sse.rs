//! Server-Sent Events (SSE) handling for the sync client.
//!
//! This module handles SSE connections to the server, processing incoming
//! edit events and managing reconnection logic.

#[cfg(unix)]
use super::ancestry::all_are_ancestors;
use super::ancestry::{determine_sync_direction, SyncDirection};
use super::client::fetch_head;
use super::urls::build_sse_url;
use crate::sync::{
    detect_from_path, flock_state::process_pending_inbound_after_confirm, is_binary_content,
    is_default_content, looks_like_base64_binary, EditEventData, FlockSyncState, PendingWrite,
    SyncState,
};
#[cfg(unix)]
use crate::sync::{
    flock::{try_flock_exclusive, FlockResult},
    flock_state::PathState,
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

/// Context for different SSE task variants.
///
/// This enum allows the shared SSE loop to dispatch to different edit handlers
/// based on the task type (basic, flock-aware, or with inode tracking).
pub enum SseContext {
    /// Basic SSE task - no special handling
    Basic,
    /// Flock-aware SSE task with ancestry checking
    WithFlock {
        flock_state: FlockSyncState,
        doc_id: Option<Uuid>,
    },
    /// SSE task with inode tracking for shadow hardlinks (Unix only)
    #[cfg(unix)]
    WithTracker {
        inode_tracker: Arc<RwLock<crate::sync::InodeTracker>>,
    },
}

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

// ============================================================================
// Helper functions for handle_server_edit variants
// ============================================================================

/// Result of checking HEAD and determining sync direction
#[derive(Debug)]
pub enum HeadCheckResult {
    /// Should proceed with writing the content
    Proceed {
        head: crate::sync::HeadResponse,
        direction: SyncDirection,
    },
    /// Should skip this edit (server has default content, local has data, etc.)
    Skip { reason: &'static str },
    /// HEAD fetch failed
    Error { message: String },
}

/// Fetch HEAD from server and determine if we should proceed with the edit.
///
/// This implements the common logic for:
/// 1. Fetching HEAD from server
/// 2. CP-v5f7 guard: skip if server has default content but local has data
/// 3. Determining sync direction via ancestry
/// 4. Skip if local is ahead or in sync
///
/// Returns a `HeadCheckResult` indicating whether to proceed, skip, or error.
pub async fn check_head_and_sync_direction(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &Path,
    local_cid: Option<&str>,
    use_paths: bool,
    content_info: &crate::sync::ContentTypeInfo,
) -> HeadCheckResult {
    // Fetch HEAD from server
    let head = match fetch_head(client, server, identifier, use_paths).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            return HeadCheckResult::Error {
                message: "HEAD not found (404)".to_string(),
            };
        }
        Err(e) => {
            return HeadCheckResult::Error {
                message: format!("Failed to fetch HEAD: {:?}", e),
            };
        }
    };

    // CP-v5f7: Guard against restart data loss.
    // When local_cid is None (fresh start, no persisted state) but local file has
    // substantive content and server has only "default" content (empty or {}),
    // skip pulling to prevent wiping local data with empty server content.
    if local_cid.is_none() {
        let server_is_default = is_default_content(&head.content, content_info);
        if server_is_default {
            // Check if local file has content
            if let Ok(local_content) = std::fs::read(file_path) {
                let local_is_default = if content_info.is_binary {
                    local_content.is_empty()
                } else {
                    let local_str = String::from_utf8_lossy(&local_content);
                    is_default_content(local_str.trim(), content_info)
                };
                if !local_is_default {
                    return HeadCheckResult::Skip {
                        reason: "server has default content but local has data",
                    };
                }
            }
        }
    }

    // Determine sync direction via ancestry
    let server_cid = head.cid.as_deref();
    let direction =
        match determine_sync_direction(client, server, identifier, local_cid, server_cid).await {
            Ok(dir) => dir,
            Err(e) => {
                return HeadCheckResult::Error {
                    message: format!("Ancestry check failed: {}", e),
                };
            }
        };

    // Check if we should pull
    if !direction.should_pull() {
        return HeadCheckResult::Skip {
            reason: "ancestry check indicates local is ahead or diverged",
        };
    }

    // Skip if already in sync
    if matches!(direction, SyncDirection::InSync) {
        return HeadCheckResult::Skip {
            reason: "already in sync, skipping redundant write",
        };
    }

    HeadCheckResult::Proceed { head, direction }
}

/// Result of setting up the pending write barrier
#[derive(Debug)]
pub enum BarrierSetupResult {
    /// Barrier set up successfully, proceed with write
    Proceed { write_id: u64 },
    /// Should skip this edit (local changes pending, concurrent write, etc.)
    Skip { reason: &'static str },
    /// Error reading local file
    Error { message: String },
}

/// Set up the pending write barrier before writing.
///
/// This implements the common logic for:
/// 1. Check if there's already a pending write (concurrent SSE events)
/// 2. Read local file content
/// 3. Check for pending local changes
/// 4. Set up the pending write barrier
/// 5. Update last_written_* state
///
/// Returns a `BarrierSetupResult` indicating whether to proceed, skip, or error.
/// If `Proceed` is returned, the state lock has been released but the barrier is set.
///
/// The `allow_missing_file` parameter controls behavior when the file doesn't exist:
/// - `false`: Return error if file read fails (used by handle_server_edit)
/// - `true`: Treat as empty content (used by handle_server_edit_with_tracker)
pub async fn setup_pending_write_barrier(
    state: &Arc<RwLock<SyncState>>,
    file_path: &Path,
    head: &crate::sync::HeadResponse,
    content_info: &crate::sync::ContentTypeInfo,
    allow_missing_file: bool,
) -> BarrierSetupResult {
    let mut s = state.write().await;

    // Check if there's already a pending write (concurrent SSE events)
    if let Some(pending) = &s.pending_write {
        if pending.started_at.elapsed() < PENDING_WRITE_TIMEOUT {
            // Another write in progress
            s.needs_head_refresh = true;
            return BarrierSetupResult::Skip {
                reason: "another write in progress",
            };
        }
        // Timeout - clear stale pending and continue
        warn!("Clearing timed-out pending write (id={})", pending.write_id);
    }

    // Read local file content to check for pending local changes
    let raw_content = match std::fs::read(file_path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && allow_missing_file => {
            // File doesn't exist yet - that's OK, we'll create it
            Vec::new()
        }
        Err(e) => {
            return BarrierSetupResult::Error {
                message: format!("Failed to read local file: {}", e),
            };
        }
    };

    // Only check for pending changes if file has content
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
            s.needs_head_refresh = true;
            return BarrierSetupResult::Skip {
                reason: "local changes pending",
            };
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

    // CRITICAL: Update last_written_* BEFORE releasing lock and writing.
    s.last_written_content = head.content.clone();
    s.last_written_cid = head.cid.clone();

    BarrierSetupResult::Proceed { write_id }
}

/// Clear the pending write barrier on failure.
pub async fn clear_pending_write_on_failure(state: &Arc<RwLock<SyncState>>, write_id: u64) {
    let mut s = state.write().await;
    if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
        s.pending_write = None;
    }
}

/// Prepare content bytes for writing from HEAD response.
///
/// Handles binary detection and base64 decoding:
/// - If extension indicates binary, decode base64
/// - If content looks like base64-encoded binary, decode it
/// - Otherwise, use content as-is (text)
///
/// Returns `Ok(bytes)` on success, `Err(message)` if base64 decode fails for binary.
pub fn prepare_content_bytes(
    head_content: &str,
    content_info: &crate::sync::ContentTypeInfo,
) -> Result<Vec<u8>, String> {
    use base64::{engine::general_purpose::STANDARD, Engine};

    if content_info.is_binary {
        // Extension says binary - decode base64
        STANDARD
            .decode(head_content)
            .map_err(|e| format!("Failed to decode base64 content: {}", e))
    } else if looks_like_base64_binary(head_content) {
        // Extension says text, but content looks like base64-encoded binary
        match STANDARD.decode(head_content) {
            Ok(decoded) if is_binary_content(&decoded) => Ok(decoded),
            _ => Ok(head_content.as_bytes().to_vec()),
        }
    } else {
        // Text content
        Ok(head_content.as_bytes().to_vec())
    }
}

/// Prepare content bytes for writing, with fallback on decode failure.
///
/// Like `prepare_content_bytes`, but falls back to text encoding if base64 decode fails.
/// This is useful for queuing writes where we want to always succeed.
pub fn prepare_content_bytes_with_fallback(
    head_content: &str,
    content_info: &crate::sync::ContentTypeInfo,
) -> Vec<u8> {
    use base64::{engine::general_purpose::STANDARD, Engine};

    if content_info.is_binary {
        // Extension says binary - try to decode base64, fall back to text
        match STANDARD.decode(head_content) {
            Ok(decoded) => decoded,
            Err(_) => head_content.as_bytes().to_vec(),
        }
    } else if looks_like_base64_binary(head_content) {
        // Extension says text, but content looks like base64-encoded binary
        match STANDARD.decode(head_content) {
            Ok(decoded) if is_binary_content(&decoded) => decoded,
            _ => head_content.as_bytes().to_vec(),
        }
    } else {
        // Text content
        head_content.as_bytes().to_vec()
    }
}

/// Log the result of a server content write.
pub fn log_server_write(cid: &Option<String>, content_len: usize, write_id: u64, suffix: &str) {
    match cid {
        Some(cid) => info!(
            "Wrote server content{}: {} bytes at {} (write_id={})",
            suffix,
            content_len,
            &cid[..8.min(cid.len())],
            write_id
        ),
        None => info!(
            "Wrote server content{}: empty document (write_id={})",
            suffix, write_id
        ),
    }
}

// ============================================================================
// SSE connection and event loop
// ============================================================================

/// Shared SSE connection and event loop.
///
/// This function implements the core SSE connect/reconnect/event-dispatch logic
/// shared by all SSE task variants. The `context` parameter determines which
/// edit handler is called when an edit event is received.
#[allow(clippy::too_many_arguments)]
pub async fn run_sse_loop(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
    context: &SseContext,
) {
    let sse_url = build_sse_url(server, identifier, use_paths);
    let log_context = match context {
        SseContext::Basic => "",
        SseContext::WithFlock { .. } => " (flock-aware)",
        #[cfg(unix)]
        SseContext::WithTracker { .. } => " (with inode tracking)",
    };

    'reconnect: loop {
        info!("Connecting to SSE{}: {}", log_context, sse_url);

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
                                    // Dispatch to appropriate handler based on context
                                    match context {
                                        SseContext::Basic => {
                                            handle_server_edit(
                                                client, server, identifier, file_path, state,
                                                &edit, use_paths,
                                            )
                                            .await;
                                        }
                                        SseContext::WithFlock {
                                            flock_state,
                                            doc_id,
                                        } => {
                                            handle_server_edit_with_flock(
                                                client,
                                                server,
                                                identifier,
                                                file_path,
                                                state,
                                                &edit,
                                                use_paths,
                                                flock_state,
                                                doc_id.as_ref(),
                                            )
                                            .await;
                                        }
                                        #[cfg(unix)]
                                        SseContext::WithTracker { inode_tracker } => {
                                            handle_server_edit_with_tracker(
                                                client,
                                                server,
                                                identifier,
                                                file_path,
                                                state,
                                                &edit,
                                                use_paths,
                                                inode_tracker,
                                            )
                                            .await;
                                        }
                                    }
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
    run_sse_loop(
        &client,
        &server,
        &identifier,
        &file_path,
        &state,
        use_paths,
        &SseContext::Basic,
    )
    .await;
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
    let context = SseContext::WithFlock {
        flock_state,
        doc_id,
    };
    run_sse_loop(
        &client,
        &server,
        &identifier,
        &file_path,
        &state,
        use_paths,
        &context,
    )
    .await;
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
    let head = match fetch_head(client, server, identifier, use_paths).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            error!("HEAD not found (404)");
            return;
        }
        Err(e) => {
            error!("Failed to fetch HEAD: {:?}", e);
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
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // File was deleted - recreate it with the queued content
                                if let Err(e) = tokio::fs::write(file_path, &content).await {
                                    error!(
                                        ?file_path,
                                        ?e,
                                        "failed to recreate file for queued inbound content"
                                    );
                                } else {
                                    debug!(
                                        ?file_path,
                                        cid, "recreated file with queued inbound content"
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
                    // Use prepare_content_bytes_with_fallback for queuing (never fails)
                    let content_bytes =
                        prepare_content_bytes_with_fallback(&head.content, &content_info);

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
                    // Use prepare_content_bytes_with_fallback for queuing (never fails)
                    let content_bytes =
                        prepare_content_bytes_with_fallback(&head.content, &content_info);

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
            // Use prepare_content_bytes_with_fallback for queuing (never fails)
            let content_bytes = prepare_content_bytes_with_fallback(&head.content, &content_info);

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
    let head = match fetch_head(client, server, identifier, use_paths).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            error!("HEAD not found for refresh (404)");
            return false;
        }
        Err(e) => {
            error!("Failed to fetch HEAD for refresh: {:?}", e);
            return false;
        }
    };

    // Get our last known CID (snapshot from state) to check ancestry
    let local_cid = state.read().await.last_written_cid.clone();

    // Use CRDT ancestry to determine if we should apply server content.
    // Only pull if server is ahead of us (our CID is ancestor of server's CID).
    let server_cid = head.cid.as_deref();
    let direction = match determine_sync_direction(
        client,
        server,
        identifier,
        local_cid.as_deref(),
        server_cid,
    )
    .await
    {
        Ok(dir) => dir,
        Err(e) => {
            // Ancestry check failed - be conservative and skip
            warn!("Ancestry check failed in refresh: {}", e);
            return false;
        }
    };

    if !direction.should_pull() {
        debug!("Ancestry check for refresh: {:?}, skipping", direction);
        return false;
    }

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

    // Get our last known CID (snapshot from state) to check ancestry
    let local_cid = state.read().await.last_written_cid.clone();

    // Check HEAD and determine sync direction using shared helper
    let head = match check_head_and_sync_direction(
        client,
        server,
        identifier,
        file_path,
        local_cid.as_deref(),
        use_paths,
        &content_info,
    )
    .await
    {
        HeadCheckResult::Proceed { head, direction: _ } => head,
        HeadCheckResult::Skip { reason } => {
            debug!("Skipping server edit: {}", reason);
            state.write().await.needs_head_refresh = true;
            return;
        }
        HeadCheckResult::Error { message } => {
            error!("{}", message);
            state.write().await.needs_head_refresh = true;
            return;
        }
    };

    // Set up the pending write barrier using shared helper
    // Note: allow_missing_file=false because this variant requires the file to exist
    let write_id =
        match setup_pending_write_barrier(state, file_path, &head, &content_info, false).await {
            BarrierSetupResult::Proceed { write_id } => write_id,
            BarrierSetupResult::Skip { reason } => {
                debug!("Skipping server edit: {}", reason);
                return;
            }
            BarrierSetupResult::Error { message } => {
                error!("{}", message);
                return;
            }
        };

    // Prepare content bytes for writing using shared helper
    let content_bytes = match prepare_content_bytes(&head.content, &content_info) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("{}", e);
            clear_pending_write_on_failure(state, write_id).await;
            return;
        }
    };

    // Write directly to local file (not atomic)
    // We avoid temp+rename because it changes the inode, which breaks
    // inotify file watchers on Linux. Since the server is authoritative,
    // partial writes on crash are recoverable via re-sync.
    let write_result = tokio::fs::write(file_path, &content_bytes).await;

    if let Err(e) = write_result {
        error!("Failed to write file: {}", e);
        clear_pending_write_on_failure(state, write_id).await;
        return;
    }

    log_server_write(&head.cid, head.content.len(), write_id, "");
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
    let context = SseContext::WithTracker { inode_tracker };
    run_sse_loop(
        &client,
        &server,
        &identifier,
        &file_path,
        &state,
        use_paths,
        &context,
    )
    .await;
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

    // Get our last known CID (snapshot from state) to check ancestry
    let local_cid = state.read().await.last_written_cid.clone();

    // Check HEAD and determine sync direction using shared helper
    let head = match check_head_and_sync_direction(
        client,
        server,
        identifier,
        file_path,
        local_cid.as_deref(),
        use_paths,
        &content_info,
    )
    .await
    {
        HeadCheckResult::Proceed { head, direction: _ } => head,
        HeadCheckResult::Skip { reason } => {
            debug!("Skipping server edit (tracker): {}", reason);
            state.write().await.needs_head_refresh = true;
            return;
        }
        HeadCheckResult::Error { message } => {
            error!("{}", message);
            state.write().await.needs_head_refresh = true;
            return;
        }
    };

    // Set up the pending write barrier using shared helper
    // Note: allow_missing_file=true because this variant can create new files
    let write_id =
        match setup_pending_write_barrier(state, file_path, &head, &content_info, true).await {
            BarrierSetupResult::Proceed { write_id } => write_id,
            BarrierSetupResult::Skip { reason } => {
                debug!("Skipping server edit (tracker): {}", reason);
                return;
            }
            BarrierSetupResult::Error { message } => {
                error!("{}", message);
                return;
            }
        };

    // Prepare content bytes for writing using shared helper
    let content_bytes = match prepare_content_bytes(&head.content, &content_info) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("{}", e);
            clear_pending_write_on_failure(state, write_id).await;
            return;
        }
    };

    // Use atomic write with shadow hardlinking
    let write_result =
        atomic_write_with_shadow(file_path, &content_bytes, head.cid.clone(), inode_tracker).await;

    if let Err(e) = write_result {
        error!("Failed to write file: {}", e);
        clear_pending_write_on_failure(state, write_id).await;
        return;
    }

    log_server_write(&head.cid, head.content.len(), write_id, " (atomic)");
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
    author: &str,
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
    let replace_url =
        super::urls::build_replace_url(server, &identifier, &commit_id, use_paths, author);

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
    author: String,
) {
    info!("Shadow write handler started");

    while let Some(event) = rx.recv().await {
        debug!(
            "Processing shadow write for inode {:x}-{:x}",
            event.inode_key.dev, event.inode_key.ino
        );

        if let Err(e) =
            handle_shadow_write(&client, &server, &event, &inode_tracker, use_paths, &author).await
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
    use crate::sync::{state::hardlink_from_fd, InodeKey};
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
            "http://localhost:5199",
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
            "http://localhost:5199",
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
            "http://localhost:5199",
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

    // Tests for prepare_content_bytes helper
    mod prepare_content_bytes_tests {
        use super::*;
        use crate::sync::content_type::ContentTypeInfo;

        #[test]
        fn test_prepare_text_content() {
            let content_info = ContentTypeInfo::text("text/plain");
            let result = prepare_content_bytes("hello world", &content_info);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), b"hello world");
        }

        #[test]
        fn test_prepare_binary_content_valid_base64() {
            use base64::{engine::general_purpose::STANDARD, Engine};
            let binary_data = vec![0x89, 0x50, 0x4E, 0x47]; // PNG magic bytes
            let encoded = STANDARD.encode(&binary_data);

            let content_info = ContentTypeInfo::binary("image/png");
            let result = prepare_content_bytes(&encoded, &content_info);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), binary_data);
        }

        #[test]
        fn test_prepare_binary_content_invalid_base64() {
            let content_info = ContentTypeInfo::binary("image/png");
            // Invalid base64 should error
            let result = prepare_content_bytes("not valid base64!!!", &content_info);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("Failed to decode"));
        }

        #[test]
        fn test_prepare_text_with_base64_like_binary() {
            use base64::{engine::general_purpose::STANDARD, Engine};
            // Create binary content with at least 12 bytes (needed for looks_like_base64_binary)
            // PNG header + IHDR chunk start = binary content that will be detected
            let binary_data = vec![
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
                0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
            ];
            let encoded = STANDARD.encode(&binary_data);

            let content_info = ContentTypeInfo::text("text/plain"); // extension says text
                                                                    // Should decode and detect binary content
            let result = prepare_content_bytes(&encoded, &content_info);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), binary_data);
        }
    }

    // Tests for prepare_content_bytes_with_fallback helper
    mod prepare_content_bytes_with_fallback_tests {
        use super::*;
        use crate::sync::content_type::ContentTypeInfo;

        #[test]
        fn test_fallback_text_content() {
            let content_info = ContentTypeInfo::text("text/plain");
            let result = prepare_content_bytes_with_fallback("hello world", &content_info);
            assert_eq!(result, b"hello world");
        }

        #[test]
        fn test_fallback_binary_content_valid_base64() {
            use base64::{engine::general_purpose::STANDARD, Engine};
            let binary_data = vec![0x89, 0x50, 0x4E, 0x47];
            let encoded = STANDARD.encode(&binary_data);

            let content_info = ContentTypeInfo::binary("image/png");
            let result = prepare_content_bytes_with_fallback(&encoded, &content_info);
            assert_eq!(result, binary_data);
        }

        #[test]
        fn test_fallback_binary_content_invalid_base64_returns_text() {
            let content_info = ContentTypeInfo::binary("image/png");
            // Invalid base64 should NOT error - falls back to text
            let result = prepare_content_bytes_with_fallback("not valid base64!!!", &content_info);
            assert_eq!(result, b"not valid base64!!!");
        }
    }

    // Tests for HeadCheckResult and BarrierSetupResult enums
    #[test]
    fn test_head_check_result_variants() {
        use crate::sync::ancestry::SyncDirection;
        use crate::sync::HeadResponse;

        let proceed = HeadCheckResult::Proceed {
            head: HeadResponse {
                content: "test".to_string(),
                cid: Some("cid123".to_string()),
                state: None,
            },
            direction: SyncDirection::Pull,
        };
        let skip = HeadCheckResult::Skip {
            reason: "test reason",
        };
        let error = HeadCheckResult::Error {
            message: "test error".to_string(),
        };

        assert!(matches!(proceed, HeadCheckResult::Proceed { .. }));
        assert!(matches!(skip, HeadCheckResult::Skip { .. }));
        assert!(matches!(error, HeadCheckResult::Error { .. }));
    }

    #[test]
    fn test_barrier_setup_result_variants() {
        let proceed = BarrierSetupResult::Proceed { write_id: 42 };
        let skip = BarrierSetupResult::Skip {
            reason: "test reason",
        };
        let error = BarrierSetupResult::Error {
            message: "test error".to_string(),
        };

        assert!(matches!(
            proceed,
            BarrierSetupResult::Proceed { write_id: 42 }
        ));
        assert!(matches!(skip, BarrierSetupResult::Skip { .. }));
        assert!(matches!(error, BarrierSetupResult::Error { .. }));
    }
}
