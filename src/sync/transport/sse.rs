//! Server-Sent Events (SSE) handling for the sync client.
//!
//! This module handles SSE connections to the server, processing incoming
//! edit events and managing reconnection logic.

#[cfg(unix)]
use super::ancestry::all_are_ancestors;
use super::ancestry::{determine_sync_direction, SyncDirection};
use super::client::fetch_head;
use super::urls::build_sse_url;
#[cfg(unix)]
use crate::sync::flock::{try_flock_exclusive, FlockResult};
use crate::sync::{
    detect_from_path, flock_state::process_pending_inbound_after_confirm, is_binary_content,
    is_default_content, looks_like_base64_binary, EditEventData, FlockSyncState, PendingWrite,
    SyncState,
};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest_eventsource::{Error as SseError, Event as SseEvent, EventSource};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Re-export from file_sync for backward compatibility
pub use crate::sync::file_sync::PENDING_WRITE_TIMEOUT;

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

// Re-export shadow write types for backward compatibility
#[cfg(unix)]
pub use super::shadow::{
    atomic_write_with_shadow, handle_shadow_write, shadow_write_handler_task,
    write_inbound_with_checks, write_inbound_with_checks_atomic,
};
pub use super::shadow::{InboundWriteError, InboundWriteResult};

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

// Re-export refresh_from_head from client.rs for backward compatibility
pub use super::client::refresh_from_head;

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

#[cfg(test)]
mod tests {
    use super::*;

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
