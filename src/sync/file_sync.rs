//! File mode synchronization.
//!
//! This module contains functions for syncing a single file with a server document.

// Note: recv_broadcast_with_lag was replaced with direct tokio::select! in receive_task_crdt
use crate::commit::Commit;
use crate::mqtt::{IncomingMessage, MqttClient, Topic};
#[cfg(unix)]
use crate::sync::atomic_write_with_shadow;
use crate::sync::crdt_merge::{get_doc_text_content, parse_edit_message, process_received_edit};
use crate::sync::crdt_publish::{publish_text_change, publish_yjs_update};
use crate::sync::crdt_state::{CrdtPeerState, DirectorySyncState};
#[cfg(unix)]
use crate::sync::flock::{try_flock_exclusive, FlockResult};
use crate::sync::{
    detect_from_path,
    error::{SyncError, SyncResult},
    file_watcher_task, is_binary_content, trace_timeline, FileEvent, SyncState, TimelineMilestone,
};
use reqwest::Client;
use rumqttc::QoS;
use serde_json::Value;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// Timeout for pending write barrier (30 seconds)
pub const PENDING_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Number of retries when content differs during a pending write (handles partial writes)
pub const BARRIER_RETRY_COUNT: u32 = 5;
/// Delay between retries when checking for stable content
pub const BARRIER_RETRY_DELAY: Duration = Duration::from_millis(50);
/// Timeout for attempting to acquire a flock before deferring inbound CRDT writes.
pub const CRDT_FLOCK_TIMEOUT: Duration = Duration::from_millis(500);

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

#[derive(Debug, PartialEq, Eq)]
enum DocContentMatch {
    MatchesOld,
    MatchesNew,
    Diverged,
    Unknown,
}

fn parse_jsonl_values(content: &str) -> Result<Vec<Value>, serde_json::Error> {
    let mut values = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        values.push(serde_json::from_str(line)?);
    }
    Ok(values)
}

fn match_doc_structured_content(
    file_state: &CrdtPeerState,
    old_content: &str,
    new_content: &str,
    is_jsonl: bool,
) -> SyncResult<DocContentMatch> {
    let doc = file_state.to_doc()?;

    let Some(doc_value) = crate::sync::crdt::yjs::doc_to_json_value(&doc) else {
        return Ok(DocContentMatch::Unknown);
    };

    if is_jsonl {
        // Guard: if Y.Doc doesn't have a YArray root, we can't compare
        // structurally. This happens when the file was initially created as
        // YText (legacy, before CP-74q1).
        if !doc_value.is_array() {
            return Ok(DocContentMatch::Unknown);
        }

        // Parse JSONL content as arrays of JSON values for comparison.
        // JSONL is NOT valid JSON — each line is a separate JSON value,
        // so we must use parse_jsonl_values instead of serde_json::from_str.
        let old_value = match parse_jsonl_values(old_content) {
            Ok(v) => Value::Array(v),
            Err(_) => return Ok(DocContentMatch::Unknown),
        };
        let new_value = match parse_jsonl_values(new_content) {
            Ok(v) => Value::Array(v),
            Err(_) => return Ok(DocContentMatch::Unknown),
        };

        if doc_value == new_value {
            return Ok(DocContentMatch::MatchesNew);
        }
        if doc_value == old_value {
            return Ok(DocContentMatch::MatchesOld);
        }

        // If the Y.Doc contains the file content as a prefix (the doc has strictly
        // more items), this is likely a receive-task echo: the doc has been updated
        // with additional MQTT edits that haven't been written to disk yet.
        // Treat as MatchesNew to skip the upload and avoid false divergence.
        if let (Some(doc_arr), Some(new_arr)) = (doc_value.as_array(), new_value.as_array()) {
            if doc_arr.len() > new_arr.len()
                && doc_arr.iter().zip(new_arr.iter()).all(|(d, n)| d == n)
            {
                return Ok(DocContentMatch::MatchesNew);
            }
        }

        return Ok(DocContentMatch::Diverged);
    }

    let old_value: Value = match serde_json::from_str(old_content) {
        Ok(value) => value,
        Err(_) => return Ok(DocContentMatch::Unknown),
    };
    let new_value: Value = match serde_json::from_str(new_content) {
        Ok(value) => value,
        Err(_) => return Ok(DocContentMatch::Unknown),
    };

    if doc_value == new_value {
        return Ok(DocContentMatch::MatchesNew);
    }
    if doc_value == old_value {
        return Ok(DocContentMatch::MatchesOld);
    }

    Ok(DocContentMatch::Diverged)
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
/// Interval for retrying deferred CRDT edits while waiting on flock (seconds).
pub const PENDING_EDIT_FLUSH_INTERVAL_SECS: u64 = 5;

static WARNED_NO_INODE_TRACKER: AtomicBool = AtomicBool::new(false);

/// Handle refresh logic after an upload attempt.
///
/// This helper encapsulates the common pattern of refreshing from HEAD after upload,
/// handling both success and failure cases consistently. If refresh is needed and
/// upload succeeded, it attempts to refresh from HEAD. If either upload or refresh
/// fails, it sets the needs_head_refresh flag so refresh is retried later.
///
/// # Arguments
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
/// * `inode_tracker` - Optional inode tracker for atomic writes
#[allow(clippy::too_many_arguments)]
pub async fn upload_task_crdt(
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    node_id: Uuid,
    file_path: PathBuf,
    crdt_state: Arc<RwLock<DirectorySyncState>>,
    filename: String,
    inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    mut rx: mpsc::Receiver<FileEvent>,
    author: String,
) {
    while let Some(event) = rx.recv().await {
        let FileEvent::Modified(raw_content) = event;

        debug!(
            "CRDT upload_task: received FileEvent for {} ({} bytes)",
            file_path.display(),
            raw_content.len()
        );

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

        // Get the old content for diff computation from Y.Doc state.
        // The Y.Doc is always updated before disk writes, so it contains the
        // last-known content for echo suppression.
        let old_content = {
            let state_guard = crdt_state.read().await;
            match state_guard.get_file(&filename) {
                Some(file_state) => {
                    crate::sync::crdt_publish::get_text_content(file_state).unwrap_or_default()
                }
                None => String::new(),
            }
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
            debug!(
                "CRDT upload_task: skipping unchanged content for {} ({} bytes)",
                file_path.display(),
                new_content.len()
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
        debug!(
            "[SANDBOX-TRACE] upload_task_crdt CHANGE_DETECTED file={} uuid={} old_len={} old_hash={:016x} new_len={} new_hash={:016x}",
            file_path.display(),
            node_id,
            old_content.len(),
            old_hash,
            new_content.len(),
            new_hash
        );
        // DEBUG: Log the diff being computed
        debug!(
            "CRDT upload_task: computing diff for {} - old_len={}, new_len={}, old_preview={:?}, new_preview={:?}",
            file_path.display(),
            old_content.len(),
            new_content.len(),
            old_content.chars().take(50).collect::<String>(),
            new_content.chars().take(50).collect::<String>()
        );

        let mut resync_attempted = false;

        loop {
            // Get or create the CRDT state for this file
            let mut state_guard = crdt_state.write().await;
            let file_state = state_guard.get_or_create_file(&filename, node_id);

            // Skip publishing if CRDT state hasn't been initialized from server yet.
            // Publishing from an empty Y.Doc creates Yjs insert operations with a new
            // client ID. When the server merges these with its existing state, the same
            // text gets inserted twice (once per client ID), causing content duplication
            // like "{}" becoming "{}{}".
            if file_state.needs_server_init() {
                info!(
                    "CRDT upload_task: SKIPPING publish for {} - CRDT state not yet initialized from server (yjs_state is None)",
                    file_path.display()
                );
                drop(state_guard);
                break;
            }

            // Legacy fallback: JSONL files created before CP-74q1 may have YText
            // Y.Docs instead of YArray. When detected, we fall back to text diffs
            // to stay compatible with the existing Y.Doc type on the server.
            let mut use_text_diff_for_jsonl = false;
            if prepared.is_json || prepared.is_jsonl {
                match match_doc_structured_content(
                    file_state,
                    &old_content,
                    &new_content,
                    prepared.is_jsonl,
                ) {
                    Ok(DocContentMatch::MatchesNew) => {
                        info!(
                            "CRDT upload_task: Y.Doc already matches new content for {} — skipping publish",
                            file_path.display()
                        );
                        drop(state_guard);
                        break;
                    }
                    Ok(DocContentMatch::Diverged) => {
                        warn!(
                            "CRDT upload_task: Y.Doc diverged from file for {}, resyncing via cyan",
                            file_path.display()
                        );
                        file_state.mark_needs_resync();
                        drop(state_guard);
                        if resync_attempted {
                            warn!(
                                "CRDT upload_task: resync already attempted for {}, skipping publish",
                                file_path.display()
                            );
                            break;
                        }
                        let resynced = resync_crdt_state_via_cyan_with_pending(
                            &mqtt_client,
                            &workspace,
                            node_id,
                            &crdt_state,
                            &filename,
                            &file_path,
                            &author,
                            inode_tracker.as_ref(),
                            false,
                            false,
                        )
                        .await
                        .is_ok();
                        if !resynced {
                            warn!(
                                "CRDT upload_task: cyan resync failed for {}, skipping publish",
                                file_path.display()
                            );
                            break;
                        }
                        resync_attempted = true;
                        continue;
                    }
                    Ok(DocContentMatch::Unknown) => {
                        if prepared.is_jsonl {
                            // Y.Doc has YText instead of YArray for JSONL.
                            // Use text diff to stay compatible with the existing
                            // Y.Doc type on the server.
                            debug!(
                                "CRDT upload_task: JSONL type mismatch for {}, using text diff",
                                file_path.display()
                            );
                            use_text_diff_for_jsonl = true;
                        } else {
                            debug!(
                                "CRDT upload_task: unable to compare structured content for {}, proceeding with publish",
                                file_path.display()
                            );
                        }
                    }
                    Ok(DocContentMatch::MatchesOld) => {}
                    Err(e) => {
                        warn!(
                            "CRDT upload_task: failed to compare structured content for {}: {}",
                            file_path.display(),
                            e
                        );
                    }
                }
            }

            // Publish the change via MQTT.
            // For JSON/JSONL files, create a structured update (YMap/YArray).
            // For text files (or JSONL with YText Y.Doc), use character-level
            // YText diffs.
            let publish_result = if !use_text_diff_for_jsonl
                && (prepared.is_json || prepared.is_jsonl)
            {
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
                        "CRDT upload_task: PUBLISHED commit {} for {} ({} bytes)",
                        result.cid,
                        file_path.display(),
                        result.update_bytes.len()
                    );

                    // Save the state to persist CID tracking
                    drop(state_guard);
                    let state_guard = crdt_state.read().await;
                    if let Err(e) = state_guard
                        .save(file_path.parent().unwrap_or(&file_path))
                        .await
                    {
                        warn!("Failed to save CRDT state: {}", e);
                    }
                    break;
                }
                Err(crate::sync::error::SyncError::ContentUnchanged) => {
                    // Y.Doc already matches new_content — no publish needed.
                    drop(state_guard);
                    break;
                }
                Err(crate::sync::error::SyncError::StaleCrdtState(reason)) => {
                    warn!(
                        "CRDT upload_task: stale CRDT state for {} ({}), resyncing via cyan",
                        file_path.display(),
                        reason
                    );
                    file_state.mark_needs_resync();
                    drop(state_guard);
                    if resync_attempted {
                        warn!(
                            "CRDT upload_task: resync already attempted for {}, skipping publish",
                            file_path.display()
                        );
                        break;
                    }
                    let resynced = resync_crdt_state_via_cyan_with_pending(
                        &mqtt_client,
                        &workspace,
                        node_id,
                        &crdt_state,
                        &filename,
                        &file_path,
                        &author,
                        inode_tracker.as_ref(),
                        false,
                        false,
                    )
                    .await
                    .is_ok();
                    if !resynced {
                        warn!(
                            "CRDT upload_task: cyan resync failed for {}, skipping publish",
                            file_path.display()
                        );
                        break;
                    }
                    resync_attempted = true;
                    continue;
                }
                Err(e) => {
                    drop(state_guard);
                    error!("CRDT upload failed for {}: {}", file_path.display(), e);
                    break;
                }
            }
        }
    }

    info!(
        "CRDT upload_task shutting down for: {}",
        file_path.display()
    );
}

/// After CRDT initialization, push local file content to MQTT if it differs
/// from the CRDT state. This handles:
/// - Offline edits made while sync was not running
/// - Initial file content that hasn't been pushed yet
/// - Local-first scenarios where the file has non-default content
///
/// Skips push if local content is empty or default content for its type.
pub async fn push_local_if_differs(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    node_id: Uuid,
    file_path: &Path,
    author: &str,
) -> SyncResult<()> {
    let local_content = match tokio::fs::read_to_string(file_path).await {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(SyncError::other(format!(
                "Failed to read {}: {}",
                file_path.display(),
                e
            )));
        }
    };

    // Skip if local content is empty or default
    let content_info = crate::sync::content_type::detect_from_path(file_path);
    if local_content.is_empty()
        || crate::sync::content_type::is_default_content(&local_content, &content_info)
    {
        return Ok(());
    }

    // Get CRDT content for comparison.
    // Use get_doc_text_content which handles YText, YArray, AND YMap formats.
    // Using get_text_content would fail for JSON files (stored as YMap) and always
    // return empty, causing us to think local differs and push stale content.
    let crdt_content = {
        let state = crdt_state.read().await;
        state.get_file(filename).and_then(|fs| {
            let doc = fs.to_doc().ok()?;
            let content = crate::sync::crdt::crdt_merge::get_doc_text_content(&doc);
            if content.is_empty() {
                None
            } else {
                Some(content)
            }
        })
    };

    // Normalize for comparison (trim trailing newlines)
    let local_trimmed = local_content.trim_end();
    let crdt_trimmed = crdt_content
        .as_deref()
        .map(|s: &str| s.trim_end())
        .unwrap_or("");
    if local_trimmed == crdt_trimmed {
        return Ok(());
    }

    info!(
        "Local content differs from CRDT state for {} — pushing via MQTT",
        file_path.display()
    );

    let node_id_str = node_id.to_string();
    let is_json = content_info.mime_type == "application/json";
    let is_jsonl = content_info.mime_type == "application/x-ndjson";

    let mut state = crdt_state.write().await;
    let file_state = state.get_or_create_file(filename, node_id);

    if is_json || is_jsonl {
        // Structured content: create Yjs update and publish
        let content_type = if is_jsonl {
            crate::content_type::ContentType::Jsonl
        } else {
            crate::content_type::ContentType::Json
        };
        let base_state = file_state.yjs_state.as_deref();
        match crate::sync::crdt::yjs::create_yjs_structured_update(
            content_type,
            &local_content,
            base_state,
        ) {
            Ok(update_b64) => match crate::sync::crdt::yjs::base64_decode(&update_b64) {
                Ok(update_bytes) => {
                    publish_yjs_update(
                        mqtt_client,
                        workspace,
                        &node_id_str,
                        file_state,
                        update_bytes,
                        author,
                    )
                    .await?;
                }
                Err(e) => {
                    warn!("Failed to decode structured update: {}", e);
                    // Fall back to text change
                    let old = crdt_content.as_deref().unwrap_or("");
                    publish_text_change(
                        mqtt_client,
                        workspace,
                        &node_id_str,
                        file_state,
                        old,
                        &local_content,
                        author,
                    )
                    .await?;
                }
            },
            Err(e) => {
                warn!(
                    "Structured update failed for {}, falling back to text: {}",
                    file_path.display(),
                    e
                );
                let old = crdt_content.as_deref().unwrap_or("");
                publish_text_change(
                    mqtt_client,
                    workspace,
                    &node_id_str,
                    file_state,
                    old,
                    &local_content,
                    author,
                )
                .await?;
            }
        }
    } else {
        // Text: compute diff and publish
        let old = crdt_content.as_deref().unwrap_or("");
        publish_text_change(
            mqtt_client,
            workspace,
            &node_id_str,
            file_state,
            old,
            &local_content,
            author,
        )
        .await?;
    }

    Ok(())
}

/// Resync CRDT state via the cyan/sync MQTT channel.
///
/// This fetches the full commit history (Ancestors(HEAD)), rebuilds the Y.Doc,
/// initializes state from that snapshot, and can optionally write content and
/// process queued edits. It is used when MissingHistory is detected so disk
/// writes only happen after gap fill.
pub async fn resync_crdt_state_via_cyan_with_pending(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    node_id: Uuid,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    file_path: &Path,
    author: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
    write_to_disk: bool,
    process_pending_edits: bool,
) -> SyncResult<()> {
    use crate::mqtt::messages::SyncMessage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use yrs::{updates::decoder::Decode, Doc, ReadTxn, Transact, Update};

    let node_id_str = node_id.to_string();
    let req_id = Uuid::new_v4().to_string();
    let sync_client_id = Uuid::new_v4().to_string();
    let sync_topic = Topic::sync(workspace, &node_id_str, &sync_client_id).to_topic_string();

    debug!(
        "[CYAN-SYNC] Resyncing file {} via {} (req={})",
        file_path.display(),
        sync_topic,
        req_id
    );

    let mut message_rx = mqtt_client.subscribe_messages();

    if let Err(e) = mqtt_client.subscribe(&sync_topic, QoS::AtLeastOnce).await {
        warn!(
            "[CYAN-SYNC] Failed to subscribe for file {}: {}",
            file_path.display(),
            e
        );
        return Err(SyncError::Mqtt(format!(
            "Failed to subscribe for file {}: {}",
            file_path.display(),
            e
        )));
    }

    let ancestors_msg = SyncMessage::Ancestors {
        req: req_id.clone(),
        commit: "HEAD".to_string(),
        depth: None,
    };
    let payload = match serde_json::to_vec(&ancestors_msg) {
        Ok(p) => p,
        Err(e) => {
            warn!(
                "[CYAN-SYNC] Failed to serialize Ancestors for {}: {}",
                file_path.display(),
                e
            );
            let _ = mqtt_client.unsubscribe(&sync_topic).await;
            return Err(SyncError::Other(format!(
                "Failed to serialize Ancestors for {}: {}",
                file_path.display(),
                e
            )));
        }
    };

    if let Err(e) = mqtt_client
        .publish(&sync_topic, &payload, QoS::AtLeastOnce)
        .await
    {
        warn!(
            "[CYAN-SYNC] Failed to publish Ancestors for {}: {}",
            file_path.display(),
            e
        );
        let _ = mqtt_client.unsubscribe(&sync_topic).await;
        return Err(SyncError::Mqtt(format!(
            "Failed to publish Ancestors for {}: {}",
            file_path.display(),
            e
        )));
    }

    let timeout = tokio::time::Duration::from_secs(2);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut commits: Vec<(String, String)> = Vec::new();
    let mut head_cid: Option<String> = None;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            warn!(
                "[CYAN-SYNC] Timeout waiting for sync response for {}",
                file_path.display()
            );
            let _ = mqtt_client.unsubscribe(&sync_topic).await;
            return Err(SyncError::Other(format!(
                "Timeout waiting for sync response for {}",
                file_path.display()
            )));
        }

        match tokio::time::timeout(remaining, message_rx.recv()).await {
            Ok(Ok(msg)) => {
                if msg.topic != sync_topic {
                    continue;
                }

                let sync_msg: SyncMessage = match serde_json::from_slice(&msg.payload) {
                    Ok(m) => m,
                    Err(e) => {
                        debug!(
                            "[CYAN-SYNC] Failed to parse sync message for {}: {}",
                            file_path.display(),
                            e
                        );
                        continue;
                    }
                };

                match sync_msg {
                    SyncMessage::Commit {
                        ref req,
                        ref id,
                        ref data,
                        ..
                    } if req == &req_id => {
                        head_cid = Some(id.clone());
                        commits.push((id.clone(), data.clone()));
                    }
                    SyncMessage::Done {
                        ref req,
                        commits: ref done_commits,
                        ..
                    } if req == &req_id => {
                        if let Some(last) = done_commits.last() {
                            head_cid = Some(last.clone());
                        }
                        break;
                    }
                    SyncMessage::Error {
                        ref req,
                        ref message,
                        ..
                    } if req == &req_id => {
                        warn!(
                            "[CYAN-SYNC] Server error for {}: {}",
                            file_path.display(),
                            message
                        );
                        let _ = mqtt_client.unsubscribe(&sync_topic).await;
                        return Err(SyncError::Other(format!(
                            "Server error for {}: {}",
                            file_path.display(),
                            message
                        )));
                    }
                    _ => continue,
                }
            }
            Ok(Err(e)) => {
                warn!(
                    "[CYAN-SYNC] Broadcast channel error for {}: {}",
                    file_path.display(),
                    e
                );
                let _ = mqtt_client.unsubscribe(&sync_topic).await;
                return Err(SyncError::Other(format!(
                    "Broadcast channel error for {}: {}",
                    file_path.display(),
                    e
                )));
            }
            Err(_) => {
                warn!(
                    "[CYAN-SYNC] Timeout waiting for sync response for {}",
                    file_path.display()
                );
                let _ = mqtt_client.unsubscribe(&sync_topic).await;
                return Err(SyncError::Other(format!(
                    "Timeout waiting for sync response for {}",
                    file_path.display()
                )));
            }
        }
    }

    if let Err(e) = mqtt_client.unsubscribe(&sync_topic).await {
        debug!("[CYAN-SYNC] Failed to unsubscribe {}: {}", sync_topic, e);
    }

    // If there's no history, initialize empty and process pending edits.
    if commits.is_empty() {
        {
            let mut state = crdt_state.write().await;
            let file_state = state.get_or_create_file(filename, node_id);
            file_state.initialize_empty();
        }

        if process_pending_edits {
            handle_pending_edits_with_flock(
                "after cyan empty init",
                Some(mqtt_client),
                workspace,
                node_id,
                filename,
                file_path,
                crdt_state,
                author,
                inode_tracker,
            )
            .await;
        }

        return Ok(());
    }

    let doc = Doc::new();
    for (cid, data_b64) in &commits {
        let data_bytes = match STANDARD.decode(data_b64) {
            Ok(b) => b,
            Err(e) => {
                warn!("[CYAN-SYNC] Failed to decode base64 for {}: {}", cid, e);
                continue;
            }
        };

        if data_bytes.is_empty() {
            continue;
        }

        let update = match Update::decode_v1(&data_bytes) {
            Ok(u) => u,
            Err(e) => {
                warn!("[CYAN-SYNC] Failed to decode Yrs update for {}: {}", cid, e);
                continue;
            }
        };

        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    let content = get_doc_text_content(&doc);

    let state_bytes = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };
    let state_b64 = STANDARD.encode(&state_bytes);
    let head_cid = head_cid.unwrap_or_else(|| "unknown".to_string());
    let commit_id = if head_cid == "unknown" {
        None
    } else {
        Some(head_cid.clone())
    };

    {
        let mut state = crdt_state.write().await;
        let file_state = state.get_or_create_file(filename, node_id);
        file_state.initialize_from_server(&state_b64, &head_cid);
        for (cid, _) in &commits {
            file_state.record_known_cid(cid);
        }
    }

    if write_to_disk {
        let local_content = tokio::fs::read_to_string(file_path)
            .await
            .unwrap_or_default();
        if local_content != content {
            if let Err(e) = write_content_with_tracker(
                file_path,
                &content,
                inode_tracker,
                commit_id,
                "CYAN-SYNC",
            )
            .await
            {
                warn!(
                    "[CYAN-SYNC] Failed to write synced content to {}: {}",
                    file_path.display(),
                    e
                );
            } else {
                info!(
                    "[CYAN-SYNC] Wrote synced content to {} ({} bytes)",
                    file_path.display(),
                    content.len()
                );
            }
        }
    }

    if process_pending_edits {
        handle_pending_edits_with_flock(
            "after cyan sync",
            Some(mqtt_client),
            workspace,
            node_id,
            filename,
            file_path,
            crdt_state,
            author,
            inode_tracker,
        )
        .await;
    }

    debug!(
        "[CYAN-SYNC] Resync complete for {} ({} commits, head={})",
        file_path.display(),
        commits.len(),
        head_cid
    );

    Ok(())
}

#[derive(Debug)]
enum WriteDecision {
    Written,
    SkippedRollback,
    Failed,
}

fn commit_id_for_write(
    result: &crate::sync::crdt_merge::MergeResult,
    received_cid: &str,
) -> Option<String> {
    match result {
        crate::sync::crdt_merge::MergeResult::FastForward { new_head } => Some(new_head.clone()),
        crate::sync::crdt_merge::MergeResult::Merged { merge_cid, .. } => Some(merge_cid.clone()),
        crate::sync::crdt_merge::MergeResult::LocalAhead => Some(received_cid.to_string()),
        _ => None,
    }
}

async fn write_content_with_tracker(
    file_path: &Path,
    content: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
    commit_id: Option<String>,
    context: &str,
) -> io::Result<()> {
    #[cfg(unix)]
    if let Some(tracker) = inode_tracker {
        let path_buf = file_path.to_path_buf();
        match atomic_write_with_shadow(&path_buf, content.as_bytes(), commit_id, tracker).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                error!(
                    "{}: failed to atomic write to {}: {}",
                    context,
                    file_path.display(),
                    e
                );
                return Err(e);
            }
        }
    }

    #[cfg(unix)]
    if inode_tracker.is_none()
        && WARNED_NO_INODE_TRACKER
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        warn!(
            "Inode tracking disabled; inbound CRDT writes will not use atomic shadow writes. Set --shadow-dir or COMMONPLACE_SHADOW_DIR to enable."
        );
    }

    match tokio::fs::write(file_path, content).await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!(
                "{}: failed to write to {}: {}",
                context,
                file_path.display(),
                e
            );
            Err(e)
        }
    }
}

async fn write_content_with_rollback_guard(
    file_path: &Path,
    content: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
    commit_id: Option<String>,
) -> WriteDecision {
    let existing_content = tokio::fs::read_to_string(file_path)
        .await
        .unwrap_or_default();

    debug!(
        "CRDT receive_task: merge produced content for {} - existing={} bytes ({:?}), new={} bytes ({:?})",
        file_path.display(),
        existing_content.len(),
        existing_content.chars().take(40).collect::<String>(),
        content.len(),
        content.chars().take(40).collect::<String>()
    );

    let is_rollback = if content.is_empty() && !existing_content.is_empty() {
        true
    } else if !content.is_empty()
        && !existing_content.is_empty()
        && content.len() < existing_content.len() / 10
    {
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
        return WriteDecision::SkippedRollback;
    }

    match write_content_with_tracker(
        file_path,
        content,
        inode_tracker,
        commit_id,
        "CRDT receive_task",
    )
    .await
    {
        Ok(()) => WriteDecision::Written,
        Err(_) => WriteDecision::Failed,
    }
}

async fn process_pending_edits_with_flock(
    pending_edits: Vec<crate::sync::PendingEdit>,
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: &str,
    node_id: Uuid,
    filename: &str,
    file_path: &Path,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    author: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
) -> Vec<crate::sync::PendingEdit> {
    let node_id_str = node_id.to_string();
    let mut remaining = pending_edits;
    let mut processed_any = false;
    let mut deferred: Option<Vec<crate::sync::PendingEdit>> = None;

    while !remaining.is_empty() {
        let pending = remaining.remove(0);
        let edit_msg = match parse_edit_message(&pending.payload) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to parse pending edit for {}, skipping: {}",
                    filename, e
                );
                continue;
            }
        };

        let received_cid = Commit::with_timestamp(
            edit_msg.parents.clone(),
            edit_msg.update.clone(),
            edit_msg.author.clone(),
            edit_msg.message.clone(),
            edit_msg.timestamp,
        )
        .calculate_cid();

        let already_known = {
            let state_guard = crdt_state.read().await;
            state_guard
                .get_file(filename)
                .map(|f| f.is_cid_known(&received_cid))
                .unwrap_or(false)
        };

        if already_known {
            continue;
        }

        #[cfg(unix)]
        let mut flock_guard = None;
        #[cfg(unix)]
        {
            match try_flock_exclusive(file_path, Some(CRDT_FLOCK_TIMEOUT)).await {
                Ok(FlockResult::Acquired(guard)) => {
                    flock_guard = Some(guard);
                }
                Ok(FlockResult::Timeout) => {
                    let mut queued = Vec::new();
                    queued.push(pending);
                    queued.extend(remaining);
                    deferred = Some(queued);
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!("Failed to acquire flock for {}: {}", file_path.display(), e);
                    let mut queued = Vec::new();
                    queued.push(pending);
                    queued.extend(remaining);
                    deferred = Some(queued);
                    break;
                }
            }
        }

        let result = {
            let mut state_guard = crdt_state.write().await;
            let file_state = state_guard.get_or_create_file(filename, node_id);
            process_received_edit(
                mqtt_client,
                workspace,
                &node_id_str,
                file_state,
                &edit_msg,
                author,
                None::<&Arc<crate::store::CommitStore>>,
            )
            .await
        };

        match result {
            Ok((merge_result, maybe_content)) => {
                processed_any = true;
                if let Some(content) = maybe_content {
                    let commit_id = commit_id_for_write(&merge_result, &received_cid);
                    match write_content_with_rollback_guard(
                        file_path,
                        &content,
                        inode_tracker,
                        commit_id,
                    )
                    .await
                    {
                        WriteDecision::Written => {
                            info!(
                                "Applied pending edit to {} ({} bytes, {:?})",
                                file_path.display(),
                                content.len(),
                                merge_result
                            );
                        }
                        WriteDecision::SkippedRollback => continue,
                        WriteDecision::Failed => continue,
                    }
                }
            }
            Err(e) => {
                warn!("Failed to process pending edit for {}: {}", filename, e);
            }
        }

        #[cfg(unix)]
        let _ = flock_guard;
    }

    if processed_any {
        let state_guard = crdt_state.read().await;
        if let Err(e) = state_guard
            .save(file_path.parent().unwrap_or(file_path))
            .await
        {
            warn!("Failed to save CRDT state after pending edits: {}", e);
        }
    }

    deferred.unwrap_or_default()
}

async fn handle_pending_edits_with_flock(
    context: &str,
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: &str,
    node_id: Uuid,
    filename: &str,
    file_path: &Path,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    author: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
) {
    let pending_edits = {
        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(filename, node_id);
        if file_state.should_queue_edits().is_some() {
            return;
        }
        file_state.take_pending_edits()
    };

    if pending_edits.is_empty() {
        return;
    }

    info!(
        "CRDT receive_task: processing {} pending edits for {} {}",
        pending_edits.len(),
        file_path.display(),
        context
    );

    let mut deferred = process_pending_edits_with_flock(
        pending_edits,
        mqtt_client,
        workspace,
        node_id,
        filename,
        file_path,
        crdt_state,
        author,
        inode_tracker,
    )
    .await;

    if !deferred.is_empty() {
        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(filename, node_id);
        if !file_state.pending_edits.is_empty() {
            deferred.extend(file_state.take_pending_edits());
        }
        let deferred_len = deferred.len();
        file_state.pending_edits = deferred;
        warn!(
            "CRDT receive_task: deferred {} pending edits for {} (flock locked)",
            deferred_len,
            file_path.display()
        );
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
    pull_only: bool,
    author: String,
    mqtt_only_config: crate::sync::MqttOnlySyncConfig,
    inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
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
            inode_tracker.clone(),
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
        author,
        mqtt_only_config,
        inode_tracker,
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
    author: String,
    mqtt_only_config: crate::sync::MqttOnlySyncConfig,
    inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    message_rx: broadcast::Receiver<IncomingMessage>,
) {
    let node_id_str = node_id.to_string();

    // The message_rx is passed in from spawn_file_sync_tasks_crdt, which subscribes
    // BEFORE spawning this task to avoid a race condition where messages could be
    // lost if the receiver was created here (after the task starts).
    let mut message_rx = message_rx;

    // Subscribe to edits for this file's UUID
    let topic = Topic::edits(&workspace, &node_id_str).to_topic_string();

    debug!(
        "CRDT receive_task: subscribing to edits for {} at topic: {}",
        file_path.display(),
        topic
    );

    if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
        error!("CRDT receive_task: failed to subscribe to {}: {}", topic, e);
        return;
    }

    debug!(
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

    if !pending_edits.is_empty() {
        let mut deferred = process_pending_edits_with_flock(
            pending_edits,
            Some(&mqtt_client),
            &workspace,
            node_id,
            &filename,
            &file_path,
            &crdt_state,
            &author,
            inode_tracker.as_ref(),
        )
        .await;

        if !deferred.is_empty() {
            let mut state_guard = crdt_state.write().await;
            let file_state = state_guard.get_or_create_file(&filename, node_id);
            if !file_state.pending_edits.is_empty() {
                deferred.extend(file_state.take_pending_edits());
            }
            let deferred_len = deferred.len();
            file_state.pending_edits = deferred;
            warn!(
                "CRDT receive_task: deferred {} pending edits for {} after becoming ready (flock locked)",
                deferred_len,
                file_path.display()
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

        // Fetch current server HEAD (skipped in MQTT-only mode).
        if mqtt_only_config.mqtt_only {
            debug!(
                "CRDT receive_task: skipping HTTP HEAD check for {} (MQTT-only mode)",
                file_path.display()
            );
        } else {
            match crate::sync::client::fetch_head(&http_client, &server, &node_id_str, false).await
            {
                Ok(Some(head)) => {
                    if let Some(ref server_cid) = head.cid {
                        // Check if we already know about this CID (e.g., we published it
                        // via push_local_if_differs before tasks were spawned). If so,
                        // no resync is needed even if head_cid doesn't match exactly.
                        let is_known = {
                            let state_guard = crdt_state.read().await;
                            state_guard
                                .get_file(&filename)
                                .map(|f| f.is_cid_known(server_cid))
                                .unwrap_or(false)
                        };
                        let needs_resync = if is_known {
                            false
                        } else {
                            match &current_head_cid {
                                Some(local_cid) => local_cid != server_cid,
                                None => true,
                            }
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

                            // Resync via cyan (MQTT)
                            if let Err(e) = resync_crdt_state_via_cyan_with_pending(
                                &mqtt_client,
                                &workspace,
                                node_id,
                                &crdt_state,
                                &filename,
                                &file_path,
                                &author,
                                inode_tracker.as_ref(),
                                true,
                                true,
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
    }

    let context = format!("CRDT receive {}", file_path.display());

    debug!(
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
    let mut pending_edit_flush_interval =
        tokio::time::interval(Duration::from_secs(PENDING_EDIT_FLUSH_INTERVAL_SECS));
    pending_edit_flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    pending_edit_flush_interval.tick().await;

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
                    node_id,
                    &file_path,
                    &crdt_state,
                    &filename,
                    &mqtt_client,
                    &workspace,
                    &author,
                    inode_tracker.as_ref(),
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
            _ = pending_edit_flush_interval.tick() => {
                handle_pending_edits_with_flock(
                    "during periodic flush",
                    Some(&mqtt_client),
                    &workspace,
                    node_id,
                    &filename,
                    &file_path,
                    &crdt_state,
                    &author,
                    inode_tracker.as_ref(),
                )
                .await;
                continue;
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

                        {
                            // Resync via cyan (MQTT) to get latest state after lag
                            if let Err(e) = resync_crdt_state_via_cyan_with_pending(
                                &mqtt_client,
                                &workspace,
                                node_id,
                                &crdt_state,
                                &filename,
                                &file_path,
                                &author,
                                inode_tracker.as_ref(),
                                true,
                                true,
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

        debug!(
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

        let received_cid = Commit::with_timestamp(
            edit_msg.parents.clone(),
            edit_msg.update.clone(),
            edit_msg.author.clone(),
            edit_msg.message.clone(),
            edit_msg.timestamp,
        )
        .calculate_cid();

        debug!(
            "CRDT receive_task: incoming edit for {} — CID={} parents={:?} author={} update_len={}",
            file_path.display(),
            received_cid,
            edit_msg.parents,
            edit_msg.author,
            edit_msg.update.len()
        );

        // Get or create the CRDT state for this file
        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(&filename, node_id);

        // Check if edits should be queued (CRDT not initialized OR receive task not ready).
        // Queue the edit for later processing instead of trying to merge it now.
        if let Some(reason) = file_state.should_queue_edits() {
            file_state.queue_pending_edit(msg.payload.clone());
            debug!(
                "CRDT receive_task: queued edit for {} (CID={} reason={:?} queue_size={})",
                file_path.display(),
                received_cid,
                reason,
                file_state.pending_edits.len()
            );
            drop(state_guard);
            continue; // Skip to next message
        }

        if file_state.is_cid_known(&received_cid) {
            debug!(
                "CRDT receive_task: skipping known CID {} for {}",
                received_cid,
                file_path.display()
            );
            drop(state_guard);
            continue;
        }

        drop(state_guard);

        #[cfg(unix)]
        let mut _flock_guard = None;
        #[cfg(unix)]
        {
            match try_flock_exclusive(&file_path, Some(CRDT_FLOCK_TIMEOUT)).await {
                Ok(FlockResult::Acquired(guard)) => {
                    _flock_guard = Some(guard);
                }
                Ok(FlockResult::Timeout) => {
                    let mut state_guard = crdt_state.write().await;
                    let file_state = state_guard.get_or_create_file(&filename, node_id);
                    file_state.queue_pending_edit(msg.payload.clone());
                    warn!(
                        "CRDT receive_task: deferring edit for {} due to flock timeout",
                        file_path.display()
                    );
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    let mut state_guard = crdt_state.write().await;
                    let file_state = state_guard.get_or_create_file(&filename, node_id);
                    file_state.queue_pending_edit(msg.payload.clone());
                    warn!(
                        "CRDT receive_task: failed to acquire flock for {} ({}), deferring edit",
                        file_path.display(),
                        e
                    );
                    continue;
                }
            }
        }

        let mut state_guard = crdt_state.write().await;
        let file_state = state_guard.get_or_create_file(&filename, node_id);

        if let Some(reason) = file_state.should_queue_edits() {
            file_state.queue_pending_edit(msg.payload.clone());
            debug!(
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
            continue;
        }

        // Capture pre-merge Y.Doc content for external edit guard.
        // The Y.Doc content before the merge represents what was last written to disk.
        let pre_merge_content = crate::sync::crdt_publish::get_text_content(file_state).ok();

        // Process the received edit
        match process_received_edit(
            Some(&mqtt_client),
            &workspace,
            &node_id_str,
            file_state,
            &edit_msg,
            &author,
            None::<&Arc<crate::store::CommitStore>>, // commit_store not available in receive_task_crdt main loop
        )
        .await
        {
            Ok((result, maybe_content)) => {
                use crate::sync::crdt_merge::MergeResult;
                let commit_id = commit_id_for_write(&result, &received_cid);

                match result {
                    MergeResult::AlreadyKnown => {
                        debug!(
                            "CRDT receive_task: already known for {}",
                            file_path.display()
                        );
                    }
                    MergeResult::FastForward { ref new_head } => {
                        debug!(
                            "CRDT receive_task: fast-forward to {} for {}",
                            new_head,
                            file_path.display()
                        );
                    }
                    MergeResult::Merged {
                        ref merge_cid,
                        ref remote_cid,
                    } => {
                        debug!(
                            "CRDT receive_task: merged {} + {} → {} for {}",
                            received_cid,
                            remote_cid,
                            merge_cid,
                            file_path.display()
                        );
                    }
                    MergeResult::LocalAhead => {
                        debug!(
                            "CRDT receive_task: local ahead for {} (received CID={})",
                            file_path.display(),
                            received_cid,
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
                        // Intermediate commits are missing - trigger a cyan/sync gap fill
                        debug!(
                            "CRDT receive_task: missing history for {} (head: {}, received: {}). Triggering resync.",
                            file_path.display(),
                            current_head,
                            received_cid
                        );

                        warn!(
                            "CRDT receive_task: missing history for {} — queueing edit and resyncing via cyan",
                            file_path.display()
                        );
                        file_state.queue_pending_edit(msg.payload.clone());
                        file_state.mark_needs_resync();

                        // Drop state_guard to release the write lock before resyncing.
                        drop(state_guard);

                        if resync_crdt_state_via_cyan_with_pending(
                            &mqtt_client,
                            &workspace,
                            node_id,
                            &crdt_state,
                            &filename,
                            &file_path,
                            &author,
                            inode_tracker.as_ref(),
                            true,
                            true,
                        )
                        .await
                        .is_err()
                        {
                            warn!(
                                "CRDT receive_task: cyan resync failed for {} after missing history",
                                file_path.display()
                            );
                        } else {
                            debug!(
                                "CRDT receive_task: cyan resync completed for {} after missing history",
                                file_path.display()
                            );
                        }

                        // Continue to next message - state has been reset
                        continue;
                    }
                }

                // Write content to file if merge produced new content
                if let Some(content) = maybe_content {
                    // Guard: if the file has been modified externally (user edit),
                    // skip the write to preserve the user's changes. The upload_task
                    // will detect the user's edit and publish it. Without this guard,
                    // incoming MQTT edits (e.g. from sandbox sync clients) can overwrite
                    // user edits before upload_task has a chance to detect them.
                    //
                    // Uses pre-merge Y.Doc content (captured before process_received_edit)
                    // as the reference for "last written content".
                    let skip_write = {
                        if let Some(ref pre_merge) = pre_merge_content {
                            match tokio::fs::read_to_string(&file_path).await {
                                Ok(current_file)
                                    if current_file.trim_end() != pre_merge.trim_end()
                                        && current_file.trim_end() != content.trim_end() =>
                                {
                                    info!(
                                        "CRDT receive_task: skipping write to {} — file has external edits ({} bytes on disk vs {} bytes pre-merge Y.Doc)",
                                        file_path.display(), current_file.len(), pre_merge.len()
                                    );
                                    true
                                }
                                _ => false,
                            }
                        } else {
                            false
                        }
                    };

                    if skip_write {
                        drop(state_guard);
                        continue;
                    }

                    match write_content_with_rollback_guard(
                        &file_path,
                        &content,
                        inode_tracker.as_ref(),
                        commit_id,
                    )
                    .await
                    {
                        WriteDecision::Written => {
                            // Trace FIRST_WRITE milestone for sync readiness timeline
                            // Note: This traces every write for debugging; the "first" is
                            // the one that appears first in chronological order
                            trace_timeline(
                                TimelineMilestone::FirstWrite,
                                &file_path.display().to_string(),
                                Some(&node_id_str),
                            );
                            debug!(
                                "CRDT receive_task: wrote {} bytes to {}",
                                content.len(),
                                file_path.display()
                            );
                        }
                        WriteDecision::SkippedRollback => continue,
                        WriteDecision::Failed => continue,
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
/// 1. Server ahead: Pull and merge server state (write to disk)
/// 2. Local ahead: Re-publish local commits that were orphaned on server
/// 3. True divergence: Perform bidirectional CRDT merge
///
/// After pulling server state, any local edits that differ from the server
/// are pushed back additively (for JSON: adds/updates keys without deleting
/// server-only keys, achieving a CRDT union of both sides).
///
/// Returns true if divergence was detected and handled, false otherwise.
#[allow(clippy::too_many_arguments)]
async fn check_and_resolve_divergence(
    node_id: Uuid,
    file_path: &Path,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    author: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
) -> bool {
    // Save local file content BEFORE resync — if we had local edits that were
    // orphaned on the server (head_advanced: false due to concurrent edits),
    // the resync will overwrite them with stale server state. We need to push
    // our edits back afterward.
    let pre_resync_content = tokio::fs::read_to_string(file_path).await.ok();

    // Mark state as needing resync
    {
        let mut state_guard = crdt_state.write().await;
        if let Some(file_state) = state_guard.get_file_mut(filename) {
            file_state.mark_needs_resync();
        }
    }

    // Resync via cyan (MQTT) - pull server state and write to disk
    match resync_crdt_state_via_cyan_with_pending(
        mqtt_client,
        workspace,
        node_id,
        crdt_state,
        filename,
        file_path,
        author,
        inode_tracker,
        true,
        true,
    )
    .await
    {
        Ok(()) => {
            info!(
                "Divergence check: successfully resynced {} via cyan",
                file_path.display()
            );

            // After resync, push any local edits that weren't in the server state.
            // This handles the case where our published commit was orphaned on the
            // server (head_advanced: false) due to concurrent edits from sandbox
            // sync clients. The additive push re-publishes with the correct parent
            // CID so the server accepts it.
            if let Some(ref pre_content) = pre_resync_content {
                if let Err(e) = push_local_edits_after_resync(
                    mqtt_client,
                    workspace,
                    crdt_state,
                    filename,
                    node_id,
                    file_path,
                    pre_content,
                    author,
                    inode_tracker,
                )
                .await
                {
                    warn!(
                        "Divergence check: failed to push local edits for {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }

            true
        }
        Err(e) => {
            warn!(
                "Divergence check: failed to resync {} via cyan: {}",
                file_path.display(),
                e
            );
            false
        }
    }
}

/// After a divergence resync, push any local edits that weren't in the server state.
///
/// When a commit is "orphaned" on the server (stored with `head_advanced: false` due to
/// concurrent edits from other sync clients), the divergence resync pulls stale server
/// state. This function detects if the pre-resync local content had edits not in the
/// server state, and pushes them back:
///
/// - **JSON**: Uses additive merge (`create_yjs_json_merge`) which adds/updates keys
///   without deleting server-only keys. This achieves a CRDT union of both sides.
/// - **JSONL/Text**: Uses structured or text diff update.
///
/// For the "server ahead" case (server has newer content), the additive push is
/// effectively a no-op since all local keys already exist on the server.
#[allow(clippy::too_many_arguments)]
async fn push_local_edits_after_resync(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    crdt_state: &Arc<RwLock<DirectorySyncState>>,
    filename: &str,
    node_id: Uuid,
    file_path: &Path,
    pre_resync_content: &str,
    author: &str,
    inode_tracker: Option<&Arc<RwLock<crate::sync::InodeTracker>>>,
) -> SyncResult<()> {
    // Get CRDT content (= server state after resync)
    // Must use get_doc_text_content which handles YText, YArray, AND YMap formats.
    // (crdt_publish::get_text_content only handles YText, returning empty for JSON files)
    let crdt_content = {
        let state = crdt_state.read().await;
        state.get_file(filename).and_then(|fs| {
            fs.to_doc()
                .ok()
                .map(|doc| get_doc_text_content(&doc))
                .filter(|s| !s.is_empty())
        })
    };

    // Compare (trimmed) — if they match, no local edits to push
    let pre_trimmed = pre_resync_content.trim_end();
    let crdt_trimmed = crdt_content.as_deref().map(|s| s.trim_end()).unwrap_or("");

    if pre_trimmed == crdt_trimmed || pre_trimmed.is_empty() {
        return Ok(());
    }

    // Skip if pre-resync content is default/empty
    let content_info = detect_from_path(file_path);
    if crate::sync::content_type::is_default_content(pre_resync_content, &content_info) {
        return Ok(());
    }

    info!(
        "Divergence check: local content differs from server for {} — pushing additively ({} local bytes vs {} server bytes)",
        file_path.display(),
        pre_resync_content.len(),
        crdt_content.as_ref().map(|s| s.len()).unwrap_or(0),
    );

    let is_json = content_info.mime_type == "application/json";
    let is_jsonl = content_info.mime_type == "application/x-ndjson";
    let node_id_str = node_id.to_string();

    // Publish the update (hold write lock for publish)
    {
        let mut state = crdt_state.write().await;
        let file_state = state.get_or_create_file(filename, node_id);

        if is_json {
            // Additive merge: adds/updates keys without deleting server-only keys
            let base_state = file_state.yjs_state.as_deref();
            match crate::sync::crdt::yjs::create_yjs_json_merge(pre_resync_content, base_state) {
                Ok(update_b64) => match crate::sync::crdt::yjs::base64_decode(&update_b64) {
                    Ok(update_bytes) => {
                        publish_yjs_update(
                            mqtt_client,
                            workspace,
                            &node_id_str,
                            file_state,
                            update_bytes,
                            author,
                        )
                        .await?;
                    }
                    Err(e) => {
                        warn!("Divergence push: failed to decode merge update: {}", e);
                        return Ok(());
                    }
                },
                Err(e) => {
                    warn!("Divergence push: failed to create merge update: {}", e);
                    return Ok(());
                }
            }
        } else if is_jsonl {
            let ct = crate::content_type::ContentType::Jsonl;
            let base_state = file_state.yjs_state.as_deref();
            match crate::sync::crdt::yjs::create_yjs_structured_update(
                ct,
                pre_resync_content,
                base_state,
            ) {
                Ok(update_b64) => match crate::sync::crdt::yjs::base64_decode(&update_b64) {
                    Ok(update_bytes) => {
                        publish_yjs_update(
                            mqtt_client,
                            workspace,
                            &node_id_str,
                            file_state,
                            update_bytes,
                            author,
                        )
                        .await?;
                    }
                    Err(e) => {
                        warn!("Divergence push: failed to decode JSONL update: {}", e);
                        return Ok(());
                    }
                },
                Err(e) => {
                    warn!("Divergence push: failed to create JSONL update: {}", e);
                    return Ok(());
                }
            }
        } else {
            // Text: use text diff
            let old = crdt_content.as_deref().unwrap_or("");
            publish_text_change(
                mqtt_client,
                workspace,
                &node_id_str,
                file_state,
                old,
                pre_resync_content,
                author,
            )
            .await?;
        }
    }

    // After publishing, write the merged CRDT content to disk
    let merged_content = {
        let state = crdt_state.read().await;
        state.get_file(filename).and_then(|fs| {
            fs.to_doc()
                .ok()
                .map(|doc| get_doc_text_content(&doc))
                .filter(|s| !s.is_empty())
        })
    };

    if let Some(ref merged) = merged_content {
        if let Err(e) =
            write_content_with_tracker(file_path, merged, inode_tracker, None, "DIVERGENCE-PUSH")
                .await
        {
            warn!(
                "Divergence push: failed to write merged content to {}: {}",
                file_path.display(),
                e
            );
        } else {
            info!(
                "Divergence push: wrote merged content to {} ({} bytes)",
                file_path.display(),
                merged.len()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::error::SyncError;
    use reqwest::Client;
    use std::path::Path;
    use tempfile::tempdir;
    use yrs::{ReadTxn, Transact};

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

    #[cfg(unix)]
    #[tokio::test]
    async fn test_pending_edits_defer_when_flocked() {
        use crate::mqtt::EditMessage;
        use crate::sync::PendingEdit;
        use base64::{engine::general_purpose::STANDARD, Engine};
        use std::fs::File;
        use std::os::unix::io::AsRawFd;
        use yrs::{Doc, StateVector, Text, Transact, WriteTxn};

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("note.txt");
        tokio::fs::write(&file_path, "").await.unwrap();

        let file = File::open(&file_path).unwrap();
        let lock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(lock_result, 0);

        let node_id = Uuid::new_v4();
        let crdt_state = Arc::new(RwLock::new(DirectorySyncState::new(node_id)));
        {
            let mut state = crdt_state.write().await;
            let file_state = state.get_or_create_file("note.txt", node_id);
            file_state.initialize_empty();
        }

        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }
        let update_bytes = doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        let edit_msg = EditMessage {
            update: STANDARD.encode(update_bytes),
            parents: vec![],
            author: "test".to_string(),
            message: None,
            timestamp: 1,
            req: None,
        };
        let payload = serde_json::to_vec(&edit_msg).unwrap();
        let pending_edits = vec![PendingEdit { payload }];

        let deferred = process_pending_edits_with_flock(
            pending_edits,
            None,
            "ws",
            node_id,
            "note.txt",
            &file_path,
            &crdt_state,
            "author",
            None,
        )
        .await;

        assert_eq!(deferred.len(), 1);
        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert!(content.is_empty());
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
