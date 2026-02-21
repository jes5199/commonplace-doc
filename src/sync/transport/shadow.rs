//! Shadow write helpers for inode-tracked atomic writes.
//!
//! This module contains functions for:
//! - Atomic file writes with shadow hardlinking for inode tracking
//! - Inbound write coordination with flock and ancestry checking
//! - Shadow write event handling (pushing shadowed inode writes to server)
//!
//! These are Unix-only utilities used by the SSE edit handlers and watcher tasks.

#[cfg(unix)]
use super::ancestry::all_are_ancestors;
use super::urls;
use crate::sync::{detect_from_path, is_binary_content};
#[cfg(unix)]
use crate::sync::{
    flock::{try_flock_exclusive, FlockResult},
    flock_state::PathState,
};
#[cfg(unix)]
use bytes::Bytes;
use reqwest::Client;
use std::io;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, warn};
#[cfg(unix)]
use uuid::Uuid;

/// Error type for inbound write operations
#[derive(Debug, Error)]
pub enum InboundWriteError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Error checking ancestry: {0}")]
    Ancestry(#[from] super::ancestry::AncestryError),
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
    let replace_url = urls::build_replace_url(server, &identifier, &commit_id, use_paths, author);

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
        tracker.track(new_key, cid, file_path.clone(), None);
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
}
