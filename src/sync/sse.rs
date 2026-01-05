//! Server-Sent Events (SSE) handling for the sync client.
//!
//! This module handles SSE connections to the server, processing incoming
//! edit events and managing reconnection logic.

use crate::sync::{
    build_head_url, build_sse_url, detect_from_path, is_binary_content, looks_like_base64_binary,
    EditEventData, HeadResponse, PendingWrite, SyncState,
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

    loop {
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
                    error!("SSE error: {}", e);
                    break;
                }
            }
        }

        warn!("SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Handle a server edit event with inode tracking.
///
/// Like `handle_server_edit`, but uses atomic writes with shadow hardlinks.
#[cfg(unix)]
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
                tracker.shadow(old_key);
                debug!(
                    "Created shadow hardlink for inode {:x}-{:x} at {}",
                    old_key.dev,
                    old_key.ino,
                    shadow_path.display()
                );
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
        if let Some(cid) = commit_id {
            tracker.track(new_key, cid, file_path.clone());
        }
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
