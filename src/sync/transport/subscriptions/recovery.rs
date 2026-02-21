//! Recovery and resync functions for MQTT subscription tasks.
//!
//! Contains functions for resyncing files and schemas after broadcast lag,
//! and for bootstrapping schema CRDT state via the cyan sync channel.

use super::CrdtFileSyncContext;
use crate::sync::FileSyncState;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

fn subdir_relative_path(full_path: &str, subdir_path: &str) -> Option<String> {
    if subdir_path.is_empty() {
        return Some(full_path.to_string());
    }

    let prefix = format!("{}/", subdir_path.trim_end_matches('/'));
    if let Some(rest) = full_path.strip_prefix(&prefix) {
        if !rest.is_empty() {
            return Some(rest.to_string());
        }
    }

    if full_path == subdir_path {
        return None;
    }

    std::path::Path::new(full_path)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
}

/// Resync all subscribed file UUIDs after broadcast lag.
///
/// When the broadcast channel lags, we may have missed edit messages for files.
/// For CRDT-managed files, it re-initializes the CRDT state via cyan sync.
/// For non-CRDT files, recovery currently relies on retained MQTT messages.
#[allow(clippy::too_many_arguments)]
pub(super) async fn resync_subscribed_files(
    _http_client: &Client,
    _server: &str,
    subscribed_uuids: &HashSet<String>,
    uuid_to_paths: &crate::sync::UuidToPathsMap,
    directory: &Path,
    _use_paths: bool,
    _file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    crdt_context: Option<&CrdtFileSyncContext>,
    inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    author: &str,
) {
    debug!(
        "Resyncing {} subscribed file UUIDs after broadcast lag",
        subscribed_uuids.len()
    );

    for uuid in subscribed_uuids {
        // Get paths for this UUID
        let paths = match uuid_to_paths.get(uuid) {
            Some(p) => p,
            None => continue,
        };

        // For CRDT-managed files, re-initialize from server
        if let Some(ctx) = crdt_context {
            if let Ok(node_id) = Uuid::parse_str(uuid) {
                // Get the filename from the first path
                let filename = paths
                    .first()
                    .and_then(|p| std::path::Path::new(p).file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or(uuid)
                    .to_string();

                let file_path = paths
                    .first()
                    .map(|p| directory.join(p))
                    .unwrap_or_else(|| directory.join(&filename));

                // Mark state as needing re-init so resync_crdt_state_via_cyan_with_pending
                // will actually fetch fresh state
                {
                    let mut state_guard = ctx.crdt_state.write().await;
                    if let Some(file_state) = state_guard.get_file_mut(&filename) {
                        file_state.mark_needs_resync();
                    }
                }

                if let Err(e) = crate::sync::file_sync::resync_crdt_state_via_cyan_with_pending(
                    &ctx.mqtt_client,
                    &ctx.workspace,
                    node_id,
                    &ctx.crdt_state,
                    &filename,
                    &file_path,
                    author,
                    inode_tracker.as_ref(),
                    true,
                    true,
                )
                .await
                {
                    warn!(
                        "Resync: Failed to re-initialize CRDT state for {}: {} — initializing empty",
                        uuid, e
                    );
                    let mut state_guard = ctx.crdt_state.write().await;
                    let fs = state_guard.get_or_create_file(&filename, node_id);
                    fs.initialize_empty();
                } else {
                    debug!("Resync: Re-initialized CRDT state for {}", uuid);
                }
                continue;
            }
        }

        // Non-CRDT files: content will arrive via MQTT retained messages
        debug!(
            "Resync: No CRDT context for UUID {} — relying on MQTT retained messages",
            uuid
        );
    }

    debug!("Resync completed for subscribed file UUIDs");
}

/// Resync subscribed file UUIDs for a subdirectory watcher after broadcast lag.
///
/// Uses the subdirectory CRDT state cache to reinitialize each subscribed file via cyan
/// and then writes recovered content to all linked local paths for that UUID.
#[allow(clippy::too_many_arguments)]
pub(super) async fn resync_subdir_subscribed_files(
    subscribed_uuids: &HashSet<String>,
    uuid_to_paths: &crate::sync::UuidToPathsMap,
    directory: &Path,
    subdir_path: &str,
    subdir_node_id: &str,
    crdt_context: &CrdtFileSyncContext,
    inode_tracker: Option<Arc<RwLock<crate::sync::InodeTracker>>>,
    author: &str,
) {
    let subdir_uuid = match Uuid::parse_str(subdir_node_id) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "Subdir file resync: invalid subdir node id {}: {}",
                subdir_node_id, e
            );
            return;
        }
    };

    let subdir_full_path = directory.join(subdir_path);
    let subdir_state = match crdt_context
        .subdir_cache
        .get_or_load(&subdir_full_path, subdir_uuid)
        .await
    {
        Ok(state) => state,
        Err(e) => {
            warn!(
                "Subdir file resync: failed to load subdir state for {}: {}",
                subdir_path, e
            );
            return;
        }
    };

    debug!(
        "Resyncing {} subscribed file UUIDs for subdir {}",
        subscribed_uuids.len(),
        subdir_path
    );

    for uuid in subscribed_uuids {
        let node_id = match Uuid::parse_str(uuid) {
            Ok(id) => id,
            Err(_) => {
                debug!("Subdir file resync: skipping non-UUID {}", uuid);
                continue;
            }
        };

        let paths = match uuid_to_paths.get(uuid) {
            Some(p) if !p.is_empty() => p,
            _ => continue,
        };

        let first_path = match paths.first() {
            Some(p) => p,
            None => continue,
        };

        let filename = match subdir_relative_path(first_path, subdir_path) {
            Some(name) => name,
            None => continue,
        };

        let primary_file_path = directory.join(first_path);

        {
            let mut state_guard = subdir_state.write().await;
            if let Some(file_state) = state_guard.get_file_mut(&filename) {
                file_state.mark_needs_resync();
            }
        }

        if let Err(e) = crate::sync::file_sync::resync_crdt_state_via_cyan_with_pending(
            &crdt_context.mqtt_client,
            &crdt_context.workspace,
            node_id,
            &subdir_state,
            &filename,
            &primary_file_path,
            author,
            inode_tracker.as_ref(),
            false,
            true,
        )
        .await
        {
            warn!(
                "Subdir file resync: failed to reinitialize {} ({}): {}",
                filename, uuid, e
            );
            let mut state_guard = subdir_state.write().await;
            let fs = state_guard.get_or_create_file(&filename, node_id);
            fs.initialize_empty();
            continue;
        }

        let content = {
            let state_guard = subdir_state.read().await;
            state_guard.get_file(&filename).and_then(|fs| {
                let doc = fs.to_doc().ok()?;
                Some(crate::sync::crdt_merge::get_doc_text_content(&doc))
            })
        };

        let Some(content) = content else {
            continue;
        };

        for rel_path in paths {
            let file_path = directory.join(rel_path);
            if let Some(parent) = file_path.parent() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    warn!(
                        "Subdir file resync: failed creating parent {}: {}",
                        parent.display(),
                        e
                    );
                    continue;
                }
            }

            if let Err(e) = tokio::fs::write(&file_path, &content).await {
                warn!(
                    "Subdir file resync: failed writing {}: {}",
                    file_path.display(),
                    e
                );
            }
        }
    }

    debug!("Subdir file resync completed for {}", subdir_path);
}

/// Resync schema CRDT state from server after broadcast lag.
///
/// When the broadcast channel lags, we may have missed schema CRDT updates.
/// Without resyncing, the local schema Y.Doc state can drift from the server's
/// authoritative state, causing issues with subsequent deltas (e.g., delete
/// operations that reference items we don't have).
///
/// This function:
/// 1. Marks the schema state as needing resync (clears yjs_state)
/// 2. Initializes the local CRDT state via cyan sync (MQTT-only)
///
/// HTTP fallback has been removed; cyan sync is the only resync path.
///
/// # Arguments
/// * `fs_root_id` - The fs-root document ID (schema document)
/// * `crdt_context` - CRDT context containing the schema state to resync
pub(super) async fn resync_schema_from_server(
    fs_root_id: &str,
    crdt_context: &CrdtFileSyncContext,
) {
    debug!(
        "Resyncing schema CRDT state via cyan sync for {}",
        fs_root_id
    );

    // Step 1: Mark schema state as needing resync
    {
        let mut state_guard = crdt_context.crdt_state.write().await;
        state_guard.schema.mark_needs_resync();
        debug!(
            "Marked schema {} as needing resync - cleared CRDT state",
            fs_root_id
        );
    }

    // Step 2: Initialize via cyan sync (CP-uals)
    let sync_client_id = Uuid::new_v4().to_string();
    let initialized = {
        let mut state_guard = crdt_context.crdt_state.write().await;
        sync_schema_via_cyan(
            &crdt_context.mqtt_client,
            &crdt_context.workspace,
            fs_root_id,
            &sync_client_id,
            &mut state_guard.schema,
        )
        .await
    };

    if initialized {
        debug!("Schema resync via cyan completed for {}", fs_root_id);
        return;
    }

    warn!(
        "Schema resync: cyan sync failed for {} — no HTTP fallback available",
        fs_root_id
    );
}

/// Resync subdirectory schema CRDT state from server after broadcast lag.
///
/// Similar to `resync_schema_from_server` but operates on the subdirectory's
/// cached CRDT state (via `subdir_cache`) instead of the root schema state.
///
/// # Arguments
/// * `subdir_node_id` - The subdirectory's document ID
/// * `subdir_path` - Relative path to subdirectory
/// * `crdt_context` - CRDT context containing the subdir state cache
/// * `directory` - Root local directory path
pub(super) async fn resync_subdir_schema_from_server(
    subdir_node_id: &str,
    subdir_path: &str,
    crdt_context: &CrdtFileSyncContext,
    directory: &Path,
) {
    debug!(
        "Resyncing subdirectory schema CRDT state via cyan sync for {} ({})",
        subdir_path, subdir_node_id
    );

    // Parse subdir_node_id as UUID for cache lookup
    let subdir_uuid = match Uuid::parse_str(subdir_node_id) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "Subdir schema resync: Failed to parse subdir_node_id {} as UUID: {}, skipping",
                subdir_node_id, e
            );
            return;
        }
    };

    let subdir_full_path = directory.join(subdir_path);

    // Step 1: Mark as needing resync
    {
        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                state.schema.mark_needs_resync();
                debug!("Marked subdir schema {} as needing resync", subdir_node_id);
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to load cached state for {}: {}",
                    subdir_path, e
                );
                return;
            }
        }
    }

    // Step 2: Initialize via cyan sync (CP-uals)
    let sync_client_id = Uuid::new_v4().to_string();
    let initialized = {
        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                sync_schema_via_cyan(
                    &crdt_context.mqtt_client,
                    &crdt_context.workspace,
                    subdir_node_id,
                    &sync_client_id,
                    &mut state.schema,
                )
                .await
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to reload state for {}: {}",
                    subdir_path, e
                );
                false
            }
        }
    };

    if initialized {
        debug!(
            "Subdir schema resync via cyan completed for {} ({})",
            subdir_path, subdir_node_id
        );
        return;
    }

    warn!(
        "Subdir schema resync: cyan sync failed for {} — no HTTP fallback available",
        subdir_path
    );
}

/// Initialize schema CRDT state via the cyan (sync) MQTT channel.
///
/// Sends an `Ancestors(HEAD)` request for the given document, receives
/// the commit history, builds a Y.Doc from commits, and initializes
/// the CRDT state. This replaces HTTP-based schema bootstrapping.
///
/// See design: docs/plans/2026-01-30-cyan-schema-sync-design.md
///
/// Returns `true` if initialization succeeded, `false` if timed out or failed.
pub(super) async fn sync_schema_via_cyan(
    mqtt_client: &Arc<crate::mqtt::MqttClient>,
    workspace: &str,
    doc_path: &str,
    client_id: &str,
    schema_state: &mut crate::sync::crdt_state::CrdtPeerState,
) -> bool {
    use crate::mqtt::messages::SyncMessage;
    use crate::mqtt::Topic;
    use base64::Engine;
    use rumqttc::QoS;
    use yrs::{updates::decoder::Decode, Doc, ReadTxn, Transact, Update};

    // Generate unique request ID for correlation
    let req_id = Uuid::new_v4().to_string();

    // Build the sync topic: {workspace}/sync/{doc_path}/{client_id}
    let sync_topic = Topic::sync(workspace, doc_path, client_id);
    let sync_topic_str = sync_topic.to_topic_string();

    debug!(
        "[CYAN-SYNC] Starting schema sync for doc={} on topic={}",
        doc_path, sync_topic_str
    );

    // Step 1: Create message receiver BEFORE subscribing (to catch retained messages)
    let mut message_rx = mqtt_client.subscribe_messages();

    // Step 2: Subscribe to the sync topic
    if let Err(e) = mqtt_client
        .subscribe(&sync_topic_str, QoS::AtLeastOnce)
        .await
    {
        warn!(
            "[CYAN-SYNC] Failed to subscribe to sync topic {}: {}",
            sync_topic_str, e
        );
        return false;
    }

    // Step 3: Send Ancestors request
    let ancestors_msg = SyncMessage::Ancestors {
        req: req_id.clone(),
        commit: "HEAD".to_string(),
        depth: None,
    };
    let payload = match serde_json::to_vec(&ancestors_msg) {
        Ok(p) => p,
        Err(e) => {
            warn!("[CYAN-SYNC] Failed to serialize Ancestors message: {}", e);
            let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
            return false;
        }
    };

    if let Err(e) = mqtt_client
        .publish(&sync_topic_str, &payload, QoS::AtLeastOnce)
        .await
    {
        warn!("[CYAN-SYNC] Failed to publish Ancestors request: {}", e);
        let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
        return false;
    }

    debug!(
        "[CYAN-SYNC] Sent Ancestors(HEAD) request req={} to {}",
        req_id, sync_topic_str
    );

    // Step 4: Collect commit messages until Done or timeout
    let mut commits: Vec<(String, String)> = Vec::new(); // (id, base64_data)
    let mut head_cid: Option<String> = None;
    let timeout = tokio::time::Duration::from_secs(5);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            warn!(
                "[CYAN-SYNC] Timeout waiting for sync response for doc={} (req={})",
                doc_path, req_id
            );
            let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
            return false;
        }

        match tokio::time::timeout(remaining, message_rx.recv()).await {
            Ok(Ok(msg)) => {
                // Only process messages on our sync topic
                if msg.topic != sync_topic_str {
                    continue;
                }

                // Parse the sync message
                let sync_msg: SyncMessage = match serde_json::from_slice(&msg.payload) {
                    Ok(m) => m,
                    Err(e) => {
                        debug!(
                            "[CYAN-SYNC] Failed to parse sync message: {} (payload len={})",
                            e,
                            msg.payload.len()
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
                        debug!(
                            "[CYAN-SYNC] Received commit {} (data len={})",
                            id,
                            data.len()
                        );
                        // Track the last commit as HEAD
                        // Server sends oldest-first, so last commit is HEAD
                        head_cid = Some(id.clone());
                        commits.push((id.clone(), data.clone()));
                    }
                    SyncMessage::Done {
                        ref req,
                        commits: ref done_commits,
                        ..
                    } if req == &req_id => {
                        debug!(
                            "[CYAN-SYNC] Received Done for doc={}: {} commits",
                            doc_path,
                            done_commits.len()
                        );
                        // Done message commits list has HEAD last
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
                        warn!("[CYAN-SYNC] Server error for doc={}: {}", doc_path, message);
                        let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
                        return false;
                    }
                    _ => {
                        // Wrong req or unrelated message type — skip
                        continue;
                    }
                }
            }
            Ok(Err(e)) => {
                warn!("[CYAN-SYNC] Broadcast channel error: {}", e);
                let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
                return false;
            }
            Err(_) => {
                warn!(
                    "[CYAN-SYNC] Timeout waiting for sync response for doc={}",
                    doc_path
                );
                let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
                return false;
            }
        }
    }

    // Step 5: Unsubscribe from sync topic
    if let Err(e) = mqtt_client.unsubscribe(&sync_topic_str).await {
        debug!(
            "[CYAN-SYNC] Failed to unsubscribe from {}: {}",
            sync_topic_str, e
        );
    }

    // Step 6: Handle empty history (new document)
    if commits.is_empty() {
        debug!(
            "[CYAN-SYNC] Empty history for doc={} — initializing empty schema",
            doc_path
        );
        schema_state.initialize_empty();
        return true;
    }

    // Step 7: Build Y.Doc from commits
    let doc = Doc::new();
    for (cid, data_b64) in &commits {
        let data_bytes = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "[CYAN-SYNC] Failed to decode base64 for commit {}: {}",
                    cid, e
                );
                continue;
            }
        };

        let update = match Update::decode_v1(&data_bytes) {
            Ok(u) => u,
            Err(e) => {
                warn!(
                    "[CYAN-SYNC] Failed to decode Yrs update for commit {}: {}",
                    cid, e
                );
                continue;
            }
        };

        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    // Step 8: Encode final state and initialize CRDT
    let txn = doc.transact();
    let state_bytes = txn.encode_state_as_update_v1(&yrs::StateVector::default());
    drop(txn);

    let state_b64 = base64::engine::general_purpose::STANDARD.encode(&state_bytes);
    let cid = head_cid.as_deref().unwrap_or("unknown");

    schema_state.initialize_from_server(&state_b64, cid);

    debug!(
        "[CYAN-SYNC] Schema CRDT initialized for doc={}: {} commits applied, head={}",
        doc_path,
        commits.len(),
        cid
    );

    true
}

#[cfg(test)]
mod tests {
    use super::subdir_relative_path;

    #[test]
    fn subdir_relative_path_strips_prefix() {
        assert_eq!(
            subdir_relative_path("alpha/file.txt", "alpha").as_deref(),
            Some("file.txt")
        );
        assert_eq!(
            subdir_relative_path("alpha/nested/file.txt", "alpha").as_deref(),
            Some("nested/file.txt")
        );
    }

    #[test]
    fn subdir_relative_path_fallbacks_to_basename_when_mismatch() {
        assert_eq!(
            subdir_relative_path("other/path/file.txt", "alpha").as_deref(),
            Some("file.txt")
        );
    }

    #[test]
    fn subdir_relative_path_handles_empty_subdir() {
        assert_eq!(
            subdir_relative_path("root.txt", "").as_deref(),
            Some("root.txt")
        );
    }
}
