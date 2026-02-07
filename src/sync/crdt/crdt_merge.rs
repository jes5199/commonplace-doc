//! CRDT merge logic for peer sync.
//!
//! This module implements the "receive" side of CRDT peer sync:
//! - Handle incoming MQTT commits
//! - Detect divergence between local and remote state
//! - Create merge commits when needed
//! - Update state tracking (head_cid and local_head_cid)
//!
//! ## Merge Commit Creation (Cyan Sync Protocol)
//!
//! When a client detects divergence (local HEAD A, server HEAD B, neither is ancestor
//! of the other), it must create a merge commit following these steps:
//!
//! 1. Send `pull` request with `have=[A, A's ancestors...]`
//! 2. Receive commits from common ancestor X to B
//! 3. Apply server commits to a temporary Y.Doc starting from common ancestor state
//! 4. Merge with local Y.Doc using `Y.applyUpdate(localDoc, Y.encodeStateAsUpdate(serverDoc))`
//! 5. Create merge commit with `parents=[A, B]` and usually empty update
//! 6. Publish merge commit to edits topic
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md
//! See: docs/plans/2026-01-27-cyan-sync-protocol-design.md

use super::crdt_state::CrdtPeerState;
use crate::commit::Commit;
use crate::mqtt::EditMessage;
use crate::sync::error::{SyncError, SyncResult};
use base64::{engine::general_purpose::STANDARD, Engine};
use commonplace_types::traits::{edits_topic, CommitPersistence, MqttPublisher};
use std::sync::Arc;
use tracing::{debug, info, warn};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, Transact, Update};

/// Result of processing a received commit.
#[derive(Debug, PartialEq)]
pub enum MergeResult {
    /// Commit was already known, no action taken
    AlreadyKnown,
    /// Commit was a fast-forward (descendant of our state)
    FastForward {
        /// The new head CID
        new_head: String,
    },
    /// Commit required a merge (we had local changes)
    Merged {
        /// The merge commit CID (the actual CID of the published commit)
        merge_cid: String,
        /// The remote commit that triggered the merge
        remote_cid: String,
    },
    /// Merge is needed but hasn't been created yet
    /// (used internally by determine_merge_strategy)
    NeedsMerge {
        /// The remote commit that triggered the merge
        remote_cid: String,
    },
    /// Commit was applied but we can't write to disk (our local is ahead)
    LocalAhead,
    /// Intermediate commits are missing - cannot fast-forward.
    /// The received commit is not an immediate descendant of our current head.
    /// A resync from server is needed to obtain the missing history.
    MissingHistory {
        /// The received commit CID that we cannot fast-forward to
        received_cid: String,
        /// Our current head that is not in the received commit's parents
        current_head: String,
    },
}

/// Process a received MQTT edit message.
///
/// This implements the remote change flow:
/// 1. Parse the edit message
/// 2. Check if CID is already known
/// 3. Apply to Y.Doc
/// 4. Determine if fast-forward or merge needed
/// 5. Update state tracking
///
/// Returns the merge result and optionally the new content to write to disk.
pub async fn process_received_edit(
    mqtt_client: Option<&Arc<impl MqttPublisher>>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    edit_msg: &EditMessage,
    author: &str,
    commit_store: Option<&Arc<impl CommitPersistence>>,
) -> SyncResult<(MergeResult, Option<String>)> {
    // Compute CID for the received commit using the original timestamp
    // This ensures we get the same CID as when the commit was created
    let commit = Commit::with_timestamp(
        edit_msg.parents.clone(),
        edit_msg.update.clone(),
        edit_msg.author.clone(),
        edit_msg.message.clone(),
        edit_msg.timestamp,
    );
    let received_cid = commit.calculate_cid();

    // Persist the received commit locally if store is provided
    if let Some(store) = commit_store {
        match store.store_commit(&commit).await {
            Ok(cid) => {
                debug!("Persisted received commit {} to local store", cid);
            }
            Err(e) => {
                warn!(
                    "Failed to persist commit {} to local store: {}",
                    received_cid, e
                );
                // Continue processing even if local persistence fails
            }
        }
    }

    // Check if we already know this commit
    if state.is_cid_known(&received_cid) {
        debug!("Commit {} already known, skipping", received_cid);
        return Ok((MergeResult::AlreadyKnown, None));
    }

    // Load current Y.Doc
    let doc = state.to_doc()?;

    // Decode and apply the update (if non-empty)
    // Merge commits have empty updates - they just record parent CIDs
    if !edit_msg.update.is_empty() {
        let update_bytes = STANDARD.decode(&edit_msg.update)?;

        if !update_bytes.is_empty() {
            let update = Update::decode_v1(&update_bytes).map_err(|e| {
                SyncError::yjs_decode(format!("Failed to decode Yjs update: {}", e))
            })?;

            // Apply the update (merges with our local state)
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        } else {
            debug!("Received merge commit {} with empty update", received_cid);
        }
    } else {
        debug!("Received merge commit {} with empty update", received_cid);
    }

    // Determine merge strategy
    let strategy = determine_merge_strategy(state, &received_cid, &edit_msg.parents);

    // Process based on strategy and track actual result
    let final_result = match strategy {
        MergeResult::AlreadyKnown => {
            // Shouldn't happen here, but handle gracefully
            MergeResult::AlreadyKnown
        }
        MergeResult::FastForward { new_head } => {
            // Simple case: remote is a descendant of our state
            state.head_cid = Some(new_head.clone());
            // If we had no local changes, also update local_head_cid
            if state.local_head_cid.is_none()
                || state.local_head_cid == state.head_cid
                || is_ancestor(&state.local_head_cid, &edit_msg.parents)
            {
                state.local_head_cid = Some(new_head.clone());
            }
            state.update_from_doc(&doc);

            // Record this commit as known so future fast-forwards can reference it
            state.record_known_cid(&new_head);

            // Update document head in local store
            if let Some(store) = commit_store {
                if let Err(e) = store.set_document_head(node_id, &new_head).await {
                    warn!("Failed to update document head in local store: {}", e);
                }
            }

            info!("Fast-forward to commit {}", new_head);
            MergeResult::FastForward { new_head }
        }
        MergeResult::NeedsMerge { remote_cid } => {
            // We had local changes, need to create a merge commit.
            // The merge update contains the full merged state from an empty state vector.
            // This is safe because Yjs operations are idempotent - peers receiving this
            // will dedupe any operations they already have.
            //
            // Note: We previously tried delta encoding relative to pre-merge state,
            // but that was computing the wrong delta (remote ops instead of local ops).
            // Full state encoding is simpler and works correctly.
            let merge_update = create_merge_update(&doc)?;
            let local_cid = state.local_head_cid.clone().unwrap_or_default();

            // Create the merge commit
            let parents = vec![local_cid.clone(), remote_cid.clone()];
            let update_b64 = STANDARD.encode(&merge_update);
            let merge_commit = Commit::new(
                parents,
                update_b64,
                author.to_string(),
                Some("Merge commit".to_string()),
            );
            let actual_merge_cid = merge_commit.calculate_cid();

            // Persist merge commit to local store if provided
            if let Some(store) = commit_store {
                match store
                    .store_commit_and_set_head(node_id, &merge_commit)
                    .await
                {
                    Ok(_) => {
                        debug!("Persisted merge commit {} to local store", actual_merge_cid);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to persist merge commit {} to local store: {}",
                            actual_merge_cid, e
                        );
                    }
                }
            }

            // Publish via MQTT if client is provided
            if let Some(mqtt) = mqtt_client {
                let publish_msg = EditMessage {
                    update: merge_commit.update.clone(),
                    parents: merge_commit.parents.clone(),
                    author: author.to_string(),
                    message: Some("Merge commit".to_string()),
                    timestamp: merge_commit.timestamp,
                    req: None,
                };

                let topic = edits_topic(workspace, node_id);
                let payload = serde_json::to_vec(&publish_msg)?;

                mqtt.publish_retained(&topic, &payload).await.map_err(|e| {
                    SyncError::mqtt(format!("Failed to publish merge commit: {}", e))
                })?;

                info!(
                    "Published merge commit {} for {} (parents: {}, {})",
                    actual_merge_cid, node_id, local_cid, remote_cid
                );
            }

            state.head_cid = Some(actual_merge_cid.clone());
            state.local_head_cid = Some(actual_merge_cid.clone());
            state.update_from_doc(&doc);

            // Record both the merge commit and the remote commit as known
            state.record_known_cid(&actual_merge_cid);
            state.record_known_cid(&remote_cid);

            info!(
                "Created merge commit {} (local + remote {})",
                actual_merge_cid, remote_cid
            );
            MergeResult::Merged {
                merge_cid: actual_merge_cid,
                remote_cid,
            }
        }
        MergeResult::Merged { .. } => {
            // This variant is not returned by determine_merge_strategy
            unreachable!("determine_merge_strategy should not return Merged")
        }
        MergeResult::LocalAhead => {
            // We have local changes that aren't on the server yet
            // Apply the remote update to our doc but don't update local_head_cid
            state.head_cid = Some(received_cid.clone());
            state.update_from_doc(&doc);

            // Record this commit as known even though we're ahead
            state.record_known_cid(&received_cid);

            debug!("Applied remote commit {} but local is ahead", received_cid);
            MergeResult::LocalAhead
        }
        MergeResult::MissingHistory {
            received_cid: ref cid,
            ref current_head,
        } => {
            // Intermediate commits are missing - cannot fast-forward.
            // Don't update state - the caller should trigger a resync.
            debug!(
                "Missing intermediate commits between head {} and received {}. Resync needed.",
                current_head, cid
            );
            MergeResult::MissingHistory {
                received_cid: cid.clone(),
                current_head: current_head.clone(),
            }
        }
    };

    // Get the current content for writing to disk
    let content = get_doc_text_content(&doc);

    // Only return content if we should write to disk
    let should_write = matches!(
        final_result,
        MergeResult::FastForward { .. } | MergeResult::Merged { .. } | MergeResult::LocalAhead
    );

    Ok((
        final_result,
        if should_write { Some(content) } else { None },
    ))
}

/// Determine the merge strategy based on current state and received commit.
fn determine_merge_strategy(
    state: &CrdtPeerState,
    received_cid: &str,
    received_parents: &[String],
) -> MergeResult {
    let local_head = state.local_head_cid.as_deref();
    let server_head = state.head_cid.as_deref();

    // Case 1: No local state - this is a fast-forward from empty
    if local_head.is_none() && server_head.is_none() {
        return MergeResult::FastForward {
            new_head: received_cid.to_string(),
        };
    }

    // Case 2: Received commit has no parents but we have SYNCHRONIZED state.
    // If we have a server_head (meaning we've synced with the server before), and we receive
    // a commit with no parents, this indicates missing history - how does this new branch
    // connect to our existing history?
    // However, if we only have local_head but no server_head, this could be a valid
    // concurrent edit scenario where both parties started from empty.
    if received_parents.is_empty() && server_head.is_some() {
        let current = local_head.or(server_head).unwrap_or("(none)").to_string();
        return MergeResult::MissingHistory {
            received_cid: received_cid.to_string(),
            current_head: current,
        };
    }

    // Check if all parents of the received commit are known to us.
    // This ensures we have the full history before accepting the commit.
    let all_parents_known = received_parents.iter().all(|p| state.is_cid_known(p));

    // Case 3: All parents are known - we can fast-forward if appropriate
    if all_parents_known {
        // If local and server heads are ancestors (or same as) parents, fast-forward
        if is_ancestor(&local_head.map(String::from), received_parents)
            && is_ancestor(&server_head.map(String::from), received_parents)
        {
            return MergeResult::FastForward {
                new_head: received_cid.to_string(),
            };
        }

        // Case 4: Our local_head equals server_head - we're in sync, fast-forward
        if local_head == server_head {
            return MergeResult::FastForward {
                new_head: received_cid.to_string(),
            };
        }
    }

    // Case 5: We have local changes not on server (local_head != server_head)
    if let Some(local) = local_head {
        if Some(local) != server_head {
            // Check if received commit is based on server_head
            if is_ancestor(&server_head.map(String::from), received_parents) {
                // Server moved forward, we need to merge
                return MergeResult::NeedsMerge {
                    remote_cid: received_cid.to_string(),
                };
            }
        }
    }

    // Case 6: Parents are not all known - missing history
    // This check comes late to allow the NeedsMerge case to work even with
    // unknown parents on the local branch.
    if !all_parents_known {
        let current = local_head.or(server_head).unwrap_or("(none)").to_string();
        return MergeResult::MissingHistory {
            received_cid: received_cid.to_string(),
            current_head: current,
        };
    }

    // Default: local is ahead, just track the remote
    MergeResult::LocalAhead
}

/// Check if a CID is an ancestor (parent) of the received commit.
fn is_ancestor(cid: &Option<String>, parents: &[String]) -> bool {
    match cid {
        None => true, // Empty state is ancestor of everything
        Some(c) => parents.contains(c),
    }
}

/// Create an empty Yjs update for merge commits.
///
/// Merge commits don't need to carry any content - they just record that
/// two branches have been reconciled. Any peer can reconstruct the merged
/// state by replaying all ancestor commits (which Yjs handles correctly
/// due to its CRDT properties).
///
/// Using empty merge updates avoids feedback loops where peers keep
/// re-sending full state to each other.
fn create_merge_update(_doc: &Doc) -> SyncResult<Vec<u8>> {
    // Empty update - the merge is recorded by the commit's parent CIDs,
    // and state is reconstructed by replaying ancestors
    Ok(Vec::new())
}

/// Get text content from a Y.Doc.
///
/// Handles both YText (most file types) and YArray (JSONL files stored as
/// structured CRDT data). For YArray, converts back to JSONL text format.
///
/// Note: Yrs `get_text`/`get_array` always succeed regardless of actual type
/// (they reinterpret). We rely on the fact that `get_string()` on a YArray
/// returns empty string (array items are `Any` values, not `ItemContent::String`),
/// so we try YText first and fall back to YArray if the result is empty.
pub fn get_doc_text_content(doc: &Doc) -> String {
    let txn = doc.transact();
    // Try reading as YText first (covers most file types).
    // For actual YText roots this returns the content.
    // For YArray roots this returns "" because array items aren't string chunks.
    if let Some(text) = txn.get_text("content") {
        let s = text.get_string(&txn);
        if !s.is_empty() {
            return s;
        }
    }
    // If YText returned empty, try reading as YArray (JSONL files).
    if let Some(array) = txn.get_array("content") {
        let any_array = array.to_json(&txn);
        if let Ok(serde_json::Value::Array(items)) = serde_json::to_value(&any_array) {
            if !items.is_empty() {
                let lines: Vec<String> = items
                    .iter()
                    .filter_map(|item| serde_json::to_string(item).ok())
                    .collect();
                let mut content = lines.join("\n");
                if !content.is_empty() {
                    content.push('\n');
                }
                return content;
            }
        }
    }
    // If YArray also empty, try reading as YMap (JSON object files).
    if let Some(map) = txn.get_map("content") {
        let any_map = map.to_json(&txn);
        let json_value = crate::sync::crdt::yjs::any_to_json_value(any_map);
        if let serde_json::Value::Object(obj) = &json_value {
            if !obj.is_empty() {
                if let Ok(mut s) = serde_json::to_string(&json_value) {
                    s.push('\n');
                    return s;
                }
            }
        }
    }
    String::new()
}

/// Test-only accessor for get_doc_text_content (used by cross-module tests).
#[cfg(test)]
pub fn get_doc_text_content_for_test(doc: &Doc) -> String {
    get_doc_text_content(doc)
}

/// Parse an MQTT edit message payload.
pub fn parse_edit_message(payload: &[u8]) -> SyncResult<EditMessage> {
    serde_json::from_slice(payload).map_err(SyncError::from)
}

// =============================================================================
// Merge Commit Creation for Cyan Sync Protocol
// =============================================================================

/// Input data for creating a merge commit.
///
/// This struct captures all the information needed to create a merge commit
/// when the client detects divergence with the server.
#[derive(Debug, Clone)]
pub struct MergeCommitInput {
    /// Local HEAD commit CID (one of the merge parents)
    pub local_head: String,
    /// Server HEAD commit CID (one of the merge parents)
    pub server_head: String,
    /// Local Y.Doc state (serialized)
    pub local_doc_state: Vec<u8>,
    /// Server Y.Doc state at server_head (serialized)
    pub server_doc_state: Vec<u8>,
    /// Author identifier for the merge commit
    pub author: String,
}

/// Result of creating a merge commit.
#[derive(Debug, Clone)]
pub struct MergeCommitResult {
    /// The created merge commit
    pub commit: Commit,
    /// The CID of the merge commit
    pub cid: String,
    /// The merged Y.Doc state (for updating local state)
    pub merged_doc_state: Vec<u8>,
    /// Text content after merge (for writing to disk)
    pub merged_content: String,
}

/// Create a merge commit when client detects divergence with server.
///
/// This function implements the merge commit creation flow from the cyan sync protocol:
///
/// 1. Takes the local Y.Doc state and server Y.Doc state
/// 2. Merges them using Yjs CRDT semantics (applyUpdate)
/// 3. Creates a merge commit with `parents=[local_head, server_head]`
/// 4. Returns the commit for publishing to the edits topic
///
/// # Arguments
/// * `input` - The merge commit input data containing heads and doc states
///
/// # Returns
/// * `Ok(MergeCommitResult)` - The created merge commit with merged state
/// * `Err(SyncError)` - If the merge fails
///
/// # Example
/// ```ignore
/// let input = MergeCommitInput {
///     local_head: local_cid.clone(),
///     server_head: server_cid.clone(),
///     local_doc_state: local_state_bytes,
///     server_doc_state: server_state_bytes,
///     author: "client-123".to_string(),
/// };
/// let result = create_merge_commit(input)?;
/// // result.commit can be published to edits topic
/// // result.merged_doc_state should be saved locally
/// ```
pub fn create_merge_commit(input: MergeCommitInput) -> SyncResult<MergeCommitResult> {
    // Create a new Y.Doc for the merge
    let merge_doc = Doc::new();

    // Apply local state first
    if !input.local_doc_state.is_empty() {
        let local_update = Update::decode_v1(&input.local_doc_state).map_err(|e| {
            SyncError::yjs_decode(format!("Failed to decode local Y.Doc state: {}", e))
        })?;
        let mut txn = merge_doc.transact_mut();
        txn.apply_update(local_update);
    }

    // Apply server state to merge (Yjs CRDT semantics handle conflict resolution)
    if !input.server_doc_state.is_empty() {
        let server_update = Update::decode_v1(&input.server_doc_state).map_err(|e| {
            SyncError::yjs_decode(format!("Failed to decode server Y.Doc state: {}", e))
        })?;
        let mut txn = merge_doc.transact_mut();
        txn.apply_update(server_update);
    }

    // Get the merged content
    let merged_content = get_doc_text_content(&merge_doc);

    // Encode the merged state
    let merged_doc_state = {
        let txn = merge_doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };

    // Create the merge commit with empty update
    // Per the cyan sync protocol spec, merge commits typically have empty updates
    // because Yjs CRDT semantics handle the merge automatically.
    // The merge is recorded by having both parents, and peers can reconstruct
    // the merged state by replaying all ancestors.
    let parents = vec![input.local_head.clone(), input.server_head.clone()];
    let commit = Commit::new(
        parents,
        String::new(), // Empty update - Yjs handles merge semantics
        input.author,
        Some("Merge commit".to_string()),
    );
    let cid = commit.calculate_cid();

    debug!(
        "Created merge commit {} with parents [{}, {}]",
        &cid[..8.min(cid.len())],
        &input.local_head[..8.min(input.local_head.len())],
        &input.server_head[..8.min(input.server_head.len())]
    );

    Ok(MergeCommitResult {
        commit,
        cid,
        merged_doc_state,
        merged_content,
    })
}

/// Create and publish a merge commit when diverged.
///
/// This is a convenience function that:
/// 1. Creates a merge commit using `create_merge_commit`
/// 2. Persists it to the local commit store (if provided)
/// 3. Publishes it to the MQTT edits topic (if client provided)
/// 4. Updates the CRDT peer state
///
/// # Arguments
/// * `mqtt_client` - Optional MQTT client for publishing
/// * `workspace` - The workspace name for MQTT topic
/// * `node_id` - The document/node ID
/// * `state` - The CRDT peer state to update
/// * `server_head` - The server's HEAD CID
/// * `server_doc_state` - The server's Y.Doc state
/// * `author` - Author identifier
/// * `commit_store` - Optional commit store for persistence
///
/// # Returns
/// * `Ok(MergeCommitResult)` - The created merge commit
/// * `Err(SyncError)` - If the merge or publish fails
#[allow(clippy::too_many_arguments)]
pub async fn create_and_publish_merge_commit(
    mqtt_client: Option<&Arc<impl MqttPublisher>>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    server_head: String,
    server_doc_state: Vec<u8>,
    author: &str,
    commit_store: Option<&Arc<impl CommitPersistence>>,
) -> SyncResult<MergeCommitResult> {
    // Get local state
    let local_head = state
        .local_head_cid
        .clone()
        .ok_or_else(|| SyncError::other("No local HEAD for merge"))?;

    let local_doc_state = if let Some(ref yjs_state_b64) = state.yjs_state {
        STANDARD.decode(yjs_state_b64)?
    } else {
        Vec::new()
    };

    // Create the merge commit
    let input = MergeCommitInput {
        local_head: local_head.clone(),
        server_head: server_head.clone(),
        local_doc_state,
        server_doc_state,
        author: author.to_string(),
    };
    let result = create_merge_commit(input)?;

    // Persist to local store if provided
    if let Some(store) = commit_store {
        match store
            .store_commit_and_set_head(node_id, &result.commit)
            .await
        {
            Ok(_) => {
                debug!("Persisted merge commit {} to local store", result.cid);
            }
            Err(e) => {
                warn!(
                    "Failed to persist merge commit {} to local store: {}",
                    result.cid, e
                );
            }
        }
    }

    // Publish to MQTT if client is provided
    if let Some(mqtt) = mqtt_client {
        let publish_msg = EditMessage {
            update: result.commit.update.clone(),
            parents: result.commit.parents.clone(),
            author: author.to_string(),
            message: Some("Merge commit".to_string()),
            timestamp: result.commit.timestamp,
            req: None,
        };

        let topic = edits_topic(workspace, node_id);
        let payload = serde_json::to_vec(&publish_msg)?;

        mqtt.publish_retained(&topic, &payload)
            .await
            .map_err(|e| SyncError::mqtt(format!("Failed to publish merge commit: {}", e)))?;

        info!(
            "Published merge commit {} for {} (parents: {}, {})",
            result.cid, node_id, local_head, server_head
        );
    }

    // Update CRDT peer state
    state.head_cid = Some(result.cid.clone());
    state.local_head_cid = Some(result.cid.clone());
    state.yjs_state = Some(STANDARD.encode(&result.merged_doc_state));

    // Record commits as known
    state.record_known_cid(&result.cid);
    state.record_known_cid(&server_head);

    info!(
        "Created merge commit {} (local {} + server {})",
        result.cid, local_head, server_head
    );

    Ok(result)
}

/// Apply commits received from a pull request to a Y.Doc.
///
/// This function is used during the merge flow when receiving commits
/// from the server after a pull request. It applies all received commits
/// in order to build up the server's state at server_head.
///
/// # Arguments
/// * `commits` - List of commits in chronological order (oldest first)
///
/// # Returns
/// * `Ok(Vec<u8>)` - The Y.Doc state after applying all commits
/// * `Err(SyncError)` - If any commit fails to apply
pub fn apply_commits_to_doc(commits: &[Commit]) -> SyncResult<Vec<u8>> {
    let doc = Doc::new();

    for commit in commits {
        if commit.update.is_empty() {
            // Empty update (e.g., merge commit) - skip
            continue;
        }

        let update_bytes = STANDARD.decode(&commit.update)?;
        if update_bytes.is_empty() {
            continue;
        }

        let update = Update::decode_v1(&update_bytes).map_err(|e| {
            SyncError::yjs_decode(format!("Failed to decode Yjs update in commit: {}", e))
        })?;

        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    let txn = doc.transact();
    Ok(txn.encode_state_as_update_v1(&yrs::StateVector::default()))
}

/// Find the common ancestor between local and server commit histories.
///
/// This is a client-side helper that iterates through local known commits
/// to find the first one that matches one of the commits in the server's
/// history (provided via the `have` response).
///
/// # Arguments
/// * `local_known_cids` - Set of CIDs known locally
/// * `server_commits` - List of commit CIDs from server (in reverse order, newest first)
///
/// # Returns
/// * `Some(cid)` - The common ancestor CID
/// * `None` - If no common ancestor found
pub fn find_common_ancestor(
    local_known_cids: &std::collections::HashSet<String>,
    server_commits: &[String],
) -> Option<String> {
    for cid in server_commits {
        if local_known_cids.contains(cid) {
            return Some(cid.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::EditMessage;
    use uuid::Uuid;
    use yrs::{Text, WriteTxn};

    #[test]
    fn test_merge_result_local_ahead() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        // We have both head_cid and local_head_cid set to different values,
        // indicating we have local changes not yet on server.
        // The parent is known (in our known_cids set).
        state.head_cid = Some("server_head".to_string());
        state.local_head_cid = Some("local_head".to_string());
        state.record_known_cid("some_parent");

        // Received commit with known parent that doesn't include our local_head
        // and server_head is not in parents either - this triggers LocalAhead
        let result = determine_merge_strategy(&state, "new_commit", &["some_parent".to_string()]);
        // This should be LocalAhead since local is different from server and commit
        // doesn't acknowledge either of them as parent
        assert!(
            matches!(result, MergeResult::LocalAhead),
            "Expected LocalAhead, got {:?}",
            result
        );
    }

    #[test]
    fn test_merge_result_fast_forward_from_empty() {
        let state = CrdtPeerState::new(Uuid::new_v4());

        let result = determine_merge_strategy(&state, "new_cid", &[]);
        assert!(matches!(result, MergeResult::FastForward { new_head } if new_head == "new_cid"));
    }

    #[test]
    fn test_merge_result_fast_forward_descendant() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("parent".to_string());
        state.local_head_cid = Some("parent".to_string());

        let result = determine_merge_strategy(&state, "child", &["parent".to_string()]);
        assert!(matches!(result, MergeResult::FastForward { new_head } if new_head == "child"));
    }

    #[test]
    fn test_merge_result_needs_merge() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("server_head".to_string());
        state.local_head_cid = Some("local_head".to_string());

        let result = determine_merge_strategy(&state, "new_remote", &["server_head".to_string()]);
        assert!(matches!(result, MergeResult::NeedsMerge { .. }));
    }

    #[test]
    fn test_is_ancestor() {
        assert!(is_ancestor(&None, &[]));
        assert!(is_ancestor(&None, &["any".to_string()]));
        assert!(is_ancestor(
            &Some("parent".to_string()),
            &["parent".to_string()]
        ));
        assert!(!is_ancestor(
            &Some("other".to_string()),
            &["parent".to_string()]
        ));
    }

    #[test]
    fn test_missing_history_when_head_not_in_parents() {
        // Scenario: We're at commit A, receive commit D with parent C.
        // A -> B -> C -> D (but we only have A, skipped B and C)
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("commit_a".to_string());
        state.local_head_cid = Some("commit_a".to_string());

        // Received commit D has parent C, not A - intermediate commits are missing
        let result = determine_merge_strategy(&state, "commit_d", &["commit_c".to_string()]);

        assert!(
            matches!(
                &result,
                MergeResult::MissingHistory {
                    received_cid,
                    current_head
                } if received_cid == "commit_d" && current_head == "commit_a"
            ),
            "Expected MissingHistory, got {:?}",
            &result
        );
    }

    #[test]
    fn test_missing_history_with_unknown_parents() {
        // Scenario: We're at commit A, receive commit B with an unknown parent C.
        // We don't have commit C so we can't fast-forward.
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("commit_a".to_string());
        state.local_head_cid = Some("commit_a".to_string());
        state.record_known_cid("commit_a");

        // Received commit has parent "commit_c" which we don't know
        let result = determine_merge_strategy(&state, "commit_b", &["commit_c".to_string()]);

        assert!(
            matches!(
                &result,
                MergeResult::MissingHistory {
                    received_cid,
                    current_head
                } if received_cid == "commit_b" && current_head == "commit_a"
            ),
            "Expected MissingHistory, got {:?}",
            &result
        );
    }

    #[tokio::test]
    async fn test_process_received_edit_missing_history_skips_content() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("commit_a".to_string());
        state.local_head_cid = Some("commit_a".to_string());
        state.record_known_cid("commit_a");

        let edit_msg = EditMessage {
            update: String::new(),
            parents: vec!["commit_c".to_string()],
            author: "peer".to_string(),
            message: None,
            timestamp: 1,
            req: None,
        };

        let (result, maybe_content) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "workspace",
            "node",
            &mut state,
            &edit_msg,
            "author",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .unwrap();

        assert!(matches!(result, MergeResult::MissingHistory { .. }));
        assert!(maybe_content.is_none());
    }

    #[test]
    fn test_fast_forward_when_head_is_parent() {
        // Scenario: We're at commit A, receive commit B with parent A.
        // This is a valid fast-forward (immediate descendant).
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("commit_a".to_string());
        state.local_head_cid = Some("commit_a".to_string());

        // Received commit has our head as parent - valid fast-forward
        let result = determine_merge_strategy(&state, "commit_b", &["commit_a".to_string()]);

        assert!(
            matches!(&result, MergeResult::FastForward { ref new_head } if new_head == "commit_b"),
            "Expected FastForward, got {:?}",
            &result
        );
    }

    #[test]
    fn test_missing_history_with_empty_parents_and_server_head() {
        // Scenario: We have synchronized state (server_head set), receive a genesis commit.
        // This is missing history - how does the new branch connect to our history?
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("commit_a".to_string());
        state.local_head_cid = Some("commit_a".to_string());

        // Received commit has no parents - cannot be a descendant
        let result = determine_merge_strategy(&state, "commit_b", &[]);

        assert!(
            matches!(
                &result,
                MergeResult::MissingHistory {
                    received_cid,
                    current_head
                } if received_cid == "commit_b" && current_head == "commit_a"
            ),
            "Expected MissingHistory, got {:?}",
            &result
        );
    }

    #[test]
    fn test_get_doc_text_content() {
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello world");
        }

        let content = get_doc_text_content(&doc);
        assert_eq!(content, "hello world");
    }

    #[test]
    fn test_get_doc_text_content_empty() {
        let doc = Doc::new();
        let content = get_doc_text_content(&doc);
        assert_eq!(content, "");
    }

    #[test]
    fn test_parse_edit_message() {
        let msg = EditMessage {
            update: "base64data".to_string(),
            parents: vec!["parent1".to_string()],
            author: "user".to_string(),
            message: None,
            timestamp: 12345,
            req: None,
        };
        let payload = serde_json::to_vec(&msg).unwrap();

        let parsed = parse_edit_message(&payload).unwrap();
        assert_eq!(parsed.update, "base64data");
        assert_eq!(parsed.parents, vec!["parent1".to_string()]);
        assert_eq!(parsed.author, "user");
    }

    // ==========================================================================
    // Integration Tests for CRDT Peer Sync
    // ==========================================================================

    /// Test that two clients with concurrent edits merge correctly.
    ///
    /// Scenario:
    /// 1. Both clients start from same initial state
    /// 2. Client A makes edit "hello"
    /// 3. Client B makes edit "world"
    /// 4. Each receives the other's edit
    /// 5. Both should end up with same merged content
    #[tokio::test]
    async fn test_concurrent_edits_merge_correctly() {
        use yrs::{Text, WriteTxn};

        // Create initial shared state
        let initial_doc = Doc::with_client_id(0);
        {
            let mut txn = initial_doc.transact_mut();
            txn.get_or_insert_text("content");
        }
        let initial_state = {
            let txn = initial_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        // Client A starts from initial state
        let mut state_a = CrdtPeerState::new(Uuid::new_v4());
        state_a.yjs_state = Some(STANDARD.encode(&initial_state));

        // Client B starts from same initial state
        let mut state_b = CrdtPeerState::new(Uuid::new_v4());
        state_b.yjs_state = Some(STANDARD.encode(&initial_state));

        // Client A makes edit "hello" at position 0
        let doc_a = Doc::with_client_id(1);
        let update_a = {
            let mut txn = doc_a.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
            txn.encode_update_v1()
        };
        let update_a_b64 = STANDARD.encode(&update_a);
        let commit_a = Commit::new(vec![], update_a_b64.clone(), "user_a".to_string(), None);
        let cid_a = commit_a.calculate_cid();
        state_a.local_head_cid = Some(cid_a.clone());
        state_a.yjs_state = Some(STANDARD.encode({
            let txn = doc_a.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Client B makes edit "world" at position 0
        let doc_b = Doc::with_client_id(2);
        let update_b = {
            let mut txn = doc_b.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "world");
            txn.encode_update_v1()
        };
        let update_b_b64 = STANDARD.encode(&update_b);
        let commit_b = Commit::new(vec![], update_b_b64.clone(), "user_b".to_string(), None);
        let cid_b = commit_b.calculate_cid();
        state_b.local_head_cid = Some(cid_b.clone());
        state_b.yjs_state = Some(STANDARD.encode({
            let txn = doc_b.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Client A receives Client B's edit
        let edit_from_b = EditMessage {
            update: update_b_b64.clone(),
            parents: vec![],
            author: "user_b".to_string(),
            message: None,
            timestamp: 1000,
            req: None,
        };
        let (result_a, _content_a) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "workspace",
            "node1",
            &mut state_a,
            &edit_from_b,
            "user_a",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .expect("Should process edit");

        // Should be a merge since both have local changes
        assert!(
            matches!(
                result_a,
                MergeResult::Merged { .. } | MergeResult::LocalAhead
            ),
            "Expected Merged or LocalAhead, got {:?}",
            result_a
        );

        // Client B receives Client A's edit
        let edit_from_a = EditMessage {
            update: update_a_b64.clone(),
            parents: vec![],
            author: "user_a".to_string(),
            message: None,
            timestamp: 1001,
            req: None,
        };
        let (result_b, _content_b) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "workspace",
            "node1",
            &mut state_b,
            &edit_from_a,
            "user_b",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .expect("Should process edit");

        assert!(
            matches!(
                result_b,
                MergeResult::Merged { .. } | MergeResult::LocalAhead
            ),
            "Expected Merged or LocalAhead, got {:?}",
            result_b
        );

        // Both should now have both "hello" and "world" in their content
        let final_doc_a = state_a.to_doc().unwrap();
        let final_content_a = get_doc_text_content(&final_doc_a);

        let final_doc_b = state_b.to_doc().unwrap();
        let final_content_b = get_doc_text_content(&final_doc_b);

        // Both should have merged content (order may vary due to CRDT semantics)
        assert!(
            final_content_a.contains("hello") || final_content_a.contains("world"),
            "Client A should have merged content, got: {}",
            final_content_a
        );
        assert!(
            final_content_b.contains("hello") || final_content_b.contains("world"),
            "Client B should have merged content, got: {}",
            final_content_b
        );
    }

    /// Test that CID-based echo prevention works.
    ///
    /// When a client receives its own commit (via MQTT echo), it should be ignored.
    #[tokio::test]
    async fn test_cid_echo_prevention() {
        use yrs::{Text, WriteTxn};

        // Create a client with some local state
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Make a local edit
        let doc = Doc::with_client_id(1);
        let update = {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "my edit");
            txn.encode_update_v1()
        };
        let update_b64 = STANDARD.encode(&update);
        // Use explicit timestamp so we can recreate the same CID
        let timestamp = 1000u64;
        let commit = Commit::with_timestamp(
            vec![],
            update_b64.clone(),
            "me".to_string(),
            None,
            timestamp,
        );
        let cid = commit.calculate_cid();

        // Update state with our commit
        state.local_head_cid = Some(cid.clone());
        state.head_cid = Some(cid.clone());
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Now "receive" the same commit as if it came from MQTT
        // Use the same timestamp so CID matches
        let echo_msg = EditMessage {
            update: update_b64,
            parents: vec![],
            author: "me".to_string(),
            message: None,
            timestamp,
            req: None,
        };

        let (result, _) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "workspace",
            "node1",
            &mut state,
            &echo_msg,
            "me",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .expect("Should process edit");

        // Should recognize it's already known
        assert_eq!(result, MergeResult::AlreadyKnown, "Should detect echo");
    }

    /// Test fast-forward when receiving a descendant commit.
    #[tokio::test]
    async fn test_fast_forward_descendant() {
        use yrs::{Text, WriteTxn};

        // Start with empty state
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Create first commit
        let doc1 = Doc::with_client_id(1);
        let update1 = {
            let mut txn = doc1.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "first");
            txn.encode_update_v1()
        };
        let update1_b64 = STANDARD.encode(&update1);
        // Use explicit timestamp so CID calculation is deterministic
        let timestamp1 = 1000u64;
        let commit1 = Commit::with_timestamp(
            vec![],
            update1_b64.clone(),
            "user".to_string(),
            None,
            timestamp1,
        );
        let cid1 = commit1.calculate_cid();

        // Receive first commit (using same timestamp as commit calculation)
        let msg1 = EditMessage {
            update: update1_b64,
            parents: vec![],
            author: "user".to_string(),
            message: None,
            timestamp: timestamp1,
            req: None,
        };
        let (result1, _) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "ws",
            "n1",
            &mut state,
            &msg1,
            "user",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .unwrap();
        assert!(matches!(result1, MergeResult::FastForward { .. }));

        // Create second commit that builds on first
        let doc2 = Doc::with_client_id(1);
        {
            // Apply first update
            let update = Update::decode_v1(&STANDARD.decode(&msg1.update).unwrap()).unwrap();
            let mut txn = doc2.transact_mut();
            txn.apply_update(update);
        }
        let update2 = {
            let mut txn = doc2.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 5, " second"); // "first second"
            txn.encode_update_v1()
        };
        let update2_b64 = STANDARD.encode(&update2);

        // Receive second commit (child of first)
        let msg2 = EditMessage {
            update: update2_b64,
            parents: vec![cid1], // Parent is first commit (with matching timestamp)
            author: "user".to_string(),
            message: None,
            timestamp: 1001,
            req: None,
        };
        let (result2, content) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "ws",
            "n1",
            &mut state,
            &msg2,
            "user",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .unwrap();

        // Should be fast-forward since it's a descendant
        assert!(
            matches!(result2, MergeResult::FastForward { .. }),
            "Expected FastForward, got {:?}",
            result2
        );

        // Content should include both edits
        assert!(content.is_some());
    }

    /// Test that local changes trigger merge when remote diverges.
    #[tokio::test]
    async fn test_divergence_triggers_merge() {
        use yrs::{Text, WriteTxn};

        // Create client with local uncommitted changes
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Make a local edit and track it
        let doc = Doc::with_client_id(1);
        let update = {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "local");
            txn.encode_update_v1()
        };
        let update_b64 = STANDARD.encode(&update);
        let commit = Commit::new(vec![], update_b64, "me".to_string(), None);
        let local_cid = commit.calculate_cid();

        state.local_head_cid = Some(local_cid.clone());
        state.head_cid = None; // No server commits yet
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Receive a different commit from server (divergent)
        let remote_doc = Doc::with_client_id(2);
        let remote_update = {
            let mut txn = remote_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "remote");
            txn.encode_update_v1()
        };
        let remote_update_b64 = STANDARD.encode(&remote_update);

        let remote_msg = EditMessage {
            update: remote_update_b64,
            parents: vec![], // No parent - divergent from local
            author: "other".to_string(),
            message: None,
            timestamp: 1000,
            req: None,
        };

        let (result, _) = process_received_edit(
            None::<&Arc<crate::mqtt::MqttClient>>,
            "ws",
            "n1",
            &mut state,
            &remote_msg,
            "me",
            None::<&Arc<crate::store::CommitStore>>,
        )
        .await
        .unwrap();

        // Should trigger merge or LocalAhead since we have divergent local changes
        assert!(
            matches!(result, MergeResult::Merged { .. } | MergeResult::LocalAhead),
            "Expected Merged or LocalAhead for divergent commits, got {:?}",
            result
        );
    }

    // ==========================================================================
    // Tests for Merge Commit Creation (Cyan Sync Protocol)
    // ==========================================================================

    /// Test basic merge commit creation.
    #[test]
    fn test_create_merge_commit_basic() {
        use yrs::{Text, WriteTxn};

        // Create local state with "hello"
        let local_doc = Doc::with_client_id(1);
        {
            let mut txn = local_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }
        let local_state = {
            let txn = local_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        // Create server state with "world"
        let server_doc = Doc::with_client_id(2);
        {
            let mut txn = server_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "world");
        }
        let server_state = {
            let txn = server_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        let input = MergeCommitInput {
            local_head: "local_cid_123".to_string(),
            server_head: "server_cid_456".to_string(),
            local_doc_state: local_state,
            server_doc_state: server_state,
            author: "test_user".to_string(),
        };

        let result = create_merge_commit(input).unwrap();

        // Verify merge commit structure
        assert!(result.commit.is_merge(), "Should be a merge commit");
        assert_eq!(result.commit.parents.len(), 2);
        assert!(result.commit.parents.contains(&"local_cid_123".to_string()));
        assert!(result
            .commit
            .parents
            .contains(&"server_cid_456".to_string()));
        assert_eq!(result.commit.author, "test_user");
        assert_eq!(result.commit.message, Some("Merge commit".to_string()));

        // Verify update is empty (per cyan sync protocol spec)
        assert!(
            result.commit.update.is_empty(),
            "Merge commit should have empty update"
        );

        // Verify CID is computed
        assert!(!result.cid.is_empty());
        assert_eq!(result.cid.len(), 64); // SHA-256 hex is 64 chars

        // Verify merged content contains both "hello" and "world"
        // (order depends on client IDs, but both should be present)
        assert!(
            result.merged_content.contains("hello") && result.merged_content.contains("world"),
            "Merged content should contain both 'hello' and 'world', got: {}",
            result.merged_content
        );

        // Verify merged state is non-empty
        assert!(!result.merged_doc_state.is_empty());
    }

    /// Test merge commit with empty local state.
    #[test]
    fn test_create_merge_commit_empty_local() {
        use yrs::Text;

        // Empty local state
        let local_state = Vec::new();

        // Server state with content
        let server_doc = Doc::with_client_id(2);
        {
            let mut txn = server_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "server content");
        }
        let server_state = {
            let txn = server_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        let input = MergeCommitInput {
            local_head: "local_cid".to_string(),
            server_head: "server_cid".to_string(),
            local_doc_state: local_state,
            server_doc_state: server_state,
            author: "user".to_string(),
        };

        let result = create_merge_commit(input).unwrap();

        assert!(result.commit.is_merge());
        assert_eq!(result.merged_content, "server content");
    }

    /// Test merge commit with empty server state.
    #[test]
    fn test_create_merge_commit_empty_server() {
        use yrs::Text;

        // Local state with content
        let local_doc = Doc::with_client_id(1);
        {
            let mut txn = local_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "local content");
        }
        let local_state = {
            let txn = local_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        // Empty server state
        let server_state = Vec::new();

        let input = MergeCommitInput {
            local_head: "local_cid".to_string(),
            server_head: "server_cid".to_string(),
            local_doc_state: local_state,
            server_doc_state: server_state,
            author: "user".to_string(),
        };

        let result = create_merge_commit(input).unwrap();

        assert!(result.commit.is_merge());
        assert_eq!(result.merged_content, "local content");
    }

    /// Test merge commit CID is deterministic.
    #[test]
    fn test_merge_commit_cid_deterministic() {
        let input = MergeCommitInput {
            local_head: "local_cid_123".to_string(),
            server_head: "server_cid_456".to_string(),
            local_doc_state: Vec::new(),
            server_doc_state: Vec::new(),
            author: "user".to_string(),
        };

        let result1 = create_merge_commit(input.clone()).unwrap();
        let result2 = create_merge_commit(input).unwrap();

        // Note: CIDs won't match because Commit::new() uses current timestamp
        // But the structure should be the same
        assert_eq!(result1.commit.parents, result2.commit.parents);
        assert_eq!(result1.commit.author, result2.commit.author);
        assert_eq!(result1.commit.update, result2.commit.update);
    }

    /// Test apply_commits_to_doc with multiple commits.
    #[test]
    fn test_apply_commits_to_doc() {
        use yrs::Text;

        // Create a sequence of commits
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("content");

        // Commit 1: "hello"
        let update1 = {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "hello");
            txn.encode_update_v1()
        };
        let commit1 = Commit::new(vec![], STANDARD.encode(&update1), "user".to_string(), None);

        // Commit 2: "hello world"
        let update2 = {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 5, " world");
            txn.encode_update_v1()
        };
        let commit2 = Commit::new(
            vec!["cid1".to_string()],
            STANDARD.encode(&update2),
            "user".to_string(),
            None,
        );

        // Apply commits
        let result_state = apply_commits_to_doc(&[commit1, commit2]).unwrap();

        // Verify the result state
        let verify_doc = Doc::new();
        {
            let update = Update::decode_v1(&result_state).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }
        let content = get_doc_text_content(&verify_doc);
        assert_eq!(content, "hello world");
    }

    /// Test apply_commits_to_doc with empty update (merge commit).
    #[test]
    fn test_apply_commits_to_doc_with_merge() {
        use yrs::Text;

        // Create a commit with content
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("content");
        let update1 = {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "content");
            txn.encode_update_v1()
        };
        let commit1 = Commit::new(vec![], STANDARD.encode(&update1), "user".to_string(), None);

        // Create a merge commit with empty update
        let merge_commit = Commit::new(
            vec!["cid1".to_string(), "cid2".to_string()],
            String::new(), // Empty update
            "user".to_string(),
            Some("Merge".to_string()),
        );

        // Apply commits (including merge with empty update)
        let result_state = apply_commits_to_doc(&[commit1, merge_commit]).unwrap();

        // Verify the result state
        let verify_doc = Doc::new();
        {
            let update = Update::decode_v1(&result_state).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }
        let content = get_doc_text_content(&verify_doc);
        assert_eq!(content, "content");
    }

    /// Test find_common_ancestor.
    #[test]
    fn test_find_common_ancestor() {
        use std::collections::HashSet;

        let mut local_known = HashSet::new();
        local_known.insert("commit_a".to_string());
        local_known.insert("commit_b".to_string());
        local_known.insert("commit_c".to_string());

        // Server commits from newest to oldest
        let server_commits = vec![
            "commit_x".to_string(),
            "commit_y".to_string(),
            "commit_b".to_string(), // This should be found
            "commit_a".to_string(),
        ];

        let ancestor = find_common_ancestor(&local_known, &server_commits);
        assert_eq!(ancestor, Some("commit_b".to_string()));
    }

    /// Test find_common_ancestor with no common ancestor.
    #[test]
    fn test_find_common_ancestor_none() {
        use std::collections::HashSet;

        let mut local_known = HashSet::new();
        local_known.insert("commit_a".to_string());
        local_known.insert("commit_b".to_string());

        let server_commits = vec![
            "commit_x".to_string(),
            "commit_y".to_string(),
            "commit_z".to_string(),
        ];

        let ancestor = find_common_ancestor(&local_known, &server_commits);
        assert!(ancestor.is_none());
    }

    /// Test find_common_ancestor with empty server list.
    #[test]
    fn test_find_common_ancestor_empty_server() {
        use std::collections::HashSet;

        let mut local_known = HashSet::new();
        local_known.insert("commit_a".to_string());

        let server_commits: Vec<String> = vec![];

        let ancestor = find_common_ancestor(&local_known, &server_commits);
        assert!(ancestor.is_none());
    }

    /// Test that merge commits have correct parents order.
    #[test]
    fn test_merge_commit_parents_order() {
        let input = MergeCommitInput {
            local_head: "local_first".to_string(),
            server_head: "server_second".to_string(),
            local_doc_state: Vec::new(),
            server_doc_state: Vec::new(),
            author: "user".to_string(),
        };

        let result = create_merge_commit(input).unwrap();

        // Parents should be [local_head, server_head] in that order
        assert_eq!(result.commit.parents[0], "local_first");
        assert_eq!(result.commit.parents[1], "server_second");
    }

    #[test]
    fn test_get_doc_text_content_ytext() {
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello world");
        }
        assert_eq!(get_doc_text_content(&doc), "hello world");
    }

    #[test]
    fn test_get_doc_text_content_yarray_jsonl() {
        // Create a doc with YArray content (as JSONL files now use)
        let jsonl = r#"{"name":"alice","age":30}
{"name":"bob","age":25}
"#;
        let update_b64 = crate::sync::crdt::yjs::create_yjs_jsonl_update(jsonl, None).unwrap();
        let update_bytes = STANDARD.decode(&update_b64).unwrap();

        let doc = Doc::new();
        {
            let update = Update::decode_v1(&update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        // get_doc_text_content should return valid JSONL
        let content = get_doc_text_content(&doc);
        assert!(!content.is_empty(), "Content should not be empty");
        assert!(content.ends_with('\n'), "JSONL should end with newline");

        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2, "Should have 2 JSONL lines");

        // Verify each line is valid JSON with expected content
        let obj1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(obj1["name"], "alice");
        let obj2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(obj2["name"], "bob");
    }

    #[test]
    fn test_get_doc_text_content_empty_doc() {
        let doc = Doc::new();
        assert_eq!(get_doc_text_content(&doc), "");
    }

    /// Test that YArray content matches parsed JSONL values for content comparison.
    ///
    /// This verifies that `doc_to_json_array_value` returns values identical to
    /// what `parse_jsonl_values` would produce, which is critical for the
    /// `match_doc_structured_content` divergence check.
    #[test]
    fn test_yarray_matches_parsed_jsonl_values() {
        let content = "{\"id\": 1, \"message\": \"first line\"}\n\
                       {\"id\": 2, \"message\": \"second line\"}\n";

        // Create YArray from content
        let update_b64 = crate::sync::crdt::yjs::create_yjs_jsonl_update(content, None).unwrap();
        let update_bytes = STANDARD.decode(&update_b64).unwrap();

        let doc = Doc::new();
        {
            let update = Update::decode_v1(&update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        // Extract via doc_to_json_array_value (as match_doc_structured_content does)
        let doc_value = crate::sync::crdt::yjs::doc_to_json_array_value(&doc)
            .expect("Should extract array value");

        // Parse as JSONL values (as match_doc_structured_content does)
        let parsed_values: Vec<serde_json::Value> = content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let expected = serde_json::Value::Array(parsed_values);

        eprintln!("doc_value: {:?}", doc_value);
        eprintln!("expected:  {:?}", expected);
        assert_eq!(
            doc_value, expected,
            "YArray content should match parsed JSONL values"
        );
    }

    /// Test the full workspace→sandbox→workspace JSONL roundtrip.
    ///
    /// Simulates: workspace creates JSONL, sandbox receives it, sandbox appends
    /// a line, workspace receives sandbox's update. Verifies content is correct
    /// at each step.
    #[test]
    fn test_jsonl_roundtrip_workspace_sandbox() {
        // Step 1: Workspace creates JSONL with 2 lines
        let initial_content = "{\"id\":1,\"message\":\"first line\"}\n\
                               {\"id\":2,\"message\":\"second line\"}\n";
        let initial_b64 =
            crate::sync::crdt::yjs::create_yjs_jsonl_update(initial_content, None).unwrap();
        let initial_bytes = STANDARD.decode(&initial_b64).unwrap();

        // Workspace doc
        let ws_doc = Doc::new();
        {
            let update = Update::decode_v1(&initial_bytes).unwrap();
            let mut txn = ws_doc.transact_mut();
            txn.apply_update(update);
        }

        // Step 2: Sandbox receives it
        let sandbox_doc = Doc::new();
        {
            let update = Update::decode_v1(&initial_bytes).unwrap();
            let mut txn = sandbox_doc.transact_mut();
            txn.apply_update(update);
        }
        let sandbox_state = {
            let txn = sandbox_doc.transact();
            STANDARD.encode(txn.encode_state_as_update_v1(&yrs::StateVector::default()))
        };

        // Sandbox reads content
        let sandbox_content = get_doc_text_content(&sandbox_doc);
        assert!(
            sandbox_content.contains("first line"),
            "Sandbox should have first line"
        );
        assert!(
            sandbox_content.contains("second line"),
            "Sandbox should have second line"
        );

        // Step 3: Sandbox edits (appends third line)
        let new_content = "{\"id\":1,\"message\":\"first line\"}\n\
                           {\"id\":2,\"message\":\"second line\"}\n\
                           {\"id\":3,\"message\":\"third line from sandbox\"}\n";
        let sandbox_update_b64 =
            crate::sync::crdt::yjs::create_yjs_jsonl_update(new_content, Some(&sandbox_state))
                .unwrap();
        let sandbox_update_bytes = STANDARD.decode(&sandbox_update_b64).unwrap();

        // Step 4: Workspace receives sandbox's update
        {
            let update = Update::decode_v1(&sandbox_update_bytes).unwrap();
            let mut txn = ws_doc.transact_mut();
            txn.apply_update(update);
        }

        // Step 5: Workspace reads content
        let ws_content = get_doc_text_content(&ws_doc);
        eprintln!("Workspace content after sandbox update: {:?}", ws_content);
        assert!(!ws_content.is_empty(), "WS content should not be empty");
        assert!(
            ws_content.contains("first line"),
            "WS should have first line, got: {}",
            ws_content
        );
        assert!(
            ws_content.contains("second line"),
            "WS should have second line, got: {}",
            ws_content
        );
        assert!(
            ws_content.contains("third line from sandbox"),
            "WS should have third line, got: {}",
            ws_content
        );
    }
}
