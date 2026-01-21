//! CRDT merge logic for peer sync.
//!
//! This module implements the "receive" side of CRDT peer sync:
//! - Handle incoming MQTT commits
//! - Detect divergence between local and remote state
//! - Create merge commits when needed
//! - Update state tracking (head_cid and local_head_cid)
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use crate::commit::Commit;
use crate::mqtt::{EditMessage, MqttClient, Topic};
use crate::sync::crdt_state::CrdtPeerState;
use base64::{engine::general_purpose::STANDARD, Engine};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};
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
        /// The merge commit CID
        merge_cid: String,
        /// The remote commit that triggered the merge
        remote_cid: String,
    },
    /// Commit was applied but we can't write to disk (our local is ahead)
    LocalAhead,
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
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    edit_msg: &EditMessage,
    author: &str,
) -> Result<(MergeResult, Option<String>), String> {
    // Compute CID for the received commit
    let commit = Commit::new(
        edit_msg.parents.clone(),
        edit_msg.update.clone(),
        edit_msg.author.clone(),
        edit_msg.message.clone(),
    );
    let received_cid = commit.calculate_cid();

    // Check if we already know this commit
    if state.is_cid_known(&received_cid) {
        debug!("Commit {} already known, skipping", received_cid);
        return Ok((MergeResult::AlreadyKnown, None));
    }

    // Decode the update
    let update_bytes = STANDARD
        .decode(&edit_msg.update)
        .map_err(|e| format!("Failed to decode update: {}", e))?;

    let update = Update::decode_v1(&update_bytes)
        .map_err(|e| format!("Failed to decode Yjs update: {}", e))?;

    // Load current Y.Doc
    let doc = state.to_doc()?;

    // Apply the update
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    // Determine merge strategy
    let result = determine_merge_strategy(state, &received_cid, &edit_msg.parents);

    match &result {
        MergeResult::AlreadyKnown => {
            // Shouldn't happen here, but handle gracefully
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
            info!("Fast-forward to commit {}", new_head);
        }
        MergeResult::Merged {
            merge_cid,
            remote_cid,
        } => {
            // We had local changes, need to create a merge commit
            if let Some(mqtt) = mqtt_client {
                // Create merge commit with two parents
                let merge_update = create_merge_update(&doc)?;
                let local_cid = state.local_head_cid.clone().unwrap_or_default();

                publish_merge_commit(
                    mqtt,
                    workspace,
                    node_id,
                    &local_cid,
                    remote_cid,
                    &merge_update,
                    author,
                )
                .await?;
            }

            state.head_cid = Some(merge_cid.clone());
            state.local_head_cid = Some(merge_cid.clone());
            state.update_from_doc(&doc);
            info!(
                "Created merge commit {} (local + remote {})",
                merge_cid, remote_cid
            );
        }
        MergeResult::LocalAhead => {
            // We have local changes that aren't on the server yet
            // Apply the remote update to our doc but don't update local_head_cid
            state.head_cid = Some(received_cid.clone());
            state.update_from_doc(&doc);
            debug!("Applied remote commit {} but local is ahead", received_cid);
        }
    }

    // Get the current content for writing to disk
    let content = get_doc_text_content(&doc);

    // Only return content if we should write to disk
    let should_write = matches!(
        result,
        MergeResult::FastForward { .. } | MergeResult::Merged { .. }
    );

    Ok((result, if should_write { Some(content) } else { None }))
}

/// Determine the merge strategy based on current state and received commit.
fn determine_merge_strategy(
    state: &CrdtPeerState,
    received_cid: &str,
    received_parents: &[String],
) -> MergeResult {
    let local_head = state.local_head_cid.as_deref();
    let server_head = state.head_cid.as_deref();

    // Case 1: No local state - this is a fast-forward
    if local_head.is_none() && server_head.is_none() {
        return MergeResult::FastForward {
            new_head: received_cid.to_string(),
        };
    }

    // Case 2: Received commit is descendant of both our heads - fast-forward
    if is_ancestor(&local_head.map(String::from), received_parents)
        && is_ancestor(&server_head.map(String::from), received_parents)
    {
        return MergeResult::FastForward {
            new_head: received_cid.to_string(),
        };
    }

    // Case 3: Our local_head equals server_head - we're in sync, fast-forward
    if local_head == server_head {
        return MergeResult::FastForward {
            new_head: received_cid.to_string(),
        };
    }

    // Case 4: We have local changes not on server
    if let Some(local) = local_head {
        if Some(local) != server_head {
            // Check if received commit is based on server_head
            if is_ancestor(&server_head.map(String::from), received_parents) {
                // Server moved forward, we need to merge
                let merge_cid = compute_merge_cid(local, received_cid);
                return MergeResult::Merged {
                    merge_cid,
                    remote_cid: received_cid.to_string(),
                };
            }
        }
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

/// Compute a CID for a merge commit.
fn compute_merge_cid(local_cid: &str, remote_cid: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(b"merge:");
    hasher.update(local_cid.as_bytes());
    hasher.update(b":");
    hasher.update(remote_cid.as_bytes());
    hex::encode(hasher.finalize())
}

/// Create a Yjs update representing the current merged state.
fn create_merge_update(doc: &Doc) -> Result<Vec<u8>, String> {
    let txn = doc.transact();
    Ok(txn.encode_state_as_update_v1(&yrs::StateVector::default()))
}

/// Publish a merge commit via MQTT.
async fn publish_merge_commit(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    node_id: &str,
    local_parent: &str,
    remote_parent: &str,
    update_bytes: &[u8],
    author: &str,
) -> Result<String, String> {
    let parents = vec![local_parent.to_string(), remote_parent.to_string()];
    let update_b64 = STANDARD.encode(update_bytes);

    let commit = Commit::new(
        parents.clone(),
        update_b64.clone(),
        author.to_string(),
        None,
    );
    let cid = commit.calculate_cid();

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: Some("Merge commit".to_string()),
        timestamp,
    };

    let topic = Topic::edits(workspace, node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)
        .map_err(|e| format!("Failed to serialize merge commit: {}", e))?;

    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("Failed to publish merge commit: {}", e))?;

    info!(
        "Published merge commit {} for {} (parents: {}, {})",
        cid, node_id, local_parent, remote_parent
    );

    Ok(cid)
}

/// Get text content from a Y.Doc.
fn get_doc_text_content(doc: &Doc) -> String {
    let txn = doc.transact();
    match txn.get_text("content") {
        Some(text) => text.get_string(&txn),
        None => String::new(),
    }
}

/// Parse an MQTT edit message payload.
pub fn parse_edit_message(payload: &[u8]) -> Result<EditMessage, String> {
    serde_json::from_slice(payload).map_err(|e| format!("Failed to parse edit message: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use yrs::{Text, WriteTxn};

    #[test]
    fn test_merge_result_local_ahead() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        // Only head_cid set, not local_head_cid - indicates we received a commit
        // but haven't made local changes based on it
        state.head_cid = Some("abc123".to_string());

        // Received commit with different parent - local is ahead
        let result = determine_merge_strategy(&state, "new_commit", &["other_parent".to_string()]);
        // This should be LocalAhead since local_head is None but head_cid is set
        assert!(matches!(result, MergeResult::LocalAhead));
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
        assert!(matches!(result, MergeResult::Merged { .. }));
    }

    #[test]
    fn test_compute_merge_cid() {
        let cid1 = compute_merge_cid("local123", "remote456");
        let cid2 = compute_merge_cid("local123", "remote456");
        assert_eq!(cid1, cid2); // Deterministic

        let cid3 = compute_merge_cid("different", "remote456");
        assert_ne!(cid1, cid3); // Different inputs = different output
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
        };
        let payload = serde_json::to_vec(&msg).unwrap();

        let parsed = parse_edit_message(&payload).unwrap();
        assert_eq!(parsed.update, "base64data");
        assert_eq!(parsed.parents, vec!["parent1".to_string()]);
        assert_eq!(parsed.author, "user");
    }
}
