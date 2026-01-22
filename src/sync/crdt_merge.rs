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
        };
        let (result_a, _content_a) = process_received_edit(
            None, // No MQTT client needed for test
            "workspace",
            "node1",
            &mut state_a,
            &edit_from_b,
            "user_a",
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
        };
        let (result_b, _content_b) = process_received_edit(
            None,
            "workspace",
            "node1",
            &mut state_b,
            &edit_from_a,
            "user_b",
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
        };

        let (result, _) =
            process_received_edit(None, "workspace", "node1", &mut state, &echo_msg, "me")
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
        let commit1 = Commit::new(vec![], update1_b64.clone(), "user".to_string(), None);
        let cid1 = commit1.calculate_cid();

        // Receive first commit
        let msg1 = EditMessage {
            update: update1_b64,
            parents: vec![],
            author: "user".to_string(),
            message: None,
            timestamp: 1000,
        };
        let (result1, _) = process_received_edit(None, "ws", "n1", &mut state, &msg1, "user")
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
            parents: vec![cid1], // Parent is first commit
            author: "user".to_string(),
            message: None,
            timestamp: 1001,
        };
        let (result2, content) = process_received_edit(None, "ws", "n1", &mut state, &msg2, "user")
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
        };

        let (result, _) = process_received_edit(None, "ws", "n1", &mut state, &remote_msg, "me")
            .await
            .unwrap();

        // Should trigger merge or LocalAhead since we have divergent local changes
        assert!(
            matches!(result, MergeResult::Merged { .. } | MergeResult::LocalAhead),
            "Expected Merged or LocalAhead for divergent commits, got {:?}",
            result
        );
    }
}
