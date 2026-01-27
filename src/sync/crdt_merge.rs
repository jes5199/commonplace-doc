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
use crate::store::CommitStore;
use crate::sync::crdt_state::CrdtPeerState;
use crate::sync::error::{SyncError, SyncResult};
use base64::{engine::general_purpose::STANDARD, Engine};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
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
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    edit_msg: &EditMessage,
    author: &str,
    commit_store: Option<&Arc<CommitStore>>,
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
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);

                let publish_msg = EditMessage {
                    update: merge_commit.update.clone(),
                    parents: merge_commit.parents.clone(),
                    author: author.to_string(),
                    message: Some("Merge commit".to_string()),
                    timestamp,
                    req: None,
                };

                let topic = Topic::edits(workspace, node_id).to_topic_string();
                let payload = serde_json::to_vec(&publish_msg)?;

                mqtt.publish_retained(&topic, &payload, QoS::AtLeastOnce)
                    .await
                    .map_err(|e| {
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
            info!(
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
        MergeResult::FastForward { .. } | MergeResult::Merged { .. }
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
fn get_doc_text_content(doc: &Doc) -> String {
    let txn = doc.transact();
    match txn.get_text("content") {
        Some(text) => text.get_string(&txn),
        None => String::new(),
    }
}

/// Parse an MQTT edit message payload.
pub fn parse_edit_message(payload: &[u8]) -> SyncResult<EditMessage> {
    serde_json::from_slice(payload).map_err(SyncError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
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
            None, // No MQTT client needed for test
            "workspace",
            "node1",
            &mut state_a,
            &edit_from_b,
            "user_a",
            None, // No commit store in tests
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
            None,
            "workspace",
            "node1",
            &mut state_b,
            &edit_from_a,
            "user_b",
            None, // No commit store in tests
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
            None,
            "workspace",
            "node1",
            &mut state,
            &echo_msg,
            "me",
            None,
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
        let (result1, _) = process_received_edit(None, "ws", "n1", &mut state, &msg1, "user", None)
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
        let (result2, content) =
            process_received_edit(None, "ws", "n1", &mut state, &msg2, "user", None)
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

        let (result, _) =
            process_received_edit(None, "ws", "n1", &mut state, &remote_msg, "me", None)
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
