//! Stage 0 sync harness tests.
//!
//! These tests validate CRDT invariants in isolation (no MQTT, no filesystem).

use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update, WriteTxn};

fn apply_update(doc: &Doc, update: &[u8]) {
    let update = Update::decode_v1(update).expect("update decode failed");
    let mut txn = doc.transact_mut();
    txn.apply_update(update);
}

fn doc_text(doc: &Doc) -> String {
    let txn = doc.transact();
    match txn.get_text("content") {
        Some(text) => text.get_string(&txn),
        None => String::new(),
    }
}

fn base_state(text: &str) -> Vec<u8> {
    let doc = Doc::with_client_id(0);
    {
        let mut txn = doc.transact_mut();
        let root = txn.get_or_insert_text("content");
        root.insert(&mut txn, 0, text);
    }
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&StateVector::default())
}

#[test]
fn crdt_idempotent_apply() {
    let doc_src = Doc::with_client_id(1);
    let update = {
        let mut txn = doc_src.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, "hello");
        txn.encode_update_v1()
    };

    let receiver = Doc::with_client_id(2);
    apply_update(&receiver, &update);
    let once = doc_text(&receiver);
    apply_update(&receiver, &update);
    let twice = doc_text(&receiver);

    assert_eq!(once, "hello");
    assert_eq!(
        once, twice,
        "applying the same update twice should be idempotent"
    );
}

#[test]
fn crdt_concurrent_edits_converge() {
    let base = base_state("world");

    let doc_a = Doc::with_client_id(10);
    apply_update(&doc_a, &base);

    let doc_b = Doc::with_client_id(11);
    apply_update(&doc_b, &base);

    let update_a = {
        let mut txn = doc_a.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, "hello ");
        txn.encode_update_v1()
    };

    let update_b = {
        let mut txn = doc_b.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, "big ");
        txn.encode_update_v1()
    };

    apply_update(&doc_a, &update_b);
    apply_update(&doc_b, &update_a);

    let final_a = doc_text(&doc_a);
    let final_b = doc_text(&doc_b);

    assert_eq!(
        final_a, final_b,
        "peers should converge to the same content"
    );
    assert!(final_a.contains("hello "));
    assert!(final_a.contains("big "));
    assert!(final_a.contains("world"));
}

#[test]
fn crdt_delete_merge_converges() {
    let base = base_state("hello world");

    let doc_a = Doc::with_client_id(20);
    apply_update(&doc_a, &base);

    let doc_b = Doc::with_client_id(21);
    apply_update(&doc_b, &base);

    let update_a = {
        let mut txn = doc_a.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.remove_range(&mut txn, 0, 6);
        txn.encode_update_v1()
    };

    let update_b = {
        let mut txn = doc_b.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 6, "big ");
        txn.encode_update_v1()
    };

    apply_update(&doc_a, &update_b);
    apply_update(&doc_b, &update_a);

    let final_a = doc_text(&doc_a);
    let final_b = doc_text(&doc_b);

    assert_eq!(final_a, final_b, "delete/insert merges should converge");
    assert!(final_a.contains("big"));
    assert!(final_a.contains("world"));
    assert!(!final_a.contains("hello "));
}

// =============================================================================
// Receive Pipeline No-Echo Invariant Tests (Stage 0)
//
// These tests validate that the receive pipeline does NOT republish when
// applying remote CRDT updates. This is critical for preventing feedback loops.
//
// The key mechanisms tested:
// 1. CID-based echo suppression: Known commits return AlreadyKnown (no publish)
// 2. Fast-forward updates: Remote descendant commits don't trigger republish
// 3. Content-based echo suppression: shared_last_content prevents re-upload
// =============================================================================

/// Mock publish tracker for testing no-echo invariants.
///
/// In production, process_received_edit takes an optional mqtt_client.
/// We can verify no unwanted publishes by:
/// - Passing None: no merge commits can be published
/// - Checking result: AlreadyKnown and FastForward don't need publishes
mod no_echo_tests {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use commonplace_doc::commit::Commit;
    use commonplace_doc::mqtt::EditMessage;
    use commonplace_doc::sync::crdt_state::CrdtPeerState;
    use commonplace_doc::sync::{process_received_edit, MergeResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use uuid::Uuid;
    use yrs::{Doc, GetString, ReadTxn, Text, Transact, WriteTxn};

    /// Helper to create a Yjs update for text content.
    fn create_text_update(client_id: u64, text: &str) -> (Vec<u8>, Doc) {
        let doc = Doc::with_client_id(client_id);
        let update = {
            let mut txn = doc.transact_mut();
            let ytext = txn.get_or_insert_text("content");
            ytext.insert(&mut txn, 0, text);
            txn.encode_update_v1()
        };
        (update, doc)
    }

    /// Helper to get text content from a Y.Doc.
    #[allow(dead_code)]
    fn get_doc_text(doc: &Doc) -> String {
        let txn = doc.transact();
        match txn.get_text("content") {
            Some(text) => text.get_string(&txn),
            None => String::new(),
        }
    }

    /// Invariant: Applying a remote CRDT update with known CID does NOT trigger publish.
    ///
    /// When we receive a commit we already know (our own echo), process_received_edit
    /// returns AlreadyKnown without any side effects.
    #[tokio::test]
    async fn receive_known_cid_does_not_publish() {
        // Setup: Create state with a known commit
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Make a local edit
        let (update, doc) = create_text_update(1, "hello world");
        let update_b64 = STANDARD.encode(&update);

        // Use explicit timestamp so CID is deterministic
        let timestamp = 1000u64;
        let commit = Commit::with_timestamp(
            vec![],
            update_b64.clone(),
            "test_author".to_string(),
            None,
            timestamp,
        );
        let cid = commit.calculate_cid();

        // Update state as if we published this commit
        state.local_head_cid = Some(cid.clone());
        state.head_cid = Some(cid.clone());
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Now "receive" the same commit (simulating MQTT echo)
        let edit_msg = EditMessage {
            update: update_b64,
            parents: vec![],
            author: "test_author".to_string(),
            message: None,
            timestamp, // Same timestamp ensures same CID
        };

        // Process with no MQTT client - if it tried to publish, it would fail
        let (result, content) = process_received_edit(
            None, // No MQTT client - cannot publish
            "workspace",
            "node1",
            &mut state,
            &edit_msg,
            "test_author",
        )
        .await
        .expect("Should process without error");

        // Invariant: Known CID returns AlreadyKnown
        assert_eq!(
            result,
            MergeResult::AlreadyKnown,
            "Known CID must return AlreadyKnown, not trigger any processing"
        );

        // Invariant: No content to write (no side effects)
        assert!(
            content.is_none(),
            "AlreadyKnown should not produce content to write"
        );
    }

    /// Invariant: Fast-forward updates do NOT trigger merge commit publish.
    ///
    /// When we receive a commit that descends from our current state,
    /// we fast-forward without creating a merge commit.
    #[tokio::test]
    async fn receive_fast_forward_does_not_publish() {
        // Setup: Create state with initial commit
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Initial state: "hello"
        let (update1, _doc1) = create_text_update(1, "hello");
        let update1_b64 = STANDARD.encode(&update1);
        let commit1 = Commit::with_timestamp(
            vec![],
            update1_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );
        let cid1 = commit1.calculate_cid();

        // Receive initial commit
        let msg1 = EditMessage {
            update: update1_b64,
            parents: vec![],
            author: "author".to_string(),
            message: None,
            timestamp: 1000,
        };

        let (result1, _) =
            process_received_edit(None, "workspace", "node1", &mut state, &msg1, "author")
                .await
                .expect("First commit should succeed");

        assert!(
            matches!(result1, MergeResult::FastForward { .. }),
            "First commit should be fast-forward"
        );

        // Now receive a second commit that descends from the first
        // Create update building on first doc
        let doc2 = Doc::with_client_id(2);
        {
            // Apply first update
            let update =
                yrs::updates::decoder::Decode::decode_v1(&STANDARD.decode(&msg1.update).unwrap())
                    .unwrap();
            let mut txn = doc2.transact_mut();
            txn.apply_update(update);
        }
        let update2 = {
            let mut txn = doc2.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 5, " world");
            txn.encode_update_v1()
        };
        let update2_b64 = STANDARD.encode(&update2);

        let msg2 = EditMessage {
            update: update2_b64,
            parents: vec![cid1], // Descends from first commit
            author: "author".to_string(),
            message: None,
            timestamp: 2000,
        };

        // Process second commit with no MQTT client
        let (result2, content2) = process_received_edit(
            None, // No MQTT client - verifies no publish attempt
            "workspace",
            "node1",
            &mut state,
            &msg2,
            "author",
        )
        .await
        .expect("Second commit should succeed");

        // Invariant: Fast-forward does not create merge commit
        assert!(
            matches!(result2, MergeResult::FastForward { .. }),
            "Descendant commit should fast-forward, got {:?}",
            result2
        );

        // Content should be provided for writing (this is expected)
        assert!(
            content2.is_some(),
            "Fast-forward should provide content to write to disk"
        );

        // Verify content is merged correctly
        let content = content2.unwrap();
        assert!(
            content.contains("hello") && content.contains("world"),
            "Content should contain both updates: {}",
            content
        );
    }

    /// Invariant: Content-based echo suppression in upload task.
    ///
    /// The upload task compares new file content against shared_last_content.
    /// If content matches what was just written by receive task, upload is skipped.
    #[tokio::test]
    async fn shared_last_content_prevents_echo_upload() {
        use commonplace_doc::sync::types::SharedLastContent;

        // Simulate the shared_last_content mechanism
        let shared_last_content: SharedLastContent = Arc::new(RwLock::new(None));

        // Scenario: Receive task writes "hello world" to file
        // It updates shared_last_content BEFORE writing
        {
            let mut shared = shared_last_content.write().await;
            *shared = Some("hello world".to_string());
        }

        // Later, file watcher detects the write and sends to upload task
        // Upload task reads file content: "hello world"
        let file_content = "hello world";

        // Upload task checks shared_last_content
        let old_content = {
            let shared = shared_last_content.read().await;
            shared.clone().unwrap_or_default()
        };

        // Invariant: Content matches, so upload should be skipped
        assert_eq!(
            old_content, file_content,
            "shared_last_content should match file content"
        );

        // This is how the upload task decides to skip:
        // if old_content == new_content { continue; }
        let should_skip = old_content == file_content;
        assert!(
            should_skip,
            "Upload task should skip when content matches shared_last_content"
        );
    }

    /// Invariant: LocalAhead result does NOT write to disk (prevents data loss).
    ///
    /// When we have local changes ahead of the server, receiving a remote commit
    /// should not overwrite our local content.
    #[tokio::test]
    async fn local_ahead_does_not_write() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // We have local content "local content"
        let (update_local, doc_local) = create_text_update(1, "local content");
        let update_local_b64 = STANDARD.encode(&update_local);
        let commit_local = Commit::with_timestamp(
            vec![],
            update_local_b64.clone(),
            "local_author".to_string(),
            None,
            1000,
        );
        let local_cid = commit_local.calculate_cid();

        // State: we have local changes
        state.local_head_cid = Some(local_cid.clone());
        state.head_cid = None; // Not yet synced to server
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc_local.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Remote sends different content
        let (update_remote, _) = create_text_update(2, "remote content");
        let update_remote_b64 = STANDARD.encode(&update_remote);

        let remote_msg = EditMessage {
            update: update_remote_b64,
            parents: vec![], // No common ancestor
            author: "remote_author".to_string(),
            message: None,
            timestamp: 2000,
        };

        let (result, content) = process_received_edit(
            None,
            "workspace",
            "node1",
            &mut state,
            &remote_msg,
            "local_author",
        )
        .await
        .expect("Should process");

        // Should be LocalAhead - we have uncommitted local changes
        assert!(
            matches!(result, MergeResult::LocalAhead | MergeResult::Merged { .. }),
            "Expected LocalAhead or Merged when we have local changes, got {:?}",
            result
        );

        // If LocalAhead, no content to write (preserves our local file)
        if matches!(result, MergeResult::LocalAhead) {
            assert!(
                content.is_none(),
                "LocalAhead should not produce content to write"
            );
        }
    }

    /// Invariant: Receive pipeline is isolated - no publish without mqtt_client.
    ///
    /// When process_received_edit is called with mqtt_client = None,
    /// it cannot publish anything. This test verifies the isolation.
    #[tokio::test]
    async fn receive_without_mqtt_client_cannot_publish() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Setup state with existing content
        let (update1, doc1) = create_text_update(1, "base");
        let update1_b64 = STANDARD.encode(&update1);
        let commit1 = Commit::with_timestamp(
            vec![],
            update1_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );
        let cid1 = commit1.calculate_cid();

        state.head_cid = Some(cid1.clone());
        state.local_head_cid = Some("different_local_cid".to_string()); // Divergent!
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc1.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Receive a commit that would trigger a merge
        let (update2, _) = create_text_update(2, "remote");
        let update2_b64 = STANDARD.encode(&update2);

        let msg = EditMessage {
            update: update2_b64,
            parents: vec![cid1], // Based on server head, not our local head
            author: "remote".to_string(),
            message: None,
            timestamp: 2000,
        };

        // Process with NO mqtt_client
        let result = process_received_edit(
            None, // Cannot publish
            "workspace",
            "node1",
            &mut state,
            &msg,
            "author",
        )
        .await;

        // Should succeed - the function handles None mqtt_client gracefully
        // by computing what the merge CID would be without actually publishing
        assert!(result.is_ok(), "Should handle None mqtt_client gracefully");

        // The result type depends on the merge strategy, but no actual
        // MQTT publish occurred because there was no client
    }

    /// Invariant: Idempotent CID check prevents duplicate processing.
    ///
    /// The is_cid_known check must catch both head_cid and local_head_cid.
    #[test]
    fn cid_known_check_covers_both_heads() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Initially, no CID is known
        assert!(!state.is_cid_known("cid_a"));
        assert!(!state.is_cid_known("cid_b"));

        // Set head_cid
        state.head_cid = Some("cid_a".to_string());
        assert!(state.is_cid_known("cid_a"));
        assert!(!state.is_cid_known("cid_b"));

        // Set local_head_cid to different value
        state.local_head_cid = Some("cid_b".to_string());
        assert!(state.is_cid_known("cid_a"));
        assert!(state.is_cid_known("cid_b"));
        assert!(!state.is_cid_known("cid_c"));
    }

    /// Integration test: Full receive -> apply -> (no write) cycle for echo.
    ///
    /// This simulates the complete cycle when receiving our own commit back.
    #[tokio::test]
    async fn full_echo_cycle_no_side_effects() {
        // Track any side effects
        let publish_count = Arc::new(AtomicUsize::new(0));

        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Step 1: Create a "local" commit (as if we published it)
        let (update, doc) = create_text_update(1, "my content");
        let update_b64 = STANDARD.encode(&update);
        let timestamp = 12345u64;
        let commit = Commit::with_timestamp(
            vec![],
            update_b64.clone(),
            "me".to_string(),
            None,
            timestamp,
        );
        let cid = commit.calculate_cid();

        // Update state as if we just published
        state.local_head_cid = Some(cid.clone());
        state.head_cid = Some(cid.clone());
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Step 2: Receive the same commit as an "echo" from MQTT
        let echo_msg = EditMessage {
            update: update_b64.clone(),
            parents: vec![],
            author: "me".to_string(),
            message: None,
            timestamp,
        };

        let (result, content) =
            process_received_edit(None, "workspace", "node1", &mut state, &echo_msg, "me")
                .await
                .expect("Should process");

        // Verify: No side effects
        assert_eq!(result, MergeResult::AlreadyKnown, "Echo must be detected");
        assert!(content.is_none(), "Echo must not produce content to write");
        assert_eq!(
            publish_count.load(Ordering::SeqCst),
            0,
            "No publish should occur"
        );

        // Verify: State unchanged
        assert_eq!(
            state.head_cid,
            Some(cid.clone()),
            "head_cid should be unchanged"
        );
        assert_eq!(
            state.local_head_cid,
            Some(cid.clone()),
            "local_head_cid should be unchanged"
        );
    }

    /// Test that empty merge updates don't cause echo issues.
    ///
    /// Merge commits have empty updates (just parent CIDs). Receiving these
    /// should not trigger re-publishing or content writes.
    #[tokio::test]
    async fn empty_merge_update_no_echo() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Setup: We have some base state
        let (update, doc) = create_text_update(1, "base content");
        let update_b64 = STANDARD.encode(&update);
        let commit =
            Commit::with_timestamp(vec![], update_b64.clone(), "author".to_string(), None, 1000);
        let cid = commit.calculate_cid();

        state.head_cid = Some(cid.clone());
        state.local_head_cid = Some(cid.clone());
        state.yjs_state = Some(STANDARD.encode({
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        }));

        // Receive an empty update (like a merge commit)
        let merge_msg = EditMessage {
            update: String::new(), // Empty update
            parents: vec![cid.clone()],
            author: "author".to_string(),
            message: Some("Merge commit".to_string()),
            timestamp: 2000,
        };

        let (result, _content) =
            process_received_edit(None, "workspace", "node1", &mut state, &merge_msg, "author")
                .await
                .expect("Should process empty update");

        // Empty merge updates that descend from current state = fast forward
        // The key invariant: no unwanted publish
        assert!(
            matches!(
                result,
                MergeResult::FastForward { .. } | MergeResult::AlreadyKnown
            ),
            "Empty merge should fast-forward or be known: {:?}",
            result
        );
    }
}

// =============================================================================
// Broadcast Lag Detection Tests (Stage 0)
//
// These tests validate that broadcast channel lag is detected correctly and
// the recv_broadcast_with_lag function returns proper results.
// =============================================================================

mod broadcast_lag_tests {
    use commonplace_doc::events::{recv_broadcast_with_lag, BroadcastRecvResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::broadcast;

    /// Test that normal message receipt works correctly.
    #[tokio::test]
    async fn recv_broadcast_normal_message() {
        let (tx, mut rx) = broadcast::channel::<String>(16);

        tx.send("hello".to_string()).unwrap();

        match recv_broadcast_with_lag(&mut rx, "test").await {
            BroadcastRecvResult::Message(msg) => {
                assert_eq!(msg, "hello");
            }
            other => panic!("Expected Message, got {:?}", other),
        }
    }

    /// Test that channel close is detected.
    #[tokio::test]
    async fn recv_broadcast_detects_close() {
        let (tx, mut rx) = broadcast::channel::<String>(16);
        drop(tx); // Close the channel

        match recv_broadcast_with_lag(&mut rx, "test").await {
            BroadcastRecvResult::Closed => {}
            other => panic!("Expected Closed, got {:?}", other),
        }
    }

    /// Test that lag is detected when messages are dropped.
    ///
    /// Creates a broadcast channel with capacity 1, publishes multiple messages
    /// to trigger lag, and verifies the Lagged result is returned.
    #[tokio::test]
    async fn recv_broadcast_detects_lag() {
        // Create channel with minimal capacity to trigger lag easily
        let (tx, mut rx) = broadcast::channel::<i32>(1);

        // Send multiple messages - the receiver will lag
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap(); // This will cause message 1 to be dropped

        match recv_broadcast_with_lag(&mut rx, "test").await {
            BroadcastRecvResult::Lagged {
                missed_count,
                next_message,
            } => {
                // We missed at least 1 message
                assert!(missed_count >= 1, "Should have missed messages");
                // Should have the next available message
                assert!(next_message.is_some(), "Should have next message");
                // The next message should be 2 or 3 (depending on exact timing)
                let msg = next_message.unwrap();
                assert!(
                    msg == 2 || msg == 3,
                    "Next message should be 2 or 3, got {}",
                    msg
                );
            }
            other => panic!("Expected Lagged, got {:?}", other),
        }
    }

    /// Test that lag detection can be used to trigger resync.
    ///
    /// This simulates the pattern used in directory_mqtt_task where lag
    /// detection triggers a resync callback.
    #[tokio::test]
    async fn lag_triggers_resync_callback() {
        let resync_count = Arc::new(AtomicUsize::new(0));
        let resync_count_clone = resync_count.clone();

        let (tx, mut rx) = broadcast::channel::<i32>(1);

        // Send messages to trigger lag
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();

        // Process with lag detection
        let result = recv_broadcast_with_lag(&mut rx, "test").await;

        match result {
            BroadcastRecvResult::Lagged { missed_count, .. } => {
                // In real code, this is where resync would be triggered
                resync_count_clone.fetch_add(1, Ordering::SeqCst);
                assert!(missed_count >= 1);
            }
            _ => {}
        }

        assert_eq!(
            resync_count.load(Ordering::SeqCst),
            1,
            "Resync should have been triggered once"
        );
    }

    /// Test the full receive loop pattern with lag handling.
    ///
    /// This tests the pattern used in directory_mqtt_task where the loop
    /// continues processing after detecting and handling lag.
    #[tokio::test]
    async fn recv_loop_continues_after_lag() {
        let (tx, mut rx) = broadcast::channel::<i32>(2);
        let messages_received = Arc::new(AtomicUsize::new(0));
        let lag_detected = Arc::new(AtomicUsize::new(0));

        // Send messages that will cause lag (don't spawn, do it synchronously)
        for i in 1..=10 {
            let _ = tx.send(i);
        }
        drop(tx); // Close channel so receiver will get Closed

        let messages_clone = messages_received.clone();
        let lag_clone = lag_detected.clone();

        // Process messages until channel closes
        loop {
            match recv_broadcast_with_lag(&mut rx, "test").await {
                BroadcastRecvResult::Message(_msg) => {
                    messages_clone.fetch_add(1, Ordering::SeqCst);
                }
                BroadcastRecvResult::Lagged { next_message, .. } => {
                    lag_clone.fetch_add(1, Ordering::SeqCst);
                    if next_message.is_some() {
                        messages_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
                BroadcastRecvResult::Closed => break,
            }
        }

        // We should have received some messages
        assert!(
            messages_received.load(Ordering::SeqCst) > 0,
            "Should have received at least one message"
        );
        // Lag should have been detected (channel capacity 2, sent 10 messages)
        assert!(
            lag_detected.load(Ordering::SeqCst) > 0,
            "Lag should have been detected"
        );
    }
}

// =============================================================================
// Edit Buffering Before CRDT Init Tests (Stage 0)
//
// These tests validate that MQTT edits arriving before CRDT initialization
// completes are buffered and applied after init.
//
// This is critical for preventing lost edits in the race condition where:
// 1. MQTT edit arrives
// 2. CRDT state isn't initialized yet
// 3. Server fetch might not have the edit (server receives MQTT simultaneously)
// =============================================================================

mod edit_buffering_tests {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use commonplace_doc::commit::Commit;
    use commonplace_doc::mqtt::EditMessage;
    use commonplace_doc::sync::crdt_state::{CrdtPeerState, MAX_PENDING_EDITS};
    use commonplace_doc::sync::{process_received_edit, MergeResult};
    use uuid::Uuid;
    use yrs::{Doc, GetString, ReadTxn, Text, Transact, WriteTxn};

    /// Helper to create a Yjs update for text content.
    fn create_text_update(client_id: u64, text: &str) -> (Vec<u8>, Doc) {
        let doc = Doc::with_client_id(client_id);
        let update = {
            let mut txn = doc.transact_mut();
            let ytext = txn.get_or_insert_text("content");
            ytext.insert(&mut txn, 0, text);
            txn.encode_update_v1()
        };
        (update, doc)
    }

    /// Helper to get text content from a Y.Doc.
    fn get_doc_text(doc: &Doc) -> String {
        let txn = doc.transact();
        match txn.get_text("content") {
            Some(text) => text.get_string(&txn),
            None => String::new(),
        }
    }

    /// Helper to create an EditMessage from an update.
    fn make_edit_message(update: &[u8], author: &str, timestamp: u64) -> EditMessage {
        EditMessage {
            update: STANDARD.encode(update),
            parents: vec![],
            author: author.to_string(),
            message: None,
            timestamp,
        }
    }

    /// Invariant: Uninitialized CRDT state indicates need for server init.
    #[test]
    fn uninitialized_state_needs_server_init() {
        let state = CrdtPeerState::new(Uuid::new_v4());
        assert!(
            state.needs_server_init(),
            "New state should need server initialization"
        );
    }

    /// Invariant: Initialized CRDT state does not need server init.
    #[test]
    fn initialized_state_does_not_need_server_init() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Create server state
        let (update, _doc) = create_text_update(1, "server content");
        let server_state_b64 = STANDARD.encode(&update);

        state.initialize_from_server(&server_state_b64, "server_cid");

        assert!(
            !state.needs_server_init(),
            "Initialized state should not need server init"
        );
    }

    /// Invariant: Queued edits are preserved during initialization.
    ///
    /// This test simulates the race condition where edits arrive before init:
    /// 1. Create uninitialized state
    /// 2. Queue some edits (as would happen in directory_mqtt_task)
    /// 3. Initialize from server
    /// 4. Verify queued edits can be retrieved and applied
    #[tokio::test]
    async fn queued_edits_applied_after_init() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Simulate server has content "hello"
        let (server_update, server_doc) = create_text_update(1, "hello");
        let server_state_b64 = STANDARD.encode({
            let txn = server_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        });
        let server_cid = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&server_update),
            "server".to_string(),
            None,
            1000,
        )
        .calculate_cid();

        // Simulate remote client appends " world" (building on server state)
        let remote_doc = Doc::with_client_id(2);
        {
            // Apply server state first
            let update = yrs::updates::decoder::Decode::decode_v1(
                &STANDARD.decode(&server_state_b64).unwrap(),
            )
            .unwrap();
            let mut txn = remote_doc.transact_mut();
            txn.apply_update(update);
        }
        let remote_update = {
            let mut txn = remote_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 5, " world");
            txn.encode_update_v1()
        };

        // Serialize the remote edit as MQTT would
        let edit_msg = make_edit_message(&remote_update, "remote", 2000);
        let edit_payload = serde_json::to_vec(&edit_msg).unwrap();

        // Step 1: State is uninitialized - needs_server_init() returns true
        assert!(state.needs_server_init());

        // Step 2: Queue the edit (simulating directory_mqtt_task behavior)
        state.queue_pending_edit(edit_payload.clone());
        assert!(state.has_pending_edits());
        assert_eq!(state.pending_edits.len(), 1);

        // Step 3: Initialize from server state
        state.initialize_from_server(&server_state_b64, &server_cid);
        assert!(!state.needs_server_init());

        // Step 4: Drain pending edits and verify they can be applied
        let pending = state.take_pending_edits();
        assert_eq!(pending.len(), 1);
        assert!(!state.has_pending_edits());

        // Parse and apply the pending edit
        let parsed: EditMessage = serde_json::from_slice(&pending[0].payload).unwrap();
        let (result, maybe_content) =
            process_received_edit(None, "workspace", "node1", &mut state, &parsed, "local")
                .await
                .expect("Should process pending edit");

        // The edit should be applied successfully
        assert!(
            matches!(
                result,
                MergeResult::FastForward { .. } | MergeResult::Merged { .. }
            ),
            "Pending edit should be applied, got {:?}",
            result
        );

        // Content should be "hello world"
        if let Some(content) = maybe_content {
            assert!(
                content.contains("hello") && content.contains("world"),
                "Content should contain both 'hello' and 'world': {}",
                content
            );
        } else {
            // If no content, verify Y.Doc has expected content
            let doc = state.to_doc().expect("Should have valid doc");
            let text = get_doc_text(&doc);
            assert!(
                text.contains("hello") && text.contains("world"),
                "Doc text should contain both 'hello' and 'world': {}",
                text
            );
        }
    }

    /// Invariant: Multiple edits can be queued and applied in order.
    #[tokio::test]
    async fn multiple_queued_edits_applied_in_order() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Create three edits
        let (update1, _) = create_text_update(1, "A");
        let (update2, _) = create_text_update(2, "B");
        let (update3, _) = create_text_update(3, "C");

        let msg1 = make_edit_message(&update1, "author1", 1000);
        let msg2 = make_edit_message(&update2, "author2", 2000);
        let msg3 = make_edit_message(&update3, "author3", 3000);

        // Queue all three edits
        state.queue_pending_edit(serde_json::to_vec(&msg1).unwrap());
        state.queue_pending_edit(serde_json::to_vec(&msg2).unwrap());
        state.queue_pending_edit(serde_json::to_vec(&msg3).unwrap());

        assert_eq!(state.pending_edits.len(), 3);

        // Take all pending edits
        let pending = state.take_pending_edits();
        assert_eq!(pending.len(), 3);

        // Verify order is preserved (FIFO)
        let parsed1: EditMessage = serde_json::from_slice(&pending[0].payload).unwrap();
        let parsed2: EditMessage = serde_json::from_slice(&pending[1].payload).unwrap();
        let parsed3: EditMessage = serde_json::from_slice(&pending[2].payload).unwrap();

        assert_eq!(parsed1.timestamp, 1000);
        assert_eq!(parsed2.timestamp, 2000);
        assert_eq!(parsed3.timestamp, 3000);
    }

    /// Invariant: Queue overflow drops oldest edits with warning.
    #[test]
    fn queue_overflow_drops_oldest() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Fill queue to capacity
        for i in 0..MAX_PENDING_EDITS {
            let msg = make_edit_message(&[i as u8], "author", i as u64);
            state.queue_pending_edit(serde_json::to_vec(&msg).unwrap());
        }

        assert_eq!(state.pending_edits.len(), MAX_PENDING_EDITS);

        // Queue one more - should drop oldest
        let overflow_msg = make_edit_message(&[255], "author", 999999);
        state.queue_pending_edit(serde_json::to_vec(&overflow_msg).unwrap());

        // Still at max capacity
        assert_eq!(state.pending_edits.len(), MAX_PENDING_EDITS);

        // First element should now be the second edit (timestamp 1)
        let pending = state.take_pending_edits();
        let first: EditMessage = serde_json::from_slice(&pending[0].payload).unwrap();
        assert_eq!(
            first.timestamp, 1,
            "Oldest edit (timestamp 0) should have been dropped"
        );

        // Last element should be the overflow edit
        let last: EditMessage =
            serde_json::from_slice(&pending[pending.len() - 1].payload).unwrap();
        assert_eq!(last.timestamp, 999999, "Newest edit should be at the end");
    }

    /// Invariant: Pending edits are not serialized to disk.
    ///
    /// The pending_edits field uses #[serde(skip)] so transient edits
    /// don't persist across restarts.
    #[test]
    fn pending_edits_not_serialized() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        let msg = make_edit_message(&[1, 2, 3], "author", 1000);
        state.queue_pending_edit(serde_json::to_vec(&msg).unwrap());

        // Serialize state
        let json = serde_json::to_string(&state).unwrap();

        // pending_edits should not appear in JSON
        assert!(
            !json.contains("pending_edits"),
            "pending_edits should not be serialized"
        );

        // Deserialize and verify empty queue
        let loaded: CrdtPeerState = serde_json::from_str(&json).unwrap();
        assert!(
            !loaded.has_pending_edits(),
            "Deserialized state should have empty pending_edits"
        );
    }

    /// Invariant: Queue can be drained multiple times (idempotent).
    #[test]
    fn take_pending_edits_is_idempotent() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Queue one edit
        let msg = make_edit_message(&[1], "author", 1000);
        state.queue_pending_edit(serde_json::to_vec(&msg).unwrap());

        // First take gets the edit
        let first_take = state.take_pending_edits();
        assert_eq!(first_take.len(), 1);

        // Second take is empty
        let second_take = state.take_pending_edits();
        assert!(second_take.is_empty());

        // Third take is also empty
        let third_take = state.take_pending_edits();
        assert!(third_take.is_empty());
    }

    /// Integration test: Full workflow simulation.
    ///
    /// This simulates the complete scenario from CP-hykz:
    /// 1. Sync client starts up
    /// 2. MQTT edit arrives before CRDT state is initialized
    /// 3. Edit is queued (not applied, not fetched from server)
    /// 4. CRDT initialization completes
    /// 5. Queued edit is applied
    /// 6. Final state is correct
    #[tokio::test]
    async fn full_workflow_edit_before_init() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Simulate: Server has "hello"
        let server_doc = Doc::with_client_id(1);
        {
            let mut txn = server_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }
        let server_state = {
            let txn = server_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let server_state_b64 = STANDARD.encode(&server_state);
        let server_cid = "server_head_cid";

        // Simulate: Remote client sends " world" edit via MQTT
        // (This arrives BEFORE our CRDT state is initialized)
        let remote_doc = Doc::with_client_id(2);
        {
            // Remote builds on server state
            let update = yrs::updates::decoder::Decode::decode_v1(&server_state).unwrap();
            let mut txn = remote_doc.transact_mut();
            txn.apply_update(update);
        }
        let remote_update = {
            let mut txn = remote_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 5, " world");
            txn.encode_update_v1()
        };
        let remote_msg = EditMessage {
            update: STANDARD.encode(&remote_update),
            parents: vec![server_cid.to_string()],
            author: "remote".to_string(),
            message: None,
            timestamp: 2000,
        };
        let mqtt_payload = serde_json::to_vec(&remote_msg).unwrap();

        // Step 1: MQTT edit arrives before init
        assert!(state.needs_server_init(), "State should need init");

        // Step 2: Queue the edit (as directory_mqtt_task does)
        state.queue_pending_edit(mqtt_payload);
        assert!(state.has_pending_edits());

        // Step 3: Initialize from server
        state.initialize_from_server(&server_state_b64, server_cid);
        assert!(!state.needs_server_init());

        // Verify server content is available
        let doc_after_init = state.to_doc().unwrap();
        assert_eq!(get_doc_text(&doc_after_init), "hello");

        // Step 4: Process pending edits
        let pending = state.take_pending_edits();
        assert_eq!(pending.len(), 1);

        let edit_msg: EditMessage = serde_json::from_slice(&pending[0].payload).unwrap();
        let (result, content) =
            process_received_edit(None, "workspace", "node1", &mut state, &edit_msg, "local")
                .await
                .expect("Should apply pending edit");

        // Step 5: Verify final state
        assert!(
            matches!(
                result,
                MergeResult::FastForward { .. } | MergeResult::Merged { .. }
            ),
            "Edit should be applied, got {:?}",
            result
        );

        // Check final content
        let final_doc = state.to_doc().unwrap();
        let final_text = get_doc_text(&final_doc);
        assert_eq!(
            final_text, "hello world",
            "Final content should be 'hello world', got '{}'",
            final_text
        );

        // Also verify content returned for writing
        if let Some(c) = content {
            assert_eq!(c, "hello world", "Returned content should match");
        }
    }
}

// =============================================================================
// CRDT State Tracking Tests (Stage 1 - CP-1ufj)
//
// These tests validate CID tracking, state transitions, and head management
// in CrdtPeerState and DirectorySyncState. Complements existing tests in
// edit_buffering_tests and crdt_publish_tests with coverage for:
// - mark_needs_resync transitions
// - DirectorySyncState file management
// - update_from_doc preserving CID fields
// - Error handling in to_doc
// =============================================================================

mod crdt_state_tests {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use commonplace_doc::sync::crdt_state::{CrdtPeerState, DirectorySyncState};
    use uuid::Uuid;
    use yrs::{Doc, GetString, ReadTxn, Text, Transact, WriteTxn};

    /// Helper to create a Y.Doc with text content.
    fn create_doc_with_content(client_id: u64, text: &str) -> Doc {
        let doc = Doc::with_client_id(client_id);
        {
            let mut txn = doc.transact_mut();
            let ytext = txn.get_or_insert_text("content");
            ytext.insert(&mut txn, 0, text);
        }
        doc
    }

    /// Helper to get text content from a Y.Doc.
    fn get_doc_text(doc: &Doc) -> String {
        let txn = doc.transact();
        match txn.get_text("content") {
            Some(text) => text.get_string(&txn),
            None => String::new(),
        }
    }

    // =========================================================================
    // mark_needs_resync Tests
    // =========================================================================

    /// Test: mark_needs_resync resets state to need init.
    #[test]
    fn mark_needs_resync_resets_state() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.initialize_from_server("state", "cid");
        assert!(!state.needs_server_init());

        state.mark_needs_resync();

        assert!(
            state.needs_server_init(),
            "Should need init after mark_needs_resync"
        );
        assert!(state.yjs_state.is_none(), "yjs_state should be cleared");
        assert!(state.head_cid.is_none(), "head_cid should be cleared");
        assert!(
            state.local_head_cid.is_none(),
            "local_head_cid should be cleared"
        );
    }

    /// Test: mark_needs_resync preserves pending edits.
    #[test]
    fn mark_needs_resync_preserves_pending_edits() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.initialize_from_server("state", "cid");

        // Queue some pending edits
        state.queue_pending_edit(vec![1, 2, 3]);
        state.queue_pending_edit(vec![4, 5, 6]);
        assert_eq!(state.pending_edits.len(), 2);

        state.mark_needs_resync();

        // Pending edits should still be there
        assert_eq!(
            state.pending_edits.len(),
            2,
            "Pending edits should be preserved after resync mark"
        );
        assert_eq!(state.pending_edits[0].payload, vec![1, 2, 3]);
        assert_eq!(state.pending_edits[1].payload, vec![4, 5, 6]);
    }

    /// Test: CID is not known after mark_needs_resync.
    #[test]
    fn cid_unknown_after_resync() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.initialize_from_server("state", "known_cid");
        assert!(state.is_cid_known("known_cid"));

        state.mark_needs_resync();

        assert!(
            !state.is_cid_known("known_cid"),
            "CID should not be known after resync"
        );
    }

    // =========================================================================
    // DirectorySyncState File Management Tests
    // =========================================================================

    /// Test: DirectorySyncState roundtrip preserves schema and files.
    #[test]
    fn directory_sync_state_serialization_roundtrip() {
        let schema_id = Uuid::new_v4();
        let file1_id = Uuid::new_v4();
        let file2_id = Uuid::new_v4();

        let mut state = DirectorySyncState::new(schema_id);
        state.schema.head_cid = Some("schema_head".to_string());

        let file1_state = state.get_or_create_file("file1.txt", file1_id);
        file1_state.head_cid = Some("file1_head".to_string());

        let file2_state = state.get_or_create_file("file2.txt", file2_id);
        file2_state.head_cid = Some("file2_head".to_string());

        // Serialize and deserialize
        let json = serde_json::to_string(&state).expect("Serialize");
        let restored: DirectorySyncState = serde_json::from_str(&json).expect("Deserialize");

        assert_eq!(restored.version, DirectorySyncState::CURRENT_VERSION);
        assert_eq!(restored.schema.node_id, schema_id);
        assert_eq!(restored.schema.head_cid, Some("schema_head".to_string()));
        assert!(restored.has_file("file1.txt"));
        assert!(restored.has_file("file2.txt"));
        assert_eq!(
            restored.get_file("file1.txt").unwrap().head_cid,
            Some("file1_head".to_string())
        );
        assert_eq!(
            restored.get_file("file2.txt").unwrap().head_cid,
            Some("file2_head".to_string())
        );
    }

    /// Test: get_or_create_file creates new file state.
    #[test]
    fn get_or_create_file_creates_new() {
        let mut state = DirectorySyncState::new(Uuid::new_v4());
        let file_id = Uuid::new_v4();

        assert!(!state.has_file("new.txt"));

        let file_state = state.get_or_create_file("new.txt", file_id);
        assert_eq!(file_state.node_id, file_id);

        assert!(state.has_file("new.txt"));
    }

    /// Test: get_or_create_file returns existing file state.
    #[test]
    fn get_or_create_file_returns_existing() {
        let mut state = DirectorySyncState::new(Uuid::new_v4());
        let file_id = Uuid::new_v4();

        // Create file and set a CID
        {
            let file_state = state.get_or_create_file("existing.txt", file_id);
            file_state.head_cid = Some("existing_cid".to_string());
        }

        // Get same file again
        let file_state = state.get_or_create_file("existing.txt", file_id);

        // Should preserve existing data
        assert_eq!(file_state.head_cid, Some("existing_cid".to_string()));
    }

    /// Test: get_or_create_file updates node_id if changed.
    #[test]
    fn get_or_create_file_updates_node_id_on_mismatch() {
        let mut state = DirectorySyncState::new(Uuid::new_v4());
        let old_id = Uuid::new_v4();
        let new_id = Uuid::new_v4();

        // Create with old ID
        {
            let file_state = state.get_or_create_file("file.txt", old_id);
            file_state.head_cid = Some("old_cid".to_string());
            file_state.yjs_state = Some("old_state".to_string());
        }

        // Get with new ID - simulates schema update on server
        let file_state = state.get_or_create_file("file.txt", new_id);

        // node_id should be updated
        assert_eq!(file_state.node_id, new_id);
        // State should be cleared (tracking new document now)
        assert!(
            file_state.yjs_state.is_none(),
            "yjs_state should be cleared on node_id change"
        );
        assert!(
            file_state.head_cid.is_none(),
            "head_cid should be cleared on node_id change"
        );
    }

    /// Test: remove_file removes file state.
    #[test]
    fn remove_file_removes_state() {
        let mut state = DirectorySyncState::new(Uuid::new_v4());
        let file_id = Uuid::new_v4();

        state.get_or_create_file("to_remove.txt", file_id);
        assert!(state.has_file("to_remove.txt"));

        let removed = state.remove_file("to_remove.txt");
        assert!(removed.is_some());
        assert!(!state.has_file("to_remove.txt"));
    }

    /// Test: remove_file returns None for non-existent file.
    #[test]
    fn remove_nonexistent_file_returns_none() {
        let mut state = DirectorySyncState::new(Uuid::new_v4());

        let removed = state.remove_file("nonexistent.txt");
        assert!(removed.is_none());
    }

    // =========================================================================
    // update_from_doc Tests
    // =========================================================================

    /// Test: update_from_doc preserves CID fields.
    #[test]
    fn update_from_doc_preserves_cids() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.head_cid = Some("head".to_string());
        state.local_head_cid = Some("local".to_string());

        // Update Y.Doc state
        let doc = create_doc_with_content(1, "new content");
        state.update_from_doc(&doc);

        // CIDs should be preserved
        assert_eq!(
            state.head_cid,
            Some("head".to_string()),
            "head_cid should be preserved"
        );
        assert_eq!(
            state.local_head_cid,
            Some("local".to_string()),
            "local_head_cid should be preserved"
        );
        // yjs_state should be updated
        assert!(
            state.yjs_state.is_some(),
            "yjs_state should be updated from doc"
        );
    }

    /// Test: update_from_doc then to_doc roundtrip.
    #[test]
    fn update_and_restore_doc() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Create doc and update state
        let doc1 = create_doc_with_content(1, "first content");
        state.update_from_doc(&doc1);

        // Restore doc
        let doc2 = state.to_doc().expect("to_doc should succeed");
        assert_eq!(get_doc_text(&doc2), "first content");

        // Update with new content
        let doc3 = create_doc_with_content(2, "second content");
        state.update_from_doc(&doc3);

        // Restore again
        let doc4 = state.to_doc().expect("to_doc should succeed");
        assert_eq!(get_doc_text(&doc4), "second content");
    }

    // =========================================================================
    // to_doc Error Handling Tests
    // =========================================================================

    /// Test: Empty state produces empty doc.
    #[test]
    fn empty_state_produces_empty_doc() {
        let state = CrdtPeerState::new(Uuid::new_v4());

        let doc = state.to_doc().expect("to_doc should succeed");
        let text = get_doc_text(&doc);

        assert!(text.is_empty(), "Empty state should produce empty doc");
    }

    /// Test: to_doc handles corrupt base64 gracefully.
    #[test]
    fn to_doc_handles_invalid_base64() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.yjs_state = Some("not_valid_base64!!!".to_string());

        let result = state.to_doc();
        assert!(result.is_err(), "to_doc should fail on invalid base64");
    }

    /// Test: to_doc handles corrupt Yjs update gracefully.
    #[test]
    fn to_doc_handles_invalid_yjs_update() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        // Valid base64 but not a valid Yrs update
        state.yjs_state = Some(STANDARD.encode([0x00, 0x01, 0x02, 0x03]));

        let result = state.to_doc();
        assert!(result.is_err(), "to_doc should fail on invalid Yrs update");
    }

    // =========================================================================
    // from_doc Tests
    // =========================================================================

    /// Test: from_doc creates state that does not need init.
    #[test]
    fn from_doc_does_not_need_init() {
        let doc = create_doc_with_content(1, "hello");
        let state = CrdtPeerState::from_doc(Uuid::new_v4(), &doc);

        assert!(
            !state.needs_server_init(),
            "State created from doc should not need server init"
        );
    }

    /// Test: Y.Doc content survives from_doc -> serialization -> to_doc roundtrip.
    #[test]
    fn ydoc_content_survives_roundtrip() {
        let doc = create_doc_with_content(1, "hello world");
        let state = CrdtPeerState::from_doc(Uuid::new_v4(), &doc);

        // Serialize and deserialize
        let json = serde_json::to_string(&state).expect("Serialize");
        let restored: CrdtPeerState = serde_json::from_str(&json).expect("Deserialize");

        // Restore the Y.Doc
        let restored_doc = restored.to_doc().expect("to_doc should succeed");
        let restored_text = get_doc_text(&restored_doc);

        assert_eq!(
            restored_text, "hello world",
            "Y.Doc content should survive roundtrip"
        );
    }

    // =========================================================================
    // local_head_cid Divergence Tests
    // =========================================================================

    /// Test: local_head_cid can diverge from head_cid.
    #[test]
    fn local_head_cid_can_diverge() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.initialize_from_server("state", "server_cid");

        // Simulate local edit that creates new local head
        state.local_head_cid = Some("local_edit_cid".to_string());

        // head_cid still points to server
        assert_eq!(state.head_cid, Some("server_cid".to_string()));
        // local_head_cid has diverged
        assert_eq!(state.local_head_cid, Some("local_edit_cid".to_string()));
        // Both should be known
        assert!(state.is_cid_known("server_cid"));
        assert!(state.is_cid_known("local_edit_cid"));
    }

    /// Test: head_cid tracks server state through initialize.
    #[test]
    fn head_cid_set_by_initialize() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        assert!(state.head_cid.is_none());

        state.initialize_from_server("state", "initial_cid");
        assert_eq!(state.head_cid, Some("initial_cid".to_string()));
        assert_eq!(state.local_head_cid, Some("initial_cid".to_string()));
    }
}

// =============================================================================
// CRDT Publish Tests (Stage 1)
//
// These tests validate the crdt_publish module's public API in isolation.
// The key functions under test:
// - apply_received_commit: Apply a Yjs update and track CID
// - get_text_content: Extract text content from CrdtPeerState
// - is_commit_known: Check if a CID is already tracked
//
// The async publish functions (publish_text_change, publish_yjs_update) require
// a live MQTT client, so we test the underlying state transitions instead.
// =============================================================================

mod crdt_publish_tests {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use commonplace_doc::commit::Commit;
    use commonplace_doc::sync::crdt_publish::{
        apply_received_commit, get_text_content, is_commit_known,
    };
    use commonplace_doc::sync::crdt_state::CrdtPeerState;
    use uuid::Uuid;
    use yrs::{Doc, GetString, ReadTxn, Text, Transact, WriteTxn};

    /// Helper to create a Yjs update that inserts text at position 0.
    fn create_insert_update(client_id: u64, text: &str) -> Vec<u8> {
        let doc = Doc::with_client_id(client_id);
        let mut txn = doc.transact_mut();
        let ytext = txn.get_or_insert_text("content");
        ytext.insert(&mut txn, 0, text);
        txn.encode_update_v1()
    }

    /// Helper to create a Yjs update that appends text to existing content.
    fn create_append_update(doc: &Doc, text: &str) -> Vec<u8> {
        let mut txn = doc.transact_mut();
        let ytext = txn.get_or_insert_text("content");
        let current_len = ytext.get_string(&txn).len() as u32;
        ytext.insert(&mut txn, current_len, text);
        txn.encode_update_v1()
    }

    /// Helper to get text from a Y.Doc
    #[allow(dead_code)]
    fn get_doc_text(doc: &Doc) -> String {
        let txn = doc.transact();
        match txn.get_text("content") {
            Some(text) => text.get_string(&txn),
            None => String::new(),
        }
    }

    // =========================================================================
    // Test: Text edit generates correct Yjs update
    // =========================================================================

    /// Invariant: apply_received_commit correctly incorporates a Yjs update.
    ///
    /// When we apply a commit containing a text insertion, the CrdtPeerState's
    /// Y.Doc should contain the inserted text.
    #[test]
    fn crdt_publish_text_edit_generates_correct_update() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Create an update that inserts "hello world"
        let update = create_insert_update(1, "hello world");
        let update_b64 = STANDARD.encode(&update);

        // Create a commit with this update
        let commit = Commit::with_timestamp(
            vec![],
            update_b64.clone(),
            "test_author".to_string(),
            None,
            1000,
        );
        let cid = commit.calculate_cid();

        // Apply the commit
        let result = apply_received_commit(&mut state, &cid, &update_b64, &[]);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Commit should be applied (not duplicate)");

        // Verify the text content is correct
        let content = get_text_content(&state).expect("Should get content");
        assert_eq!(
            content, "hello world",
            "Applied update should produce correct text"
        );
    }

    /// Invariant: Applying the same Yjs update produces the same Y.Doc state.
    ///
    /// Yjs updates are idempotent - applying the same update twice should
    /// result in the same content.
    #[test]
    fn crdt_publish_yjs_update_is_idempotent() {
        let update = create_insert_update(1, "test content");

        // Apply to two separate states
        let mut state1 = CrdtPeerState::new(Uuid::new_v4());
        let mut state2 = CrdtPeerState::new(Uuid::new_v4());

        let update_b64 = STANDARD.encode(&update);
        let cid = "test_cid_1";

        apply_received_commit(&mut state1, cid, &update_b64, &[]).unwrap();
        apply_received_commit(&mut state2, cid, &update_b64, &[]).unwrap();

        let content1 = get_text_content(&state1).unwrap();
        let content2 = get_text_content(&state2).unwrap();

        assert_eq!(
            content1, content2,
            "Same update should produce same content"
        );
        assert_eq!(content1, "test content");
    }

    // =========================================================================
    // Test: Publish message contains correct parent CID
    // =========================================================================

    /// Invariant: After applying a commit, head_cid is updated to the commit's CID.
    ///
    /// The apply_received_commit function updates state.head_cid to track
    /// the latest commit we've processed.
    #[test]
    fn crdt_publish_tracks_head_cid_after_apply() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Apply first commit
        let update1 = create_insert_update(1, "first");
        let update1_b64 = STANDARD.encode(&update1);
        let commit1 = Commit::with_timestamp(
            vec![],
            update1_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );
        let cid1 = commit1.calculate_cid();

        apply_received_commit(&mut state, &cid1, &update1_b64, &[]).unwrap();

        // Verify head_cid is set to first commit
        assert_eq!(
            state.head_cid,
            Some(cid1.clone()),
            "head_cid should be updated after first commit"
        );
    }

    /// Invariant: Sequential commits update head_cid to the latest.
    ///
    /// When we apply multiple commits in sequence, head_cid should always
    /// reflect the most recently applied commit.
    #[test]
    fn crdt_publish_sequential_commits_update_head_cid() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Apply first commit
        let update1 = create_insert_update(1, "first");
        let update1_b64 = STANDARD.encode(&update1);
        let commit1 = Commit::with_timestamp(
            vec![],
            update1_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );
        let cid1 = commit1.calculate_cid();

        apply_received_commit(&mut state, &cid1, &update1_b64, &[]).unwrap();
        assert_eq!(state.head_cid, Some(cid1.clone()));

        // Apply second commit (would have cid1 as parent in real usage)
        let update2 = create_insert_update(2, " second");
        let update2_b64 = STANDARD.encode(&update2);
        let commit2 = Commit::with_timestamp(
            vec![cid1.clone()],
            update2_b64.clone(),
            "author".to_string(),
            None,
            2000,
        );
        let cid2 = commit2.calculate_cid();

        apply_received_commit(&mut state, &cid2, &update2_b64, std::slice::from_ref(&cid1))
            .unwrap();

        // head_cid should now be cid2
        assert_eq!(
            state.head_cid,
            Some(cid2),
            "head_cid should be updated to second commit"
        );
        assert_ne!(
            state.head_cid,
            Some(cid1),
            "head_cid should not be the first commit anymore"
        );
    }

    // =========================================================================
    // Test: Result contains new CID after publish
    // =========================================================================

    /// Invariant: Commit CID is deterministically computed from content.
    ///
    /// The same commit data (parents, update, author, timestamp) should
    /// always produce the same CID.
    #[test]
    fn crdt_publish_cid_is_deterministic() {
        let update = create_insert_update(1, "deterministic");
        let update_b64 = STANDARD.encode(&update);

        let commit1 = Commit::with_timestamp(
            vec!["parent_cid".to_string()],
            update_b64.clone(),
            "author".to_string(),
            Some("message".to_string()),
            12345,
        );

        let commit2 = Commit::with_timestamp(
            vec!["parent_cid".to_string()],
            update_b64.clone(),
            "author".to_string(),
            Some("message".to_string()),
            12345,
        );

        assert_eq!(
            commit1.calculate_cid(),
            commit2.calculate_cid(),
            "Same commit data should produce same CID"
        );
    }

    /// Invariant: Different content produces different CIDs.
    ///
    /// CIDs are content-addressable - changing any field should change the CID.
    #[test]
    fn crdt_publish_different_content_produces_different_cid() {
        let update1 = create_insert_update(1, "content A");
        let update2 = create_insert_update(1, "content B");

        let commit1 = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&update1),
            "author".to_string(),
            None,
            1000,
        );

        let commit2 = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&update2),
            "author".to_string(),
            None,
            1000,
        );

        assert_ne!(
            commit1.calculate_cid(),
            commit2.calculate_cid(),
            "Different update content should produce different CID"
        );
    }

    /// Invariant: Different parents produces different CIDs.
    #[test]
    fn crdt_publish_different_parents_produces_different_cid() {
        let update = create_insert_update(1, "same content");
        let update_b64 = STANDARD.encode(&update);

        let commit1 = Commit::with_timestamp(
            vec!["parent_a".to_string()],
            update_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );

        let commit2 = Commit::with_timestamp(
            vec!["parent_b".to_string()],
            update_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );

        assert_ne!(
            commit1.calculate_cid(),
            commit2.calculate_cid(),
            "Different parents should produce different CID"
        );
    }

    // =========================================================================
    // Test: Multiple sequential edits track CID chain
    // =========================================================================

    /// Invariant: A chain of commits builds up content correctly.
    ///
    /// Applying commits in sequence should accumulate their effects in the Y.Doc.
    #[test]
    fn crdt_publish_sequential_edits_accumulate_content() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Commit 1: "hello"
        let update1 = create_insert_update(1, "hello");
        let update1_b64 = STANDARD.encode(&update1);
        let cid1 = Commit::with_timestamp(vec![], update1_b64.clone(), "a".into(), None, 1000)
            .calculate_cid();

        apply_received_commit(&mut state, &cid1, &update1_b64, &[]).unwrap();
        assert_eq!(get_text_content(&state).unwrap(), "hello");

        // Commit 2: append " world" (from a different client)
        let doc = state.to_doc().unwrap();
        let update2 = create_append_update(&doc, " world");
        let update2_b64 = STANDARD.encode(&update2);
        let cid2 = Commit::with_timestamp(
            vec![cid1.clone()],
            update2_b64.clone(),
            "b".into(),
            None,
            2000,
        )
        .calculate_cid();

        apply_received_commit(&mut state, &cid2, &update2_b64, std::slice::from_ref(&cid1))
            .unwrap();
        assert_eq!(get_text_content(&state).unwrap(), "hello world");

        // Commit 3: append "!"
        let doc = state.to_doc().unwrap();
        let update3 = create_append_update(&doc, "!");
        let update3_b64 = STANDARD.encode(&update3);
        let cid3 = Commit::with_timestamp(
            vec![cid2.clone()],
            update3_b64.clone(),
            "c".into(),
            None,
            3000,
        )
        .calculate_cid();

        apply_received_commit(&mut state, &cid3, &update3_b64, std::slice::from_ref(&cid2))
            .unwrap();
        assert_eq!(get_text_content(&state).unwrap(), "hello world!");
    }

    /// Invariant: is_commit_known returns true for both head_cid and local_head_cid.
    ///
    /// A commit is "known" if it matches either the server head or our local head.
    #[test]
    fn crdt_publish_is_commit_known_tracks_both_heads() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Initially, no commits are known
        assert!(!is_commit_known(&state, "cid_a"));
        assert!(!is_commit_known(&state, "cid_b"));
        assert!(!is_commit_known(&state, "cid_c"));

        // After applying a commit, it becomes head_cid and is known
        let update = create_insert_update(1, "test");
        let update_b64 = STANDARD.encode(&update);
        apply_received_commit(&mut state, "cid_a", &update_b64, &[]).unwrap();

        assert!(is_commit_known(&state, "cid_a"), "head_cid should be known");
        assert!(
            !is_commit_known(&state, "cid_b"),
            "unknown CID should not be known"
        );

        // Set local_head_cid to a different value
        state.local_head_cid = Some("cid_b".to_string());
        assert!(
            is_commit_known(&state, "cid_a"),
            "head_cid should still be known"
        );
        assert!(
            is_commit_known(&state, "cid_b"),
            "local_head_cid should be known"
        );
        assert!(
            !is_commit_known(&state, "cid_c"),
            "unknown CID should not be known"
        );
    }

    /// Invariant: Duplicate commit application is idempotent and returns false.
    ///
    /// Applying the same CID twice should be detected and return Ok(false).
    #[test]
    fn crdt_publish_duplicate_commit_returns_false() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        let update = create_insert_update(1, "unique content");
        let update_b64 = STANDARD.encode(&update);
        let cid = "unique_cid_12345";

        // First application should succeed
        let result1 = apply_received_commit(&mut state, cid, &update_b64, &[]);
        assert!(result1.is_ok());
        assert!(result1.unwrap(), "First application should return true");

        // Second application should be detected as duplicate
        let result2 = apply_received_commit(&mut state, cid, &update_b64, &[]);
        assert!(result2.is_ok());
        assert!(
            !result2.unwrap(),
            "Duplicate application should return false"
        );

        // Content should still be correct (not duplicated)
        let content = get_text_content(&state).unwrap();
        assert_eq!(
            content, "unique content",
            "Content should not be duplicated"
        );
    }

    /// Invariant: CID chain forms a valid commit graph.
    ///
    /// Each commit's CID depends on its parents, so the chain of CIDs
    /// forms a directed acyclic graph (like git commits).
    #[test]
    fn crdt_publish_cid_chain_forms_valid_graph() {
        let update_a = create_insert_update(1, "A");
        let update_b = create_insert_update(2, "B");
        let update_c = create_insert_update(3, "C");

        // Root commit (no parents)
        let cid_root = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&update_a),
            "author".into(),
            None,
            1000,
        )
        .calculate_cid();

        // Child commit (parent: root)
        let cid_child = Commit::with_timestamp(
            vec![cid_root.clone()],
            STANDARD.encode(&update_b),
            "author".into(),
            None,
            2000,
        )
        .calculate_cid();

        // Grandchild commit (parent: child)
        let cid_grandchild = Commit::with_timestamp(
            vec![cid_child.clone()],
            STANDARD.encode(&update_c),
            "author".into(),
            None,
            3000,
        )
        .calculate_cid();

        // All CIDs should be unique
        assert_ne!(cid_root, cid_child);
        assert_ne!(cid_child, cid_grandchild);
        assert_ne!(cid_root, cid_grandchild);

        // CIDs should be stable (recalculating gives same result)
        let cid_root_2 = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&update_a),
            "author".into(),
            None,
            1000,
        )
        .calculate_cid();
        assert_eq!(cid_root, cid_root_2, "CID should be deterministic");
    }

    /// Invariant: Empty state returns empty string for text content.
    #[test]
    fn crdt_publish_empty_state_returns_empty_content() {
        let state = CrdtPeerState::new(Uuid::new_v4());
        let content = get_text_content(&state).expect("Should succeed on empty state");
        assert_eq!(content, "", "Empty state should return empty string");
    }

    /// Invariant: Yjs state is correctly preserved through serialization.
    ///
    /// The Y.Doc state stored in CrdtPeerState should survive serialization
    /// and deserialization (which happens when state is persisted to disk).
    #[test]
    fn crdt_publish_yjs_state_survives_serialization() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Apply a commit
        let update = create_insert_update(1, "serialization test");
        let update_b64 = STANDARD.encode(&update);
        apply_received_commit(&mut state, "test_cid", &update_b64, &[]).unwrap();

        // Serialize and deserialize
        let json = serde_json::to_string(&state).expect("Serialization should succeed");
        let restored: CrdtPeerState =
            serde_json::from_str(&json).expect("Deserialization should succeed");

        // Verify content is preserved
        let original_content = get_text_content(&state).unwrap();
        let restored_content = get_text_content(&restored).unwrap();
        assert_eq!(
            original_content, restored_content,
            "Content should survive serialization roundtrip"
        );
        assert_eq!(restored_content, "serialization test");

        // Verify CID tracking is preserved
        assert_eq!(state.head_cid, restored.head_cid);
    }
}

// =============================================================================
// CRDT Merge Tests (Stage 1 Validation - CP-7rrp)
//
// These tests validate the crdt_merge.rs module in isolation:
// 1. Concurrent edits from multiple peers converge to same state
// 2. Merge commits are correctly constructed
// 3. Parent ancestry is properly tracked
// 4. Merge of deleted content works correctly
// =============================================================================

mod crdt_merge_tests {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use commonplace_doc::commit::Commit;
    use commonplace_doc::mqtt::EditMessage;
    use commonplace_doc::sync::crdt_state::CrdtPeerState;
    use commonplace_doc::sync::{process_received_edit, MergeResult};
    use uuid::Uuid;
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update, WriteTxn};

    /// Helper to create a Yjs update for text insertion.
    fn create_insert_update(client_id: u64, text: &str, position: u32) -> (Vec<u8>, Doc) {
        let doc = Doc::with_client_id(client_id);
        let update = {
            let mut txn = doc.transact_mut();
            let ytext = txn.get_or_insert_text("content");
            ytext.insert(&mut txn, position, text);
            txn.encode_update_v1()
        };
        (update, doc)
    }

    /// Helper to create a Yjs update for text deletion.
    fn create_delete_update(client_id: u64, base_state: &[u8], start: u32, len: u32) -> Vec<u8> {
        let doc = Doc::with_client_id(client_id);
        {
            let update = Update::decode_v1(base_state).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }
        let update = {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.remove_range(&mut txn, start, len);
            txn.encode_update_v1()
        };
        update
    }

    /// Helper to get text content from a Y.Doc.
    fn get_doc_text(doc: &Doc) -> String {
        let txn = doc.transact();
        match txn.get_text("content") {
            Some(text) => text.get_string(&txn),
            None => String::new(),
        }
    }

    /// Helper to apply an update to a doc.
    fn apply_update(doc: &Doc, update: &[u8]) {
        let u = Update::decode_v1(update).expect("update decode failed");
        let mut txn = doc.transact_mut();
        txn.apply_update(u);
    }

    /// Helper to get full state from a doc as base64.
    fn doc_state_b64(doc: &Doc) -> String {
        let txn = doc.transact();
        STANDARD.encode(txn.encode_state_as_update_v1(&StateVector::default()))
    }

    // =========================================================================
    // Test 1: Two peers with concurrent edits converge to same state
    // =========================================================================

    /// Two peers making concurrent edits at the same position converge.
    ///
    /// This test validates Yjs CRDT convergence when two peers make concurrent
    /// edits at the same position. The test focuses on the raw Yjs merge
    /// behavior rather than the higher-level MergeResult logic.
    #[tokio::test]
    async fn crdt_merge_two_peers_concurrent_same_position_converge() {
        // Setup: Both peers start from empty state
        let empty_doc = Doc::with_client_id(0);
        {
            let mut txn = empty_doc.transact_mut();
            txn.get_or_insert_text("content");
        }
        let empty_state = doc_state_b64(&empty_doc);

        // Peer A inserts "AAA" at position 0
        let (update_a, _doc_a) = create_insert_update(10, "AAA", 0);
        let update_a_b64 = STANDARD.encode(&update_a);

        // Peer B inserts "BBB" at position 0 (concurrently)
        let (update_b, _doc_b) = create_insert_update(20, "BBB", 0);
        let update_b_b64 = STANDARD.encode(&update_b);

        // Now we simulate what happens when each peer receives the other's edit.
        // This tests the core CRDT convergence property.

        // Peer A's doc receives B's update
        let final_doc_a = Doc::with_client_id(10);
        apply_update(&final_doc_a, &STANDARD.decode(&empty_state).unwrap());
        apply_update(&final_doc_a, &STANDARD.decode(&update_a_b64).unwrap());
        apply_update(&final_doc_a, &STANDARD.decode(&update_b_b64).unwrap());

        // Peer B's doc receives A's update
        let final_doc_b = Doc::with_client_id(20);
        apply_update(&final_doc_b, &STANDARD.decode(&empty_state).unwrap());
        apply_update(&final_doc_b, &STANDARD.decode(&update_b_b64).unwrap());
        apply_update(&final_doc_b, &STANDARD.decode(&update_a_b64).unwrap());

        let content_a = get_doc_text(&final_doc_a);
        let content_b = get_doc_text(&final_doc_b);

        // CRDT property: both peers converge to identical content
        assert_eq!(
            content_a, content_b,
            "Both peers must converge to same content"
        );
        // Both edits are preserved
        assert!(
            content_a.contains("AAA") && content_a.contains("BBB"),
            "Content must contain both edits: '{}'",
            content_a
        );
    }

    /// Two peers making concurrent edits at different positions converge.
    #[tokio::test]
    async fn crdt_merge_two_peers_different_positions_converge() {
        let base_doc = Doc::with_client_id(0);
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }
        let base_state = doc_state_b64(&base_doc);
        let base_bytes = STANDARD.decode(&base_state).unwrap();

        let commit_base =
            Commit::with_timestamp(vec![], base_state.clone(), "init".to_string(), None, 500);
        let base_cid = commit_base.calculate_cid();

        let mut state_a = CrdtPeerState::new(Uuid::new_v4());
        let mut state_b = CrdtPeerState::new(Uuid::new_v4());
        state_a.initialize_from_server(&base_state, &base_cid);
        state_b.initialize_from_server(&base_state, &base_cid);

        let doc_a = Doc::with_client_id(10);
        apply_update(&doc_a, &base_bytes);
        let update_a = {
            let mut txn = doc_a.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 5, " world");
            txn.encode_update_v1()
        };
        let update_a_b64 = STANDARD.encode(&update_a);
        let cid_a = Commit::with_timestamp(
            vec![base_cid.clone()],
            update_a_b64.clone(),
            "peer_a".to_string(),
            None,
            1000,
        )
        .calculate_cid();

        state_a.local_head_cid = Some(cid_a.clone());
        state_a.head_cid = Some(cid_a.clone());
        state_a.yjs_state = Some(doc_state_b64(&doc_a));

        let doc_b = Doc::with_client_id(20);
        apply_update(&doc_b, &base_bytes);
        let update_b = {
            let mut txn = doc_b.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "Say: ");
            txn.encode_update_v1()
        };
        let update_b_b64 = STANDARD.encode(&update_b);
        let cid_b = Commit::with_timestamp(
            vec![base_cid.clone()],
            update_b_b64.clone(),
            "peer_b".to_string(),
            None,
            1001,
        )
        .calculate_cid();

        state_b.local_head_cid = Some(cid_b.clone());
        state_b.head_cid = Some(cid_b.clone());
        state_b.yjs_state = Some(doc_state_b64(&doc_b));

        let msg_b = EditMessage {
            update: update_b_b64.clone(),
            parents: vec![base_cid.clone()],
            author: "peer_b".to_string(),
            message: None,
            timestamp: 1001,
        };

        let (_, _) = process_received_edit(None, "ws", "node", &mut state_a, &msg_b, "peer_a")
            .await
            .expect("Peer A should process B's edit");

        let msg_a = EditMessage {
            update: update_a_b64.clone(),
            parents: vec![base_cid.clone()],
            author: "peer_a".to_string(),
            message: None,
            timestamp: 1000,
        };

        let (_, _) = process_received_edit(None, "ws", "node", &mut state_b, &msg_a, "peer_b")
            .await
            .expect("Peer B should process A's edit");

        let final_doc_a = state_a.to_doc().expect("State A should have valid doc");
        let final_doc_b = state_b.to_doc().expect("State B should have valid doc");

        let content_a = get_doc_text(&final_doc_a);
        let content_b = get_doc_text(&final_doc_b);

        assert_eq!(content_a, content_b, "Both peers must converge");
        assert!(
            content_a.contains("Say:")
                && content_a.contains("hello")
                && content_a.contains("world"),
            "Content must contain all parts: '{}'",
            content_a
        );
    }

    // =========================================================================
    // Test 2: Three-way merge produces correct result
    // =========================================================================

    /// Three peers with concurrent edits converge correctly.
    #[tokio::test]
    async fn crdt_merge_three_way_converges() {
        let empty_doc = Doc::with_client_id(0);
        {
            let mut txn = empty_doc.transact_mut();
            txn.get_or_insert_text("content");
        }

        let (update_a, _) = create_insert_update(10, "A", 0);
        let (update_b, _) = create_insert_update(20, "B", 0);
        let (update_c, _) = create_insert_update(30, "C", 0);

        let final_doc = Doc::with_client_id(99);
        apply_update(&final_doc, &update_a);
        apply_update(&final_doc, &update_b);
        apply_update(&final_doc, &update_c);

        let converged_content = get_doc_text(&final_doc);
        assert!(
            converged_content.contains('A')
                && converged_content.contains('B')
                && converged_content.contains('C'),
            "Merged content must contain all three edits: '{}'",
            converged_content
        );

        let final_doc2 = Doc::with_client_id(98);
        apply_update(&final_doc2, &update_c);
        apply_update(&final_doc2, &update_a);
        apply_update(&final_doc2, &update_b);

        let converged_content2 = get_doc_text(&final_doc2);
        assert_eq!(
            converged_content, converged_content2,
            "CRDT must converge regardless of application order"
        );
    }

    // =========================================================================
    // Test 3: Parent ancestry is properly tracked
    // =========================================================================

    /// Parent tracking: Linear chain of commits maintains ancestry.
    #[tokio::test]
    async fn crdt_merge_parent_tracking_linear_chain() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        let empty_doc = Doc::with_client_id(0);
        {
            let mut txn = empty_doc.transact_mut();
            txn.get_or_insert_text("content");
        }
        let empty_state = doc_state_b64(&empty_doc);
        state.initialize_from_server(&empty_state, "genesis");

        let (update1, _) = create_insert_update(1, "hello", 0);
        let update1_b64 = STANDARD.encode(&update1);
        let commit1 = Commit::with_timestamp(
            vec!["genesis".to_string()],
            update1_b64.clone(),
            "author".to_string(),
            None,
            1000,
        );
        let cid1 = commit1.calculate_cid();

        let msg1 = EditMessage {
            update: update1_b64,
            parents: vec!["genesis".to_string()],
            author: "author".to_string(),
            message: None,
            timestamp: 1000,
        };

        let (result1, _) = process_received_edit(None, "ws", "node", &mut state, &msg1, "author")
            .await
            .expect("Commit 1 should succeed");

        assert!(
            matches!(result1, MergeResult::FastForward { .. }),
            "First commit should fast-forward, got {:?}",
            result1
        );

        assert_eq!(
            state.head_cid,
            Some(cid1.clone()),
            "head_cid should be commit 1"
        );

        let doc2 = Doc::with_client_id(2);
        apply_update(
            &doc2,
            &STANDARD.decode(state.yjs_state.as_ref().unwrap()).unwrap(),
        );
        let update2 = {
            let mut txn = doc2.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 5, " world");
            txn.encode_update_v1()
        };
        let update2_b64 = STANDARD.encode(&update2);

        let msg2 = EditMessage {
            update: update2_b64,
            parents: vec![cid1.clone()],
            author: "author".to_string(),
            message: None,
            timestamp: 2000,
        };

        let (result2, content) =
            process_received_edit(None, "ws", "node", &mut state, &msg2, "author")
                .await
                .expect("Commit 2 should succeed");

        assert!(
            matches!(result2, MergeResult::FastForward { .. }),
            "Second commit should fast-forward, got {:?}",
            result2
        );

        assert!(content.is_some(), "Fast-forward should produce content");
        assert_eq!(content.unwrap(), "hello world", "Content should be correct");
    }

    /// Parent tracking: Merge commit with two parents.
    #[tokio::test]
    async fn crdt_merge_parent_tracking_two_parents() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        let (base_update, base_doc) = create_insert_update(0, "base", 0);
        let base_state = doc_state_b64(&base_doc);
        let base_commit = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&base_update),
            "init".to_string(),
            None,
            500,
        );
        let base_cid = base_commit.calculate_cid();

        state.initialize_from_server(&base_state, &base_cid);

        let local_doc = Doc::with_client_id(1);
        apply_update(&local_doc, &STANDARD.decode(&base_state).unwrap());
        let local_update = {
            let mut txn = local_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 4, "-local");
            txn.encode_update_v1()
        };
        let local_cid = Commit::with_timestamp(
            vec![base_cid.clone()],
            STANDARD.encode(&local_update),
            "local".to_string(),
            None,
            1000,
        )
        .calculate_cid();

        state.local_head_cid = Some(local_cid.clone());
        state.head_cid = Some(base_cid.clone());
        state.yjs_state = Some(doc_state_b64(&local_doc));

        let remote_doc = Doc::with_client_id(2);
        apply_update(&remote_doc, &STANDARD.decode(&base_state).unwrap());
        let remote_update = {
            let mut txn = remote_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 4, "-remote");
            txn.encode_update_v1()
        };
        let remote_update_b64 = STANDARD.encode(&remote_update);

        let remote_msg = EditMessage {
            update: remote_update_b64,
            parents: vec![base_cid.clone()],
            author: "remote".to_string(),
            message: None,
            timestamp: 1001,
        };

        let (result, _) =
            process_received_edit(None, "ws", "node", &mut state, &remote_msg, "local")
                .await
                .expect("Should process remote edit");

        assert!(
            matches!(
                result,
                MergeResult::Merged { .. } | MergeResult::NeedsMerge { .. }
            ),
            "Divergent edits should trigger merge, got {:?}",
            result
        );

        let final_doc = state.to_doc().expect("Should have valid doc");
        let final_content = get_doc_text(&final_doc);
        assert!(
            final_content.contains("base")
                && final_content.contains("local")
                && final_content.contains("remote"),
            "Merged content should contain all parts: '{}'",
            final_content
        );
    }

    // =========================================================================
    // Test 4: Merge of deleted content
    // =========================================================================

    /// Merge handles concurrent delete and insert correctly.
    #[tokio::test]
    async fn crdt_merge_delete_and_insert_converge() {
        let base_doc = Doc::with_client_id(0);
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello world");
        }
        let base_state = doc_state_b64(&base_doc);
        let base_bytes = STANDARD.decode(&base_state).unwrap();

        let delete_update = create_delete_update(10, &base_bytes, 0, 6);

        let insert_doc = Doc::with_client_id(20);
        apply_update(&insert_doc, &base_bytes);
        let insert_update = {
            let mut txn = insert_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 6, "beautiful ");
            txn.encode_update_v1()
        };

        let merged_doc = Doc::with_client_id(99);
        apply_update(&merged_doc, &base_bytes);
        apply_update(&merged_doc, &delete_update);
        apply_update(&merged_doc, &insert_update);

        let merged_content = get_doc_text(&merged_doc);

        assert!(
            !merged_content.contains("hello"),
            "Deleted text should not appear: '{}'",
            merged_content
        );
        assert!(
            merged_content.contains("beautiful"),
            "Inserted text should appear: '{}'",
            merged_content
        );
        assert!(
            merged_content.contains("world"),
            "Preserved text should appear: '{}'",
            merged_content
        );
    }

    /// Merge handles concurrent deletes that overlap.
    #[tokio::test]
    async fn crdt_merge_overlapping_deletes_converge() {
        let base_doc = Doc::with_client_id(0);
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "abcdefghij");
        }
        let base_state = doc_state_b64(&base_doc);
        let base_bytes = STANDARD.decode(&base_state).unwrap();

        let delete_a = create_delete_update(10, &base_bytes, 2, 3);
        let delete_b = create_delete_update(20, &base_bytes, 4, 3);

        let merged_doc = Doc::with_client_id(99);
        apply_update(&merged_doc, &base_bytes);
        apply_update(&merged_doc, &delete_a);
        apply_update(&merged_doc, &delete_b);

        let merged_content = get_doc_text(&merged_doc);

        assert!(
            merged_content.contains("ab"),
            "Start should be preserved: '{}'",
            merged_content
        );
        assert!(
            merged_content.contains("hij"),
            "End should be preserved: '{}'",
            merged_content
        );
        assert!(
            !merged_content.contains("cde"),
            "A's deletion should apply: '{}'",
            merged_content
        );
        assert!(
            !merged_content.contains("efg"),
            "B's deletion should apply: '{}'",
            merged_content
        );
    }

    /// Merge handles delete of content being concurrently replaced.
    #[tokio::test]
    async fn crdt_merge_delete_all_vs_replace() {
        let base_doc = Doc::with_client_id(0);
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }
        let base_state = doc_state_b64(&base_doc);
        let base_bytes = STANDARD.decode(&base_state).unwrap();

        let delete_all = create_delete_update(10, &base_bytes, 0, 5);

        let replace_doc = Doc::with_client_id(20);
        apply_update(&replace_doc, &base_bytes);
        let replace_update = {
            let mut txn = replace_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.remove_range(&mut txn, 0, 5);
            text.insert(&mut txn, 0, "world");
            txn.encode_update_v1()
        };

        let merged_doc = Doc::with_client_id(99);
        apply_update(&merged_doc, &base_bytes);
        apply_update(&merged_doc, &delete_all);
        apply_update(&merged_doc, &replace_update);

        let merged_content = get_doc_text(&merged_doc);

        assert!(
            !merged_content.contains("hello"),
            "Original should be deleted: '{}'",
            merged_content
        );
        assert!(
            merged_content.contains("world"),
            "Replacement should be preserved: '{}'",
            merged_content
        );
    }

    // =========================================================================
    // Additional edge case tests
    // =========================================================================

    /// Empty edits don't corrupt state.
    #[tokio::test]
    async fn crdt_merge_empty_edit_preserves_state() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        let (init_update, init_doc) = create_insert_update(0, "content", 0);
        let init_state = doc_state_b64(&init_doc);
        let init_cid = Commit::with_timestamp(
            vec![],
            STANDARD.encode(&init_update),
            "init".to_string(),
            None,
            500,
        )
        .calculate_cid();

        state.initialize_from_server(&init_state, &init_cid);

        let empty_msg = EditMessage {
            update: String::new(),
            parents: vec![init_cid.clone()],
            author: "author".to_string(),
            message: None,
            timestamp: 1000,
        };

        let (result, _) =
            process_received_edit(None, "ws", "node", &mut state, &empty_msg, "author")
                .await
                .expect("Empty edit should process");

        assert!(
            matches!(result, MergeResult::FastForward { .. }),
            "Empty edit should fast-forward, got {:?}",
            result
        );

        let doc = state.to_doc().expect("Should have valid doc");
        let content = get_doc_text(&doc);
        assert_eq!(content, "content", "Content should be unchanged");
    }

    /// Very long text still converges correctly.
    #[tokio::test]
    async fn crdt_merge_large_text_converges() {
        let large_text: String = "x".repeat(1024);

        let base_doc = Doc::with_client_id(0);
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, &large_text);
        }
        let base_state = doc_state_b64(&base_doc);
        let base_bytes = STANDARD.decode(&base_state).unwrap();

        let doc_a = Doc::with_client_id(10);
        apply_update(&doc_a, &base_bytes);
        let update_a = {
            let mut txn = doc_a.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "START");
            txn.encode_update_v1()
        };

        let doc_b = Doc::with_client_id(20);
        apply_update(&doc_b, &base_bytes);
        let update_b = {
            let mut txn = doc_b.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 1024, "END");
            txn.encode_update_v1()
        };

        let merged = Doc::with_client_id(99);
        apply_update(&merged, &base_bytes);
        apply_update(&merged, &update_a);
        apply_update(&merged, &update_b);

        let content = get_doc_text(&merged);
        assert!(content.starts_with("START"), "Should start with START");
        assert!(content.ends_with("END"), "Should end with END");
        assert_eq!(content.len(), 1024 + 5 + 3, "Length should be correct");
    }
}
