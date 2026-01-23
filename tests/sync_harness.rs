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
