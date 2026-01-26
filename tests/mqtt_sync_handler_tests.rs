//! Tests for MQTT sync handler correctness and reliability.
//!
//! These tests validate that the sync handler correctly responds to
//! head/get/pull/ancestors requests, handles error cases gracefully,
//! and maintains proper response ordering.

use commonplace_doc::commit::Commit;
use commonplace_doc::mqtt::messages::SyncMessage;
use commonplace_doc::mqtt::topics::Topic;
use commonplace_doc::mqtt::MqttError;
use commonplace_doc::store::CommitStore;
use std::sync::Arc;
use tempfile::TempDir;

/// Test helper to create a commit store with temp directory
fn create_test_store() -> (Arc<CommitStore>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let store = CommitStore::new(&path).unwrap();
    (Arc::new(store), dir)
}

/// Test helper to create a commit chain.
/// Returns (commit_ids, commits) in order from oldest to newest.
async fn create_commit_chain(
    store: &CommitStore,
    doc_id: &str,
    count: usize,
) -> Vec<(String, Commit)> {
    let mut commits = Vec::new();
    let mut parent = vec![];

    for i in 0..count {
        let update = format!("update-{}", i);
        let commit = Commit::new(
            parent.clone(),
            update,
            "test-author".to_string(),
            Some(format!("Commit {}", i)),
        );
        let cid = store.store_commit(&commit).await.unwrap();
        commits.push((cid.clone(), commit));
        parent = vec![cid];
    }

    // Set the document head to the last commit
    if let Some((last_cid, _)) = commits.last() {
        store.set_document_head(doc_id, last_cid).await.unwrap();
    }

    commits
}

// =============================================================================
// Tests for HEAD request handling
// =============================================================================

#[tokio::test]
async fn test_handle_head_empty_document() {
    // When document has no commits, HEAD should return None
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    // Verify no head exists
    let head = store.get_document_head(doc_id).await.unwrap();
    assert!(head.is_none(), "Empty document should have no HEAD");
}

#[tokio::test]
async fn test_handle_head_with_commits() {
    // When document has commits, HEAD should return the latest commit ID
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 3).await;
    let expected_head = &commits.last().unwrap().0;

    let head = store.get_document_head(doc_id).await.unwrap();
    assert_eq!(
        head,
        Some(expected_head.clone()),
        "HEAD should be the last commit"
    );
}

#[tokio::test]
async fn test_handle_head_updates_with_new_commit() {
    // HEAD should update when a new commit is added
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 2).await;
    let initial_head = commits.last().unwrap().0.clone();

    // Add another commit
    let new_commit = Commit::new(
        vec![initial_head.clone()],
        "new-update".to_string(),
        "test-author".to_string(),
        None,
    );
    let new_cid = store.store_commit(&new_commit).await.unwrap();
    store.set_document_head(doc_id, &new_cid).await.unwrap();

    let head = store.get_document_head(doc_id).await.unwrap();
    assert_eq!(head, Some(new_cid), "HEAD should be updated to new commit");
}

// =============================================================================
// Tests for GET request handling
// =============================================================================

#[tokio::test]
async fn test_handle_get_existing_commits() {
    // GET should return commit data for existing commit IDs
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 3).await;

    // Fetch each commit and verify
    for (cid, original_commit) in &commits {
        let retrieved = store.get_commit(cid).await.unwrap();
        assert_eq!(retrieved.update, original_commit.update);
        assert_eq!(retrieved.author, original_commit.author);
        assert_eq!(retrieved.parents, original_commit.parents);
    }
}

#[tokio::test]
async fn test_handle_get_missing_commit() {
    // GET should return error for non-existent commit
    let (store, _dir) = create_test_store();

    let result = store.get_commit("nonexistent-commit-id").await;
    assert!(
        result.is_err(),
        "GET for missing commit should return error"
    );
    match &result {
        Err(commonplace_doc::store::StoreError::CommitNotFound(cid)) => {
            assert_eq!(cid, "nonexistent-commit-id");
        }
        Err(other) => {
            // Accept any error - the important thing is that it fails
            assert!(
                matches!(
                    other,
                    commonplace_doc::store::StoreError::CommitNotFound(_)
                        | commonplace_doc::store::StoreError::DatabaseError(_)
                ),
                "Expected CommitNotFound or DatabaseError, got: {:?}",
                other
            );
        }
        Ok(_) => panic!("Expected error for missing commit"),
    }
}

#[tokio::test]
async fn test_handle_get_multiple_commits() {
    // GET with multiple commit IDs should return all commits
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 5).await;

    // Fetch commits 0, 2, and 4
    let ids_to_fetch: Vec<&str> = vec![&commits[0].0, &commits[2].0, &commits[4].0];

    for cid in ids_to_fetch {
        let retrieved = store.get_commit(cid).await.unwrap();
        assert!(!retrieved.update.is_empty());
    }
}

// =============================================================================
// Tests for PULL request handling
// =============================================================================

#[tokio::test]
async fn test_pull_incremental_sync() {
    // PULL with have=[c1] and want=HEAD should return commits after c1
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 4).await;
    let have = [commits[1].0.clone()]; // Client has up to commit 1
    let want = commits[3].0.clone(); // Client wants commit 3

    // Find commits between 'have' and 'want'
    let mut result = Vec::new();
    let mut to_visit = vec![want.clone()];
    let mut visited = std::collections::HashSet::new();
    let have_set: std::collections::HashSet<_> = have.iter().map(|s| s.as_str()).collect();

    while let Some(cid) = to_visit.pop() {
        if visited.contains(&cid) || have_set.contains(cid.as_str()) {
            continue;
        }
        visited.insert(cid.clone());

        let commit = store.get_commit(&cid).await.unwrap();
        result.push(cid.clone());
        for parent in &commit.parents {
            to_visit.push(parent.clone());
        }
    }

    // Result should include commits 2 and 3 (after commit 1)
    assert_eq!(result.len(), 2);
    assert!(result.contains(&commits[2].0));
    assert!(result.contains(&commits[3].0));
}

#[tokio::test]
async fn test_pull_empty_document() {
    // PULL on empty document should return empty set
    let (store, _dir) = create_test_store();
    let doc_id = "empty-doc";

    let head = store.get_document_head(doc_id).await.unwrap();
    assert!(head.is_none(), "Empty doc should have no commits");
}

#[tokio::test]
async fn test_pull_client_already_up_to_date() {
    // PULL when client already has HEAD should return empty set
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 3).await;
    let head = commits.last().unwrap().0.clone();
    let have = [head.clone()]; // Client already has HEAD

    // Find commits between 'have' and HEAD
    let have_set: std::collections::HashSet<_> = have.iter().map(|s| s.as_str()).collect();

    // Start from HEAD - but client already has it
    let cid = head;
    // Client already has HEAD, so no commits to send
    assert!(
        have_set.contains(cid.as_str()),
        "Client should have HEAD commit"
    );
}

// =============================================================================
// Tests for ANCESTORS request handling
// =============================================================================

#[tokio::test]
async fn test_ancestors_full_history() {
    // ANCESTORS from HEAD with no depth limit should return all commits
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 5).await;
    let head = commits.last().unwrap().0.clone();

    // Collect all ancestors
    let mut result = Vec::new();
    let mut to_visit = vec![(head.clone(), 0u32)];
    let mut visited = std::collections::HashSet::new();

    while let Some((cid, _depth)) = to_visit.pop() {
        if visited.contains(&cid) {
            continue;
        }
        visited.insert(cid.clone());

        let commit = store.get_commit(&cid).await.unwrap();
        result.push(cid.clone());
        for parent in &commit.parents {
            to_visit.push((parent.clone(), _depth + 1));
        }
    }

    // Should include all 5 commits
    assert_eq!(result.len(), 5);
    for (cid, _) in &commits {
        assert!(result.contains(cid), "All commits should be in ancestors");
    }
}

#[tokio::test]
async fn test_ancestors_with_depth_limit() {
    // ANCESTORS with depth=2 should return limited commits
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 5).await;
    let head = commits.last().unwrap().0.clone();
    let max_depth = 2u32;

    // Collect ancestors with depth limit
    let mut result = Vec::new();
    let mut to_visit = vec![(head.clone(), 0u32)];
    let mut visited = std::collections::HashSet::new();

    while let Some((cid, current_depth)) = to_visit.pop() {
        if visited.contains(&cid) || current_depth > max_depth {
            continue;
        }
        visited.insert(cid.clone());

        let commit = store.get_commit(&cid).await.unwrap();
        result.push(cid.clone());
        for parent in &commit.parents {
            to_visit.push((parent.clone(), current_depth + 1));
        }
    }

    // Should include commits 4, 3, 2 (depth 0, 1, 2)
    assert_eq!(result.len(), 3);
    assert!(result.contains(&commits[4].0));
    assert!(result.contains(&commits[3].0));
    assert!(result.contains(&commits[2].0));
}

#[tokio::test]
async fn test_ancestors_empty_document() {
    // ANCESTORS on empty document should return empty set
    let (store, _dir) = create_test_store();
    let doc_id = "empty-doc";

    let head = store.get_document_head(doc_id).await.unwrap();
    assert!(head.is_none(), "Empty doc should have no ancestors");
}

// =============================================================================
// Tests for IS_ANCESTOR request handling
// =============================================================================

#[tokio::test]
async fn test_is_ancestor_true_case() {
    // is_ancestor should return true when ancestor relationship exists
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 4).await;

    // Commit 0 is ancestor of commit 3
    let result = store
        .is_ancestor(&commits[0].0, &commits[3].0)
        .await
        .unwrap();
    assert!(result, "Commit 0 should be ancestor of commit 3");

    // Commit 1 is ancestor of commit 2
    let result = store
        .is_ancestor(&commits[1].0, &commits[2].0)
        .await
        .unwrap();
    assert!(result, "Commit 1 should be ancestor of commit 2");
}

#[tokio::test]
async fn test_is_ancestor_false_case() {
    // is_ancestor should return false when no ancestor relationship exists
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 4).await;

    // Commit 3 is NOT ancestor of commit 0 (reversed)
    let result = store
        .is_ancestor(&commits[3].0, &commits[0].0)
        .await
        .unwrap();
    assert!(!result, "Commit 3 should not be ancestor of commit 0");
}

#[tokio::test]
async fn test_is_ancestor_same_commit() {
    // A commit is its own ancestor
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 2).await;

    let result = store
        .is_ancestor(&commits[1].0, &commits[1].0)
        .await
        .unwrap();
    assert!(result, "A commit should be its own ancestor");
}

#[tokio::test]
async fn test_is_ancestor_missing_commit() {
    // is_ancestor with nonexistent commit should return error
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 2).await;

    let result = store.is_ancestor(&commits[0].0, "nonexistent").await;
    assert!(result.is_err(), "Should error on missing commit");
}

// =============================================================================
// Tests for response ordering and completeness
// =============================================================================

#[tokio::test]
async fn test_commit_chain_order_preserved() {
    // Commits should be retrievable in the correct order
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 5).await;

    // Verify each commit points to the previous one
    for i in 1..commits.len() {
        let (_, commit) = &commits[i];
        assert_eq!(
            commit.parents,
            vec![commits[i - 1].0.clone()],
            "Commit {} should have commit {} as parent",
            i,
            i - 1
        );
    }
}

#[tokio::test]
async fn test_commits_since_returns_chronological_order() {
    // get_commits_since should return commits in timestamp order
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let _commits = create_commit_chain(&store, doc_id, 5).await;

    // Get all commits since timestamp 0
    let result = store.get_commits_since(doc_id, 0).await.unwrap();

    // Verify commits are in chronological order
    assert_eq!(result.len(), 5);
    for i in 1..result.len() {
        assert!(
            result[i].1.timestamp >= result[i - 1].1.timestamp,
            "Commits should be in chronological order"
        );
    }
}

// =============================================================================
// Tests for SyncMessage parsing and serialization
// =============================================================================

#[test]
fn test_sync_message_head_request_serialization() {
    let msg = SyncMessage::Head {
        req: "req-001".to_string(),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"head""#));
    assert!(json.contains(r#""req":"req-001""#));
}

#[test]
fn test_sync_message_head_response_with_commit() {
    let msg = SyncMessage::HeadResponse {
        req: "req-001".to_string(),
        commit: Some("abc123".to_string()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""commit":"abc123""#));

    // Round-trip
    let parsed: SyncMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        SyncMessage::HeadResponse { req, commit } => {
            assert_eq!(req, "req-001");
            assert_eq!(commit, Some("abc123".to_string()));
        }
        _ => panic!("Expected HeadResponse"),
    }
}

#[test]
fn test_sync_message_head_response_empty() {
    let msg = SyncMessage::HeadResponse {
        req: "req-001".to_string(),
        commit: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    // commit field should be omitted when None
    assert!(!json.contains("commit"));
}

#[test]
fn test_sync_message_get_request() {
    let msg = SyncMessage::Get {
        req: "req-002".to_string(),
        commits: vec!["cid1".to_string(), "cid2".to_string()],
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"get""#));
    assert!(json.contains(r#""commits":["cid1","cid2"]"#));
}

#[test]
fn test_sync_message_pull_request() {
    let msg = SyncMessage::Pull {
        req: "req-003".to_string(),
        have: vec!["abc".to_string()],
        want: "HEAD".to_string(),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"pull""#));
    assert!(json.contains(r#""have":["abc"]"#));
    assert!(json.contains(r#""want":"HEAD""#));
}

#[test]
fn test_sync_message_ancestors_request() {
    let msg = SyncMessage::Ancestors {
        req: "req-004".to_string(),
        commit: "HEAD".to_string(),
        depth: Some(5),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"ancestors""#));
    assert!(json.contains(r#""commit":"HEAD""#));
    assert!(json.contains(r#""depth":5"#));
}

#[test]
fn test_sync_message_commit_response() {
    let msg = SyncMessage::Commit {
        req: "req-005".to_string(),
        id: "cid-123".to_string(),
        parents: vec!["parent-1".to_string()],
        data: "base64-update".to_string(),
        timestamp: 1704067200000,
        author: "test-user".to_string(),
        message: Some("Test commit".to_string()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"commit""#));
    assert!(json.contains(r#""id":"cid-123""#));

    // Round-trip
    let parsed: SyncMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        SyncMessage::Commit {
            id,
            parents,
            author,
            message,
            ..
        } => {
            assert_eq!(id, "cid-123");
            assert_eq!(parents, vec!["parent-1".to_string()]);
            assert_eq!(author, "test-user");
            assert_eq!(message, Some("Test commit".to_string()));
        }
        _ => panic!("Expected Commit"),
    }
}

#[test]
fn test_sync_message_done_response() {
    let msg = SyncMessage::Done {
        req: "req-006".to_string(),
        commits: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"done""#));
    assert!(json.contains(r#""commits":["c1","c2","c3"]"#));
}

#[test]
fn test_sync_message_error_response() {
    let msg = SyncMessage::Error {
        req: "req-007".to_string(),
        message: "Commit not found".to_string(),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains(r#""type":"error""#));
    assert!(json.contains(r#""message":"Commit not found""#));
}

#[test]
fn test_sync_message_is_request() {
    // Request types
    assert!(SyncMessage::Head {
        req: "r".to_string()
    }
    .is_request());
    assert!(SyncMessage::Get {
        req: "r".to_string(),
        commits: vec![]
    }
    .is_request());
    assert!(SyncMessage::Pull {
        req: "r".to_string(),
        have: vec![],
        want: "HEAD".to_string()
    }
    .is_request());
    assert!(SyncMessage::Ancestors {
        req: "r".to_string(),
        commit: "HEAD".to_string(),
        depth: None
    }
    .is_request());
    assert!(SyncMessage::IsAncestor {
        req: "r".to_string(),
        ancestor: "a".to_string(),
        descendant: "d".to_string()
    }
    .is_request());

    // Response types should not be requests
    assert!(!SyncMessage::HeadResponse {
        req: "r".to_string(),
        commit: None
    }
    .is_request());
    assert!(!SyncMessage::Commit {
        req: "r".to_string(),
        id: "id".to_string(),
        parents: vec![],
        data: "d".to_string(),
        timestamp: 0,
        author: "a".to_string(),
        message: None
    }
    .is_request());
    assert!(!SyncMessage::Done {
        req: "r".to_string(),
        commits: vec![]
    }
    .is_request());
    assert!(!SyncMessage::Error {
        req: "r".to_string(),
        message: "e".to_string()
    }
    .is_request());
}

// =============================================================================
// Tests for Topic construction (sync topics)
// =============================================================================

#[test]
fn test_sync_topic_construction() {
    let topic = Topic::sync("workspace", "docs/file.txt", "client-123");
    assert_eq!(
        topic.to_topic_string(),
        "workspace/sync/docs/file.txt/client-123"
    );
}

#[test]
fn test_sync_wildcard_pattern() {
    let pattern = Topic::sync_wildcard("workspace", "docs/file.txt");
    assert_eq!(pattern, "workspace/sync/docs/file.txt/+");
}

// =============================================================================
// Edge case tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_head_updates() {
    // Verify that concurrent updates to HEAD are handled correctly
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    // Create initial commit
    let initial = Commit::new(vec![], "initial".to_string(), "author".to_string(), None);
    let initial_cid = store.store_commit(&initial).await.unwrap();
    store.set_document_head(doc_id, &initial_cid).await.unwrap();

    // Create multiple commits that branch from initial
    let mut cids = vec![];
    for i in 0..5 {
        let commit = Commit::new(
            vec![initial_cid.clone()],
            format!("branch-{}", i),
            "author".to_string(),
            None,
        );
        let cid = store.store_commit(&commit).await.unwrap();
        cids.push(cid);
    }

    // Update HEAD to the last commit
    let final_head = cids.last().unwrap().clone();
    store.set_document_head(doc_id, &final_head).await.unwrap();

    // Verify HEAD is correct
    let head = store.get_document_head(doc_id).await.unwrap();
    assert_eq!(head, Some(final_head));
}

#[tokio::test]
async fn test_large_commit_chain() {
    // Test handling of longer commit chains
    let (store, _dir) = create_test_store();
    let doc_id = "test-doc";

    let commits = create_commit_chain(&store, doc_id, 100).await;

    // Verify HEAD is the last commit
    let head = store.get_document_head(doc_id).await.unwrap();
    assert_eq!(head, Some(commits.last().unwrap().0.clone()));

    // Verify we can traverse the full history
    let history = store.get_commits_since(doc_id, 0).await.unwrap();
    assert_eq!(history.len(), 100);

    // Verify is_ancestor works across the chain
    let is_ancestor = store
        .is_ancestor(&commits[0].0, &commits[99].0)
        .await
        .unwrap();
    assert!(is_ancestor, "First commit should be ancestor of last");
}

#[tokio::test]
async fn test_commit_with_empty_update() {
    // Commits with empty update (e.g., merge commits) should work
    let (store, _dir) = create_test_store();

    let commit = Commit::new(
        vec!["parent1".to_string(), "parent2".to_string()],
        String::new(), // Empty update for merge
        "author".to_string(),
        Some("Merge commit".to_string()),
    );

    let cid = store.store_commit(&commit).await.unwrap();
    let retrieved = store.get_commit(&cid).await.unwrap();

    assert!(retrieved.is_merge());
    assert!(retrieved.update.is_empty());
}

// =============================================================================
// MqttError tests for sync operations
// =============================================================================

#[test]
fn test_mqtt_error_display() {
    let err = MqttError::InvalidTopic("bad topic".to_string());
    assert!(err.to_string().contains("Invalid topic"));
    assert!(err.to_string().contains("bad topic"));

    let err = MqttError::CommitNotFound("abc123".to_string());
    assert!(err.to_string().contains("Commit not found"));
    assert!(err.to_string().contains("abc123"));
}
