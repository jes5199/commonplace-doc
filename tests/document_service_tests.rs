//! Tests for DocumentService layer.
//!
//! These tests cover:
//! - Commit broadcasting behavior
//! - Service error handling
//! - Edit and replace operations
//! - HEAD operations with at_commit
//! - Fork operations
//! - Parent CID requirement for CRDT safety

use commonplace_doc::{
    b64,
    document::{ContentType, DocumentStore},
    events::CommitBroadcaster,
    services::DocumentService,
    store::CommitStore,
};
use std::sync::Arc;
use yrs::{Doc, Text, Transact};

/// Helper to create a Yjs text update.
fn create_text_update(text: &str) -> Vec<u8> {
    let doc = Doc::new();
    let ytext = doc.get_or_insert_text("content");
    let mut txn = doc.transact_mut();
    ytext.push(&mut txn, text);
    txn.encode_update_v1()
}

/// Create a document service with commit store for tests.
fn create_service_with_store() -> (Arc<DocumentService>, Arc<DocumentStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let commit_store = Arc::new(CommitStore::new(&path).unwrap());
    let doc_store = Arc::new(DocumentStore::new());
    let broadcaster = CommitBroadcaster::new(1024);

    let service = Arc::new(DocumentService::new(
        doc_store.clone(),
        Some(commit_store),
        Some(broadcaster),
    ));

    (service, doc_store, dir)
}

/// Create a document service without commit store (no persistence).
fn create_service_without_store() -> (Arc<DocumentService>, Arc<DocumentStore>) {
    let doc_store = Arc::new(DocumentStore::new());
    let service = Arc::new(DocumentService::new(doc_store.clone(), None, None));
    (service, doc_store)
}

/// Test create_document creates a document with correct type.
#[tokio::test]
async fn test_create_document() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    let doc = doc_store.get_document(&id).await;
    assert!(doc.is_some());
    assert_eq!(doc.unwrap().content_type, ContentType::Text);
}

/// Test get_document returns NotFound for non-existent doc.
#[tokio::test]
async fn test_get_document_not_found() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let result = service.get_document("nonexistent").await;
    assert!(result.is_err());
}

/// Test delete_document removes the document.
#[tokio::test]
async fn test_delete_document() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;
    assert!(doc_store.get_document(&id).await.is_some());

    let deleted = service.delete_document(&id).await;
    assert!(deleted);

    assert!(doc_store.get_document(&id).await.is_none());
}

/// Test get_head returns correct content.
#[tokio::test]
async fn test_get_head() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // Initial HEAD should have empty content
    let head = service.get_head(&id, None).await.unwrap();
    assert_eq!(head.content, "");
    assert!(head.cid.is_none()); // No commits yet
}

/// Test edit_document creates commit and updates content.
#[tokio::test]
async fn test_edit_document() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // Make an edit
    let update = create_text_update("hello");
    let update_b64 = b64::encode(&update);

    let result = service
        .edit_document(&id, &update_b64, Some("test".to_string()), None)
        .await
        .unwrap();

    // Should have created a commit
    assert!(!result.cid.is_empty());
    assert!(result.timestamp > 0);

    // Content should be updated
    let doc = doc_store.get_document(&id).await.unwrap();
    assert_eq!(doc.content, "hello");
}

/// Test edit_document fails without commit store.
#[tokio::test]
async fn test_edit_document_no_persistence() {
    let (service, _doc_store) = create_service_without_store();

    let id = service.create_document(ContentType::Text).await;

    let update = create_text_update("hello");
    let update_b64 = b64::encode(&update);

    let result = service.edit_document(&id, &update_b64, None, None).await;

    // Should fail - NoPersistence
    assert!(result.is_err());
}

/// Test replace_content computes diff and creates commit.
#[tokio::test]
async fn test_replace_content() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // Replace content (initial, no parent_cid needed)
    let result = service
        .replace_content(&id, "hello world", None, None)
        .await
        .unwrap();

    assert!(!result.cid.is_empty());
    assert!(result.chars_inserted > 0);

    // Content should be updated
    let doc = doc_store.get_document(&id).await.unwrap();
    assert_eq!(doc.content, "hello world");
}

/// Test replace_content requires parent_cid when document has history.
#[tokio::test]
async fn test_replace_content_requires_parent_cid() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // First replace - no parent needed
    let result = service
        .replace_content(&id, "initial", None, None)
        .await
        .unwrap();
    let cid1 = result.cid;

    // Second replace WITHOUT parent_cid should fail (CRDT safety)
    let result = service.replace_content(&id, "second", None, None).await;

    // Should return ParentRequired error with the current HEAD
    assert!(result.is_err());
    if let Err(commonplace_doc::services::ServiceError::ParentRequired(head)) = result {
        assert_eq!(head, cid1);
    } else {
        panic!("Expected ParentRequired error");
    }

    // Now with correct parent_cid should work
    let result = service
        .replace_content(&id, "second", Some(cid1), None)
        .await;
    assert!(result.is_ok());
}

/// Test replace_content skips empty diffs.
#[tokio::test]
async fn test_replace_content_no_changes() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // First replace
    let result1 = service
        .replace_content(&id, "same content", None, None)
        .await
        .unwrap();
    let cid1 = result1.cid.clone();

    // Replace with same content (should skip creating commit)
    let result2 = service
        .replace_content(&id, "same content", Some(cid1.clone()), None)
        .await
        .unwrap();

    // CID should be the same (no new commit)
    assert_eq!(result2.chars_inserted, 0);
    assert_eq!(result2.chars_deleted, 0);
}

/// Test create_commit with merge handling.
#[tokio::test]
async fn test_create_commit_simple() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    let update = create_text_update("hello");
    let update_b64 = b64::encode(&update);

    // First commit (no parent)
    let result = service
        .create_commit(&id, &update_b64, "test".to_string(), None, None)
        .await
        .unwrap();

    assert!(!result.cid.is_empty());
    assert!(result.merge_cid.is_none());
}

/// Test create_commit requires parent when document has history.
#[tokio::test]
async fn test_create_commit_requires_parent() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    let update1 = create_text_update("hello");
    let update1_b64 = b64::encode(&update1);

    // First commit
    let result1 = service
        .create_commit(&id, &update1_b64, "test".to_string(), None, None)
        .await
        .unwrap();

    let update2 = create_text_update(" world");
    let update2_b64 = b64::encode(&update2);

    // Second commit WITHOUT parent should fail
    let result2 = service
        .create_commit(&id, &update2_b64, "test".to_string(), None, None)
        .await;

    assert!(result2.is_err());
    if let Err(commonplace_doc::services::ServiceError::ParentRequired(head)) = result2 {
        assert_eq!(head, result1.cid);
    } else {
        panic!("Expected ParentRequired error");
    }
}

/// Test get_head with at_commit returns historical state.
#[tokio::test]
async fn test_get_head_at_commit() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // First edit
    let update1 = create_text_update("hello");
    let update1_b64 = b64::encode(&update1);
    let result1 = service
        .edit_document(&id, &update1_b64, Some("test".to_string()), None)
        .await
        .unwrap();
    let cid1 = result1.cid;

    // Second edit
    let doc = yrs::Doc::new();
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "hello");
    }
    let update2 = {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, " world");
        txn.encode_update_v1()
    };
    let update2_b64 = b64::encode(&update2);

    service
        .create_commit(
            &id,
            &update2_b64,
            "test".to_string(),
            None,
            Some(cid1.clone()),
        )
        .await
        .unwrap();

    // Get HEAD at first commit (historical state)
    let head_at_cid1 = service.get_head(&id, Some(&cid1)).await.unwrap();
    assert_eq!(head_at_cid1.content, "hello");
    assert_eq!(head_at_cid1.cid, Some(cid1));
}

/// Test get_head at_commit rejects foreign CID.
#[tokio::test]
async fn test_get_head_rejects_foreign_cid() {
    let (service, _doc_store, _dir) = create_service_with_store();

    // Create two documents
    let id1 = service.create_document(ContentType::Text).await;
    let id2 = service.create_document(ContentType::Text).await;

    // Make a commit to doc1
    let update = create_text_update("hello");
    let update_b64 = b64::encode(&update);
    let result = service
        .edit_document(&id1, &update_b64, Some("test".to_string()), None)
        .await
        .unwrap();
    let doc1_cid = result.cid;

    // Make a commit to doc2
    let _result2 = service
        .edit_document(&id2, &update_b64, Some("test".to_string()), None)
        .await
        .unwrap();

    // Try to get doc2's HEAD at doc1's CID - should fail
    let result = service.get_head(&id2, Some(&doc1_cid)).await;
    assert!(result.is_err());
}

/// Test fork_document creates new document at specified commit.
#[tokio::test]
async fn test_fork_document() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // Make initial edit
    let update = create_text_update("hello");
    let update_b64 = b64::encode(&update);
    let result = service
        .edit_document(&id, &update_b64, Some("test".to_string()), None)
        .await
        .unwrap();
    let _cid = result.cid;

    // Fork at HEAD
    let fork_result = service.fork_document(&id, None).await.unwrap();

    // New document should exist
    let forked_doc = doc_store.get_document(&fork_result.id).await.unwrap();
    assert_eq!(forked_doc.content, "hello");
    assert_eq!(forked_doc.content_type, ContentType::Text);

    // Should have its own HEAD
    assert!(!fork_result.head.is_empty());
    assert_ne!(fork_result.id, id);
}

/// Test fork_document fails without commit store.
#[tokio::test]
async fn test_fork_document_no_persistence() {
    let (service, _doc_store) = create_service_without_store();

    let id = service.create_document(ContentType::Text).await;

    let result = service.fork_document(&id, None).await;
    assert!(result.is_err());
}

/// Test commit broadcast is triggered on edit.
#[tokio::test]
async fn test_commit_broadcast_on_edit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let commit_store = Arc::new(CommitStore::new(&path).unwrap());
    let doc_store = Arc::new(DocumentStore::new());
    let broadcaster = CommitBroadcaster::new(1024);

    // Subscribe BEFORE creating service
    let mut receiver = broadcaster.subscribe();

    let service = Arc::new(DocumentService::new(
        doc_store.clone(),
        Some(commit_store),
        Some(broadcaster),
    ));

    let id = service.create_document(ContentType::Text).await;

    // Make an edit
    let update = create_text_update("hello");
    let update_b64 = b64::encode(&update);

    let result = service
        .edit_document(&id, &update_b64, Some("test".to_string()), None)
        .await
        .unwrap();

    // Should receive broadcast notification
    let notification = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .expect("timeout waiting for broadcast")
        .expect("failed to receive broadcast");

    assert_eq!(notification.doc_id, id);
    assert_eq!(notification.commit_id, result.cid);
}

/// Test commit broadcast is triggered on replace.
#[tokio::test]
async fn test_commit_broadcast_on_replace() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let commit_store = Arc::new(CommitStore::new(&path).unwrap());
    let doc_store = Arc::new(DocumentStore::new());
    let broadcaster = CommitBroadcaster::new(1024);

    // Subscribe BEFORE creating service
    let mut receiver = broadcaster.subscribe();

    let service = Arc::new(DocumentService::new(
        doc_store.clone(),
        Some(commit_store),
        Some(broadcaster),
    ));

    let id = service.create_document(ContentType::Text).await;

    let result = service
        .replace_content(&id, "hello world", None, None)
        .await
        .unwrap();

    // Should receive broadcast notification
    let notification = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .expect("timeout waiting for broadcast")
        .expect("failed to receive broadcast");

    assert_eq!(notification.doc_id, id);
    assert_eq!(notification.commit_id, result.cid);
}

/// Test JSON document type works with replace.
#[tokio::test]
async fn test_json_document_replace() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Json).await;

    // Replace with JSON content
    let result = service
        .replace_content(&id, r#"{"key": "value"}"#, None, None)
        .await
        .unwrap();

    assert!(!result.cid.is_empty());

    let doc = doc_store.get_document(&id).await.unwrap();
    // JSON might be reordered, so parse and compare
    let expected: serde_json::Value = serde_json::from_str(r#"{"key": "value"}"#).unwrap();
    let actual: serde_json::Value = serde_json::from_str(&doc.content).unwrap();
    assert_eq!(actual, expected);
}

/// Test XML document type works with replace.
#[tokio::test]
async fn test_xml_document_replace() {
    let (service, doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Xml).await;

    // Replace with XML content
    let result = service
        .replace_content(
            &id,
            r#"<?xml version="1.0" encoding="UTF-8"?><root><item>value</item></root>"#,
            None,
            None,
        )
        .await
        .unwrap();

    assert!(!result.cid.is_empty());

    let doc = doc_store.get_document(&id).await.unwrap();
    assert!(doc.content.contains("<item>value</item>"));
}

/// Test invalid update is rejected.
#[tokio::test]
async fn test_invalid_update_rejected() {
    let (service, _doc_store, _dir) = create_service_with_store();

    let id = service.create_document(ContentType::Text).await;

    // Invalid base64
    let result = service
        .edit_document(&id, "not-valid-base64!!!", None, None)
        .await;

    assert!(result.is_err());
}
