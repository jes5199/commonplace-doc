//! Integration tests for filesystem-in-JSON feature.

use commonplace_doc::{
    create_router_with_config,
    document::{ContentType, DocumentStore},
    fs::{Entry, FilesystemReconciler, FsSchema},
    RouterConfig,
};
use std::sync::Arc;

/// Check if a string looks like a UUID (36 chars with hyphens)
fn is_uuid_format(s: &str) -> bool {
    s.len() == 36 && s.chars().filter(|c| *c == '-').count() == 4
}

/// Test that reconciler creates documents from filesystem JSON.
#[tokio::test]
async fn test_reconciler_creates_documents() {
    let store = Arc::new(DocumentStore::new());

    // Create fs-root document
    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    // Reconcile with a simple filesystem
    let content = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "notes.txt": { "type": "doc" },
                "data.json": { "type": "doc", "content_type": "application/json" }
            }
        }
    }"#;

    reconciler.reconcile(content).await.unwrap();

    // Get the updated fs-root to find the generated UUIDs
    let fs_root = store.get_document("test-fs").await.unwrap();
    let schema: FsSchema = serde_json::from_str(&fs_root.content).unwrap();

    if let Some(Entry::Dir(root)) = schema.root {
        let entries = root.entries.unwrap();

        // Verify notes.txt got a UUID and document was created
        if let Some(Entry::Doc(notes)) = entries.get("notes.txt") {
            assert!(notes.node_id.is_some(), "notes.txt should have node_id");
            let notes_id = notes.node_id.as_ref().unwrap();
            assert!(is_uuid_format(notes_id), "node_id should be a UUID");
            assert!(
                store.get_document(notes_id).await.is_some(),
                "notes.txt document should exist"
            );
        } else {
            panic!("notes.txt entry should exist");
        }

        // Verify data.json got a UUID and document was created
        if let Some(Entry::Doc(data)) = entries.get("data.json") {
            assert!(data.node_id.is_some(), "data.json should have node_id");
            let data_id = data.node_id.as_ref().unwrap();
            assert!(is_uuid_format(data_id), "node_id should be a UUID");
            assert!(
                store.get_document(data_id).await.is_some(),
                "data.json document should exist"
            );
        } else {
            panic!("data.json entry should exist");
        }
    } else {
        panic!("Expected root directory");
    }
}

/// Test that explicit node_id is used instead of generated UUID.
#[tokio::test]
async fn test_reconciler_uses_explicit_node_id() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    let content = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "stable.txt": {
                    "type": "doc",
                    "node_id": "my-stable-id"
                }
            }
        }
    }"#;

    reconciler.reconcile(content).await.unwrap();

    // Verify explicit ID was used
    assert!(
        store.get_document("my-stable-id").await.is_some(),
        "explicit node ID should exist"
    );
}

/// Test that removing entry from JSON doesn't delete the document.
#[tokio::test]
async fn test_reconciler_non_destructive() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    // First, create a document with an explicit node_id
    let content1 = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "file.txt": { "type": "doc", "node_id": "stable-file-id" }
            }
        }
    }"#;

    reconciler.reconcile(content1).await.unwrap();

    assert!(store.get_document("stable-file-id").await.is_some());

    // Remove entry from JSON
    let content2 = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    }"#;

    reconciler.reconcile(content2).await.unwrap();

    // Document should still exist (non-destructive)
    assert!(
        store.get_document("stable-file-id").await.is_some(),
        "document should NOT be deleted"
    );
}

/// Test that invalid JSON triggers error.
#[tokio::test]
async fn test_reconciler_handles_invalid_json() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    let result = reconciler.reconcile("not valid json").await;
    assert!(result.is_err());
}

/// Test that invalid entry names are rejected.
#[tokio::test]
async fn test_reconciler_rejects_invalid_names() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    // Entry with slash in name
    let content = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "invalid/name": { "type": "doc" }
            }
        }
    }"#;

    let result = reconciler.reconcile(content).await;
    assert!(result.is_err());
}

/// Test that unsupported version is rejected.
#[tokio::test]
async fn test_reconciler_rejects_unsupported_version() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    let content = r#"{"version": 99}"#;

    let result = reconciler.reconcile(content).await;
    assert!(result.is_err());
}

/// Test that non-directory root is rejected.
#[tokio::test]
async fn test_reconciler_rejects_non_directory_root() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    // Root is a doc instead of a dir
    let content = r#"{
        "version": 1,
        "root": { "type": "doc" }
    }"#;

    let result = reconciler.reconcile(content).await;
    assert!(result.is_err(), "non-directory root should be rejected");
}

/// Test nested directories (inline subdirs become node-backed with UUIDs).
#[tokio::test]
async fn test_reconciler_nested_dirs() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    let content = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "level1": {
                    "type": "dir",
                    "entries": {
                        "level2": {
                            "type": "dir",
                            "entries": {
                                "deep.txt": { "type": "doc" }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    reconciler.reconcile(content).await.unwrap();

    // Get the updated fs-root to find the generated UUIDs
    let fs_root = store.get_document("test-fs").await.unwrap();
    let schema: FsSchema = serde_json::from_str(&fs_root.content).unwrap();

    // Navigate through the node-backed directories to find deep.txt
    if let Some(Entry::Dir(root)) = schema.root {
        let entries = root.entries.unwrap();
        if let Some(Entry::Dir(level1)) = entries.get("level1") {
            // level1 should now be node-backed with a UUID
            assert!(level1.node_id.is_some(), "level1 should have node_id");
            let level1_id = level1.node_id.as_ref().unwrap();
            assert!(is_uuid_format(level1_id), "level1 node_id should be a UUID");

            // Get level1's document
            let level1_doc = store.get_document(level1_id).await.unwrap();
            let level1_schema: FsSchema = serde_json::from_str(&level1_doc.content).unwrap();

            if let Some(Entry::Dir(level1_root)) = level1_schema.root {
                let level1_entries = level1_root.entries.unwrap();
                if let Some(Entry::Dir(level2)) = level1_entries.get("level2") {
                    assert!(level2.node_id.is_some(), "level2 should have node_id");
                    let level2_id = level2.node_id.as_ref().unwrap();
                    assert!(is_uuid_format(level2_id), "level2 node_id should be a UUID");

                    // Get level2's document and verify deep.txt exists
                    let level2_doc = store.get_document(level2_id).await.unwrap();
                    let level2_schema: FsSchema =
                        serde_json::from_str(&level2_doc.content).unwrap();

                    if let Some(Entry::Dir(level2_root)) = level2_schema.root {
                        let level2_entries = level2_root.entries.unwrap();
                        if let Some(Entry::Doc(deep)) = level2_entries.get("deep.txt") {
                            assert!(deep.node_id.is_some(), "deep.txt should have node_id");
                            let deep_id = deep.node_id.as_ref().unwrap();
                            assert!(is_uuid_format(deep_id), "deep.txt node_id should be a UUID");
                            assert!(
                                store.get_document(deep_id).await.is_some(),
                                "deeply nested document should exist"
                            );
                        } else {
                            panic!("deep.txt should exist in level2");
                        }
                    } else {
                        panic!("level2 should have root dir");
                    }
                } else {
                    panic!("level2 should exist in level1");
                }
            } else {
                panic!("level1 should have root dir");
            }
        } else {
            panic!("level1 should exist in root");
        }
    } else {
        panic!("Expected root directory");
    }
}

/// Test that documents are recreated if deleted externally.
#[tokio::test]
async fn test_reconciler_recreates_deleted_documents() {
    let store = Arc::new(DocumentStore::new());

    store
        .get_or_create_with_id("test-fs", ContentType::Json)
        .await;

    let reconciler = Arc::new(FilesystemReconciler::new(
        "test-fs".to_string(),
        store.clone(),
    ));

    // Use explicit node_id so we can track the document across reconciles
    let content = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "file.txt": { "type": "doc", "node_id": "stable-file-id" }
            }
        }
    }"#;

    // First reconcile creates the document
    reconciler.reconcile(content).await.unwrap();

    assert!(store.get_document("stable-file-id").await.is_some());

    // Delete the document externally (simulating DELETE /docs/:id)
    store.delete_document("stable-file-id").await;
    assert!(
        store.get_document("stable-file-id").await.is_none(),
        "document should be deleted"
    );

    // Re-reconcile should recreate the document
    reconciler.reconcile(content).await.unwrap();
    assert!(
        store.get_document("stable-file-id").await.is_some(),
        "document should be recreated after external deletion"
    );
}

/// Test that router with fs_root creates the fs-root node.
#[tokio::test]
async fn test_router_with_fs_root() {
    let _app = create_router_with_config(RouterConfig {
        commit_store: None,
        fs_root: Some("my-filesystem".to_string()),
        mqtt: None,
        mqtt_subscribe: vec![],
        static_dir: None,
    })
    .await;

    // The router should have created the fs-root node internally
    // This test mainly verifies that the async setup doesn't panic
}
