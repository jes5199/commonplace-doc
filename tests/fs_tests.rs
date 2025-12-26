//! Integration tests for filesystem-in-JSON feature.

use commonplace_doc::{
    create_router_with_config,
    document::ContentType,
    fs::FilesystemReconciler,
    node::{NodeId, NodeRegistry},
    RouterConfig,
};
use std::sync::Arc;

/// Test that reconciler creates nodes from filesystem JSON.
#[tokio::test]
async fn test_reconciler_creates_nodes() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    // Create fs-root node
    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
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

    // Verify nodes were created with derived IDs
    let notes_id = NodeId::new("test-fs:notes.txt");
    let data_id = NodeId::new("test-fs:data.json");

    assert!(
        registry.get(&notes_id).await.is_some(),
        "notes.txt node should exist"
    );
    assert!(
        registry.get(&data_id).await.is_some(),
        "data.json node should exist"
    );
}

/// Test that explicit node_id is used instead of derived ID.
#[tokio::test]
async fn test_reconciler_uses_explicit_node_id() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
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
    let explicit_id = NodeId::new("my-stable-id");
    let derived_id = NodeId::new("test-fs:stable.txt");

    assert!(
        registry.get(&explicit_id).await.is_some(),
        "explicit node ID should exist"
    );
    assert!(
        registry.get(&derived_id).await.is_none(),
        "derived ID should NOT exist"
    );
}

/// Test that removing entry from JSON doesn't delete the node.
#[tokio::test]
async fn test_reconciler_non_destructive() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
    ));

    // First, create a node
    let content1 = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "file.txt": { "type": "doc" }
            }
        }
    }"#;

    reconciler.reconcile(content1).await.unwrap();

    let file_id = NodeId::new("test-fs:file.txt");
    assert!(registry.get(&file_id).await.is_some());

    // Remove entry from JSON
    let content2 = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    }"#;

    reconciler.reconcile(content2).await.unwrap();

    // Node should still exist (non-destructive)
    assert!(
        registry.get(&file_id).await.is_some(),
        "node should NOT be deleted"
    );
}

/// Test that invalid JSON triggers error.
#[tokio::test]
async fn test_reconciler_handles_invalid_json() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
    ));

    let result = reconciler.reconcile("not valid json").await;
    assert!(result.is_err());
}

/// Test that invalid entry names are rejected.
#[tokio::test]
async fn test_reconciler_rejects_invalid_names() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
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
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
    ));

    let content = r#"{"version": 99}"#;

    let result = reconciler.reconcile(content).await;
    assert!(result.is_err());
}

/// Test that non-directory root is rejected.
#[tokio::test]
async fn test_reconciler_rejects_non_directory_root() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
    ));

    // Root is a doc instead of a dir
    let content = r#"{
        "version": 1,
        "root": { "type": "doc" }
    }"#;

    let result = reconciler.reconcile(content).await;
    assert!(result.is_err(), "non-directory root should be rejected");
}

/// Test nested directories.
#[tokio::test]
async fn test_reconciler_nested_dirs() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
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

    let deep_id = NodeId::new("test-fs:level1/level2/deep.txt");
    assert!(
        registry.get(&deep_id).await.is_some(),
        "deeply nested node should exist"
    );
}

/// Test that nodes are recreated if deleted externally.
#[tokio::test]
async fn test_reconciler_recreates_deleted_nodes() {
    let registry = Arc::new(NodeRegistry::new());
    let fs_root_id = NodeId::new("test-fs");

    registry
        .get_or_create_document(&fs_root_id, ContentType::Json)
        .await
        .unwrap();

    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        registry.clone(),
    ));

    let content = r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "file.txt": { "type": "doc" }
            }
        }
    }"#;

    // First reconcile creates the node
    reconciler.reconcile(content).await.unwrap();

    let file_id = NodeId::new("test-fs:file.txt");
    assert!(registry.get(&file_id).await.is_some());

    // Delete the node externally (simulating DELETE /nodes/:id)
    registry.unregister(&file_id).await.ok();
    assert!(
        registry.get(&file_id).await.is_none(),
        "node should be deleted"
    );

    // Re-reconcile should recreate the node
    reconciler.reconcile(content).await.unwrap();
    assert!(
        registry.get(&file_id).await.is_some(),
        "node should be recreated after external deletion"
    );
}

/// Test that router with fs_root creates the fs-root node.
#[tokio::test]
async fn test_router_with_fs_root() {
    let _app = create_router_with_config(RouterConfig {
        commit_store: None,
        fs_root: Some("my-filesystem".to_string()),
    })
    .await;

    // The router should have created the fs-root node internally
    // This test mainly verifies that the async setup doesn't panic
}
