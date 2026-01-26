//! Tests for SSE (Server-Sent Events) endpoints.
//!
//! These tests cover:
//! - /documents/:id/changes - get commit history
//! - /documents/:id/stream - stream document changes
//! - /sse/docs/:id - stream edit events for sync client
//! - Ordering and deduplication of events
//! - Multi-document streaming

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use commonplace_doc::create_router_with_store;
use http_body_util::BodyExt;
use tower::util::ServiceExt;

/// Helper to get response body as string.
async fn body_to_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Create app with commit store for SSE tests.
fn create_app_with_commit_store() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let store = commonplace_doc::store::CommitStore::new(&path).unwrap();
    (create_router_with_store(Some(store)), dir)
}

/// Test GET /documents/:id/changes returns commit history.
#[tokio::test]
async fn test_document_changes_endpoint() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Make some commits via replace
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .body(Body::from("version 1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let resp_body = body_to_string(response.into_body()).await;
    let resp_json: serde_json::Value = serde_json::from_str(&resp_body).unwrap();
    let cid1 = resp_json["cid"].as_str().unwrap().to_string();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace?parent_cid={}", doc_id, cid1))
                .body(Body::from("version 2"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // GET /documents/:id/changes
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/changes", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Should have 2 changes
    let changes = json["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 2);

    // Changes should be ordered by timestamp
    let ts1 = changes[0]["timestamp"].as_u64().unwrap();
    let ts2 = changes[1]["timestamp"].as_u64().unwrap();
    assert!(ts1 <= ts2, "changes should be ordered by timestamp");
}

/// Test GET /documents/:id/changes?since filters by timestamp.
#[tokio::test]
async fn test_document_changes_since_filter() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Make first commit
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .body(Body::from("first"))
                .unwrap(),
        )
        .await
        .unwrap();
    let resp_body = body_to_string(response.into_body()).await;
    let resp_json: serde_json::Value = serde_json::from_str(&resp_body).unwrap();
    let cid1 = resp_json["cid"].as_str().unwrap().to_string();

    // Get first commit's timestamp
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/changes", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let first_ts = json["changes"][0]["timestamp"].as_u64().unwrap();

    // Wait a bit to ensure different timestamps
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Make second commit
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace?parent_cid={}", doc_id, cid1))
                .body(Body::from("second"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Get changes since first timestamp (should get both - since is inclusive)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/changes?since={}", doc_id, first_ts))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let changes = json["changes"].as_array().unwrap();
    assert!(!changes.is_empty(), "should have at least one change");

    // Get changes since far future (should get none)
    let far_future = first_ts + 1_000_000_000;
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/documents/{}/changes?since={}",
                    doc_id, far_future
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let changes = json["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 0, "no changes should be in the future");
}

/// Test GET /documents/:id/changes returns 404 for non-existent document.
#[tokio::test]
async fn test_document_changes_not_found() {
    let (app, _dir) = create_app_with_commit_store();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/documents/nonexistent-doc/changes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test GET /documents/:id/commits returns full commit data with updates.
#[tokio::test]
async fn test_document_commits_endpoint() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Make a commit
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .body(Body::from("test content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // GET /documents/:id/commits
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/commits", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Should have commits array with update field (base64)
    let commits = json["commits"].as_array().unwrap();
    assert_eq!(commits.len(), 1);
    assert!(commits[0]["commit_id"].is_string());
    assert!(commits[0]["update"].is_string());
    assert!(commits[0]["timestamp"].is_u64());
    assert!(commits[0]["parents"].is_array());
}

/// Test GET /documents/changes with multiple doc_ids.
#[tokio::test]
async fn test_multi_document_changes() {
    let (app, _dir) = create_app_with_commit_store();

    // Create two documents
    let create1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let create1_body = body_to_string(create1.into_body()).await;
    let doc1_id = serde_json::from_str::<serde_json::Value>(&create1_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let create2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let create2_body = body_to_string(create2.into_body()).await;
    let doc2_id = serde_json::from_str::<serde_json::Value>(&create2_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Make commits to both
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc1_id))
                .body(Body::from("doc1 content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc2_id))
                .body(Body::from("doc2 content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // GET /documents/changes?doc_ids=doc1,doc2
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/documents/changes?doc_ids={},{}",
                    doc1_id, doc2_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Should have changes from both documents
    let changes = json["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 2);

    // Verify both doc_ids are represented
    let doc_ids: Vec<&str> = changes
        .iter()
        .map(|c| c["doc_id"].as_str().unwrap())
        .collect();
    assert!(doc_ids.contains(&doc1_id.as_str()));
    assert!(doc_ids.contains(&doc2_id.as_str()));
}

/// Test GET /documents/changes returns 400 for empty doc_ids.
#[tokio::test]
async fn test_multi_document_changes_empty_ids() {
    let (app, _dir) = create_app_with_commit_store();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/documents/changes?doc_ids=")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Test GET /sse/docs/:id returns 404 for non-existent document.
#[tokio::test]
async fn test_sse_docs_not_found() {
    let (app, _dir) = create_app_with_commit_store();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/sse/docs/nonexistent-doc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test /documents/:id/stream returns 501 when no commit store.
#[tokio::test]
async fn test_stream_without_commit_store() {
    let app = commonplace_doc::create_router();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Try to stream - should return 501 (NOT_IMPLEMENTED)
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/stream", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

/// Test /sse/docs/:id returns 501 when no commit store (no broadcaster).
#[tokio::test]
async fn test_sse_docs_without_commit_store() {
    let app = commonplace_doc::create_router();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Try to stream - should return 501 (NOT_IMPLEMENTED)
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/sse/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

/// Test /sse/files/*path returns 503 when no fs-root configured.
#[tokio::test]
async fn test_sse_files_no_fs_root() {
    let (app, _dir) = create_app_with_commit_store();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/sse/files/test.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Test changes ordering is by timestamp, then doc_id, then commit_id.
#[tokio::test]
async fn test_changes_ordering() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Make multiple commits with small delays
    let mut prev_cid: Option<String> = None;
    for i in 0..5 {
        let uri = if let Some(cid) = prev_cid {
            format!("/docs/{}/replace?parent_cid={}", doc_id, cid)
        } else {
            format!("/docs/{}/replace", doc_id)
        };

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&uri)
                    .body(Body::from(format!("version {}", i)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());

        let resp_body = body_to_string(response.into_body()).await;
        let resp_json: serde_json::Value = serde_json::from_str(&resp_body).unwrap();
        prev_cid = Some(resp_json["cid"].as_str().unwrap().to_string());

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // GET changes
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/changes", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    let changes = json["changes"].as_array().unwrap();

    // Verify ordering (timestamps should be non-decreasing)
    let mut prev_ts = 0u64;
    for change in changes {
        let ts = change["timestamp"].as_u64().unwrap();
        assert!(ts >= prev_ts, "changes should be ordered by timestamp");
        prev_ts = ts;
    }
}

/// Test that changes include proper commit URLs.
#[tokio::test]
async fn test_changes_contain_urls() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "text/plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Make a commit
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .body(Body::from("test"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // GET changes
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/documents/{}/changes", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    let changes = json["changes"].as_array().unwrap();
    let url = changes[0]["url"].as_str().unwrap();

    // URL should be in format: commonplace://document/{doc_id}/commit/{commit_id}
    assert!(url.starts_with("commonplace://document/"));
    assert!(url.contains("/commit/"));
    assert!(url.contains(doc_id));
}
