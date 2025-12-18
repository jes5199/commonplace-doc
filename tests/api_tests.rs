use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use tower::util::ServiceExt;
use yrs::Map;
use yrs::Text;
use yrs::Transact;
use yrs::XmlElementPrelim;
use yrs::XmlFragment;
use yrs::XmlTextPrelim;

// Helper to create test app
fn create_app() -> axum::Router {
    commonplace_doc::create_router()
}

fn create_app_with_commit_store() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let store = commonplace_doc::store::CommitStore::new(&path).unwrap();
    (commonplace_doc::create_router_with_store(Some(store)), dir)
}

// Helper to get response body as string
async fn body_to_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn test_health_check() {
    let app = create_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "OK");
}

#[tokio::test]
async fn test_create_json_document() {
    let app = create_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["id"].is_string());
}

#[tokio::test]
async fn test_create_xml_document() {
    let app = create_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["id"].is_string());
}

#[tokio::test]
async fn test_create_text_document() {
    let app = create_app();

    let response = app
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

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["id"].is_string());
}

#[tokio::test]
async fn test_create_unsupported_content_type() {
    let app = create_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/pdf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn test_get_json_document() {
    let app = create_app();

    // First create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Now get the document
    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        get_response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = body_to_string(get_response.into_body()).await;
    assert_eq!(body, "{}");
}

#[tokio::test]
async fn test_get_xml_document() {
    let app = create_app();

    // First create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap();

    // Now get the document
    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        get_response.headers().get("content-type").unwrap(),
        "application/xml"
    );
    let body = body_to_string(get_response.into_body()).await;
    assert_eq!(body, r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#);
}

#[tokio::test]
async fn test_get_text_document() {
    let app = create_app();

    // First create a document
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

    // Now get the document
    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        get_response.headers().get("content-type").unwrap(),
        "text/plain"
    );
    let body = body_to_string(get_response.into_body()).await;
    assert_eq!(body, "");
}

#[tokio::test]
async fn test_get_nonexistent_document() {
    let app = create_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/docs/nonexistent-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_document() {
    let app = create_app();

    // First create a document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap().to_string();

    // Delete the document
    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_document() {
    let app = create_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/docs/nonexistent-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_text_document_yjs_commit_updates_document_body() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a text document
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

    assert_eq!(create_response.status(), StatusCode::OK);
    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap().to_string();

    // Generate a Yrs update that inserts "hello" into the "content" text root.
    let ydoc = yrs::Doc::new();
    let text = ydoc.get_or_insert_text("content");
    let mut txn = ydoc.transact_mut();
    text.push(&mut txn, "hello");
    let update = txn.encode_update_v1();
    let update_b64 = commonplace_doc::b64::encode(&update);

    // Commit update
    let commit_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/commit", doc_id))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "verb": "update",
                        "value": update_b64,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit_response.status(), StatusCode::OK);

    // Verify document body reflects the committed update
    let get_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(get_response.headers().get("content-type").unwrap(), "text/plain");
    let body = body_to_string(get_response.into_body()).await;
    assert_eq!(body, "hello");
}

#[tokio::test]
async fn test_json_document_yjs_commit_updates_document_body() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a JSON document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::OK);
    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap().to_string();

    // Create update that sets {"hello":"world"} via Y.Map("content")
    let ydoc = yrs::Doc::new();
    let map = ydoc.get_or_insert_map("content");
    let mut txn = ydoc.transact_mut();
    map.insert(&mut txn, "hello", "world");
    let update_b64 = commonplace_doc::b64::encode(&txn.encode_update_v1());

    let commit_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/commit", doc_id))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "verb": "update",
                        "value": update_b64,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit_response.status(), StatusCode::OK);

    let get_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        get_response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = body_to_string(get_response.into_body()).await;
    assert_eq!(body, r#"{"hello":"world"}"#);
}

#[tokio::test]
async fn test_xml_document_yjs_commit_updates_document_body() {
    let (app, _dir) = create_app_with_commit_store();

    // Create an XML document
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("content-type", "application/xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::OK);
    let create_body = body_to_string(create_response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&create_body).unwrap();
    let doc_id = json["id"].as_str().unwrap().to_string();

    let ydoc = yrs::Doc::new();
    let fragment = ydoc.get_or_insert_xml_fragment("content");
    let mut txn = ydoc.transact_mut();
    let hello = fragment.push_back(&mut txn, XmlElementPrelim::empty("hello"));
    hello.push_back(&mut txn, XmlTextPrelim::new("world"));
    let update_b64 = commonplace_doc::b64::encode(&txn.encode_update_v1());

    let commit_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/commit", doc_id))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "verb": "update",
                        "value": update_b64,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit_response.status(), StatusCode::OK);

    let get_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        get_response.headers().get("content-type").unwrap(),
        "application/xml"
    );
    let body = body_to_string(get_response.into_body()).await;
    assert_eq!(
        body,
        r#"<?xml version="1.0" encoding="UTF-8"?><root><hello>world</hello></root>"#
    );
}

// ============================================================================
// Replace endpoint integration tests
// ============================================================================

/// Helper to create a text node and return its ID
async fn create_text_node(app: &axum::Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/nodes")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "node_type": "document",
                        "content_type": "text/plain"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    json["id"].as_str().unwrap().to_string()
}

/// Helper to send an edit (Yjs update) to a node and get the commit CID
async fn send_edit(app: &axum::Router, node_id: &str, update_b64: &str) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/nodes/{}/edit", node_id))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "update": update_b64,
                        "author": "test"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    json["cid"].as_str().unwrap().to_string()
}

/// Helper to create a Yjs text update
fn create_yjs_text_update(text: &str) -> String {
    let ydoc = yrs::Doc::new();
    let ytext = ydoc.get_or_insert_text("content");
    let mut txn = ydoc.transact_mut();
    ytext.push(&mut txn, text);
    commonplace_doc::b64::encode(&txn.encode_update_v1())
}

#[tokio::test]
async fn test_replace_content_basic() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a text node
    let node_id = create_text_node(&app).await;

    // Initialize with "hello world"
    let initial_update = create_yjs_text_update("hello world");
    let cid = send_edit(&app, &node_id, &initial_update).await;

    // Replace with "hello rust" - using query params and raw body
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/nodes/{}/replace?parent_cid={}&author=test",
                    node_id, cid
                ))
                .header("content-type", "text/plain")
                .body(Body::from("hello rust"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Verify response structure
    assert!(json["cid"].is_string());
    assert!(json["edit_cid"].is_string());
    assert!(json["summary"]["chars_deleted"].is_number());
    assert!(json["summary"]["chars_inserted"].is_number());

    // For fast-forward case, cid == edit_cid (no merge needed)
    assert_eq!(json["cid"], json["edit_cid"]);
}

#[tokio::test]
async fn test_replace_content_no_change() {
    let (app, _dir) = create_app_with_commit_store();

    let node_id = create_text_node(&app).await;

    // Initialize with "hello"
    let initial_update = create_yjs_text_update("hello");
    let cid = send_edit(&app, &node_id, &initial_update).await;

    // Replace with identical content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/nodes/{}/replace?parent_cid={}&author=test",
                    node_id, cid
                ))
                .header("content-type", "text/plain")
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Should have zero changes
    assert_eq!(json["summary"]["chars_deleted"].as_i64().unwrap(), 0);
    assert_eq!(json["summary"]["chars_inserted"].as_i64().unwrap(), 0);
}

#[tokio::test]
async fn test_replace_content_complete_rewrite() {
    let (app, _dir) = create_app_with_commit_store();

    let node_id = create_text_node(&app).await;

    // Initialize with some content
    let initial_update = create_yjs_text_update("old content here");
    let cid = send_edit(&app, &node_id, &initial_update).await;

    // Complete rewrite
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/nodes/{}/replace?parent_cid={}&author=test",
                    node_id, cid
                ))
                .header("content-type", "text/plain")
                .body(Body::from("completely new text"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Verify significant changes occurred
    assert!(json["summary"]["chars_deleted"].as_i64().unwrap() > 0);
    assert!(json["summary"]["chars_inserted"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn test_replace_nonexistent_node() {
    let (app, _dir) = create_app_with_commit_store();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/nodes/nonexistent-node/replace?parent_cid=some-cid&author=test")
                .header("content-type", "text/plain")
                .body(Body::from("new content"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_replace_invalid_parent_cid() {
    let (app, _dir) = create_app_with_commit_store();

    let node_id = create_text_node(&app).await;

    // Initialize with some content
    let initial_update = create_yjs_text_update("hello");
    let _cid = send_edit(&app, &node_id, &initial_update).await;

    // Try to replace with invalid parent_cid
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/nodes/{}/replace?parent_cid=invalid-cid-that-does-not-exist&author=test",
                    node_id
                ))
                .header("content-type", "text/plain")
                .body(Body::from("new content"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_node_head_empty() {
    let (app, _dir) = create_app_with_commit_store();

    // Create a text node but don't add any content
    let node_id = create_text_node(&app).await;

    // Get head - should return null cid and empty content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/nodes/{}/head", node_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert!(json["cid"].is_null());
    assert_eq!(json["content"].as_str().unwrap(), "");
}

#[tokio::test]
async fn test_get_node_head_with_content() {
    let (app, _dir) = create_app_with_commit_store();

    let node_id = create_text_node(&app).await;

    // Add content
    let initial_update = create_yjs_text_update("hello world");
    let cid = send_edit(&app, &node_id, &initial_update).await;

    // Get head - should return the cid and content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/nodes/{}/head", node_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(json["cid"].as_str().unwrap(), cid);
    assert_eq!(json["content"].as_str().unwrap(), "hello world");
}

#[tokio::test]
async fn test_get_node_head_after_replace() {
    let (app, _dir) = create_app_with_commit_store();

    let node_id = create_text_node(&app).await;

    // Initialize with content
    let initial_update = create_yjs_text_update("hello");
    let cid = send_edit(&app, &node_id, &initial_update).await;

    // Replace with new content
    let replace_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/nodes/{}/replace?parent_cid={}&author=test",
                    node_id, cid
                ))
                .header("content-type", "text/plain")
                .body(Body::from("hello world"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(replace_response.status(), StatusCode::OK);
    let replace_body = body_to_string(replace_response.into_body()).await;
    let replace_json: serde_json::Value = serde_json::from_str(&replace_body).unwrap();
    let new_cid = replace_json["cid"].as_str().unwrap();

    // Get head - should return the new cid and updated content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/nodes/{}/head", node_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(json["cid"].as_str().unwrap(), new_cid);
    assert_eq!(json["content"].as_str().unwrap(), "hello world");
}

#[tokio::test]
async fn test_get_node_head_nonexistent_node() {
    let (app, _dir) = create_app_with_commit_store();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/nodes/nonexistent-node/head")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
