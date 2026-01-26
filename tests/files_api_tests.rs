//! Tests for the /files path-based API endpoints.
//!
//! These tests cover:
//! - Path resolution through nested node-backed directories
//! - URL-encoded paths with special characters
//! - /files/*path GET, DELETE, POST operations
//! - /files/*path/head, /files/*path/edit, /files/*path/replace suffixes

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use commonplace_doc::{create_router_with_config, RouterConfig};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

/// Helper to get response body as string.
async fn body_to_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Helper to create an app with fs-root and commit store for /files tests.
async fn create_app_with_fs_root() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let store = commonplace_doc::store::CommitStore::new(&path).unwrap();
    let router = create_router_with_config(RouterConfig {
        commit_store: Some(store),
        fs_root: Some("test-fs-root".to_string()),
        mqtt: None,
        mqtt_subscribe: vec![],
        static_dir: None,
    })
    .await;
    (router, dir)
}

/// Helper to set up a schema in the fs-root document.
async fn setup_fs_root_schema(app: &axum::Router, schema: &str) {
    // Get current HEAD for parent_cid (required after first commit)
    let head_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/docs/test-fs-root/head")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let parent_cid = if head_response.status().is_success() {
        let head_body = body_to_string(head_response.into_body()).await;
        let head_json: serde_json::Value = serde_json::from_str(&head_body).unwrap_or_default();
        head_json["cid"].as_str().map(|s| s.to_string())
    } else {
        None
    };

    // Use replace endpoint to set the fs-root schema
    let uri = if let Some(cid) = parent_cid {
        format!("/docs/test-fs-root/replace?parent_cid={}", cid)
    } else {
        "/docs/test-fs-root/replace".to_string()
    };

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "text/plain")
                .body(Body::from(schema.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response.status().is_success(),
        "Failed to set fs-root schema: {:?}",
        body_to_string(response.into_body()).await
    );
}

/// Test that /files returns 503 when no fs-root is configured.
#[tokio::test]
async fn test_files_no_fs_root_returns_service_unavailable() {
    let app = commonplace_doc::create_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/test.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Test that /files returns 404 for non-existent paths.
#[tokio::test]
async fn test_files_nonexistent_path_returns_not_found() {
    let (app, _dir) = create_app_with_fs_root().await;

    // Set up a minimal fs-root schema
    setup_fs_root_schema(
        &app,
        r#"{
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    }"#,
    )
    .await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/nonexistent.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test GET /files/*path retrieves file content.
#[tokio::test]
async fn test_files_get_content() {
    let (app, _dir) = create_app_with_fs_root().await;

    // Create a document to be referenced
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

    // Set content on the document
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .header("content-type", "text/plain")
                .body(Body::from("Hello from file!"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Set up fs-root schema pointing to this document
    let schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "hello.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        doc_id
    );

    // Get the current HEAD for parent_cid
    let head_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/docs/test-fs-root/head")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let head_body = body_to_string(head_response.into_body()).await;
    let head_json: serde_json::Value = serde_json::from_str(&head_body).unwrap();
    let parent_cid = head_json["cid"].as_str();

    // Set the schema (with parent_cid if available)
    let uri = if let Some(cid) = parent_cid {
        format!("/docs/test-fs-root/replace?parent_cid={}", cid)
    } else {
        "/docs/test-fs-root/replace".to_string()
    };

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "text/plain")
                .body(Body::from(schema))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Now GET /files/hello.txt
    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "Hello from file!");
}

/// Test GET /files/*path/head returns document HEAD info.
#[tokio::test]
async fn test_files_get_head() {
    let (app, _dir) = create_app_with_fs_root().await;

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

    // Set content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .header("content-type", "text/plain")
                .body(Body::from("test content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Set up fs-root schema
    let schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "test.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        doc_id
    );
    setup_fs_root_schema(&app, &schema).await;

    // GET /files/test.txt/head
    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/test.txt/head")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["cid"].is_string());
    assert_eq!(json["content"].as_str().unwrap(), "test content");
}

/// Test nested node-backed directories (level1/level2/file.txt).
#[tokio::test]
async fn test_files_nested_node_backed_directories() {
    let (app, _dir) = create_app_with_fs_root().await;

    // Create the nested directory document (level2)
    let create_level2 = app
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

    let level2_body = body_to_string(create_level2.into_body()).await;
    let level2_json: serde_json::Value = serde_json::from_str(&level2_body).unwrap();
    let level2_id = level2_json["id"].as_str().unwrap();

    // Create the file document
    let create_file = app
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

    let file_body = body_to_string(create_file.into_body()).await;
    let file_json: serde_json::Value = serde_json::from_str(&file_body).unwrap();
    let file_id = file_json["id"].as_str().unwrap();

    // Set file content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", file_id))
                .header("content-type", "text/plain")
                .body(Body::from("deeply nested content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Set level2 schema (contains the file)
    let level2_schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "deep.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        file_id
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", level2_id))
                .header("content-type", "text/plain")
                .body(Body::from(level2_schema))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Create level1 directory document
    let create_level1 = app
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

    let level1_body = body_to_string(create_level1.into_body()).await;
    let level1_json: serde_json::Value = serde_json::from_str(&level1_body).unwrap();
    let level1_id = level1_json["id"].as_str().unwrap();

    // Set level1 schema (points to level2)
    let level1_schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "level2": {{ "type": "dir", "node_id": "{}" }}
            }}
        }}
    }}"#,
        level2_id
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", level1_id))
                .header("content-type", "text/plain")
                .body(Body::from(level1_schema))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Set fs-root schema (points to level1)
    let fs_root_schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "level1": {{ "type": "dir", "node_id": "{}" }}
            }}
        }}
    }}"#,
        level1_id
    );
    setup_fs_root_schema(&app, &fs_root_schema).await;

    // GET /files/level1/level2/deep.txt
    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/level1/level2/deep.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "deeply nested content");
}

/// Test URL-encoded paths (spaces, special characters).
#[tokio::test]
async fn test_files_url_encoded_paths() {
    let (app, _dir) = create_app_with_fs_root().await;

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

    // Set content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .header("content-type", "text/plain")
                .body(Body::from("special file content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Set up fs-root schema with filename containing spaces
    // Note: The entry name in JSON is the actual filename (not encoded)
    let schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "my file.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        doc_id
    );
    setup_fs_root_schema(&app, &schema).await;

    // GET /files/my%20file.txt (URL-encoded space)
    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/my%20file.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "special file content");
}

/// Test DELETE /files/*path.
#[tokio::test]
async fn test_files_delete() {
    let (app, _dir) = create_app_with_fs_root().await;

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

    // Set up fs-root schema
    let schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "deleteme.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        doc_id
    );
    setup_fs_root_schema(&app, &schema).await;

    // DELETE /files/deleteme.txt
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/files/deleteme.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify document is gone
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/docs/{}", doc_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test POST /files/*path/replace.
#[tokio::test]
async fn test_files_replace() {
    let (app, _dir) = create_app_with_fs_root().await;

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

    // Set initial content
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .header("content-type", "text/plain")
                .body(Body::from("initial content"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Set up fs-root schema
    let schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "replaceme.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        doc_id
    );
    setup_fs_root_schema(&app, &schema).await;

    // Get HEAD for parent_cid
    let head_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/files/replaceme.txt/head")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let head_body = body_to_string(head_response.into_body()).await;
    let head_json: serde_json::Value = serde_json::from_str(&head_body).unwrap();
    let parent_cid = head_json["cid"].as_str().unwrap();

    // POST /files/replaceme.txt/replace
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/files/replaceme.txt/replace?parent_cid={}",
                    parent_cid
                ))
                .header("content-type", "text/plain")
                .body(Body::from("replaced content"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify content was replaced
    let response = app
        .oneshot(
            Request::builder()
                .uri("/files/replaceme.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "replaced content");
}

/// Test that file extension affects Content-Type header.
#[tokio::test]
async fn test_files_content_type_by_extension() {
    let (app, _dir) = create_app_with_fs_root().await;

    // Create documents for different file types
    let extensions = vec![
        ("test.ts", "text/typescript"),
        ("test.js", "text/javascript"),
        ("test.py", "text/x-python"),
        ("test.rs", "text/x-rust"),
        ("test.md", "text/markdown"),
        ("test.css", "text/css"),
    ];

    for (filename, expected_mime) in extensions {
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

        // Set up fs-root schema
        let schema = format!(
            r#"{{
            "version": 1,
            "root": {{
                "type": "dir",
                "entries": {{
                    "{}": {{ "type": "doc", "node_id": "{}" }}
                }}
            }}
        }}"#,
            filename, doc_id
        );
        setup_fs_root_schema(&app, &schema).await;

        // GET the file and check Content-Type
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/files/{}", filename))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(
            content_type, expected_mime,
            "Expected {} for {}, got {}",
            expected_mime, filename, content_type
        );
    }
}

/// Test POST to unknown endpoint returns 404.
#[tokio::test]
async fn test_files_unknown_post_endpoint() {
    let (app, _dir) = create_app_with_fs_root().await;

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

    // Set up fs-root schema
    let schema = format!(
        r#"{{
        "version": 1,
        "root": {{
            "type": "dir",
            "entries": {{
                "test.txt": {{ "type": "doc", "node_id": "{}" }}
            }}
        }}
    }}"#,
        doc_id
    );
    setup_fs_root_schema(&app, &schema).await;

    // POST to /files/test.txt (no suffix) should return 404
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/files/test.txt")
                .header("content-type", "text/plain")
                .body(Body::from("some content"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
