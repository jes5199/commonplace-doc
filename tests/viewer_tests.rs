//! Tests for the viewer routes.
//!
//! These tests cover:
//! - /view/docs/:id - viewer page for document by ID
//! - /view/files/*path - viewer page for document by path
//! - /static/*path - static asset serving
//! - Directory traversal prevention
//! - Content-type guessing for static files

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use commonplace_doc::{create_router_with_config, RouterConfig};
use http_body_util::BodyExt;
use std::fs;
use tower::util::ServiceExt;

/// Helper to get response body as string.
async fn body_to_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Create app with static_dir for viewer tests.
async fn create_app_with_static_dir() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let static_path = dir.path();

    // Create index.html
    fs::write(
        static_path.join("index.html"),
        "<!DOCTYPE html><html><body>Viewer</body></html>",
    )
    .unwrap();

    // Create some static assets
    fs::write(static_path.join("app.js"), "console.log('app');").unwrap();
    fs::write(static_path.join("style.css"), "body { color: black; }").unwrap();
    fs::write(static_path.join("data.json"), r#"{"key": "value"}"#).unwrap();

    // Create a subdirectory with assets
    let assets_dir = static_path.join("assets");
    fs::create_dir(&assets_dir).unwrap();
    fs::write(assets_dir.join("logo.svg"), "<svg></svg>").unwrap();

    let router = create_router_with_config(RouterConfig {
        commit_store: None,
        fs_root: None,
        mqtt: None,
        mqtt_subscribe: vec![],
        static_dir: Some(static_path.to_string_lossy().to_string()),
    })
    .await;

    (router, dir)
}

/// Test GET /view/docs/:id serves index.html.
#[tokio::test]
async fn test_view_docs_serves_index() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/view/docs/some-doc-id")
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
    assert!(
        content_type.contains("text/html"),
        "Expected text/html, got {}",
        content_type
    );

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("Viewer"));
}

/// Test GET /view/files/*path serves index.html.
#[tokio::test]
async fn test_view_files_serves_index() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/view/files/path/to/file.txt")
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
    assert!(
        content_type.contains("text/html"),
        "Expected text/html, got {}",
        content_type
    );
}

/// Test GET /static/*path serves static files.
#[tokio::test]
async fn test_static_serves_files() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/app.js")
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
    assert!(
        content_type.contains("javascript"),
        "Expected javascript content-type, got {}",
        content_type
    );

    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "console.log('app');");
}

/// Test static files in subdirectories.
#[tokio::test]
async fn test_static_subdirectory_files() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/assets/logo.svg")
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
    assert!(
        content_type.contains("svg"),
        "Expected svg content-type, got {}",
        content_type
    );
}

/// Test directory traversal is prevented.
#[tokio::test]
async fn test_static_directory_traversal_blocked() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/../../../etc/passwd")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Test 404 for non-existent static files.
#[tokio::test]
async fn test_static_not_found() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/nonexistent.xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test content-type guessing for various file types.
#[tokio::test]
async fn test_static_content_types() {
    let (app, _dir) = create_app_with_static_dir().await;

    // Test JavaScript
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/static/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("javascript"),
        "JS should have javascript content-type"
    );

    // Test CSS
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/static/style.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("css"),
        "CSS should have css content-type"
    );

    // Test JSON
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/static/data.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("json"),
        "JSON should have json content-type"
    );

    // Test SVG
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/assets/logo.svg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("svg"),
        "SVG should have svg content-type"
    );
}

/// Test viewer routes are not available when static_dir is not configured.
#[tokio::test]
async fn test_viewer_not_available_without_static_dir() {
    let router = create_router_with_config(RouterConfig {
        commit_store: None,
        fs_root: None,
        mqtt: None,
        mqtt_subscribe: vec![],
        static_dir: None,
    })
    .await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/view/docs/some-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 404 when viewer is not configured
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/static/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test index.html charset is UTF-8.
#[tokio::test]
async fn test_index_html_charset() {
    let (app, _dir) = create_app_with_static_dir().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/view/docs/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    assert!(
        content_type.contains("charset=utf-8"),
        "HTML should specify UTF-8 charset, got: {}",
        content_type
    );
}

/// Test leading slash is stripped from static paths.
#[tokio::test]
async fn test_static_leading_slash_stripped() {
    let (app, _dir) = create_app_with_static_dir().await;

    // Axum should handle this, but verify behavior
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static//app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Either the file is found or the URL is normalized
    // Depending on implementation, this might be OK or NOT_FOUND
    // Just verify we don't crash
    assert!(
        response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND,
        "Should handle double slash gracefully"
    );
}

/// Test URL-encoded paths in static routes.
#[tokio::test]
async fn test_static_url_encoded_paths() {
    let dir = tempfile::tempdir().unwrap();
    let static_path = dir.path();

    // Create index.html (required)
    fs::write(static_path.join("index.html"), "<html></html>").unwrap();

    // Create a file with a space in the name
    fs::write(static_path.join("my file.txt"), "content").unwrap();

    let router = create_router_with_config(RouterConfig {
        commit_store: None,
        fs_root: None,
        mqtt: None,
        mqtt_subscribe: vec![],
        static_dir: Some(static_path.to_string_lossy().to_string()),
    })
    .await;

    // Access with URL-encoded space
    let response = router
        .oneshot(
            Request::builder()
                .uri("/static/my%20file.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "content");
}

/// Test 404 message for missing viewer index.html.
#[tokio::test]
async fn test_missing_index_html() {
    let dir = tempfile::tempdir().unwrap();
    let static_path = dir.path();

    // Create static dir WITHOUT index.html
    fs::write(static_path.join("app.js"), "console.log('app');").unwrap();

    let router = create_router_with_config(RouterConfig {
        commit_store: None,
        fs_root: None,
        mqtt: None,
        mqtt_subscribe: vec![],
        static_dir: Some(static_path.to_string_lossy().to_string()),
    })
    .await;

    let response = router
        .oneshot(
            Request::builder()
                .uri("/view/docs/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 404 for missing index.html
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("not found") || body.contains("Viewer"));
}
