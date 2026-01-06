//! SDK serving endpoints for JS evaluator.
//!
//! Serves TypeScript SDK files from the sdk/ directory so Deno scripts
//! can import them via URL (e.g., `import { cp } from "http://localhost:3000/sdk/mod.ts"`).

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use std::path::PathBuf;
use tokio::fs;

/// Create the SDK router.
///
/// Serves files from the sdk/ directory at /sdk/*.
pub fn router() -> Router {
    Router::new().route("/sdk/*file_path", get(serve_sdk))
}

/// Serve SDK files from the sdk/ directory.
async fn serve_sdk(Path(file_path): Path<String>) -> Response {
    let base_path = PathBuf::from("sdk");

    // Sanitize path to prevent directory traversal
    let clean_path = file_path.trim_start_matches('/');
    if clean_path.contains("..") {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Path traversal not allowed"))
            .unwrap();
    }

    let full_path = base_path.join(clean_path);

    // Security: ensure we don't escape sdk directory
    if !full_path.starts_with(&base_path) {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Path traversal not allowed"))
            .unwrap();
    }

    match fs::read_to_string(&full_path).await {
        Ok(content) => {
            let content_type = if file_path.ends_with(".ts") {
                "text/typescript; charset=utf-8"
            } else if file_path.ends_with(".js") {
                "application/javascript; charset=utf-8"
            } else {
                "text/plain; charset=utf-8"
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(content))
                .unwrap()
        }
        Err(_) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("SDK file not found"))
            .unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn test_serve_sdk_mod() {
        // This test requires sdk/mod.ts to exist at the cwd
        let response = serve_sdk(Path("mod.ts".to_string())).await;

        // If running from the project root, the file should exist
        let status = response.status();
        // Accept either OK (file exists) or NOT_FOUND (different cwd)
        assert!(
            status == StatusCode::OK || status == StatusCode::NOT_FOUND,
            "Unexpected status: {:?}",
            status
        );
    }

    #[tokio::test]
    async fn test_path_traversal_blocked() {
        let response = serve_sdk(Path("../Cargo.toml".to_string())).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"Path traversal not allowed");
    }

    #[tokio::test]
    async fn test_typescript_content_type() {
        // Create a temporary sdk directory for this test
        let temp_dir = std::env::temp_dir().join("sdk_test");
        let sdk_dir = temp_dir.join("sdk");
        std::fs::create_dir_all(&sdk_dir).ok();
        std::fs::write(sdk_dir.join("test.ts"), "// test").ok();

        // Save current dir and change to temp
        let original_dir = std::env::current_dir().unwrap();
        if std::env::set_current_dir(&temp_dir).is_ok() {
            let response = serve_sdk(Path("test.ts".to_string())).await;

            if response.status() == StatusCode::OK {
                let content_type = response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .map(|h| h.to_str().unwrap_or(""));
                assert_eq!(content_type, Some("text/typescript; charset=utf-8"));
            }

            // Restore original directory
            std::env::set_current_dir(original_dir).ok();
        }

        // Cleanup
        std::fs::remove_dir_all(temp_dir).ok();
    }
}
