//! Static file serving and document viewer routes.
//!
//! When a static directory is configured, this module serves:
//! - `/view/docs/:id` - Viewer page for document by ID
//! - `/view/files/*path` - Viewer page for document by path
//! - `/static/*path` - Static assets (JS, CSS, etc.)

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use std::path::PathBuf;
use tokio::fs;

/// Create the viewer router.
///
/// Returns None if static_dir is not configured.
pub fn router(static_dir: Option<String>) -> Option<Router> {
    let static_dir = static_dir?;
    let static_path = PathBuf::from(&static_dir);

    if !static_path.exists() {
        tracing::warn!("Static directory does not exist: {}", static_dir);
        return None;
    }

    tracing::info!("Serving viewer from: {}", static_dir);

    // Clone for each handler
    let static_dir_view = static_dir.clone();
    let static_dir_files = static_dir.clone();
    let static_dir_assets = static_dir.clone();

    Some(
        Router::new()
            // Viewer routes - serve index.html for both
            .route(
                "/view/docs/:id",
                get(move |Path(_id): Path<String>| serve_index(static_dir_view.clone())),
            )
            .route(
                "/view/files/*path",
                get(move |Path(_path): Path<String>| serve_index(static_dir_files.clone())),
            )
            // Static assets
            .route(
                "/static/*path",
                get(move |Path(path): Path<String>| serve_static(static_dir_assets.clone(), path)),
            ),
    )
}

/// Serve the index.html file.
async fn serve_index(static_dir: String) -> Response {
    let path = PathBuf::from(&static_dir).join("index.html");

    match fs::read(&path).await {
        Ok(content) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Body::from(content))
            .unwrap(),
        Err(e) => {
            tracing::error!("Failed to read index.html: {}", e);
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Viewer not found"))
                .unwrap()
        }
    }
}

/// Serve a static file.
async fn serve_static(static_dir: String, path: String) -> Response {
    // Sanitize path to prevent directory traversal
    let clean_path = path.trim_start_matches('/');
    if clean_path.contains("..") {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("Invalid path"))
            .unwrap();
    }

    let file_path = PathBuf::from(&static_dir).join(clean_path);

    match fs::read(&file_path).await {
        Ok(content) => {
            let content_type = guess_content_type(&file_path);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(content))
                .unwrap()
        }
        Err(_) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("File not found"))
            .unwrap(),
    }
}

/// Guess content type from file extension.
fn guess_content_type(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}
