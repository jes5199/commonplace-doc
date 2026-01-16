//! Path-based file API endpoints.
//!
//! This module handles `/files/*path` endpoints that resolve filesystem paths
//! to document IDs using the fs-root schema. It provides the same operations
//! as the ID-based `/docs/{id}` endpoints but with path-based addressing.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use std::sync::Arc;

use crate::api::{
    DocEditRequest, DocEditResponse, DocHeadResponse, ReplaceParams, ReplaceResponse,
    ReplaceSummary,
};
use crate::content_type::ContentType;
use crate::document::DocumentStore;
use crate::events::CommitBroadcaster;
use crate::services::{DocumentService, ServiceError};
use crate::store::CommitStore;

/// Get the MIME type to serve for a file based on its extension.
/// For text files, returns the appropriate specific MIME type (text/typescript, etc.)
/// For other types, delegates to ContentType::to_mime().
fn get_serving_mime_type(path: &str, content_type: &ContentType) -> &'static str {
    // Only override for Text content type - other types have specific MIME types
    if *content_type != ContentType::Text {
        return content_type.to_mime();
    }

    // Get extension from path
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext.to_lowercase().as_str() {
        "ts" => "text/typescript",
        "tsx" => "text/typescript",
        "js" => "text/javascript",
        "jsx" => "text/javascript",
        "mjs" => "text/javascript",
        "rs" => "text/x-rust",
        "py" => "text/x-python",
        "rb" => "text/x-ruby",
        "sh" => "text/x-shellscript",
        "css" => "text/css",
        "html" | "htm" => "text/html",
        "md" => "text/markdown",
        "yaml" | "yml" => "text/yaml",
        "toml" => "text/x-toml",
        _ => "text/plain",
    }
}

/// Shared state for file handlers.
#[derive(Clone)]
pub struct FileApiState {
    pub doc_store: Arc<DocumentStore>,
    pub commit_store: Option<Arc<CommitStore>>,
    pub commit_broadcaster: Option<CommitBroadcaster>,
    pub fs_root: Option<String>,
    pub service: Arc<DocumentService>,
}

/// Error type for path resolution failures.
pub enum PathResolveError {
    /// No fs-root configured on server
    NoFsRoot,
    /// fs-root document not found
    FsRootNotFound,
    /// Path not found in filesystem schema
    PathNotFound,
}

impl IntoResponse for PathResolveError {
    fn into_response(self) -> Response {
        match self {
            PathResolveError::NoFsRoot => {
                (StatusCode::SERVICE_UNAVAILABLE, "No fs-root configured").into_response()
            }
            PathResolveError::FsRootNotFound => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "fs-root document not found",
            )
                .into_response(),
            PathResolveError::PathNotFound => {
                (StatusCode::NOT_FOUND, "Path not found in filesystem").into_response()
            }
        }
    }
}

/// Combined error type for file handlers.
pub enum FileError {
    PathResolve(PathResolveError),
    Service(ServiceError),
}

impl From<PathResolveError> for FileError {
    fn from(e: PathResolveError) -> Self {
        FileError::PathResolve(e)
    }
}

impl From<ServiceError> for FileError {
    fn from(e: ServiceError) -> Self {
        FileError::Service(e)
    }
}

impl IntoResponse for FileError {
    fn into_response(self) -> Response {
        match self {
            FileError::PathResolve(e) => e.into_response(),
            FileError::Service(e) => e.into_response(),
        }
    }
}

/// Create a router for path-based file endpoints.
pub fn router(
    doc_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    commit_broadcaster: Option<CommitBroadcaster>,
    fs_root: Option<String>,
    service: Arc<DocumentService>,
) -> Router {
    let state = FileApiState {
        doc_store,
        commit_store,
        commit_broadcaster,
        fs_root,
        service,
    };

    Router::new()
        .route(
            "/files/*path",
            get(handle_file_request)
                .delete(handle_file_delete)
                .post(handle_file_post),
        )
        .with_state(state)
}

/// Resolve a filesystem path to a document ID using the fs-root schema.
///
/// This function handles subdirectory documents - when a directory has `entries: null`
/// and a `node_id`, it fetches that document to continue the path resolution.
async fn resolve_path(state: &FileApiState, path: &str) -> Result<String, PathResolveError> {
    let fs_root_id = state.fs_root.as_ref().ok_or(PathResolveError::NoFsRoot)?;

    // Split path into segments
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        // Empty path means fs-root itself
        return Ok(fs_root_id.clone());
    }

    // Start at fs-root
    let mut current_doc_id = fs_root_id.clone();

    for (i, segment) in segments.iter().enumerate() {
        // Fetch the current document
        let doc = state
            .doc_store
            .get_document(&current_doc_id)
            .await
            .ok_or(PathResolveError::FsRootNotFound)?;

        // Parse the schema
        let schema: serde_json::Value =
            serde_json::from_str(&doc.content).map_err(|_| PathResolveError::PathNotFound)?;

        let root = schema.get("root").ok_or(PathResolveError::PathNotFound)?;
        let entries = root.get("entries").ok_or(PathResolveError::PathNotFound)?;

        // entries could be null if this is a reference to another document
        if entries.is_null() {
            return Err(PathResolveError::PathNotFound);
        }

        let entry = entries
            .get(*segment)
            .ok_or(PathResolveError::PathNotFound)?;
        let entry_type = entry
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or(PathResolveError::PathNotFound)?;

        let is_last = i == segments.len() - 1;

        if is_last {
            // Last segment - must be a doc
            if entry_type != "doc" {
                return Err(PathResolveError::PathNotFound);
            }
            // Return node_id if set, otherwise derive from path
            return entry
                .get("node_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| Some(format!("{}:{}", fs_root_id, path)))
                .ok_or(PathResolveError::PathNotFound);
        } else {
            // Not last segment - must be a dir
            if entry_type != "dir" {
                return Err(PathResolveError::PathNotFound);
            }

            // All directories are node-backed (inline subdirectories were deprecated)
            let node_id = entry
                .get("node_id")
                .and_then(|v| v.as_str())
                .ok_or(PathResolveError::PathNotFound)?;
            current_doc_id = node_id.to_string();
        }
    }

    Err(PathResolveError::PathNotFound)
}

/// Create a new file entry in the parent directory's schema.
///
/// This adds an entry with `node_id: null` to the parent schema, which triggers
/// the filesystem reconciler to create a document and assign a UUID.
async fn create_file_in_schema(state: &FileApiState, path: &str) -> Result<(), PathResolveError> {
    let fs_root_id = state.fs_root.as_ref().ok_or(PathResolveError::NoFsRoot)?;

    // Split path into parent and filename
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Err(PathResolveError::PathNotFound);
    }

    let filename = segments.last().unwrap();
    let parent_segments = &segments[..segments.len() - 1];

    // Get parent document ID (fs-root if no parent segments)
    let parent_id = if parent_segments.is_empty() {
        fs_root_id.clone()
    } else {
        let parent_path = parent_segments.join("/");
        resolve_path(state, &parent_path).await?
    };

    // Fetch parent schema via service (for proper HEAD/CID handling)
    let head = state
        .service
        .get_head(&parent_id, None)
        .await
        .map_err(|_| PathResolveError::FsRootNotFound)?;

    // Parse and update schema
    let mut schema: serde_json::Value =
        serde_json::from_str(&head.content).map_err(|_| PathResolveError::PathNotFound)?;

    // Determine content type from file extension
    let content_type = match path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_lowercase()
        .as_str()
    {
        "json" => "application/json",
        "jsonl" => "application/x-ndjson",
        "xml" => "application/xml",
        "html" | "htm" => "text/html",
        "ts" | "tsx" => "text/typescript",
        "js" | "jsx" | "mjs" => "text/javascript",
        _ => "text/plain",
    };

    // Add entry with no node_id (reconciler will assign one)
    let entries = schema
        .get_mut("root")
        .and_then(|r| r.get_mut("entries"))
        .and_then(|e| e.as_object_mut())
        .ok_or(PathResolveError::PathNotFound)?;

    entries.insert(
        filename.to_string(),
        serde_json::json!({
            "type": "doc",
            "node_id": null,
            "content_type": content_type
        }),
    );

    // Save updated schema via service (triggers reconciliation)
    let updated_schema =
        serde_json::to_string(&schema).map_err(|_| PathResolveError::PathNotFound)?;
    state
        .service
        .replace_content(
            &parent_id,
            &updated_schema,
            head.cid,
            Some("create-file".to_string()),
        )
        .await
        .map_err(|_| PathResolveError::PathNotFound)?;

    tracing::info!("Created file entry '{}' in schema {}", filename, parent_id);
    Ok(())
}

// ============================================================================
// Handler implementations
// ============================================================================

/// GET /files/*path - Handle GET requests (content or /head)
async fn handle_file_request(
    State(state): State<FileApiState>,
    Path(path): Path<String>,
) -> Result<Response, FileError> {
    // Check if path ends with /head
    if let Some(clean_path) = path.strip_suffix("/head") {
        // Use service to get HEAD
        let doc_id = resolve_path(&state, clean_path).await?;
        let head = state.service.get_head(&doc_id, None).await?;

        let response = DocHeadResponse {
            cid: head.cid,
            content: head.content,
            state: head.state,
        };

        Ok(Json(response).into_response())
    } else {
        // Return content
        let doc_id = resolve_path(&state, &path).await?;

        let doc = state
            .doc_store
            .get_document(&doc_id)
            .await
            .ok_or(PathResolveError::PathNotFound)?;

        let mime_type = get_serving_mime_type(&path, &doc.content_type);
        Ok(([(axum::http::header::CONTENT_TYPE, mime_type)], doc.content).into_response())
    }
}

/// DELETE /files/*path - Delete document by path
async fn handle_file_delete(
    State(state): State<FileApiState>,
    Path(path): Path<String>,
) -> Result<StatusCode, PathResolveError> {
    let doc_id = resolve_path(&state, &path).await?;

    if state.doc_store.delete_document(&doc_id).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(PathResolveError::PathNotFound)
    }
}

/// POST /files/*path - Handle POST requests (/edit or /replace)
async fn handle_file_post(
    State(state): State<FileApiState>,
    Path(path): Path<String>,
    Query(params): Query<ReplaceParams>,
    body: String,
) -> Result<Response, Response> {
    // Check if path ends with /edit
    if let Some(clean_path) = path.strip_suffix("/edit") {
        // Parse body as JSON edit request
        let req: DocEditRequest =
            serde_json::from_str(&body).map_err(|_| StatusCode::BAD_REQUEST.into_response())?;

        let doc_id = resolve_path(&state, clean_path)
            .await
            .map_err(|e| e.into_response())?;

        // Use service for edit
        let result = state
            .service
            .edit_document(&doc_id, &req.update, req.author, req.message)
            .await
            .map_err(|e| e.into_response())?;

        Ok(Json(DocEditResponse { cid: result.cid }).into_response())
    } else if let Some(clean_path) = path.strip_suffix("/replace") {
        // Handle replace - try to resolve, optionally create if missing
        let doc_id = match resolve_path(&state, clean_path).await {
            Ok(id) => id,
            Err(PathResolveError::PathNotFound) if params.create => {
                // Create the file and retry resolution
                create_file_in_schema(&state, clean_path)
                    .await
                    .map_err(|e| e.into_response())?;

                // Retry resolution after creation
                resolve_path(&state, clean_path)
                    .await
                    .map_err(|e| e.into_response())?
            }
            Err(e) => return Err(e.into_response()),
        };

        // Use service for replace
        let result = state
            .service
            .replace_content(&doc_id, &body, params.parent_cid, params.author)
            .await
            .map_err(|e| e.into_response())?;

        Ok(Json(ReplaceResponse {
            cid: result.cid,
            edit_cid: result.edit_cid,
            summary: ReplaceSummary {
                chars_inserted: result.chars_inserted,
                chars_deleted: result.chars_deleted,
                operations: result.operations,
            },
        })
        .into_response())
    } else {
        // Unknown POST endpoint
        Err(StatusCode::NOT_FOUND.into_response())
    }
}
