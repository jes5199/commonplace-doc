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
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::document::DocumentStore;
use crate::events::CommitBroadcaster;
use crate::services::{DocumentService, ServiceError};
use crate::store::CommitStore;

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
async fn resolve_path(state: &FileApiState, path: &str) -> Result<String, PathResolveError> {
    let fs_root_id = state.fs_root.as_ref().ok_or(PathResolveError::NoFsRoot)?;

    let fs_root_doc = state
        .doc_store
        .get_document(fs_root_id)
        .await
        .ok_or(PathResolveError::FsRootNotFound)?;

    crate::document::resolve_path_to_uuid(&fs_root_doc.content, path, fs_root_id)
        .ok_or(PathResolveError::PathNotFound)
}

// ============================================================================
// Response types (shared with api.rs)
// ============================================================================

#[derive(Serialize)]
struct DocHeadResponse {
    cid: Option<String>,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
}

#[derive(Deserialize)]
struct DocEditRequest {
    update: String,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Serialize)]
struct DocEditResponse {
    cid: String,
}

#[derive(Deserialize)]
struct ReplaceParams {
    parent_cid: Option<String>,
    #[serde(default)]
    author: Option<String>,
}

#[derive(Serialize)]
struct ReplaceResponse {
    cid: String,
    edit_cid: String,
    summary: ReplaceSummary,
}

#[derive(Serialize)]
struct ReplaceSummary {
    chars_inserted: usize,
    chars_deleted: usize,
    operations: usize,
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

        Ok((
            [(axum::http::header::CONTENT_TYPE, doc.content_type.to_mime())],
            doc.content,
        )
            .into_response())
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
        // Handle replace
        let doc_id = resolve_path(&state, clean_path)
            .await
            .map_err(|e| e.into_response())?;

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
