use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::b64;
use crate::commit::Commit;
use crate::document::{ContentType, DocumentStore};
use crate::events::{CommitBroadcaster, CommitNotification};
use crate::services::{DocumentService, ServiceError};
use crate::store::CommitStore;

/// Broadcast a commit notification to subscribers.
/// Used by path-based handlers that aren't yet migrated to DocumentService.
fn broadcast_commit(state: &ApiState, doc_id: &str, commit_id: &str, timestamp: u64) {
    if let Some(broadcaster) = state.commit_broadcaster.as_ref() {
        broadcaster.notify(CommitNotification {
            doc_id: doc_id.to_string(),
            commit_id: commit_id.to_string(),
            timestamp,
        });
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub doc_store: Arc<DocumentStore>,
    pub commit_store: Option<Arc<CommitStore>>,
    pub commit_broadcaster: Option<CommitBroadcaster>,
    pub fs_root: Option<String>,
    pub service: Arc<DocumentService>,
}

impl ServiceError {
    /// Convert a ServiceError to an HTTP StatusCode.
    fn status_code(&self) -> StatusCode {
        match self {
            ServiceError::NotFound => StatusCode::NOT_FOUND,
            ServiceError::NoPersistence => StatusCode::NOT_IMPLEMENTED,
            ServiceError::InvalidInput(_) => StatusCode::BAD_REQUEST,
            ServiceError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServiceError::Conflict => StatusCode::CONFLICT,
        }
    }
}

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        (self.status_code(), format!("{:?}", self)).into_response()
    }
}

pub fn router(
    doc_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    commit_broadcaster: Option<CommitBroadcaster>,
    fs_root: Option<String>,
) -> Router {
    let service = Arc::new(DocumentService::new(
        doc_store.clone(),
        commit_store.clone(),
        commit_broadcaster.clone(),
    ));

    let state = ApiState {
        doc_store,
        commit_store,
        commit_broadcaster,
        fs_root,
        service,
    };

    Router::new()
        // Document endpoints (ID-based)
        .route("/docs", post(create_doc))
        .route("/docs/:id", get(get_doc_content))
        .route("/docs/:id", delete(delete_doc))
        .route("/docs/:id/commit", post(create_commit))
        .route("/docs/:id/info", get(get_doc_info))
        .route("/docs/:id/head", get(get_doc_head))
        .route("/docs/:id/edit", post(edit_doc))
        .route("/docs/:id/replace", post(replace_doc))
        .route("/docs/:id/fork", post(fork_doc))
        // Path-based endpoints (resolve via fs-root)
        // Use single wildcard route and parse suffix in handlers
        .route(
            "/files/*path",
            get(handle_file_request)
                .delete(handle_file_delete)
                .post(handle_file_post),
        )
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateDocRequest {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    doc_type: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
}

#[derive(Serialize)]
struct CreateDocResponse {
    id: String,
}

async fn create_doc(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Option<Json<CreateDocRequest>>,
) -> Result<Json<CreateDocResponse>, StatusCode> {
    // Support both Content-Type header and JSON body for content_type
    // Priority: JSON body content_type > HTTP Content-Type header
    let content_type = if let Some(Json(ref req)) = body {
        // Check JSON body content_type field first
        if let Some(ct) = req.content_type.as_deref().and_then(ContentType::from_mime) {
            ct
        } else {
            // Fall back to Content-Type header
            let content_type_str = headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("text/plain");

            ContentType::from_mime(content_type_str).ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?
        }
    } else {
        // No body - use Content-Type header
        let content_type_str = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/json");

        ContentType::from_mime(content_type_str).ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?
    };

    // Create document
    let id = state.doc_store.create_document(content_type).await;

    Ok(Json(CreateDocResponse { id }))
}

async fn get_doc_content(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Response, StatusCode> {
    let doc = state
        .doc_store
        .get_document(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Return content with appropriate Content-Type header
    Ok((
        [(axum::http::header::CONTENT_TYPE, doc.content_type.to_mime())],
        doc.content,
    )
        .into_response())
}

async fn delete_doc(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    if state.doc_store.delete_document(&id).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[derive(Deserialize)]
struct CreateCommitRequest {
    verb: String,
    value: String,
    #[serde(default)]
    author: String,
    #[serde(default)]
    message: Option<String>,
    /// Optional parent CID - if specified, creates a merge commit
    #[serde(default)]
    parent_cid: Option<String>,
}

#[derive(Serialize)]
struct CreateCommitResponse {
    cid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    merge_cid: Option<String>,
}

async fn create_commit(
    State(state): State<ApiState>,
    Path(doc_id): Path<String>,
    Json(req): Json<CreateCommitRequest>,
) -> Result<Json<CreateCommitResponse>, ServiceError> {
    // Only support "update" verb for now
    if req.verb != "update" {
        return Err(ServiceError::InvalidInput(
            "Only 'update' verb is supported".to_string(),
        ));
    }

    let result = state
        .service
        .create_commit(&doc_id, &req.value, req.author, req.message, req.parent_cid)
        .await?;

    Ok(Json(CreateCommitResponse {
        cid: result.cid,
        merge_cid: result.merge_cid,
    }))
}

// ============================================================================
// Document info/head/edit/replace/fork endpoints
// ============================================================================

#[derive(Serialize)]
struct DocInfoResponse {
    id: String,
    #[serde(rename = "type")]
    doc_type: String,
}

async fn get_doc_info(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<DocInfoResponse>, StatusCode> {
    let _doc = state
        .doc_store
        .get_document(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(DocInfoResponse {
        id: id.clone(),
        doc_type: "document".to_string(),
    }))
}

#[derive(Deserialize)]
struct DocHeadParams {
    at_commit: Option<String>,
}

#[derive(Serialize)]
struct DocHeadResponse {
    cid: Option<String>,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
}

async fn get_doc_head(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(params): Query<DocHeadParams>,
) -> Result<Json<DocHeadResponse>, ServiceError> {
    let head = state
        .service
        .get_head(&id, params.at_commit.as_deref())
        .await?;

    Ok(Json(DocHeadResponse {
        cid: head.cid,
        content: head.content,
        state: head.state,
    }))
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

async fn edit_doc(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<DocEditRequest>,
) -> Result<Json<DocEditResponse>, ServiceError> {
    let result = state
        .service
        .edit_document(&id, &req.update, req.author, req.message)
        .await?;

    Ok(Json(DocEditResponse { cid: result.cid }))
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

async fn replace_doc(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(params): Query<ReplaceParams>,
    body: String,
) -> Result<Json<ReplaceResponse>, ServiceError> {
    let result = state
        .service
        .replace_content(&id, &body, params.parent_cid, params.author)
        .await?;

    Ok(Json(ReplaceResponse {
        cid: result.cid,
        edit_cid: result.edit_cid,
        summary: ReplaceSummary {
            chars_inserted: result.chars_inserted,
            chars_deleted: result.chars_deleted,
            operations: result.operations,
        },
    }))
}

#[derive(Deserialize)]
struct ForkParams {
    at_commit: Option<String>,
}

#[derive(Serialize)]
struct ForkResponse {
    id: String,
    head: String,
}

async fn fork_doc(
    State(state): State<ApiState>,
    Path(source_id): Path<String>,
    Query(params): Query<ForkParams>,
) -> Result<Json<ForkResponse>, ServiceError> {
    let result = state
        .service
        .fork_document(&source_id, params.at_commit)
        .await?;

    Ok(Json(ForkResponse {
        id: result.id,
        head: result.head,
    }))
}

// ============================================================================
// Path-based endpoints (resolve paths via fs-root)
// ============================================================================

/// Error type for path resolution failures
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

/// Resolve a filesystem path to a document ID using the fs-root schema.
async fn resolve_path(state: &ApiState, path: &str) -> Result<String, PathResolveError> {
    let fs_root_id = state.fs_root.as_ref().ok_or(PathResolveError::NoFsRoot)?;

    let fs_root_doc = state
        .doc_store
        .get_document(fs_root_id)
        .await
        .ok_or(PathResolveError::FsRootNotFound)?;

    crate::document::resolve_path_to_uuid(&fs_root_doc.content, path, fs_root_id)
        .ok_or(PathResolveError::PathNotFound)
}

/// GET /files/*path - Handle GET requests (content or /head)
async fn handle_file_request(
    State(state): State<ApiState>,
    Path(path): Path<String>,
) -> Result<Response, PathResolveError> {
    // Check if path ends with /head
    if let Some(clean_path) = path.strip_suffix("/head") {
        // Return HEAD response
        let doc_id = resolve_path(&state, clean_path).await?;

        let doc = state
            .doc_store
            .get_document(&doc_id)
            .await
            .ok_or(PathResolveError::PathNotFound)?;

        let cid = if let Some(store) = &state.commit_store {
            store.get_document_head(&doc_id).await.ok().flatten()
        } else {
            None
        };

        let state_bytes = state.doc_store.get_yjs_state(&doc_id).await;
        let state_b64 = state_bytes.map(|b| b64::encode(&b));

        let response = DocHeadResponse {
            cid,
            content: doc.content,
            state: state_b64,
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
    State(state): State<ApiState>,
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
    State(state): State<ApiState>,
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

        let commit_store = state
            .commit_store
            .as_ref()
            .ok_or_else(|| StatusCode::NOT_IMPLEMENTED.into_response())?;

        let _doc = state
            .doc_store
            .get_document(&doc_id)
            .await
            .ok_or_else(|| StatusCode::NOT_FOUND.into_response())?;

        let update_bytes =
            b64::decode(&req.update).map_err(|_| StatusCode::BAD_REQUEST.into_response())?;

        let current_head = commit_store
            .get_document_head(&doc_id)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        let parents = current_head.into_iter().collect();
        let author = req.author.unwrap_or_else(|| "anonymous".to_string());

        let commit = Commit::new(parents, req.update, author, req.message);
        let timestamp = commit.timestamp;

        let cid = commit_store
            .store_commit(&commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        state
            .doc_store
            .apply_yjs_update(&doc_id, &update_bytes)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        commit_store
            .set_document_head(&doc_id, &cid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        broadcast_commit(&state, &doc_id, &cid, timestamp);

        Ok(Json(DocEditResponse { cid }).into_response())
    } else if let Some(clean_path) = path.strip_suffix("/replace") {
        // Handle replace
        let doc_id = resolve_path(&state, clean_path)
            .await
            .map_err(|e| e.into_response())?;

        let commit_store = state
            .commit_store
            .as_ref()
            .ok_or_else(|| StatusCode::NOT_IMPLEMENTED.into_response())?;

        let doc = state
            .doc_store
            .get_document(&doc_id)
            .await
            .ok_or_else(|| StatusCode::NOT_FOUND.into_response())?;

        let diff_result = crate::diff::compute_diff_update(&doc.content, &body)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        let parent = if let Some(p) = params.parent_cid.clone() {
            Some(p)
        } else {
            commit_store
                .get_document_head(&doc_id)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?
        };

        let parents: Vec<String> = parent.into_iter().collect();
        let author = params
            .author
            .clone()
            .unwrap_or_else(|| "anonymous".to_string());

        let commit = Commit::new(parents, diff_result.update_b64.clone(), author, None);
        let timestamp = commit.timestamp;

        let cid = commit_store
            .store_commit(&commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        state
            .doc_store
            .apply_yjs_update(&doc_id, &diff_result.update_bytes)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        commit_store
            .set_document_head(&doc_id, &cid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

        broadcast_commit(&state, &doc_id, &cid, timestamp);

        Ok(Json(ReplaceResponse {
            cid: cid.clone(),
            edit_cid: cid,
            summary: ReplaceSummary {
                chars_inserted: diff_result.summary.chars_inserted,
                chars_deleted: diff_result.summary.chars_deleted,
                operations: diff_result.operation_count,
            },
        })
        .into_response())
    } else {
        // Unknown POST endpoint
        Err(StatusCode::NOT_FOUND.into_response())
    }
}
