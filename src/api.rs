use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::document::{ContentType, DocumentStore};
use crate::events::CommitBroadcaster;
use crate::services::{DocumentService, ServiceError};
use crate::store::CommitStore;

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
    service: Arc<DocumentService>,
) -> Router {
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
        // fs-root discovery endpoint
        .route("/fs-root", get(get_fs_root))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateDocRequest {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    doc_type: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    /// Optional initial content for the document
    #[serde(default)]
    content: Option<String>,
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

    // Set initial content if provided
    if let Some(Json(ref req)) = body {
        if let Some(ref content) = req.content {
            if let Err(e) = state.doc_store.set_content(&id, content).await {
                tracing::warn!("Failed to set initial content for document {}: {:?}", id, e);
                // Delete the document we just created since we couldn't set the content
                state.doc_store.delete_document(&id).await;
                return Err(StatusCode::BAD_REQUEST);
            }
        }
    }

    Ok(Json(CreateDocResponse { id }))
}

/// Response for GET /fs-root
#[derive(Serialize)]
struct FsRootResponse {
    id: String,
}

/// Get the fs-root document ID.
///
/// Returns the document ID for the fs-root, allowing clients to discover
/// it without needing to specify --node. Only works if server was started
/// with --fs-root.
async fn get_fs_root(State(state): State<ApiState>) -> Result<Json<FsRootResponse>, StatusCode> {
    let id = state
        .fs_root
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    Ok(Json(FsRootResponse { id: id.clone() }))
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
