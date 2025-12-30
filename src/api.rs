use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::commit::Commit;
use crate::document::{ContentType, DocumentStore};
use crate::events::{CommitBroadcaster, CommitNotification};
use crate::store::CommitStore;
use crate::{b64, document::ApplyError};

#[derive(Clone)]
pub struct ApiState {
    pub doc_store: Arc<DocumentStore>,
    pub commit_store: Option<Arc<CommitStore>>,
    pub commit_broadcaster: Option<CommitBroadcaster>,
    pub fs_root: Option<String>,
}

fn broadcast_commit(state: &ApiState, doc_id: &str, commit_id: &str, timestamp: u64) {
    if let Some(broadcaster) = state.commit_broadcaster.as_ref() {
        broadcaster.notify(CommitNotification {
            doc_id: doc_id.to_string(),
            commit_id: commit_id.to_string(),
            timestamp,
        });
    }
}

fn broadcast_commits(state: &ApiState, doc_id: &str, commits: &[(String, u64)]) {
    for (commit_id, timestamp) in commits {
        broadcast_commit(state, doc_id, commit_id, *timestamp);
    }
}

pub fn router(
    doc_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    commit_broadcaster: Option<CommitBroadcaster>,
    fs_root: Option<String>,
) -> Router {
    let state = ApiState {
        doc_store,
        commit_store,
        commit_broadcaster,
        fs_root,
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
) -> Result<Json<CreateCommitResponse>, StatusCode> {
    // Check if commit store is available
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    // Check if document exists
    let _doc = state
        .doc_store
        .get_document(&doc_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Only support "update" verb for now
    if req.verb != "update" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let author = if req.author.is_empty() {
        "anonymous".to_string()
    } else {
        req.author
    };

    let update_bytes = b64::decode(&req.value).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get current head commit (if any)
    let current_head = commit_store
        .get_document_head(&doc_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut notifications: Vec<(String, u64)> = Vec::new();

    let (commit_cid, merge_cid) = if let Some(parent_cid) = req.parent_cid {
        // Case 4b: Create edit commit + merge commit

        // First, create the edit commit as a child of the specified parent
        let edit_commit = Commit::new(
            vec![parent_cid.clone()],
            req.value.clone(),
            author.clone(),
            req.message.clone(),
        );
        let edit_timestamp = edit_commit.timestamp;

        let edit_cid = commit_store
            .store_commit(&edit_commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        notifications.push((edit_cid.clone(), edit_timestamp));

        // Then create a merge commit with both the edit and current head as parents
        let merge_parents = if let Some(head_cid) = current_head.as_ref() {
            vec![edit_cid.clone(), head_cid.clone()]
        } else {
            // If there's no current head, just make the edit commit the head
            state
                .doc_store
                .apply_yjs_update(&doc_id, &update_bytes)
                .await
                .map_err(|e| match e {
                    ApplyError::NotFound => StatusCode::NOT_FOUND,
                    ApplyError::MissingYDoc => StatusCode::INTERNAL_SERVER_ERROR,
                    ApplyError::InvalidUpdate(_) => StatusCode::BAD_REQUEST,
                    ApplyError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
                })?;

            commit_store
                .set_document_head(&doc_id, &edit_cid)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            broadcast_commits(&state, &doc_id, &notifications);
            return Ok(Json(CreateCommitResponse {
                cid: edit_cid,
                merge_cid: None,
            }));
        };

        let merge_commit = Commit::new(
            merge_parents,
            String::new(), // Empty update for merge
            author,
            Some("Merge commit".to_string()),
        );
        let merge_timestamp = merge_commit.timestamp;

        let merge_cid = commit_store
            .store_commit(&merge_commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        notifications.push((merge_cid.clone(), merge_timestamp));

        // Validate monotonic descent for the merge commit
        commit_store
            .validate_monotonic_descent(&doc_id, &merge_cid)
            .await
            .map_err(|_| StatusCode::CONFLICT)?;

        state
            .doc_store
            .apply_yjs_update(&doc_id, &update_bytes)
            .await
            .map_err(|e| match e {
                ApplyError::NotFound => StatusCode::NOT_FOUND,
                ApplyError::MissingYDoc => StatusCode::INTERNAL_SERVER_ERROR,
                ApplyError::InvalidUpdate(_) => StatusCode::BAD_REQUEST,
                ApplyError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            })?;

        // Update document head to merge commit
        commit_store
            .set_document_head(&doc_id, &merge_cid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        (edit_cid, Some(merge_cid))
    } else {
        // Case 4a: Simple commit

        // Create commit with current head as parent
        let parents = current_head.clone().into_iter().collect();

        let commit = Commit::new(parents, req.value, author, req.message);
        let commit_timestamp = commit.timestamp;

        let cid = commit_store
            .store_commit(&commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Validate monotonic descent
        commit_store
            .validate_monotonic_descent(&doc_id, &cid)
            .await
            .map_err(|_| StatusCode::CONFLICT)?;

        state
            .doc_store
            .apply_yjs_update(&doc_id, &update_bytes)
            .await
            .map_err(|e| match e {
                ApplyError::NotFound => StatusCode::NOT_FOUND,
                ApplyError::MissingYDoc => StatusCode::INTERNAL_SERVER_ERROR,
                ApplyError::InvalidUpdate(_) => StatusCode::BAD_REQUEST,
                ApplyError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            })?;

        // Update document head
        commit_store
            .set_document_head(&doc_id, &cid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        notifications.push((cid.clone(), commit_timestamp));

        (cid, None)
    };

    broadcast_commits(&state, &doc_id, &notifications);

    Ok(Json(CreateCommitResponse {
        cid: commit_cid,
        merge_cid,
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
) -> Result<Json<DocHeadResponse>, StatusCode> {
    let doc = state
        .doc_store
        .get_document(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // If at_commit is specified, replay commits to get historical state
    if let Some(target_cid) = params.at_commit {
        let commit_store = state
            .commit_store
            .as_ref()
            .ok_or(StatusCode::NOT_IMPLEMENTED)?;

        let replayer = crate::replay::CommitReplayer::new(commit_store);
        let (content, state_bytes) = replayer
            .get_content_and_state_at_commit(&id, &target_cid, &doc.content_type)
            .await
            .map_err(|_| StatusCode::NOT_FOUND)?;

        return Ok(Json(DocHeadResponse {
            cid: Some(target_cid),
            content,
            state: Some(b64::encode(&state_bytes)),
        }));
    }

    // Default: return current HEAD
    let cid = if let Some(store) = &state.commit_store {
        store.get_document_head(&id).await.ok().flatten()
    } else {
        None
    };

    // Get Yjs state bytes if document has a ydoc
    let state_bytes = state.doc_store.get_yjs_state(&id).await;
    let state_b64 = state_bytes.map(|b| b64::encode(&b));

    Ok(Json(DocHeadResponse {
        cid,
        content: doc.content,
        state: state_b64,
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
) -> Result<Json<DocEditResponse>, StatusCode> {
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    // Verify document exists
    let _doc = state
        .doc_store
        .get_document(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    let update_bytes = b64::decode(&req.update).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get current head
    let current_head = commit_store
        .get_document_head(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let parents = current_head.into_iter().collect();
    let author = req.author.unwrap_or_else(|| "anonymous".to_string());

    let commit = Commit::new(parents, req.update, author, req.message);
    let timestamp = commit.timestamp;

    let cid = commit_store
        .store_commit(&commit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply update to document
    state
        .doc_store
        .apply_yjs_update(&id, &update_bytes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Update head
    commit_store
        .set_document_head(&id, &cid)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    broadcast_commit(&state, &id, &cid, timestamp);

    Ok(Json(DocEditResponse { cid }))
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
) -> Result<Json<ReplaceResponse>, StatusCode> {
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    let doc = state
        .doc_store
        .get_document(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Get current HEAD to check if parent_cid differs
    let current_head = commit_store
        .get_document_head(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Determine parents and compute diff appropriately
    let (diff_result, parents) = if let Some(ref parent_cid) = params.parent_cid {
        // Check if parent differs from current HEAD
        let parent_differs = current_head.as_ref() != Some(parent_cid);

        if parent_differs {
            // Parent differs from HEAD - use historical state for proper CRDT merge
            // This ensures offline client changes merge correctly with concurrent server changes
            let replayer = crate::replay::CommitReplayer::new(commit_store);
            let (old_content, base_state_bytes) = replayer
                .get_content_and_state_at_commit(&id, parent_cid, &doc.content_type)
                .await
                .map_err(|_| StatusCode::NOT_FOUND)?;

            let diff =
                crate::diff::compute_diff_update_with_base(&base_state_bytes, &old_content, &body)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            // Include BOTH parents for merge commit: client's parent AND current HEAD
            // This preserves concurrent edits in the commit graph for proper history replay
            let mut merge_parents = vec![parent_cid.clone()];
            if let Some(head) = current_head {
                merge_parents.push(head);
            }
            (diff, merge_parents)
        } else {
            // Parent matches HEAD - use current content for diff
            let diff = crate::diff::compute_diff_update(&doc.content, &body)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            (diff, vec![parent_cid.clone()])
        }
    } else {
        // No parent specified - use current content and HEAD as parent
        let diff = crate::diff::compute_diff_update(&doc.content, &body)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        (diff, current_head.into_iter().collect())
    };
    let author = params.author.unwrap_or_else(|| "anonymous".to_string());

    let commit = Commit::new(parents, diff_result.update_b64.clone(), author, None);
    let timestamp = commit.timestamp;

    let cid = commit_store
        .store_commit(&commit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply update
    state
        .doc_store
        .apply_yjs_update(&id, &diff_result.update_bytes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    commit_store
        .set_document_head(&id, &cid)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    broadcast_commit(&state, &id, &cid, timestamp);

    Ok(Json(ReplaceResponse {
        cid: cid.clone(),
        edit_cid: cid,
        summary: ReplaceSummary {
            chars_inserted: diff_result.summary.chars_inserted,
            chars_deleted: diff_result.summary.chars_deleted,
            operations: diff_result.operation_count,
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
) -> Result<Json<ForkResponse>, StatusCode> {
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    // Get source document
    let source_doc = state
        .doc_store
        .get_document(&source_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Create new document with same content type
    let new_id = state
        .doc_store
        .create_document(source_doc.content_type.clone())
        .await;

    // Get target commit (specified or HEAD)
    let target_cid = if let Some(cid) = params.at_commit {
        cid
    } else {
        commit_store
            .get_document_head(&source_id)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?
    };

    // Replay commits to build state
    let replayer = crate::replay::CommitReplayer::new(commit_store);
    let (_content, state_bytes) = replayer
        .get_content_and_state_at_commit(&source_id, &target_cid, &source_doc.content_type)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply the replayed state to the new document
    state
        .doc_store
        .apply_yjs_update(&new_id, &state_bytes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create a root commit for the forked document with the full state
    let update_b64 = b64::encode(&state_bytes);
    let commit = Commit::new(
        vec![],
        update_b64,
        "fork".to_string(),
        Some(format!("Forked from {} at {}", source_id, target_cid)),
    );

    let new_cid = commit_store
        .store_commit(&commit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    commit_store
        .set_document_head(&new_id, &new_cid)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ForkResponse {
        id: new_id,
        head: new_cid,
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
