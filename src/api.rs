use axum::{
    extract::{Path, State},
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
) -> Router {
    let state = ApiState {
        doc_store,
        commit_store,
        commit_broadcaster,
    };

    Router::new()
        // Document endpoints
        .route("/docs", post(create_document))
        .route("/docs/:id", get(get_document))
        .route("/docs/:id", delete(delete_document))
        .route("/docs/:id/commit", post(create_commit))
        .with_state(state)
}

#[derive(Serialize)]
struct CreateDocumentResponse {
    id: String,
}

async fn create_document(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<CreateDocumentResponse>, StatusCode> {
    // Get Content-Type header
    let content_type_str = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    // Parse content type
    let content_type =
        ContentType::from_mime(content_type_str).ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?;

    // Create document
    let id = state.doc_store.create_document(content_type).await;

    Ok(Json(CreateDocumentResponse { id }))
}

async fn get_document(
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

async fn delete_document(
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
