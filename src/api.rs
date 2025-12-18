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
use crate::node::{DocumentNode, Edit, Event, NodeError, NodeId, NodeRegistry};
use crate::store::CommitStore;
use crate::{b64, document::ApplyError};

#[derive(Clone)]
pub struct ApiState {
    pub doc_store: Arc<DocumentStore>,
    pub commit_store: Option<Arc<CommitStore>>,
    pub node_registry: Arc<NodeRegistry>,
}

pub fn router(commit_store: Option<CommitStore>, node_registry: Arc<NodeRegistry>) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let state = ApiState {
        doc_store,
        commit_store: commit_store.map(Arc::new),
        node_registry,
    };

    Router::new()
        // Existing document endpoints (backward compatible)
        .route("/docs", post(create_document))
        .route("/docs/:id", get(get_document))
        .route("/docs/:id", delete(delete_document))
        .route("/docs/:id/commit", post(create_commit))
        // New node endpoints
        .route("/nodes", post(create_node))
        .route("/nodes", get(list_nodes))
        .route("/nodes/:id", get(get_node))
        .route("/nodes/:id", delete(delete_node))
        .route("/nodes/:id/edit", post(send_edit))
        .route("/nodes/:id/event", post(send_event))
        .route("/nodes/:from/wire/:to", post(wire_nodes))
        .route("/nodes/:from/wire/:to", delete(unwire_nodes))
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
    let content_type = ContentType::from_mime(content_type_str)
        .ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?;

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
        [(
            axum::http::header::CONTENT_TYPE,
            doc.content_type.to_mime(),
        )],
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

    let (commit_cid, merge_cid) = if let Some(parent_cid) = req.parent_cid {
        // Case 4b: Create edit commit + merge commit

        // First, create the edit commit as a child of the specified parent
        let edit_commit = Commit::new(
            vec![parent_cid.clone()],
            req.value.clone(),
            author.clone(),
            req.message.clone(),
        );

        let edit_cid = commit_store
            .store_commit(&edit_commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

        let merge_cid = commit_store
            .store_commit(&merge_commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

        (cid, None)
    };

    Ok(Json(CreateCommitResponse {
        cid: commit_cid,
        merge_cid,
    }))
}

// ============================================================================
// Node API endpoints
// ============================================================================

#[derive(Deserialize)]
struct CreateNodeRequest {
    /// Node type: "document" (future: "computed", "aggregator", "gateway")
    node_type: String,
    /// For document nodes: content type (application/json, application/xml, text/plain)
    #[serde(default)]
    content_type: Option<String>,
    /// Optional custom ID (UUID generated if not provided)
    #[serde(default)]
    id: Option<String>,
}

#[derive(Serialize)]
struct CreateNodeResponse {
    id: String,
    node_type: String,
}

async fn create_node(
    State(state): State<ApiState>,
    Json(req): Json<CreateNodeRequest>,
) -> Result<Json<CreateNodeResponse>, StatusCode> {
    match req.node_type.as_str() {
        "document" => {
            let content_type_str = req.content_type.as_deref().unwrap_or("application/json");
            let content_type = ContentType::from_mime(content_type_str)
                .ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?;

            let id = req.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let node = Arc::new(DocumentNode::new(id.clone(), content_type));

            state
                .node_registry
                .register(node)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(Json(CreateNodeResponse {
                id,
                node_type: "document".to_string(),
            }))
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Serialize)]
struct NodeInfo {
    id: String,
    node_type: String,
    subscriber_count: usize,
    is_healthy: bool,
}

async fn list_nodes(State(state): State<ApiState>) -> Json<Vec<NodeInfo>> {
    let node_ids = state.node_registry.list_nodes().await;
    let mut infos = Vec::new();

    for node_id in node_ids {
        if let Some(node) = state.node_registry.get(&node_id).await {
            infos.push(NodeInfo {
                id: node_id.0,
                node_type: node.node_type().to_string(),
                subscriber_count: node.subscriber_count(),
                is_healthy: node.is_healthy(),
            });
        }
    }

    Json(infos)
}

async fn get_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let node_id = NodeId::new(id);
    let node = state
        .node_registry
        .get(&node_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(NodeInfo {
        id: node_id.0,
        node_type: node.node_type().to_string(),
        subscriber_count: node.subscriber_count(),
        is_healthy: node.is_healthy(),
    }))
}

async fn delete_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let node_id = NodeId::new(id);

    // Shutdown the node first
    if let Some(node) = state.node_registry.get(&node_id).await {
        let _ = node.shutdown().await;
    }

    state
        .node_registry
        .unregister(&node_id)
        .await
        .map_err(|e| match e {
            NodeError::NotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct SendEditRequest {
    /// Yjs update as base64
    update: String,
    /// Optional parent commit IDs
    #[serde(default)]
    parents: Vec<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Serialize)]
struct SendEditResponse {
    cid: String,
}

async fn send_edit(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<SendEditRequest>,
) -> Result<Json<SendEditResponse>, StatusCode> {
    let node_id = NodeId::new(id);
    let node = state
        .node_registry
        .get(&node_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    let author = req.author.unwrap_or_else(|| "anonymous".to_string());
    let commit = Commit::new(req.parents, req.update, author, req.message);
    let cid = commit.calculate_cid();

    let edit = Edit {
        commit: Arc::new(commit),
        source: NodeId::new("api"),
    };

    node.receive_edit(edit).await.map_err(|e| match e {
        NodeError::InvalidEdit(_) => StatusCode::BAD_REQUEST,
        NodeError::Shutdown => StatusCode::GONE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    Ok(Json(SendEditResponse { cid }))
}

#[derive(Deserialize)]
struct SendEventRequest {
    event_type: String,
    payload: serde_json::Value,
}

async fn send_event(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<SendEventRequest>,
) -> Result<StatusCode, StatusCode> {
    let node_id = NodeId::new(id);
    let node = state
        .node_registry
        .get(&node_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    let event = Event {
        event_type: req.event_type,
        payload: req.payload,
        source: NodeId::new("api"),
    };

    node.receive_event(event).await.map_err(|e| match e {
        NodeError::Shutdown => StatusCode::GONE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct WireResponse {
    subscription_id: String,
    from: String,
    to: String,
}

async fn wire_nodes(
    State(state): State<ApiState>,
    Path((from, to)): Path<(String, String)>,
) -> Result<Json<WireResponse>, StatusCode> {
    let from_id = NodeId::new(from.clone());
    let to_id = NodeId::new(to.clone());

    let subscription_id = state
        .node_registry
        .wire(&from_id, &to_id)
        .await
        .map_err(|e| match e {
            NodeError::NotFound(_) => StatusCode::NOT_FOUND,
            NodeError::CycleDetected(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    Ok(Json(WireResponse {
        subscription_id: subscription_id.to_string(),
        from,
        to,
    }))
}

async fn unwire_nodes(
    State(state): State<ApiState>,
    Path((from, to)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    let from_id = NodeId::new(from);
    let to_id = NodeId::new(to);

    // Find the wiring between these nodes
    let wirings = state.node_registry.get_outgoing_wirings(&from_id).await;
    let wiring = wirings
        .into_iter()
        .find(|(_, target)| target == &to_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    state
        .node_registry
        .unwire(&wiring.0)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}
