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
use crate::diff::compute_diff_update_with_base;
use crate::document::{ContentType, DocumentStore};
use crate::events::{CommitBroadcaster, CommitNotification};
use crate::node::{DocumentNode, Edit, Event, NodeError, NodeId, NodeRegistry};
use crate::replay::CommitReplayer;
use crate::store::CommitStore;
use crate::{b64, document::ApplyError};

#[derive(Clone)]
pub struct ApiState {
    pub doc_store: Arc<DocumentStore>,
    pub commit_store: Option<Arc<CommitStore>>,
    pub node_registry: Arc<NodeRegistry>,
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
    node_registry: Arc<NodeRegistry>,
) -> Router {
    let state = ApiState {
        doc_store,
        commit_store,
        commit_broadcaster,
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
        .route("/nodes/:id/replace", post(replace_content))
        .route("/nodes/:id/head", get(get_node_head))
        .route("/nodes/:id/event", post(send_event))
        .route("/nodes/:id/fork", post(fork_node))
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

#[derive(Serialize)]
struct NodeHeadResponse {
    /// Current HEAD commit ID (None if no commits yet)
    cid: Option<String>,
    /// Document content at HEAD
    content: String,
}

async fn get_node_head(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<NodeHeadResponse>, (StatusCode, String)> {
    // 1. Check commit store is available
    let commit_store = state.commit_store.as_ref().ok_or((
        StatusCode::NOT_IMPLEMENTED,
        "Commit store not enabled. Start server with --database flag.".to_string(),
    ))?;

    // 2. Verify node exists
    let node_id = NodeId::new(&id);
    let _node = state
        .node_registry
        .get(&node_id)
        .await
        .ok_or((StatusCode::NOT_FOUND, format!("Node {} not found", id)))?;

    // 3. Get current HEAD
    let head_cid = commit_store
        .get_document_head(&id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 4. If no HEAD, return empty content
    let Some(cid) = head_cid else {
        return Ok(Json(NodeHeadResponse {
            cid: None,
            content: String::new(),
        }));
    };

    // 5. Replay content at HEAD
    let replayer = CommitReplayer::new(commit_store.as_ref());
    let content = replayer
        .get_content_at_commit(&id, &cid, &ContentType::Text)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(NodeHeadResponse {
        cid: Some(cid),
        content,
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
    let node_id = NodeId::new(id.clone());
    let node = state
        .node_registry
        .get(&node_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Determine parents - use provided or get current head
    let parents = if req.parents.is_empty() {
        // If no parents specified, use current head (if any)
        if let Some(commit_store) = &state.commit_store {
            commit_store
                .get_document_head(&id)
                .await
                .ok()
                .flatten()
                .map(|h| vec![h])
                .unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        req.parents
    };

    let author = req.author.unwrap_or_else(|| "anonymous".to_string());
    let commit = Commit::new(parents, req.update, author, req.message);
    let cid = commit.calculate_cid();

    // Store commit in commit store if available
    if let Some(commit_store) = &state.commit_store {
        commit_store
            .store_commit(&commit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Set as new document head
        commit_store
            .set_document_head(&id, &cid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

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

    let subscription_id =
        state
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

// ============================================================================
// Replace content endpoint
// ============================================================================

/// Query parameters for replace endpoint
#[derive(Deserialize)]
struct ReplaceQuery {
    /// Commit we're editing from (must exist in history)
    parent_cid: String,
    /// Author identifier (optional, defaults to "anonymous")
    #[serde(default)]
    author: Option<String>,
    /// Optional commit message
    #[serde(default)]
    message: Option<String>,
}

#[derive(Serialize)]
struct ReplaceResponse {
    /// The commit that HEAD now points to (merge_cid if merge occurred, else edit_cid)
    cid: String,
    /// The edit commit CID (may equal cid if no merge was needed)
    edit_cid: String,
    /// Summary of changes applied
    summary: ReplaceSummary,
}

#[derive(Serialize)]
struct ReplaceSummary {
    chars_inserted: usize,
    chars_deleted: usize,
    operations: usize,
}

async fn replace_content(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(query): Query<ReplaceQuery>,
    body: String,
) -> Result<Json<ReplaceResponse>, (StatusCode, String)> {
    // 1. Check commit store is available
    let commit_store = state.commit_store.as_ref().ok_or((
        StatusCode::NOT_IMPLEMENTED,
        "Commit store not enabled. Start server with --database flag.".to_string(),
    ))?;

    // 2. Verify node exists
    let node_id = NodeId::new(&id);
    let node = state
        .node_registry
        .get(&node_id)
        .await
        .ok_or((StatusCode::NOT_FOUND, format!("Node {} not found", id)))?;

    // 3. Only support text content type for now
    let content_type = ContentType::Text;

    // 4. Validate parent_cid exists in document history
    let replayer = CommitReplayer::new(commit_store.as_ref());

    if !replayer
        .verify_commit_in_history(&id, &query.parent_cid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Commit {} not found in document history", query.parent_cid),
        ));
    }

    // 5. Reconstruct content and state at parent_cid
    let (old_content, base_state) = replayer
        .get_content_and_state_at_commit(&id, &query.parent_cid, &content_type)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 6. Compute diff and generate Yjs update using actual base state
    let diff_result = compute_diff_update_with_base(&base_state, &old_content, &body)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 7. Get current HEAD
    let current_head = commit_store
        .get_document_head(&id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let author = query.author.unwrap_or_else(|| "anonymous".to_string());

    // 8. Create edit commit
    let edit_commit = Commit::new(
        vec![query.parent_cid.clone()],
        diff_result.update_b64.clone(),
        author.clone(),
        query.message.clone(),
    );
    let edit_cid = commit_store
        .store_commit(&edit_commit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 9. Determine if merge is needed and apply update
    let merge_cid = if current_head.as_ref() == Some(&query.parent_cid) {
        // Fast-forward: parent_cid == HEAD, no merge needed
        // Apply update to node
        let edit = Edit {
            commit: Arc::new(edit_commit),
            source: NodeId::new("api"),
        };
        node.receive_edit(edit).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to apply edit: {}", e),
            )
        })?;

        // Update HEAD
        commit_store
            .set_document_head(&id, &edit_cid)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        None
    } else if let Some(head_cid) = current_head {
        // Need merge: parent_cid != HEAD
        let merge_commit = Commit::new(
            vec![edit_cid.clone(), head_cid],
            String::new(), // Empty update for merge (CRDT handles convergence)
            author,
            Some("Merge commit".to_string()),
        );
        let merge_cid = commit_store
            .store_commit(&merge_commit)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Apply update to node
        let edit = Edit {
            commit: Arc::new(edit_commit),
            source: NodeId::new("api"),
        };
        node.receive_edit(edit).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to apply edit: {}", e),
            )
        })?;

        // Update HEAD to merge commit
        commit_store
            .set_document_head(&id, &merge_cid)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        Some(merge_cid)
    } else {
        // No HEAD yet - this is the first commit for this document
        let edit = Edit {
            commit: Arc::new(edit_commit),
            source: NodeId::new("api"),
        };
        node.receive_edit(edit).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to apply edit: {}", e),
            )
        })?;

        commit_store
            .set_document_head(&id, &edit_cid)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        None
    };

    // cid is what HEAD points to (merge if exists, else edit)
    let head_cid = merge_cid.unwrap_or_else(|| edit_cid.clone());

    Ok(Json(ReplaceResponse {
        cid: head_cid,
        edit_cid,
        summary: ReplaceSummary {
            chars_inserted: diff_result.summary.chars_inserted,
            chars_deleted: diff_result.summary.chars_deleted,
            operations: diff_result.operation_count,
        },
    }))
}

// ============================================================================
// Fork endpoint
// ============================================================================

/// Query parameters for fork endpoint
#[derive(Deserialize)]
struct ForkQuery {
    /// Optional commit to fork from. If not specified, uses current HEAD.
    #[serde(default)]
    at_commit: Option<String>,
}

#[derive(Serialize)]
struct ForkResponse {
    /// New node ID
    id: String,
    /// HEAD commit of the forked node (same as source at fork point)
    head: String,
}

async fn fork_node(
    State(state): State<ApiState>,
    Path(source_id): Path<String>,
    Query(query): Query<ForkQuery>,
) -> Result<Json<ForkResponse>, (StatusCode, String)> {
    // 1. Check commit store is available
    let commit_store = state.commit_store.as_ref().ok_or((
        StatusCode::NOT_IMPLEMENTED,
        "Commit store not enabled. Start server with --database flag.".to_string(),
    ))?;

    // 2. Verify source node exists and get its content type
    let source_node_id = NodeId::new(&source_id);
    let source_node = state.node_registry.get(&source_node_id).await.ok_or((
        StatusCode::NOT_FOUND,
        format!("Node {} not found", source_id),
    ))?;

    // Get content type from the source node (DocumentNode stores it)
    let content_type = source_node
        .as_any()
        .downcast_ref::<DocumentNode>()
        .map(|dn| dn.content_type())
        .unwrap_or(ContentType::Text);

    // Currently only text content type is supported for forking
    if !matches!(content_type, ContentType::Text) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!(
                "Fork only supports text/plain documents. Node {} has content type {}",
                source_id,
                content_type.to_mime()
            ),
        ));
    }

    // 3. Determine which commit to fork from
    let replayer = CommitReplayer::new(commit_store.as_ref());
    let fork_cid = match query.at_commit {
        Some(cid) => {
            // Validate the specified commit exists AND is in source's history
            if !replayer
                .verify_commit_in_history(&source_id, &cid)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("Commit {} is not in the history of node {}", cid, source_id),
                ));
            }
            cid
        }
        None => {
            // Use current HEAD
            commit_store
                .get_document_head(&source_id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                .ok_or((
                    StatusCode::BAD_REQUEST,
                    "Source node has no commits to fork from".to_string(),
                ))?
        }
    };

    // 4. Create new node with new UUID
    let new_id = uuid::Uuid::new_v4().to_string();
    let new_node = Arc::new(DocumentNode::new(new_id.clone(), content_type.clone()));

    // 5. Register the new node
    state
        .node_registry
        .register(new_node.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 6. Replay content at fork point and apply to new node
    let (content, _state) = replayer
        .get_content_and_state_at_commit(&source_id, &fork_cid, &content_type)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Apply content to the new node's Yjs doc
    new_node.set_content(&content);

    // 7. Set new node's HEAD to the same commit (shares the commit DAG)
    commit_store
        .set_document_head(&new_id, &fork_cid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ForkResponse {
        id: new_id,
        head: fork_cid,
    }))
}
