//! HTTP Gateway API routes - translates HTTP to MQTT
//!
//! ## Document ID Encoding
//!
//! Document IDs may contain path separators (e.g., `foo/bar/file.txt`). When making
//! HTTP requests, these must be URL-encoded. For example:
//!
//! - Doc ID: `foo/bar/file.txt`
//! - HTTP path: `/docs/foo%2Fbar%2Ffile.txt/edit`
//!
//! Axum automatically URL-decodes the path parameter, so the handler receives
//! the original document ID.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::HttpGateway;
use crate::mqtt::{messages::EditMessage, topics::Topic};

/// Create the HTTP gateway router
pub fn router(gateway: Arc<HttpGateway>) -> Router {
    Router::new()
        .route("/docs/:id", get(get_doc_content))
        .route("/docs/:id/edit", post(send_edit))
        .route("/docs/:id/event", post(send_event))
        .nest("/sse", super::sse::router(gateway.clone()))
        .with_state(gateway)
}

// ============================================================================
// Request/Response types
// ============================================================================

#[derive(Deserialize)]
struct SendEditRequest {
    /// Yjs update as base64
    update: String,
    #[serde(default)]
    author: Option<String>,
}

#[derive(Serialize)]
struct SendEditResponse {
    status: String,
}

#[derive(Deserialize)]
struct SendEventRequest {
    event_type: String,
    payload: serde_json::Value,
}

#[derive(Serialize)]
struct DocContentResponse {
    /// Current HEAD commit ID (None if no commits yet)
    cid: Option<String>,
    /// Document content at HEAD
    content: String,
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /docs/{id}/edit - Send a Yjs edit to a document via MQTT
async fn send_edit(
    State(gateway): State<Arc<HttpGateway>>,
    Path(id): Path<String>,
    Json(req): Json<SendEditRequest>,
) -> Result<Json<SendEditResponse>, (StatusCode, String)> {
    // Build the edit message
    let author = req.author.unwrap_or_else(|| "http-gateway".to_string());
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let edit_msg = EditMessage {
        update: req.update,
        author,
        parents: vec![], // Store will determine parents
        message: None,
        timestamp,
    };

    // Build the topic
    let topic = Topic::edits(&id);

    // Serialize and publish
    let payload =
        serde_json::to_vec(&edit_msg).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    gateway
        .client
        .publish(&topic.to_topic_string(), &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SendEditResponse {
        status: "published".to_string(),
    }))
}

/// POST /docs/{id}/event - Send an event to a document via MQTT
async fn send_event(
    State(gateway): State<Arc<HttpGateway>>,
    Path(id): Path<String>,
    Json(req): Json<SendEventRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Build the topic
    let topic = Topic::events(&id, &req.event_type);

    // Serialize and publish
    let payload =
        serde_json::to_vec(&req.payload).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    gateway
        .client
        .publish(&topic.to_topic_string(), &payload, QoS::AtMostOnce)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /docs/{id} - Get current document content via MQTT sync protocol
///
/// Note: This is a simplified implementation. Full content retrieval would
/// require running the MQTT event loop to receive responses. For now, this
/// returns a placeholder indicating the gateway doesn't maintain document state.
async fn get_doc_content(
    State(_gateway): State<Arc<HttpGateway>>,
    Path(id): Path<String>,
) -> Result<Json<DocContentResponse>, (StatusCode, String)> {
    // The HTTP gateway is stateless - it doesn't maintain document state.
    // To get document content, clients should:
    // 1. Subscribe via SSE to receive updates
    // 2. Use the sync protocol directly via MQTT
    //
    // For now, return a response indicating the document ID was received
    // but content is not available through the stateless gateway.
    //
    // TODO: Implement proper sync protocol request/response via MQTT
    // This would require changes to MqttClient to support per-request
    // response correlation.

    tracing::warn!(
        "GET /docs/{} - content retrieval not implemented in stateless gateway",
        id
    );

    Err((
        StatusCode::NOT_IMPLEMENTED,
        format!(
            "Content retrieval not available via stateless HTTP gateway. \
             Subscribe via SSE at /sse/docs/{} to receive updates.",
            id
        ),
    ))
}
