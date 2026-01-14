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
use std::time::Duration;

use super::HttpGateway;
use crate::mqtt::{
    messages::{EditMessage, GetContentRequest, GetContentResponse},
    topics::Topic,
};

// ============================================================================
// Error helpers
// ============================================================================

/// Convert an error to a BAD_REQUEST response tuple.
fn bad_request<E: ToString>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}

/// Convert an error to an INTERNAL_SERVER_ERROR response tuple.
fn internal_error<E: ToString>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

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
    let topic = Topic::edits(&gateway.workspace, &id);

    // Serialize and publish
    let payload = serde_json::to_vec(&edit_msg).map_err(bad_request)?;

    gateway
        .client
        .publish(&topic.to_topic_string(), &payload, QoS::AtLeastOnce)
        .await
        .map_err(internal_error)?;

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
    let topic = Topic::events(&gateway.workspace, &id, &req.event_type);

    // Serialize and publish
    let payload = serde_json::to_vec(&req.payload).map_err(bad_request)?;

    gateway
        .client
        .publish(&topic.to_topic_string(), &payload, QoS::AtMostOnce)
        .await
        .map_err(internal_error)?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /docs/{id} - Get current document content via MQTT command/response
///
/// Sends a get-content command to the server via MQTT and waits for the response.
async fn get_doc_content(
    State(gateway): State<Arc<HttpGateway>>,
    Path(id): Path<String>,
) -> Result<Json<DocContentResponse>, (StatusCode, String)> {
    // Generate a unique request ID for correlation
    let req_id = format!("http-{}", uuid::Uuid::new_v4());

    // Subscribe to responses topic to receive the answer
    // Use reference-counted subscription so concurrent requests share the subscription
    let responses_topic = format!("{}/responses", gateway.workspace);
    gateway
        .add_subscriber(&responses_topic)
        .await
        .map_err(internal_error)?;

    // Get a receiver for incoming messages
    let mut rx = gateway.client.subscribe_messages();

    // Build and send the get-content request
    let request = GetContentRequest {
        req: req_id.clone(),
        id: id.clone(),
    };
    let payload = match serde_json::to_vec(&request) {
        Ok(p) => p,
        Err(e) => {
            gateway.remove_subscriber(&responses_topic).await;
            return Err(bad_request(e));
        }
    };

    let command_topic = format!("{}/commands/__system/get-content", gateway.workspace);
    if let Err(e) = gateway
        .client
        .publish(&command_topic, &payload, QoS::AtLeastOnce)
        .await
    {
        gateway.remove_subscriber(&responses_topic).await;
        return Err(internal_error(e));
    }

    // Wait for response with timeout
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            // Remove our subscription reference before returning
            gateway.remove_subscriber(&responses_topic).await;
            return Err((
                StatusCode::GATEWAY_TIMEOUT,
                "Timeout waiting for response from document store".to_string(),
            ));
        }

        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Ok(msg)) => {
                if msg.topic == responses_topic {
                    // Try to parse as GetContentResponse
                    if let Ok(response) = serde_json::from_slice::<GetContentResponse>(&msg.payload)
                    {
                        if response.req == req_id {
                            // Remove our subscription reference before returning
                            gateway.remove_subscriber(&responses_topic).await;

                            if let Some(error) = response.error {
                                return Err((StatusCode::NOT_FOUND, error));
                            }

                            return Ok(Json(DocContentResponse {
                                cid: None, // get-content doesn't return cid currently
                                content: response.content.unwrap_or_default(),
                            }));
                        }
                    }
                }
            }
            Ok(Err(_)) => {
                // Channel closed, broker disconnected
                gateway.remove_subscriber(&responses_topic).await;
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "MQTT connection lost".to_string(),
                ));
            }
            Err(_) => {
                // Timeout on recv, continue loop
            }
        }
    }
}
