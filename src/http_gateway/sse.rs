//! HTTP Gateway SSE routes - bridges MQTT to Server-Sent Events
//!
//! ## Document ID Encoding
//!
//! Document IDs may contain path separators (e.g., `foo/bar/file.txt`). When making
//! HTTP requests, these must be URL-encoded:
//!
//! - Document ID: `foo/bar/file.txt`
//! - SSE endpoint: `/sse/docs/foo%2Fbar%2Ffile.txt`
//!
//! Axum automatically URL-decodes the path parameter.

use axum::{
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Router,
};
use futures::stream::Stream;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::HttpGateway;

/// Create the SSE router for the HTTP gateway
pub fn router(_gateway: Arc<HttpGateway>) -> Router<Arc<HttpGateway>> {
    Router::new().route("/docs/:id", get(subscribe_to_doc))
}

/// GET /sse/docs/{id} - Subscribe to document updates via SSE
///
/// This subscribes to the MQTT edits topic for the document and streams
/// updates to the client as SSE events.
async fn subscribe_to_doc(
    State(gateway): State<Arc<HttpGateway>>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Create a channel for SSE events
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(100);

    // Subscribe to the MQTT edits topic
    let edits_topic = format!("{}/{}/edits", gateway.workspace, id);

    let gateway_clone = gateway.clone();
    let edits_topic_clone = edits_topic.clone();
    let tx_clone = tx.clone();
    let id_clone = id.clone();

    // Spawn a task to handle the MQTT subscription and forward messages
    tokio::spawn(async move {
        // Subscribe to edits using reference-counted subscription
        if let Err(e) = gateway_clone.add_subscriber(&edits_topic_clone).await {
            tracing::error!("Failed to subscribe to {}: {}", edits_topic_clone, e);
            let _ = tx_clone
                .send(Ok(Event::default()
                    .event("error")
                    .data(format!("Failed to subscribe: {}", e))))
                .await;
            return;
        }

        // Send initial connected event
        let _ = tx_clone
            .send(Ok(Event::default()
                .event("connected")
                .data(format!("{{\"node_id\": \"{}\"}}", id_clone))))
            .await;

        // Subscribe to the MQTT message broadcast and forward matching messages
        let mut mqtt_rx = gateway_clone.client.subscribe_messages();

        loop {
            tokio::select! {
                // Wait for MQTT messages
                result = mqtt_rx.recv() => {
                    match result {
                        Ok(msg) => {
                            // Check if this message is for our topic
                            if msg.topic == edits_topic_clone {
                                // Convert payload to string (it's JSON)
                                let payload = String::from_utf8_lossy(&msg.payload);
                                if tx_clone.send(Ok(Event::default()
                                    .event("edit")
                                    .data(&*payload))).await.is_err() {
                                    // Client disconnected
                                    break;
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("SSE subscriber lagged {} messages", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // MQTT client shut down
                            break;
                        }
                    }
                }
                // Check if SSE client disconnected
                _ = tx_clone.closed() => {
                    break;
                }
            }
        }

        // Cleanup: decrement reference count (only unsubscribes if last subscriber)
        gateway_clone.remove_subscriber(&edits_topic_clone).await;
        tracing::debug!("SSE client disconnected from {}", edits_topic_clone);
    });

    Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::default())
}
