//! HTTP Gateway SSE routes - bridges MQTT to Server-Sent Events

use axum::{
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Router,
};
use futures::stream::Stream;
use rumqttc::QoS;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::HttpGateway;

/// Create the SSE router for the HTTP gateway
pub fn router(_gateway: Arc<HttpGateway>) -> Router<Arc<HttpGateway>> {
    Router::new().route("/nodes/:id", get(subscribe_to_node))
}

/// GET /sse/nodes/{id} - Subscribe to node updates via SSE
///
/// This subscribes to the MQTT edits topic for the node and streams
/// updates to the client as SSE events.
async fn subscribe_to_node(
    State(gateway): State<Arc<HttpGateway>>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Create a channel for SSE events
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(100);

    // Subscribe to the MQTT edits topic
    let edits_topic = format!("{}/edits", id);

    let gateway_clone = gateway.clone();
    let edits_topic_clone = edits_topic.clone();
    let tx_clone = tx.clone();
    let id_clone = id.clone();

    // Spawn a task to handle the MQTT subscription
    tokio::spawn(async move {
        // Subscribe to edits
        if let Err(e) = gateway_clone
            .client
            .subscribe(&edits_topic_clone, QoS::AtLeastOnce)
            .await
        {
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

        // Note: In a full implementation, we would need to:
        // 1. Set up a message handler on the MQTT client
        // 2. Forward matching messages to this channel
        // 3. Handle client disconnect to unsubscribe
        //
        // For now, this is a placeholder that sends keepalive pings.
        // The actual message bridging would require changes to MqttClient
        // to support per-topic callbacks or a shared event bus.

        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;

            // Check if client disconnected (channel closed)
            if tx_clone.is_closed() {
                // Unsubscribe from MQTT
                let _ = gateway_clone.client.unsubscribe(&edits_topic_clone).await;
                break;
            }
        }
    });

    Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::default())
}
