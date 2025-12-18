use axum::{
    extract::{Path, State},
    response::sse::{Event as SseEvent, Sse},
    routing::get,
    Router,
};
use futures::stream::Stream;
use std::convert::Infallible;
use std::sync::Arc;

use crate::node::{NodeId, NodeMessage, NodeRegistry};

#[derive(Clone)]
pub struct SseState {
    pub registry: Arc<NodeRegistry>,
}

pub fn router(registry: Arc<NodeRegistry>) -> Router {
    let state = SseState { registry };

    Router::new()
        .route("/nodes/:id", get(subscribe_to_node))
        .with_state(state)
}

async fn subscribe_to_node(
    State(state): State<SseState>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, axum::http::StatusCode> {
    let node_id = NodeId::new(id);
    let node = state
        .registry
        .get(&node_id)
        .await
        .ok_or(axum::http::StatusCode::NOT_FOUND)?;

    let mut subscription = node.subscribe();

    let stream = async_stream::stream! {
        loop {
            match subscription.recv().await {
                Ok(msg) => {
                    let sse_event = match msg {
                        NodeMessage::Edit(edit) => {
                            let data = serde_json::json!({
                                "source": edit.source.0,
                                "commit": {
                                    "update": edit.commit.update,
                                    "parents": edit.commit.parents,
                                    "timestamp": edit.commit.timestamp,
                                    "author": edit.commit.author,
                                    "message": edit.commit.message,
                                }
                            });
                            SseEvent::default()
                                .event("edit")
                                .json_data(data)
                                .unwrap_or_else(|_| SseEvent::default().event("error").data("serialization failed"))
                        }
                        NodeMessage::Event(event) => {
                            let data = serde_json::json!({
                                "source": event.source.0,
                                "payload": event.payload,
                            });
                            SseEvent::default()
                                .event(&event.event_type)
                                .json_data(data)
                                .unwrap_or_else(|_| SseEvent::default().event("error").data("serialization failed"))
                        }
                    };
                    yield Ok(sse_event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(SseEvent::default()
                        .event("warning")
                        .data(format!("Lagged by {} messages", n)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    yield Ok(SseEvent::default().event("closed").data("Node shut down"));
                    break;
                }
            }
        }
    };

    Ok(Sse::new(stream))
}
