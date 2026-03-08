//! Sync agent lifecycle event emission via MQTT.
//!
//! Publishes red events on sync lifecycle moments:
//! started, initial-sync-complete, file-created, file-deleted,
//! file-updated, stopped.

use crate::mqtt::{MqttClient, Topic};
use rumqttc::QoS;
use serde::Serialize;
use std::sync::Arc;
use tracing::warn;

/// Payload wrapper matching EventMessage format expected by the server.
#[derive(Debug, Serialize)]
struct EventMessage {
    payload: serde_json::Value,
    source: String,
}

/// Emits sync lifecycle events via MQTT.
#[derive(Clone)]
pub struct SyncEventEmitter {
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    source: String,
}

impl SyncEventEmitter {
    /// Create a new emitter for sync lifecycle events.
    pub fn new(mqtt_client: Arc<MqttClient>, workspace: &str, source: &str) -> Self {
        Self {
            mqtt_client,
            workspace: workspace.to_string(),
            source: source.to_string(),
        }
    }

    /// Emit a lifecycle event with the given type and payload.
    async fn emit(&self, event_type: &str, payload: serde_json::Value) {
        let topic = Topic::events(&self.workspace, "sync", event_type);
        let message = EventMessage {
            payload,
            source: self.source.clone(),
        };

        match serde_json::to_vec(&message) {
            Ok(bytes) => {
                if let Err(e) = self
                    .mqtt_client
                    .publish(&topic.to_topic_string(), &bytes, QoS::AtMostOnce)
                    .await
                {
                    warn!("Failed to publish sync lifecycle event '{}': {}", event_type, e);
                }
            }
            Err(e) => {
                warn!("Failed to serialize sync lifecycle event '{}': {}", event_type, e);
            }
        }
    }

    /// Emit a "started" event.
    pub async fn emit_started(&self) {
        self.emit("started", serde_json::json!({
            "pid": std::process::id(),
        }))
        .await;
    }

    /// Emit a "stopped" event.
    pub async fn emit_stopped(&self) {
        self.emit("stopped", serde_json::json!({
            "pid": std::process::id(),
        }))
        .await;
    }

    /// Emit a file lifecycle event (file-created, file-deleted, file-updated).
    pub async fn emit_file_event(&self, event_type: &str, path: &str, uuid: &str) {
        self.emit(event_type, serde_json::json!({
            "path": path,
            "uuid": uuid,
        }))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_message_serializes_with_payload_and_source() {
        let message = EventMessage {
            payload: serde_json::json!({"pid": 12345}),
            source: "sync-client".to_string(),
        };

        let json: serde_json::Value = serde_json::to_value(&message).unwrap();
        assert_eq!(json["source"], "sync-client");
        assert_eq!(json["payload"]["pid"], 12345);
    }

    #[test]
    fn test_event_topic_format() {
        let topic = Topic::events("my-workspace", "sync", "started");
        assert_eq!(topic.to_topic_string(), "my-workspace/events/sync/started");
    }

    #[test]
    fn test_file_event_topic_format() {
        let topic = Topic::events("my-workspace", "sync", "file-created");
        assert_eq!(
            topic.to_topic_string(),
            "my-workspace/events/sync/file-created"
        );
    }
}
