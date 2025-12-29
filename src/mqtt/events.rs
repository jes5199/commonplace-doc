//! Events port handler.
//!
//! Publishes events from nodes to `{path}/events/{event-name}` topics.

use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::EventMessage;
use crate::mqtt::topics::Topic;
use crate::mqtt::MqttError;
use rumqttc::QoS;
use std::sync::Arc;
use tracing::debug;

/// Handler for the events port.
pub struct EventsHandler {
    client: Arc<MqttClient>,
}

impl EventsHandler {
    /// Create a new events handler.
    pub fn new(client: Arc<MqttClient>) -> Self {
        Self { client }
    }

    /// Publish an event from a node.
    ///
    /// This is called when a node emits an event on its red port.
    /// The event is published to `{path}/events/{event_name}`.
    pub async fn publish_event(
        &self,
        path: &str,
        event_name: &str,
        payload: &serde_json::Value,
        source: &str,
    ) -> Result<(), MqttError> {
        let topic = Topic::events(path, event_name);
        let topic_str = topic.to_topic_string();

        let message = EventMessage {
            payload: payload.clone(),
            source: source.to_string(),
        };

        let payload_bytes = serde_json::to_vec(&message)?;

        // Use QoS 0 for events - ephemeral broadcasts
        self.client
            .publish(&topic_str, &payload_bytes, QoS::AtMostOnce)
            .await?;

        debug!(
            "Published event '{}' for path: {} from source: {}",
            event_name, path, source
        );

        Ok(())
    }

    /// Get a reference to the MQTT client.
    pub fn client(&self) -> &Arc<MqttClient> {
        &self.client
    }
}
