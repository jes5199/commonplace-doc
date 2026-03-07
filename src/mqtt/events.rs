//! Events port handler.
//!
//! Publishes events from nodes to `{path}/events/{event-name}` topics.
//! Also persists incoming events to JSONL event log documents.

use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::EventMessage;
use crate::mqtt::topics::Topic;
use crate::mqtt::MqttError;
use crate::services::event_log::{EventLogEntry, EventLogService};
use rumqttc::QoS;
use std::sync::Arc;
use tracing::debug;

/// Handler for the events port.
pub struct EventsHandler {
    client: Arc<MqttClient>,
    workspace: String,
    event_log_service: Option<Arc<EventLogService>>,
}

impl EventsHandler {
    /// Create a new events handler.
    pub fn new(client: Arc<MqttClient>, workspace: String) -> Self {
        Self {
            client,
            workspace,
            event_log_service: None,
        }
    }

    /// Set the event log service for persistent event logging.
    pub fn with_event_log_service(mut self, service: Arc<EventLogService>) -> Self {
        self.event_log_service = Some(service);
        self
    }

    /// Publish an event from a node.
    ///
    /// This is called when a node emits an event on its red port.
    /// The event is published to `{workspace}/{path}/events/{event_name}`.
    pub async fn publish_event(
        &self,
        path: &str,
        event_name: &str,
        payload: &serde_json::Value,
        source: &str,
    ) -> Result<(), MqttError> {
        let topic = Topic::events(&self.workspace, path, event_name);
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

    /// Handle an incoming event and persist it to the event log.
    ///
    /// The topic's path is used to identify which document's event log to append to.
    /// The qualifier (event name) becomes the event_type.
    pub async fn handle_event_log_append(
        &self,
        topic: &Topic,
        payload: &[u8],
    ) -> Result<(), MqttError> {
        let Some(ref service) = self.event_log_service else {
            debug!("No event log service configured, skipping event persistence");
            return Ok(());
        };

        let message: EventMessage = crate::mqtt::parse_json(payload)?;
        let event_type = topic
            .qualifier
            .as_deref()
            .unwrap_or("unknown");

        let entry = EventLogEntry::from_event(event_type, &message.source, &message.payload);

        // Use path as event_log UUID if it's a UUID, otherwise use it as-is
        let doc_id = if uuid::Uuid::parse_str(&topic.path).is_ok() {
            Some(topic.path.as_str())
        } else {
            // Path-based: would need schema lookup (handled by caller)
            None
        };

        service
            .append_event(doc_id, &entry)
            .await
            .map_err(|e| MqttError::InvalidMessage(format!("Event log append failed: {}", e)))?;

        debug!(
            "Persisted event '{}' from '{}' to log for path: {}",
            event_type, message.source, topic.path
        );

        Ok(())
    }

    /// Get a reference to the MQTT client.
    pub fn client(&self) -> &Arc<MqttClient> {
        &self.client
    }
}
