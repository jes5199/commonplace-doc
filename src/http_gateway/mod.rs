//! HTTP Gateway - translates HTTP requests to MQTT messages
//!
//! This module provides a stateless HTTP interface that communicates
//! with the document store via MQTT.

mod api;
mod sse;

pub use api::router;

use crate::mqtt::{client::MqttClient, MqttConfig, MqttError};
use rumqttc::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// HTTP Gateway that translates HTTP to MQTT
pub struct HttpGateway {
    /// MQTT client for publishing and subscribing
    pub(crate) client: Arc<MqttClient>,
    /// Workspace name for topic namespacing
    pub(crate) workspace: String,
    /// Reference counts for MQTT topic subscriptions (for SSE)
    pub(crate) subscription_counts: Arc<RwLock<HashMap<String, usize>>>,
}

impl HttpGateway {
    /// Create a new HTTP gateway connected to MQTT
    ///
    /// This spawns the MQTT event loop in the background so that
    /// publish/subscribe operations can actually execute.
    pub async fn new(config: MqttConfig) -> Result<Self, MqttError> {
        let workspace = config.workspace.clone();
        let client = Arc::new(MqttClient::connect(config).await?);

        // Spawn the MQTT event loop so publishes/subscribes actually work
        let client_for_loop = client.clone();
        tokio::spawn(async move {
            if let Err(e) = client_for_loop.run_event_loop().await {
                tracing::error!("HTTP gateway MQTT event loop error: {}", e);
            }
        });

        Ok(Self {
            client,
            workspace,
            subscription_counts: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get a reference to the MQTT client
    pub fn client(&self) -> &Arc<MqttClient> {
        &self.client
    }

    /// Increment the subscription count for a topic.
    /// If this is the first subscriber, actually subscribe to MQTT.
    pub(crate) async fn add_subscriber(&self, topic: &str) -> Result<(), MqttError> {
        let mut counts = self.subscription_counts.write().await;
        let count = counts.entry(topic.to_string()).or_insert(0);

        if *count == 0 {
            // First subscriber - actually subscribe to MQTT
            // Subscribe before incrementing count so failures don't leave stale counts
            self.client.subscribe(topic, QoS::AtLeastOnce).await?;
            tracing::debug!("SSE subscribed to MQTT topic: {}", topic);
        }

        // Only increment after successful subscribe
        *count += 1;
        Ok(())
    }

    /// Decrement the subscription count for a topic.
    /// If this was the last subscriber, actually unsubscribe from MQTT.
    pub(crate) async fn remove_subscriber(&self, topic: &str) {
        let mut counts = self.subscription_counts.write().await;
        if let Some(count) = counts.get_mut(topic) {
            *count = count.saturating_sub(1);

            if *count == 0 {
                counts.remove(topic);
                // Last subscriber - unsubscribe from MQTT
                if let Err(e) = self.client.unsubscribe(topic).await {
                    tracing::warn!("Failed to unsubscribe from {}: {}", topic, e);
                } else {
                    tracing::debug!("SSE unsubscribed from MQTT topic: {}", topic);
                }
            }
        }
    }
}
