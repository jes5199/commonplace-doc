//! HTTP Gateway - translates HTTP requests to MQTT messages
//!
//! This module provides a stateless HTTP interface that communicates
//! with the document store via MQTT.

mod api;
mod sse;

pub use api::router;

use crate::mqtt::{client::MqttClient, MqttConfig, MqttError};
use std::sync::Arc;

/// HTTP Gateway that translates HTTP to MQTT
pub struct HttpGateway {
    /// MQTT client for publishing and subscribing
    pub(crate) client: Arc<MqttClient>,
}

impl HttpGateway {
    /// Create a new HTTP gateway connected to MQTT
    pub async fn new(config: MqttConfig) -> Result<Self, MqttError> {
        let client = Arc::new(MqttClient::connect(config).await?);
        Ok(Self { client })
    }

    /// Get a reference to the MQTT client
    pub fn client(&self) -> &Arc<MqttClient> {
        &self.client
    }
}
