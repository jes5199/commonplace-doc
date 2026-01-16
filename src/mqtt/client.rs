//! MQTT client wrapper using rumqttc.
//!
//! Provides a high-level async interface for MQTT operations.

use crate::mqtt::{MqttConfig, MqttError};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, error, info};

/// Incoming MQTT message.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// Topic the message was published to
    pub topic: String,
    /// Message payload
    pub payload: Vec<u8>,
}

/// MQTT client wrapper.
pub struct MqttClient {
    client: AsyncClient,
    client_id: String,
    event_loop: Arc<Mutex<EventLoop>>,
    message_tx: broadcast::Sender<IncomingMessage>,
}

impl MqttClient {
    /// Connect to an MQTT broker.
    pub async fn connect(config: MqttConfig) -> Result<Self, MqttError> {
        // Parse the broker URL
        let (host, port) = parse_broker_url(&config.broker_url)?;

        let mut options = MqttOptions::new(&config.client_id, host, port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs));
        options.set_clean_session(config.clean_session);

        // Create client and event loop
        let (client, event_loop) = AsyncClient::new(options, 256);

        // Create broadcast channel for incoming messages
        let (message_tx, _) = broadcast::channel(1024);

        info!(
            "MQTT client created for {}:{} as {}",
            config.broker_url, port, config.client_id
        );

        Ok(Self {
            client,
            client_id: config.client_id,
            event_loop: Arc::new(Mutex::new(event_loop)),
            message_tx,
        })
    }

    /// Get the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Subscribe to a topic.
    pub async fn subscribe(&self, topic: &str, qos: QoS) -> Result<(), MqttError> {
        self.client
            .subscribe(topic, qos)
            .await
            .map_err(|e| MqttError::Subscribe(e.to_string()))?;
        debug!("Subscribed to topic: {}", topic);
        Ok(())
    }

    /// Unsubscribe from a topic.
    pub async fn unsubscribe(&self, topic: &str) -> Result<(), MqttError> {
        self.client
            .unsubscribe(topic)
            .await
            .map_err(|e| MqttError::Subscribe(e.to_string()))?;
        debug!("Unsubscribed from topic: {}", topic);
        Ok(())
    }

    /// Publish a message to a topic.
    pub async fn publish(&self, topic: &str, payload: &[u8], qos: QoS) -> Result<(), MqttError> {
        self.client
            .publish(topic, qos, false, payload)
            .await
            .map_err(|e| MqttError::Publish(e.to_string()))?;
        debug!("Published {} bytes to topic: {}", payload.len(), topic);
        Ok(())
    }

    /// Publish a retained message to a topic.
    ///
    /// Retained messages are stored by the broker and sent to new subscribers.
    /// Used for system state like fs-root ID that clients need on connect.
    pub async fn publish_retained(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
    ) -> Result<(), MqttError> {
        self.client
            .publish(topic, qos, true, payload)
            .await
            .map_err(|e| MqttError::Publish(e.to_string()))?;
        debug!(
            "Published {} bytes (retained) to topic: {}",
            payload.len(),
            topic
        );
        Ok(())
    }

    /// Get a receiver for incoming messages.
    pub fn subscribe_messages(&self) -> broadcast::Receiver<IncomingMessage> {
        self.message_tx.subscribe()
    }

    /// Run the MQTT event loop.
    ///
    /// This should be spawned as a background task. It processes incoming
    /// messages and broadcasts them to subscribers.
    pub async fn run_event_loop(&self) -> Result<(), MqttError> {
        loop {
            let notification = {
                let mut event_loop = self.event_loop.lock().await;
                event_loop.poll().await
            };

            match notification {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let msg = IncomingMessage {
                        topic: publish.topic.clone(),
                        payload: publish.payload.to_vec(),
                    };

                    // Broadcast to all receivers (ignore if no receivers)
                    let _ = self.message_tx.send(msg);
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    debug!("Subscription acknowledged");
                }
                Ok(Event::Incoming(Packet::UnsubAck(_))) => {
                    debug!("Unsubscription acknowledged");
                }
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    info!("Connected to MQTT broker: {:?}", ack.code);
                }
                Ok(Event::Incoming(Packet::PingResp)) => {
                    // Ping response, ignore
                }
                Ok(Event::Outgoing(_)) => {
                    // Outgoing packet, ignore
                }
                Ok(event) => {
                    debug!("MQTT event: {:?}", event);
                }
                Err(e) => {
                    error!("MQTT error: {:?}", e);
                    // Connection errors will be retried by rumqttc
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Disconnect from the broker.
    pub async fn disconnect(&self) -> Result<(), MqttError> {
        self.client
            .disconnect()
            .await
            .map_err(|e| MqttError::Connection(e.to_string()))?;
        info!("Disconnected from MQTT broker");
        Ok(())
    }
}

/// Parse a broker URL into host and port.
fn parse_broker_url(url: &str) -> Result<(String, u16), MqttError> {
    // Handle mqtt:// or tcp:// prefix
    let stripped = url
        .strip_prefix("mqtt://")
        .or_else(|| url.strip_prefix("tcp://"))
        .unwrap_or(url);

    // Split host and port
    let parts: Vec<&str> = stripped.split(':').collect();

    let host = parts
        .first()
        .ok_or_else(|| MqttError::Connection(format!("Invalid broker URL: {}", url)))?
        .to_string();

    let port = if parts.len() > 1 {
        parts[1]
            .parse()
            .map_err(|_| MqttError::Connection(format!("Invalid port in URL: {}", url)))?
    } else {
        1883 // Default MQTT port
    };

    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_broker_url_with_port() {
        let (host, port) = parse_broker_url("mqtt://localhost:1883").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 1883);
    }

    #[test]
    fn test_parse_broker_url_without_scheme() {
        let (host, port) = parse_broker_url("localhost:1883").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 1883);
    }

    #[test]
    fn test_parse_broker_url_default_port() {
        let (host, port) = parse_broker_url("mqtt://broker.example.com").unwrap();
        assert_eq!(host, "broker.example.com");
        assert_eq!(port, 1883);
    }

    #[test]
    fn test_parse_broker_url_custom_port() {
        let (host, port) = parse_broker_url("mqtt://broker.example.com:8883").unwrap();
        assert_eq!(host, "broker.example.com");
        assert_eq!(port, 8883);
    }
}
