//! MQTT request/response client.
//!
//! Provides request-response semantics over MQTT using the commonplace
//! command protocol. Publishes requests to `{workspace}/commands/__system/{verb}`
//! and correlates responses from `{workspace}/responses` using a `req` field.
//!
//! Extracted from http_gateway/mod.rs for reuse by the orchestrator and
//! sync client (CP-8j6c).

use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{GetContentRequest, GetContentResponse};
use crate::mqtt::MqttError;
use rumqttc::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};
use tracing::{debug, trace, warn};

/// Type alias for the pending request map.
pub type PendingRequests = Arc<RwLock<HashMap<String, oneshot::Sender<Vec<u8>>>>>;

/// Default timeout for MQTT request/response operations.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// MQTT request/response client.
///
/// Subscribes to `{workspace}/responses` and routes incoming responses
/// to pending requests by matching the `req` correlation ID.
///
/// This is the MQTT equivalent of HTTP GET — publish a command,
/// wait for the response.
pub struct MqttRequestClient {
    client: Arc<MqttClient>,
    workspace: String,
    pending: PendingRequests,
    timeout: Duration,
}

impl MqttRequestClient {
    /// Create a new MQTT request client and start the response dispatcher.
    ///
    /// This subscribes to `{workspace}/responses` and spawns a background
    /// task that routes responses to pending requests.
    pub async fn new(client: Arc<MqttClient>, workspace: String) -> Result<Self, MqttError> {
        let pending: PendingRequests = Arc::new(RwLock::new(HashMap::new()));

        let responses_topic = format!("{}/responses", workspace);
        client.subscribe(&responses_topic, QoS::AtLeastOnce).await?;
        debug!("MqttRequestClient subscribed to {}", responses_topic);

        // Spawn response dispatcher (same pattern as http_gateway/mod.rs:94-139)
        let mut rx = client.subscribe_messages();
        let pending_clone = pending.clone();
        let topic = responses_topic.clone();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        if msg.topic != topic {
                            continue;
                        }
                        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                        {
                            if let Some(req_id) = json.get("req").and_then(|v| v.as_str()) {
                                let sender = {
                                    let mut guard = pending_clone.write().await;
                                    guard.remove(req_id)
                                };
                                if let Some(tx) = sender {
                                    let _ = tx.send(msg.payload);
                                } else {
                                    trace!("No pending request for response req={}", req_id);
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("MqttRequestClient dispatcher lagged {} messages", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        debug!("MqttRequestClient dispatcher shutting down");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            client,
            workspace,
            pending,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Set the timeout for request/response operations.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Register a pending request and return a receiver for the response.
    ///
    /// The caller should await on the returned receiver with a timeout.
    /// The request ID must match the `req` field in the response.
    pub async fn register_pending_request(&self, req_id: String) -> oneshot::Receiver<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.pending.write().await;
            guard.insert(req_id, tx);
        }
        rx
    }

    /// Remove a pending request (used for cleanup on timeout/error).
    pub async fn remove_pending_request(&self, req_id: &str) {
        let mut guard = self.pending.write().await;
        guard.remove(req_id);
    }

    /// Get document content by ID.
    ///
    /// Publishes a `GetContentRequest` to `{workspace}/commands/__system/get-content`
    /// and waits for the matching `GetContentResponse`.
    pub async fn get_content(&self, id: &str) -> Result<GetContentResponse, MqttError> {
        let req_id = uuid::Uuid::new_v4().to_string();

        let request = GetContentRequest {
            req: req_id.clone(),
            id: id.to_string(),
        };

        let payload =
            serde_json::to_vec(&request).map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        // Register pending request
        let rx = self.register_pending_request(req_id.clone()).await;

        // Publish request
        let topic = format!("{}/commands/__system/get-content", self.workspace);
        self.client
            .publish(&topic, &payload, QoS::AtLeastOnce)
            .await?;

        // Wait for response with timeout
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(response_bytes)) => serde_json::from_slice(&response_bytes)
                .map_err(|e| MqttError::InvalidMessage(e.to_string())),
            Ok(Err(_)) => Err(MqttError::InvalidMessage(
                "Response channel closed unexpectedly".to_string(),
            )),
            Err(_) => {
                // Timeout - clean up pending request
                self.remove_pending_request(&req_id).await;
                Err(MqttError::Connection(format!(
                    "Timeout waiting for get-content response for {}",
                    id
                )))
            }
        }
    }

    /// Get a reference to the underlying MQTT client.
    pub fn client(&self) -> &Arc<MqttClient> {
        &self.client
    }

    /// Get the workspace name.
    pub fn workspace(&self) -> &str {
        &self.workspace
    }

    /// Get a reference to the pending requests map (for shared use by HttpGateway).
    pub fn pending(&self) -> &PendingRequests {
        &self.pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that register + dispatch delivers the response to the correct receiver.
    #[tokio::test]
    async fn test_register_and_dispatch() {
        let pending: PendingRequests = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = oneshot::channel();
        {
            pending.write().await.insert("req-1".to_string(), tx);
        }
        // Simulate dispatcher delivering a response
        {
            let mut guard = pending.write().await;
            if let Some(sender) = guard.remove("req-1") {
                sender.send(b"response-payload".to_vec()).unwrap();
            }
        }
        let result = rx.await.unwrap();
        assert_eq!(result, b"response-payload");
    }

    /// Test that unknown request IDs are silently ignored.
    #[tokio::test]
    async fn test_unknown_request_ignored() {
        let pending: PendingRequests = Arc::new(RwLock::new(HashMap::new()));
        let (tx, _rx) = oneshot::channel();
        pending.write().await.insert("req-1".to_string(), tx);

        // Try to dispatch to non-existent request
        let result = pending.write().await.remove("req-999");
        assert!(result.is_none());
        // Original request still pending
        assert!(pending.read().await.contains_key("req-1"));
    }

    /// Test get_content request serialization.
    #[test]
    fn test_get_content_request_serialization() {
        let req = crate::mqtt::messages::GetContentRequest {
            req: "test-req".to_string(),
            id: "doc-123".to_string(),
        };
        let json = serde_json::to_vec(&req).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["req"], "test-req");
        assert_eq!(parsed["id"], "doc-123");
    }

    /// Test get_content response deserialization (success case).
    #[test]
    fn test_get_content_response_success() {
        let json = r#"{"req":"r-1","content":"hello","content_type":"text/plain"}"#;
        let resp: crate::mqtt::messages::GetContentResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.req, "r-1");
        assert_eq!(resp.content.as_deref(), Some("hello"));
        assert!(resp.error.is_none());
    }

    /// Test get_content response deserialization (error case).
    #[test]
    fn test_get_content_response_error() {
        let json = r#"{"req":"r-1","error":"Document not found"}"#;
        let resp: crate::mqtt::messages::GetContentResponse = serde_json::from_str(json).unwrap();
        assert!(resp.content.is_none());
        assert_eq!(resp.error.as_deref(), Some("Document not found"));
    }
}
