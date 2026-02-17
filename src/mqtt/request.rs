//! MQTT request/response client.
//!
//! Provides request-response semantics over MQTT using the commonplace
//! command protocol. Publishes requests to `{workspace}/commands/__system/{verb}`
//! and correlates responses from `{workspace}/responses` using a `req` field.
//!
//! Extracted from http_gateway/mod.rs for reuse by the orchestrator and
//! sync client (CP-8j6c).

use crate::fs::{Entry, FsSchema};
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{
    CreateDocumentRequest, CreateDocumentResponse, ForkDocumentRequest, ForkDocumentResponse,
    GetContentRequest, GetContentResponse, GetInfoRequest, GetInfoResponse,
};
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
/// Maximum attempts for request/response operations when timeout occurs.
const DEFAULT_MAX_ATTEMPTS: usize = 4;
/// Base delay before retrying a timed-out request.
const RETRY_BASE_DELAY_MS: u64 = 200;

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
    fn retry_delay(attempt: usize) -> Duration {
        // Exponential backoff: 200ms, 400ms, 800ms...
        let exponent = attempt.saturating_sub(1).min(5) as u32;
        Duration::from_millis(RETRY_BASE_DELAY_MS * (1u64 << exponent))
    }

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
        let topic = format!("{}/commands/__system/get-content", self.workspace);
        for attempt in 1..=DEFAULT_MAX_ATTEMPTS {
            let req_id = uuid::Uuid::new_v4().to_string();
            let request = GetContentRequest {
                req: req_id.clone(),
                id: id.to_string(),
            };
            let payload = serde_json::to_vec(&request)
                .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

            // Register pending request
            let rx = self.register_pending_request(req_id.clone()).await;

            // Publish request
            self.client
                .publish(&topic, &payload, QoS::AtLeastOnce)
                .await?;

            // Wait for response with timeout
            match tokio::time::timeout(self.timeout, rx).await {
                Ok(Ok(response_bytes)) => {
                    return serde_json::from_slice(&response_bytes)
                        .map_err(|e| MqttError::InvalidMessage(e.to_string()));
                }
                Ok(Err(_)) => {
                    return Err(MqttError::InvalidMessage(
                        "Response channel closed unexpectedly".to_string(),
                    ));
                }
                Err(_) => {
                    // Timeout - clean up pending request
                    self.remove_pending_request(&req_id).await;

                    if attempt == DEFAULT_MAX_ATTEMPTS {
                        return Err(MqttError::Connection(format!(
                            "Timeout waiting for get-content response for {} after {} attempts",
                            id, DEFAULT_MAX_ATTEMPTS
                        )));
                    }

                    let delay = Self::retry_delay(attempt);
                    warn!(
                        "Timeout waiting for get-content response for {} (attempt {}/{}), retrying in {}ms",
                        id,
                        attempt,
                        DEFAULT_MAX_ATTEMPTS,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        unreachable!("retry loop should always return or error");
    }

    /// Get document info/metadata by ID.
    ///
    /// Publishes a `GetInfoRequest` to `{workspace}/commands/__system/get-info`
    /// and waits for the matching `GetInfoResponse`.
    pub async fn get_info(&self, id: &str) -> Result<GetInfoResponse, MqttError> {
        let topic = format!("{}/commands/__system/get-info", self.workspace);
        for attempt in 1..=DEFAULT_MAX_ATTEMPTS {
            let req_id = uuid::Uuid::new_v4().to_string();
            let request = GetInfoRequest {
                req: req_id.clone(),
                id: id.to_string(),
            };
            let payload = serde_json::to_vec(&request)
                .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

            let rx = self.register_pending_request(req_id.clone()).await;

            self.client
                .publish(&topic, &payload, QoS::AtLeastOnce)
                .await?;

            match tokio::time::timeout(self.timeout, rx).await {
                Ok(Ok(response_bytes)) => {
                    return serde_json::from_slice(&response_bytes)
                        .map_err(|e| MqttError::InvalidMessage(e.to_string()));
                }
                Ok(Err(_)) => {
                    return Err(MqttError::InvalidMessage(
                        "Response channel closed unexpectedly".to_string(),
                    ));
                }
                Err(_) => {
                    self.remove_pending_request(&req_id).await;

                    if attempt == DEFAULT_MAX_ATTEMPTS {
                        return Err(MqttError::Connection(format!(
                            "Timeout waiting for get-info response for {} after {} attempts",
                            id, DEFAULT_MAX_ATTEMPTS
                        )));
                    }

                    let delay = Self::retry_delay(attempt);
                    warn!(
                        "Timeout waiting for get-info response for {} (attempt {}/{}), retrying in {}ms",
                        id,
                        attempt,
                        DEFAULT_MAX_ATTEMPTS,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        unreachable!("retry loop should always return or error");
    }

    /// Create a new document.
    ///
    /// Publishes a `CreateDocumentRequest` to `{workspace}/commands/__system/create-document`
    /// and waits for the matching `CreateDocumentResponse`.
    ///
    /// If `id` is `Some`, the document will be created with the specified ID.
    /// Otherwise, a random UUID will be generated.
    pub async fn create_document(
        &self,
        content_type: &str,
        id: Option<&str>,
    ) -> Result<CreateDocumentResponse, MqttError> {
        let req_id = uuid::Uuid::new_v4().to_string();

        let request = CreateDocumentRequest {
            req: req_id.clone(),
            content_type: content_type.to_string(),
            id: id.map(|s| s.to_string()),
        };

        let payload =
            serde_json::to_vec(&request).map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        let rx = self.register_pending_request(req_id.clone()).await;

        let topic = format!("{}/commands/__system/create-document", self.workspace);
        self.client
            .publish(&topic, &payload, QoS::AtLeastOnce)
            .await?;

        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(response_bytes)) => serde_json::from_slice(&response_bytes)
                .map_err(|e| MqttError::InvalidMessage(e.to_string())),
            Ok(Err(_)) => Err(MqttError::InvalidMessage(
                "Response channel closed unexpectedly".to_string(),
            )),
            Err(_) => {
                self.remove_pending_request(&req_id).await;
                Err(MqttError::Connection(
                    "Timeout waiting for create-document response".to_string(),
                ))
            }
        }
    }

    /// Fork a document, optionally at a specific commit.
    ///
    /// Publishes a `ForkDocumentRequest` to `{workspace}/commands/__system/fork-document`
    /// and waits for the matching `ForkDocumentResponse`.
    pub async fn fork_document(
        &self,
        source_id: &str,
        at_commit: Option<&str>,
    ) -> Result<ForkDocumentResponse, MqttError> {
        let req_id = uuid::Uuid::new_v4().to_string();

        let request = ForkDocumentRequest {
            req: req_id.clone(),
            source_id: source_id.to_string(),
            at_commit: at_commit.map(|s| s.to_string()),
        };

        let payload =
            serde_json::to_vec(&request).map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        let rx = self.register_pending_request(req_id.clone()).await;

        let topic = format!("{}/commands/__system/fork-document", self.workspace);
        self.client
            .publish(&topic, &payload, QoS::AtLeastOnce)
            .await?;

        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(response_bytes)) => serde_json::from_slice(&response_bytes)
                .map_err(|e| MqttError::InvalidMessage(e.to_string())),
            Ok(Err(_)) => Err(MqttError::InvalidMessage(
                "Response channel closed unexpectedly".to_string(),
            )),
            Err(_) => {
                self.remove_pending_request(&req_id).await;
                Err(MqttError::Connection(format!(
                    "Timeout waiting for fork-document response for {}",
                    source_id
                )))
            }
        }
    }

    /// Discover the fs-root document ID via MQTT retained message.
    ///
    /// Subscribes to `{workspace}/_system/fs-root` and reads the retained message
    /// published by the server at startup. This is the MQTT equivalent of the
    /// HTTP `GET /fs-root` endpoint.
    pub async fn discover_fs_root(&self) -> Result<String, MqttError> {
        let topic = format!("{}/_system/fs-root", self.workspace);

        // Create receiver BEFORE subscribing so we catch the retained message
        let mut rx = self.client.subscribe_messages();
        self.client.subscribe(&topic, QoS::AtLeastOnce).await?;

        // Wait for the retained message
        let result = tokio::time::timeout(self.timeout, async {
            loop {
                match rx.recv().await {
                    Ok(msg) if msg.topic == topic => {
                        let id = String::from_utf8_lossy(&msg.payload).to_string();
                        if id.is_empty() {
                            return Err(MqttError::InvalidMessage(
                                "Empty fs-root retained message".to_string(),
                            ));
                        }
                        return Ok(id);
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => {
                        return Err(MqttError::InvalidMessage(
                            "Message channel closed".to_string(),
                        ))
                    }
                }
            }
        })
        .await;

        match result {
            Ok(Ok(id)) => {
                debug!("Discovered fs-root via MQTT: {}", id);
                Ok(id)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(MqttError::Connection(
                "Timeout waiting for fs-root retained message (server may not have --fs-root configured)".to_string(),
            )),
        }
    }

    /// Resolve a path relative to fs-root to a UUID by traversing schema documents via MQTT.
    ///
    /// This is the MQTT equivalent of HTTP path traversal:
    /// 1. Fetch schema content for the current directory node
    /// 2. Find the next segment entry
    /// 3. Follow its node_id
    pub async fn resolve_path_to_uuid(
        &self,
        fs_root_id: &str,
        path: &str,
    ) -> Result<String, MqttError> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return Ok(fs_root_id.to_string());
        }

        let mut current_id = fs_root_id.to_string();

        for (i, segment) in segments.iter().enumerate() {
            let traversed = segments[..=i].join("/");
            let response = self.get_content(&current_id).await.map_err(|e| {
                MqttError::Node(format!("Failed to fetch schema for '{}': {}", traversed, e))
            })?;

            if let Some(error) = response.error {
                return Err(MqttError::Node(format!(
                    "Failed to fetch schema for '{}': {}",
                    traversed, error
                )));
            }

            let content = response.content.ok_or_else(|| {
                MqttError::Node(format!(
                    "Failed to fetch schema for '{}': empty content",
                    traversed
                ))
            })?;

            let schema: FsSchema = if content.trim() == "{}" {
                FsSchema {
                    version: 1,
                    root: None,
                }
            } else {
                serde_json::from_str(&content).map_err(|e| {
                    MqttError::InvalidMessage(format!(
                        "Failed to parse schema for '{}': {}",
                        traversed, e
                    ))
                })?
            };

            let parent_scope = if i == 0 {
                "fs-root".to_string()
            } else {
                segments[..i].join("/")
            };

            let entries = schema
                .root
                .as_ref()
                .and_then(|root| match root {
                    Entry::Dir(dir) => dir.entries.as_ref(),
                    Entry::Doc(_) => None,
                })
                .ok_or_else(|| {
                    MqttError::Node(format!(
                        "Path '{}' not found: '{}' has no entries",
                        path, parent_scope
                    ))
                })?;

            let entry = entries.get(*segment).ok_or_else(|| {
                let mut available: Vec<_> = entries.keys().map(|k| k.as_str()).collect();
                available.sort_unstable();
                MqttError::Node(format!(
                    "Path '{}' not found: no entry '{}' in '{}'. Available: {:?}",
                    path, segment, parent_scope, available
                ))
            })?;

            let node_id = match entry {
                Entry::Dir(dir) => dir.node_id.as_ref(),
                Entry::Doc(doc) => doc.node_id.as_ref(),
            }
            .ok_or_else(|| {
                MqttError::Node(format!(
                    "Path '{}' not found: entry '{}' has no node_id",
                    path, segment
                ))
            })?;

            current_id = node_id.clone();
        }

        Ok(current_id)
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
