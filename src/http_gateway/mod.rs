//! HTTP Gateway - translates HTTP requests to MQTT messages
//!
//! This module provides a stateless HTTP interface that communicates
//! with the document store via MQTT.
//!
//! ## Response Correlation
//!
//! Request-response correlation is handled by `MqttRequestClient`:
//! - Each request registers with a unique ID and a oneshot channel
//! - A background task routes responses to the correct waiting request
//! - This ensures concurrent requests never receive each other's responses

mod api;
mod sse;

pub use api::router;

use crate::mqtt::{client::MqttClient, MqttConfig, MqttError, MqttRequestClient};
use rumqttc::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// HTTP Gateway that translates HTTP to MQTT
pub struct HttpGateway {
    /// Shared MQTT request/response client (handles correlation)
    pub(crate) request_client: Arc<MqttRequestClient>,
    /// Reference counts for MQTT topic subscriptions (for SSE)
    pub(crate) subscription_counts: Arc<RwLock<HashMap<String, usize>>>,
}

impl HttpGateway {
    /// Create a new HTTP gateway connected to MQTT
    ///
    /// This spawns the MQTT event loop in the background so that
    /// publish/subscribe operations can actually execute.
    /// Also spawns a response dispatcher via MqttRequestClient.
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

        let request_client = Arc::new(MqttRequestClient::new(client, workspace).await?);

        Ok(Self {
            request_client,
            subscription_counts: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get a reference to the MQTT client
    pub fn client(&self) -> &Arc<MqttClient> {
        self.request_client.client()
    }

    /// Get the workspace name
    pub fn workspace(&self) -> &str {
        self.request_client.workspace()
    }

    /// Increment the subscription count for a topic.
    /// If this is the first subscriber, actually subscribe to MQTT.
    pub(crate) async fn add_subscriber(&self, topic: &str) -> Result<(), MqttError> {
        let mut counts = self.subscription_counts.write().await;
        let count = counts.entry(topic.to_string()).or_insert(0);

        if *count == 0 {
            // First subscriber - actually subscribe to MQTT
            // Subscribe before incrementing count so failures don't leave stale counts
            self.client().subscribe(topic, QoS::AtLeastOnce).await?;
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
                if let Err(e) = self.client().unsubscribe(topic).await {
                    tracing::warn!("Failed to unsubscribe from {}: {}", topic, e);
                } else {
                    tracing::debug!("SSE unsubscribed from MQTT topic: {}", topic);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::request::PendingRequests;
    use tokio::sync::oneshot;

    /// Create a pending requests map for testing the correlation logic
    fn create_pending_requests_only() -> PendingRequests {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[tokio::test]
    async fn test_register_pending_request() {
        let pending = create_pending_requests_only();

        // Register a request
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending_guard = pending.write().await;
            pending_guard.insert("req-1".to_string(), tx);
        }

        // Verify it's registered
        assert_eq!(pending.read().await.len(), 1);
        assert!(pending.read().await.contains_key("req-1"));
    }

    #[tokio::test]
    async fn test_dispatch_response_to_correct_request() {
        let pending = create_pending_requests_only();

        // Register multiple requests
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();

        {
            let mut pending_guard = pending.write().await;
            pending_guard.insert("req-1".to_string(), tx1);
            pending_guard.insert("req-2".to_string(), tx2);
            pending_guard.insert("req-3".to_string(), tx3);
        }

        // Dispatch response to req-2
        let payload = b"response for req-2".to_vec();
        {
            let mut pending_guard = pending.write().await;
            if let Some(tx) = pending_guard.remove("req-2") {
                tx.send(payload.clone()).unwrap();
            }
        }

        // Verify req-2 received the correct response
        let received = rx2.await.unwrap();
        assert_eq!(received, payload);

        // Verify req-1 and req-3 are still pending
        assert_eq!(pending.read().await.len(), 2);
        assert!(pending.read().await.contains_key("req-1"));
        assert!(pending.read().await.contains_key("req-3"));

        // Cleanup: drop remaining senders so receivers don't hang
        drop(pending);
    }

    #[tokio::test]
    async fn test_concurrent_requests_receive_correct_responses() {
        let pending = create_pending_requests_only();

        // Simulate 10 concurrent requests
        let mut receivers = Vec::new();
        for i in 0..10 {
            let (tx, rx) = oneshot::channel();
            let req_id = format!("concurrent-req-{}", i);
            {
                let mut pending_guard = pending.write().await;
                pending_guard.insert(req_id, tx);
            }
            receivers.push((i, rx));
        }

        // Dispatch responses in random order (3, 7, 1, 9, 0, 5, 2, 8, 4, 6)
        let dispatch_order = [3, 7, 1, 9, 0, 5, 2, 8, 4, 6];
        for &i in &dispatch_order {
            let req_id = format!("concurrent-req-{}", i);
            let payload = format!("response-for-{}", i).into_bytes();
            {
                let mut pending_guard = pending.write().await;
                if let Some(tx) = pending_guard.remove(&req_id) {
                    tx.send(payload).unwrap();
                }
            }
        }

        // Verify each request received its own response
        for (i, rx) in receivers {
            let received = rx.await.unwrap();
            let expected = format!("response-for-{}", i);
            assert_eq!(
                String::from_utf8(received).unwrap(),
                expected,
                "Request {} received wrong response",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_response_for_unknown_request_is_ignored() {
        let pending = create_pending_requests_only();

        // Register one request
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending_guard = pending.write().await;
            pending_guard.insert("req-1".to_string(), tx);
        }

        // Try to dispatch to a non-existent request
        {
            let mut pending_guard = pending.write().await;
            let result = pending_guard.remove("req-999");
            assert!(result.is_none());
        }

        // Original request should still be pending
        assert_eq!(pending.read().await.len(), 1);

        // Cleanup
        drop(pending);
    }

    #[tokio::test]
    async fn test_timeout_cleanup() {
        let pending = create_pending_requests_only();

        // Register a request
        let (tx, rx) = oneshot::channel();
        {
            let mut pending_guard = pending.write().await;
            pending_guard.insert("req-timeout".to_string(), tx);
        }

        // Simulate timeout by dropping the receiver and cleaning up
        drop(rx);

        // Remove from pending (as would happen on timeout)
        {
            let mut pending_guard = pending.write().await;
            pending_guard.remove("req-timeout");
        }

        // Verify it's cleaned up
        assert_eq!(pending.read().await.len(), 0);
    }

    #[tokio::test]
    async fn test_response_after_timeout_is_harmless() {
        let pending = create_pending_requests_only();

        // Register a request
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending_guard = pending.write().await;
            pending_guard.insert("req-late".to_string(), tx);
        }

        // Simulate timeout - receiver dropped, entry removed (rx already dropped)
        {
            let mut pending_guard = pending.write().await;
            pending_guard.remove("req-late");
        }

        // Late response arrives - should be ignored gracefully
        {
            let pending_guard = pending.read().await;
            assert!(!pending_guard.contains_key("req-late"));
        }
    }

    #[tokio::test]
    async fn test_parallel_register_and_dispatch() {
        use tokio::task::JoinSet;

        let pending = create_pending_requests_only();

        // Spawn parallel tasks that register and dispatch
        let mut tasks = JoinSet::new();

        // Register 50 requests in parallel
        for i in 0..50 {
            let pending_clone = pending.clone();
            tasks.spawn(async move {
                let (tx, rx) = oneshot::channel::<Vec<u8>>();
                let req_id = format!("parallel-req-{}", i);
                {
                    let mut pending_guard = pending_clone.write().await;
                    pending_guard.insert(req_id.clone(), tx);
                }
                (i, req_id, rx)
            });
        }

        // Collect all registered requests
        let mut registered = Vec::new();
        while let Some(result) = tasks.join_next().await {
            registered.push(result.unwrap());
        }

        // Verify all 50 are registered
        assert_eq!(pending.read().await.len(), 50);

        // Dispatch all responses in parallel
        let mut dispatch_tasks = JoinSet::new();
        for (i, req_id, _rx) in &registered {
            let pending_clone = pending.clone();
            let req_id = req_id.clone();
            let i = *i;
            dispatch_tasks.spawn(async move {
                let payload = format!("parallel-response-{}", i).into_bytes();
                let mut pending_guard = pending_clone.write().await;
                if let Some(tx) = pending_guard.remove(&req_id) {
                    let _ = tx.send(payload);
                }
            });
        }

        // Wait for all dispatches
        while dispatch_tasks.join_next().await.is_some() {}

        // Verify all requests received correct responses
        for (i, _req_id, rx) in registered {
            let received = rx.await.unwrap();
            let expected = format!("parallel-response-{}", i);
            assert_eq!(
                String::from_utf8(received).unwrap(),
                expected,
                "Parallel request {} received wrong response",
                i
            );
        }

        // Verify all pending requests are cleared
        assert_eq!(pending.read().await.len(), 0);
    }
}
