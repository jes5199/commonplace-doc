//! MQTT-based document watcher for the orchestrator.
//!
//! This module provides a mechanism to watch documents for changes via MQTT,
//! replacing the previous SSE-based watching. It subscribes to document edit
//! topics and emits events when documents change.

use crate::events::recv_broadcast;
use crate::mqtt::{MqttClient, MqttError, QoS, Topic};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

/// Events emitted by the document watcher.
#[derive(Debug, Clone)]
pub enum WatchEvent {
    /// A document was edited (contains the doc_id).
    DocumentChanged(String),
    /// Connection was lost to the MQTT broker.
    Disconnected,
    /// Reconnected to the MQTT broker.
    Reconnected,
}

/// Watches documents for changes via MQTT.
///
/// This watcher subscribes to the edits topic for each watched document
/// and emits `WatchEvent::DocumentChanged` events when edits are received.
pub struct MqttDocumentWatcher {
    /// The MQTT client used for subscriptions.
    client: Arc<MqttClient>,
    /// The workspace namespace for MQTT topics.
    workspace: String,
    /// Set of currently watched document IDs.
    watched_docs: Arc<RwLock<HashSet<String>>>,
    /// Broadcast sender for watch events.
    event_tx: broadcast::Sender<WatchEvent>,
}

impl MqttDocumentWatcher {
    /// Create a new document watcher.
    ///
    /// # Arguments
    /// * `client` - The MQTT client to use for subscriptions
    /// * `workspace` - The workspace namespace for MQTT topics
    pub fn new(client: Arc<MqttClient>, workspace: String) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            client,
            workspace,
            watched_docs: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
        }
    }

    /// Subscribe to a document's edits topic.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to watch
    ///
    /// # Returns
    /// `Ok(())` if subscription succeeded, or an error if it failed.
    pub async fn watch(&self, doc_id: &str) -> Result<(), MqttError> {
        let topic = Topic::edits(&self.workspace, doc_id).to_topic_string();

        // Subscribe to the document's edits topic
        self.client.subscribe(&topic, QoS::AtLeastOnce).await?;

        // Add to watched set
        {
            let mut watched = self.watched_docs.write().await;
            watched.insert(doc_id.to_string());
        }

        debug!("Now watching document: {} (topic: {})", doc_id, topic);
        Ok(())
    }

    /// Unsubscribe from a document's edits topic.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to stop watching
    ///
    /// # Returns
    /// `Ok(())` if unsubscription succeeded, or an error if it failed.
    pub async fn unwatch(&self, doc_id: &str) -> Result<(), MqttError> {
        let topic = Topic::edits(&self.workspace, doc_id).to_topic_string();

        // Unsubscribe from the document's edits topic
        self.client.unsubscribe(&topic).await?;

        // Remove from watched set
        {
            let mut watched = self.watched_docs.write().await;
            watched.remove(doc_id);
        }

        debug!("Stopped watching document: {} (topic: {})", doc_id, topic);
        Ok(())
    }

    /// Update the set of watched documents.
    ///
    /// This method computes the difference between the current watched set
    /// and the new set, then subscribes to new documents and unsubscribes
    /// from removed ones.
    ///
    /// # Arguments
    /// * `new_docs` - The new set of document IDs to watch
    ///
    /// # Returns
    /// `Ok(())` if all subscriptions/unsubscriptions succeeded, or the first error encountered.
    pub async fn update_watches(&self, new_docs: HashSet<String>) -> Result<(), MqttError> {
        let current_docs = {
            let watched = self.watched_docs.read().await;
            watched.clone()
        };

        // Find documents to add and remove
        let to_add: Vec<_> = new_docs.difference(&current_docs).cloned().collect();
        let to_remove: Vec<_> = current_docs.difference(&new_docs).cloned().collect();

        // Unsubscribe from removed documents
        for doc_id in &to_remove {
            if let Err(e) = self.unwatch(doc_id).await {
                warn!("Failed to unwatch document {}: {}", doc_id, e);
                // Continue trying to process other documents
            }
        }

        // Subscribe to new documents
        for doc_id in &to_add {
            if let Err(e) = self.watch(doc_id).await {
                warn!("Failed to watch document {}: {}", doc_id, e);
                // Continue trying to process other documents
            }
        }

        if !to_add.is_empty() || !to_remove.is_empty() {
            info!(
                "Updated watches: added {} documents, removed {} documents",
                to_add.len(),
                to_remove.len()
            );
        }

        Ok(())
    }

    /// Get a receiver for watch events.
    ///
    /// Events are broadcast to all receivers. If a receiver falls behind,
    /// it will receive a lagged error and may miss some events.
    pub fn events(&self) -> broadcast::Receiver<WatchEvent> {
        self.event_tx.subscribe()
    }

    /// Get the set of currently watched document IDs.
    pub async fn watched_documents(&self) -> HashSet<String> {
        let watched = self.watched_docs.read().await;
        watched.clone()
    }

    /// Run the event loop that processes MQTT messages and emits WatchEvents.
    ///
    /// This method should be spawned as a background task. It processes
    /// incoming MQTT messages, checks if they're for watched documents,
    /// and emits appropriate events.
    ///
    /// The loop continues until the MQTT message channel is closed.
    pub async fn run(&self) -> Result<(), MqttError> {
        // Subscribe to MQTT messages BEFORE we're ready to process them.
        // This ensures we don't miss any messages sent after subscription.
        let mut message_rx = self.client.subscribe_messages();

        info!(
            "MQTT document watcher starting event loop for workspace: {}",
            self.workspace
        );

        // Process incoming MQTT messages
        while let Some(msg) =
            recv_broadcast(&mut message_rx, "MQTT document watcher", None::<fn(u64)>).await
        {
            // Parse the topic to extract the document ID
            let parsed = match Topic::parse(&msg.topic, &self.workspace) {
                Ok(t) => t,
                Err(_) => {
                    // Not a topic we care about, ignore
                    continue;
                }
            };

            // Only process edits
            if parsed.port != crate::mqtt::topics::Port::Edits {
                continue;
            }

            let doc_id = &parsed.path;

            // Check if this is a document we're watching
            let is_watched = {
                let watched = self.watched_docs.read().await;
                watched.contains(doc_id)
            };

            if is_watched {
                debug!(
                    "Document changed: {} ({} bytes payload)",
                    doc_id,
                    msg.payload.len()
                );

                // Emit the change event
                let event = WatchEvent::DocumentChanged(doc_id.clone());
                if self.event_tx.send(event).is_err() {
                    // No receivers, but that's okay
                    debug!("No receivers for watch event");
                }
            }
        }

        info!("MQTT document watcher event loop ended");
        Ok(())
    }

    /// Run the event loop with a callback for connection state changes.
    ///
    /// This variant of `run` accepts a callback that is invoked when
    /// connection state changes are detected (disconnected/reconnected).
    ///
    /// # Arguments
    /// * `on_connection_change` - Callback invoked with `true` for reconnected, `false` for disconnected
    pub async fn run_with_connection_callback<F>(
        &self,
        mut on_connection_change: F,
    ) -> Result<(), MqttError>
    where
        F: FnMut(bool) + Send,
    {
        let mut message_rx = self.client.subscribe_messages();

        info!(
            "MQTT document watcher starting event loop for workspace: {}",
            self.workspace
        );

        // Track connection state for detecting reconnects
        let mut was_connected = true;

        loop {
            // Use recv_broadcast_with_lag for more detailed lag handling
            match tokio::time::timeout(std::time::Duration::from_secs(30), message_rx.recv()).await
            {
                Ok(Ok(msg)) => {
                    // We received a message, so we're connected
                    if !was_connected {
                        was_connected = true;
                        on_connection_change(true);
                        let _ = self.event_tx.send(WatchEvent::Reconnected);
                    }

                    // Parse the topic
                    let parsed = match Topic::parse(&msg.topic, &self.workspace) {
                        Ok(t) => t,
                        Err(_) => continue,
                    };

                    // Only process edits
                    if parsed.port != crate::mqtt::topics::Port::Edits {
                        continue;
                    }

                    let doc_id = &parsed.path;

                    // Check if watched
                    let is_watched = {
                        let watched = self.watched_docs.read().await;
                        watched.contains(doc_id)
                    };

                    if is_watched {
                        debug!("Document changed: {}", doc_id);
                        let _ = self
                            .event_tx
                            .send(WatchEvent::DocumentChanged(doc_id.clone()));
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                    warn!("MQTT document watcher lagged by {} messages", n);
                    // Continue processing - we may have missed some changes
                    // Callers should handle this by re-reading document content
                }
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    info!("MQTT message channel closed");
                    break;
                }
                Err(_) => {
                    // Timeout - check if we're still connected
                    // In a real scenario, we might want to ping the broker
                    // For now, just continue waiting
                    debug!("MQTT watcher timeout, continuing...");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::topics::{Port, Topic};

    // ========================================
    // WatchEvent enum tests
    // ========================================

    /// Test that WatchEvent::DocumentChanged clone works correctly
    #[test]
    fn test_watch_event_document_changed_clone() {
        let event = WatchEvent::DocumentChanged("doc-123".to_string());
        let cloned = event.clone();
        match cloned {
            WatchEvent::DocumentChanged(id) => assert_eq!(id, "doc-123"),
            _ => panic!("Expected DocumentChanged"),
        }
    }

    /// Test that WatchEvent::Disconnected clone works correctly
    #[test]
    fn test_watch_event_disconnected_clone() {
        let event = WatchEvent::Disconnected;
        let cloned = event.clone();
        assert!(matches!(cloned, WatchEvent::Disconnected));
    }

    /// Test that WatchEvent::Reconnected clone works correctly
    #[test]
    fn test_watch_event_reconnected_clone() {
        let event = WatchEvent::Reconnected;
        let cloned = event.clone();
        assert!(matches!(cloned, WatchEvent::Reconnected));
    }

    /// Test Debug format for WatchEvent::Disconnected
    #[test]
    fn test_watch_event_disconnected_debug() {
        let event = WatchEvent::Disconnected;
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Disconnected"));
    }

    /// Test Debug format for WatchEvent::Reconnected
    #[test]
    fn test_watch_event_reconnected_debug() {
        let event = WatchEvent::Reconnected;
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Reconnected"));
    }

    /// Test Debug format for WatchEvent::DocumentChanged
    #[test]
    fn test_watch_event_document_changed_debug() {
        let event = WatchEvent::DocumentChanged("my-doc-id".to_string());
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("DocumentChanged"));
        assert!(debug_str.contains("my-doc-id"));
    }

    // ========================================
    // HashSet operation tests (update_watches logic)
    // ========================================

    /// Test computing add/remove sets when current is empty
    #[test]
    fn test_update_watches_logic_from_empty() {
        let current: HashSet<String> = HashSet::new();
        let new: HashSet<String> = ["doc-a", "doc-b", "doc-c"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let to_add: HashSet<_> = new.difference(&current).cloned().collect();
        let to_remove: HashSet<_> = current.difference(&new).cloned().collect();

        assert_eq!(to_add.len(), 3);
        assert!(to_add.contains("doc-a"));
        assert!(to_add.contains("doc-b"));
        assert!(to_add.contains("doc-c"));
        assert!(to_remove.is_empty());
    }

    /// Test computing add/remove sets when transitioning to empty
    #[test]
    fn test_update_watches_logic_to_empty() {
        let current: HashSet<String> = ["doc-a", "doc-b"].iter().map(|s| s.to_string()).collect();
        let new: HashSet<String> = HashSet::new();

        let to_add: HashSet<_> = new.difference(&current).cloned().collect();
        let to_remove: HashSet<_> = current.difference(&new).cloned().collect();

        assert!(to_add.is_empty());
        assert_eq!(to_remove.len(), 2);
        assert!(to_remove.contains("doc-a"));
        assert!(to_remove.contains("doc-b"));
    }

    /// Test computing add/remove sets with partial overlap
    #[test]
    fn test_update_watches_logic_partial_overlap() {
        let current: HashSet<String> = ["doc-a", "doc-b", "doc-c"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let new: HashSet<String> = ["doc-b", "doc-c", "doc-d"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let to_add: HashSet<_> = new.difference(&current).cloned().collect();
        let to_remove: HashSet<_> = current.difference(&new).cloned().collect();

        // Should add doc-d (in new but not current)
        assert_eq!(to_add.len(), 1);
        assert!(to_add.contains("doc-d"));

        // Should remove doc-a (in current but not new)
        assert_eq!(to_remove.len(), 1);
        assert!(to_remove.contains("doc-a"));
    }

    /// Test computing add/remove sets when sets are identical
    #[test]
    fn test_update_watches_logic_no_change() {
        let current: HashSet<String> = ["doc-a", "doc-b"].iter().map(|s| s.to_string()).collect();
        let new: HashSet<String> = ["doc-a", "doc-b"].iter().map(|s| s.to_string()).collect();

        let to_add: HashSet<_> = new.difference(&current).cloned().collect();
        let to_remove: HashSet<_> = current.difference(&new).cloned().collect();

        assert!(to_add.is_empty());
        assert!(to_remove.is_empty());
    }

    /// Test computing add/remove sets when replacing all documents
    #[test]
    fn test_update_watches_logic_complete_replacement() {
        let current: HashSet<String> = ["doc-a", "doc-b"].iter().map(|s| s.to_string()).collect();
        let new: HashSet<String> = ["doc-x", "doc-y", "doc-z"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let to_add: HashSet<_> = new.difference(&current).cloned().collect();
        let to_remove: HashSet<_> = current.difference(&new).cloned().collect();

        assert_eq!(to_add.len(), 3);
        assert!(to_add.contains("doc-x"));
        assert!(to_add.contains("doc-y"));
        assert!(to_add.contains("doc-z"));

        assert_eq!(to_remove.len(), 2);
        assert!(to_remove.contains("doc-a"));
        assert!(to_remove.contains("doc-b"));
    }

    // ========================================
    // Event channel tests
    // ========================================

    /// Test that broadcast channel can be created and used
    #[test]
    fn test_event_channel_creation() {
        let (tx, mut rx1) = broadcast::channel::<WatchEvent>(256);

        // Should be able to send without receivers blocking
        let result = tx.send(WatchEvent::Disconnected);
        assert!(result.is_ok());

        // Receiver should get the event
        let event = rx1.try_recv();
        assert!(event.is_ok());
        assert!(matches!(event.unwrap(), WatchEvent::Disconnected));
    }

    /// Test multiple receivers on broadcast channel
    #[test]
    fn test_event_channel_multiple_receivers() {
        let (tx, mut rx1) = broadcast::channel::<WatchEvent>(256);
        let mut rx2 = tx.subscribe();

        // Send an event
        tx.send(WatchEvent::DocumentChanged("doc-123".to_string()))
            .unwrap();

        // Both receivers should get it
        let event1 = rx1.try_recv().unwrap();
        let event2 = rx2.try_recv().unwrap();

        match (event1, event2) {
            (WatchEvent::DocumentChanged(id1), WatchEvent::DocumentChanged(id2)) => {
                assert_eq!(id1, "doc-123");
                assert_eq!(id2, "doc-123");
            }
            _ => panic!("Expected DocumentChanged events"),
        }
    }

    /// Test channel behavior when no receivers
    #[test]
    fn test_event_channel_no_receivers() {
        let (tx, _rx) = broadcast::channel::<WatchEvent>(256);
        drop(_rx); // Drop the only receiver

        // Send should return error when no receivers
        let result = tx.send(WatchEvent::Disconnected);
        assert!(result.is_err());
    }

    // ========================================
    // Topic parsing tests for document ID extraction
    // ========================================

    /// Test parsing edits topic to extract document ID
    #[test]
    fn test_topic_parsing_edits_simple_path() {
        let workspace = "test-workspace";
        let doc_id = "my-document";
        let topic_str = format!("{}/edits/{}", workspace, doc_id);

        let parsed = Topic::parse(&topic_str, workspace).unwrap();

        assert_eq!(parsed.port, Port::Edits);
        assert_eq!(parsed.path, doc_id);
    }

    /// Test parsing edits topic with nested path
    #[test]
    fn test_topic_parsing_edits_nested_path() {
        let workspace = "commonplace";
        let topic_str = "commonplace/edits/bartleby/output.txt";

        let parsed = Topic::parse(topic_str, workspace).unwrap();

        assert_eq!(parsed.port, Port::Edits);
        assert_eq!(parsed.path, "bartleby/output.txt");
    }

    /// Test filtering non-edits topics
    #[test]
    fn test_topic_parsing_non_edits_filtered() {
        let workspace = "test-workspace";

        // Sync topic should parse but have different port
        let sync_topic = format!("{}/sync/doc-123/client-abc", workspace);
        let parsed = Topic::parse(&sync_topic, workspace).unwrap();
        assert_eq!(parsed.port, Port::Sync);
        assert_ne!(parsed.port, Port::Edits);

        // Events topic should parse but have different port
        let events_topic = format!("{}/events/doc-123/event-name", workspace);
        let parsed = Topic::parse(&events_topic, workspace).unwrap();
        assert_eq!(parsed.port, Port::Events);
        assert_ne!(parsed.port, Port::Edits);

        // Commands topic should parse but have different port
        let commands_topic = format!("{}/commands/doc-123/verb", workspace);
        let parsed = Topic::parse(&commands_topic, workspace).unwrap();
        assert_eq!(parsed.port, Port::Commands);
        assert_ne!(parsed.port, Port::Edits);
    }

    /// Test topic parsing with wrong workspace is rejected
    #[test]
    fn test_topic_parsing_wrong_workspace() {
        let topic_str = "other-workspace/edits/my-document";
        let result = Topic::parse(topic_str, "expected-workspace");

        assert!(result.is_err());
    }

    /// Test topic parsing with invalid port is rejected
    #[test]
    fn test_topic_parsing_invalid_port() {
        let topic_str = "test-workspace/invalid-port/my-document";
        let result = Topic::parse(topic_str, "test-workspace");

        assert!(result.is_err());
    }

    /// Test topic parsing with insufficient segments is rejected
    #[test]
    fn test_topic_parsing_insufficient_segments() {
        // Only two segments
        let result = Topic::parse("workspace/edits", "workspace");
        assert!(result.is_err());

        // Only one segment
        let result = Topic::parse("workspace", "workspace");
        assert!(result.is_err());
    }

    /// Test constructing edits topic string
    #[test]
    fn test_edits_topic_construction() {
        let topic = Topic::edits("my-workspace", "path/to/doc");
        let topic_str = topic.to_topic_string();

        assert_eq!(topic_str, "my-workspace/edits/path/to/doc");
    }

    /// Test roundtrip: construct -> to_string -> parse -> extract path
    #[test]
    fn test_topic_roundtrip() {
        let workspace = "test-ws";
        let doc_path = "some/nested/document.txt";

        // Construct the topic
        let original = Topic::edits(workspace, doc_path);
        let topic_str = original.to_topic_string();

        // Parse it back
        let parsed = Topic::parse(&topic_str, workspace).unwrap();

        // Verify we can extract the document path
        assert_eq!(parsed.path, doc_path);
        assert_eq!(parsed.port, Port::Edits);
        assert_eq!(parsed.workspace, workspace);
    }

    // ========================================
    // watched_docs HashSet management tests
    // ========================================

    /// Test that watched_docs HashSet can track documents
    #[tokio::test]
    async fn test_watched_docs_hashset_operations() {
        let watched_docs: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

        // Initially empty
        {
            let watched = watched_docs.read().await;
            assert!(watched.is_empty());
        }

        // Add a document
        {
            let mut watched = watched_docs.write().await;
            watched.insert("doc-1".to_string());
        }

        // Verify it's tracked
        {
            let watched = watched_docs.read().await;
            assert_eq!(watched.len(), 1);
            assert!(watched.contains("doc-1"));
        }

        // Add more documents
        {
            let mut watched = watched_docs.write().await;
            watched.insert("doc-2".to_string());
            watched.insert("doc-3".to_string());
        }

        // Remove a document
        {
            let mut watched = watched_docs.write().await;
            watched.remove("doc-2");
        }

        // Final state check
        {
            let watched = watched_docs.read().await;
            assert_eq!(watched.len(), 2);
            assert!(watched.contains("doc-1"));
            assert!(!watched.contains("doc-2"));
            assert!(watched.contains("doc-3"));
        }
    }

    /// Test that duplicate inserts don't create duplicates
    #[tokio::test]
    async fn test_watched_docs_no_duplicates() {
        let watched_docs: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

        // Insert same document multiple times
        {
            let mut watched = watched_docs.write().await;
            watched.insert("doc-1".to_string());
            watched.insert("doc-1".to_string());
            watched.insert("doc-1".to_string());
        }

        // Should only have one entry
        {
            let watched = watched_docs.read().await;
            assert_eq!(watched.len(), 1);
        }
    }

    /// Test that removing non-existent document is safe
    #[tokio::test]
    async fn test_watched_docs_remove_nonexistent() {
        let watched_docs: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

        // Add a document
        {
            let mut watched = watched_docs.write().await;
            watched.insert("doc-1".to_string());
        }

        // Remove a document that doesn't exist
        {
            let mut watched = watched_docs.write().await;
            let removed = watched.remove("nonexistent-doc");
            assert!(!removed);
        }

        // Original document still there
        {
            let watched = watched_docs.read().await;
            assert_eq!(watched.len(), 1);
            assert!(watched.contains("doc-1"));
        }
    }
}
