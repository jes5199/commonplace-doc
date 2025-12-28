//! Edits port handler.
//!
//! Subscribes to `{path}/edits` topics, persists Yjs updates to the commit store,
//! and applies them to document nodes.
//!
//! IMPORTANT: The doc store does NOT re-emit edits. MQTT broker handles fanout.

use crate::commit::Commit;
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::EditMessage;
use crate::mqtt::topics::{content_type_for_path, Topic};
use crate::mqtt::MqttError;
use crate::node::{Edit, NodeId, NodeRegistry};
use crate::store::CommitStore;
use rumqttc::QoS;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Handler for the edits port.
pub struct EditsHandler {
    client: Arc<MqttClient>,
    node_registry: Arc<NodeRegistry>,
    commit_store: Option<Arc<CommitStore>>,
    subscribed_paths: RwLock<HashSet<String>>,
}

impl EditsHandler {
    /// Create a new edits handler.
    pub fn new(
        client: Arc<MqttClient>,
        node_registry: Arc<NodeRegistry>,
        commit_store: Option<Arc<CommitStore>>,
    ) -> Self {
        Self {
            client,
            node_registry,
            commit_store,
            subscribed_paths: RwLock::new(HashSet::new()),
        }
    }

    /// Subscribe to edits for a path.
    pub async fn subscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic = Topic::edits(path);
        let topic_str = topic.to_topic_string();

        // Use QoS 1 (at least once) for edits - important not to lose them
        self.client.subscribe(&topic_str, QoS::AtLeastOnce).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.insert(path.to_string());

        debug!("Subscribed to edits for path: {}", path);
        Ok(())
    }

    /// Unsubscribe from edits for a path.
    pub async fn unsubscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic = Topic::edits(path);
        let topic_str = topic.to_topic_string();

        self.client.unsubscribe(&topic_str).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.remove(path);

        debug!("Unsubscribed from edits for path: {}", path);
        Ok(())
    }

    /// Handle an incoming edit message.
    ///
    /// This:
    /// 1. Parses the EditMessage
    /// 2. Creates a Commit and stores it (if commit_store is available)
    /// 3. Applies the update to the DocumentNode
    ///
    /// IMPORTANT: Does NOT re-emit. MQTT broker handles fanout.
    pub async fn handle_edit(&self, topic: &Topic, payload: &[u8]) -> Result<(), MqttError> {
        // Parse the edit message
        let edit_msg: EditMessage =
            serde_json::from_slice(payload).map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        debug!(
            "Received edit for path: {} from author: {}",
            topic.path, edit_msg.author
        );

        // Create commit from the edit message
        let commit = Commit {
            parents: edit_msg.parents.clone(),
            timestamp: edit_msg.timestamp,
            update: edit_msg.update.clone(),
            author: edit_msg.author.clone(),
            message: edit_msg.message.clone(),
            extensions: Default::default(),
        };

        // Store the commit if we have a commit store
        let cid = if let Some(store) = &self.commit_store {
            let cid = store.store_commit(&commit).await?;

            // Update document head
            // Use path as the document ID
            store.set_document_head(&topic.path, &cid).await?;

            debug!("Stored commit {} for path {}", cid, topic.path);
            Some(cid)
        } else {
            None
        };

        // Get or create the document node
        let node_id = NodeId::new(&topic.path);
        let content_type = content_type_for_path(&topic.path)?;

        let node = self
            .node_registry
            .get_or_create_document(&node_id, content_type)
            .await
            .map_err(|e| MqttError::Node(e.to_string()))?;

        // Create Edit and apply to node
        // Note: We pass the commit without re-emitting
        let edit = Edit {
            source: NodeId::new("mqtt"),
            commit: Arc::new(commit),
        };

        // Apply the edit to the node
        // The node will update its internal state but we don't re-emit to MQTT
        if let Err(e) = node.receive_edit(edit).await {
            debug!("Error applying edit to node {}: {:?}", topic.path, e);
        }

        debug!(
            "Applied edit to node {} (cid: {:?})",
            topic.path,
            cid.as_deref().unwrap_or("none")
        );

        Ok(())
    }

    /// Check if a path is subscribed.
    pub async fn is_subscribed(&self, path: &str) -> bool {
        let paths = self.subscribed_paths.read().await;
        paths.contains(path)
    }

    /// Get all subscribed paths.
    pub async fn subscribed_paths(&self) -> Vec<String> {
        let paths = self.subscribed_paths.read().await;
        paths.iter().cloned().collect()
    }
}
