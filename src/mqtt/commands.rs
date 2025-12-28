//! Commands port handler.
//!
//! Subscribes to `{path}/commands/{verb}` topics and dispatches commands to nodes.

use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::CommandMessage;
use crate::mqtt::topics::Topic;
use crate::mqtt::MqttError;
use crate::node::{Event, NodeId, NodeRegistry};
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Handler for the commands port.
pub struct CommandsHandler {
    client: Arc<MqttClient>,
    node_registry: Arc<NodeRegistry>,
    /// Map of path -> set of subscribed verbs
    subscribed_commands: RwLock<HashMap<String, HashSet<String>>>,
}

impl CommandsHandler {
    /// Create a new commands handler.
    pub fn new(client: Arc<MqttClient>, node_registry: Arc<NodeRegistry>) -> Self {
        Self {
            client,
            node_registry,
            subscribed_commands: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe to commands for a path.
    /// Uses wildcard: `{path}/commands/#`
    pub async fn subscribe_commands(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::commands_wildcard(path);

        // Use QoS 1 for commands - important not to lose them
        self.client
            .subscribe(&topic_pattern, QoS::AtLeastOnce)
            .await?;

        let mut commands = self.subscribed_commands.write().await;
        commands.entry(path.to_string()).or_default();

        debug!("Subscribed to commands for path: {}", path);
        Ok(())
    }

    /// Subscribe to a specific command verb for a path.
    pub async fn subscribe_command(&self, path: &str, verb: &str) -> Result<(), MqttError> {
        let topic = Topic::commands(path, verb);
        let topic_str = topic.to_topic_string();

        // Use QoS 1 for commands
        self.client.subscribe(&topic_str, QoS::AtLeastOnce).await?;

        let mut commands = self.subscribed_commands.write().await;
        commands
            .entry(path.to_string())
            .or_default()
            .insert(verb.to_string());

        debug!("Subscribed to command '{}' for path: {}", verb, path);
        Ok(())
    }

    /// Unsubscribe from commands for a path.
    pub async fn unsubscribe_commands(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::commands_wildcard(path);

        self.client.unsubscribe(&topic_pattern).await?;

        let mut commands = self.subscribed_commands.write().await;
        commands.remove(path);

        debug!("Unsubscribed from commands for path: {}", path);
        Ok(())
    }

    /// Handle an incoming command.
    ///
    /// Commands are dispatched to the target node's red port via `receive_event()`.
    /// The verb becomes the event_type.
    pub async fn handle_command(
        &self,
        topic: &Topic,
        payload: &[u8],
    ) -> Result<(), MqttError> {
        let verb = topic.qualifier.as_deref().ok_or_else(|| {
            MqttError::InvalidTopic("Command topic missing verb qualifier".to_string())
        })?;

        // Parse the command message
        let command: CommandMessage = serde_json::from_slice(payload)
            .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        debug!(
            "Received command '{}' for path: {} from: {:?}",
            verb, topic.path, command.source
        );

        // Get the target node
        let node_id = NodeId::new(&topic.path);

        match self.node_registry.get(&node_id).await {
            Some(node) => {
                // Create an Event with the verb as event_type
                let event = Event {
                    source: NodeId::new(command.source.as_deref().unwrap_or("mqtt")),
                    event_type: verb.to_string(),
                    payload: command.payload,
                };

                // Dispatch to the node's red port
                if let Err(e) = node.receive_event(event).await {
                    debug!("Error dispatching command to node {}: {:?}", topic.path, e);
                }

                debug!("Dispatched command '{}' to node: {}", verb, topic.path);
                Ok(())
            }
            None => {
                warn!(
                    "No node found for command '{}' at path: {}",
                    verb, topic.path
                );
                // Don't error - the node might not exist yet
                Ok(())
            }
        }
    }

    /// Check if commands are subscribed for a path.
    pub async fn is_subscribed(&self, path: &str) -> bool {
        let commands = self.subscribed_commands.read().await;
        commands.contains_key(path)
    }
}
