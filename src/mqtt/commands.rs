//! Commands port handler.
//!
//! Handles store-level commands like create-document and path-specific commands.

use crate::document::{ContentType, DocumentStore};
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{CommandMessage, CreateDocumentRequest, CreateDocumentResponse};
use crate::mqtt::topics::Topic;
use crate::mqtt::MqttError;
use rumqttc::QoS;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Handler for the commands port.
pub struct CommandsHandler {
    client: Arc<MqttClient>,
    document_store: Arc<DocumentStore>,
    workspace: String,
    /// Map of path -> set of subscribed verbs
    subscribed_commands: RwLock<HashMap<String, HashSet<String>>>,
}

impl CommandsHandler {
    /// Create a new commands handler.
    pub fn new(
        client: Arc<MqttClient>,
        document_store: Arc<DocumentStore>,
        workspace: String,
    ) -> Self {
        Self {
            client,
            document_store,
            workspace,
            subscribed_commands: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe to commands for a path.
    /// Uses wildcard: `{workspace}/{path}/commands/#`
    pub async fn subscribe_commands(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::commands_wildcard(&self.workspace, path);

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
        let topic = Topic::commands(&self.workspace, path, verb);
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
        let topic_pattern = Topic::commands_wildcard(&self.workspace, path);

        self.client.unsubscribe(&topic_pattern).await?;

        let mut commands = self.subscribed_commands.write().await;
        commands.remove(path);

        debug!("Unsubscribed from commands for path: {}", path);
        Ok(())
    }

    /// Handle an incoming command on a path-specific topic.
    ///
    /// Note: Path-specific commands are not currently implemented.
    /// Use store-level commands like `{workspace}/commands/create-document` instead.
    pub async fn handle_command(&self, topic: &Topic, payload: &[u8]) -> Result<(), MqttError> {
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

        // Path-specific commands are not currently implemented
        warn!(
            "Path-specific command '{}' at path '{}' not implemented - use store-level commands",
            verb, topic.path
        );
        Ok(())
    }

    /// Handle create-document command.
    /// Topic: {workspace}/commands/create-document
    pub async fn handle_create_document(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: CreateDocumentRequest = serde_json::from_slice(payload)
            .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        debug!(
            "Received create-document command: req={}, content_type={}",
            request.req, request.content_type
        );

        let response = match ContentType::from_mime(&request.content_type) {
            Some(content_type) => {
                let uuid = self.document_store.create_document(content_type).await;
                debug!(
                    "Created document with uuid={} for req={}",
                    uuid, request.req
                );
                CreateDocumentResponse {
                    req: request.req,
                    uuid: Some(uuid),
                    error: None,
                }
            }
            None => {
                warn!(
                    "Invalid content type '{}' for req={}",
                    request.content_type, request.req
                );
                CreateDocumentResponse {
                    req: request.req,
                    uuid: None,
                    error: Some(format!("Invalid content type: {}", request.content_type)),
                }
            }
        };

        // Publish response to {workspace}/responses
        let payload =
            serde_json::to_vec(&response).map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        let response_topic = format!("{}/responses", self.workspace);
        self.client
            .publish(&response_topic, &payload, QoS::AtLeastOnce)
            .await?;

        Ok(())
    }

    /// Check if commands are subscribed for a path.
    pub async fn is_subscribed(&self, path: &str) -> bool {
        let commands = self.subscribed_commands.read().await;
        commands.contains_key(path)
    }
}
