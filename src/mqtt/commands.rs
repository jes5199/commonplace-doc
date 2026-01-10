//! Commands port handler.
//!
//! Handles store-level commands like create-document and path-specific commands.

use crate::document::{ContentType, DocumentStore};
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{
    CommandMessage, CreateDocumentRequest, CreateDocumentResponse, DeleteDocumentRequest,
    DeleteDocumentResponse, GetContentRequest, GetContentResponse, GetInfoRequest, GetInfoResponse,
};
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

    /// Publish a response to the {workspace}/responses topic.
    async fn publish_response<T: serde::Serialize>(&self, response: &T) -> Result<(), MqttError> {
        let payload =
            serde_json::to_vec(response).map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        let response_topic = format!("{}/responses", self.workspace);
        self.client
            .publish(&response_topic, &payload, QoS::AtLeastOnce)
            .await
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

        self.publish_response(&response).await
    }

    /// Handle delete-document command.
    /// Topic: {workspace}/commands/delete-document
    pub async fn handle_delete_document(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: DeleteDocumentRequest = serde_json::from_slice(payload)
            .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        debug!(
            "Received delete-document command: req={}, id={}",
            request.req, request.id
        );

        let deleted = self.document_store.delete_document(&request.id).await;

        let response = DeleteDocumentResponse {
            req: request.req.clone(),
            deleted,
            error: if deleted {
                None
            } else {
                Some(format!("Document '{}' not found", request.id))
            },
        };

        debug!(
            "Delete-document response: deleted={} for req={}",
            deleted, request.req
        );

        self.publish_response(&response).await
    }

    /// Handle get-content command.
    /// Topic: {workspace}/commands/get-content
    pub async fn handle_get_content(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: GetContentRequest = serde_json::from_slice(payload)
            .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        debug!(
            "Received get-content command: req={}, id={}",
            request.req, request.id
        );

        let response = match self.document_store.get_document(&request.id).await {
            Some(doc) => GetContentResponse {
                req: request.req.clone(),
                content: Some(doc.content.clone()),
                content_type: Some(doc.content_type.to_mime().to_string()),
                error: None,
            },
            None => GetContentResponse {
                req: request.req.clone(),
                content: None,
                content_type: None,
                error: Some(format!("Document '{}' not found", request.id)),
            },
        };

        debug!("Get-content response for req={}", request.req);

        self.publish_response(&response).await
    }

    /// Handle get-info command.
    /// Topic: {workspace}/commands/get-info
    pub async fn handle_get_info(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: GetInfoRequest = serde_json::from_slice(payload)
            .map_err(|e| MqttError::InvalidMessage(e.to_string()))?;

        debug!(
            "Received get-info command: req={}, id={}",
            request.req, request.id
        );

        let response = match self.document_store.get_document(&request.id).await {
            Some(doc) => GetInfoResponse {
                req: request.req.clone(),
                id: Some(request.id.clone()),
                content_type: Some(doc.content_type.to_mime().to_string()),
                error: None,
            },
            None => GetInfoResponse {
                req: request.req.clone(),
                id: None,
                content_type: None,
                error: Some(format!("Document '{}' not found", request.id)),
            },
        };

        debug!("Get-info response for req={}", request.req);

        self.publish_response(&response).await
    }

    /// Check if commands are subscribed for a path.
    pub async fn is_subscribed(&self, path: &str) -> bool {
        let commands = self.subscribed_commands.read().await;
        commands.contains_key(path)
    }
}
