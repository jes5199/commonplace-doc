//! MQTT transport for commonplace document store.
//!
//! This module implements the MQTT protocol as defined in docs/MQTT.md,
//! providing four ports:
//! - `edits`: Persistent Yjs deltas
//! - `sync`: Git-like merkle tree synchronization
//! - `events`: Node broadcasts
//! - `commands`: Commands to nodes

pub mod client;
pub mod commands;
pub mod edits;
pub mod events;
pub mod messages;
pub mod sync;
pub mod topics;

use crate::document::DocumentStore;
use crate::events::recv_broadcast;
use crate::store::CommitStore;
use crate::{DEFAULT_MQTT_BROKER_URL, DEFAULT_WORKSPACE};
use rumqttc::QoS;
use std::sync::Arc;
use thiserror::Error;

pub use client::MqttClient;
pub use messages::{
    CommandMessage, CreateDocumentRequest, CreateDocumentResponse, DeleteDocumentRequest,
    DeleteDocumentResponse, EditMessage, EventMessage, GetContentRequest, GetContentResponse,
    GetInfoRequest, GetInfoResponse, SyncMessage,
};
pub use topics::{Port, Topic};

/// Errors that can occur in MQTT operations.
#[derive(Debug, Error)]
pub enum MqttError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Publish error: {0}")]
    Publish(String),

    #[error("Subscribe error: {0}")]
    Subscribe(String),

    #[error("Invalid topic: {0}")]
    InvalidTopic(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Store error: {0}")]
    Store(#[from] crate::store::StoreError),

    #[error("Commit not found: {0}")]
    CommitNotFound(String),

    #[error("Node error: {0}")]
    Node(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Parse JSON from a byte slice, mapping errors to MqttError::InvalidMessage.
///
/// This is a convenience wrapper around serde_json::from_slice that provides
/// consistent error handling across all MQTT handlers.
pub fn parse_json<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, MqttError> {
    serde_json::from_slice(payload).map_err(|e| MqttError::InvalidMessage(e.to_string()))
}

/// Encode a value to JSON bytes, mapping errors to MqttError::InvalidMessage.
///
/// This is a convenience wrapper around serde_json::to_vec that provides
/// consistent error handling across all MQTT handlers.
pub fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, MqttError> {
    serde_json::to_vec(value).map_err(|e| MqttError::InvalidMessage(e.to_string()))
}

/// Configuration for MQTT connection.
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// MQTT broker URL (e.g., "mqtt://localhost:1883")
    pub broker_url: String,
    /// Client ID for this doc store instance
    pub client_id: String,
    /// Workspace name for topic namespacing
    pub workspace: String,
    /// Keep-alive interval in seconds
    pub keep_alive_secs: u64,
    /// Whether to use clean session
    pub clean_session: bool,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker_url: DEFAULT_MQTT_BROKER_URL.to_string(),
            client_id: uuid::Uuid::new_v4().to_string(),
            workspace: DEFAULT_WORKSPACE.to_string(),
            keep_alive_secs: 60,
            clean_session: true,
        }
    }
}

/// Main MQTT service that coordinates all port handlers.
#[allow(dead_code)] // Fields used for future integration
pub struct MqttService {
    client: Arc<MqttClient>,
    workspace: String,
    document_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    edits_handler: edits::EditsHandler,
    sync_handler: sync::SyncHandler,
    events_handler: events::EventsHandler,
    commands_handler: commands::CommandsHandler,
}

impl MqttService {
    /// Create a new MQTT service with the given configuration.
    pub async fn new(
        config: MqttConfig,
        document_store: Arc<DocumentStore>,
        commit_store: Option<Arc<CommitStore>>,
    ) -> Result<Self, MqttError> {
        // Validate workspace name
        topics::validate_workspace_name(&config.workspace)?;

        let workspace = config.workspace.clone();
        let client = Arc::new(MqttClient::connect(config).await?);

        let edits_handler = edits::EditsHandler::new(
            client.clone(),
            document_store.clone(),
            commit_store.clone(),
            workspace.clone(),
        );

        let sync_handler =
            sync::SyncHandler::new(client.clone(), commit_store.clone(), workspace.clone());

        let events_handler = events::EventsHandler::new(client.clone(), workspace.clone());

        let commands_handler = commands::CommandsHandler::new(
            client.clone(),
            document_store.clone(),
            workspace.clone(),
        );

        Ok(Self {
            client,
            workspace,
            document_store,
            commit_store,
            edits_handler,
            sync_handler,
            events_handler,
            commands_handler,
        })
    }

    /// Get the store commands topic for create-document.
    /// Uses `__system` prefix to avoid conflicts with document paths.
    fn create_document_topic(&self) -> String {
        format!("{}/commands/__system/create-document", self.workspace)
    }

    /// Get the store commands topic for delete-document.
    /// Uses `__system` prefix to avoid conflicts with document paths.
    fn delete_document_topic(&self) -> String {
        format!("{}/commands/__system/delete-document", self.workspace)
    }

    /// Get the store commands topic for get-content.
    /// Uses `__system` prefix to avoid conflicts with document paths.
    fn get_content_topic(&self) -> String {
        format!("{}/commands/__system/get-content", self.workspace)
    }

    /// Get the store commands topic for get-info.
    /// Uses `__system` prefix to avoid conflicts with document paths.
    fn get_info_topic(&self) -> String {
        format!("{}/commands/__system/get-info", self.workspace)
    }

    /// Subscribe to store-level commands.
    /// This subscribes to document management command topics.
    pub async fn subscribe_store_commands(&self) -> Result<(), MqttError> {
        let create_topic = self.create_document_topic();
        self.client
            .subscribe(&create_topic, QoS::AtLeastOnce)
            .await?;
        tracing::debug!("Subscribed to create-document commands: {}", create_topic);

        let delete_topic = self.delete_document_topic();
        self.client
            .subscribe(&delete_topic, QoS::AtLeastOnce)
            .await?;
        tracing::debug!("Subscribed to delete-document commands: {}", delete_topic);

        let get_content_topic = self.get_content_topic();
        self.client
            .subscribe(&get_content_topic, QoS::AtLeastOnce)
            .await?;
        tracing::debug!("Subscribed to get-content commands: {}", get_content_topic);

        let get_info_topic = self.get_info_topic();
        self.client
            .subscribe(&get_info_topic, QoS::AtLeastOnce)
            .await?;
        tracing::debug!("Subscribed to get-info commands: {}", get_info_topic);

        Ok(())
    }

    /// Subscribe to edits for a path.
    pub async fn subscribe_path(&self, path: &str) -> Result<(), MqttError> {
        self.edits_handler.subscribe_path(path).await?;
        self.sync_handler.subscribe_path(path).await?;
        Ok(())
    }

    /// Unsubscribe from a path.
    pub async fn unsubscribe_path(&self, path: &str) -> Result<(), MqttError> {
        self.edits_handler.unsubscribe_path(path).await?;
        self.sync_handler.unsubscribe_path(path).await?;
        Ok(())
    }

    /// Run the MQTT service event loop.
    /// This processes incoming messages and dispatches them to handlers.
    pub async fn run(&self) -> Result<(), MqttError> {
        // Subscribe to the message broadcast channel
        let mut message_rx = self.client.subscribe_messages();

        // Spawn the client event loop in a separate task
        let client = self.client.clone();
        tokio::spawn(async move {
            if let Err(e) = client.run_event_loop().await {
                tracing::error!("MQTT client event loop error: {}", e);
            }
        });

        // Process incoming messages and dispatch to handlers
        while let Some(msg) = recv_broadcast(&mut message_rx, "MQTT message receiver").await {
            if let Err(e) = self.dispatch_message(&msg.topic, &msg.payload).await {
                tracing::warn!("Error dispatching MQTT message: {}", e);
            }
        }

        tracing::info!("MQTT message channel closed");

        Ok(())
    }

    /// Dispatch an incoming message to the appropriate handler.
    async fn dispatch_message(&self, topic_str: &str, payload: &[u8]) -> Result<(), MqttError> {
        // Check for store-level commands first (these don't follow the document path pattern)
        if topic_str == self.create_document_topic() {
            return self.commands_handler.handle_create_document(payload).await;
        }
        if topic_str == self.delete_document_topic() {
            return self.commands_handler.handle_delete_document(payload).await;
        }
        if topic_str == self.get_content_topic() {
            return self.commands_handler.handle_get_content(payload).await;
        }
        if topic_str == self.get_info_topic() {
            return self.commands_handler.handle_get_info(payload).await;
        }

        // Parse the topic
        let topic = match topics::Topic::parse(topic_str, &self.workspace) {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!("Ignoring unparseable topic {}: {}", topic_str, e);
                return Ok(());
            }
        };

        tracing::debug!("Dispatching message for topic: {:?}", topic);

        match topic.port {
            topics::Port::Edits => {
                self.edits_handler.handle_edit(&topic, payload).await?;
            }
            topics::Port::Sync => {
                // Parse sync message
                let sync_msg: messages::SyncMessage = serde_json::from_slice(payload)?;
                // Only handle requests, not responses
                if sync_msg.is_request() {
                    self.sync_handler
                        .handle_sync_request(&topic, sync_msg)
                        .await?;
                }
            }
            topics::Port::Commands => {
                // Path-specific commands are not implemented - store-level commands
                // are handled above via exact topic matching
                tracing::debug!(
                    "Ignoring path-specific command on {} - use store-level commands",
                    topic_str
                );
            }
            topics::Port::Events => {
                // Events are outbound only from this doc store - we don't receive them
                tracing::debug!("Ignoring incoming event message on {}", topic_str);
            }
        }

        Ok(())
    }

    /// Get a reference to the MQTT client.
    pub fn client(&self) -> &Arc<MqttClient> {
        &self.client
    }

    /// Get a reference to the edits handler.
    pub fn edits_handler(&self) -> &edits::EditsHandler {
        &self.edits_handler
    }

    /// Get a reference to the sync handler.
    pub fn sync_handler(&self) -> &sync::SyncHandler {
        &self.sync_handler
    }

    /// Get a reference to the events handler.
    pub fn events_handler(&self) -> &events::EventsHandler {
        &self.events_handler
    }

    /// Get a reference to the commands handler.
    pub fn commands_handler(&self) -> &commands::CommandsHandler {
        &self.commands_handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that MqttConfig::default() uses "commonplace" as the workspace
    #[test]
    fn test_config_default_workspace() {
        let config = MqttConfig::default();
        assert_eq!(config.workspace, "commonplace");
    }

    /// Test default values for MqttConfig
    #[test]
    fn test_config_default_values() {
        let config = MqttConfig::default();
        assert_eq!(config.broker_url, "mqtt://localhost:1883");
        assert_eq!(config.keep_alive_secs, 60);
        assert!(config.clean_session);
        // client_id should be a valid UUID
        assert!(!config.client_id.is_empty());
    }
}
