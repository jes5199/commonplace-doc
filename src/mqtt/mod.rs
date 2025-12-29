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

use crate::node::NodeRegistry;
use crate::store::CommitStore;
use std::sync::Arc;
use thiserror::Error;

pub use client::MqttClient;
pub use messages::{CommandMessage, EditMessage, EventMessage, SyncMessage};
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

/// Configuration for MQTT connection.
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// MQTT broker URL (e.g., "mqtt://localhost:1883")
    pub broker_url: String,
    /// Client ID for this doc store instance
    pub client_id: String,
    /// Keep-alive interval in seconds
    pub keep_alive_secs: u64,
    /// Whether to use clean session
    pub clean_session: bool,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker_url: "mqtt://localhost:1883".to_string(),
            client_id: uuid::Uuid::new_v4().to_string(),
            keep_alive_secs: 60,
            clean_session: true,
        }
    }
}

/// Main MQTT service that coordinates all port handlers.
#[allow(dead_code)] // Fields used for future integration
pub struct MqttService {
    client: Arc<MqttClient>,
    node_registry: Arc<NodeRegistry>,
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
        node_registry: Arc<NodeRegistry>,
        commit_store: Option<Arc<CommitStore>>,
    ) -> Result<Self, MqttError> {
        let client = Arc::new(MqttClient::connect(config).await?);

        let edits_handler =
            edits::EditsHandler::new(client.clone(), node_registry.clone(), commit_store.clone());

        let sync_handler = sync::SyncHandler::new(client.clone(), commit_store.clone());

        let events_handler = events::EventsHandler::new(client.clone());

        let commands_handler =
            commands::CommandsHandler::new(client.clone(), node_registry.clone());

        Ok(Self {
            client,
            node_registry,
            commit_store,
            edits_handler,
            sync_handler,
            events_handler,
            commands_handler,
        })
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
        loop {
            match message_rx.recv().await {
                Ok(msg) => {
                    if let Err(e) = self.dispatch_message(&msg.topic, &msg.payload).await {
                        tracing::warn!("Error dispatching MQTT message: {}", e);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("MQTT message receiver lagged by {} messages", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::info!("MQTT message channel closed");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Dispatch an incoming message to the appropriate handler.
    async fn dispatch_message(&self, topic_str: &str, payload: &[u8]) -> Result<(), MqttError> {
        // Parse the topic
        let topic = match topics::Topic::parse(topic_str) {
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
                self.commands_handler
                    .handle_command(&topic, payload)
                    .await?;
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
