pub mod document_node;
pub mod registry;
pub mod subscription;
pub mod types;

use async_trait::async_trait;

pub use document_node::DocumentNode;
pub use registry::NodeRegistry;
pub use subscription::{Subscription, SubscriptionId};
pub use types::{Edit, Event, NodeError, NodeId, NodeMessage};

/// Trait defining the interface for all nodes in the document graph.
///
/// Nodes are the fundamental building blocks that:
/// - Receive and apply edits (commits/Yjs updates)
/// - Receive and handle ephemeral events
/// - Emit edits and events to subscribers
/// - Manage subscriptions from other nodes or external clients
#[async_trait]
pub trait Node: Send + Sync {
    /// Returns the unique identifier for this node
    fn id(&self) -> &NodeId;

    /// Returns a human-readable description of this node's type
    fn node_type(&self) -> &'static str;

    // --- Receiving ---

    /// Receive an edit from another node or external source.
    /// The node should apply this edit to its internal state.
    async fn receive_edit(&self, edit: Edit) -> Result<(), NodeError>;

    /// Receive an ephemeral event from another node or external source.
    /// Events are not persisted and may be ignored by some node types.
    async fn receive_event(&self, event: Event) -> Result<(), NodeError>;

    // --- Subscribing to this node ---

    /// Subscribe to all emissions (edits and events) from this node.
    /// Returns a Subscription that can be used to receive messages.
    fn subscribe(&self) -> Subscription;

    /// Get the number of active subscribers
    fn subscriber_count(&self) -> usize;

    // --- Lifecycle ---

    /// Gracefully shut down this node
    async fn shutdown(&self) -> Result<(), NodeError>;

    /// Check if the node is healthy and operational
    fn is_healthy(&self) -> bool;
}

/// Extension trait for nodes that can be observed for specific content
#[async_trait]
pub trait ObservableNode: Node {
    /// Get the current rendered content (e.g., document text/JSON/XML)
    async fn get_content(&self) -> Result<String, NodeError>;

    /// Get content type (MIME type)
    fn content_type(&self) -> &str;
}
