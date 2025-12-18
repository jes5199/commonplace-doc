use crate::commit::Commit;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A unique identifier for a node in the graph
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An edit represents a persistent state change (wraps Commit)
#[derive(Clone, Debug)]
pub struct Edit {
    /// The underlying commit data
    pub commit: Arc<Commit>,
    /// Source node that produced this edit
    pub source: NodeId,
}

/// An event is an ephemeral message (not persisted)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    /// Event type identifier (e.g., "cursor", "presence", "metadata")
    pub event_type: String,
    /// Arbitrary JSON payload
    pub payload: serde_json::Value,
    /// Source node that produced this event
    pub source: NodeId,
}

/// Combined stream item for subscribers
#[derive(Clone, Debug)]
pub enum NodeMessage {
    Edit(Edit),
    Event(Event),
}

/// Errors that can occur during node operations
#[derive(Debug, Clone)]
pub enum NodeError {
    /// Node not found
    NotFound(NodeId),
    /// Invalid edit (e.g., failed to apply Yjs update)
    InvalidEdit(String),
    /// Subscription error
    SubscriptionFailed(String),
    /// Node is shutting down
    Shutdown,
    /// Cycle detected in node graph
    CycleDetected(Vec<NodeId>),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::NotFound(id) => write!(f, "Node not found: {}", id.0),
            NodeError::InvalidEdit(msg) => write!(f, "Invalid edit: {}", msg),
            NodeError::SubscriptionFailed(msg) => write!(f, "Subscription failed: {}", msg),
            NodeError::Shutdown => write!(f, "Node is shutting down"),
            NodeError::CycleDetected(path) => {
                write!(
                    f,
                    "Cycle detected: {:?}",
                    path.iter().map(|n| &n.0).collect::<Vec<_>>()
                )
            }
        }
    }
}

impl std::error::Error for NodeError {}
