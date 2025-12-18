use super::types::{NodeId, NodeMessage};
use tokio::sync::broadcast;

/// A subscription handle that can be used to receive messages from a node
pub struct Subscription {
    /// The node this subscription is from
    pub source: NodeId,
    /// Receiver for node messages
    pub receiver: broadcast::Receiver<NodeMessage>,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(source: NodeId, receiver: broadcast::Receiver<NodeMessage>) -> Self {
        Self { source, receiver }
    }

    /// Receive the next message, waiting if necessary
    pub async fn recv(&mut self) -> Result<NodeMessage, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}

/// A unique identifier for a subscription (used for unsubscribe/unwire)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub uuid::Uuid);

impl SubscriptionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
