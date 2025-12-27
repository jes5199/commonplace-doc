use super::subscription::SubscriptionId;
use super::types::{NodeError, NodeId, NodeMessage, Port};
use super::{DocumentNode, Node};
use crate::document::ContentType;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Tracks a wiring between two nodes
#[allow(dead_code)]
struct NodeWiring {
    from: NodeId,
    to: NodeId,
    /// Which port this wiring is for
    port: Port,
    /// Handle to the task forwarding messages
    task_handle: tokio::task::JoinHandle<()>,
}

/// Registry managing all nodes and their interconnections
pub struct NodeRegistry {
    /// All registered nodes
    nodes: RwLock<HashMap<NodeId, Arc<dyn Node>>>,
    /// Adjacency list for the node graph (from -> set of to)
    edges: RwLock<HashMap<NodeId, HashSet<NodeId>>>,
    /// Active wirings between nodes
    wirings: RwLock<HashMap<SubscriptionId, NodeWiring>>,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            edges: RwLock::new(HashMap::new()),
            wirings: RwLock::new(HashMap::new()),
        }
    }

    /// Register a node with the registry
    pub async fn register(&self, node: Arc<dyn Node>) -> Result<(), NodeError> {
        let id = node.id().clone();
        let mut nodes = self.nodes.write().await;
        nodes.insert(id, node);
        Ok(())
    }

    /// Unregister a node (also removes all wirings)
    pub async fn unregister(&self, id: &NodeId) -> Result<(), NodeError> {
        // Remove all wirings involving this node
        let wirings_to_remove: Vec<SubscriptionId> = {
            let wirings = self.wirings.read().await;
            wirings
                .iter()
                .filter(|(_, w)| &w.from == id || &w.to == id)
                .map(|(sid, _)| sid.clone())
                .collect()
        };

        for sid in wirings_to_remove {
            self.unwire(&sid).await?;
        }

        // Remove from nodes
        let mut nodes = self.nodes.write().await;
        nodes.remove(id).ok_or(NodeError::NotFound(id.clone()))?;

        // Remove from edges
        let mut edges = self.edges.write().await;
        edges.remove(id);
        for targets in edges.values_mut() {
            targets.remove(id);
        }

        Ok(())
    }

    /// Get a node by ID
    pub async fn get(&self, id: &NodeId) -> Option<Arc<dyn Node>> {
        let nodes = self.nodes.read().await;
        nodes.get(id).cloned()
    }

    /// Get or create a document node with the given ID and content type.
    /// If a node with this ID already exists, returns it (ignoring content_type).
    /// Otherwise creates a new DocumentNode and registers it.
    pub async fn get_or_create_document(
        &self,
        id: &NodeId,
        content_type: ContentType,
    ) -> Result<Arc<dyn Node>, NodeError> {
        // First try to get existing
        {
            let nodes = self.nodes.read().await;
            if let Some(node) = nodes.get(id) {
                return Ok(node.clone());
            }
        }

        // Create new node
        let node: Arc<dyn Node> = Arc::new(DocumentNode::new(id.0.clone(), content_type));
        self.register(node.clone()).await?;
        Ok(node)
    }

    /// Wire two nodes together on both ports: edits/events from `from` will be sent to `to`
    pub async fn wire(&self, from: &NodeId, to: &NodeId) -> Result<SubscriptionId, NodeError> {
        self.wire_port(from, to, Port::Both).await
    }

    /// Wire only the blue port (edits) from one node to another
    pub async fn wire_blue(&self, from: &NodeId, to: &NodeId) -> Result<SubscriptionId, NodeError> {
        self.wire_port(from, to, Port::Blue).await
    }

    /// Wire only the red port (events) from one node to another
    pub async fn wire_red(&self, from: &NodeId, to: &NodeId) -> Result<SubscriptionId, NodeError> {
        self.wire_port(from, to, Port::Red).await
    }

    /// Wire a specific port from one node to another
    async fn wire_port(
        &self,
        from: &NodeId,
        to: &NodeId,
        port: Port,
    ) -> Result<SubscriptionId, NodeError> {
        // Check for cycles before wiring
        if self.would_create_cycle(from, to).await {
            return Err(NodeError::CycleDetected(vec![from.clone(), to.clone()]));
        }

        let (from_node, to_node) = {
            let nodes = self.nodes.read().await;
            let from_node = nodes
                .get(from)
                .ok_or(NodeError::NotFound(from.clone()))?
                .clone();
            let to_node = nodes
                .get(to)
                .ok_or(NodeError::NotFound(to.clone()))?
                .clone();
            (from_node, to_node)
        };

        let subscription_id = SubscriptionId::new();

        // Spawn appropriate forwarding task based on port
        let task_handle = match port {
            Port::Blue => {
                let mut subscription = from_node.subscribe_blue();
                let to_node_clone = to_node.clone();
                tokio::spawn(async move {
                    loop {
                        match subscription.recv().await {
                            Ok(edit) => {
                                if let Err(e) = to_node_clone.receive_edit(edit).await {
                                    tracing::warn!("Error forwarding edit: {}", e);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("Blue subscription lagged by {} messages", n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                tracing::info!("Source node closed, stopping blue wiring");
                                break;
                            }
                        }
                    }
                })
            }
            Port::Red => {
                let mut subscription = from_node.subscribe_red();
                let to_node_clone = to_node.clone();
                tokio::spawn(async move {
                    loop {
                        match subscription.recv().await {
                            Ok(event) => {
                                if let Err(e) = to_node_clone.receive_event(event).await {
                                    tracing::warn!("Error forwarding event: {}", e);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("Red subscription lagged by {} messages", n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                tracing::info!("Source node closed, stopping red wiring");
                                break;
                            }
                        }
                    }
                })
            }
            Port::Both => {
                let mut subscription = from_node.subscribe();
                let to_node_clone = to_node.clone();
                tokio::spawn(async move {
                    loop {
                        match subscription.recv().await {
                            Ok(msg) => {
                                let result = match msg {
                                    NodeMessage::Edit(edit) => {
                                        to_node_clone.receive_edit(edit).await
                                    }
                                    NodeMessage::Event(event) => {
                                        to_node_clone.receive_event(event).await
                                    }
                                };
                                if let Err(e) = result {
                                    tracing::warn!("Error forwarding message: {}", e);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("Subscription lagged by {} messages", n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                tracing::info!("Source node closed, stopping wiring");
                                break;
                            }
                        }
                    }
                })
            }
        };

        // Record the wiring
        let wiring = NodeWiring {
            from: from.clone(),
            to: to.clone(),
            port,
            task_handle,
        };

        {
            let mut wirings = self.wirings.write().await;
            wirings.insert(subscription_id.clone(), wiring);
        }

        {
            let mut edges = self.edges.write().await;
            edges.entry(from.clone()).or_default().insert(to.clone());
        }

        Ok(subscription_id)
    }

    /// Remove a wiring between nodes
    pub async fn unwire(&self, subscription_id: &SubscriptionId) -> Result<(), NodeError> {
        let wiring = {
            let mut wirings = self.wirings.write().await;
            wirings.remove(subscription_id)
        };

        if let Some(wiring) = wiring {
            wiring.task_handle.abort();

            let mut edges = self.edges.write().await;
            if let Some(targets) = edges.get_mut(&wiring.from) {
                targets.remove(&wiring.to);
            }

            Ok(())
        } else {
            Err(NodeError::SubscriptionFailed(
                "Wiring not found".to_string(),
            ))
        }
    }

    /// Check if adding an edge from->to would create a cycle
    async fn would_create_cycle(&self, from: &NodeId, to: &NodeId) -> bool {
        // If to can reach from, then adding from->to creates a cycle
        let edges = self.edges.read().await;
        let mut visited = HashSet::new();
        let mut stack = vec![to.clone()];

        while let Some(current) = stack.pop() {
            if &current == from {
                return true;
            }
            if visited.insert(current.clone()) {
                if let Some(neighbors) = edges.get(&current) {
                    stack.extend(neighbors.iter().cloned());
                }
            }
        }

        false
    }

    /// List all node IDs
    pub async fn list_nodes(&self) -> Vec<NodeId> {
        let nodes = self.nodes.read().await;
        nodes.keys().cloned().collect()
    }

    /// Get all wirings from a node
    pub async fn get_outgoing_wirings(&self, from: &NodeId) -> Vec<(SubscriptionId, NodeId)> {
        let wirings = self.wirings.read().await;
        wirings
            .iter()
            .filter(|(_, w)| &w.from == from)
            .map(|(sid, w)| (sid.clone(), w.to.clone()))
            .collect()
    }

    /// Check if a wiring with the given (from, to, port) already exists.
    /// Returns the subscription ID if found.
    pub async fn find_existing_wiring(
        &self,
        from: &NodeId,
        to: &NodeId,
        port: Port,
    ) -> Option<SubscriptionId> {
        let wirings = self.wirings.read().await;
        for (sub_id, wiring) in wirings.iter() {
            if &wiring.from == from && &wiring.to == to && wiring.port == port {
                return Some(sub_id.clone());
            }
        }
        None
    }

    /// Get count of registered nodes
    pub async fn node_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registry_register_and_get() {
        let registry = NodeRegistry::new();
        let node = Arc::new(DocumentNode::new("test-doc", ContentType::Text));

        registry.register(node.clone()).await.unwrap();

        let retrieved = registry.get(&NodeId::new("test-doc")).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id().0, "test-doc");
    }

    #[tokio::test]
    async fn test_registry_unregister() {
        let registry = NodeRegistry::new();
        let node = Arc::new(DocumentNode::new("test-doc", ContentType::Text));

        registry.register(node).await.unwrap();
        assert_eq!(registry.node_count().await, 1);

        registry.unregister(&NodeId::new("test-doc")).await.unwrap();
        assert_eq!(registry.node_count().await, 0);
    }

    #[tokio::test]
    async fn test_registry_wire_nodes() {
        let registry = NodeRegistry::new();
        let node1 = Arc::new(DocumentNode::new("doc1", ContentType::Text));
        let node2 = Arc::new(DocumentNode::new("doc2", ContentType::Text));

        registry.register(node1).await.unwrap();
        registry.register(node2).await.unwrap();

        let sub_id = registry
            .wire(&NodeId::new("doc1"), &NodeId::new("doc2"))
            .await
            .unwrap();

        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc1")).await;
        assert_eq!(wirings.len(), 1);
        assert_eq!(wirings[0].0, sub_id);
        assert_eq!(wirings[0].1 .0, "doc2");
    }

    #[tokio::test]
    async fn test_registry_cycle_detection() {
        let registry = NodeRegistry::new();
        let node1 = Arc::new(DocumentNode::new("doc1", ContentType::Text));
        let node2 = Arc::new(DocumentNode::new("doc2", ContentType::Text));
        let node3 = Arc::new(DocumentNode::new("doc3", ContentType::Text));

        registry.register(node1).await.unwrap();
        registry.register(node2).await.unwrap();
        registry.register(node3).await.unwrap();

        // Create chain: doc1 -> doc2 -> doc3
        registry
            .wire(&NodeId::new("doc1"), &NodeId::new("doc2"))
            .await
            .unwrap();
        registry
            .wire(&NodeId::new("doc2"), &NodeId::new("doc3"))
            .await
            .unwrap();

        // Attempt to create cycle: doc3 -> doc1 should fail
        let result = registry
            .wire(&NodeId::new("doc3"), &NodeId::new("doc1"))
            .await;
        assert!(matches!(result, Err(NodeError::CycleDetected(_))));
    }

    #[tokio::test]
    async fn test_registry_unwire() {
        let registry = NodeRegistry::new();
        let node1 = Arc::new(DocumentNode::new("doc1", ContentType::Text));
        let node2 = Arc::new(DocumentNode::new("doc2", ContentType::Text));

        registry.register(node1).await.unwrap();
        registry.register(node2).await.unwrap();

        let sub_id = registry
            .wire(&NodeId::new("doc1"), &NodeId::new("doc2"))
            .await
            .unwrap();

        assert_eq!(
            registry
                .get_outgoing_wirings(&NodeId::new("doc1"))
                .await
                .len(),
            1
        );

        registry.unwire(&sub_id).await.unwrap();

        assert_eq!(
            registry
                .get_outgoing_wirings(&NodeId::new("doc1"))
                .await
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn test_registry_get_or_create_document() {
        let registry = NodeRegistry::new();

        // First call should create
        let node1 = registry
            .get_or_create_document(&NodeId::new("lazy-doc"), ContentType::Json)
            .await
            .unwrap();
        assert_eq!(node1.id().0, "lazy-doc");
        assert_eq!(registry.node_count().await, 1);

        // Second call should return existing
        let node2 = registry
            .get_or_create_document(&NodeId::new("lazy-doc"), ContentType::Text)
            .await
            .unwrap();
        assert_eq!(node2.id().0, "lazy-doc");
        assert_eq!(registry.node_count().await, 1); // Still just 1 node
    }
}
