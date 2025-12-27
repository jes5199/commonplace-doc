//! Router manager: watches a router document and manages wiring.

use super::error::RouterError;
use super::schema::{PortType, RouterSchema};
use crate::document::ContentType;
use crate::node::subscription::SubscriptionId;
use crate::node::types::Port;
use crate::node::{DocumentNode, Event, NodeId, NodeRegistry, ObservableNode};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// A tracked wire created by this router.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct WireKey {
    from: String,
    to: String,
    port: PortType,
}

/// Manages wiring based on a router document.
pub struct RouterManager {
    /// The router document node ID
    router_id: NodeId,
    /// Reference to node registry
    registry: Arc<NodeRegistry>,
    /// Last successfully parsed schema (kept on parse errors)
    last_valid_schema: RwLock<Option<RouterSchema>>,
    /// Wires created by this router: WireKey -> SubscriptionId
    managed_wires: RwLock<HashMap<WireKey, SubscriptionId>>,
}

impl RouterManager {
    /// Create a new RouterManager.
    pub fn new(router_id: NodeId, registry: Arc<NodeRegistry>) -> Self {
        Self {
            router_id,
            registry,
            last_valid_schema: RwLock::new(None),
            managed_wires: RwLock::new(HashMap::new()),
        }
    }

    /// Start watching the router document for changes.
    /// Returns a JoinHandle for the background task.
    pub async fn start(self: Arc<Self>) -> JoinHandle<()> {
        let manager = self.clone();

        tokio::spawn(async move {
            // Get the router node
            let node = match manager.registry.get(&manager.router_id).await {
                Some(n) => n,
                None => {
                    tracing::error!("Router node not found: {}", manager.router_id.0);
                    return;
                }
            };

            // Subscribe to blue port for edits
            let mut blue_sub = node.subscribe_blue();

            tracing::info!("Router manager started for: {}", manager.router_id.0);

            // Perform initial wiring before entering the loop (avoids race condition)
            manager.apply_wiring().await;

            loop {
                match blue_sub.recv().await {
                    Ok(_edit) => {
                        tracing::debug!("Router received edit, applying wiring");
                        manager.apply_wiring().await;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Router manager lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("Router node closed, stopping manager");
                        break;
                    }
                }
            }
        })
    }

    /// Apply wiring based on current router document content.
    pub async fn apply_wiring(self: &Arc<Self>) {
        let node = match self.registry.get(&self.router_id).await {
            Some(n) => n,
            None => return,
        };

        let doc_node = match node.as_any().downcast_ref::<DocumentNode>() {
            Some(d) => d,
            None => return,
        };

        let content = match doc_node.get_content().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("Failed to get router content: {}", e);
                return;
            }
        };

        // Skip empty content
        if content.is_empty() || content == "{}" {
            return;
        }

        if let Err(e) = self.reconcile_wiring(&content).await {
            self.emit_error(e).await;
        }
    }

    /// Reconcile wiring based on router document content.
    async fn reconcile_wiring(&self, content: &str) -> Result<(), RouterError> {
        // 1. Parse JSON
        let schema: RouterSchema =
            serde_json::from_str(content).map_err(|e| RouterError::ParseError(e.to_string()))?;

        // 2. Validate schema
        if schema.version != 1 {
            return Err(RouterError::UnsupportedVersion(schema.version));
        }

        schema.validate().map_err(RouterError::SchemaError)?;

        // 3. Create nodes from hints (if they don't exist)
        for (node_id, spec) in &schema.nodes {
            if spec.node_type != "document" {
                return Err(RouterError::SchemaError(format!(
                    "Unsupported node type '{}' for node '{}'",
                    spec.node_type, node_id
                )));
            }

            let content_type = match &spec.content_type {
                Some(mime) => ContentType::from_mime(mime).ok_or_else(|| {
                    RouterError::SchemaError(format!(
                        "Unsupported content_type '{}' for node '{}'",
                        mime, node_id
                    ))
                })?,
                None => ContentType::Json,
            };

            let nid = NodeId::new(node_id);
            if self
                .registry
                .get_or_create_document(&nid, content_type)
                .await
                .is_err()
            {
                tracing::warn!("Failed to create node {} from router hints", node_id);
            }
        }

        // 4. Build desired wires list (maintaining schema order for deterministic cycle detection)
        let desired_wires: Vec<WireKey> = schema
            .edges
            .iter()
            .map(|edge| WireKey {
                from: edge.from.clone(),
                to: edge.to.clone(),
                port: edge.port,
            })
            .collect();

        // Convert to set for comparison
        let desired_set: HashSet<WireKey> = desired_wires.iter().cloned().collect();

        // 5. Prune stale entries from managed_wires (handles external unwiring)
        {
            let mut managed = self.managed_wires.write().await;
            let stale_keys: Vec<WireKey> = {
                let mut stale = Vec::new();
                for (wire_key, sub_id) in managed.iter() {
                    // Check if this wire still exists in the registry
                    let from_id = NodeId::new(&wire_key.from);
                    let actual_wires = self.registry.get_outgoing_wirings(&from_id).await;
                    if !actual_wires.iter().any(|(sid, _)| sid == sub_id) {
                        stale.push(wire_key.clone());
                    }
                }
                stale
            };
            for key in stale_keys {
                tracing::debug!(
                    "Pruning stale managed wire: {} -> {} (externally removed)",
                    key.from,
                    key.to
                );
                managed.remove(&key);
            }
        }

        // 6. Diff against current managed wires
        let current_wires: HashSet<WireKey> = {
            let managed = self.managed_wires.read().await;
            managed.keys().cloned().collect()
        };

        // Find wires to add - use desired_wires vec to preserve order, but dedupe to avoid
        // creating multiple wires for duplicate edges in the schema
        let mut seen_wires = HashSet::new();
        let to_add: Vec<WireKey> = desired_wires
            .into_iter()
            .filter(|w| !current_wires.contains(w) && seen_wires.insert(w.clone()))
            .collect();
        let to_remove: Vec<WireKey> = current_wires.difference(&desired_set).cloned().collect();

        // 6. Remove wires that are no longer declared
        for wire_key in to_remove {
            let sub_id = {
                let mut managed = self.managed_wires.write().await;
                managed.remove(&wire_key)
            };

            if let Some(sid) = sub_id {
                if let Err(e) = self.registry.unwire(&sid).await {
                    tracing::warn!(
                        "Failed to remove wire {} -> {}: {}",
                        wire_key.from,
                        wire_key.to,
                        e
                    );
                } else {
                    tracing::debug!("Removed wire: {} -> {}", wire_key.from, wire_key.to);
                }
            }
        }

        // 7. Add new wires
        for wire_key in to_add {
            let from_id = NodeId::new(&wire_key.from);
            let to_id = NodeId::new(&wire_key.to);

            // Check that nodes exist
            if self.registry.get(&from_id).await.is_none() {
                self.emit_error(RouterError::MissingNode(wire_key.from.clone()))
                    .await;
                continue;
            }
            if self.registry.get(&to_id).await.is_none() {
                self.emit_error(RouterError::MissingNode(wire_key.to.clone()))
                    .await;
                continue;
            }

            // Convert PortType to Port for registry lookup
            let port = match wire_key.port {
                PortType::Blue => Port::Blue,
                PortType::Red => Port::Red,
                PortType::Both => Port::Both,
            };

            // Check if an identical wire already exists (e.g., externally created or
            // owned by another router). Skip to avoid duplicates but don't claim ownership
            // - the other owner may remove it later, and we'll recreate on next apply_wiring.
            if self
                .registry
                .find_existing_wiring(&from_id, &to_id, port)
                .await
                .is_some()
            {
                tracing::debug!(
                    "Wire already exists (external): {} -> {} (port: {:?})",
                    wire_key.from,
                    wire_key.to,
                    wire_key.port
                );
                continue;
            }

            // Create the wire
            let result = match wire_key.port {
                PortType::Blue => self.registry.wire_blue(&from_id, &to_id).await,
                PortType::Red => self.registry.wire_red(&from_id, &to_id).await,
                PortType::Both => self.registry.wire(&from_id, &to_id).await,
            };

            match result {
                Ok(sub_id) => {
                    let mut managed = self.managed_wires.write().await;
                    managed.insert(wire_key.clone(), sub_id);
                    tracing::info!(
                        "Created wire: {} -> {} (port: {:?})",
                        wire_key.from,
                        wire_key.to,
                        wire_key.port
                    );
                }
                Err(crate::node::NodeError::CycleDetected(_)) => {
                    self.emit_error(RouterError::CycleDetected(
                        wire_key.from.clone(),
                        wire_key.to.clone(),
                    ))
                    .await;
                }
                Err(e) => {
                    self.emit_error(RouterError::NodeError(e.to_string())).await;
                }
            }
        }

        // 8. Update last valid schema
        *self.last_valid_schema.write().await = Some(schema);

        Ok(())
    }

    /// Emit an error event on the router document's red port.
    pub async fn emit_error(&self, error: RouterError) {
        tracing::warn!("Router error: {}", error);

        let event = Event {
            event_type: "router.error".to_string(),
            payload: error.to_event_payload(),
            source: self.router_id.clone(),
        };

        if let Some(node) = self.registry.get(&self.router_id).await {
            if let Err(e) = node.receive_event(event).await {
                tracing::warn!("Failed to emit router.error event: {}", e);
            }
        }
    }

    /// Get the router document node ID.
    pub fn router_id(&self) -> &NodeId {
        &self.router_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: Create a Y.Text update containing JSON content (for Text-typed nodes)
    fn make_text_update(content: &str) -> Vec<u8> {
        use yrs::{Doc, Text, Transact};
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("content");
        let update = {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, content);
            txn.encode_update_v1()
        };
        update
    }

    #[tokio::test]
    async fn test_router_creates_wires() {
        let registry = Arc::new(NodeRegistry::new());

        // Create two document nodes
        let doc_a = Arc::new(DocumentNode::new("doc-a", ContentType::Text));
        let doc_b = Arc::new(DocumentNode::new("doc-b", ContentType::Text));
        registry.register(doc_a).await.unwrap();
        registry.register(doc_b).await.unwrap();

        // Create router node as Text type (stores JSON as text)
        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // Set router content with edge
        let content =
            r#"{"version": 1, "edges": [{"from": "doc-a", "to": "doc-b", "port": "blue"}]}"#;
        router.apply_state(&make_text_update(content)).unwrap();

        // Create and run router manager
        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));

        manager.apply_wiring().await;

        // Verify wire was created
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(wirings.len(), 1);
        assert_eq!(wirings[0].1, NodeId::new("doc-b"));
    }

    #[tokio::test]
    async fn test_router_removes_wires() {
        let registry = Arc::new(NodeRegistry::new());

        // Create two document nodes
        let doc_a = Arc::new(DocumentNode::new("doc-a", ContentType::Text));
        let doc_b = Arc::new(DocumentNode::new("doc-b", ContentType::Text));
        registry.register(doc_a).await.unwrap();
        registry.register(doc_b).await.unwrap();

        // Create router node as Text type
        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // Initial content with edge
        let content1 = r#"{"version": 1, "edges": [{"from": "doc-a", "to": "doc-b"}]}"#;
        router.apply_state(&make_text_update(content1)).unwrap();

        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));
        manager.apply_wiring().await;

        // Verify wire exists
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(wirings.len(), 1);

        // Update content to remove edge - need to replace text entirely
        // Since apply_state appends, we set content directly for testing
        router.set_content(r#"{"version": 1, "edges": []}"#);

        manager.apply_wiring().await;

        // Verify wire was removed
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(wirings.len(), 0);
    }

    #[tokio::test]
    async fn test_router_creates_nodes_from_hints() {
        let registry = Arc::new(NodeRegistry::new());

        // Create router node as Text type
        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // Content with node hints
        let content = r#"{
            "version": 1,
            "nodes": {
                "new-doc": { "type": "document", "content_type": "text/plain" }
            },
            "edges": []
        }"#;
        router.apply_state(&make_text_update(content)).unwrap();

        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));
        manager.apply_wiring().await;

        // Verify node was created
        assert!(registry.get(&NodeId::new("new-doc")).await.is_some());
    }

    #[tokio::test]
    async fn test_router_rejects_cycles() {
        let registry = Arc::new(NodeRegistry::new());

        // Create three document nodes
        let doc_a = Arc::new(DocumentNode::new("doc-a", ContentType::Text));
        let doc_b = Arc::new(DocumentNode::new("doc-b", ContentType::Text));
        let doc_c = Arc::new(DocumentNode::new("doc-c", ContentType::Text));
        registry.register(doc_a).await.unwrap();
        registry.register(doc_b).await.unwrap();
        registry.register(doc_c).await.unwrap();

        // Create router node with cyclic edges
        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // A -> B -> C -> A would be a cycle
        let content = r#"{
            "version": 1,
            "edges": [
                {"from": "doc-a", "to": "doc-b"},
                {"from": "doc-b", "to": "doc-c"},
                {"from": "doc-c", "to": "doc-a"}
            ]
        }"#;
        router.apply_state(&make_text_update(content)).unwrap();

        // Verify content was set
        let router_content = router.get_content().await.unwrap();
        assert!(
            !router_content.is_empty(),
            "Router content should not be empty"
        );

        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));
        manager.apply_wiring().await;

        // Check managed wires
        let managed = manager.managed_wires.read().await;

        // First two edges should succeed, third should fail (cycle)
        // We should have 2 wires managed (A->B and B->C)
        assert_eq!(
            managed.len(),
            2,
            "Expected 2 managed wires, got {}",
            managed.len()
        );

        // Verify wires in registry as well
        let wirings_a = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        let wirings_b = registry.get_outgoing_wirings(&NodeId::new("doc-b")).await;
        let wirings_c = registry.get_outgoing_wirings(&NodeId::new("doc-c")).await;

        // A -> B and B -> C should exist, C -> A should be skipped
        assert_eq!(wirings_a.len(), 1, "Expected 1 wire from A");
        assert_eq!(wirings_b.len(), 1, "Expected 1 wire from B");
        assert_eq!(
            wirings_c.len(),
            0,
            "Expected 0 wires from C (cycle prevented)"
        );
    }

    #[tokio::test]
    async fn test_router_rejects_unsupported_version() {
        let registry = Arc::new(NodeRegistry::new());

        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        let content = r#"{"version": 2, "edges": []}"#;
        router.apply_state(&make_text_update(content)).unwrap();

        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));

        // This should emit an error but not crash
        manager.apply_wiring().await;

        // Schema should not be stored
        let last_schema = manager.last_valid_schema.read().await;
        assert!(last_schema.is_none());
    }

    #[tokio::test]
    async fn test_router_rejects_unsupported_content_type() {
        let registry = Arc::new(NodeRegistry::new());

        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // Content with invalid content_type (typo: text/plan instead of text/plain)
        let content = r#"{
            "version": 1,
            "nodes": {
                "bad-node": { "type": "document", "content_type": "text/plan" }
            },
            "edges": []
        }"#;
        router.apply_state(&make_text_update(content)).unwrap();

        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));
        manager.apply_wiring().await;

        // Node should NOT be created (invalid content_type rejected)
        assert!(
            registry.get(&NodeId::new("bad-node")).await.is_none(),
            "node with invalid content_type should NOT be created"
        );

        // Schema should not be stored due to error
        let last_schema = manager.last_valid_schema.read().await;
        assert!(last_schema.is_none());
    }

    #[tokio::test]
    async fn test_router_recreates_externally_unwired() {
        let registry = Arc::new(NodeRegistry::new());

        // Create two document nodes
        let doc_a = Arc::new(DocumentNode::new("doc-a", ContentType::Text));
        let doc_b = Arc::new(DocumentNode::new("doc-b", ContentType::Text));
        registry.register(doc_a).await.unwrap();
        registry.register(doc_b).await.unwrap();

        // Create router node as Text type
        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // Initial content with edge
        let content = r#"{"version": 1, "edges": [{"from": "doc-a", "to": "doc-b"}]}"#;
        router.apply_state(&make_text_update(content)).unwrap();

        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));
        manager.apply_wiring().await;

        // Verify wire exists
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(wirings.len(), 1, "wire should exist after first apply");
        let sub_id = wirings[0].0.clone();

        // Externally unwire (simulating DELETE /nodes/doc-a/wire/doc-b)
        registry.unwire(&sub_id).await.ok();
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(
            wirings.len(),
            0,
            "wire should be removed after external unwire"
        );

        // Re-apply wiring - router should detect stale managed_wire and recreate
        manager.apply_wiring().await;

        // Wire should be recreated
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(
            wirings.len(),
            1,
            "wire should be recreated after external unwire"
        );
    }

    #[tokio::test]
    async fn test_router_adopts_externally_created_wire() {
        let registry = Arc::new(NodeRegistry::new());

        // Create two document nodes
        let doc_a = Arc::new(DocumentNode::new("doc-a", ContentType::Text));
        let doc_b = Arc::new(DocumentNode::new("doc-b", ContentType::Text));
        registry.register(doc_a).await.unwrap();
        registry.register(doc_b).await.unwrap();

        // Externally create a wire (simulating POST /nodes/doc-a/wire/doc-b)
        let external_sub_id = registry
            .wire(&NodeId::new("doc-a"), &NodeId::new("doc-b"))
            .await
            .unwrap();

        // Verify wire exists
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(wirings.len(), 1, "external wire should exist");

        // Create router node as Text type (stores JSON as text)
        let router = Arc::new(DocumentNode::new("router", ContentType::Text));
        registry.register(router.clone()).await.unwrap();

        // Set router content with edge that matches the external wire
        let content =
            r#"{"version": 1, "edges": [{"from": "doc-a", "to": "doc-b", "port": "both"}]}"#;
        router.apply_state(&make_text_update(content)).unwrap();

        // Create and run router manager
        let manager = Arc::new(RouterManager::new(NodeId::new("router"), registry.clone()));
        manager.apply_wiring().await;

        // Should still have exactly 1 wire (no duplicate created)
        let wirings = registry.get_outgoing_wirings(&NodeId::new("doc-a")).await;
        assert_eq!(wirings.len(), 1, "should skip creating duplicate wire");

        // The wire should be the same one that was externally created
        assert_eq!(
            wirings[0].0, external_sub_id,
            "external wire should still exist unchanged"
        );

        // Verify managed_wires was NOT updated (we don't claim ownership of external wires)
        let managed = manager.managed_wires.read().await;
        assert_eq!(
            managed.len(),
            0,
            "managed_wires should NOT include external wire"
        );
    }
}
