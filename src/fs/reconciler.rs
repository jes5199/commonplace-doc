//! Filesystem reconciler: watches the fs-root node and creates document nodes.

use super::error::FsError;
use super::schema::{Entry, FsSchema};
use crate::document::ContentType;
use crate::node::{DocumentNode, Event, NodeId, NodeRegistry, ObservableNode};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Manages the filesystem abstraction layer.
///
/// Watches the fs-root document node for changes and creates document nodes
/// for entries declared in the filesystem JSON.
pub struct FilesystemReconciler {
    /// The fs-root node ID
    fs_root_id: NodeId,
    /// Reference to node registry
    registry: Arc<NodeRegistry>,
    /// Last successfully parsed schema (kept on parse errors)
    last_valid_schema: RwLock<Option<FsSchema>>,
    /// Set of node IDs we've already created
    known_nodes: RwLock<HashSet<String>>,
}

impl FilesystemReconciler {
    /// Create a new FilesystemReconciler.
    pub fn new(fs_root_id: NodeId, registry: Arc<NodeRegistry>) -> Self {
        Self {
            fs_root_id,
            registry,
            last_valid_schema: RwLock::new(None),
            known_nodes: RwLock::new(HashSet::new()),
        }
    }

    /// Start watching the fs-root node for changes.
    /// Returns a JoinHandle for the background task.
    pub async fn start(self: Arc<Self>) -> JoinHandle<()> {
        let reconciler = self.clone();

        tokio::spawn(async move {
            // Get the fs-root node
            let node = match reconciler.registry.get(&reconciler.fs_root_id).await {
                Some(n) => n,
                None => {
                    tracing::error!("fs-root node not found: {}", reconciler.fs_root_id.0);
                    return;
                }
            };

            // Subscribe to blue port for edits
            let mut blue_sub = node.subscribe_blue();

            tracing::info!(
                "Filesystem reconciler started for: {}",
                reconciler.fs_root_id.0
            );

            loop {
                match blue_sub.recv().await {
                    Ok(_edit) => {
                        // Edit received - fetch current content and reconcile
                        if let Some(doc_node) = node.as_any().downcast_ref::<DocumentNode>() {
                            match doc_node.get_content().await {
                                Ok(content) => {
                                    if let Err(e) = reconciler.reconcile(&content).await {
                                        reconciler.emit_error(e, None).await;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to get fs-root content: {}", e);
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("fs reconciler lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("fs-root node closed, stopping reconciler");
                        break;
                    }
                }
            }
        })
    }

    /// Reconcile current state: parse JSON, collect entries, create missing nodes.
    pub async fn reconcile(&self, content: &str) -> Result<(), FsError> {
        // 1. Parse JSON
        let schema: FsSchema =
            serde_json::from_str(content).map_err(|e| FsError::ParseError(e.to_string()))?;

        // 2. Validate version
        if schema.version != 1 {
            return Err(FsError::UnsupportedVersion(schema.version));
        }

        // 3. Collect all entries from schema
        let entries = if let Some(ref root) = schema.root {
            self.collect_entries(root, "").await?
        } else {
            vec![]
        };

        // 4. For each entry, ensure node exists
        let mut known = self.known_nodes.write().await;
        for (path, node_id, content_type) in entries {
            let nid = NodeId::new(&node_id);

            // Check if node exists in registry (not just known_nodes)
            // This handles the case where a node was deleted externally
            let node_exists = self.registry.get(&nid).await.is_some();

            if !node_exists {
                self.registry
                    .get_or_create_document(&nid, content_type)
                    .await
                    .map_err(|e| FsError::NodeError(e.to_string()))?;

                tracing::debug!("Created fs node: {} -> {}", path, node_id);
            }

            // Track in known_nodes regardless
            known.insert(node_id.clone());
        }

        // 5. Update last valid schema
        *self.last_valid_schema.write().await = Some(schema);

        Ok(())
    }

    /// Walk the entry tree, collecting all (path, resolved_node_id, content_type).
    ///
    /// For node-backed directories, recursively fetches and parses their content.
    async fn collect_entries(
        &self,
        entry: &Entry,
        current_path: &str,
    ) -> Result<Vec<(String, String, ContentType)>, FsError> {
        let mut results = vec![];

        match entry {
            Entry::Doc(doc) => {
                let node_id = doc
                    .node_id
                    .clone()
                    .unwrap_or_else(|| self.derive_node_id(current_path));
                let content_type = doc
                    .content_type
                    .as_deref()
                    .and_then(ContentType::from_mime)
                    .unwrap_or(ContentType::Json);
                results.push((current_path.to_string(), node_id, content_type));
            }
            Entry::Dir(dir) => {
                // Spec: node-backed and inline forms are mutually exclusive
                if dir.node_id.is_some() && dir.entries.is_some() {
                    return Err(FsError::SchemaError(format!(
                        "Directory at '{}' has both node_id and entries (mutually exclusive)",
                        if current_path.is_empty() {
                            "/"
                        } else {
                            current_path
                        }
                    )));
                }

                // Handle node-backed directory
                if let Some(ref nid) = dir.node_id {
                    let content_type = dir
                        .content_type
                        .as_deref()
                        .and_then(ContentType::from_mime)
                        .unwrap_or(ContentType::Json);

                    // First, ensure the node exists
                    results.push((current_path.to_string(), nid.clone(), content_type.clone()));

                    // Then, try to recursively process its content
                    if let Some(child_entries) = self
                        .collect_node_backed_dir_entries(nid, current_path)
                        .await
                    {
                        results.extend(child_entries);
                    }
                }

                // Handle inline entries
                if let Some(ref entries) = dir.entries {
                    for (name, child) in entries {
                        // Validate name
                        Entry::validate_name(name)?;

                        let child_path = if current_path.is_empty() {
                            name.clone()
                        } else {
                            format!("{}/{}", current_path, name)
                        };
                        results.extend(Box::pin(self.collect_entries(child, &child_path)).await?);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Try to fetch and parse a node-backed directory's content.
    /// Returns None if the node doesn't exist yet or content is invalid.
    async fn collect_node_backed_dir_entries(
        &self,
        node_id: &str,
        base_path: &str,
    ) -> Option<Vec<(String, String, ContentType)>> {
        let nid = NodeId::new(node_id);

        // Try to get existing node
        let node = self.registry.get(&nid).await?;

        // Try to get content
        let doc_node = node.as_any().downcast_ref::<DocumentNode>()?;
        let content = doc_node.get_content().await.ok()?;

        // Try to parse as filesystem schema
        let schema: FsSchema = serde_json::from_str(&content).ok()?;

        // Validate version (same as main reconcile)
        if schema.version != 1 {
            return None;
        }

        // Recursively collect from the nested root
        if let Some(ref root) = schema.root {
            // Use Box::pin for recursive async
            Box::pin(self.collect_entries(root, base_path)).await.ok()
        } else {
            Some(vec![])
        }
    }

    /// Derive node ID from path: `<fs-root-id>:<path>`.
    fn derive_node_id(&self, path: &str) -> String {
        if path.is_empty() {
            // Root entry without explicit node_id - use fs-root itself
            self.fs_root_id.0.clone()
        } else {
            format!("{}:{}", self.fs_root_id.0, path)
        }
    }

    /// Emit an error event on the fs-root's red port.
    pub async fn emit_error(&self, error: FsError, path: Option<&str>) {
        let event = Event {
            event_type: "fs.error".to_string(),
            payload: error.to_event_payload(path),
            source: self.fs_root_id.clone(),
        };

        if let Some(node) = self.registry.get(&self.fs_root_id).await {
            if let Err(e) = node.receive_event(event).await {
                tracing::warn!("Failed to emit fs.error event: {}", e);
            }
        }
    }

    /// Get the fs-root node ID.
    pub fn fs_root_id(&self) -> &NodeId {
        &self.fs_root_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_node_id() {
        let registry = Arc::new(NodeRegistry::new());
        let reconciler = FilesystemReconciler::new(NodeId::new("my-fs"), registry);

        assert_eq!(
            reconciler.derive_node_id("notes/ideas.txt"),
            "my-fs:notes/ideas.txt"
        );
        assert_eq!(reconciler.derive_node_id("file.txt"), "my-fs:file.txt");
        assert_eq!(reconciler.derive_node_id(""), "my-fs");
    }
}
