//! Filesystem reconciler: watches the fs-root node and creates document nodes.

use super::error::FsError;
use super::schema::{Entry, FsSchema};
use crate::document::ContentType;
use crate::node::{DocumentNode, Event, NodeId, NodeRegistry, ObservableNode};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
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
    /// Set of node-backed directory IDs we're watching
    watched_dirs: RwLock<HashSet<String>>,
    /// Channel sender to trigger reconciliation (None until start() is called)
    reconcile_trigger: RwLock<Option<mpsc::Sender<()>>>,
}

impl FilesystemReconciler {
    /// Create a new FilesystemReconciler.
    pub fn new(fs_root_id: NodeId, registry: Arc<NodeRegistry>) -> Self {
        Self {
            fs_root_id,
            registry,
            last_valid_schema: RwLock::new(None),
            known_nodes: RwLock::new(HashSet::new()),
            watched_dirs: RwLock::new(HashSet::new()),
            reconcile_trigger: RwLock::new(None),
        }
    }

    /// Start watching the fs-root node for changes.
    /// Returns a JoinHandle for the background task.
    pub async fn start(self: Arc<Self>) -> JoinHandle<()> {
        let reconciler = self.clone();

        // Create channel for triggering reconciliation
        let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(16);
        *reconciler.reconcile_trigger.write().await = Some(trigger_tx.clone());

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
                tokio::select! {
                    // Handle fs-root changes
                    result = blue_sub.recv() => {
                        match result {
                            Ok(_edit) => {
                                reconciler.do_reconcile().await;
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
                    // Handle triggers from node-backed directory watchers
                    Some(()) = trigger_rx.recv() => {
                        reconciler.do_reconcile().await;
                    }
                }
            }
        })
    }

    /// Internal: perform reconciliation and update node-backed dir watchers.
    async fn do_reconcile(self: &Arc<Self>) {
        let node = match self.registry.get(&self.fs_root_id).await {
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
                tracing::warn!("Failed to get fs-root content: {}", e);
                return;
            }
        };

        // Reconcile and collect discovered node-backed directories
        let discovered_dirs = match self.reconcile_and_collect_dirs(&content).await {
            Ok(dirs) => dirs,
            Err(e) => {
                self.emit_error(e, None).await;
                return;
            }
        };

        // Start watching any new node-backed directories
        self.update_dir_watchers(discovered_dirs).await;
    }

    /// Reconcile and return the set of node-backed directory IDs discovered.
    async fn reconcile_and_collect_dirs(&self, content: &str) -> Result<HashSet<String>, FsError> {
        // 1. Parse JSON
        let schema: FsSchema =
            serde_json::from_str(content).map_err(|e| FsError::ParseError(e.to_string()))?;

        // 2. Validate version
        if schema.version != 1 {
            return Err(FsError::UnsupportedVersion(schema.version));
        }

        // 3. Collect all entries and node-backed dirs from schema
        let mut node_backed_dirs = HashSet::new();
        let entries = if let Some(ref root) = schema.root {
            self.collect_entries_with_dirs(root, "", &mut node_backed_dirs)
                .await?
        } else {
            vec![]
        };

        // 4. For each entry, ensure node exists
        let mut known = self.known_nodes.write().await;
        for (path, node_id, content_type) in entries {
            let nid = NodeId::new(&node_id);

            // Check if node exists in registry
            let node_exists = self.registry.get(&nid).await.is_some();

            if !node_exists {
                self.registry
                    .get_or_create_document(&nid, content_type)
                    .await
                    .map_err(|e| FsError::NodeError(e.to_string()))?;

                tracing::debug!("Created fs node: {} -> {}", path, node_id);
            }

            known.insert(node_id.clone());
        }

        // 5. Update last valid schema
        *self.last_valid_schema.write().await = Some(schema);

        Ok(node_backed_dirs)
    }

    /// Update watchers for node-backed directories.
    async fn update_dir_watchers(self: &Arc<Self>, discovered: HashSet<String>) {
        let mut watched = self.watched_dirs.write().await;

        for dir_id in discovered {
            if !watched.contains(&dir_id) {
                // New node-backed directory - start watching it
                watched.insert(dir_id.clone());

                let reconciler = self.clone();
                let node_id = NodeId::new(&dir_id);

                tokio::spawn(async move {
                    if let Some(node) = reconciler.registry.get(&node_id).await {
                        let mut sub = node.subscribe_blue();
                        loop {
                            match sub.recv().await {
                                Ok(_) => {
                                    // Trigger reconciliation via channel
                                    if let Some(ref tx) = *reconciler.reconcile_trigger.read().await
                                    {
                                        let _ = tx.send(()).await;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(n)) => {
                                    tracing::warn!(
                                        "node-backed dir {} watcher lagged by {} messages",
                                        node_id.0,
                                        n
                                    );
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    tracing::debug!(
                                        "node-backed dir {} closed, stopping watcher",
                                        node_id.0
                                    );
                                    break;
                                }
                            }
                        }
                    }
                });

                tracing::debug!("Started watching node-backed dir: {}", dir_id);
            }
        }
    }

    /// Reconcile current state and register watchers for node-backed directories.
    /// Use this for initial reconciliation when you want node-backed dirs to be watched.
    pub async fn reconcile_with_watchers(self: &Arc<Self>, content: &str) -> Result<(), FsError> {
        let dirs = self.reconcile_and_collect_dirs(content).await?;
        self.update_dir_watchers(dirs).await;
        Ok(())
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

    /// Walk the entry tree, collecting entries and tracking node-backed directory IDs.
    async fn collect_entries_with_dirs(
        &self,
        entry: &Entry,
        current_path: &str,
        node_backed_dirs: &mut HashSet<String>,
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

                    // Check for cycles before recursing - skip if we've seen this node
                    if node_backed_dirs.contains(nid) {
                        tracing::warn!(
                            "Cycle detected: node-backed dir {} already visited, skipping",
                            nid
                        );
                    } else {
                        // Track this as a node-backed directory
                        node_backed_dirs.insert(nid.clone());

                        // Then, try to recursively process its content
                        if let Some(child_entries) = self
                            .collect_node_backed_dir_entries_with_dirs(
                                nid,
                                current_path,
                                node_backed_dirs,
                            )
                            .await
                        {
                            results.extend(child_entries);
                        }
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
                        results.extend(
                            Box::pin(self.collect_entries_with_dirs(
                                child,
                                &child_path,
                                node_backed_dirs,
                            ))
                            .await?,
                        );
                    }
                }
            }
        }

        Ok(results)
    }

    /// Try to fetch and parse a node-backed directory's content.
    /// Returns None if the node doesn't exist yet or content is invalid.
    /// Emits fs.error events for parse/schema errors so clients can debug.
    async fn collect_node_backed_dir_entries(
        &self,
        node_id: &str,
        base_path: &str,
    ) -> Option<Vec<(String, String, ContentType)>> {
        let nid = NodeId::new(node_id);

        // Try to get existing node - not an error if it doesn't exist yet
        let node = self.registry.get(&nid).await?;

        // Try to get content - not an error if node isn't a document
        let doc_node = node.as_any().downcast_ref::<DocumentNode>()?;
        let content = match doc_node.get_content().await {
            Ok(c) => c,
            Err(_) => return None, // Content retrieval failure is not a schema error
        };

        // Empty content is not an error (node just hasn't been populated)
        if content.is_empty() || content == "{}" {
            return None;
        }

        // Try to parse as filesystem schema - emit error if invalid
        let schema: FsSchema = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                self.emit_error(FsError::ParseError(e.to_string()), Some(base_path))
                    .await;
                return None;
            }
        };

        // Validate version - emit error if unsupported
        if schema.version != 1 {
            self.emit_error(FsError::UnsupportedVersion(schema.version), Some(base_path))
                .await;
            return None;
        }

        // Recursively collect from the nested root
        if let Some(ref root) = schema.root {
            // Use Box::pin for recursive async
            match Box::pin(self.collect_entries(root, base_path)).await {
                Ok(entries) => Some(entries),
                Err(e) => {
                    self.emit_error(e, Some(base_path)).await;
                    None
                }
            }
        } else {
            Some(vec![])
        }
    }

    /// Like collect_node_backed_dir_entries but also tracks discovered node-backed dirs.
    async fn collect_node_backed_dir_entries_with_dirs(
        &self,
        node_id: &str,
        base_path: &str,
        node_backed_dirs: &mut HashSet<String>,
    ) -> Option<Vec<(String, String, ContentType)>> {
        let nid = NodeId::new(node_id);

        // Try to get existing node - not an error if it doesn't exist yet
        let node = self.registry.get(&nid).await?;

        // Try to get content - not an error if node isn't a document
        let doc_node = node.as_any().downcast_ref::<DocumentNode>()?;
        let content = match doc_node.get_content().await {
            Ok(c) => c,
            Err(_) => return None,
        };

        // Empty content is not an error (node just hasn't been populated)
        if content.is_empty() || content == "{}" {
            return None;
        }

        // Try to parse as filesystem schema - emit error if invalid
        let schema: FsSchema = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                self.emit_error(FsError::ParseError(e.to_string()), Some(base_path))
                    .await;
                return None;
            }
        };

        // Validate version - emit error if unsupported
        if schema.version != 1 {
            self.emit_error(FsError::UnsupportedVersion(schema.version), Some(base_path))
                .await;
            return None;
        }

        // Recursively collect from the nested root
        if let Some(ref root) = schema.root {
            match Box::pin(self.collect_entries_with_dirs(root, base_path, node_backed_dirs)).await
            {
                Ok(entries) => Some(entries),
                Err(e) => {
                    self.emit_error(e, Some(base_path)).await;
                    None
                }
            }
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
