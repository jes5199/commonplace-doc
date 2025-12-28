pub mod api;
pub mod b64;
pub mod cli;
pub mod commit;
pub mod diff;
pub mod document;
pub mod events;
pub mod fs;
pub mod mqtt;
pub mod node;
pub mod replay;
pub mod router;
pub mod sse;
pub mod store;
pub mod sync;

use axum::{routing::get, Router};
use document::{ContentType, DocumentStore};
use events::CommitBroadcaster;
use fs::FilesystemReconciler;
use node::{NodeId, NodeRegistry, ObservableNode};
use router::RouterManager;
use std::sync::Arc;
use store::CommitStore;
use tower_http::cors::CorsLayer;

async fn health_check() -> &'static str {
    "OK"
}

/// Configuration for creating a router.
#[derive(Default)]
pub struct RouterConfig {
    /// Path to the commit store database
    pub commit_store: Option<CommitStore>,
    /// Node ID for filesystem root document
    pub fs_root: Option<String>,
    /// Node IDs for router documents
    pub routers: Vec<String>,
    /// MQTT configuration (if specified, enables MQTT transport)
    pub mqtt: Option<mqtt::MqttConfig>,
}

/// Create a router with the given configuration.
pub async fn create_router_with_config(config: RouterConfig) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = config.commit_store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));
    let node_registry = Arc::new(NodeRegistry::new());

    // Initialize filesystem if --fs-root is specified
    if let Some(fs_root_id) = config.fs_root {
        let node_id = NodeId::new(&fs_root_id);

        // Get or create the fs-root document node
        // Use Text type since the edit system uses TEXT-based Yjs updates
        match node_registry
            .get_or_create_document(&node_id, ContentType::Text)
            .await
        {
            Ok(fs_node) => {
                tracing::info!("Filesystem root initialized at node: {}", fs_root_id);

                // Create and start the reconciler
                let reconciler = Arc::new(FilesystemReconciler::new(
                    node_id.clone(),
                    node_registry.clone(),
                ));

                // Start watching (spawns background task)
                reconciler.clone().start().await;

                // Perform initial reconciliation with watcher registration
                if let Some(observable) = fs_node.as_any().downcast_ref::<node::DocumentNode>() {
                    match observable.get_content().await {
                        Ok(content) if !content.is_empty() && content != "{}" => {
                            if let Err(e) = reconciler.reconcile_with_watchers(&content).await {
                                tracing::warn!("Initial fs reconcile failed: {}", e);
                                // Emit fs.error event so clients are notified
                                reconciler.emit_error(e, None).await;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to create fs-root node: {}", e);
            }
        }
    }

    // Initialize router documents
    for router_id_str in config.routers {
        let node_id = NodeId::new(&router_id_str);

        // Get or create the router document node
        // Use Json type since router documents are JSON
        match node_registry
            .get_or_create_document(&node_id, ContentType::Json)
            .await
        {
            Ok(_) => {
                tracing::info!("Router document initialized at node: {}", router_id_str);

                // Create and start the router manager
                // (start() performs initial wiring before listening for edits)
                let manager = Arc::new(RouterManager::new(node_id.clone(), node_registry.clone()));
                manager.start().await;
            }
            Err(e) => {
                tracing::error!("Failed to create router node {}: {}", router_id_str, e);
            }
        }
    }

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            node_registry.clone(),
        ))
        .merge(sse::router(
            doc_store,
            commit_store,
            commit_broadcaster,
            node_registry,
        ))
        .layer(CorsLayer::permissive())
}

/// Create a router with a commit store (no filesystem).
/// This is synchronous because no async initialization is needed when fs_root is None.
pub fn create_router_with_store(store: Option<CommitStore>) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));
    let node_registry = Arc::new(NodeRegistry::new());

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            node_registry.clone(),
        ))
        .merge(sse::router(
            doc_store,
            commit_store,
            commit_broadcaster,
            node_registry,
        ))
        .layer(CorsLayer::permissive())
}

pub fn create_router() -> Router {
    create_router_with_store(None)
}
