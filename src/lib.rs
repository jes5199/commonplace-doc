pub mod api;
pub mod b64;
pub mod cli;
pub mod commit;
pub mod diff;
pub mod document;
pub mod events;
pub mod fs;
pub mod http_gateway;
pub mod mqtt;
pub mod orchestrator;
pub mod replay;
pub mod sse;
pub mod store;
pub mod sync;

use axum::{routing::get, Router};
use document::{ContentType, DocumentStore};
use events::CommitBroadcaster;
use fs::FilesystemReconciler;
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
    /// MQTT configuration (if specified, enables MQTT transport)
    pub mqtt: Option<mqtt::MqttConfig>,
    /// Document paths to subscribe via MQTT (requires mqtt to be set)
    pub mqtt_subscribe: Vec<String>,
}

/// Create a router with the given configuration.
pub async fn create_router_with_config(config: RouterConfig) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = config.commit_store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));

    // Initialize filesystem if --fs-root is specified
    if let Some(ref fs_root_id) = config.fs_root {
        // Get or create the fs-root document
        // Use Text type since the edit system uses TEXT-based Yjs updates
        let fs_doc = doc_store
            .get_or_create_with_id(fs_root_id, ContentType::Text)
            .await;

        tracing::info!("Filesystem root initialized at document: {}", fs_root_id);

        // Create the reconciler
        let reconciler = Arc::new(FilesystemReconciler::new(
            fs_root_id.clone(),
            doc_store.clone(),
        ));

        // Perform initial reconciliation
        let content = fs_doc.content;
        if !content.is_empty() && content != "{}" {
            if let Err(e) = reconciler.reconcile(&content).await {
                tracing::warn!("Initial fs reconcile failed: {}", e);
            }
        }
    }

    // Initialize MQTT service if configured
    if let Some(mqtt_config) = config.mqtt {
        match mqtt::MqttService::new(mqtt_config, doc_store.clone(), commit_store.clone()).await {
            Ok(mqtt_service) => {
                tracing::info!("MQTT service connected");
                let mqtt_service = Arc::new(mqtt_service);

                // Subscribe to store-level commands (e.g., create-document)
                if let Err(e) = mqtt_service.subscribe_store_commands().await {
                    tracing::warn!("Failed to subscribe to store commands: {}", e);
                } else {
                    tracing::info!("MQTT subscribed to store commands");
                }

                // Subscribe to configured document paths
                for path in &config.mqtt_subscribe {
                    if let Err(e) = mqtt_service.subscribe_path(path).await {
                        tracing::warn!("Failed to subscribe MQTT to path {}: {}", path, e);
                    } else {
                        tracing::info!("MQTT subscribed to path: {}", path);
                    }
                }

                // Start the event loop
                let service_for_loop = mqtt_service.clone();
                tokio::spawn(async move {
                    if let Err(e) = service_for_loop.run().await {
                        tracing::error!("MQTT event loop error: {}", e);
                    }
                });
            }
            Err(e) => {
                tracing::error!("Failed to connect MQTT service: {}", e);
            }
        }
    }

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
        ))
        .merge(sse::router(doc_store, commit_store, commit_broadcaster))
        .layer(CorsLayer::permissive())
}

/// Create a router with a commit store (no filesystem).
/// This is synchronous because no async initialization is needed when fs_root is None.
pub fn create_router_with_store(store: Option<CommitStore>) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
        ))
        .merge(sse::router(doc_store, commit_store, commit_broadcaster))
        .layer(CorsLayer::permissive())
}

pub fn create_router() -> Router {
    create_router_with_store(None)
}
