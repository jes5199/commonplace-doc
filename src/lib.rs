pub mod api;
pub mod b64;
pub mod cbd;
pub mod cli;
pub mod commands;
pub mod commit;
pub mod content_type;
pub mod diff;
pub mod document;
pub mod events;
pub mod files;
pub mod fs;
pub mod http_gateway;
pub mod mqtt;
pub mod orchestrator;
pub mod path;
pub mod replay;
pub mod sdk;
pub mod services;
pub mod sse;
pub mod store;
pub mod sync;
pub mod viewer;
pub mod workspace;
pub mod ws;

// Default configuration constants
// These are used across CLI argument structs and configuration to ensure consistency.

/// Default server URL for the commonplace HTTP server.
pub const DEFAULT_SERVER_URL: &str = "http://localhost:5199";

/// Default MQTT broker URL.
pub const DEFAULT_MQTT_BROKER_URL: &str = "mqtt://localhost:1883";

/// Default MQTT broker address (without protocol prefix, for direct socket connections).
pub const DEFAULT_MQTT_BROKER_ADDR: &str = "localhost:1883";

/// Default workspace name for MQTT topic namespacing.
pub const DEFAULT_WORKSPACE: &str = "commonplace";

use axum::{routing::get, Router};
use content_type::ContentType;
use document::DocumentStore;
use events::CommitBroadcaster;
use fs::FilesystemReconciler;
use services::DocumentService;
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
    /// Directory containing static files for the document viewer
    pub static_dir: Option<String>,
}

/// Create a router with the given configuration.
pub async fn create_router_with_config(config: RouterConfig) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = config.commit_store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));

    // Initialize filesystem if --fs-root is specified
    // Capture fs-root content for MQTT handlers and reconciler for DocumentService
    let (fs_root_context, reconciler): (
        Option<(String, String)>,
        Option<Arc<FilesystemReconciler>>,
    ) = if let Some(ref fs_root_id) = config.fs_root {
        // Get or create the fs-root document
        // Use Json type since the fs-root schema is a JSON map structure
        let fs_doc = doc_store
            .get_or_create_with_id(fs_root_id, ContentType::Json)
            .await;

        tracing::info!("Filesystem root initialized at document: {}", fs_root_id);

        // Create the reconciler
        let reconciler = Arc::new(FilesystemReconciler::new(
            fs_root_id.clone(),
            doc_store.clone(),
        ));

        // Perform initial reconciliation
        let content = fs_doc.content.clone();
        if !content.is_empty() && content != "{}" {
            if let Err(e) = reconciler.reconcile(&content).await {
                tracing::warn!("Initial fs reconcile failed: {}", e);
            }
        }

        (Some((fs_root_id.clone(), content)), Some(reconciler))
    } else {
        (None, None)
    };

    // Initialize MQTT service if configured
    // Capture MQTT client and workspace for DocumentService to publish commits
    let mqtt_context: Option<(Arc<mqtt::MqttClient>, String)> = if let Some(mqtt_config) =
        config.mqtt
    {
        let workspace = mqtt_config.workspace.clone();
        match mqtt::MqttService::new(mqtt_config, doc_store.clone(), commit_store.clone()).await {
            Ok(mqtt_service) => {
                tracing::info!("MQTT service connected");
                let mqtt_service = Arc::new(mqtt_service);
                let mqtt_client = mqtt_service.client().clone();

                // Initialize fs-root caches on MQTT handlers if fs-root is configured
                if let Some((ref fs_root_id, ref fs_root_content)) = fs_root_context {
                    mqtt_service
                        .edits_handler()
                        .set_fs_root_content(fs_root_content.clone())
                        .await;
                    mqtt_service
                        .edits_handler()
                        .set_fs_root_path(fs_root_id.clone())
                        .await;
                    mqtt_service
                        .sync_handler()
                        .set_fs_root_content(fs_root_content.clone())
                        .await;
                    mqtt_service
                        .sync_handler()
                        .set_fs_root_path(fs_root_id.clone())
                        .await;
                    tracing::info!("MQTT handlers initialized with fs-root context");

                    // Publish fs-root ID as retained message for client discovery
                    if let Err(e) = mqtt_client
                        .publish_retained(
                            &format!("{}/_system/fs-root", workspace),
                            fs_root_id.as_bytes(),
                            mqtt::QoS::AtLeastOnce,
                        )
                        .await
                    {
                        tracing::warn!("Failed to publish fs-root to MQTT: {}", e);
                    } else {
                        tracing::info!(
                            "Published fs-root ID to MQTT: {}/_system/fs-root",
                            workspace
                        );
                    }
                }

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

                Some((mqtt_client, workspace))
            }
            Err(e) => {
                tracing::error!("Failed to connect MQTT service: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Extract MQTT client and workspace for commands router
    let (mqtt_client_for_commands, mqtt_workspace_for_commands) = match &mqtt_context {
        Some((client, workspace)) => (Some(client.clone()), Some(workspace.clone())),
        None => (None, None),
    };

    // Create shared service for handlers
    // DocumentService needs MQTT client to publish commits for real-time sync
    let service = Arc::new(match (reconciler, &config.fs_root, mqtt_context) {
        // Reconciler + MQTT
        (Some(reconciler), Some(ref fs_root_id), Some((mqtt_client, mqtt_workspace))) => {
            DocumentService::with_reconciler_and_mqtt(
                doc_store.clone(),
                commit_store.clone(),
                commit_broadcaster.clone(),
                reconciler,
                fs_root_id.clone(),
                mqtt_client,
                mqtt_workspace,
            )
        }
        // Reconciler only
        (Some(reconciler), Some(ref fs_root_id), None) => DocumentService::with_reconciler(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            reconciler,
            fs_root_id.clone(),
        ),
        // MQTT only
        (_, _, Some((mqtt_client, mqtt_workspace))) => DocumentService::with_mqtt(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            mqtt_client,
            mqtt_workspace,
        ),
        // Neither
        _ => DocumentService::new(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
        ),
    });

    let mut router = Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            config.fs_root.clone(),
            service.clone(),
        ))
        .merge(files::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            config.fs_root.clone(),
            service,
        ))
        .merge(sse::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            config.fs_root.clone(),
        ))
        .merge(ws::router(
            doc_store,
            commit_store,
            commit_broadcaster,
            config.fs_root,
        ));

    // Add viewer routes if static_dir is configured
    if let Some(viewer_router) = viewer::router(config.static_dir) {
        router = router.merge(viewer_router);
    }

    // Add SDK routes for JS evaluator
    router = router.merge(sdk::router());

    // Add commands routes if MQTT is configured
    if let Some(commands_router) =
        commands::router(mqtt_client_for_commands, mqtt_workspace_for_commands)
    {
        router = router.merge(commands_router);
    }

    router.layer(CorsLayer::permissive())
}

/// Create a router with a commit store (no filesystem).
/// This is synchronous because no async initialization is needed when fs_root is None.
pub fn create_router_with_store(store: Option<CommitStore>) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));

    let service = Arc::new(DocumentService::new(
        doc_store.clone(),
        commit_store.clone(),
        commit_broadcaster.clone(),
    ));

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            None, // No fs-root in this variant
            service.clone(),
        ))
        .merge(files::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            None, // No fs-root in this variant
            service,
        ))
        .merge(sse::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            None,
        ))
        .merge(ws::router(
            doc_store,
            commit_store,
            commit_broadcaster,
            None,
        ))
        .merge(sdk::router())
        .layer(CorsLayer::permissive())
}

pub fn create_router() -> Router {
    create_router_with_store(None)
}
