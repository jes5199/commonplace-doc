//! commonplace-store: Document storage with MQTT transport (no HTTP)
//!
//! This binary manages document persistence and responds to MQTT messages.
//! It has no HTTP endpoints - use commonplace-http for HTTP access.

use clap::Parser;
use commonplace_doc::{
    cli::StoreArgs,
    document::ContentType,
    fs::FilesystemReconciler,
    mqtt::{MqttConfig, MqttService},
    node::{NodeId, NodeRegistry, ObservableNode},
    router::RouterManager,
    store::CommitStore,
};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Parse CLI arguments
    let args = StoreArgs::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "commonplace_doc=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting commonplace-store");

    // Create commit store (required for store binary)
    tracing::info!("Using database at: {}", args.database.display());
    let commit_store =
        Arc::new(CommitStore::new(&args.database).expect("Failed to create commit store"));

    // Create node registry
    let node_registry = Arc::new(NodeRegistry::new());

    // Initialize filesystem root
    let fs_root_id = NodeId::new(&args.fs_root);
    tracing::info!("Filesystem root: {}", args.fs_root);

    // Get or create the fs-root document node
    let fs_node = match node_registry
        .get_or_create_document(&fs_root_id, ContentType::Text)
        .await
    {
        Ok(node) => {
            tracing::info!("Filesystem root initialized at node: {}", args.fs_root);
            Some(node)
        }
        Err(e) => {
            tracing::error!("Failed to create fs-root node: {}", e);
            None
        }
    };

    // Create filesystem reconciler
    let reconciler = Arc::new(FilesystemReconciler::new(
        fs_root_id.clone(),
        node_registry.clone(),
    ));

    // Start watching for filesystem changes
    reconciler.clone().start().await;

    // Perform initial reconciliation
    if let Some(ref fs_node) = fs_node {
        if let Some(observable) = fs_node
            .as_any()
            .downcast_ref::<commonplace_doc::node::DocumentNode>()
        {
            match observable.get_content().await {
                Ok(content) if !content.is_empty() && content != "{}" => {
                    if let Err(e) = reconciler.reconcile_with_watchers(&content).await {
                        tracing::warn!("Initial fs reconcile failed: {}", e);
                        reconciler.emit_error(e, None).await;
                    }
                }
                _ => {}
            }
        }
    }

    // Initialize router documents
    for router_id_str in &args.routers {
        let node_id = NodeId::new(router_id_str);

        match node_registry
            .get_or_create_document(&node_id, ContentType::Json)
            .await
        {
            Ok(_) => {
                tracing::info!("Router document initialized at node: {}", router_id_str);
                let manager = Arc::new(RouterManager::new(node_id.clone(), node_registry.clone()));
                manager.start().await;
            }
            Err(e) => {
                tracing::error!("Failed to create router node {}: {}", router_id_str, e);
            }
        }
    }

    // Create MQTT config
    let mqtt_config = MqttConfig {
        broker_url: args.mqtt_broker.clone(),
        client_id: args.mqtt_client_id.clone(),
        ..Default::default()
    };

    tracing::info!(
        "Connecting to MQTT broker: {} (client: {})",
        args.mqtt_broker,
        args.mqtt_client_id
    );

    // Connect to MQTT
    let mqtt_service = match MqttService::new(
        mqtt_config,
        node_registry.clone(),
        Some(commit_store.clone()),
    )
    .await
    {
        Ok(service) => {
            tracing::info!("MQTT service connected");
            Arc::new(service)
        }
        Err(e) => {
            tracing::error!("Failed to connect MQTT service: {}", e);
            std::process::exit(1);
        }
    };

    // Subscribe to paths based on fs-root content
    // Parse the fs-root JSON to get document paths
    if let Some(ref fs_node) = fs_node {
        if let Some(observable) = fs_node
            .as_any()
            .downcast_ref::<commonplace_doc::node::DocumentNode>()
        {
            if let Ok(content) = observable.get_content().await {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                    subscribe_to_paths(&mqtt_service, &json, "").await;
                }
            }
        }
    }

    // Run the MQTT event loop (blocks forever)
    tracing::info!("Starting MQTT event loop");
    if let Err(e) = mqtt_service.run().await {
        tracing::error!("MQTT event loop error: {}", e);
        std::process::exit(1);
    }
}

/// Recursively subscribe to document paths found in the fs-root JSON
fn subscribe_to_paths<'a>(
    mqtt: &'a MqttService,
    value: &'a serde_json::Value,
    prefix: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        if let serde_json::Value::Object(map) = value {
            for (key, val) in map {
                // Skip metadata keys
                if key.starts_with('_') {
                    continue;
                }

                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}/{}", prefix, key)
                };

                // If this key has an extension, it's a document - subscribe
                if key.contains('.') {
                    if let Err(e) = mqtt.subscribe_path(&path).await {
                        tracing::warn!("Failed to subscribe to {}: {}", path, e);
                    } else {
                        tracing::info!("Subscribed to path: {}", path);
                    }
                } else {
                    // It's a directory, recurse
                    subscribe_to_paths(mqtt, val, &path).await;
                }
            }
        }
    })
}
