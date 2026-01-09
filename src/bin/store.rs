//! commonplace-store: Document storage with MQTT transport (no HTTP)
//!
//! This binary manages document persistence and responds to MQTT messages.
//! It has no HTTP endpoints - use commonplace-http for HTTP access.

use clap::Parser;
use commonplace_doc::{
    cli::StoreArgs,
    document::{ContentType, DocumentStore},
    fs::FilesystemReconciler,
    mqtt::{MqttConfig, MqttService},
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

    // Create document store
    let doc_store = Arc::new(DocumentStore::new());

    // Initialize filesystem root
    tracing::info!("Filesystem root: {}", args.fs_root);

    // Get or create the fs-root document (required)
    // Use Json type since the fs-root schema is a JSON map structure
    let fs_doc = doc_store
        .get_or_create_with_id(&args.fs_root, ContentType::Json)
        .await;

    tracing::info!("Filesystem root initialized at document: {}", args.fs_root);

    // Create filesystem reconciler
    let reconciler = Arc::new(FilesystemReconciler::new(
        args.fs_root.clone(),
        doc_store.clone(),
    ));

    // Perform initial reconciliation
    let content = fs_doc.content.clone();
    if !content.is_empty() && content != "{}" {
        if let Err(e) = reconciler.reconcile(&content).await {
            tracing::warn!("Initial fs reconcile failed: {}", e);
        }
    }

    // Create MQTT config
    let mqtt_config = MqttConfig {
        broker_url: args.mqtt_broker.clone(),
        client_id: args.mqtt_client_id.clone(),
        workspace: args.workspace.clone(),
        ..Default::default()
    };

    tracing::info!(
        "Connecting to MQTT broker: {} (client: {})",
        args.mqtt_broker,
        args.mqtt_client_id
    );

    // Connect to MQTT
    let mqtt_service =
        match MqttService::new(mqtt_config, doc_store.clone(), Some(commit_store.clone())).await {
            Ok(service) => {
                tracing::info!("MQTT service connected");
                Arc::new(service)
            }
            Err(e) => {
                tracing::error!("Failed to connect MQTT service: {}", e);
                std::process::exit(1);
            }
        };

    // Populate the handlers' fs_root_content cache for path->document ID resolution
    mqtt_service
        .edits_handler()
        .set_fs_root_content(content.clone())
        .await;
    mqtt_service
        .sync_handler()
        .set_fs_root_content(content.clone())
        .await;

    // Set the fs-root path so handlers can resolve paths correctly
    mqtt_service
        .edits_handler()
        .set_fs_root_path(args.fs_root.clone())
        .await;
    mqtt_service
        .sync_handler()
        .set_fs_root_path(args.fs_root.clone())
        .await;

    // Subscribe to store-level commands (e.g., create-document)
    if let Err(e) = mqtt_service.subscribe_store_commands().await {
        tracing::warn!("Failed to subscribe to store commands: {}", e);
    } else {
        tracing::info!("Subscribed to store commands");
    }

    // Subscribe to fs-root document itself (so we receive updates to filesystem structure)
    if let Err(e) = mqtt_service.subscribe_path(&args.fs_root).await {
        tracing::warn!("Failed to subscribe to fs-root {}: {}", args.fs_root, e);
    } else {
        tracing::info!("Subscribed to fs-root path: {}", args.fs_root);
    }

    // Subscribe to paths based on fs-root content
    // Parse the versioned fs-root schema to get document paths
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
        if let Some(root) = json.get("root") {
            subscribe_to_entry(&mqtt_service, root, "").await;
        }
    }

    // Run the MQTT event loop (blocks forever)
    tracing::info!("Starting MQTT event loop");
    if let Err(e) = mqtt_service.run().await {
        tracing::error!("MQTT event loop error: {}", e);
        std::process::exit(1);
    }
}

/// Recursively subscribe to document paths found in the versioned fs-root schema.
///
/// The schema format is:
/// ```json
/// {
///   "version": 1,
///   "root": {
///     "type": "dir",
///     "entries": {
///       "notes": { "type": "dir", "entries": { "todo.txt": { "type": "doc" } } },
///       "readme.txt": { "type": "doc" }
///     }
///   }
/// }
/// ```
fn subscribe_to_entry<'a>(
    mqtt: &'a MqttService,
    entry: &'a serde_json::Value,
    prefix: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let entry_type = match entry.get("type").and_then(|t| t.as_str()) {
            Some(t) => t,
            None => return,
        };

        match entry_type {
            "doc" => {
                // This is a document - subscribe to its path
                if !prefix.is_empty() {
                    if let Err(e) = mqtt.subscribe_path(prefix).await {
                        tracing::warn!("Failed to subscribe to {}: {}", prefix, e);
                    } else {
                        tracing::info!("Subscribed to path: {}", prefix);
                    }
                }
            }
            "dir" => {
                // This is a directory - recurse into entries
                if let Some(serde_json::Value::Object(map)) = entry.get("entries") {
                    for (name, child) in map {
                        let path = if prefix.is_empty() {
                            name.clone()
                        } else {
                            format!("{}/{}", prefix, name)
                        };
                        subscribe_to_entry(mqtt, child, &path).await;
                    }
                }
            }
            _ => {
                tracing::debug!("Unknown entry type: {}", entry_type);
            }
        }
    })
}
