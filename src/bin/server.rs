use clap::Parser;
use commonplace_doc::{
    cli::Args, create_router_with_config, mqtt::MqttConfig, store::CommitStore, RouterConfig,
};
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Parse CLI arguments
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "commonplace_doc=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Create commit store if database path is provided
    let commit_store = args.database.as_ref().map(|path| {
        tracing::info!("Using database at: {}", path.display());
        CommitStore::new(path).expect("Failed to create commit store")
    });

    if commit_store.is_none() {
        tracing::warn!("No database specified - commit functionality will be disabled");
        tracing::warn!("Use --database <path> to enable commits");
    }

    // Log filesystem root if configured
    if let Some(ref fs_root) = args.fs_root {
        tracing::info!("Filesystem root: {}", fs_root);
    }

    // Create MQTT config if broker URL is specified
    let mqtt_config = args.mqtt_broker.as_ref().map(|broker_url| {
        let client_id = args
            .mqtt_client_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        tracing::info!("MQTT broker: {} (client: {})", broker_url, client_id);
        MqttConfig {
            broker_url: broker_url.clone(),
            client_id,
            ..Default::default()
        }
    });

    // Log MQTT subscriptions if configured
    for path in &args.mqtt_subscribe {
        tracing::info!("MQTT subscribe: {}", path);
    }

    // Log static directory if configured
    if let Some(ref static_dir) = args.static_dir {
        tracing::info!("Static directory: {}", static_dir.display());
    }

    // Build our application with routes
    let app = create_router_with_config(RouterConfig {
        commit_store,
        fs_root: args.fs_root,
        mqtt: mqtt_config,
        mqtt_subscribe: args.mqtt_subscribe,
        static_dir: args.static_dir.map(|p| p.to_string_lossy().to_string()),
    })
    .await;

    // Run the server
    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("Invalid address");

    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
