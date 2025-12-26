use clap::Parser;
use commonplace_doc::{cli::Args, create_router_with_config, store::CommitStore, RouterConfig};
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

    // Build our application with routes
    let app = create_router_with_config(RouterConfig {
        commit_store,
        fs_root: args.fs_root,
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
