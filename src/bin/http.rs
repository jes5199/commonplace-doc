//! commonplace-http: HTTP gateway that translates requests to MQTT
//!
//! This binary provides HTTP/SSE endpoints that communicate with
//! commonplace-store via MQTT. It is stateless - all document state
//! lives in the store.

use axum::{routing::get, Router};
use clap::Parser;
use commonplace_doc::{cli::HttpArgs, http_gateway, mqtt::MqttConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

async fn health_check() -> &'static str {
    "OK"
}

#[tokio::main]
async fn main() {
    // Parse CLI arguments
    let args = HttpArgs::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "commonplace_doc=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting commonplace-http gateway");

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

    // Create the HTTP gateway
    let gateway = match http_gateway::HttpGateway::new(mqtt_config).await {
        Ok(gw) => {
            tracing::info!("HTTP gateway connected to MQTT");
            Arc::new(gw)
        }
        Err(e) => {
            tracing::error!("Failed to create HTTP gateway: {}", e);
            std::process::exit(1);
        }
    };

    // Build the router
    let app = Router::new()
        .route("/health", get(health_check))
        .merge(http_gateway::router(gateway))
        .layer(CorsLayer::permissive());

    // Run the server
    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("Invalid address");

    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
