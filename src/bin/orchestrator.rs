//! commonplace-orchestrator: Process supervisor for commonplace services
//!
//! Starts and manages child processes (store, http) with automatic restart on failure.

use clap::Parser;
use commonplace_doc::cli::OrchestratorArgs;
use commonplace_doc::orchestrator::{OrchestratorConfig, ProcessManager};
use std::net::TcpStream;
use std::time::Duration;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    let args = OrchestratorArgs::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("[orchestrator] Starting commonplace-orchestrator");
    tracing::info!("[orchestrator] Config file: {:?}", args.config);

    let config = match OrchestratorConfig::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("[orchestrator] Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    let broker = args.mqtt_broker.as_ref().unwrap_or(&config.mqtt_broker);

    tracing::info!("[orchestrator] Checking MQTT broker at {}", broker);
    // Use ToSocketAddrs to resolve hostname (e.g., "localhost:1883")
    use std::net::ToSocketAddrs;
    let addr = match broker.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(a) => a,
            None => {
                tracing::error!("[orchestrator] No addresses found for broker: {}", broker);
                std::process::exit(1);
            }
        },
        Err(e) => {
            tracing::error!("[orchestrator] Invalid broker address '{}': {}", broker, e);
            std::process::exit(1);
        }
    };
    match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
        Ok(_) => {
            tracing::info!("[orchestrator] MQTT broker is reachable");
        }
        Err(e) => {
            tracing::error!(
                "[orchestrator] Cannot connect to MQTT broker at {}: {}",
                broker,
                e
            );
            tracing::error!(
                "[orchestrator] Make sure mosquitto is running (systemctl status mosquitto)"
            );
            std::process::exit(1);
        }
    }

    let mut manager = ProcessManager::new(config, args.mqtt_broker.clone(), args.disable.clone());

    if let Some(only) = &args.only {
        tracing::info!("[orchestrator] Running only: {}", only);
        if let Err(e) = manager.spawn_process(only).await {
            tracing::error!("[orchestrator] Failed to start '{}': {}", only, e);
            std::process::exit(1);
        }
        // Wait for the single process
        tokio::select! {
            _ = signal::ctrl_c() => {
                tracing::info!("[orchestrator] Received Ctrl+C");
            }
        }
        manager.shutdown().await;
    } else {
        // Start all processes
        if let Err(e) = manager.start_all().await {
            tracing::error!("[orchestrator] Failed to start processes: {}", e);
            std::process::exit(1);
        }

        // Create a shutdown signal
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn Ctrl+C handler
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            tracing::info!("[orchestrator] Received Ctrl+C");
            let _ = shutdown_tx.send(());
        });

        // Run monitoring loop until shutdown
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    // Check for exited processes and restart if needed
                    manager.check_and_restart().await;
                }
            }
        }

        // Gracefully shutdown all child processes
        manager.shutdown().await;
    }
}
