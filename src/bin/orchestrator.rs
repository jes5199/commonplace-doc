//! commonplace-orchestrator: Process supervisor for commonplace services
//!
//! Starts and manages child processes (store, http) with automatic restart on failure.
//! Supports dynamic process management via --watch-processes flag.

use clap::Parser;
use commonplace_doc::cli::OrchestratorArgs;
use commonplace_doc::orchestrator::{DiscoveredProcessManager, OrchestratorConfig, ProcessManager};
use fs2::FileExt;
use std::fs::File;
use std::net::TcpStream;
use std::time::Duration;
#[cfg(not(unix))]
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Wait for either SIGINT (Ctrl+C) or SIGTERM.
/// Returns when either signal is received.
#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    let mut sigint =
        unix_signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");
    let mut sigterm =
        unix_signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");

    tokio::select! {
        _ = sigint.recv() => {
            tracing::info!("[orchestrator] Received SIGINT (Ctrl+C)");
        }
        _ = sigterm.recv() => {
            tracing::info!("[orchestrator] Received SIGTERM");
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("Failed to register Ctrl+C handler");
    tracing::info!("[orchestrator] Received Ctrl+C");
}

#[tokio::main]
async fn main() {
    let args = OrchestratorArgs::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("[orchestrator] Starting commonplace-orchestrator");
    tracing::info!("[orchestrator] Config file: {:?}", args.config);

    // Acquire global lock to prevent multiple orchestrators from running
    let lock_path = std::env::temp_dir().join("commonplace-orchestrator.lock");
    let _lock_file = match File::create(&lock_path) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(
                "[orchestrator] Failed to create lock file at {:?}: {}",
                lock_path,
                e
            );
            std::process::exit(1);
        }
    };

    match _lock_file.try_lock_exclusive() {
        Ok(()) => {
            tracing::info!("[orchestrator] Acquired global lock");
        }
        Err(e) => {
            tracing::error!(
                "[orchestrator] Another orchestrator is already running (lock file: {:?}): {}",
                lock_path,
                e
            );
            tracing::error!("[orchestrator] Only one orchestrator instance can run at a time");
            std::process::exit(1);
        }
    }

    // Keep _lock_file alive for the duration of the program
    // The lock is released automatically when the file is dropped (on exit)

    let config = match OrchestratorConfig::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("[orchestrator] Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    let broker_raw = args.mqtt_broker.as_ref().unwrap_or(&config.mqtt_broker);

    // Strip mqtt:// or tcp:// scheme if present (ToSocketAddrs only handles host:port)
    let broker = broker_raw
        .strip_prefix("mqtt://")
        .or_else(|| broker_raw.strip_prefix("tcp://"))
        .unwrap_or(broker_raw);

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

    // Handle --recursive mode (discover all processes.json files from fs-root)
    if args.recursive {
        tracing::info!("[orchestrator] Recursive discovery mode");
        tracing::info!("[orchestrator] Server: {}", args.server);

        let mut discovered_manager =
            DiscoveredProcessManager::new(broker_raw.to_string(), args.server.clone());

        let client = reqwest::Client::new();

        // Get fs-root ID from server
        let fs_root_url = format!("{}/fs-root", args.server);
        let fs_root_id = match client.get(&fs_root_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                #[derive(serde::Deserialize)]
                struct FsRootResponse {
                    id: String,
                }
                match resp.json::<FsRootResponse>().await {
                    Ok(r) => r.id,
                    Err(e) => {
                        tracing::error!("[orchestrator] Failed to parse fs-root response: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            Ok(resp) => {
                tracing::error!(
                    "[orchestrator] Failed to get fs-root: HTTP {}",
                    resp.status()
                );
                tracing::error!("[orchestrator] Make sure server was started with --fs-root");
                std::process::exit(1);
            }
            Err(e) => {
                tracing::error!("[orchestrator] Failed to connect to server: {}", e);
                std::process::exit(1);
            }
        };

        tracing::info!("[orchestrator] Using fs-root: {}", fs_root_id);

        // Run the recursive watcher with shutdown handling
        tokio::select! {
            result = discovered_manager.run_with_recursive_watch(&client, &fs_root_id) => {
                if let Err(e) = result {
                    tracing::error!("[orchestrator] Recursive watch failed: {}", e);
                }
            }
            _ = wait_for_shutdown_signal() => {
                tracing::info!("[orchestrator] Shutting down...");
            }
        }

        discovered_manager.shutdown().await;
        return;
    }

    // Handle --watch-processes mode (dynamic process management)
    if let Some(ref doc_path) = args.watch_processes {
        tracing::info!("[orchestrator] Dynamic process mode: watching {}", doc_path);
        tracing::info!("[orchestrator] Server: {}", args.server);

        let mut discovered_manager =
            DiscoveredProcessManager::new(broker_raw.to_string(), args.server.clone());

        let client = reqwest::Client::new();

        // Run the document watcher with shutdown handling
        tokio::select! {
            result = discovered_manager.run_with_document_watch(&client, doc_path, args.use_paths) => {
                if let Err(e) = result {
                    tracing::error!("[orchestrator] Document watch failed: {}", e);
                }
            }
            _ = wait_for_shutdown_signal() => {
                tracing::info!("[orchestrator] Shutting down...");
            }
        }

        discovered_manager.shutdown().await;
        return;
    }

    let mut manager = ProcessManager::new(config, args.mqtt_broker.clone(), args.disable.clone());

    if let Some(only) = &args.only {
        tracing::info!("[orchestrator] Running only: {}", only);
        if let Err(e) = manager.spawn_process(only).await {
            tracing::error!("[orchestrator] Failed to start '{}': {}", only, e);
            std::process::exit(1);
        }

        // Monitor the single process with restart support until shutdown signal
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = wait_for_shutdown_signal() => {
                    break;
                }
                _ = monitor_interval.tick() => {
                    manager.check_and_restart().await;
                }
            }
        }

        manager.shutdown().await;
    } else {
        // Start all processes
        if let Err(e) = manager.start_all().await {
            tracing::error!("[orchestrator] Failed to start processes: {}", e);
            std::process::exit(1);
        }

        // Run monitoring loop until shutdown signal
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = wait_for_shutdown_signal() => {
                    break;
                }
                _ = monitor_interval.tick() => {
                    // Check for exited processes and restart if needed
                    manager.check_and_restart().await;
                }
            }
        }

        // Gracefully shutdown all child processes
        manager.shutdown().await;
    }
}
