//! commonplace-orchestrator: Process supervisor for commonplace services
//!
//! Starts server and sync from commonplace.json, then recursively discovers all
//! __processes.json files and manages discovered processes with automatic restart.

use clap::Parser;
use commonplace_doc::cli::OrchestratorArgs;
use commonplace_doc::mqtt::{MqttClient, MqttConfig};
use commonplace_doc::orchestrator::{DiscoveredProcessManager, OrchestratorConfig, ProcessManager};
use commonplace_doc::sync::types::InitialSyncComplete;
use commonplace_doc::sync::{
    build_head_url, build_health_url, discover_fs_root, DiscoverFsRootError,
};
use commonplace_doc::DEFAULT_WORKSPACE;
use fs2::FileExt;
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use rumqttc::QoS;
use std::fs::File;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(unix))]
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
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

/// Config file change event
#[derive(Debug)]
enum ConfigEvent {
    Changed,
}

/// Task that watches the config file for changes.
/// Sends ConfigEvent::Changed when the file is modified.
async fn config_watcher_task(config_path: PathBuf, tx: mpsc::Sender<ConfigEvent>) {
    // Get the parent directory - we watch this to catch atomic renames
    let parent_dir = match config_path.parent() {
        Some(p) if p.as_os_str().is_empty() => PathBuf::from("."),
        Some(p) => p.to_path_buf(),
        None => {
            tracing::error!(
                "[orchestrator] Cannot watch config without parent directory: {}",
                config_path.display()
            );
            return;
        }
    };

    // Canonicalize the file path for reliable comparison
    let canonical_config_path = config_path
        .canonicalize()
        .unwrap_or_else(|_| config_path.clone());

    // Create a channel for notify events
    let (notify_tx, mut notify_rx) = mpsc::channel::<Result<Event, notify::Error>>(100);

    // Create watcher
    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.blocking_send(res);
        },
        NotifyConfig::default().with_poll_interval(Duration::from_millis(100)),
    ) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("[orchestrator] Failed to create config watcher: {}", e);
            return;
        }
    };

    // Watch the parent directory to catch atomic renames
    if let Err(e) = watcher.watch(&parent_dir, RecursiveMode::NonRecursive) {
        tracing::error!(
            "[orchestrator] Failed to watch config directory {}: {}",
            parent_dir.display(),
            e
        );
        return;
    }

    tracing::info!(
        "[orchestrator] Watching config file: {} (via parent dir: {})",
        config_path.display(),
        parent_dir.display()
    );

    // Debounce timer
    let debounce_duration = Duration::from_millis(500);
    let mut debounce_timer: Option<tokio::time::Instant> = None;

    // Helper to check if an event path matches our target file
    let matches_target = |event_path: &PathBuf| -> bool {
        if let Ok(canonical) = event_path.canonicalize() {
            if canonical == canonical_config_path {
                return true;
            }
        }
        event_path.file_name() == config_path.file_name()
            && event_path.parent() == config_path.parent()
    };

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        let affects_target = event.paths.iter().any(&matches_target);
                        if !affects_target {
                            continue;
                        }

                        // Handle modify, create, and rename events
                        let should_trigger = matches!(
                            event.kind,
                            EventKind::Modify(_) | EventKind::Create(_)
                        );

                        if should_trigger {
                            tracing::debug!("[orchestrator] Config file change detected: {:?}", event.kind);
                            debounce_timer = Some(tokio::time::Instant::now() + debounce_duration);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[orchestrator] Config watcher error: {}", e);
                    }
                }
            }
            _ = async {
                if let Some(deadline) = debounce_timer {
                    tokio::time::sleep_until(deadline).await;
                    true
                } else {
                    std::future::pending::<bool>().await
                }
            } => {
                debounce_timer = None;
                if tx.send(ConfigEvent::Changed).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Schema structure for parsing fs-root content
#[derive(serde::Deserialize)]
struct FsSchema {
    #[allow(dead_code)]
    version: Option<u32>,
    root: Option<SchemaRoot>,
}

#[derive(serde::Deserialize)]
struct SchemaRoot {
    entries: Option<serde_json::Value>,
    node_id: Option<String>,
}

/// Wait for sync to push initial content by polling the fs-root schema.
/// Returns when schema has valid entries, or times out after max_wait.
async fn wait_for_sync_initial_push(
    client: &reqwest::Client,
    server_url: &str,
    fs_root_id: &str,
    max_wait: Duration,
) -> bool {
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    let mut last_log = std::time::Instant::now();
    let log_interval = Duration::from_secs(5);

    tracing::info!(
        "[orchestrator] Waiting for sync to push initial content (timeout: {:?})...",
        max_wait
    );

    loop {
        // Check if we've exceeded max wait time
        if start.elapsed() >= max_wait {
            tracing::warn!(
                "[orchestrator] Timed out waiting for sync initial push after {:?}",
                max_wait
            );
            return false;
        }

        // Fetch fs-root HEAD
        let head_url = build_head_url(server_url, fs_root_id, false);
        match client.get(&head_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.text().await {
                    // Parse the response to check for schema entries
                    #[derive(serde::Deserialize)]
                    struct HeadResponse {
                        content: String,
                    }

                    if let Ok(head) = serde_json::from_str::<HeadResponse>(&body) {
                        if !head.content.is_empty() {
                            // Try to parse schema and check for entries
                            if let Ok(schema) = serde_json::from_str::<FsSchema>(&head.content) {
                                if let Some(root) = &schema.root {
                                    // Schema is valid if it has entries or a node_id
                                    let has_entries = root
                                        .entries
                                        .as_ref()
                                        .map(|e| e.as_object().is_some_and(|o| !o.is_empty()))
                                        .unwrap_or(false);
                                    let has_node_id = root.node_id.is_some();

                                    if has_entries || has_node_id {
                                        tracing::info!(
                                            "[orchestrator] Sync initial push complete (schema has entries), waited {:?}",
                                            start.elapsed()
                                        );
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(resp) => {
                tracing::debug!(
                    "[orchestrator] fs-root HEAD returned {}, waiting...",
                    resp.status()
                );
            }
            Err(e) => {
                tracing::debug!(
                    "[orchestrator] Failed to fetch fs-root HEAD: {}, waiting...",
                    e
                );
            }
        }

        // Log progress periodically
        if last_log.elapsed() >= log_interval {
            tracing::info!(
                "[orchestrator] Still waiting for sync initial push ({:.1}s elapsed)...",
                start.elapsed().as_secs_f64()
            );
            last_log = std::time::Instant::now();
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Wait for sync initial-complete event via MQTT subscription.
/// Returns the event data on success, or None on timeout.
async fn wait_for_sync_via_mqtt(
    mqtt_client: Arc<MqttClient>,
    fs_root_id: &str,
    max_wait: Duration,
) -> Option<InitialSyncComplete> {
    let topic = format!("{}/events/sync/initial-complete", fs_root_id);

    // Subscribe to the topic
    if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
        tracing::warn!(
            "[orchestrator] Failed to subscribe to sync-complete event: {}",
            e
        );
        return None;
    }

    tracing::info!(
        "[orchestrator] Subscribed to {} (timeout: {:?})",
        topic,
        max_wait
    );

    // Get message receiver
    let mut receiver = mqtt_client.subscribe_messages();

    // Wait for the event with timeout
    let result = tokio::time::timeout(max_wait, async {
        loop {
            match receiver.recv().await {
                Ok(msg) if msg.topic == topic => {
                    // Parse the event - crash on malformed (indicates bug)
                    let event: InitialSyncComplete = serde_json::from_slice(&msg.payload)
                        .expect("Malformed initial-sync-complete event from sync");
                    return Some(event);
                }
                Ok(_) => {
                    // Message for different topic, continue waiting
                    continue;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Missed some messages, continue
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::warn!("[orchestrator] MQTT message channel closed");
                    return None;
                }
            }
        }
    })
    .await;

    match result {
        Ok(Some(event)) => {
            tracing::info!(
                "[orchestrator] Received initial-sync-complete: {} files in {}ms",
                event.files_synced,
                event.duration_ms
            );
            Some(event)
        }
        Ok(None) => None,
        Err(_) => {
            tracing::warn!(
                "[orchestrator] Timed out waiting for sync-complete event after {:?}",
                max_wait
            );
            None
        }
    }
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

    // Acquire scoped lock based on config file path
    // This allows multiple orchestrators to run with different configs
    let lock_path = OrchestratorConfig::lock_file_path(&args.config);
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
            tracing::info!("[orchestrator] Acquired lock for config {:?}", args.config);
        }
        Err(e) => {
            tracing::error!(
                "[orchestrator] Another orchestrator is already running with config {:?}",
                args.config
            );
            tracing::error!("[orchestrator] Lock file: {:?}, error: {}", lock_path, e);
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

    // Start server and sync from commonplace.json, then discover processes recursively
    tracing::info!("[orchestrator] Server: {}", args.server);

    // First, start server and sync from commonplace.json using ProcessManager
    // This ensures the server is running before we try to discover processes
    let mut base_manager = ProcessManager::new(
        config.clone(),
        &args.config,
        args.mqtt_broker.clone(),
        args.disable.clone(),
    );

    // Handle --only mode: just run a single base process from config
    if let Some(only) = &args.only {
        tracing::info!("[orchestrator] Running only: {}", only);
        if let Err(e) = base_manager.spawn_process(only).await {
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
                    base_manager.check_and_restart().await;
                }
            }
        }

        base_manager.shutdown().await;
        return;
    }

    // Start server first (and any processes it depends on)
    if config.processes.contains_key("server") {
        tracing::info!("[orchestrator] Starting server from config...");
        if let Err(e) = base_manager.spawn_process("server").await {
            tracing::error!("[orchestrator] Failed to start server: {}", e);
            std::process::exit(1);
        }
    }

    // Wait for server to be healthy
    let client = reqwest::Client::new();
    let health_url = build_health_url(&args.server);
    tracing::info!("[orchestrator] Waiting for server to be healthy...");
    let mut attempts = 0;
    loop {
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("[orchestrator] Server is healthy");
                break;
            }
            _ => {
                attempts += 1;
                if attempts > 120 {
                    tracing::error!(
                        "[orchestrator] Server failed to become healthy after 120 attempts (60s)"
                    );
                    base_manager.shutdown().await;
                    std::process::exit(1);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    // Get fs-root ID from server (needed for sync wait polling)
    let fs_root_id = match discover_fs_root(&client, &args.server).await {
        Ok(id) => id,
        Err(DiscoverFsRootError::NotConfigured) => {
            tracing::error!("[orchestrator] Server was not started with --fs-root");
            base_manager.shutdown().await;
            std::process::exit(1);
        }
        Err(e) => {
            tracing::error!("[orchestrator] Failed to get fs-root: {}", e);
            base_manager.shutdown().await;
            std::process::exit(1);
        }
    };
    tracing::info!(
        "[orchestrator] Got fs-root ID: {}",
        &fs_root_id[..8.min(fs_root_id.len())]
    );

    // Connect to MQTT if configured (for sync-complete event subscription)
    // Use DEFAULT_WORKSPACE to match what sync client uses for MQTT topics
    let mqtt_client: Option<Arc<MqttClient>> = if !broker_raw.is_empty() {
        let mqtt_config = MqttConfig {
            broker_url: broker_raw.to_string(),
            client_id: format!("orchestrator-{}", std::process::id()),
            workspace: DEFAULT_WORKSPACE.to_string(),
            keep_alive_secs: 30,
            clean_session: true,
        };
        match MqttClient::connect(mqtt_config).await {
            Ok(client) => {
                let client = Arc::new(client);
                // Start event loop in background
                let client_for_loop = client.clone();
                tokio::spawn(async move {
                    if let Err(e) = client_for_loop.run_event_loop().await {
                        tracing::error!("[orchestrator] MQTT event loop error: {}", e);
                    }
                });
                tracing::info!("[orchestrator] Connected to MQTT broker: {}", broker_raw);
                Some(client)
            }
            Err(e) => {
                tracing::warn!(
                    "[orchestrator] Failed to connect to MQTT ({}), will poll instead: {}",
                    broker_raw,
                    e
                );
                None
            }
        }
    } else {
        None
    };

    // Start sync if configured (to push initial content to server)
    if config.processes.contains_key("sync") {
        tracing::info!("[orchestrator] Starting sync from config...");
        if let Err(e) = base_manager.spawn_process("sync").await {
            tracing::error!("[orchestrator] Failed to start sync: {}", e);
            base_manager.shutdown().await;
            std::process::exit(1);
        }

        // Wait for sync to complete initial push
        // Use MQTT subscription if available, otherwise fall back to polling
        let sync_ready = if let Some(ref mqtt) = mqtt_client {
            wait_for_sync_via_mqtt(mqtt.clone(), &fs_root_id, Duration::from_secs(120))
                .await
                .is_some()
        } else {
            wait_for_sync_initial_push(&client, &args.server, &fs_root_id, Duration::from_secs(120))
                .await
        };

        if !sync_ready {
            tracing::warn!(
                "[orchestrator] Sync initial push timed out, continuing anyway. \
                Discovery may find empty __processes.json files."
            );
        }
    }

    // Start remaining processes from commonplace.json (e.g., additional syncs)
    for name in config.processes.keys() {
        if name != "server" && name != "sync" {
            tracing::info!("[orchestrator] Starting {} from config...", name);
            if let Err(e) = base_manager.spawn_process(name).await {
                tracing::warn!("[orchestrator] Failed to start '{}': {}", name, e);
                // Continue with other processes, don't exit
            }
        }
    }

    // Now we can start recursive discovery
    let mut discovered_manager = DiscoveredProcessManager::new(
        broker_raw.to_string(),
        args.server.clone(),
        base_manager.status_file_path().to_path_buf(),
    );

    // Set up MQTT client for discovered process manager if available
    if let Some(ref mqtt) = mqtt_client {
        discovered_manager.set_mqtt_client(mqtt.clone(), DEFAULT_WORKSPACE.to_string());
    }

    tracing::info!(
        "[orchestrator] Starting recursive discovery with fs-root: {}",
        &fs_root_id[..8.min(fs_root_id.len())]
    );

    // Start config file watcher
    let (config_tx, mut config_rx) = mpsc::channel::<ConfigEvent>(10);
    let config_path_for_watcher = args.config.clone();
    tokio::spawn(async move {
        config_watcher_task(config_path_for_watcher, config_tx).await;
    });

    // Keep a copy of the config path for reloading
    let config_path = args.config.clone();

    // Run the recursive watcher with shutdown handling
    // Also monitor base processes (server, sync) for restarts
    // And handle config file changes
    let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));
    tokio::select! {
        result = discovered_manager.run_with_recursive_watch(&client, &fs_root_id) => {
            if let Err(e) = result {
                tracing::error!("[orchestrator] Recursive watch failed: {}", e);
            }
        }
        _ = async {
            loop {
                tokio::select! {
                    _ = monitor_interval.tick() => {
                        base_manager.check_and_restart().await;
                    }
                    Some(ConfigEvent::Changed) = config_rx.recv() => {
                        tracing::info!("[orchestrator] Config file changed, reloading...");
                        match OrchestratorConfig::load(&config_path) {
                            Ok(new_config) => {
                                base_manager.reload_config(new_config).await;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "[orchestrator] Failed to reload config: {}",
                                    e
                                );
                            }
                        }
                    }
                }
            }
        } => {}
        _ = wait_for_shutdown_signal() => {
            tracing::info!("[orchestrator] Shutting down...");
        }
    }

    discovered_manager.shutdown().await;
    base_manager.shutdown().await;
}
