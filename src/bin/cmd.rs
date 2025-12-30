//! commonplace-cmd: Send commands to document paths via MQTT
//!
//! Usage:
//!   # Explicit server path
//!   commonplace-cmd examples/counter.json increment
//!   commonplace-cmd examples/counter.json increment --payload '{"amount": 5}'
//!
//!   # Relative path inside synced directory (uses state file to resolve)
//!   cd /tmp/my-sync-dir && commonplace-cmd ./counter.json increment
//!
//!   # With COMMONPLACE_PATH env var (set by orchestrator)
//!   COMMONPLACE_PATH=examples/counter.json commonplace-cmd increment

use clap::Parser;
use commonplace_doc::{
    cli::CmdArgs,
    mqtt::{CommandMessage, MqttClient, MqttConfig, Topic},
    sync::state_file::SyncStateFile,
};
use rumqttc::QoS;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Schema filename used to identify synced directories
const SCHEMA_FILENAME: &str = ".commonplace.json";

/// Resolve a path argument to a commonplace document path.
///
/// Resolution order:
/// 1. If COMMONPLACE_PATH env var is set, use it
/// 2. If path looks like filesystem path and we're in a synced directory,
///    resolve to server path using state file
/// 3. Otherwise, use the path as-is (assumed to be server path)
async fn resolve_path(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Check COMMONPLACE_PATH environment variable first
    if let Ok(env_path) = std::env::var("COMMONPLACE_PATH") {
        return Ok(env_path);
    }

    // Check if this looks like a filesystem path
    let fs_path = Path::new(path);
    let is_fs_path = path.starts_with("./")
        || path.starts_with("../")
        || path.starts_with('/')
        || fs_path.exists();

    if !is_fs_path {
        // Treat as server path directly
        return Ok(path.to_string());
    }

    // Try to find synced directory root and resolve path
    let absolute_path = if fs_path.is_absolute() {
        fs_path.to_path_buf()
    } else {
        std::env::current_dir()?.join(fs_path)
    };

    let canonical_path = absolute_path
        .canonicalize()
        .unwrap_or_else(|_| absolute_path.clone());

    // Search for sync root by looking for .commonplace.json in parent directories
    if let Some((sync_root, state_file_path)) = find_sync_root(&canonical_path)? {
        // Load the state file to get node_id
        let state = SyncStateFile::load(&state_file_path)
            .await?
            .ok_or_else(|| format!("State file not found: {}", state_file_path.display()))?;

        // Calculate relative path from sync root to target
        let relative_path = canonical_path
            .strip_prefix(&sync_root)
            .map_err(|_| "Path is not inside sync root")?;

        // Normalize to forward slashes
        let relative_str = relative_path.to_string_lossy().replace('\\', "/");

        // Build full path: node_id:relative_path
        let full_path = if relative_str.is_empty() {
            state.node_id.clone()
        } else {
            format!("{}:{}", state.node_id, relative_str)
        };

        return Ok(full_path);
    }

    // Fallback: use COMMONPLACE_NODE if available
    if let Ok(node_id) = std::env::var("COMMONPLACE_NODE") {
        // Treat path as relative within that node
        let relative = path.trim_start_matches("./");
        return Ok(format!("{}:{}", node_id, relative));
    }

    // No resolution possible, use as-is
    Ok(path.to_string())
}

/// Find the sync root directory by searching for .commonplace.json upward.
/// Returns (sync_root_dir, state_file_path) if found.
fn find_sync_root(start: &Path) -> Result<Option<(PathBuf, PathBuf)>, Box<dyn std::error::Error>> {
    // Start from the file's directory (or the path itself if it's a dir)
    let search_start = if start.is_file() {
        start.parent().map(|p| p.to_path_buf())
    } else {
        Some(start.to_path_buf())
    };

    let mut current = match search_start {
        Some(p) => p,
        None => return Ok(None),
    };

    loop {
        // Check if this directory contains .commonplace.json
        let schema_path = current.join(SCHEMA_FILENAME);
        if schema_path.exists() {
            // Found sync root! Now find the corresponding state file
            let state_file_path = SyncStateFile::state_file_path(&current);
            if state_file_path.exists() {
                return Ok(Some((current, state_file_path)));
            }
            // Schema exists but no state file - might be freshly synced
            // Continue searching upward in case this is a nested sync
        }

        // Move to parent directory
        match current.parent() {
            Some(parent) if parent != current => {
                current = parent.to_path_buf();
            }
            _ => break,
        }
    }

    Ok(None)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CmdArgs::parse();

    // Resolve the path (handles relative paths in synced directories)
    let resolved_path = resolve_path(&args.path).await?;

    // Parse the payload JSON
    let payload: serde_json::Value =
        serde_json::from_str(&args.payload).map_err(|e| format!("Invalid JSON payload: {}", e))?;

    // Build the command message
    let message = CommandMessage {
        payload,
        source: Some(args.source.clone()),
    };

    // Build the topic
    let topic = Topic::commands(&resolved_path, &args.verb);
    let topic_str = topic.to_topic_string();

    // Connect to MQTT
    let config = MqttConfig {
        broker_url: args.mqtt_broker.clone(),
        client_id: format!("commonplace-cmd-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };

    let client = MqttClient::connect(config).await?;

    // Need to poll the event loop once to establish connection
    // Spawn the event loop briefly
    let client_for_loop = std::sync::Arc::new(client);
    let client_clone = client_for_loop.clone();

    let loop_handle = tokio::spawn(async move {
        // Run for a short time to establish connection and send message
        let _ = tokio::time::timeout(Duration::from_secs(2), client_clone.run_event_loop()).await;
    });

    // Give the connection time to establish (500ms is generous for localhost)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish the command
    let payload_bytes = serde_json::to_vec(&message)?;
    client_for_loop
        .publish(&topic_str, &payload_bytes, QoS::AtLeastOnce)
        .await?;

    // Show resolved path if different from input
    if resolved_path != args.path {
        println!(
            "Sent {} to {} (resolved from {})",
            args.verb, resolved_path, args.path
        );
    } else {
        println!("Sent {} to {}", args.verb, resolved_path);
    }

    // Wait for PUBACK confirmation (500ms for QoS1 delivery)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel the event loop
    loop_handle.abort();

    Ok(())
}
