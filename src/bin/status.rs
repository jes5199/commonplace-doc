//! commonplace-status: Show workspace sync status
//!
//! Queries the server via MQTT to show current workspace state.
//! Shows branch info, sync status, and directory listing.

use clap::Parser;
use commonplace_doc::cli::StatusArgs;
use commonplace_doc::mqtt::{MqttClient, MqttConfig, MqttRequestClient};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = StatusArgs::parse();

    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    // Read .commonplace.json to find the workspace root UUID
    let schema_path = directory.join(".commonplace.json");
    let sync_state_path = directory.join(".commonplace-sync.json");

    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    let schema_content = std::fs::read_to_string(&schema_path)?;
    let schema: serde_json::Value = serde_json::from_str(&schema_content)?;

    // Connect to MQTT broker
    let config = MqttConfig {
        broker_url: args.mqtt_broker.clone(),
        client_id: format!("commonplace-status-{}", uuid::Uuid::new_v4()),
        workspace: args.workspace.clone(),
        ..Default::default()
    };

    let client = Arc::new(MqttClient::connect(config).await?);
    let client_for_loop = client.clone();
    let loop_handle = tokio::spawn(async move {
        let _ = client_for_loop.run_event_loop().await;
    });

    // Brief delay for connection establishment
    tokio::time::sleep(Duration::from_millis(200)).await;

    let request_client = MqttRequestClient::new(client.clone(), args.workspace.clone()).await?;

    if args.json {
        let status =
            build_status_json(&request_client, &args.mqtt_broker, &directory, &schema).await?;
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print_status(
            &request_client,
            &args.mqtt_broker,
            &directory,
            &schema,
            &sync_state_path,
        )
        .await?;
    }

    loop_handle.abort();
    Ok(())
}

async fn print_status(
    request_client: &MqttRequestClient,
    broker: &str,
    directory: &Path,
    schema: &serde_json::Value,
    sync_state_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Workspace: {}", directory.display());
    println!("Broker:    {}", broker);

    // Check server connectivity by trying to discover fs-root
    match request_client.discover_fs_root().await {
        Ok(root_id) => {
            println!("Server:    connected (fs-root: {})", &root_id[..8.min(root_id.len())]);
        }
        Err(_) => {
            println!("Server:    disconnected (no fs-root response)");
        }
    }

    // Count entries in schema
    let entry_count = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.as_object())
        .map(|entries| entries.len())
        .unwrap_or(0);

    println!("Entries:   {}", entry_count);

    // Check sync state
    if sync_state_path.exists() {
        let sync_content = std::fs::read_to_string(sync_state_path)?;
        let sync_state: serde_json::Value = serde_json::from_str(&sync_content)?;

        if let Some(last_sync) = sync_state.get("last_sync_timestamp") {
            println!("Last sync: {}", last_sync);
        }
    } else {
        println!("Sync:      not initialized");
    }

    // List branches if in a repo structure
    list_branches(schema)?;

    Ok(())
}

fn list_branches(schema: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let entries = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.as_object());

    if let Some(entries) = entries {
        let branches: Vec<&String> = entries
            .iter()
            .filter(|(_, v)| {
                v.get("type")
                    .and_then(|t| t.as_str())
                    .map(|t| t == "dir")
                    .unwrap_or(false)
            })
            .map(|(name, _)| name)
            .collect();

        if !branches.is_empty() {
            println!("\nDirectories:");
            for branch in &branches {
                println!("  {}/", branch);
            }
        }
    }

    Ok(())
}

async fn build_status_json(
    request_client: &MqttRequestClient,
    broker: &str,
    directory: &Path,
    schema: &serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let server_connected = request_client.discover_fs_root().await.is_ok();

    let entry_count = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.as_object())
        .map(|entries| entries.len())
        .unwrap_or(0);

    Ok(serde_json::json!({
        "workspace": directory.display().to_string(),
        "broker": broker,
        "server_connected": server_connected,
        "entry_count": entry_count,
    }))
}
