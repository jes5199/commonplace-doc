//! commonplace-status: Show workspace sync status
//!
//! Queries the sync agent and server to show current workspace state.
//! Shows branch info, sync status, and pending changes.

use clap::Parser;
use commonplace_doc::cli::StatusArgs;
use reqwest::blocking::Client;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = StatusArgs::parse();

    let client = Client::new();
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

    // Find workspace UUID from schema
    let _workspace_uuid = schema
        .as_object()
        .and_then(|obj| {
            // Look for the root node_id or document_id
            obj.get("root")
                .and_then(|r| r.get("node_id"))
                .and_then(|n| n.as_str())
        })
        .or({
            // Try sync state for the root UUID
            None
        });

    if args.json {
        let status = build_status_json(&client, &args.server, &directory, &schema)?;
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print_status(&client, &args.server, &directory, &schema, &sync_state_path)?;
    }

    Ok(())
}

fn print_status(
    client: &Client,
    server: &str,
    directory: &Path,
    schema: &serde_json::Value,
    sync_state_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Workspace: {}", directory.display());
    println!("Server:    {}", server);

    // Check server health
    match client.get(format!("{}/health", server)).send() {
        Ok(resp) if resp.status().is_success() => {
            println!("Server:    connected");
        }
        _ => {
            println!("Server:    disconnected");
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
        // Look for directories that contain __branch.json
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

fn build_status_json(
    client: &Client,
    server: &str,
    directory: &Path,
    schema: &serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let server_connected = client
        .get(format!("{}/health", server))
        .send()
        .map(|r| r.status().is_success())
        .unwrap_or(false);

    let entry_count = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.as_object())
        .map(|entries| entries.len())
        .unwrap_or(0);

    Ok(serde_json::json!({
        "workspace": directory.display().to_string(),
        "server": server,
        "server_connected": server_connected,
        "entry_count": entry_count,
    }))
}
