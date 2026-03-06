//! commonplace-checkout: Switch active branch (flat-branch model)
//!
//! Sends a re-root MQTT command to the sync agent, causing it to
//! switch from the current branch's root UUID to the target branch's.
//!
//! Note: With the repo/branch layout (`commonplace init`), checkout is
//! typically unnecessary — all branches are materialized simultaneously
//! as subdirectories. This command is primarily for the flat-branch model
//! where branches are separate sync agents.

use clap::Parser;
use commonplace_doc::cli::CheckoutArgs;
use commonplace_doc::mqtt::{MqttClient, MqttConfig};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CheckoutArgs::parse();

    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    let schema_path = directory.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    // Find the branch's root UUID from the schema
    let schema_content = std::fs::read_to_string(&schema_path)?;
    let schema: serde_json::Value = serde_json::from_str(&schema_content)?;

    let branch_entry = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.get(&args.branch))
        .ok_or_else(|| {
            format!(
                "Branch '{}' not found in schema. Use 'commonplace branch' to list branches.",
                args.branch
            )
        })?;

    // Verify the entry is a directory (branch), not a file
    let entry_type = branch_entry.get("type").and_then(|t| t.as_str()).unwrap_or("file");
    if entry_type != "dir" {
        return Err(format!(
            "'{}' is a {} entry, not a branch. Use 'commonplace branch' to list branches.",
            args.branch, entry_type
        ).into());
    }

    let branch_uuid = branch_entry
        .get("node_id")
        .and_then(|n| n.as_str())
        .ok_or_else(|| format!("Branch '{}' has no node_id in schema", args.branch))?;

    let sync_name = &args.sync_name;

    println!(
        "Checking out branch '{}' (root: {})...",
        args.branch,
        &branch_uuid[..8.min(branch_uuid.len())]
    );

    // Connect to MQTT and send re-root command
    let config = MqttConfig {
        broker_url: args.mqtt_broker.clone(),
        client_id: format!("commonplace-checkout-{}", uuid::Uuid::new_v4()),
        workspace: args.workspace.clone(),
        ..Default::default()
    };

    let client = Arc::new(MqttClient::connect(config).await?);
    let client_for_loop = client.clone();
    let loop_handle = tokio::spawn(async move {
        let _ = client_for_loop.run_event_loop().await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish re-root command
    let topic = format!(
        "{}/commands/__sync/{}/re-root",
        args.workspace, sync_name
    );
    let payload = serde_json::json!({
        "root_uuid": branch_uuid,
        "branch": args.branch,
    });
    let payload_bytes = serde_json::to_vec(&payload)?;

    client
        .publish(&topic, &payload_bytes, QoS::AtLeastOnce)
        .await?;

    // Wait for PUBACK delivery
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!(
        "Sent re-root command to sync agent '{}'. Branch '{}' is now active.",
        sync_name, args.branch
    );

    loop_handle.abort();
    Ok(())
}
