//! commonplace-branch: Manage workspace branches
//!
//! Creates, lists, and deletes branches in a commonplace workspace.
//! Uses MQTT commands to fork documents and manage branches.
//! After forking, registers a sync agent in `__processes.json` so the
//! orchestrator auto-starts sync for the new branch.

use clap::Parser;
use commonplace_doc::cli::{BranchArgs, BranchCommand};
use commonplace_doc::mqtt::{MqttClient, MqttConfig, MqttRequestClient};
use commonplace_doc::orchestrator::ProcessesConfig;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = BranchArgs::parse();

    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    match args.command {
        Some(BranchCommand::Create { name }) => {
            // Connect to MQTT for fork operation
            let config = MqttConfig {
                broker_url: args.mqtt_broker.clone(),
                client_id: format!("commonplace-branch-{}", uuid::Uuid::new_v4()),
                workspace: args.workspace.clone(),
                ..Default::default()
            };

            let client = Arc::new(MqttClient::connect(config).await?);
            let client_for_loop = client.clone();
            let loop_handle = tokio::spawn(async move {
                let _ = client_for_loop.run_event_loop().await;
            });

            tokio::time::sleep(Duration::from_millis(200)).await;

            let request_client =
                MqttRequestClient::new(client.clone(), args.workspace.clone()).await?;
            create_branch(&request_client, &directory, &name).await?;

            loop_handle.abort();
        }
        Some(BranchCommand::Delete { name }) => {
            delete_branch(&directory, &name)?;
        }
        None => {
            list_branches(&directory)?;
        }
    }

    Ok(())
}

fn list_branches(directory: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema_path = directory.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    let schema_content = std::fs::read_to_string(&schema_path)?;
    let schema: serde_json::Value = serde_json::from_str(&schema_content)?;

    let entries = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.as_object());

    if let Some(entries) = entries {
        let mut found_branches = false;
        for (name, entry) in entries {
            let is_dir = entry
                .get("type")
                .and_then(|t| t.as_str())
                .map(|t| t == "dir")
                .unwrap_or(false);

            if is_dir {
                println!("  {}", name);
                found_branches = true;
            }
        }
        if !found_branches {
            println!("No branches found. Use 'commonplace branch create <name>' to create one.");
        }
    } else {
        println!("Empty workspace.");
    }

    Ok(())
}

async fn create_branch(
    request_client: &MqttRequestClient,
    directory: &Path,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Validate branch name
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        eprintln!("Invalid branch name: '{}'", name);
        std::process::exit(1);
    }

    let schema_path = directory.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    // Find the root directory UUID from sync state
    let sync_path = directory.join(".commonplace-sync.json");
    if !sync_path.exists() {
        eprintln!("Workspace not synced (no .commonplace-sync.json found)");
        std::process::exit(1);
    }

    let sync_content = std::fs::read_to_string(&sync_path)?;
    let sync_state: serde_json::Value = serde_json::from_str(&sync_content)?;

    let root_uuid = sync_state
        .get("document_id")
        .and_then(|d| d.as_str())
        .ok_or("Cannot find root document UUID in sync state")?;

    println!(
        "Creating branch '{}' from root {}...",
        name,
        &root_uuid[..8.min(root_uuid.len())]
    );

    // Fork via MQTT command
    let fork_result = request_client.fork_document(root_uuid, None).await?;

    if let Some(error) = fork_result.error {
        eprintln!("Fork failed: {}", error);
        std::process::exit(1);
    }

    let new_id = fork_result.id.as_deref().unwrap_or("unknown");
    println!(
        "Branch '{}' created (root: {})",
        name,
        &new_id[..8.min(new_id.len())]
    );

    // Register sync agent in __processes.json
    let branch_dir = directory.join("branches").join(name);
    register_sync_agent(directory, name, new_id, &branch_dir)?;

    println!(
        "Sync agent registered. Branch will sync to: {}",
        branch_dir.display()
    );

    Ok(())
}

/// Register a sync agent entry in `__processes.json` for the new branch.
///
/// The orchestrator will detect the change and auto-start a commonplace-sync
/// process pointing at the forked root UUID.
fn register_sync_agent(
    workspace_dir: &Path,
    branch_name: &str,
    forked_root_uuid: &str,
    branch_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let processes_path = workspace_dir.join("__processes.json");

    // Load existing config or start fresh
    let mut config_json: serde_json::Value = if processes_path.exists() {
        let content = std::fs::read_to_string(&processes_path)?;
        serde_json::from_str(&content)?
    } else {
        serde_json::json!({})
    };

    // Find the commonplace-sync binary (same directory as this executable)
    let sync_binary = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("commonplace-sync")))
        .unwrap_or_else(|| std::path::PathBuf::from("commonplace-sync"));

    let sync_binary_str = sync_binary.to_string_lossy().to_string();
    let branch_dir_str = branch_dir.to_string_lossy().to_string();

    // Build the sync agent entry
    let process_name = format!("branch-sync-{}", branch_name);
    let entry = serde_json::json!({
        "comment": format!("Sync agent for branch '{}' (forked from root)", branch_name),
        "command": [
            sync_binary_str,
            "--server", "http://localhost:5199",
            "--node", forked_root_uuid,
            "--directory", branch_dir_str,
            "--initial-sync", "remote"
        ],
        "cwd": workspace_dir.to_string_lossy()
    });

    // Handle both legacy and new format
    if let Some(obj) = config_json.as_object_mut() {
        if obj.contains_key("processes") {
            // Legacy format
            if let Some(processes) = obj
                .get_mut("processes")
                .and_then(|p| p.as_object_mut())
            {
                processes.insert(process_name, entry);
            }
        } else {
            // New format — but we need to be careful not to break existing entries
            obj.insert(process_name, entry);
        }
    }

    // Create branch directory if it doesn't exist
    std::fs::create_dir_all(branch_dir)?;

    // Write the updated config
    let updated = serde_json::to_string_pretty(&config_json)?;
    std::fs::write(&processes_path, format!("{}\n", updated))?;

    // Verify the config is parseable
    ProcessesConfig::parse(&updated).map_err(|e| {
        format!(
            "Generated invalid __processes.json: {}. File saved but may not be picked up.",
            e
        )
    })?;

    Ok(())
}

fn delete_branch(_directory: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Branch deletion not yet implemented (branch: '{}')", name);
    std::process::exit(1);
}
