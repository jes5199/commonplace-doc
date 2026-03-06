//! commonplace-branch: Manage workspace branches
//!
//! Creates, lists, and deletes branches in a commonplace repo.
//! Branches are directory entries within a repo container (created by
//! `commonplace init`). Creating a branch forks an existing branch's
//! directory doc and adds the new branch as an entry in the repo schema.
//!
//! The main sync agent discovers new branches automatically since they
//! are reachable from the root tree via the repo directory.

use clap::Parser;
use commonplace_doc::cli::{BranchArgs, BranchCommand};
use commonplace_doc::mqtt::{MqttClient, MqttConfig, MqttRequestClient};
use commonplace_doc::sync::client::push_schema_to_server;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = BranchArgs::parse();

    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    match args.command {
        Some(BranchCommand::Create { name, from }) => {
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
            create_branch(&request_client, &args.server, &directory, &name, &from).await?;

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
        eprintln!("Not a commonplace repo (no .commonplace.json found)");
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
        println!("Empty repo.");
    }

    Ok(())
}

async fn create_branch(
    request_client: &MqttRequestClient,
    server: &str,
    directory: &Path,
    name: &str,
    from: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Validate branch name
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        eprintln!("Invalid branch name: '{}'", name);
        std::process::exit(1);
    }

    let schema_path = directory.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace repo (no .commonplace.json found)");
        std::process::exit(1);
    }

    let schema_content = std::fs::read_to_string(&schema_path)?;
    let schema: serde_json::Value = serde_json::from_str(&schema_content)?;

    let entries = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.as_object())
        .ok_or("Invalid repo schema: missing root.entries")?;

    // Check that the target branch doesn't already exist
    if entries.contains_key(name) {
        eprintln!("Branch '{}' already exists.", name);
        std::process::exit(1);
    }

    // Find the source branch to fork from
    let source_entry = entries.get(from).ok_or_else(|| {
        format!(
            "Source branch '{}' not found. Available branches: {}",
            from,
            entries
                .keys()
                .filter(|k| {
                    entries
                        .get(*k)
                        .and_then(|e| e.get("type"))
                        .and_then(|t| t.as_str())
                        == Some("dir")
                })
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    })?;

    let source_uuid = source_entry
        .get("node_id")
        .and_then(|n| n.as_str())
        .ok_or_else(|| format!("Source branch '{}' has no node_id", from))?;

    println!(
        "Creating branch '{}' from '{}' ({})...",
        name,
        from,
        &source_uuid[..8.min(source_uuid.len())]
    );

    // Fork via MQTT command
    let fork_result = request_client.fork_document(source_uuid, None).await?;

    if let Some(error) = fork_result.error {
        eprintln!("Fork failed: {}", error);
        std::process::exit(1);
    }

    let new_id = fork_result
        .id
        .as_deref()
        .ok_or("Fork succeeded but no ID returned")?;

    println!(
        "Branch '{}' created (root: {})",
        name,
        &new_id[..8.min(new_id.len())]
    );

    // Find the repo's node_id so we can update its schema on the server
    let repo_node_id = find_repo_node_id(directory)?;

    // Update repo schema to include the new branch
    let repo_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                name: {
                    "type": "dir",
                    "node_id": new_id
                }
            }
        }
    });

    let http_client = reqwest::Client::new();
    push_schema_to_server(
        &http_client,
        server,
        &repo_node_id,
        &repo_schema.to_string(),
        "commonplace-branch",
    )
    .await
    .map_err(|e| format!("Failed to update repo schema: {}", e))?;

    println!(
        "Branch '{}' added to repo. The sync agent will discover it automatically.",
        name
    );

    Ok(())
}

/// Find the node_id for the repo directory by reading the parent schema.
///
/// Walks up from `repo_dir` to find the parent `.commonplace.json` that
/// contains this repo as an entry, then returns the repo's node_id.
fn find_repo_node_id(repo_dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
    // First check the sync metadata file which has the node_id directly
    let sync_path = repo_dir.join(".commonplace-sync.json");
    if sync_path.exists() {
        let sync_content = std::fs::read_to_string(&sync_path)?;
        let sync_json: serde_json::Value = serde_json::from_str(&sync_content)?;
        if let Some(node_id) = sync_json.get("node_id").and_then(|n| n.as_str()) {
            return Ok(node_id.to_string());
        }
    }

    // Fall back: look at parent directory's schema for this dir's node_id
    let repo_name = repo_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("Cannot determine repo directory name")?;

    let parent_dir = repo_dir
        .parent()
        .ok_or("Repo directory has no parent")?;

    let parent_schema_path = parent_dir.join(".commonplace.json");
    if !parent_schema_path.exists() {
        return Err(format!(
            "No .commonplace.json found in parent directory: {}",
            parent_dir.display()
        )
        .into());
    }

    let parent_schema_content = std::fs::read_to_string(&parent_schema_path)?;
    let parent_schema: serde_json::Value = serde_json::from_str(&parent_schema_content)?;

    let node_id = parent_schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.get(repo_name))
        .and_then(|entry| entry.get("node_id"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| {
            format!(
                "Cannot find node_id for '{}' in parent schema",
                repo_name
            )
        })?;

    Ok(node_id.to_string())
}

fn delete_branch(_directory: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Branch deletion not yet implemented (branch: '{}')", name);
    std::process::exit(1);
}
