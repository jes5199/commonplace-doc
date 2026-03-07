//! commonplace-branch: Manage workspace branches
//!
//! Creates, lists, and deletes branches in a commonplace repo.
//! Branches are directory entries within a repo container (created by
//! `commonplace init`). Creating a branch forks an existing branch's
//! directory doc and adds the new branch as an entry in the repo schema.
//!
//! All schema reads go through the server (not local files) so commands
//! work immediately after `commonplace init` without waiting for sync.

use clap::Parser;
use commonplace_doc::cli::{BranchArgs, BranchCommand};
use commonplace_doc::mqtt::{MqttClient, MqttConfig, MqttRequestClient};
use commonplace_doc::sync::client::{discover_fs_root, fetch_head, push_schema_to_server};
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
        Some(BranchCommand::Activate { name }) => {
            set_branch_sync(&args.server, &directory, &name, true).await?;
        }
        Some(BranchCommand::Deactivate { name }) => {
            set_branch_sync(&args.server, &directory, &name, false).await?;
        }
        None => {
            list_branches(&args.server, &directory).await?;
        }
    }

    Ok(())
}

async fn list_branches(server: &str, directory: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let (_, schema) = fetch_repo_schema_from_server(server, directory).await?;

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
                let sync_disabled = entry
                    .get("sync")
                    .and_then(|s| s.as_bool())
                    == Some(false);
                if sync_disabled {
                    println!("  {} (inactive)", name);
                } else {
                    println!("  {}", name);
                }
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

    let (repo_node_id, schema) = fetch_repo_schema_from_server(server, directory).await?;

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

    // Update repo schema to include the new branch
    let update_schema = serde_json::json!({
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
        &update_schema.to_string(),
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

/// Fetch the repo schema from the server by navigating root → repo.
///
/// Returns (repo_node_id, repo_schema_json). Determines the repo name
/// from the directory path, discovers the fs-root via HTTP, looks up
/// the repo entry in the root schema, then fetches the repo's own schema.
async fn fetch_repo_schema_from_server(
    server: &str,
    repo_dir: &Path,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    let repo_name = repo_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("Cannot determine repo directory name")?;

    let http_client = reqwest::Client::new();

    // Discover the fs-root UUID
    let root_id = discover_fs_root(&http_client, server)
        .await
        .map_err(|e| format!("Cannot discover fs-root: {}. Is the server running?", e))?;

    // Fetch root schema to find the repo's node_id
    let root_head = fetch_head(&http_client, server, &root_id, false)
        .await
        .map_err(|e| format!("Cannot read root schema: {}", e))?
        .ok_or("Root schema is empty")?;

    let root_schema: serde_json::Value = serde_json::from_str(&root_head.content)?;

    let repo_node_id = root_schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.get(repo_name))
        .and_then(|entry| entry.get("node_id"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| {
            format!(
                "Repo '{}' not found in root schema on server. Use 'commonplace init {}' first.",
                repo_name, repo_name
            )
        })?
        .to_string();

    // Fetch the repo's own schema
    let repo_head = fetch_head(&http_client, server, &repo_node_id, false)
        .await
        .map_err(|e| format!("Cannot read repo schema: {}", e))?
        .ok_or_else(|| format!("Repo '{}' schema is empty on server", repo_name))?;

    let repo_schema: serde_json::Value = serde_json::from_str(&repo_head.content)?;

    Ok((repo_node_id, repo_schema))
}

/// Set the `sync` flag on a branch entry in the repo schema.
///
/// When `sync` is false, the sync agent will skip this branch entirely
/// (sparse sync). When true (or absent), the branch syncs normally.
async fn set_branch_sync(
    server: &str,
    directory: &Path,
    name: &str,
    sync: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (repo_node_id, schema) = fetch_repo_schema_from_server(server, directory).await?;

    // Verify the branch exists
    let branch_entry = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.get(name))
        .ok_or_else(|| format!("Branch '{}' not found in repo schema", name))?;

    let node_id = branch_entry
        .get("node_id")
        .and_then(|n| n.as_str())
        .ok_or_else(|| format!("Branch '{}' has no node_id", name))?;

    // Build a partial schema update that sets the sync flag.
    // Always include sync explicitly — CRDT merge can't delete keys by omission.
    let entry = serde_json::json!({
        "type": "dir",
        "node_id": node_id,
        "sync": sync,
    });

    let update_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                name: entry
            }
        }
    });

    let http_client = reqwest::Client::new();
    push_schema_to_server(
        &http_client,
        server,
        &repo_node_id,
        &update_schema.to_string(),
        "commonplace-branch",
    )
    .await
    .map_err(|e| format!("Failed to update repo schema: {}", e))?;

    let action = if sync { "activated" } else { "deactivated" };
    println!("Branch '{}' {} (sync: {}).", name, action, sync);
    if !sync {
        println!("The sync agent will stop syncing this branch on next reconciliation.");
        println!("Local files are preserved. Use 'branch activate' to re-enable.");
    }

    Ok(())
}

fn delete_branch(_directory: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Branch deletion not yet implemented (branch: '{}')", name);
    std::process::exit(1);
}
