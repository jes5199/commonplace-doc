//! commonplace-branch: Manage workspace branches
//!
//! Creates, lists, and deletes branches in a commonplace workspace.
//! Uses the directory fork API to deep-clone branch content.

use clap::Parser;
use commonplace_doc::cli::{BranchArgs, BranchCommand};
use reqwest::blocking::Client;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = BranchArgs::parse();

    let client = Client::new();
    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    match args.command {
        Some(BranchCommand::Create { name }) => {
            create_branch(&client, &args.server, &directory, &name)?;
        }
        Some(BranchCommand::Delete { name }) => {
            delete_branch(&client, &args.server, &directory, &name)?;
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
                // Check if directory has __branch.json by looking at its schema
                let node_id = entry.get("node_id").and_then(|n| n.as_str());
                if let Some(_node_id) = node_id {
                    // TODO: fetch the subdirectory schema and check for __branch.json
                    println!("  {}", name);
                    found_branches = true;
                }
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

fn create_branch(
    client: &Client,
    server: &str,
    directory: &Path,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Validate branch name (no slashes, not empty)
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        eprintln!("Invalid branch name: '{}'", name);
        std::process::exit(1);
    }

    let schema_path = directory.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    let _schema_content = std::fs::read_to_string(&schema_path)?;
    let _schema: serde_json::Value = serde_json::from_str(&_schema_content)?;

    // Find the root directory UUID to fork
    // For now, we need to find the sync state which has the root UUID
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

    println!("Creating branch '{}' from root {}...", name, &root_uuid[..8.min(root_uuid.len())]);

    // Call the fork API
    let fork_url = format!("{}/docs/{}/fork", server, root_uuid);
    let resp = client.post(&fork_url).send()?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        eprintln!("Fork failed ({}): {}", status, body);
        std::process::exit(1);
    }

    let fork_result: serde_json::Value = resp.json()?;
    let new_id = fork_result
        .get("id")
        .and_then(|i| i.as_str())
        .unwrap_or("unknown");

    println!("Branch '{}' created (root: {})", name, &new_id[..8.min(new_id.len())]);

    if let Some(manifest) = fork_result.get("manifest") {
        let doc_count = manifest
            .get("document_map")
            .and_then(|d| d.as_object())
            .map(|m| m.len())
            .unwrap_or(0);
        println!("  {} documents forked", doc_count);
    }

    Ok(())
}

fn delete_branch(
    _client: &Client,
    _server: &str,
    _directory: &Path,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Branch deletion not yet implemented (branch: '{}')", name);
    std::process::exit(1);
}
