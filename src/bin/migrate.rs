//! One-time migration: nest existing workspace under repo/main layout.
//!
//! Before: fs-root → { bartleby: ..., text-to-telegram: ..., ... }
//! After:  fs-root → { workspace: { node_id: REPO } }
//!         REPO    → { main: { node_id: OLD_FS_ROOT } }
//!         OLD_FS_ROOT → { bartleby: ..., text-to-telegram: ..., ... }
//!
//! The sync client changes from `--node workspace` to `--node workspace/main`
//! but the physical directory layout stays the same.
//!
//! Run with the server running:
//!   commonplace-migrate --server http://localhost:5199 --database ./data/commonplace.redb

use clap::Parser;
use commonplace_doc::store::CommitStore;
use commonplace_doc::sync::client::{discover_fs_root, push_schema_to_server};
use reqwest::Client;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "commonplace-migrate", about = "Migrate workspace to repo/main layout")]
struct Args {
    /// Server URL
    #[clap(long, default_value = "http://localhost:5199")]
    server: String,

    /// Path to the redb database file
    #[clap(short, long)]
    database: PathBuf,

    /// Repo name (the directory that will contain branches)
    #[clap(long, default_value = "workspace")]
    repo_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Client::new();
    let server = &args.server;

    // 1. Discover the current fs-root UUID
    let old_fs_root_id = discover_fs_root(&client, server).await.map_err(|e| {
        format!("Cannot discover fs-root: {}. Is the server running?", e)
    })?;
    println!("Current fs-root: {}", &old_fs_root_id[..8]);

    // 2. Create repo container doc
    let repo_id = create_json_doc(&client, server).await?;
    println!("Created repo container: {}", &repo_id[..8]);

    // 3. Set repo schema: { main: { node_id: OLD_FS_ROOT } }
    let repo_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "main": {
                    "type": "dir",
                    "node_id": old_fs_root_id
                }
            }
        }
    });
    push_schema_to_server(
        &client,
        server,
        &repo_id,
        &repo_schema.to_string(),
        "commonplace-migrate",
    )
    .await
    .map_err(|e| format!("Failed to set repo schema: {}", e))?;
    println!("Repo schema set: main -> {}", &old_fs_root_id[..8]);

    // 4. Create new fs-root doc
    let new_fs_root_id = create_json_doc(&client, server).await?;
    println!("Created new fs-root: {}", &new_fs_root_id[..8]);

    // 5. Set new fs-root schema: { workspace: { node_id: REPO } }
    let new_root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                &args.repo_name: {
                    "type": "dir",
                    "node_id": repo_id
                }
            }
        }
    });
    push_schema_to_server(
        &client,
        server,
        &new_fs_root_id,
        &new_root_schema.to_string(),
        "commonplace-migrate",
    )
    .await
    .map_err(|e| format!("Failed to set new fs-root schema: {}", e))?;
    println!(
        "New fs-root schema set: {} -> {}",
        args.repo_name,
        &repo_id[..8]
    );

    // 6. Update redb metadata to point to new fs-root
    let store = CommitStore::new(&args.database)?;
    store
        .set_metadata("fs_root_uuid", &new_fs_root_id)
        .await?;
    println!(
        "Updated redb metadata: fs_root_uuid = {}",
        &new_fs_root_id[..8]
    );

    println!("\nMigration complete!");
    println!("  Old fs-root: {} (now the 'main' branch)", &old_fs_root_id[..8]);
    println!("  Repo:        {} (container for branches)", &repo_id[..8]);
    println!("  New fs-root: {} (root of document tree)", &new_fs_root_id[..8]);
    println!();
    println!("Next steps:");
    println!("  1. Update commonplace.json sync args: --node workspace/main");
    println!("  2. Restart the server and sync client");

    Ok(())
}

async fn create_json_doc(
    client: &Client,
    server: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let resp = client
        .post(format!("{}/docs", server))
        .json(&serde_json::json!({
            "content_type": "application/json"
        }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to create doc: {} - {}", status, body).into());
    }

    let body: serde_json::Value = resp.json().await?;
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("Response missing 'id'")?;

    Ok(id.to_string())
}
