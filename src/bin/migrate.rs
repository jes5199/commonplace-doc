//! One-time migration: nest existing workspace under repo/main layout.
//!
//! Before: fs-root → { bartleby: ..., text-to-telegram: ..., ... }
//! After:  fs-root → { workspace: { node_id: REPO } }
//!         REPO    → { main: { node_id: OLD_FS_ROOT } }
//!         OLD_FS_ROOT → { bartleby: ..., text-to-telegram: ..., ... }
//!
//! Two-phase migration:
//!   Phase 1 (server stopped):  commonplace-migrate --database ./data/commonplace.redb
//!   Phase 2 (server running):  commonplace-migrate --server http://localhost:5199 --phase2
//!
//! Phase 1 creates new doc IDs and updates the redb metadata.
//! Phase 2 pushes schemas through HTTP so the server has them in memory.

use clap::Parser;
use commonplace_doc::store::CommitStore;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "commonplace-migrate",
    about = "Migrate workspace to repo/main layout"
)]
struct Args {
    /// Path to the redb database file
    #[clap(short, long)]
    database: PathBuf,

    /// Server URL (for phase 2)
    #[clap(long, default_value = "http://localhost:5199")]
    server: String,

    /// Run phase 2 (push schemas to running server)
    #[clap(long)]
    phase2: bool,

    /// Show current database state (metadata + doc heads)
    #[clap(long)]
    info: bool,

    /// Restore fs_root_uuid to a specific UUID (undo broken migration)
    #[clap(long)]
    restore_fs_root: Option<String>,

    /// Repo name
    #[clap(long, default_value = "workspace")]
    repo_name: String,

    /// Old fs-root UUID (for phase 2 without redb access)
    #[clap(long)]
    old_fs_root: Option<String>,

    /// Repo UUID (for phase 2 without redb access)
    #[clap(long)]
    repo_id: Option<String>,

    /// New fs-root UUID (for phase 2 without redb access)
    #[clap(long)]
    new_fs_root: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.info {
        return info(&args).await;
    }

    if let Some(ref uuid) = args.restore_fs_root {
        return restore(&args, uuid).await;
    }

    if args.phase2 {
        return phase2(&args).await;
    }

    phase1(&args).await
}

/// Show current database state for debugging.
async fn info(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let store = CommitStore::new(&args.database)?;

    println!("=== Metadata ===");
    for key in &[
        "fs_root_uuid",
        "migrate_old_fs_root",
        "migrate_repo_id",
        "migrate_new_fs_root",
        "migrate_repo_name",
    ] {
        let val = store.get_metadata(key).await?;
        println!("  {:25} = {:?}", key, val);
    }

    println!("\n=== Doc Heads ===");
    // Check the known UUIDs
    let fs_root = store.get_metadata("fs_root_uuid").await?;
    let old_root = store.get_metadata("migrate_old_fs_root").await?;
    let repo = store.get_metadata("migrate_repo_id").await?;
    let new_root = store.get_metadata("migrate_new_fs_root").await?;

    let mut uuids_to_check: Vec<(String, String)> = vec![
        ("948b3c3f-f3ce-4d02-bfb0-e49e34ceb0a1".to_string(), "original-fs-root".to_string()),
    ];
    if let Some(ref id) = fs_root {
        uuids_to_check.push((id.clone(), "fs_root_uuid".to_string()));
    }
    if let Some(ref id) = old_root {
        if !id.is_empty() { uuids_to_check.push((id.clone(), "migrate_old_fs_root".to_string())); }
    }
    if let Some(ref id) = repo {
        if !id.is_empty() { uuids_to_check.push((id.clone(), "migrate_repo_id".to_string())); }
    }
    if let Some(ref id) = new_root {
        if !id.is_empty() { uuids_to_check.push((id.clone(), "migrate_new_fs_root".to_string())); }
    }

    // Deduplicate
    uuids_to_check.sort_by(|a, b| a.0.cmp(&b.0));
    uuids_to_check.dedup_by(|a, b| a.0 == b.0);

    for (uuid, label) in &uuids_to_check {
        let head = store.get_document_head(uuid).await?;
        println!("  {} ({}) head={:?}", &uuid[..12], label, head.as_deref().map(|s| &s[..12.min(s.len())]));
    }

    // Also check legacy "workspace" key
    let legacy = store.get_document_head("workspace").await?;
    println!("  workspace (legacy) head={:?}", legacy.as_deref().map(|s| &s[..12.min(s.len())]));

    Ok(())
}

/// Restore fs_root_uuid metadata to a specific UUID and clean up stale migration artifacts.
async fn restore(args: &Args, uuid: &str) -> Result<(), Box<dyn std::error::Error>> {
    let store = CommitStore::new(&args.database)?;

    // Verify the UUID has a doc head
    if let Some(head_cid) = store.get_document_head(uuid).await? {
        println!("UUID {} has head: {}", &uuid[..12], &head_cid[..12]);
    } else {
        eprintln!("Warning: UUID {} has no doc head in redb", uuid);
        eprintln!("Proceeding anyway...");
    }

    // Collect stale UUIDs from migration metadata before clearing
    let mut stale_uuids = Vec::new();
    for key in &["migrate_old_fs_root", "migrate_repo_id", "migrate_new_fs_root"] {
        if let Some(val) = store.get_metadata(key).await? {
            if !val.is_empty() && val != uuid {
                stale_uuids.push(val);
            }
        }
    }
    // Also check the current fs_root_uuid if it differs from the target
    if let Some(current) = store.get_metadata("fs_root_uuid").await? {
        if current != uuid && !stale_uuids.contains(&current) {
            stale_uuids.push(current);
        }
    }

    // Known stale UUIDs from previous failed migration attempts
    for known_stale in &[
        "15c8b425-9757-4247-9a09-19c1b8eb9de0",
        "120c504a-8c71-418b-9a10-ddea344abafe",
        "cbf8db25-bea9-4cbd-9f5e-6a27146ef131",
        "4b5b8bfa-e72e-473a-92a5-fe4195e15593",
        "cc29134e-d403-40c8-ba2e-327fddf82bfb",
        "1e5fe064-0baf-48d7-af79-72f507c67566",
        "7766d373-7dd9-4bc8-8deb-912aadb52cdc",
    ] {
        let s = known_stale.to_string();
        if s != uuid && !stale_uuids.contains(&s) {
            stale_uuids.push(s);
        }
    }

    // Clean up stale doc heads from previous migration attempts
    for stale_id in &stale_uuids {
        if store.get_document_head(stale_id).await?.is_some() {
            store.delete_document_head(stale_id).await?;
            println!("Deleted stale doc head for {}", &stale_id[..12.min(stale_id.len())]);
        }
    }

    // Reset fs_root_uuid
    store.set_metadata("fs_root_uuid", uuid).await?;
    println!("Restored fs_root_uuid to {}", uuid);

    // Clean up migration metadata
    for key in &[
        "migrate_old_fs_root",
        "migrate_repo_id",
        "migrate_new_fs_root",
        "migrate_repo_name",
    ] {
        store.set_metadata(key, "").await?;
    }
    println!("Cleared migration metadata");

    Ok(())
}

/// Phase 1: offline — create doc IDs, update metadata.
async fn phase1(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let store = CommitStore::new(&args.database)?;

    let old_fs_root_id = store.get_or_create_fs_root().await?;
    println!("Current fs-root: {}", old_fs_root_id);

    let old_head = store.get_document_head(&old_fs_root_id).await?;
    if old_head.is_none() {
        eprintln!("Error: fs-root has no HEAD. Run the server first.");
        std::process::exit(1);
    }

    let repo_id = uuid::Uuid::new_v4().to_string();
    let new_fs_root_id = uuid::Uuid::new_v4().to_string();

    // Store the migration plan in metadata so phase 2 can read it
    store
        .set_metadata("migrate_old_fs_root", &old_fs_root_id)
        .await?;
    store.set_metadata("migrate_repo_id", &repo_id).await?;
    store
        .set_metadata("migrate_new_fs_root", &new_fs_root_id)
        .await?;
    store
        .set_metadata("migrate_repo_name", &args.repo_name)
        .await?;

    // Update fs-root to new UUID
    store.set_metadata("fs_root_uuid", &new_fs_root_id).await?;

    println!("Phase 1 complete!");
    println!("  Old fs-root: {} (will become 'main' branch)", old_fs_root_id);
    println!("  Repo:        {}", repo_id);
    println!("  New fs-root: {}", new_fs_root_id);
    println!();
    println!("Next: start the server, then run:");
    println!(
        "  commonplace-migrate --database {} --phase2",
        args.database.display()
    );

    Ok(())
}

/// Phase 2: online — push schemas via HTTP.
async fn phase2(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // Try CLI args first (when server holds the redb lock), fall back to metadata
    let (old_fs_root_id, repo_id, new_fs_root_id, repo_name) =
        if args.old_fs_root.is_some() && args.repo_id.is_some() && args.new_fs_root.is_some() {
            (
                args.old_fs_root.clone().unwrap(),
                args.repo_id.clone().unwrap(),
                args.new_fs_root.clone().unwrap(),
                args.repo_name.clone(),
            )
        } else {
            let store = CommitStore::new(&args.database)?;
            let old = store
                .get_metadata("migrate_old_fs_root")
                .await?
                .ok_or("No migration plan found. Run phase 1 first.")?;
            let repo = store
                .get_metadata("migrate_repo_id")
                .await?
                .ok_or("Missing migrate_repo_id")?;
            let new = store
                .get_metadata("migrate_new_fs_root")
                .await?
                .ok_or("Missing migrate_new_fs_root")?;
            let name = store
                .get_metadata("migrate_repo_name")
                .await?
                .unwrap_or_else(|| args.repo_name.clone());
            drop(store);
            (old, repo, new, name)
        };

    println!("Pushing schemas to server at {}...", args.server);

    let client = reqwest::Client::new();

    // Verify server is up and has the new fs-root
    let fs_root_resp: serde_json::Value = client
        .get(format!("{}/fs-root", args.server))
        .send()
        .await?
        .json()
        .await?;
    let server_fs_root = fs_root_resp["id"].as_str().unwrap_or("");
    if server_fs_root != new_fs_root_id {
        eprintln!(
            "Server fs-root ({}) doesn't match expected ({}). Was phase 1 run?",
            server_fs_root, &new_fs_root_id[..12]
        );
        std::process::exit(1);
    }

    // Push repo schema: { main: { node_id: OLD_FS_ROOT } }
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
    push_schema(&client, &args.server, &repo_id, &repo_schema.to_string()).await?;
    println!("Pushed repo schema: {} -> main -> {}", &repo_id[..12], &old_fs_root_id[..12]);

    // Push new fs-root schema: { workspace: { node_id: REPO } }
    let root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                &repo_name: {
                    "type": "dir",
                    "node_id": repo_id
                }
            }
        }
    });
    push_schema(&client, &args.server, &new_fs_root_id, &root_schema.to_string()).await?;
    println!(
        "Pushed fs-root schema: {} -> {} -> {}",
        &new_fs_root_id[..12],
        repo_name,
        &repo_id[..12]
    );

    println!("\nMigration complete! Restart the orchestrator.");

    Ok(())
}

/// Push schema content to a document via HTTP replace endpoint.
async fn push_schema(
    client: &reqwest::Client,
    server: &str,
    doc_id: &str,
    content: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // First ensure the doc exists
    let create_resp = client
        .post(format!("{}/docs", server))
        .json(&serde_json::json!({
            "id": doc_id,
            "content_type": "application/json"
        }))
        .send()
        .await?;

    if !create_resp.status().is_success() {
        // Doc might already exist, that's fine
        let status = create_resp.status();
        if status.as_u16() != 409 {
            let body = create_resp.text().await.unwrap_or_default();
            eprintln!("Warning: create doc {} returned {}: {}", &doc_id[..12], status, body);
        }
    }

    // Replace content (this computes a Yjs diff and persists)
    let replace_resp = client
        .post(format!("{}/docs/{}/replace", server, doc_id))
        .header("Content-Type", "text/plain")
        .body(content.to_string())
        .send()
        .await?;

    if !replace_resp.status().is_success() {
        let status = replace_resp.status();
        let body = replace_resp.text().await.unwrap_or_default();
        return Err(format!("Failed to replace {}: {} - {}", &doc_id[..12], status, body).into());
    }

    Ok(())
}
