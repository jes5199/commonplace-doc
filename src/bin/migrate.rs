//! One-time migration: nest existing workspace under repo/main layout.
//!
//! Before: fs-root → { bartleby: ..., text-to-telegram: ..., ... }
//! After:  fs-root → { workspace: { node_id: REPO } }
//!         REPO    → { main: { node_id: OLD_FS_ROOT } }
//!         OLD_FS_ROOT → { bartleby: ..., text-to-telegram: ..., ... }
//!
//! Run with the server STOPPED:
//!   commonplace-migrate --database ./data/commonplace.redb

use clap::Parser;
use commonplace_doc::b64;
use commonplace_doc::commit::Commit;
use commonplace_doc::store::CommitStore;
use std::path::PathBuf;
use yrs::{Doc, GetString, ReadTxn, Text, Transact};

#[derive(Parser)]
#[command(
    name = "commonplace-migrate",
    about = "Migrate workspace to repo/main layout (run with server stopped)"
)]
struct Args {
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

    let store = CommitStore::new(&args.database)?;

    // 1. Get current fs-root
    let old_fs_root_id = store.get_or_create_fs_root().await?;
    println!("Current fs-root: {}", old_fs_root_id);

    let old_head = store.get_document_head(&old_fs_root_id).await?;
    if old_head.is_none() {
        eprintln!("Error: fs-root has no HEAD. Run the server first to populate the database.");
        std::process::exit(1);
    }
    println!("  HEAD: {}", old_head.unwrap());

    // 2. Generate UUIDs for new docs
    let repo_id = uuid::Uuid::new_v4().to_string();
    let new_fs_root_id = uuid::Uuid::new_v4().to_string();

    // 3. Write repo schema: { main: { node_id: OLD_FS_ROOT } }
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
    write_schema_commit(&store, &repo_id, &repo_schema.to_string()).await?;
    println!("Wrote repo schema: {} -> main -> {}", &repo_id[..12], &old_fs_root_id[..12]);

    // 4. Write new fs-root schema: { workspace: { node_id: REPO } }
    let root_schema = serde_json::json!({
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
    write_schema_commit(&store, &new_fs_root_id, &root_schema.to_string()).await?;
    println!("Wrote new fs-root schema: {} -> {} -> {}", &new_fs_root_id[..12], args.repo_name, &repo_id[..12]);

    // 5. Update metadata
    store.set_metadata("fs_root_uuid", &new_fs_root_id).await?;
    println!("Updated metadata: fs_root_uuid = {}", new_fs_root_id);

    println!("\nMigration complete!");
    println!("  Old fs-root: {} (now the 'main' branch)", old_fs_root_id);
    println!("  Repo:        {} (container)", repo_id);
    println!("  New fs-root: {}", new_fs_root_id);
    println!("\nRestart the server. Sync config should use --node workspace/main");

    Ok(())
}

async fn write_schema_commit(
    store: &CommitStore,
    doc_id: &str,
    content: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, content);
    }

    let update = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };

    let update_b64 = b64::encode(&update);

    let commit = Commit::new(
        vec![],
        update_b64,
        "commonplace-migrate".to_string(),
        Some(format!("Initialize schema for {}", &doc_id[..12])),
    );

    store.store_commit_and_set_head(doc_id, &commit).await?;
    Ok(())
}
