//! commonplace-replay: View or replay edit history for a synced file
//!
//! Usage:
//!   commonplace-replay path/to/file.txt --list       # List commits
//!   commonplace-replay path/to/file.txt              # Show current content
//!   commonplace-replay path/to/file.txt --at <cid>   # Show content at commit

use clap::Parser;
use commonplace_doc::cli::{fetch_changes, fetch_head, ReplayArgs};
use commonplace_doc::workspace::{
    find_workspace_root, format_timestamp, normalize_path, resolve_uuid, split_path,
};
use reqwest::Client;
use serde::Serialize;

#[derive(Serialize)]
struct CommitInfo {
    cid: String,
    timestamp: u64,
    datetime: String,
}

#[derive(Serialize)]
struct ListOutput {
    uuid: String,
    path: String,
    commits: Vec<CommitInfo>,
}

#[derive(Serialize)]
struct ContentOutput {
    uuid: String,
    path: String,
    cid: Option<String>,
    content: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = ReplayArgs::parse();

    // Find workspace root and resolve UUID
    // Start from the file's parent directory, not cwd, so we work from anywhere
    let cwd = std::env::current_dir()?;
    let file_path = if args.path.is_absolute() {
        args.path.clone()
    } else {
        cwd.join(&args.path)
    };
    let file_dir = file_path.parent().ok_or("Cannot get parent directory")?;
    let (workspace_root, _) = find_workspace_root(file_dir)?;
    let rel_path = normalize_path(&args.path, &cwd, &workspace_root)?;
    let (dirs, filename) = split_path(&rel_path)?;
    let uuid = resolve_uuid(&workspace_root, &dirs, &filename)?;

    let client = Client::new();

    if args.list {
        // Fetch commit history
        let changes = match fetch_changes(&client, &args.server, &uuid).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        };

        if args.json {
            let output = ListOutput {
                uuid: uuid.clone(),
                path: rel_path.clone(),
                commits: changes
                    .changes
                    .iter()
                    .map(|c| CommitInfo {
                        cid: c.commit_id.clone(),
                        timestamp: c.timestamp,
                        datetime: format_timestamp(c.timestamp),
                    })
                    .collect(),
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("File: {}", rel_path);
            println!("UUID: {}", uuid);
            println!();

            for change in &changes.changes {
                println!(
                    "{} {}",
                    &change.commit_id,
                    format_timestamp(change.timestamp)
                );
            }
            println!();
            println!("{} commits", changes.changes.len());
        }
    } else {
        // Fetch content (optionally at specific commit)
        let head = match fetch_head(&client, &args.server, &uuid, args.at.as_deref()).await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        };

        // Also fetch commit count for context (unless showing historical content)
        let commit_count = if args.at.is_none() {
            fetch_changes(&client, &args.server, &uuid)
                .await
                .ok()
                .map(|c| c.changes.len())
        } else {
            None
        };

        if args.json {
            let output = ContentOutput {
                uuid: uuid.clone(),
                path: rel_path.clone(),
                cid: head.cid,
                content: head.content,
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            if let Some(ref cid) = head.cid {
                if let Some(count) = commit_count {
                    eprintln!("Commit: {} ({} total, use --list to see all)", cid, count);
                } else {
                    eprintln!("Commit: {}", cid);
                }
            }
            if let Some(content) = head.content {
                print!("{}", content);
            }
        }
    }

    Ok(())
}
