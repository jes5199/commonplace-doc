//! commonplace-replay: View or replay edit history for a synced file
//!
//! Usage:
//!   commonplace-replay path/to/file.txt --list       # List commits
//!   commonplace-replay path/to/file.txt              # Show current content
//!   commonplace-replay path/to/file.txt --at <cid>   # Show content at commit

use clap::Parser;
use commonplace_doc::cli::{fetch_changes, fetch_head, ReplayArgs};
use commonplace_doc::fs::{Entry, FsSchema};
use commonplace_doc::sync::SCHEMA_FILENAME;
use commonplace_doc::workspace::format_timestamp;
use reqwest::Client;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

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

fn find_workspace_root(start: &Path) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let mut current = start.to_path_buf();
    loop {
        let schema_path = current.join(SCHEMA_FILENAME);
        if schema_path.exists() {
            return Ok((current, schema_path));
        }
        if !current.pop() {
            return Err(format!(
                "Not in a commonplace sync directory: {} not found",
                SCHEMA_FILENAME
            )
            .into());
        }
    }
}

fn normalize_path(
    path: &Path,
    cwd: &Path,
    workspace_root: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };

    let rel_path = abs_path.strip_prefix(workspace_root).map_err(|_| {
        format!(
            "Path {} is not under workspace {}",
            path.display(),
            workspace_root.display()
        )
    })?;

    Ok(rel_path.to_string_lossy().to_string())
}

fn split_path(path: &str) -> Result<(Vec<String>, String), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return Err("Empty path".into());
    }
    let filename = parts.last().unwrap().to_string();
    let dirs: Vec<String> = parts[..parts.len() - 1]
        .iter()
        .map(|s| s.to_string())
        .collect();
    Ok((dirs, filename))
}

fn resolve_uuid(
    workspace_root: &Path,
    dirs: &[String],
    filename: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let root_schema_path = workspace_root.join(SCHEMA_FILENAME);
    let root_schema_content = fs::read_to_string(&root_schema_path)?;
    let mut current_schema: FsSchema = serde_json::from_str(&root_schema_content)?;
    let mut current_dir_path = workspace_root.to_path_buf();

    let mut remaining_dirs = dirs.to_vec();
    let mut i = 0;

    while i < remaining_dirs.len() {
        let dir_name = &remaining_dirs[i];

        let root = current_schema
            .root
            .as_ref()
            .ok_or("Schema has no root directory")?;
        let entries = match root {
            Entry::Dir(d) => d.entries.as_ref().ok_or("Directory has no entries")?,
            _ => return Err("Root is not a directory".into()),
        };

        let entry = entries
            .get(dir_name)
            .ok_or_else(|| format!("Directory not found in schema: {}", dir_name))?;

        match entry {
            Entry::Dir(dir) => {
                if dir.entries.is_none() && dir.node_id.is_some() {
                    current_dir_path = current_dir_path.join(dir_name);
                    let subdir_schema_path = current_dir_path.join(SCHEMA_FILENAME);

                    if subdir_schema_path.exists() {
                        let content = fs::read_to_string(&subdir_schema_path)?;
                        current_schema = serde_json::from_str(&content)?;
                        remaining_dirs = remaining_dirs[i + 1..].to_vec();
                        i = 0;
                        continue;
                    } else {
                        return Err(format!(
                            "Node-backed directory {} has no {} file",
                            dir_name, SCHEMA_FILENAME
                        )
                        .into());
                    }
                }
                current_dir_path = current_dir_path.join(dir_name);
            }
            _ => return Err(format!("{} is not a directory", dir_name).into()),
        }
        i += 1;
    }

    let root = current_schema
        .root
        .as_ref()
        .ok_or("Schema has no root directory")?;

    let mut entries = match root {
        Entry::Dir(d) => d.entries.as_ref().ok_or("Directory has no entries")?,
        _ => return Err("Root is not a directory".into()),
    };

    for dir_name in &remaining_dirs {
        let entry = entries
            .get(dir_name)
            .ok_or_else(|| format!("Directory not found: {}", dir_name))?;
        entries = match entry {
            Entry::Dir(d) => d.entries.as_ref().ok_or("Directory has no entries")?,
            _ => return Err(format!("{} is not a directory", dir_name).into()),
        };
    }

    let file_entry = entries
        .get(filename)
        .ok_or_else(|| format!("File not found in schema: {}", filename))?;

    match file_entry {
        Entry::Doc(doc) => doc
            .node_id
            .clone()
            .ok_or_else(|| format!("File {} has no UUID assigned", filename).into()),
        Entry::Dir(_) => Err(format!("{} is a directory, not a file", filename).into()),
    }
}
