//! commonplace-show: Show content at a specific commit (like git show)
//!
//! Usage:
//!   commonplace-show path/to/file.txt               # Show HEAD content
//!   commonplace-show path/to/file.txt <cid>         # Show content at commit
//!   commonplace-show --stat path/to/file.txt        # Show with change stats

use clap::Parser;
use commonplace_doc::cli::{HeadResponse, ShowArgs};
use commonplace_doc::workspace::{format_timestamp, resolve_path_to_uuid};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct CommitChange {
    #[allow(dead_code)]
    doc_id: String,
    commit_id: String,
    timestamp: u64,
    #[allow(dead_code)]
    url: String,
}

#[derive(Deserialize)]
struct ChangesResponse {
    changes: Vec<CommitChange>,
}

#[derive(Serialize)]
struct ShowOutput {
    uuid: String,
    path: String,
    cid: Option<String>,
    timestamp: Option<u64>,
    datetime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<ChangeStats>,
    content: Option<String>,
}

#[derive(Serialize, Clone)]
struct ChangeStats {
    lines_added: usize,
    lines_removed: usize,
    chars_added: usize,
    chars_removed: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = ShowArgs::parse();

    // Resolve file path to UUID
    let (uuid, _workspace_root, rel_path) = resolve_path_to_uuid(&args.path)?;

    let client = Client::new();

    // Fetch content (optionally at specific commit)
    let url = if let Some(ref commit) = args.commit {
        format!("{}/docs/{}/head?at_commit={}", args.server, uuid, commit)
    } else {
        format!("{}/docs/{}/head", args.server, uuid)
    };

    let resp = client.get(&url).send().await?;

    if !resp.status().is_success() {
        eprintln!("Failed to fetch content: HTTP {}", resp.status());
        std::process::exit(1);
    }

    let head: HeadResponse = resp.json().await?;

    // Get commit info (timestamp, etc.)
    let commit_info: Option<CommitChange> = if let Some(ref cid) = head.cid {
        // Fetch changes to find timestamp for this commit
        let changes_url = format!("{}/documents/{}/changes", args.server, uuid);
        if let Ok(resp) = client.get(&changes_url).send().await {
            if let Ok(changes) = resp.json::<ChangesResponse>().await {
                changes.changes.into_iter().find(|c| &c.commit_id == cid)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    // Compute stats if requested
    let stats: Option<ChangeStats> = if args.stat {
        if let (Some(ref cid), Some(ref content)) = (&head.cid, &head.content) {
            compute_stats(&client, &args.server, &uuid, cid, content).await?
        } else {
            None
        }
    } else {
        None
    };

    if args.json {
        let output = ShowOutput {
            uuid: uuid.clone(),
            path: rel_path.clone(),
            cid: head.cid.clone(),
            timestamp: commit_info.as_ref().map(|c| c.timestamp),
            datetime: commit_info.as_ref().map(|c| format_timestamp(c.timestamp)),
            stats,
            content: head.content.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        // Header
        if let Some(ref cid) = head.cid {
            println!("commit {}", cid);
        }
        if let Some(ref info) = commit_info {
            println!("Date:   {}", format_timestamp(info.timestamp));
        }

        if args.stat {
            if let Some(ref s) = stats {
                println!();
                println!(
                    " {} chars (+{}/-{}), {} lines (+{}/-{})",
                    s.chars_added + s.chars_removed,
                    s.chars_added,
                    s.chars_removed,
                    s.lines_added + s.lines_removed,
                    s.lines_added,
                    s.lines_removed
                );
            }
        }

        println!();

        // Content
        if let Some(content) = head.content {
            print!("{}", content);
            // Ensure trailing newline if content doesn't have one
            if !content.ends_with('\n') {
                println!();
            }
        }
    }

    Ok(())
}

async fn compute_stats(
    client: &Client,
    server: &str,
    uuid: &str,
    current_cid: &str,
    current_content: &str,
) -> Result<Option<ChangeStats>, Box<dyn std::error::Error>> {
    // Fetch all changes to find the parent commit
    let changes_url = format!("{}/documents/{}/changes", server, uuid);
    let resp = client.get(&changes_url).send().await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let changes: ChangesResponse = resp.json().await?;

    // Find the current commit and its predecessor
    let current_idx = changes
        .changes
        .iter()
        .position(|c| c.commit_id == current_cid);

    if let Some(idx) = current_idx {
        // API returns oldest→newest, so parent is at idx-1 (older commit)
        if idx > 0 {
            // Get parent commit content
            let parent = &changes.changes[idx - 1];
            let parent_url = format!(
                "{}/docs/{}/head?at_commit={}",
                server, uuid, parent.commit_id
            );

            if let Ok(resp) = client.get(&parent_url).send().await {
                if resp.status().is_success() {
                    if let Ok(head) = resp.json::<HeadResponse>().await {
                        if let Some(parent_content) = head.content {
                            return Ok(Some(compute_diff_stats(&parent_content, current_content)));
                        }
                    }
                }
            }
        } else {
            // First commit (idx=0) - all additions
            return Ok(Some(ChangeStats {
                lines_added: current_content.lines().count(),
                lines_removed: 0,
                chars_added: current_content.len(),
                chars_removed: 0,
            }));
        }
    }

    Ok(None)
}

fn compute_diff_stats(old: &str, new: &str) -> ChangeStats {
    // Simple line-based diff stats
    let old_lines: std::collections::HashSet<&str> = old.lines().collect();
    let new_lines: std::collections::HashSet<&str> = new.lines().collect();

    let added: usize = new_lines.difference(&old_lines).count();
    let removed: usize = old_lines.difference(&new_lines).count();

    // Character diff (simple approximation)
    let chars_added = if new.len() > old.len() {
        new.len() - old.len()
    } else {
        0
    };
    let chars_removed = if old.len() > new.len() {
        old.len() - new.len()
    } else {
        0
    };

    ChangeStats {
        lines_added: added,
        lines_removed: removed,
        chars_added,
        chars_removed,
    }
}
