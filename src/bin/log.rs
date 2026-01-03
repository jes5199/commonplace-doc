//! commonplace-log: Show commit history for a synced file (like git log)
//!
//! Usage:
//!   commonplace-log path/to/file.txt               # Full log output
//!   commonplace-log --oneline path/to/file.txt     # Compact one-line format
//!   commonplace-log --stat path/to/file.txt        # Show change statistics
//!   commonplace-log -p path/to/file.txt            # Show diffs
//!   commonplace-log -n 5 path/to/file.txt          # Limit to 5 commits
//!   commonplace-log --since 2024-01-01 path.txt    # Commits after date

use clap::Parser;
use commonplace_doc::cli::LogArgs;
use commonplace_doc::workspace::{
    format_timestamp, format_timestamp_short, parse_date, resolve_path_to_uuid,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::{self, IsTerminal, Write};
use std::process::{Command, Stdio};

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

#[derive(Deserialize)]
struct HeadResponse {
    #[allow(dead_code)]
    cid: Option<String>,
    content: Option<String>,
}

#[derive(Serialize)]
struct CommitInfo {
    cid: String,
    timestamp: u64,
    datetime: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<ChangeStats>,
}

#[derive(Serialize, Clone)]
struct ChangeStats {
    lines_added: usize,
    lines_removed: usize,
    chars_added: usize,
    chars_removed: usize,
}

#[derive(Serialize)]
struct LogOutput {
    uuid: String,
    path: String,
    commits: Vec<CommitInfo>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = LogArgs::parse();

    // Resolve file path to UUID
    let (uuid, _workspace_root, rel_path) = resolve_path_to_uuid(&args.path)?;

    let client = Client::new();

    // Fetch commit history
    let url = format!("{}/documents/{}/changes", args.server, uuid);
    let resp = client.get(&url).send().await?;

    if !resp.status().is_success() {
        eprintln!("Failed to fetch changes: HTTP {}", resp.status());
        std::process::exit(1);
    }

    let mut changes: ChangesResponse = resp.json().await?;

    // Reverse to show newest first (git log order) - API returns oldest first
    changes.changes.reverse();

    // Apply date filters
    if let Some(ref since) = args.since {
        if let Some(since_ts) = parse_date(since) {
            changes.changes.retain(|c| c.timestamp >= since_ts);
        } else {
            eprintln!("Warning: could not parse --since date: {}", since);
        }
    }

    if let Some(ref until) = args.until {
        if let Some(until_ts) = parse_date(until) {
            changes.changes.retain(|c| c.timestamp <= until_ts);
        } else {
            eprintln!("Warning: could not parse --until date: {}", until);
        }
    }

    // Apply max count limit
    if let Some(max) = args.max_count {
        changes.changes.truncate(max);
    }

    // Compute stats if requested
    let stats_map: Option<Vec<Option<ChangeStats>>> = if args.stat {
        Some(compute_stats(&client, &args.server, &uuid, &changes.changes).await?)
    } else {
        None
    };

    // Apply opt-out flags
    let graph = args.graph && !args.no_graph && !args.oneline;
    let decorate = (args.decorate || graph) && !args.no_decorate;

    // Fetch diffs (default on, unless --no-patch or --oneline)
    let show_patch = !args.no_patch && !args.oneline;
    let diffs: Option<Vec<Option<String>>> = if show_patch {
        Some(compute_diffs(&client, &args.server, &uuid, &changes.changes).await?)
    } else {
        None
    };

    // Build output string
    let output = build_output(
        &args,
        &rel_path,
        &uuid,
        &changes.changes,
        &stats_map,
        &diffs,
        graph,
        decorate,
    )?;

    // Use pager if interactive terminal and not disabled
    if !args.no_pager && !args.json && io::stdout().is_terminal() {
        output_with_pager(&output);
    } else {
        print!("{}", output);
    }

    Ok(())
}

fn build_output(
    args: &LogArgs,
    rel_path: &str,
    uuid: &str,
    changes: &[CommitChange],
    stats_map: &Option<Vec<Option<ChangeStats>>>,
    diffs: &Option<Vec<Option<String>>>,
    graph: bool,
    decorate: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut out = String::new();

    if args.json {
        let commits: Vec<CommitInfo> = changes
            .iter()
            .enumerate()
            .map(|(i, c)| CommitInfo {
                cid: c.commit_id.clone(),
                timestamp: c.timestamp,
                datetime: format_timestamp(c.timestamp),
                stats: stats_map.as_ref().and_then(|m| m.get(i).cloned().flatten()),
            })
            .collect();

        let output = LogOutput {
            uuid: uuid.to_string(),
            path: rel_path.to_string(),
            commits,
        };
        out.push_str(&serde_json::to_string_pretty(&output)?);
        out.push('\n');
    } else if args.oneline {
        for (i, change) in changes.iter().enumerate() {
            let cid_short = &change.commit_id[..12.min(change.commit_id.len())];
            let date = format_timestamp_short(change.timestamp);
            let decoration = if decorate && i == 0 {
                " \x1b[36m(HEAD)\x1b[0m"
            } else {
                ""
            };

            if args.stat {
                if let Some(ref stats_vec) = stats_map {
                    if let Some(Some(stats)) = stats_vec.get(i) {
                        out.push_str(&format!(
                            "\x1b[33m{}\x1b[0m{} {} (+{} -{} chars)\n",
                            cid_short, decoration, date, stats.chars_added, stats.chars_removed
                        ));
                        continue;
                    }
                }
            }
            out.push_str(&format!(
                "\x1b[33m{}\x1b[0m{} {}\n",
                cid_short, decoration, date
            ));
        }
    } else if graph {
        out.push_str(&build_graph_view(changes, stats_map.as_ref(), decorate));
    } else {
        // Full output (like git log)
        out.push_str(&format!("File: {}\n", rel_path));
        out.push_str(&format!("UUID: {}\n", uuid));
        out.push('\n');

        for (i, change) in changes.iter().enumerate() {
            let decoration = if decorate && i == 0 {
                " \x1b[36m(HEAD)\x1b[0m"
            } else {
                ""
            };
            out.push_str(&format!(
                "\x1b[33mcommit {}\x1b[0m{}\n",
                change.commit_id, decoration
            ));
            out.push_str(&format!("Date:   {}\n", format_timestamp(change.timestamp)));

            if args.stat {
                if let Some(ref stats_vec) = stats_map {
                    if let Some(Some(stats)) = stats_vec.get(i) {
                        out.push('\n');
                        out.push_str(&format!(
                            " {} chars (+{}/-{}), {} lines (+{}/-{})\n",
                            stats.chars_added + stats.chars_removed,
                            stats.chars_added,
                            stats.chars_removed,
                            stats.lines_added + stats.lines_removed,
                            stats.lines_added,
                            stats.lines_removed
                        ));
                    }
                }
            }

            // Show diff if available (default on, suppressed by --no-patch)
            if let Some(ref diff_vec) = diffs {
                if let Some(Some(diff)) = diff_vec.get(i) {
                    if !diff.is_empty() {
                        out.push('\n');
                        out.push_str(&colorize_diff(diff));
                    }
                }
            }

            out.push('\n');
        }

        out.push_str(&format!("{} commits\n", changes.len()));
    }

    Ok(out)
}

fn colorize_diff(diff: &str) -> String {
    let mut out = String::new();
    for line in diff.lines() {
        if line.starts_with('+') {
            out.push_str(&format!("\x1b[32m{}\x1b[0m\n", line));
        } else if line.starts_with('-') {
            out.push_str(&format!("\x1b[31m{}\x1b[0m\n", line));
        } else if line.starts_with("@@") {
            out.push_str(&format!("\x1b[36m{}\x1b[0m\n", line));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn output_with_pager(output: &str) {
    // Try PAGER env var, then less, then more
    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());

    // Build pager command with -R for color support
    let mut cmd = if pager.contains("less") {
        let mut c = Command::new(&pager);
        c.arg("-R"); // Enable raw control codes for colors
        c
    } else {
        Command::new(&pager)
    };

    match cmd.stdin(Stdio::piped()).spawn() {
        Ok(mut child) => {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(output.as_bytes());
            }
            let _ = child.wait();
        }
        Err(_) => {
            // Fall back to direct output
            print!("{}", output);
        }
    }
}

fn build_graph_view(
    changes: &[CommitChange],
    stats_map: Option<&Vec<Option<ChangeStats>>>,
    decorate: bool,
) -> String {
    let mut out = String::new();
    // Simple linear graph - for now we don't have parent info
    // Full graph would need to fetch commit parents from server
    for (i, change) in changes.iter().enumerate() {
        let cid_short = &change.commit_id[..8.min(change.commit_id.len())];
        let date = format_timestamp_short(change.timestamp);
        let is_last = i == changes.len() - 1;
        let decoration = if decorate && i == 0 {
            " \x1b[36m(HEAD)\x1b[0m"
        } else {
            ""
        };

        let connector = if is_last { "  " } else { "| " };

        out.push_str(&format!(
            "* \x1b[33m{}\x1b[0m{} {}",
            cid_short, decoration, date
        ));

        if let Some(stats_vec) = stats_map {
            if let Some(Some(stats)) = stats_vec.get(i) {
                out.push_str(&format!(
                    " (+{} -{} chars)",
                    stats.chars_added, stats.chars_removed
                ));
            }
        }
        out.push('\n');

        if !is_last {
            out.push_str(&format!("{}\n", connector));
        }
    }
    out
}

async fn compute_stats(
    client: &Client,
    server: &str,
    uuid: &str,
    changes: &[CommitChange],
) -> Result<Vec<Option<ChangeStats>>, Box<dyn std::error::Error>> {
    let mut result = Vec::with_capacity(changes.len());

    // We need content at each commit and the previous one to compute diffs
    // For efficiency, we'll fetch content sequentially and diff with previous
    let mut prev_content: Option<String> = None;

    // Sort by timestamp to process chronologically
    let mut sorted_indices: Vec<usize> = (0..changes.len()).collect();
    sorted_indices.sort_by_key(|&i| changes[i].timestamp);

    // Map from original index to stats
    let mut stats_by_index: std::collections::HashMap<usize, ChangeStats> =
        std::collections::HashMap::new();

    for &orig_idx in &sorted_indices {
        let change = &changes[orig_idx];
        let url = format!(
            "{}/docs/{}/head?at_commit={}",
            server, uuid, change.commit_id
        );

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(head) = resp.json::<HeadResponse>().await {
                    if let Some(content) = head.content {
                        let stats = if let Some(ref prev) = prev_content {
                            compute_diff_stats(prev, &content)
                        } else {
                            // First commit - all additions
                            ChangeStats {
                                lines_added: content.lines().count(),
                                lines_removed: 0,
                                chars_added: content.len(),
                                chars_removed: 0,
                            }
                        };
                        stats_by_index.insert(orig_idx, stats);
                        prev_content = Some(content);
                    }
                }
            }
            _ => {
                // Skip commits we can't fetch
            }
        }
    }

    // Build result in original order
    for i in 0..changes.len() {
        result.push(stats_by_index.remove(&i));
    }

    Ok(result)
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

async fn compute_diffs(
    client: &Client,
    server: &str,
    uuid: &str,
    changes: &[CommitChange],
) -> Result<Vec<Option<String>>, Box<dyn std::error::Error>> {
    let mut result = Vec::with_capacity(changes.len());

    // We need content at each commit and the previous one to compute diffs
    let mut prev_content: Option<String> = None;

    // Sort by timestamp to process chronologically (oldest first)
    let mut sorted_indices: Vec<usize> = (0..changes.len()).collect();
    sorted_indices.sort_by_key(|&i| changes[i].timestamp);

    // Map from original index to diff
    let mut diffs_by_index: std::collections::HashMap<usize, String> =
        std::collections::HashMap::new();

    for &orig_idx in &sorted_indices {
        let change = &changes[orig_idx];
        let url = format!(
            "{}/docs/{}/head?at_commit={}",
            server, uuid, change.commit_id
        );

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(head) = resp.json::<HeadResponse>().await {
                    if let Some(content) = head.content {
                        let diff = if let Some(ref prev) = prev_content {
                            compute_unified_diff(prev, &content)
                        } else {
                            // First commit - show all as additions
                            content
                                .lines()
                                .map(|l| format!("+{}", l))
                                .collect::<Vec<_>>()
                                .join("\n")
                        };
                        diffs_by_index.insert(orig_idx, diff);
                        prev_content = Some(content);
                    }
                }
            }
            _ => {
                // Skip commits we can't fetch
            }
        }
    }

    // Build result in original order (newest first)
    for i in 0..changes.len() {
        result.push(diffs_by_index.remove(&i));
    }

    Ok(result)
}

fn compute_unified_diff(old: &str, new: &str) -> String {
    // Simple unified diff format
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();

    let mut out = String::new();

    // Find lines that changed
    let old_set: std::collections::HashSet<&str> = old_lines.iter().copied().collect();
    let new_set: std::collections::HashSet<&str> = new_lines.iter().copied().collect();

    let removed: Vec<&str> = old_lines
        .iter()
        .filter(|l| !new_set.contains(*l))
        .copied()
        .collect();
    let added: Vec<&str> = new_lines
        .iter()
        .filter(|l| !old_set.contains(*l))
        .copied()
        .collect();

    if !removed.is_empty() || !added.is_empty() {
        out.push_str(&format!(
            "@@ -{},{} +{},{} @@\n",
            1,
            old_lines.len(),
            1,
            new_lines.len()
        ));

        for line in &removed {
            out.push_str(&format!("-{}\n", line));
        }
        for line in &added {
            out.push_str(&format!("+{}\n", line));
        }
    }

    out
}
