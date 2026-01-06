//! commonplace-log: Show commit history for a synced file (like git log)
//!
//! Usage:
//!   commonplace-log path/to/file.txt               # Full log output
//!   commonplace-log --oneline path/to/file.txt     # Compact one-line format
//!   commonplace-log --stat path/to/file.txt        # Show change statistics
//!   commonplace-log -p path/to/file.txt            # Show diffs
//!   commonplace-log -n 5 path/to/file.txt          # Limit to 5 commits
//!   commonplace-log --since 2024-01-01 path.txt    # Commits after date

use base64::prelude::*;
use clap::Parser;
use commonplace_doc::cli::LogArgs;
use commonplace_doc::workspace::{
    format_timestamp, format_timestamp_short, parse_date, resolve_path_to_uuid,
};
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Transact, Update};

/// Commit with Yjs update from /documents/:id/commits endpoint
#[derive(Deserialize)]
struct CommitWithUpdate {
    commit_id: String,
    timestamp: u64,
    update: String, // base64-encoded Yjs update
    #[allow(dead_code)]
    parents: Vec<String>,
}

#[derive(Deserialize)]
struct CommitsResponse {
    #[allow(dead_code)]
    doc_id: String,
    commits: Vec<CommitWithUpdate>,
}

/// Legacy commit change (for --follow mode which uses SSE)
#[derive(Deserialize)]
struct CommitChange {
    #[allow(dead_code)]
    doc_id: String,
    commit_id: String,
    timestamp: u64,
    #[allow(dead_code)]
    url: String,
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

/// SSE edit event from server
#[derive(Deserialize)]
struct SseEditEvent {
    #[serde(default)]
    cid: Option<String>,
    #[serde(default)]
    timestamp: Option<u64>,
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

    // Fetch commits with Yjs updates (single HTTP request)
    let url = format!("{}/documents/{}/commits", args.server, uuid);
    let resp = client.get(&url).send().await?;

    if !resp.status().is_success() {
        eprintln!("Failed to fetch commits: HTTP {}", resp.status());
        std::process::exit(1);
    }

    let mut commits_resp: CommitsResponse = resp.json().await?;

    // API returns commits in chronological order (oldest first)

    // Apply date filters
    if let Some(ref since) = args.since {
        if let Some(since_ts) = parse_date(since) {
            commits_resp.commits.retain(|c| c.timestamp >= since_ts);
        } else {
            eprintln!("Warning: could not parse --since date: {}", since);
        }
    }

    if let Some(ref until) = args.until {
        if let Some(until_ts) = parse_date(until) {
            commits_resp.commits.retain(|c| c.timestamp <= until_ts);
        } else {
            eprintln!("Warning: could not parse --until date: {}", until);
        }
    }

    // Apply max count limit
    if let Some(max) = args.max_count {
        commits_resp.commits.truncate(max);
    }

    // Apply opt-out flags
    let graph = args.graph && !args.no_graph && !args.oneline;
    let decorate = (args.decorate || graph) && !args.no_decorate;

    // Determine if we need diffs (default on, unless --no-patch or --oneline)
    let show_patch = !args.no_patch && !args.oneline;

    // HEAD is always at the end (newest commit, chronological order)
    let head_idx = commits_resp.commits.len().saturating_sub(1);

    // Stream output using incremental Yjs replay (O(n) not O(n²))
    stream_output_yjs(
        &args,
        &rel_path,
        &uuid,
        &commits_resp.commits,
        show_patch,
        graph,
        decorate,
        head_idx,
    )?;

    // If --follow, subscribe to SSE and watch for new commits
    if args.follow {
        follow_commits(&client, &args, &uuid, graph, decorate).await?;
    }

    Ok(())
}

/// Watch for new commits via SSE and print them as they arrive
async fn follow_commits(
    client: &Client,
    args: &LogArgs,
    uuid: &str,
    graph: bool,
    decorate: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let sse_url = format!("{}/sse/docs/{}", args.server, uuid);

    eprintln!("Watching for new commits... (Ctrl+C to stop)");

    let request_builder = client.get(&sse_url);
    let mut es = EventSource::new(request_builder)?;

    while let Some(event) = es.next().await {
        match event {
            Ok(SseEvent::Open) => {
                // Connection opened
            }
            Ok(SseEvent::Message(msg)) => {
                if msg.event == "edit" {
                    // Parse the edit event to get commit info
                    if let Ok(edit) = serde_json::from_str::<SseEditEvent>(&msg.data) {
                        if let (Some(cid), Some(ts)) = (edit.cid, edit.timestamp) {
                            // Create a minimal commit change for display
                            let change = CommitChange {
                                doc_id: uuid.to_string(),
                                commit_id: cid,
                                timestamp: ts,
                                url: String::new(),
                            };

                            // Format and print the new commit
                            let line = format_single_commit(args, &change, graph, decorate);
                            print!("{}", line);
                            io::stdout().flush()?;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("SSE error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// Format a single commit for --follow output
fn format_single_commit(
    args: &LogArgs,
    change: &CommitChange,
    graph: bool,
    _decorate: bool,
) -> String {
    if args.oneline {
        let cid_short = &change.commit_id[..12.min(change.commit_id.len())];
        let date = format_timestamp_short(change.timestamp);
        format!("\x1b[33m{}\x1b[0m {}\n", cid_short, date)
    } else if graph {
        let cid_short = &change.commit_id[..8.min(change.commit_id.len())];
        let date = format_timestamp_short(change.timestamp);
        format!("* \x1b[33m{}\x1b[0m {}\n| \n", cid_short, date)
    } else {
        format!(
            "\x1b[33mcommit {}\x1b[0m\nDate:   {}\n\n",
            change.commit_id,
            format_timestamp(change.timestamp)
        )
    }
}

/// Stream output using incremental Yjs replay - O(n) not O(n²)
/// Applies Yjs updates incrementally instead of fetching content per-commit
#[allow(clippy::too_many_arguments)]
fn stream_output_yjs(
    args: &LogArgs,
    rel_path: &str,
    uuid: &str,
    commits: &[CommitWithUpdate],
    show_patch: bool,
    graph: bool,
    decorate: bool,
    head_idx: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let stdout = io::stdout();
    let mut out = stdout.lock();

    // Create Yjs doc for incremental replay
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");

    // Track previous content for diff computation
    let mut prev_content = String::new();

    // JSON mode: buffer all data
    if args.json {
        let mut commit_infos = Vec::with_capacity(commits.len());

        for commit in commits {
            // Apply update
            if let Ok(update_bytes) = BASE64_STANDARD.decode(&commit.update) {
                if let Ok(update) = Update::decode_v1(&update_bytes) {
                    let mut txn = doc.transact_mut();
                    txn.apply_update(update);
                }
            }

            let current_content = text.get_string(&doc.transact());

            let stats = if args.stat {
                let s = if prev_content.is_empty() && !current_content.is_empty() {
                    ChangeStats {
                        lines_added: current_content.lines().count(),
                        lines_removed: 0,
                        chars_added: current_content.len(),
                        chars_removed: 0,
                    }
                } else {
                    compute_diff_stats(&prev_content, &current_content)
                };
                prev_content = current_content;
                Some(s)
            } else {
                prev_content = current_content;
                None
            };

            commit_infos.push(CommitInfo {
                cid: commit.commit_id.clone(),
                timestamp: commit.timestamp,
                datetime: format_timestamp(commit.timestamp),
                stats,
            });
        }

        let output = LogOutput {
            uuid: uuid.to_string(),
            path: rel_path.to_string(),
            commits: commit_infos,
        };
        writeln!(out, "{}", serde_json::to_string_pretty(&output)?)?;
        return Ok(());
    }

    // Print header for full output mode
    if !args.oneline && !graph {
        writeln!(out, "File: {}", rel_path)?;
        writeln!(out, "UUID: {}", uuid)?;
        writeln!(out)?;
    }

    for (i, commit) in commits.iter().enumerate() {
        // Apply update to doc and keep raw bytes for --show-yjs
        let raw_update = BASE64_STANDARD.decode(&commit.update).unwrap_or_default();
        if let Ok(update) = Update::decode_v1(&raw_update) {
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let current_content = text.get_string(&doc.transact());

        let decoration = if decorate && i == head_idx {
            " \x1b[36m(HEAD)\x1b[0m"
        } else {
            ""
        };

        // Compute stats if needed
        let stats = if args.stat {
            if prev_content.is_empty() && !current_content.is_empty() {
                Some(ChangeStats {
                    lines_added: current_content.lines().count(),
                    lines_removed: 0,
                    chars_added: current_content.len(),
                    chars_removed: 0,
                })
            } else {
                Some(compute_diff_stats(&prev_content, &current_content))
            }
        } else {
            None
        };

        // Compute diff if needed
        let diff = if show_patch {
            if prev_content.is_empty() && !current_content.is_empty() {
                Some(
                    current_content
                        .lines()
                        .map(|l| format!("+{}", l))
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            } else {
                Some(compute_unified_diff(&prev_content, &current_content))
            }
        } else {
            None
        };

        // Update prev_content
        prev_content = current_content;

        // Format and output this commit
        if args.oneline {
            let cid_short = &commit.commit_id[..12.min(commit.commit_id.len())];
            let date = format_timestamp_short(commit.timestamp);
            if let Some(ref s) = stats {
                write!(
                    out,
                    "\x1b[33m{}\x1b[0m{} {} (+{} -{} chars)",
                    cid_short, decoration, date, s.chars_added, s.chars_removed
                )?;
            } else {
                write!(out, "\x1b[33m{}\x1b[0m{} {}", cid_short, decoration, date)?;
            }
            if args.show_yjs {
                write!(
                    out,
                    " \x1b[90m[{}b] {}\x1b[0m",
                    raw_update.len(),
                    bytes_to_ascii_ish(&raw_update)
                )?;
            }
            writeln!(out)?;
        } else if graph {
            let cid_short = &commit.commit_id[..8.min(commit.commit_id.len())];
            let date = format_timestamp_short(commit.timestamp);
            let is_last = i == commits.len() - 1;
            let connector = if is_last { "  " } else { "| " };

            write!(out, "* \x1b[33m{}\x1b[0m{} {}", cid_short, decoration, date)?;
            if let Some(ref s) = stats {
                write!(out, " (+{} -{} chars)", s.chars_added, s.chars_removed)?;
            }
            writeln!(out)?;

            if let Some(ref d) = diff {
                if !d.is_empty() {
                    for line in colorize_diff(d).lines() {
                        writeln!(out, "{} {}", connector, line)?;
                    }
                }
            }
            if args.show_yjs {
                writeln!(
                    out,
                    "{} \x1b[90m[{} bytes] {}\x1b[0m",
                    connector,
                    raw_update.len(),
                    bytes_to_ascii_ish(&raw_update)
                )?;
            }
            if !is_last {
                writeln!(out, "{}", connector)?;
            }
        } else {
            // Full output
            writeln!(
                out,
                "\x1b[33mcommit {}\x1b[0m{}",
                commit.commit_id, decoration
            )?;
            writeln!(out, "Date:   {}", format_timestamp(commit.timestamp))?;

            if let Some(ref s) = stats {
                writeln!(out)?;
                writeln!(
                    out,
                    " {} chars (+{}/-{}), {} lines (+{}/-{})",
                    s.chars_added + s.chars_removed,
                    s.chars_added,
                    s.chars_removed,
                    s.lines_added + s.lines_removed,
                    s.lines_added,
                    s.lines_removed
                )?;
            }

            if let Some(ref d) = diff {
                if !d.is_empty() {
                    writeln!(out)?;
                    write!(out, "{}", colorize_diff(d))?;
                }
            }

            if args.show_yjs {
                writeln!(out)?;
                writeln!(
                    out,
                    "\x1b[90mYjs: [{} bytes] {}\x1b[0m",
                    raw_update.len(),
                    bytes_to_ascii_ish(&raw_update)
                )?;
            }

            writeln!(out)?;
        }

        out.flush()?;
    }

    if !args.oneline && !graph {
        writeln!(out, "{} commits", commits.len())?;
    }

    Ok(())
}

/// Render bytes as ASCII-ish: printable chars shown, non-printable as dots
fn bytes_to_ascii_ish(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| {
            if (0x20..0x7f).contains(&b) {
                b as char
            } else {
                '.'
            }
        })
        .collect()
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

fn compute_diff_stats(old: &str, new: &str) -> ChangeStats {
    use similar::{ChangeTag, TextDiff};

    let diff = TextDiff::from_lines(old, new);
    let mut lines_added = 0;
    let mut lines_removed = 0;
    let mut chars_added = 0;
    let mut chars_removed = 0;

    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Insert => {
                lines_added += 1;
                chars_added += change.value().len();
            }
            ChangeTag::Delete => {
                lines_removed += 1;
                chars_removed += change.value().len();
            }
            ChangeTag::Equal => {}
        }
    }

    ChangeStats {
        lines_added,
        lines_removed,
        chars_added,
        chars_removed,
    }
}

fn compute_unified_diff(old: &str, new: &str) -> String {
    use similar::TextDiff;

    let diff = TextDiff::from_lines(old, new);
    diff.unified_diff()
        .context_radius(3)
        .header("old", "new")
        .to_string()
}
