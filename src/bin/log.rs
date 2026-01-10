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
use commonplace_doc::cli::{CommitChange, LogArgs};
use commonplace_doc::workspace::{
    format_timestamp, format_timestamp_short, parse_date, resolve_path_to_uuid,
};
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Write};
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Transact, Update};

/// DAG graph state for rendering git-like ASCII topology.
/// Works in either direction: newest-first (like git log) or oldest-first (--reverse).
struct DagGraph {
    /// Active columns: each entry is the commit ID that column is waiting for.
    /// None means empty/available slot.
    columns: Vec<Option<String>>,
    /// Child map for oldest-first traversal: commit_id -> children
    children: HashMap<String, Vec<String>>,
    /// Direction: true = oldest-first (reverse), false = newest-first
    reverse: bool,
}

impl DagGraph {
    /// Create a new DagGraph
    /// - `reverse`: true for oldest-first (--reverse), false for newest-first (default)
    /// - `commits`: needed to build child map for reverse mode
    fn new(reverse: bool, commits: &[PrecomputedCommit]) -> Self {
        let mut children: HashMap<String, Vec<String>> = HashMap::new();

        if reverse {
            // Build child map from parent relationships
            for commit in commits {
                for parent in &commit.parents {
                    children
                        .entry(parent.clone())
                        .or_default()
                        .push(commit.commit_id.clone());
                }
            }
        }

        Self {
            columns: vec![],
            children,
            reverse,
        }
    }

    /// Process a commit and return (graph_prefix, merge_lines_after)
    fn process_commit(&mut self, commit_id: &str, parents: &[String]) -> (String, Vec<String>) {
        if self.reverse {
            self.process_commit_reverse(commit_id)
        } else {
            self.process_commit_forward(commit_id, parents)
        }
    }

    /// Process commit in newest-first order (default, like git log)
    fn process_commit_forward(
        &mut self,
        commit_id: &str,
        parents: &[String],
    ) -> (String, Vec<String>) {
        // Find which column is waiting for this commit (if any)
        let my_col = self
            .columns
            .iter()
            .position(|c| c.as_ref().map(|s| s.as_str()) == Some(commit_id));

        let col = if let Some(c) = my_col {
            c
        } else {
            self.columns
                .iter()
                .position(|c| c.is_none())
                .unwrap_or_else(|| {
                    self.columns.push(None);
                    self.columns.len() - 1
                })
        };

        let prefix = self.build_prefix(col);
        let mut merge_lines = Vec::new();

        if parents.is_empty() {
            // Root commit - this column becomes empty
            if col < self.columns.len() {
                self.columns[col] = None;
            }
        } else {
            // First parent continues in our column
            if col < self.columns.len() {
                self.columns[col] = Some(parents[0].clone());
            } else {
                self.columns.push(Some(parents[0].clone()));
            }

            // Additional parents need new columns
            for parent in parents.iter().skip(1) {
                let parent_col = self.find_or_create_column_right_of(col);
                self.columns[parent_col] = Some(parent.clone());
                merge_lines.push(self.draw_branch_line(col, parent_col));
            }
        }

        self.cleanup_columns();
        (prefix, merge_lines)
    }

    /// Process commit in oldest-first order (--reverse)
    fn process_commit_reverse(&mut self, commit_id: &str) -> (String, Vec<String>) {
        // Find which column is waiting for this commit (assigned by parent below)
        let my_col = self
            .columns
            .iter()
            .position(|c| c.as_ref().map(|s| s.as_str()) == Some(commit_id));

        // Prefer leftmost available column
        let col = if let Some(c) = my_col {
            c
        } else {
            self.find_or_create_empty_column()
        };

        let prefix = self.build_prefix(col);
        let mut merge_lines = Vec::new();

        // Look up children of this commit
        let commit_children = self.children.get(commit_id).cloned().unwrap_or_default();

        if commit_children.is_empty() {
            // Leaf commit (no children) - this column becomes empty
            if col < self.columns.len() {
                self.columns[col] = None;
            }
        } else {
            // Check if first child is already assigned to another column (merge convergence)
            let first_child = &commit_children[0];
            let existing_col = self
                .columns
                .iter()
                .position(|c| c.as_ref().map(|s| s.as_str()) == Some(first_child.as_str()));

            if let Some(existing) = existing_col {
                // Child already has a column - this branch converges
                // Draw merge line and clear our column
                if existing != col {
                    merge_lines.push(self.draw_converge_line(col, existing));
                }
                if col < self.columns.len() {
                    self.columns[col] = None;
                }
            } else {
                // First child - clear our column first, then find leftmost available
                if col < self.columns.len() {
                    self.columns[col] = None;
                }
                let target_col = self.find_or_create_empty_column();
                if target_col < self.columns.len() {
                    self.columns[target_col] = Some(first_child.clone());
                } else {
                    self.columns.push(Some(first_child.clone()));
                }
            }

            // Additional children - prefer leftmost available columns
            for child in commit_children.iter().skip(1) {
                let existing_col = self
                    .columns
                    .iter()
                    .position(|c| c.as_ref().map(|s| s.as_str()) == Some(child.as_str()));

                if existing_col.is_none() {
                    // Child needs a new column - use leftmost available
                    let child_col = self.find_or_create_empty_column();
                    self.columns[child_col] = Some(child.clone());
                    if child_col != col {
                        merge_lines.push(self.draw_branch_line(col, child_col));
                    }
                }
            }
        }

        self.cleanup_columns();
        (prefix, merge_lines)
    }

    /// Draw a line showing branches converging (for oldest-first mode)
    fn draw_converge_line(&self, from_col: usize, to_col: usize) -> String {
        let max_col = from_col.max(to_col);

        let chars: String = (0..=max_col)
            .map(|i| {
                if i == to_col {
                    '|'
                } else if i == from_col {
                    '/'
                } else if i > to_col && i < from_col {
                    '_'
                } else if i < self.columns.len() && self.columns[i].is_some() {
                    '|'
                } else {
                    ' '
                }
            })
            .collect();

        chars
    }

    fn build_prefix(&self, col: usize) -> String {
        let mut chars: Vec<char> = self
            .columns
            .iter()
            .map(|c| if c.is_some() { '|' } else { ' ' })
            .collect();
        while chars.len() <= col {
            chars.push(' ');
        }
        chars[col] = '*';

        // Trim trailing spaces
        while chars.last() == Some(&' ') && chars.len() > col + 1 {
            chars.pop();
        }

        chars.iter().collect()
    }

    /// Find leftmost empty column, or create a new one
    fn find_or_create_empty_column(&mut self) -> usize {
        self.columns
            .iter()
            .position(|c| c.is_none())
            .unwrap_or_else(|| {
                self.columns.push(None);
                self.columns.len() - 1
            })
    }

    /// Find empty column to the right of col (for newest-first branch spawning)
    fn find_or_create_column_right_of(&mut self, col: usize) -> usize {
        self.columns
            .iter()
            .enumerate()
            .skip(col + 1)
            .find(|(_, c)| c.is_none())
            .map(|(i, _)| i)
            .unwrap_or_else(|| {
                self.columns.push(None);
                self.columns.len() - 1
            })
    }

    fn cleanup_columns(&mut self) {
        while self.columns.last() == Some(&None) {
            self.columns.pop();
        }
    }

    /// Get connector line (continuation lines between commits)
    fn get_connector(&self) -> String {
        if self.columns.is_empty() {
            return String::new();
        }

        let chars: String = self
            .columns
            .iter()
            .map(|c| if c.is_some() { '|' } else { ' ' })
            .collect();

        chars.trim_end().to_string()
    }

    /// Draw a line showing a branch point
    fn draw_branch_line(&self, col: usize, other_col: usize) -> String {
        let max_col = col.max(other_col);

        let chars: String = (0..=max_col)
            .map(|i| {
                if i == col {
                    '|'
                } else if i == other_col {
                    '\\'
                } else if i > col && i < other_col {
                    '_'
                } else if i < self.columns.len() && self.columns[i].is_some() {
                    '|'
                } else {
                    ' '
                }
            })
            .collect();

        chars
    }
}

/// Commit with Yjs update from /documents/:id/commits endpoint
#[derive(Deserialize, Clone)]
struct CommitWithUpdate {
    commit_id: String,
    timestamp: u64,
    update: String, // base64-encoded Yjs update
    parents: Vec<String>,
}

/// Pre-computed commit display data (computed oldest-first, displayed in any order)
struct PrecomputedCommit {
    commit_id: String,
    timestamp: u64,
    parents: Vec<String>,
    raw_update: Vec<u8>,
    stats: Option<ChangeStats>,
    diff: Option<String>,
    is_head: bool,
}

#[derive(Deserialize)]
struct CommitsResponse {
    #[allow(dead_code)]
    doc_id: String,
    commits: Vec<CommitWithUpdate>,
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

/// Pre-compute all commit data in chronological order (oldest-first) for efficient Yjs replay
fn precompute_commits(
    commits: &[CommitWithUpdate],
    compute_stats: bool,
    compute_diff: bool,
    head_idx: usize,
) -> Vec<PrecomputedCommit> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");

    let mut content_by_commit: HashMap<String, String> = HashMap::new();
    content_by_commit.insert(String::new(), String::new());

    let mut result = Vec::with_capacity(commits.len());

    for (i, commit) in commits.iter().enumerate() {
        // Decode and apply Yjs update
        let raw_update = BASE64_STANDARD.decode(&commit.update).unwrap_or_else(|e| {
            eprintln!(
                "Warning: commit {} has invalid base64 update: {}",
                commit.commit_id, e
            );
            Vec::new()
        });

        if let Ok(update) = Update::decode_v1(&raw_update) {
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let current_content = text.get_string(&doc.transact());

        // Get parent content (first parent or empty for root)
        let parent_content = if commit.parents.is_empty() {
            String::new()
        } else {
            content_by_commit
                .get(&commit.parents[0])
                .cloned()
                .unwrap_or_default()
        };

        let stats = if compute_stats {
            if parent_content.is_empty() && !current_content.is_empty() {
                Some(ChangeStats {
                    lines_added: current_content.lines().count(),
                    lines_removed: 0,
                    chars_added: current_content.len(),
                    chars_removed: 0,
                })
            } else {
                Some(compute_diff_stats(&parent_content, &current_content))
            }
        } else {
            None
        };

        let diff = if compute_diff {
            if parent_content.is_empty() && !current_content.is_empty() {
                Some(
                    current_content
                        .lines()
                        .map(|l| format!("+{}", l))
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            } else {
                Some(compute_unified_diff(&parent_content, &current_content))
            }
        } else {
            None
        };

        content_by_commit.insert(commit.commit_id.clone(), current_content);

        result.push(PrecomputedCommit {
            commit_id: commit.commit_id.clone(),
            timestamp: commit.timestamp,
            parents: commit.parents.clone(),
            raw_update,
            stats,
            diff,
            is_head: i == head_idx,
        });
    }

    result
}

/// Stream output using incremental Yjs replay - O(n) not O(n²)
/// Computes in chronological order, displays in chosen order (newest-first by default)
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

    // JSON mode: compute and output
    if args.json {
        let precomputed = precompute_commits(commits, args.stat, false, head_idx);
        let commit_infos: Vec<CommitInfo> = precomputed
            .iter()
            .map(|c| CommitInfo {
                cid: c.commit_id.clone(),
                timestamp: c.timestamp,
                datetime: format_timestamp(c.timestamp),
                stats: c.stats.clone(),
            })
            .collect();

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

    // Pre-compute all commit data (oldest-first for Yjs efficiency)
    let precomputed = precompute_commits(commits, args.stat, show_patch, head_idx);

    // Display order: newest-first by default, oldest-first with --reverse
    let display_order: Vec<usize> = if args.reverse {
        (0..precomputed.len()).collect()
    } else {
        (0..precomputed.len()).rev().collect()
    };

    // Create DAG graph for graph mode (processes in display order)
    let mut dag = if graph {
        Some(DagGraph::new(args.reverse, &precomputed))
    } else {
        None
    };

    for idx in display_order {
        let commit = &precomputed[idx];
        let is_merge = commit.parents.len() > 1;

        let decoration = if decorate && commit.is_head {
            if is_merge {
                format!(
                    " \x1b[36m(HEAD)\x1b[0m \x1b[35m(merge: {})\x1b[0m",
                    commit
                        .parents
                        .iter()
                        .map(|p| &p[..8.min(p.len())])
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                " \x1b[36m(HEAD)\x1b[0m".to_string()
            }
        } else if is_merge {
            format!(
                " \x1b[35m(merge: {})\x1b[0m",
                commit
                    .parents
                    .iter()
                    .map(|p| &p[..8.min(p.len())])
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            String::new()
        };

        // Format and output this commit
        if args.oneline {
            let cid_short = &commit.commit_id[..12.min(commit.commit_id.len())];
            let date = format_timestamp_short(commit.timestamp);
            if let Some(ref s) = commit.stats {
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
                    commit.raw_update.len(),
                    bytes_to_ascii_ish(&commit.raw_update)
                )?;
            }
            writeln!(out)?;
        } else if graph {
            let dag = dag.as_mut().unwrap();
            let cid_short = &commit.commit_id[..8.min(commit.commit_id.len())];
            let date = format_timestamp_short(commit.timestamp);

            // Get graph prefix and merge lines
            let (prefix, merge_lines) = dag.process_commit(&commit.commit_id, &commit.parents);

            // Write the commit line
            write!(
                out,
                "{} \x1b[33m{}\x1b[0m{} {}",
                prefix, cid_short, decoration, date
            )?;
            if let Some(ref s) = commit.stats {
                write!(out, " (+{} -{} chars)", s.chars_added, s.chars_removed)?;
            }
            writeln!(out)?;

            // Draw merge lines if any
            for line in &merge_lines {
                writeln!(out, "{}", line)?;
            }

            // Get connector for continuation lines
            let connector = dag.get_connector();
            let connector_prefix = if connector.is_empty() {
                "  ".to_string()
            } else {
                format!("{} ", connector)
            };

            if let Some(ref d) = commit.diff {
                if !d.is_empty() {
                    for line in colorize_diff(d).lines() {
                        writeln!(out, "{}{}", connector_prefix, line)?;
                    }
                }
            }
            if args.show_yjs {
                writeln!(
                    out,
                    "{}\x1b[90m[{} bytes] {}\x1b[0m",
                    connector_prefix,
                    commit.raw_update.len(),
                    bytes_to_ascii_ish(&commit.raw_update)
                )?;
            }
            // Add a blank connector line between commits
            if !connector.is_empty() {
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

            if let Some(ref s) = commit.stats {
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

            if let Some(ref d) = commit.diff {
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
                    commit.raw_update.len(),
                    bytes_to_ascii_ish(&commit.raw_update)
                )?;
            }

            writeln!(out)?;
        }

        out.flush()?;
    }

    if !args.oneline && !graph {
        writeln!(out, "{} commits", precomputed.len())?;
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
