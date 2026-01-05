//! commonplace-bd (cbd): Bug database CLI backed by JSONL in commonplace
//!
//! Works directly with the commonplace server API - no disk sync needed.
//!
//! Usage:
//!   cbd list                    # List all issues
//!   cbd ready                   # List issues ready to work on
//!   cbd show <id>               # Show issue details
//!   cbd create <title>          # Create new issue (requires server fix for JSONL)
//!   cbd close <id> [reason]     # Close an issue (requires server fix for JSONL)
//!   cbd update <id> --status X  # Update issue fields (requires server fix for JSONL)
//!
//! NOTE: Write operations (create/close/update) currently require a server fix.
//! The server treats JSONL as JSON type, causing replace to fail.
//! Read operations (list/ready/show) work correctly.

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Default server URL
const DEFAULT_SERVER: &str = "http://localhost:3000";

/// Default path to issues JSONL in commonplace
const DEFAULT_PATH: &str = "beads/commonplace-issues.jsonl";

#[derive(Parser)]
#[command(name = "cbd", about = "Commonplace Bug Database CLI")]
struct Cli {
    /// Server URL
    #[arg(long, default_value = DEFAULT_SERVER, env = "COMMONPLACE_SERVER")]
    server: String,

    /// Path to issues JSONL in commonplace
    #[arg(long, default_value = DEFAULT_PATH, env = "CBD_PATH")]
    path: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List all issues
    List {
        /// Filter by status (open, closed, all)
        #[arg(long, default_value = "open")]
        status: String,
    },
    /// List issues ready to work on (open, no blockers)
    Ready,
    /// Show issue details
    Show { id: String },
    /// Create a new issue
    Create {
        /// Issue title
        title: String,
        /// Issue type (bug, feature, task, chore)
        #[arg(long, short = 't', default_value = "task")]
        issue_type: String,
        /// Priority (1-4, 1=highest)
        #[arg(long, short = 'p', default_value = "2")]
        priority: u8,
        /// Description
        #[arg(long, short = 'd')]
        description: Option<String>,
    },
    /// Close an issue
    Close {
        /// Issue ID
        id: String,
        /// Reason for closing
        #[arg(long, short = 'r')]
        reason: Option<String>,
    },
    /// Update an issue
    Update {
        /// Issue ID
        id: String,
        /// New status
        #[arg(long)]
        status: Option<String>,
        /// New priority
        #[arg(long)]
        priority: Option<u8>,
        /// New title
        #[arg(long)]
        title: Option<String>,
    },
}

/// Issue structure matching beads format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Issue {
    id: String,
    title: String,
    #[serde(default)]
    description: String,
    status: String,
    priority: u8,
    issue_type: String,
    created_at: String,
    #[serde(default)]
    created_by: String,
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    close_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dependencies: Option<Vec<Dependency>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Dependency {
    issue_id: String,
    depends_on_id: String,
    #[serde(rename = "type")]
    dep_type: String,
    created_at: String,
    created_by: String,
}

#[derive(Debug, Deserialize)]
struct HeadResponse {
    #[allow(dead_code)]
    cid: String,
    content: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = Client::new();

    match &cli.command {
        Commands::List { status } => cmd_list(&client, &cli, status),
        Commands::Ready => cmd_ready(&client, &cli),
        Commands::Show { id } => cmd_show(&client, &cli, id),
        Commands::Create {
            title,
            issue_type,
            priority,
            description,
        } => cmd_create(
            &client,
            &cli,
            title,
            issue_type,
            *priority,
            description.clone(),
        ),
        Commands::Close { id, reason } => cmd_close(&client, &cli, id, reason.clone()),
        Commands::Update {
            id,
            status,
            priority,
            title,
        } => cmd_update(&client, &cli, id, status.clone(), *priority, title.clone()),
    }
}

/// Resolve a path to a UUID via fs-root schema
fn resolve_path_to_uuid(
    client: &Client,
    server: &str,
    path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // Get fs-root
    let fs_root_url = format!("{}/fs-root", server);
    let resp = client.get(&fs_root_url).send()?;
    if !resp.status().is_success() {
        return Err(format!("Failed to get fs-root: {}", resp.status()).into());
    }

    #[derive(Deserialize)]
    struct FsRootResponse {
        id: String,
    }
    let fs_root: FsRootResponse = resp.json()?;

    // Get fs-root schema
    let schema_url = format!("{}/docs/{}/head", server, fs_root.id);
    let resp = client.get(&schema_url).send()?;
    if !resp.status().is_success() {
        return Err(format!("Failed to get fs-root schema: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json()?;

    #[derive(Deserialize)]
    struct Schema {
        root: SchemaRoot,
    }
    #[derive(Deserialize)]
    struct SchemaRoot {
        entries: Option<HashMap<String, SchemaEntry>>,
    }
    #[derive(Deserialize, Clone)]
    struct SchemaEntry {
        node_id: Option<String>,
        entries: Option<HashMap<String, SchemaEntry>>,
    }

    let schema: Schema = serde_json::from_str(&head.content)?;

    // Walk path segments
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let mut current_entries = schema.root.entries;

    for (i, part) in parts.iter().enumerate() {
        let entries = current_entries.ok_or_else(|| format!("Path not found: {}", path))?;
        let entry = entries
            .get(*part)
            .ok_or_else(|| format!("Path segment '{}' not found", part))?;

        if i == parts.len() - 1 {
            // Last segment - return node_id
            return entry
                .node_id
                .clone()
                .ok_or_else(|| format!("No node_id for path: {}", path).into());
        }

        // Intermediate segment - need to fetch subdirectory schema
        if let Some(node_id) = &entry.node_id {
            let subdir_url = format!("{}/docs/{}/head", server, node_id);
            let resp = client.get(&subdir_url).send()?;
            if !resp.status().is_success() {
                return Err(format!("Failed to get subdir schema: {}", resp.status()).into());
            }
            let head: HeadResponse = resp.json()?;
            let sub_schema: Schema = serde_json::from_str(&head.content)?;
            current_entries = sub_schema.root.entries;
        } else {
            current_entries = entry.entries.clone();
        }
    }

    Err(format!("Path not found: {}", path).into())
}

/// Fetch all issues from the server
fn fetch_issues(client: &Client, cli: &Cli) -> Result<Vec<Issue>, Box<dyn std::error::Error>> {
    // Resolve path to UUID
    let uuid = resolve_path_to_uuid(client, &cli.server, &cli.path)?;

    let url = format!("{}/docs/{}/head", cli.server, uuid);

    let resp = client.get(&url).send()?;

    if !resp.status().is_success() {
        if resp.status().as_u16() == 404 {
            // Document doesn't exist yet - return empty list
            return Ok(Vec::new());
        }
        return Err(format!("Server error: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json()?;

    // Parse JSONL content
    let mut issues = Vec::new();
    for line in head.content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Issue>(line) {
            Ok(issue) => issues.push(issue),
            Err(e) => eprintln!("Warning: Failed to parse issue: {}", e),
        }
    }

    Ok(issues)
}

/// Build an index of issues by ID (last occurrence wins for updates)
fn build_issue_map(issues: Vec<Issue>) -> HashMap<String, Issue> {
    let mut map = HashMap::new();
    for issue in issues {
        map.insert(issue.id.clone(), issue);
    }
    map
}

/// Append a new issue line to the JSONL
fn append_issue(
    client: &Client,
    cli: &Cli,
    issue: &Issue,
) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve path to UUID
    let uuid = resolve_path_to_uuid(client, &cli.server, &cli.path)?;

    // Fetch current content and CID
    let url = format!("{}/docs/{}/head", cli.server, uuid);
    let resp = client.get(&url).send()?;

    let (current_content, parent_cid) = if resp.status().is_success() {
        let head: HeadResponse = resp.json()?;
        (head.content, head.cid)
    } else {
        (String::new(), String::new())
    };

    // Append new issue
    let new_line = serde_json::to_string(issue)?;
    let new_content = if current_content.is_empty() {
        format!("{}\n", new_line)
    } else if current_content.ends_with('\n') {
        format!("{}{}\n", current_content, new_line)
    } else {
        format!("{}\n{}\n", current_content, new_line)
    };

    // Replace content with parent CID
    let replace_url = if parent_cid.is_empty() {
        format!("{}/docs/{}/replace", cli.server, uuid)
    } else {
        format!("{}/docs/{}/replace?parent={}", cli.server, uuid, parent_cid)
    };

    let resp = client
        .post(&replace_url)
        .header("Content-Type", "text/plain")
        .body(new_content)
        .send()?;

    if !resp.status().is_success() {
        let body = resp.text().unwrap_or_default();
        return Err(format!("Failed to update: {}", body).into());
    }

    Ok(())
}

/// Generate a new issue ID using UUID
fn generate_id() -> String {
    // Use first 4 chars of uuid for short readable ID
    let uuid = uuid::Uuid::new_v4();
    let short = &uuid.to_string()[0..4];
    format!("CP-{}", short)
}

fn cmd_list(
    client: &Client,
    cli: &Cli,
    status_filter: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let map = build_issue_map(issues);

    let mut filtered: Vec<_> = map
        .values()
        .filter(|i| match status_filter {
            "open" => i.status == "open",
            "closed" => i.status == "closed",
            "all" => true,
            _ => i.status == status_filter,
        })
        .collect();

    // Sort by priority, then by ID
    filtered.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.id.cmp(&b.id)));

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&filtered)?);
    } else {
        if filtered.is_empty() {
            println!("No issues found");
            return Ok(());
        }
        for issue in filtered {
            println!(
                "[P{}] [{}] {} - {}",
                issue.priority, issue.issue_type, issue.id, issue.title
            );
        }
    }

    Ok(())
}

fn cmd_ready(client: &Client, cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let map = build_issue_map(issues);

    // Get all open issues
    let open_issues: Vec<_> = map.values().filter(|i| i.status == "open").collect();

    // Find issues that have blockers
    let blocked_ids: std::collections::HashSet<_> = open_issues
        .iter()
        .filter_map(|i| i.dependencies.as_ref())
        .flatten()
        .filter(|d| d.dep_type == "blocks")
        .map(|d| d.issue_id.clone())
        .collect();

    // Ready = open and not blocked
    let mut ready: Vec<_> = open_issues
        .into_iter()
        .filter(|i| !blocked_ids.contains(&i.id))
        .collect();

    ready.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.id.cmp(&b.id)));

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&ready)?);
    } else {
        if ready.is_empty() {
            println!("No ready issues");
            return Ok(());
        }
        println!("📋 Ready work ({} issues):\n", ready.len());
        for (i, issue) in ready.iter().enumerate() {
            println!(
                "{}. [P{}] [{}] {}: {}",
                i + 1,
                issue.priority,
                issue.issue_type,
                issue.id,
                issue.title
            );
        }
    }

    Ok(())
}

fn cmd_show(client: &Client, cli: &Cli, id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let map = build_issue_map(issues);

    let issue = map
        .get(id)
        .ok_or_else(|| format!("Issue {} not found", id))?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(issue)?);
    } else {
        println!("{}: {}", issue.id, issue.title);
        println!("Status: {}", issue.status);
        println!("Priority: P{}", issue.priority);
        println!("Type: {}", issue.issue_type);
        println!("Created: {}", issue.created_at);
        if !issue.description.is_empty() {
            println!("\nDescription:\n{}", issue.description);
        }
        if let Some(reason) = &issue.close_reason {
            println!("\nClose reason: {}", reason);
        }
    }

    Ok(())
}

fn cmd_create(
    client: &Client,
    cli: &Cli,
    title: &str,
    issue_type: &str,
    priority: u8,
    description: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now: DateTime<Utc> = Utc::now();
    let now_str = now.to_rfc3339();

    let issue = Issue {
        id: generate_id(),
        title: title.to_string(),
        description: description.unwrap_or_default(),
        status: "open".to_string(),
        priority,
        issue_type: issue_type.to_string(),
        created_at: now_str.clone(),
        created_by: std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()),
        updated_at: now_str,
        closed_at: None,
        close_reason: None,
        labels: None,
        dependencies: None,
    };

    append_issue(client, cli, &issue)?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&issue)?);
    } else {
        println!("✓ Created {}: {}", issue.id, issue.title);
    }

    Ok(())
}

fn cmd_close(
    client: &Client,
    cli: &Cli,
    id: &str,
    reason: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let mut map = build_issue_map(issues);

    let issue = map
        .get_mut(id)
        .ok_or_else(|| format!("Issue {} not found", id))?;

    if issue.status == "closed" {
        return Err(format!("Issue {} is already closed", id).into());
    }

    let now: DateTime<Utc> = Utc::now();
    let now_str = now.to_rfc3339();

    issue.status = "closed".to_string();
    issue.updated_at = now_str.clone();
    issue.closed_at = Some(now_str);
    issue.close_reason = reason;

    // Append the updated issue (JSONL semantics: last entry wins)
    append_issue(client, cli, issue)?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(issue)?);
    } else {
        println!("✓ Closed {}: {}", issue.id, issue.title);
    }

    Ok(())
}

fn cmd_update(
    client: &Client,
    cli: &Cli,
    id: &str,
    status: Option<String>,
    priority: Option<u8>,
    title: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let mut map = build_issue_map(issues);

    let issue = map
        .get_mut(id)
        .ok_or_else(|| format!("Issue {} not found", id))?;

    let now: DateTime<Utc> = Utc::now();
    issue.updated_at = now.to_rfc3339();

    if let Some(s) = status {
        issue.status = s;
    }
    if let Some(p) = priority {
        issue.priority = p;
    }
    if let Some(t) = title {
        issue.title = t;
    }

    append_issue(client, cli, issue)?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(issue)?);
    } else {
        println!("✓ Updated {}: {}", issue.id, issue.title);
    }

    Ok(())
}
