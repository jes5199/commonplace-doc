//! cbd: Commonplace Bug Database
//!
//! A CLI for viewing and managing beads issues stored in commonplace.
//! Works directly with the commonplace server API - no disk sync needed.
//!
//! Usage:
//!   cbd list                    # List all issues
//!   cbd ready                   # List issues ready to work on
//!   cbd show <id>               # Show issue details
//!   cbd create <title>          # Create new issue
//!   cbd close <id> [reason]     # Close an issue
//!   cbd update <id> --status X  # Update issue fields

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Default server URL
const DEFAULT_SERVER: &str = "http://localhost:3000";

/// Default path to issues JSONL in commonplace
const DEFAULT_PATH: &str = "beads/commonplace-issues.jsonl";

/// Config file structure for .cbd.json
#[derive(Debug, Deserialize)]
struct CbdConfig {
    /// Path to issues JSONL in commonplace workspace
    path: String,
    /// Optional server URL override
    server: Option<String>,
}

/// Discover .cbd.json by walking up directories from cwd
fn discover_config() -> Option<CbdConfig> {
    let mut dir = std::env::current_dir().ok()?;

    loop {
        // Check .cbd.json in current dir
        let config_path = dir.join(".cbd.json");
        if config_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&config_path) {
                if let Ok(config) = serde_json::from_str::<CbdConfig>(&content) {
                    return Some(config);
                }
            }
        }

        // Check .beads/.cbd.json
        let beads_config = dir.join(".beads").join(".cbd.json");
        if beads_config.exists() {
            if let Ok(content) = std::fs::read_to_string(&beads_config) {
                if let Ok(config) = serde_json::from_str::<CbdConfig>(&content) {
                    return Some(config);
                }
            }
        }

        // Move to parent
        if !dir.pop() {
            break;
        }
    }

    None
}

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
        /// Filter by issue type (bug, feature, task, chore)
        #[arg(long, short = 't')]
        issue_type: Option<String>,
        /// Filter by priority (1-4)
        #[arg(long, short = 'p')]
        priority: Option<u8>,
        /// Filter by label
        #[arg(long, short = 'l')]
        label: Option<String>,
        /// Search in title and description
        #[arg(long, short = 's')]
        search: Option<String>,
        /// Sort by field (priority, created, updated)
        #[arg(long, default_value = "priority")]
        sort: String,
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
        /// Read description from file (use - for stdin)
        #[arg(long)]
        body_file: Option<String>,
        /// Explicit issue ID (e.g., 'CP-42')
        #[arg(long)]
        id: Option<String>,
        /// Assignee
        #[arg(long, short = 'a')]
        assignee: Option<String>,
        /// Labels (comma-separated)
        #[arg(long, short = 'l', value_delimiter = ',')]
        labels: Option<Vec<String>>,
        /// Parent issue ID
        #[arg(long)]
        parent: Option<String>,
        /// Dependencies in format 'type:id' or 'id' (blocks by default)
        #[arg(long, value_delimiter = ',')]
        deps: Option<Vec<String>>,
        /// Due date
        #[arg(long)]
        due: Option<String>,
        /// Defer until date
        #[arg(long)]
        defer: Option<String>,
        /// Time estimate in minutes
        #[arg(long, short = 'e')]
        estimate: Option<i64>,
        /// Acceptance criteria
        #[arg(long)]
        acceptance: Option<String>,
        /// Design notes
        #[arg(long)]
        design: Option<String>,
        /// Additional notes
        #[arg(long)]
        notes: Option<String>,
        /// External reference (e.g., 'gh-9', 'jira-ABC')
        #[arg(long)]
        external_ref: Option<String>,
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
        /// New issue type
        #[arg(long, short = 't')]
        issue_type: Option<String>,
        /// New description
        #[arg(long, short = 'd')]
        description: Option<String>,
        /// Read description from file (use - for stdin)
        #[arg(long)]
        body_file: Option<String>,
        /// New assignee
        #[arg(long, short = 'a')]
        assignee: Option<String>,
        /// New labels (comma-separated, replaces existing)
        #[arg(long, short = 'l', value_delimiter = ',')]
        labels: Option<Vec<String>>,
        /// New parent issue ID
        #[arg(long)]
        parent: Option<String>,
        /// New due date
        #[arg(long)]
        due: Option<String>,
        /// New defer date
        #[arg(long)]
        defer: Option<String>,
        /// New time estimate in minutes
        #[arg(long, short = 'e')]
        estimate: Option<i64>,
        /// New acceptance criteria
        #[arg(long)]
        acceptance: Option<String>,
        /// New design notes
        #[arg(long)]
        design: Option<String>,
        /// New additional notes
        #[arg(long)]
        notes: Option<String>,
        /// New external reference
        #[arg(long)]
        external_ref: Option<String>,
        /// Close reason (used when status=closed)
        #[arg(long, short = 'r')]
        reason: Option<String>,
    },
    /// Manage dependencies
    Dep {
        #[command(subcommand)]
        action: DepAction,
    },
    /// List blocked issues (open issues with unresolved blockers)
    Blocked,
}

#[derive(Subcommand)]
enum DepAction {
    /// Add a dependency (issue_id depends on depends_on_id)
    Add {
        /// The issue that is blocked
        issue_id: String,
        /// The issue that blocks it
        depends_on_id: String,
    },
    /// Remove a dependency
    Remove {
        /// The issue that was blocked
        issue_id: String,
        /// The issue that was blocking it
        depends_on_id: String,
    },
    /// List dependencies for an issue
    List {
        /// Issue ID
        issue_id: String,
    },
}

/// Options for creating a new issue
#[derive(Debug)]
struct CreateOptions {
    title: String,
    issue_type: String,
    priority: u8,
    description: Option<String>,
    body_file: Option<String>,
    id: Option<String>,
    assignee: Option<String>,
    labels: Option<Vec<String>>,
    parent: Option<String>,
    deps: Option<Vec<String>>,
    due: Option<String>,
    defer: Option<String>,
    estimate: Option<i64>,
    acceptance: Option<String>,
    design: Option<String>,
    notes: Option<String>,
    external_ref: Option<String>,
}

/// Options for updating an issue
#[derive(Debug)]
struct UpdateOptions {
    status: Option<String>,
    priority: Option<u8>,
    title: Option<String>,
    issue_type: Option<String>,
    description: Option<String>,
    body_file: Option<String>,
    assignee: Option<String>,
    labels: Option<Vec<String>>,
    parent: Option<String>,
    due: Option<String>,
    defer: Option<String>,
    estimate: Option<i64>,
    acceptance: Option<String>,
    design: Option<String>,
    notes: Option<String>,
    external_ref: Option<String>,
    close_reason: Option<String>,
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
    // Additional beads fields
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    due: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    defer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimate: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acceptance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    design: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    comments: Option<Vec<serde_json::Value>>,
    // Preserve any unknown fields for round-trip fidelity
    #[serde(flatten)]
    extra: HashMap<String, serde_json::Value>,
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

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cli = Cli::parse();
    let client = Client::new();

    // Apply discovered config if CLI args are defaults
    if let Some(config) = discover_config() {
        // Only override if user didn't explicitly set via CLI/env
        if cli.path == DEFAULT_PATH {
            cli.path = config.path;
        }
        if cli.server == DEFAULT_SERVER {
            if let Some(server) = config.server {
                cli.server = server;
            }
        }
    }

    match &cli.command {
        Commands::List {
            status,
            issue_type,
            priority,
            label,
            search,
            sort,
        } => cmd_list(
            &client,
            &cli,
            status,
            issue_type.as_deref(),
            *priority,
            label.as_deref(),
            search.as_deref(),
            sort,
        ),
        Commands::Ready => cmd_ready(&client, &cli),
        Commands::Show { id } => cmd_show(&client, &cli, id),
        Commands::Create {
            title,
            issue_type,
            priority,
            description,
            body_file,
            id,
            assignee,
            labels,
            parent,
            deps,
            due,
            defer,
            estimate,
            acceptance,
            design,
            notes,
            external_ref,
        } => cmd_create(
            &client,
            &cli,
            CreateOptions {
                title: title.clone(),
                issue_type: issue_type.clone(),
                priority: *priority,
                description: description.clone(),
                body_file: body_file.clone(),
                id: id.clone(),
                assignee: assignee.clone(),
                labels: labels.clone(),
                parent: parent.clone(),
                deps: deps.clone(),
                due: due.clone(),
                defer: defer.clone(),
                estimate: *estimate,
                acceptance: acceptance.clone(),
                design: design.clone(),
                notes: notes.clone(),
                external_ref: external_ref.clone(),
            },
        ),
        Commands::Close { id, reason } => cmd_close(&client, &cli, id, reason.clone()),
        Commands::Update {
            id,
            status,
            priority,
            title,
            issue_type,
            description,
            body_file,
            assignee,
            labels,
            parent,
            due,
            defer,
            estimate,
            acceptance,
            design,
            notes,
            external_ref,
            reason,
        } => cmd_update(
            &client,
            &cli,
            id,
            UpdateOptions {
                status: status.clone(),
                priority: *priority,
                title: title.clone(),
                issue_type: issue_type.clone(),
                description: description.clone(),
                body_file: body_file.clone(),
                assignee: assignee.clone(),
                labels: labels.clone(),
                parent: parent.clone(),
                due: due.clone(),
                defer: defer.clone(),
                estimate: *estimate,
                acceptance: acceptance.clone(),
                design: design.clone(),
                notes: notes.clone(),
                external_ref: external_ref.clone(),
                close_reason: reason.clone(),
            },
        ),
        Commands::Dep { action } => match action {
            DepAction::Add {
                issue_id,
                depends_on_id,
            } => cmd_dep_add(&client, &cli, issue_id, depends_on_id),
            DepAction::Remove {
                issue_id,
                depends_on_id,
            } => cmd_dep_remove(&client, &cli, issue_id, depends_on_id),
            DepAction::List { issue_id } => cmd_dep_list(&client, &cli, issue_id),
        },
        Commands::Blocked => cmd_blocked(&client, &cli),
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
    } else if resp.status().as_u16() == 404 {
        // Document doesn't exist yet - start fresh
        (String::new(), String::new())
    } else {
        // Any other error should fail, not silently truncate
        return Err(format!("Failed to fetch current content: {}", resp.status()).into());
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
    type_filter: Option<&str>,
    priority_filter: Option<u8>,
    label_filter: Option<&str>,
    search_filter: Option<&str>,
    sort_by: &str,
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
        .filter(|i| type_filter.is_none_or(|t| i.issue_type == t))
        .filter(|i| priority_filter.is_none_or(|p| i.priority == p))
        .filter(|i| {
            label_filter.is_none_or(|l| {
                i.labels
                    .as_ref()
                    .is_some_and(|labels| labels.iter().any(|lab| lab == l))
            })
        })
        .filter(|i| {
            search_filter.is_none_or(|s| {
                let s_lower = s.to_lowercase();
                i.title.to_lowercase().contains(&s_lower)
                    || i.description.to_lowercase().contains(&s_lower)
            })
        })
        .collect();

    // Sort based on sort_by parameter
    match sort_by {
        "created" => filtered.sort_by(|a, b| b.created_at.cmp(&a.created_at)),
        "updated" => filtered.sort_by(|a, b| b.updated_at.cmp(&a.updated_at)),
        _ => {
            // Default: priority ascending, then created_at descending
            filtered.sort_by(|a, b| {
                a.priority
                    .cmp(&b.priority)
                    .then_with(|| b.created_at.cmp(&a.created_at))
            });
        }
    }

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&filtered)?);
    } else {
        if filtered.is_empty() {
            println!("No issues found");
            return Ok(());
        }
        for issue in filtered {
            let labels = issue
                .labels
                .as_ref()
                .map(|l| {
                    l.iter()
                        .map(|s| format!("[{}]", s))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            let labels_str = if labels.is_empty() {
                String::new()
            } else {
                format!(" {}", labels)
            };
            println!(
                "{} [P{}] [{}] {}{} - {}",
                issue.id, issue.priority, issue.issue_type, issue.status, labels_str, issue.title
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

    // Ready = open and has no OPEN blockers
    // An issue is blocked only if it depends on another OPEN issue
    let mut ready: Vec<_> = open_issues
        .into_iter()
        .filter(|i| {
            // Check if this issue has any open blockers
            let has_open_blocker = i
                .dependencies
                .as_ref()
                .map(|deps| {
                    deps.iter().filter(|d| d.dep_type == "blocks").any(|d| {
                        map.get(&d.depends_on_id)
                            .map(|blocker| blocker.status == "open")
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);
            !has_open_blocker
        })
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
        if let Some(design) = &issue.design {
            println!("\nDesign:\n{}", design);
        }
        if let Some(notes) = &issue.notes {
            println!("\nNotes:\n{}", notes);
        }
        if let Some(reason) = &issue.close_reason {
            println!("\nClose reason: {}", reason);
        }
    }

    Ok(())
}

/// Read description from file or stdin (if path is "-")
fn read_body_file(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    use std::io::Read;
    if path == "-" {
        let mut content = String::new();
        std::io::stdin().read_to_string(&mut content)?;
        Ok(content.trim().to_string())
    } else {
        Ok(std::fs::read_to_string(path)?.trim().to_string())
    }
}

/// Parse dependency string in format "type:id" or "id" (defaults to blocks)
fn parse_dep_string(s: &str, issue_id: &str, created_by: &str, created_at: &str) -> Dependency {
    let (dep_type, depends_on_id) = if let Some((t, id)) = s.split_once(':') {
        (t.to_string(), id.to_string())
    } else {
        ("blocks".to_string(), s.to_string())
    };
    Dependency {
        issue_id: issue_id.to_string(),
        depends_on_id,
        dep_type,
        created_at: created_at.to_string(),
        created_by: created_by.to_string(),
    }
}

fn cmd_create(
    client: &Client,
    cli: &Cli,
    opts: CreateOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let now: DateTime<Utc> = Utc::now();
    let now_str = now.to_rfc3339();
    let created_by = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());

    // Resolve description: body_file takes precedence over description flag
    let description = if let Some(path) = &opts.body_file {
        read_body_file(path)?
    } else {
        opts.description.unwrap_or_default()
    };

    // Generate or use provided ID
    let id = opts.id.unwrap_or_else(generate_id);

    // Parse dependencies
    let dependencies = opts.deps.map(|deps| {
        deps.iter()
            .map(|d| parse_dep_string(d, &id, &created_by, &now_str))
            .collect()
    });

    let issue = Issue {
        id,
        title: opts.title,
        description,
        status: "open".to_string(),
        priority: opts.priority,
        issue_type: opts.issue_type,
        created_at: now_str.clone(),
        created_by,
        updated_at: now_str,
        closed_at: None,
        close_reason: None,
        labels: opts.labels,
        dependencies,
        assignee: opts.assignee,
        parent: opts.parent,
        due: opts.due,
        defer: opts.defer,
        estimate: opts.estimate,
        acceptance: opts.acceptance,
        design: opts.design,
        notes: opts.notes,
        external_ref: opts.external_ref,
        comments: None,
        extra: HashMap::new(),
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
    opts: UpdateOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let mut map = build_issue_map(issues);

    let issue = map
        .get_mut(id)
        .ok_or_else(|| format!("Issue {} not found", id))?;

    let now: DateTime<Utc> = Utc::now();
    issue.updated_at = now.to_rfc3339();

    // Update only fields that are explicitly set
    if let Some(ref s) = opts.status {
        issue.status = s.clone();
        // Set closed_at when status changes to closed
        if s == "closed" {
            issue.closed_at = Some(now.to_rfc3339());
        }
    }
    // Set close_reason if provided (typically with status=closed)
    if let Some(r) = opts.close_reason {
        issue.close_reason = Some(r);
    }
    if let Some(p) = opts.priority {
        issue.priority = p;
    }
    if let Some(t) = opts.title {
        issue.title = t;
    }
    if let Some(t) = opts.issue_type {
        issue.issue_type = t;
    }
    // body_file takes precedence over description
    if let Some(path) = opts.body_file {
        issue.description = read_body_file(&path)?;
    } else if let Some(d) = opts.description {
        issue.description = d;
    }
    if let Some(a) = opts.assignee {
        issue.assignee = Some(a);
    }
    if let Some(l) = opts.labels {
        issue.labels = Some(l);
    }
    if let Some(p) = opts.parent {
        issue.parent = Some(p);
    }
    if let Some(d) = opts.due {
        issue.due = Some(d);
    }
    if let Some(d) = opts.defer {
        issue.defer = Some(d);
    }
    if let Some(e) = opts.estimate {
        issue.estimate = Some(e);
    }
    if let Some(a) = opts.acceptance {
        issue.acceptance = Some(a);
    }
    if let Some(d) = opts.design {
        issue.design = Some(d);
    }
    if let Some(n) = opts.notes {
        issue.notes = Some(n);
    }
    if let Some(e) = opts.external_ref {
        issue.external_ref = Some(e);
    }

    append_issue(client, cli, issue)?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(issue)?);
    } else {
        println!("✓ Updated {}: {}", issue.id, issue.title);
    }

    Ok(())
}

fn cmd_dep_add(
    client: &Client,
    cli: &Cli,
    issue_id: &str,
    depends_on_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let mut map = build_issue_map(issues);

    // Verify both issues exist
    if !map.contains_key(issue_id) {
        return Err(format!("Issue {} not found", issue_id).into());
    }
    if !map.contains_key(depends_on_id) {
        return Err(format!("Issue {} not found", depends_on_id).into());
    }

    let issue = map.get_mut(issue_id).unwrap();
    let now: DateTime<Utc> = Utc::now();

    let dep = Dependency {
        issue_id: issue_id.to_string(),
        depends_on_id: depends_on_id.to_string(),
        dep_type: "blocks".to_string(),
        created_at: now.to_rfc3339(),
        created_by: std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()),
    };

    // Add to existing dependencies or create new list
    if let Some(ref mut deps) = issue.dependencies {
        // Check if dependency already exists
        if deps
            .iter()
            .any(|d| d.depends_on_id == depends_on_id && d.dep_type == "blocks")
        {
            return Err(format!(
                "Dependency already exists: {} is blocked by {}",
                issue_id, depends_on_id
            )
            .into());
        }
        deps.push(dep);
    } else {
        issue.dependencies = Some(vec![dep]);
    }

    issue.updated_at = now.to_rfc3339();
    append_issue(client, cli, issue)?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(issue)?);
    } else {
        println!(
            "✓ Added dependency: {} is now blocked by {}",
            issue_id, depends_on_id
        );
    }

    Ok(())
}

fn cmd_dep_remove(
    client: &Client,
    cli: &Cli,
    issue_id: &str,
    depends_on_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let mut map = build_issue_map(issues);

    let issue = map
        .get_mut(issue_id)
        .ok_or_else(|| format!("Issue {} not found", issue_id))?;

    let now: DateTime<Utc> = Utc::now();

    if let Some(ref mut deps) = issue.dependencies {
        let orig_len = deps.len();
        deps.retain(|d| !(d.depends_on_id == depends_on_id && d.dep_type == "blocks"));
        if deps.len() == orig_len {
            return Err(format!(
                "No dependency found: {} blocked by {}",
                issue_id, depends_on_id
            )
            .into());
        }
        if deps.is_empty() {
            issue.dependencies = None;
        }
    } else {
        return Err(format!("Issue {} has no dependencies", issue_id).into());
    }

    issue.updated_at = now.to_rfc3339();
    append_issue(client, cli, issue)?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(issue)?);
    } else {
        println!(
            "✓ Removed dependency: {} is no longer blocked by {}",
            issue_id, depends_on_id
        );
    }

    Ok(())
}

fn cmd_dep_list(
    client: &Client,
    cli: &Cli,
    issue_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let map = build_issue_map(issues);

    let issue = map
        .get(issue_id)
        .ok_or_else(|| format!("Issue {} not found", issue_id))?;

    // Find what this issue depends on (blockers)
    let blockers: Vec<_> = issue
        .dependencies
        .as_ref()
        .map(|deps| {
            deps.iter()
                .filter(|d| d.dep_type == "blocks")
                .filter_map(|d| map.get(&d.depends_on_id))
                .collect()
        })
        .unwrap_or_default();

    // Find what depends on this issue (blocks)
    let blocks: Vec<_> = map
        .values()
        .filter(|i| {
            i.dependencies
                .as_ref()
                .map(|deps| {
                    deps.iter()
                        .any(|d| d.depends_on_id == issue_id && d.dep_type == "blocks")
                })
                .unwrap_or(false)
        })
        .collect();

    if cli.json {
        #[derive(Serialize)]
        struct DepInfo<'a> {
            blockers: Vec<&'a Issue>,
            blocks: Vec<&'a Issue>,
        }
        let info = DepInfo { blockers, blocks };
        println!("{}", serde_json::to_string_pretty(&info)?);
    } else {
        println!("Dependencies for {}:\n", issue_id);
        if blockers.is_empty() {
            println!("Blocked by: (none)");
        } else {
            println!("Blocked by:");
            for b in &blockers {
                println!("  → {}: {} [{}]", b.id, b.title, b.status);
            }
        }
        println!();
        if blocks.is_empty() {
            println!("Blocks: (none)");
        } else {
            println!("Blocks:");
            for b in &blocks {
                println!("  ← {}: {} [{}]", b.id, b.title, b.status);
            }
        }
    }

    Ok(())
}

fn cmd_blocked(client: &Client, cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let issues = fetch_issues(client, cli)?;
    let map = build_issue_map(issues);

    // Get open issues
    let open_issues: Vec<_> = map.values().filter(|i| i.status == "open").collect();

    // Find issues that have open blockers
    let mut blocked: Vec<(&Issue, Vec<&Issue>)> = Vec::new();

    for issue in &open_issues {
        if let Some(deps) = &issue.dependencies {
            let open_blockers: Vec<_> = deps
                .iter()
                .filter(|d| d.dep_type == "blocks")
                .filter_map(|d| map.get(&d.depends_on_id))
                .filter(|blocker| blocker.status == "open")
                .collect();

            if !open_blockers.is_empty() {
                blocked.push((issue, open_blockers));
            }
        }
    }

    blocked.sort_by(|a, b| a.0.priority.cmp(&b.0.priority));

    if cli.json {
        #[derive(Serialize)]
        struct BlockedInfo<'a> {
            issue: &'a Issue,
            blocked_by: Vec<&'a Issue>,
        }
        let info: Vec<_> = blocked
            .iter()
            .map(|(issue, blockers)| BlockedInfo {
                issue,
                blocked_by: blockers.clone(),
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&info)?);
    } else {
        if blocked.is_empty() {
            println!("No blocked issues");
            return Ok(());
        }
        println!("🚫 Blocked issues ({}):\n", blocked.len());
        for (issue, blockers) in &blocked {
            println!("[P{}] {}: {}", issue.priority, issue.id, issue.title);
            for blocker in blockers {
                println!("  ↳ blocked by {}: {}", blocker.id, blocker.title);
            }
            println!();
        }
    }

    Ok(())
}
