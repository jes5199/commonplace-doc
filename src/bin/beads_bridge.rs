//! commonplace-beads-bridge: Sync bd issues to commonplace
//!
//! Wraps commonplace-sync to sync .beads/issues.jsonl files between
//! a git repository and commonplace.
//!
//! Usage:
//!   commonplace-beads-bridge                    # Auto-discover repo, use defaults
//!   commonplace-beads-bridge --push-only        # Only push local changes
//!   commonplace-beads-bridge --path cbd/myrepo  # Custom commonplace path

use clap::Parser;
use commonplace_doc::DEFAULT_SERVER_URL;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Default server URL
const DEFAULT_SERVER: &str = DEFAULT_SERVER_URL;

#[derive(Parser)]
#[command(
    name = "commonplace-beads-bridge",
    about = "Sync bd issues to commonplace"
)]
struct Cli {
    /// Server URL
    #[arg(long, default_value = DEFAULT_SERVER, env = "COMMONPLACE_SERVER")]
    server: String,

    /// Path in commonplace workspace (default: cbd/data/{repo-name}.issues.jsonl)
    #[arg(long)]
    path: Option<String>,

    /// Only push local changes to commonplace (don't pull)
    #[arg(long)]
    push_only: bool,

    /// Only pull from commonplace (don't push local changes)
    #[arg(long)]
    pull_only: bool,

    /// Force push (overwrite server state with local)
    #[arg(long)]
    force_push: bool,

    /// Path to local .beads/issues.jsonl (auto-discovered if not set)
    #[arg(long)]
    file: Option<PathBuf>,

    /// Don't create/update .cbd.json config file
    #[arg(long)]
    no_config: bool,

    /// MQTT broker address
    #[arg(long, env = "MQTT_BROKER")]
    mqtt_broker: Option<String>,
}

/// Config file structure for .cbd.json
#[derive(Debug, Serialize, Deserialize)]
struct CbdConfig {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    server: Option<String>,
}

/// Find .beads/issues.jsonl by walking up directories
fn find_beads_file() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;

    loop {
        let beads_file = dir.join(".beads").join("issues.jsonl");
        if beads_file.exists() {
            return Some(beads_file);
        }

        if !dir.pop() {
            break;
        }
    }

    None
}

/// Get the repo name from the git directory
fn get_repo_name(beads_file: &Path) -> Option<String> {
    // .beads/issues.jsonl -> repo root is parent of .beads
    let repo_root = beads_file.parent()?.parent()?;

    // Try to get name from .git/config or just use dir name
    repo_root.file_name()?.to_str().map(|s| s.to_string())
}

/// Write .cbd.json config file next to .beads directory
fn write_config(beads_file: &Path, commonplace_path: &str, server: &str) -> std::io::Result<()> {
    let repo_root = beads_file.parent().and_then(|p| p.parent());
    if let Some(root) = repo_root {
        let config_path = root.join(".cbd.json");
        let config = CbdConfig {
            path: commonplace_path.to_string(),
            server: if server != DEFAULT_SERVER {
                Some(server.to_string())
            } else {
                None
            },
        };
        let content = serde_json::to_string_pretty(&config)?;
        std::fs::write(config_path, content)?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Find beads file
    let beads_file = cli
        .file
        .clone()
        .or_else(find_beads_file)
        .ok_or("Could not find .beads/issues.jsonl - run from within a git repo with beads")?;

    if !beads_file.exists() {
        return Err(format!("File not found: {}", beads_file.display()).into());
    }

    // Determine commonplace path
    let commonplace_path = cli.path.clone().unwrap_or_else(|| {
        let repo_name = get_repo_name(&beads_file).unwrap_or_else(|| "unknown".to_string());
        format!("cbd/data/{}.issues.jsonl", repo_name)
    });

    // Write config file unless disabled
    if !cli.no_config {
        if let Err(e) = write_config(&beads_file, &commonplace_path, &cli.server) {
            eprintln!("Warning: Could not write .cbd.json: {}", e);
        }
    }

    // Build commonplace-sync command
    let sync_binary = std::env::current_exe()?
        .parent()
        .map(|p| p.join("commonplace-sync"))
        .unwrap_or_else(|| PathBuf::from("commonplace-sync"));

    let mut cmd = Command::new(&sync_binary);
    cmd.arg("--server").arg(&cli.server);
    cmd.arg("--path").arg(&commonplace_path);
    cmd.arg("--file").arg(&beads_file);

    if cli.push_only {
        cmd.arg("--push-only");
    }
    if cli.pull_only {
        cmd.arg("--pull-only");
    }
    if cli.force_push {
        cmd.arg("--force-push");
    }
    if let Some(broker) = &cli.mqtt_broker {
        cmd.arg("--mqtt-broker").arg(broker);
    }

    eprintln!("Syncing {} <-> {}", beads_file.display(), commonplace_path);

    // Execute sync
    let status = cmd.status()?;

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}
