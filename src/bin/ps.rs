//! commonplace-ps: List processes managed by the commonplace orchestrator
//!
//! Usage:
//!   commonplace-ps           # Human-readable table (auto-finds status file)
//!   commonplace-ps --json    # JSON output
//!   commonplace-ps -c /path/to/config.json  # Use specific config

use clap::Parser;
use commonplace_doc::cli::PsArgs;
use commonplace_doc::orchestrator::{get_process_cwd, OrchestratorConfig, OrchestratorStatus};
use commonplace_doc::workspace::{format_timestamp_secs, is_process_running};
use std::path::PathBuf;

/// Find any orchestrator status file in /tmp.
/// Returns the first valid status file found, or None if none exist.
fn find_status_file() -> Option<PathBuf> {
    let temp_dir = std::env::temp_dir();
    if let Ok(entries) = std::fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("commonplace-orchestrator-") && name.ends_with(".status.json") {
                    return Some(path);
                }
            }
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = PsArgs::parse();

    // Try to find status file:
    // 1. First, try the config-scoped path (for explicit --config or when in right directory)
    // 2. Fall back to scanning /tmp for any status file (for convenience)
    let status_file_path = OrchestratorConfig::status_file_path(&args.config);

    let (status, actual_path) = match OrchestratorStatus::read(&status_file_path) {
        Ok(s) => (s, status_file_path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Try to find any status file in /tmp
            if let Some(found_path) = find_status_file() {
                match OrchestratorStatus::read(&found_path) {
                    Ok(s) => (s, found_path),
                    Err(e) => {
                        eprintln!("Found status file but failed to read: {}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("Orchestrator is not running (no status file found)");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Failed to read orchestrator status: {}", e);
            std::process::exit(1);
        }
    };

    // Check if orchestrator is still running
    let is_running = is_process_running(status.orchestrator_pid);
    if !is_running {
        eprintln!(
            "Orchestrator status file exists but process {} is not running",
            status.orchestrator_pid
        );
        eprintln!(
            "The orchestrator may have crashed. Stale status file at: {}",
            actual_path.display()
        );
        std::process::exit(1);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!(
            "Orchestrator PID: {} (started at {})",
            status.orchestrator_pid,
            format_timestamp_secs(status.started_at)
        );
        println!();

        if status.processes.is_empty() {
            println!("No processes running");
        } else {
            // Print header
            println!(
                "{:<25} {:>8} {:<10} {:<20} CWD",
                "NAME", "PID", "STATE", "SOURCE"
            );
            println!("{}", "-".repeat(100));

            for proc in &status.processes {
                // Check if the PID is still running
                let (pid_str, state_display) = if let Some(pid) = proc.pid {
                    if is_process_running(pid) {
                        (pid.to_string(), proc.state.clone())
                    } else {
                        // Process has died but status file wasn't updated
                        (format!("{}!", pid), "Dead".to_string())
                    }
                } else {
                    ("-".to_string(), proc.state.clone())
                };

                // Source: where this process is defined
                // - Discovered processes have source_path (e.g., "/beads")
                // - Base processes (from commonplace.json) have None
                let source = proc
                    .source_path
                    .clone()
                    .unwrap_or_else(|| "(commonplace.json)".to_string());

                // Look up CWD dynamically to find sandbox directories
                // This finds the deepest child's CWD for sandbox processes
                let cwd = proc
                    .pid
                    .and_then(get_process_cwd)
                    .or_else(|| proc.cwd.clone())
                    .unwrap_or_else(|| "-".to_string());
                println!(
                    "{:<25} {:>8} {:<10} {:<20} {}",
                    proc.name, pid_str, state_display, source, cwd
                );
            }
        }
    }

    Ok(())
}
