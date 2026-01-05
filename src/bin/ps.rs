//! commonplace-ps: List processes managed by the commonplace orchestrator
//!
//! Usage:
//!   commonplace-ps           # Human-readable table
//!   commonplace-ps --json    # JSON output

use clap::Parser;
use commonplace_doc::cli::PsArgs;
use commonplace_doc::orchestrator::{get_process_cwd, OrchestratorStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = PsArgs::parse();

    let status = match OrchestratorStatus::read() {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("Orchestrator is not running (no status file found)");
            std::process::exit(1);
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
            commonplace_doc::orchestrator::STATUS_FILE_PATH
        );
        std::process::exit(1);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!(
            "Orchestrator PID: {} (started at {})",
            status.orchestrator_pid,
            format_timestamp(status.started_at)
        );
        println!();

        if status.processes.is_empty() {
            println!("No processes running");
        } else {
            // Print header
            println!("{:<20} {:>8} {:<10} {:<40}", "NAME", "PID", "STATE", "CWD");
            println!("{}", "-".repeat(80));

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

                // Look up CWD dynamically to find sandbox directories
                // This finds the deepest child's CWD for sandbox processes
                let cwd = proc
                    .pid
                    .and_then(get_process_cwd)
                    .or_else(|| proc.cwd.clone())
                    .unwrap_or_else(|| "-".to_string());
                println!(
                    "{:<20} {:>8} {:<10} {}",
                    proc.name, pid_str, state_display, cwd
                );
            }
        }
    }

    Ok(())
}

/// Check if a process is running by sending signal 0
fn is_process_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // kill(pid, 0) returns 0 if process exists, -1 otherwise
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        // On non-Unix, assume running
        true
    }
}

/// Format Unix timestamp as human-readable
fn format_timestamp(ts: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let time = UNIX_EPOCH + Duration::from_secs(ts);
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
