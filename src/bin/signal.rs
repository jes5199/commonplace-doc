//! commonplace-signal: Send a signal to an orchestrator-managed process
//!
//! Usage:
//!   commonplace-signal -n bartleby              # Send SIGTERM to bartleby
//!   commonplace-signal -n sync -s KILL          # Send SIGKILL to sync
//!   commonplace-signal -n sync -p /text-to-telegram  # Signal sync for specific path

use clap::Parser;
use commonplace_doc::cli::SignalArgs;
use commonplace_doc::orchestrator::OrchestratorStatus;
use commonplace_doc::workspace::is_process_running;
use serde::Serialize;

#[derive(Serialize)]
struct SignalResult {
    name: String,
    pid: u32,
    signal: String,
    document_path: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = SignalArgs::parse();

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
    if !is_process_running(status.orchestrator_pid) {
        eprintln!(
            "Orchestrator status file exists but process {} is not running",
            status.orchestrator_pid
        );
        std::process::exit(1);
    }

    // Find matching process
    let matching: Vec<_> = status
        .processes
        .iter()
        .filter(|p| {
            if p.name != args.name {
                return false;
            }
            if let Some(ref filter_path) = args.path {
                if let Some(ref doc_path) = p.document_path {
                    return doc_path.contains(filter_path);
                }
                return false;
            }
            true
        })
        .collect();

    if matching.is_empty() {
        eprintln!("No process found matching name '{}'", args.name);
        if args.path.is_some() {
            eprintln!("  (with path filter: {:?})", args.path);
        }
        std::process::exit(1);
    }

    if matching.len() > 1 {
        eprintln!(
            "Multiple processes match '{}'. Use --path to disambiguate:",
            args.name
        );
        for p in &matching {
            eprintln!(
                "  {} (pid: {:?}, path: {:?})",
                p.name, p.pid, p.document_path
            );
        }
        std::process::exit(1);
    }

    let process = matching[0];
    let pid = match process.pid {
        Some(p) => p,
        None => {
            eprintln!("Process '{}' has no PID (not running)", args.name);
            std::process::exit(1);
        }
    };

    // Parse signal
    let signal_num = parse_signal(&args.signal)?;

    // Send signal
    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(pid as i32, signal_num) };
        if result != 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("Failed to send signal to pid {}: {}", pid, err);
            std::process::exit(1);
        }
    }

    #[cfg(not(unix))]
    {
        eprintln!("Signal sending is only supported on Unix systems");
        std::process::exit(1);
    }

    if args.json {
        let result = SignalResult {
            name: process.name.clone(),
            pid,
            signal: args.signal.clone(),
            document_path: process.document_path.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "Sent SIG{} to {} (pid {})",
            args.signal.to_uppercase(),
            process.name,
            pid
        );
        if let Some(ref path) = process.document_path {
            println!("  document_path: {}", path);
        }
    }

    Ok(())
}

/// Parse signal name to signal number
fn parse_signal(name: &str) -> Result<i32, Box<dyn std::error::Error>> {
    let name = name.to_uppercase();
    let name = name.strip_prefix("SIG").unwrap_or(&name);

    #[cfg(unix)]
    {
        Ok(match name {
            "TERM" => libc::SIGTERM,
            "KILL" => libc::SIGKILL,
            "HUP" => libc::SIGHUP,
            "INT" => libc::SIGINT,
            "QUIT" => libc::SIGQUIT,
            "USR1" => libc::SIGUSR1,
            "USR2" => libc::SIGUSR2,
            "CONT" => libc::SIGCONT,
            "STOP" => libc::SIGSTOP,
            _ => {
                // Try parsing as number
                name.parse::<i32>()
                    .map_err(|_| format!("Unknown signal: {}", name))?
            }
        })
    }
    #[cfg(not(unix))]
    {
        Err("Signals are only supported on Unix".into())
    }
}
