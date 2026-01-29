//! Shared process spawning utilities for orchestrator managers.
//!
//! This module provides common functionality for spawning managed processes,
//! including stdout/stderr capture, process group setup, and cleanup handling.

use chrono::Local;
#[cfg(unix)]
#[allow(unused_imports)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tracing::warn;

/// Default directory for process log files.
const LOG_DIR: &str = "/tmp/commonplace-logs";

/// Result of spawning a managed process.
pub struct SpawnResult {
    /// The spawned child process.
    pub child: Child,
    /// Path to the log file, if logging was enabled.
    pub log_file: Option<PathBuf>,
}

/// Spawns a managed process with standard orchestrator setup.
///
/// This function:
/// - Configures stdout/stderr piping
/// - Enables kill_on_drop for cleanup safety
/// - Sets up Unix process groups (setpgid) for clean termination
/// - Sets PR_SET_PDEATHSIG on Linux so children die if parent dies
/// - Spawns background tasks to log stdout/stderr with a `[name]` prefix
/// - Writes process output to persistent log files
///
/// Returns the spawned Child handle on success.
pub fn spawn_managed_process(cmd: Command, name: &str) -> Result<Child, String> {
    let result = spawn_managed_process_with_logging(cmd, name)?;
    Ok(result.child)
}

/// Spawns a managed process with logging and returns the log file path.
///
/// Same as `spawn_managed_process` but also returns the path to the log file.
pub fn spawn_managed_process_with_logging(
    mut cmd: Command,
    name: &str,
) -> Result<SpawnResult, String> {
    // Capture stdout/stderr
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    // Enable kill_on_drop for extra safety when handle is dropped
    cmd.kill_on_drop(true);

    // Set up process group on Unix (works on macOS and Linux)
    // Also set death signal on Linux (prctl is Linux-specific)
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            // Put child in its own process group so we can kill all descendants
            libc::setpgid(0, 0);
            // Request SIGTERM if parent dies (Linux-only, covers SIGKILL of parent)
            #[cfg(target_os = "linux")]
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn {}: {}", name, e))?;

    // Create log file path
    let log_file = get_log_file_path(name);

    // Spawn tasks to read stdout/stderr with prefix and write to log file
    spawn_output_loggers(&mut child, name, log_file.clone());

    Ok(SpawnResult {
        child,
        log_file: Some(log_file),
    })
}

/// Get the log file path for a process.
fn get_log_file_path(name: &str) -> PathBuf {
    // Sanitize name to be safe for filenames
    let safe_name: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    PathBuf::from(LOG_DIR).join(format!("{}.log", safe_name))
}

/// Ensure the log directory exists.
async fn ensure_log_dir() -> Result<(), std::io::Error> {
    tokio::fs::create_dir_all(LOG_DIR).await
}

/// Open a log file for appending, creating if necessary.
async fn open_log_file(path: &PathBuf) -> Option<File> {
    if let Err(e) = ensure_log_dir().await {
        warn!("Failed to create log directory {}: {}", LOG_DIR, e);
        return None;
    }

    match OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    {
        Ok(f) => Some(f),
        Err(e) => {
            warn!("Failed to open log file {}: {}", path.display(), e);
            None
        }
    }
}

/// Spawns background tasks to log stdout/stderr with a `[name]` prefix.
/// Also writes to a persistent log file with timestamps.
fn spawn_output_loggers(child: &mut Child, name: &str, log_path: PathBuf) {
    if let Some(stdout) = child.stdout.take() {
        let name = name.to_string();
        let log_path = log_path.clone();
        tokio::spawn(async move {
            let mut log_file = open_log_file(&log_path).await;
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // Print to console
                println!("[{}] {}", name, line);
                // Write to log file with timestamp
                if let Some(ref mut f) = log_file {
                    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
                    let log_line = format!("{} [{}] {}\n", timestamp, name, line);
                    let _ = f.write_all(log_line.as_bytes()).await;
                }
            }
        });
    }

    if let Some(stderr) = child.stderr.take() {
        let name = name.to_string();
        tokio::spawn(async move {
            let mut log_file = open_log_file(&log_path).await;
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // Print to console
                eprintln!("[{}] {}", name, line);
                // Write to log file with timestamp and ERR marker
                if let Some(ref mut f) = log_file {
                    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
                    let log_line = format!("{} [{}] ERR: {}\n", timestamp, name, line);
                    let _ = f.write_all(log_line.as_bytes()).await;
                }
            }
        });
    }
}
