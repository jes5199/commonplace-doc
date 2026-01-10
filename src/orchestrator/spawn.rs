//! Shared process spawning utilities for orchestrator managers.
//!
//! This module provides common functionality for spawning managed processes,
//! including stdout/stderr capture, process group setup, and cleanup handling.

#[cfg(unix)]
#[allow(unused_imports)]
use std::os::unix::process::CommandExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// Spawns a managed process with standard orchestrator setup.
///
/// This function:
/// - Configures stdout/stderr piping
/// - Enables kill_on_drop for cleanup safety
/// - Sets up Unix process groups (setpgid) for clean termination
/// - Sets PR_SET_PDEATHSIG on Linux so children die if parent dies
/// - Spawns background tasks to log stdout/stderr with a `[name]` prefix
///
/// Returns the spawned Child handle on success.
pub fn spawn_managed_process(mut cmd: Command, name: &str) -> Result<Child, String> {
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

    // Spawn tasks to read stdout/stderr with prefix
    spawn_output_loggers(&mut child, name);

    Ok(child)
}

/// Spawns background tasks to log stdout/stderr with a `[name]` prefix.
fn spawn_output_loggers(child: &mut Child, name: &str) {
    if let Some(stdout) = child.stdout.take() {
        let name = name.to_string();
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                println!("[{}] {}", name, line);
            }
        });
    }

    if let Some(stderr) = child.stderr.take() {
        let name = name.to_string();
        tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("[{}] {}", name, line);
            }
        });
    }
}
