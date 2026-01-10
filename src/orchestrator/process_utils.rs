//! Process utilities for the orchestrator.
//!
//! This module provides shared helpers for process management operations
//! like graceful termination with timeout.

use std::time::Duration;
use tokio::process::Child;

/// Gracefully stop a process with SIGTERM, then SIGKILL fallback.
///
/// This helper encapsulates the common pattern of:
/// 1. Sending SIGTERM to the process group (Unix only)
/// 2. Waiting up to `timeout` for graceful shutdown
/// 3. If timeout expires, sending SIGKILL and forcing termination
///
/// Returns `true` if the process stopped gracefully, `false` if force-killed.
///
/// # Arguments
/// * `child` - The child process handle
/// * `name` - Process name for logging
/// * `log_prefix` - Log prefix like "[orchestrator]" or "[discovery]"
/// * `timeout` - How long to wait for graceful shutdown
pub async fn stop_process_gracefully(
    child: &mut Child,
    name: &str,
    log_prefix: &str,
    timeout: Duration,
) -> bool {
    // Send SIGTERM to process group (Unix only)
    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            tracing::debug!("{} Sending SIGTERM to process group {}", log_prefix, pid);
            unsafe {
                libc::killpg(pid as i32, libc::SIGTERM);
            }
        }
    }

    // Wait with timeout for graceful shutdown
    let wait_result = tokio::time::timeout(timeout, child.wait()).await;

    match wait_result {
        Ok(Ok(_)) => {
            tracing::info!("{} '{}' stopped gracefully", log_prefix, name);
            true
        }
        _ => {
            tracing::warn!(
                "{} '{}' didn't stop gracefully, force killing",
                log_prefix,
                name
            );
            // Force kill the entire process group
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                tracing::debug!("{} Sending SIGKILL to process group {}", log_prefix, pid);
                unsafe {
                    libc::killpg(pid as i32, libc::SIGKILL);
                }
            }
            let _ = child.kill().await;
            false
        }
    }
}
