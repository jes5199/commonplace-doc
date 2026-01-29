//! Status file for orchestrator process information.
//!
//! Writes a JSON file to /tmp that can be read by commonplace-ps.
//! The status file path is scoped to the config file to allow multiple
//! orchestrators to run with different configs.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;
use std::time::SystemTime;

/// Legacy path for backwards compatibility (used by commonplace-ps when no config specified)
pub const LEGACY_STATUS_FILE_PATH: &str = "/tmp/commonplace-orchestrator-status.json";

/// Information about a single managed process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessStatus {
    /// Process name
    pub name: String,
    /// Process ID (if running)
    pub pid: Option<u32>,
    /// Working directory
    pub cwd: Option<String>,
    /// Current state
    pub state: String,
    /// Document path (for discovered processes)
    pub document_path: Option<String>,
    /// Source path (for discovered processes, which __processes.json defined this)
    pub source_path: Option<String>,
    /// Log file path (if logging is enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_file: Option<String>,
}

/// Full orchestrator status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorStatus {
    /// Orchestrator PID
    pub orchestrator_pid: u32,
    /// When the orchestrator started (Unix timestamp)
    pub started_at: u64,
    /// List of managed processes
    pub processes: Vec<ProcessStatus>,
}

impl OrchestratorStatus {
    /// Create a new status with current orchestrator info
    pub fn new() -> Self {
        let started_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            orchestrator_pid: std::process::id(),
            started_at,
            processes: Vec::new(),
        }
    }

    /// Write status to the status file
    pub fn write(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(path, json)
    }

    /// Read status from the status file
    pub fn read(path: &Path) -> io::Result<Self> {
        let content = fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Remove the status file
    pub fn remove(path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Merge processes from a specific source and write.
    ///
    /// This reads the existing status file, replaces processes that match
    /// the given filter, adds the new processes, and writes the result.
    ///
    /// - `is_base_process`: if true, merge base processes (source_path is None)
    /// - `is_base_process`: if false, merge discovered processes (source_path is Some)
    pub fn merge_and_write(&self, path: &Path, is_base_process: bool) -> io::Result<()> {
        // Read existing status, or start fresh if not found
        let mut merged = match Self::read(path) {
            Ok(existing) => existing,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Self::new(),
            Err(e) => return Err(e),
        };

        // Update orchestrator info
        merged.orchestrator_pid = self.orchestrator_pid;
        merged.started_at = self.started_at;

        // Remove processes from this source (base or discovered)
        merged.processes.retain(|p| {
            if is_base_process {
                // Keep discovered processes (those with source_path)
                p.source_path.is_some()
            } else {
                // Keep base processes (those without source_path)
                p.source_path.is_none()
            }
        });

        // Add our processes
        merged.processes.extend(self.processes.clone());

        // Sort by name for consistent output
        merged.processes.sort_by(|a, b| a.name.cmp(&b.name));

        merged.write(path)
    }
}

impl Default for OrchestratorStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a ProcessStatus entry from process information.
///
/// This helper encapsulates the common logic of getting CWD from config or /proc,
/// reducing duplication between ProcessManager and DiscoveredProcessManager.
///
/// # Arguments
/// * `name` - Process name
/// * `pid` - Process ID (if running)
/// * `config_cwd` - CWD from process config (preferred if present)
/// * `state` - Process state as string (caller maps their state enum)
/// * `document_path` - Document path for discovered processes (None for base processes)
/// * `source_path` - Source path for discovered processes (None for base processes)
/// * `log_file` - Path to the process log file (if logging is enabled)
pub fn build_process_status(
    name: String,
    pid: Option<u32>,
    config_cwd: Option<&std::path::Path>,
    state: &str,
    document_path: Option<String>,
    source_path: Option<String>,
    log_file: Option<String>,
) -> ProcessStatus {
    // Try to get CWD from config first, otherwise read from /proc/<pid>/cwd
    // For sandbox processes, this finds the deepest child's CWD
    let cwd = config_cwd
        .map(|p| p.to_string_lossy().to_string())
        .or_else(|| pid.and_then(get_process_cwd));

    ProcessStatus {
        name,
        pid,
        cwd,
        state: state.to_string(),
        document_path,
        source_path,
        log_file,
    }
}

/// Get the working directory for a process, following the process tree to find
/// the deepest child's CWD. This is needed for sandbox processes where the
/// actual sandboxed process runs in a temp directory.
///
/// On Linux, this walks the process tree looking for children and returns
/// the CWD of the deepest descendant (the actual sandboxed process).
#[cfg(target_os = "linux")]
pub fn get_process_cwd(pid: u32) -> Option<String> {
    // First, try to find the deepest child process
    if let Some(deepest_pid) = find_deepest_child(pid) {
        if let Ok(link) = std::fs::read_link(format!("/proc/{}/cwd", deepest_pid)) {
            return Some(link.to_string_lossy().to_string());
        }
    }

    // Fall back to the process's own CWD
    if let Ok(link) = std::fs::read_link(format!("/proc/{}/cwd", pid)) {
        return Some(link.to_string_lossy().to_string());
    }

    None
}

/// Find the deepest child process in the process tree.
/// Returns the PID of the deepest descendant, or None if no children.
#[cfg(target_os = "linux")]
fn find_deepest_child(pid: u32) -> Option<u32> {
    let children_path = format!("/proc/{}/task/{}/children", pid, pid);

    if let Ok(children) = std::fs::read_to_string(&children_path) {
        // Get the first child PID
        if let Some(child_str) = children.split_whitespace().next() {
            if let Ok(child_pid) = child_str.parse::<u32>() {
                // Recursively look for deeper children
                if let Some(deeper) = find_deepest_child(child_pid) {
                    return Some(deeper);
                }
                // No deeper children, this is the deepest
                return Some(child_pid);
            }
        }
    }

    None
}

/// Get the working directory for a process (non-Linux fallback).
#[cfg(not(target_os = "linux"))]
pub fn get_process_cwd(_pid: u32) -> Option<String> {
    None
}
