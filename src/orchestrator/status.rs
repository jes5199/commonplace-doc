//! Status file for orchestrator process information.
//!
//! Writes a JSON file to /tmp that can be read by commonplace-ps.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

/// Path to the orchestrator status file
pub const STATUS_FILE_PATH: &str = "/tmp/commonplace-orchestrator-status.json";

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
    /// Source path (for discovered processes, which processes.json defined this)
    pub source_path: Option<String>,
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
    pub fn write(&self) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(STATUS_FILE_PATH, json)
    }

    /// Read status from the status file
    pub fn read() -> io::Result<Self> {
        let content = fs::read_to_string(STATUS_FILE_PATH)?;
        serde_json::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Get the status file path
    pub fn path() -> PathBuf {
        PathBuf::from(STATUS_FILE_PATH)
    }

    /// Remove the status file
    pub fn remove() -> io::Result<()> {
        match fs::remove_file(STATUS_FILE_PATH) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl Default for OrchestratorStatus {
    fn default() -> Self {
        Self::new()
    }
}
