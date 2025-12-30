//! Discovery module for `.processes.json` files.
//!
//! This module provides the configuration format and process manager for
//! processes discovered from `.processes.json` files in the filesystem.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// Configuration parsed from `.processes.json` files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessesConfig {
    pub processes: HashMap<String, DiscoveredProcess>,
}

/// A process discovered from a `.processes.json` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredProcess {
    /// Command to run (either a string or array of strings)
    pub command: CommandSpec,
    /// Relative path within same directory that this process owns
    pub owns: String,
    /// Required absolute path on host for working directory
    pub cwd: PathBuf,
}

/// Command specification - supports both simple string and array formats.
///
/// Note: The simple string form uses whitespace splitting, not shell-style quoting.
/// For commands with spaces in arguments, use the array form instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CommandSpec {
    /// Simple command string (split on whitespace - no quoting support)
    Simple(String),
    /// Array of command and arguments (recommended for complex commands)
    Array(Vec<String>),
}

impl CommandSpec {
    /// Get the program to execute.
    pub fn program(&self) -> &str {
        match self {
            CommandSpec::Simple(s) => s.split_whitespace().next().unwrap_or(""),
            CommandSpec::Array(arr) => arr.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }

    /// Get the arguments for the command.
    pub fn args(&self) -> Vec<&str> {
        match self {
            CommandSpec::Simple(s) => s.split_whitespace().skip(1).collect(),
            CommandSpec::Array(arr) => arr.iter().skip(1).map(|s| s.as_str()).collect(),
        }
    }
}

/// State of a managed discovered process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiscoveredProcessState {
    Stopped,
    Starting,
    Running,
    Failed,
}

/// A running discovered process with its metadata.
#[derive(Debug)]
pub struct ManagedDiscoveredProcess {
    /// The process name
    pub name: String,
    /// The document path this process owns
    pub document_path: String,
    /// Configuration for this process
    pub config: DiscoveredProcess,
    /// Child process handle
    pub handle: Option<Child>,
    /// Current state
    pub state: DiscoveredProcessState,
    /// Number of consecutive failures
    pub consecutive_failures: u32,
    /// When the process was last started
    pub last_start: Option<Instant>,
}

/// Manager for processes discovered from `.processes.json` files.
pub struct DiscoveredProcessManager {
    /// MQTT broker address
    mqtt_broker: String,
    /// Currently managed processes
    processes: HashMap<String, ManagedDiscoveredProcess>,
    /// Initial backoff in milliseconds
    initial_backoff_ms: u64,
    /// Maximum backoff in milliseconds
    max_backoff_ms: u64,
    /// Time in seconds after which to reset failure count
    reset_after_secs: u64,
}

impl DiscoveredProcessManager {
    /// Create a new discovered process manager.
    pub fn new(mqtt_broker: String) -> Self {
        Self {
            mqtt_broker,
            processes: HashMap::new(),
            initial_backoff_ms: 500,
            max_backoff_ms: 10_000,
            reset_after_secs: 30,
        }
    }

    /// Get the MQTT broker address.
    pub fn mqtt_broker(&self) -> &str {
        &self.mqtt_broker
    }

    /// Get all managed processes.
    pub fn processes(&self) -> &HashMap<String, ManagedDiscoveredProcess> {
        &self.processes
    }

    /// Add a process to be managed.
    ///
    /// # Arguments
    /// * `name` - Unique name for this process
    /// * `document_path` - Full document path (e.g., "examples/counter.json")
    /// * `config` - Process configuration from `.processes.json`
    pub fn add_process(&mut self, name: String, document_path: String, config: DiscoveredProcess) {
        let process = ManagedDiscoveredProcess {
            name: name.clone(),
            document_path,
            config,
            handle: None,
            state: DiscoveredProcessState::Stopped,
            consecutive_failures: 0,
            last_start: None,
        };
        self.processes.insert(name, process);
    }

    /// Remove a process from management.
    ///
    /// Returns the removed process if it existed.
    pub async fn remove_process(&mut self, name: &str) -> Option<ManagedDiscoveredProcess> {
        if let Some(mut process) = self.processes.remove(name) {
            // Stop the process if it's running
            if let Some(ref mut child) = process.handle {
                #[cfg(unix)]
                {
                    if let Some(pid) = child.id() {
                        unsafe {
                            libc::kill(pid as i32, libc::SIGTERM);
                        }
                    }
                }

                let timeout = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;

                if timeout.is_err() {
                    let _ = child.kill().await;
                }
            }
            Some(process)
        } else {
            None
        }
    }

    /// Spawn a specific process by name.
    pub async fn spawn_process(&mut self, name: &str) -> Result<(), String> {
        let mqtt_broker = self.mqtt_broker.clone();

        let process = self
            .processes
            .get_mut(name)
            .ok_or_else(|| format!("Unknown process: {}", name))?;

        let config = &process.config;
        let document_path = &process.document_path;

        // Build command
        let program = config.command.program();
        if program.is_empty() {
            return Err(format!("Empty command for process: {}", name));
        }

        let mut cmd = Command::new(program);
        cmd.args(config.command.args());
        cmd.current_dir(&config.cwd);

        // Set environment variables
        cmd.env("COMMONPLACE_PATH", document_path);
        cmd.env("COMMONPLACE_MQTT", &mqtt_broker);

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        tracing::info!(
            "[discovery] Starting process '{}' (path: {}, cwd: {:?})",
            name,
            document_path,
            config.cwd
        );

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn {}: {}", name, e))?;

        // Spawn tasks to read stdout/stderr with prefix
        let name_clone = name.to_string();
        if let Some(stdout) = child.stdout.take() {
            let name = name_clone.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("[{}] {}", name, line);
                }
            });
        }

        if let Some(stderr) = child.stderr.take() {
            let name = name_clone;
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[{}] {}", name, line);
                }
            });
        }

        process.handle = Some(child);
        process.state = DiscoveredProcessState::Running;
        process.last_start = Some(Instant::now());

        Ok(())
    }

    /// Start all managed processes.
    pub async fn start_all(&mut self) -> Result<(), String> {
        let names: Vec<String> = self.processes.keys().cloned().collect();
        for name in names {
            self.spawn_process(&name).await?;
            // Small delay between starts
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Check all processes and restart any that have exited.
    pub async fn check_and_restart(&mut self) {
        let names: Vec<String> = self.processes.keys().cloned().collect();

        for name in names {
            let should_restart = {
                let process = match self.processes.get_mut(&name) {
                    Some(p) => p,
                    None => continue,
                };

                if let Some(ref mut child) = process.handle {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            tracing::warn!(
                                "[discovery] Process '{}' exited with status: {}",
                                name,
                                status
                            );
                            process.handle = None;
                            true // Always restart discovered processes
                        }
                        Ok(None) => {
                            // Still running - reset failure count if running long enough
                            if let Some(start) = process.last_start {
                                if start.elapsed() > Duration::from_secs(self.reset_after_secs)
                                    && process.consecutive_failures > 0
                                {
                                    process.consecutive_failures = 0;
                                    tracing::debug!(
                                        "[discovery] Reset failure count for '{}' after stable run",
                                        name
                                    );
                                }
                            }
                            false
                        }
                        Err(e) => {
                            tracing::error!("[discovery] Error checking process '{}': {}", name, e);
                            false
                        }
                    }
                } else {
                    // No handle - retry if in Failed state (handles spawn failures)
                    process.state == DiscoveredProcessState::Failed
                }
            };

            if should_restart {
                let backoff = {
                    let process = self.processes.get_mut(&name).unwrap();
                    process.consecutive_failures += 1;
                    process.state = DiscoveredProcessState::Failed;

                    std::cmp::min(
                        self.initial_backoff_ms
                            * 2u64.pow(process.consecutive_failures.saturating_sub(1)),
                        self.max_backoff_ms,
                    )
                };

                let failures = self.processes.get(&name).unwrap().consecutive_failures;
                tracing::info!(
                    "[discovery] Restarting '{}' in {}ms (attempt {})",
                    name,
                    backoff,
                    failures
                );

                tokio::time::sleep(Duration::from_millis(backoff)).await;

                if let Err(e) = self.spawn_process(&name).await {
                    tracing::error!("[discovery] Failed to restart '{}': {}", name, e);
                }
            }
        }
    }

    /// Run the manager loop, starting all processes and monitoring for restarts.
    pub async fn run(&mut self) -> Result<(), String> {
        self.start_all().await?;

        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.check_and_restart().await;
        }
    }

    /// Gracefully shutdown all managed processes.
    pub async fn shutdown(&mut self) {
        tracing::info!("[discovery] Shutting down all discovered processes...");

        let names: Vec<String> = self.processes.keys().cloned().collect();

        for name in names {
            if let Some(process) = self.processes.get_mut(&name) {
                if let Some(ref mut child) = process.handle {
                    tracing::info!("[discovery] Stopping '{}'", name);

                    #[cfg(unix)]
                    {
                        if let Some(pid) = child.id() {
                            unsafe {
                                libc::kill(pid as i32, libc::SIGTERM);
                            }
                        }
                    }

                    let timeout = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;

                    match timeout {
                        Ok(Ok(_)) => {
                            tracing::info!("[discovery] '{}' stopped gracefully", name);
                        }
                        _ => {
                            tracing::warn!(
                                "[discovery] '{}' didn't stop gracefully, killing",
                                name
                            );
                            let _ = child.kill().await;
                        }
                    }

                    process.handle = None;
                    process.state = DiscoveredProcessState::Stopped;
                }
            }
        }

        tracing::info!("[discovery] Shutdown complete");
    }
}

impl ProcessesConfig {
    /// Load a `.processes.json` file from the given path.
    pub fn load(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Parse from a JSON string.
    pub fn parse(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_processes_json_with_string_command() {
        let json = r#"{
            "processes": {
                "counter": {
                    "command": "python counter.py",
                    "owns": "counter.json",
                    "cwd": "/home/user/examples"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        let counter = &config.processes["counter"];
        assert!(matches!(counter.command, CommandSpec::Simple(_)));
        assert_eq!(counter.command.program(), "python");
        assert_eq!(counter.command.args(), vec!["counter.py"]);
        assert_eq!(counter.owns, "counter.json");
        assert_eq!(counter.cwd, PathBuf::from("/home/user/examples"));
    }

    #[test]
    fn test_parse_processes_json_with_array_command() {
        let json = r#"{
            "processes": {
                "server": {
                    "command": ["node", "server.js", "--port", "3000"],
                    "owns": "state.json",
                    "cwd": "/opt/app"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        let server = &config.processes["server"];
        assert!(matches!(server.command, CommandSpec::Array(_)));
        assert_eq!(server.command.program(), "node");
        assert_eq!(server.command.args(), vec!["server.js", "--port", "3000"]);
        assert_eq!(server.owns, "state.json");
        assert_eq!(server.cwd, PathBuf::from("/opt/app"));
    }

    #[test]
    fn test_command_spec_deserialization() {
        // Test simple string
        let simple: CommandSpec = serde_json::from_str(r#""echo hello world""#).unwrap();
        assert!(matches!(simple, CommandSpec::Simple(_)));
        assert_eq!(simple.program(), "echo");
        assert_eq!(simple.args(), vec!["hello", "world"]);

        // Test array
        let array: CommandSpec = serde_json::from_str(r#"["echo", "hello", "world"]"#).unwrap();
        assert!(matches!(array, CommandSpec::Array(_)));
        assert_eq!(array.program(), "echo");
        assert_eq!(array.args(), vec!["hello", "world"]);
    }

    #[test]
    fn test_command_spec_serialization() {
        let simple = CommandSpec::Simple("python script.py".to_string());
        let json = serde_json::to_string(&simple).unwrap();
        assert_eq!(json, r#""python script.py""#);

        let array = CommandSpec::Array(vec![
            "python".to_string(),
            "script.py".to_string(),
            "--verbose".to_string(),
        ]);
        let json = serde_json::to_string(&array).unwrap();
        assert_eq!(json, r#"["python","script.py","--verbose"]"#);
    }

    #[test]
    fn test_empty_command() {
        let spec = CommandSpec::Simple("".to_string());
        assert_eq!(spec.program(), "");
        assert!(spec.args().is_empty());

        let spec = CommandSpec::Array(vec![]);
        assert_eq!(spec.program(), "");
        assert!(spec.args().is_empty());
    }

    #[test]
    fn test_multiple_processes() {
        let json = r#"{
            "processes": {
                "frontend": {
                    "command": "npm start",
                    "owns": "frontend-state.json",
                    "cwd": "/app/frontend"
                },
                "backend": {
                    "command": ["cargo", "run", "--release"],
                    "owns": "backend-state.json",
                    "cwd": "/app/backend"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 2);
        assert!(config.processes.contains_key("frontend"));
        assert!(config.processes.contains_key("backend"));
    }

    #[test]
    fn test_discovered_process_manager_new() {
        let manager = DiscoveredProcessManager::new("localhost:1883".to_string());
        assert_eq!(manager.mqtt_broker(), "localhost:1883");
        assert!(manager.processes().is_empty());
    }

    #[test]
    fn test_add_process() {
        let mut manager = DiscoveredProcessManager::new("localhost:1883".to_string());

        let config = DiscoveredProcess {
            command: CommandSpec::Simple("python test.py".to_string()),
            owns: "test.json".to_string(),
            cwd: PathBuf::from("/tmp"),
        };

        manager.add_process("test".to_string(), "examples/test.json".to_string(), config);

        assert_eq!(manager.processes().len(), 1);
        let process = manager.processes().get("test").unwrap();
        assert_eq!(process.name, "test");
        assert_eq!(process.document_path, "examples/test.json");
        assert_eq!(process.state, DiscoveredProcessState::Stopped);
    }
}
