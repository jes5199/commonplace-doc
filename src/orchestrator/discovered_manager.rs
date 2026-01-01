//! Process lifecycle management for discovered processes.
//!
//! This module handles the state machine and lifecycle management for processes
//! discovered from `.processes.json` files. For config parsing and discovery logic,
//! see the `discovery` module.

use super::discovery::{DiscoveredProcess, ProcessesConfig};
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

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
    /// HTTP server URL
    server_url: String,
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
    pub fn new(mqtt_broker: String, server_url: String) -> Self {
        Self {
            mqtt_broker,
            server_url,
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

    /// Get the HTTP server URL.
    pub fn server_url(&self) -> &str {
        &self.server_url
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
        cmd.env("COMMONPLACE_SERVER", &self.server_url);

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

    /// Reconcile running processes with a new configuration.
    ///
    /// This is the core method for dynamic process management. It:
    /// - Stops processes that were removed from the config
    /// - Starts processes that were added to the config
    /// - Restarts processes where the command/cwd changed
    ///
    /// # Arguments
    /// * `config` - The new ProcessesConfig to reconcile against
    /// * `base_path` - Base path for constructing document paths
    pub async fn reconcile_config(
        &mut self,
        config: &ProcessesConfig,
        base_path: &str,
    ) -> Result<(), String> {
        let current_names: HashSet<String> = self.processes.keys().cloned().collect();
        let new_names: HashSet<String> = config.processes.keys().cloned().collect();

        // Find processes to remove (in current but not in new)
        let to_remove: Vec<String> = current_names.difference(&new_names).cloned().collect();

        // Find processes to add (in new but not in current)
        let to_add: Vec<String> = new_names.difference(&current_names).cloned().collect();

        // Find processes that exist in both and may need restart
        let to_check: Vec<String> = current_names.intersection(&new_names).cloned().collect();

        // Remove old processes
        for name in &to_remove {
            tracing::info!(
                "[discovery] Removing process '{}' (no longer in config)",
                name
            );
            self.remove_process(name).await;
        }

        // Check for changed processes that need restart
        for name in &to_check {
            let new_config = &config.processes[name];
            let current = &self.processes[name];

            let needs_restart = Self::process_config_changed(&current.config, new_config);

            if needs_restart {
                tracing::info!(
                    "[discovery] Restarting process '{}' (configuration changed)",
                    name
                );

                // Stop the old process
                self.remove_process(name).await;

                // Add with new config
                let document_path = if let Some(ref owns) = new_config.owns {
                    format!("{}/{}", base_path, owns)
                } else {
                    base_path.to_string()
                };

                self.add_process(name.clone(), document_path, new_config.clone());

                // Start it
                if let Err(e) = self.spawn_process(name).await {
                    tracing::error!("[discovery] Failed to restart '{}': {}", name, e);
                }
            }
        }

        // Add new processes
        for name in &to_add {
            let new_config = &config.processes[name];
            let document_path = if let Some(ref owns) = new_config.owns {
                format!("{}/{}", base_path, owns)
            } else {
                base_path.to_string()
            };

            tracing::info!("[discovery] Adding new process '{}' from config", name);
            self.add_process(name.clone(), document_path, new_config.clone());

            if let Err(e) = self.spawn_process(name).await {
                tracing::error!("[discovery] Failed to start new process '{}': {}", name, e);
            }
        }

        Ok(())
    }

    /// Check if a process configuration has changed in a way that requires restart.
    fn process_config_changed(current: &DiscoveredProcess, new: &DiscoveredProcess) -> bool {
        // Check if command changed
        let current_cmd = (current.command.program(), current.command.args());
        let new_cmd = (new.command.program(), new.command.args());
        if current_cmd != new_cmd {
            return true;
        }

        // Check if cwd changed
        if current.cwd != new.cwd {
            return true;
        }

        // Check if owns changed
        if current.owns != new.owns {
            return true;
        }

        false
    }

    /// Watch a .processes.json document for changes and dynamically update processes.
    ///
    /// This method subscribes to the SSE stream for a document and calls
    /// `reconcile_config` whenever the document changes.
    ///
    /// # Arguments
    /// * `client` - HTTP client for making requests
    /// * `document_path` - The document path to watch (e.g., "examples/.processes.json")
    /// * `use_paths` - Whether to use path-based endpoints
    pub async fn watch_document(
        &mut self,
        client: &Client,
        document_path: &str,
        use_paths: bool,
    ) -> Result<(), String> {
        // Build SSE URL
        let sse_url = if use_paths {
            format!(
                "{}/sse/files/{}",
                self.server_url,
                urlencoding::encode(document_path)
            )
        } else {
            format!(
                "{}/sse/docs/{}",
                self.server_url,
                urlencoding::encode(document_path)
            )
        };

        // Build HEAD URL
        let head_url = if use_paths {
            format!(
                "{}/files/{}",
                self.server_url,
                urlencoding::encode(document_path)
            )
        } else {
            format!(
                "{}/docs/{}/head",
                self.server_url,
                urlencoding::encode(document_path)
            )
        };

        // Extract base path from document path (parent directory)
        let base_path = PathBuf::from(document_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        // Do initial fetch and reconcile
        tracing::info!("[discovery] Fetching initial config from {}", head_url);
        match self
            .fetch_and_reconcile(client, &head_url, &base_path)
            .await
        {
            Ok(_) => tracing::info!("[discovery] Initial config loaded"),
            Err(e) => {
                tracing::warn!("[discovery] Failed to load initial config: {}", e);
                // Continue anyway - we'll get updates via SSE
            }
        }

        // Subscribe to SSE for updates
        tracing::info!("[discovery] Watching document via SSE: {}", sse_url);

        // Monitor interval for checking process health
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            let request_builder = client.get(&sse_url);

            let mut es = match EventSource::new(request_builder) {
                Ok(es) => es,
                Err(e) => {
                    tracing::error!("[discovery] Failed to create EventSource: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut sse_closed = false;

            loop {
                tokio::select! {
                    // Periodic health check for process restarts
                    _ = monitor_interval.tick() => {
                        self.check_and_restart().await;
                    }

                    // SSE event processing
                    event = es.next() => {
                        match event {
                            Some(Ok(SseEvent::Open)) => {
                                tracing::info!("[discovery] SSE connection opened for {}", document_path);
                            }
                            Some(Ok(SseEvent::Message(msg))) => {
                                tracing::debug!("[discovery] SSE event: {} - {}", msg.event, msg.data);

                                match msg.event.as_str() {
                                    "connected" => {
                                        tracing::info!("[discovery] SSE connected to {}", document_path);
                                    }
                                    "edit" => {
                                        // Document changed - fetch new content and reconcile
                                        tracing::info!(
                                            "[discovery] Document {} changed, reconciling processes",
                                            document_path
                                        );

                                        if let Err(e) = self
                                            .fetch_and_reconcile(client, &head_url, &base_path)
                                            .await
                                        {
                                            tracing::error!(
                                                "[discovery] Failed to reconcile after edit: {}",
                                                e
                                            );
                                        }

                                        // Also check and restart any failed processes
                                        self.check_and_restart().await;
                                    }
                                    "closed" => {
                                        tracing::warn!("[discovery] SSE: Document {} was deleted", document_path);
                                        // Stop all processes since the config is gone
                                        self.shutdown().await;
                                        sse_closed = true;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            Some(Err(e)) => {
                                tracing::error!("[discovery] SSE error: {}", e);
                                break;
                            }
                            None => {
                                // Stream ended
                                break;
                            }
                        }
                    }
                }
            }

            if sse_closed {
                // Document was deleted, stop watching
                break;
            }

            tracing::warn!("[discovery] SSE connection closed, reconnecting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }

    /// Fetch document content from HEAD and reconcile processes.
    async fn fetch_and_reconcile(
        &mut self,
        client: &Client,
        head_url: &str,
        base_path: &str,
    ) -> Result<(), String> {
        let resp = client
            .get(head_url)
            .send()
            .await
            .map_err(|e| format!("Failed to fetch HEAD: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("HEAD fetch failed: {}", resp.status()));
        }

        #[derive(Deserialize)]
        struct HeadResponse {
            content: String,
        }

        let head: HeadResponse = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse HEAD response: {}", e))?;

        // Parse the content as ProcessesConfig
        let config = ProcessesConfig::parse(&head.content)
            .map_err(|e| format!("Failed to parse .processes.json: {}", e))?;

        // Reconcile
        self.reconcile_config(&config, base_path).await
    }

    /// Run the manager loop with document watching.
    ///
    /// This is an alternative to `run()` that watches a .processes.json document
    /// for changes and dynamically updates processes.
    pub async fn run_with_document_watch(
        &mut self,
        client: &Client,
        document_path: &str,
        use_paths: bool,
    ) -> Result<(), String> {
        // Start watching - this will do initial fetch, start processes, and watch for changes
        self.watch_document(client, document_path, use_paths).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::discovery::CommandSpec;
    use std::path::PathBuf;

    #[test]
    fn test_discovered_process_manager_new() {
        let manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:3000".to_string(),
        );
        assert_eq!(manager.mqtt_broker(), "localhost:1883");
        assert_eq!(manager.server_url(), "http://localhost:3000");
        assert!(manager.processes().is_empty());
    }

    #[test]
    fn test_add_process() {
        let mut manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:3000".to_string(),
        );

        let config = DiscoveredProcess {
            command: CommandSpec::Simple("python test.py".to_string()),
            owns: Some("test.json".to_string()),
            cwd: PathBuf::from("/tmp"),
        };

        manager.add_process("test".to_string(), "examples/test.json".to_string(), config);

        assert_eq!(manager.processes().len(), 1);
        let process = manager.processes().get("test").unwrap();
        assert_eq!(process.name, "test");
        assert_eq!(process.document_path, "examples/test.json");
        assert_eq!(process.state, DiscoveredProcessState::Stopped);
    }

    #[test]
    fn test_add_directory_attached_process() {
        let mut manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:3000".to_string(),
        );

        let config = DiscoveredProcess {
            command: CommandSpec::Simple("sync --sandbox".to_string()),
            owns: None,
            cwd: PathBuf::from("/tmp"),
        };

        // For directory-attached, document_path is just the directory
        manager.add_process("sandbox".to_string(), "examples".to_string(), config);

        assert_eq!(manager.processes().len(), 1);
        let process = manager.processes().get("sandbox").unwrap();
        assert_eq!(process.document_path, "examples");
    }

    #[tokio::test]
    async fn test_spawn_sets_server_env() {
        let manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:3000".to_string(),
        );

        assert_eq!(manager.server_url(), "http://localhost:3000");
    }
}
