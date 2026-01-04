//! Process lifecycle management for discovered processes.
//!
//! This module handles the state machine and lifecycle management for processes
//! discovered from `.processes.json` files. For config parsing and discovery logic,
//! see the `discovery` module.

use super::discovery::{DiscoveredProcess, ProcessesConfig};
use super::status::{OrchestratorStatus, ProcessStatus};
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
#[cfg(unix)]
#[allow(unused_imports)]
use std::os::unix::process::CommandExt;
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
    /// The base_path (source) this process was loaded from
    /// Used to track which processes.json file defined this process
    pub source_path: String,
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

/// Result of recursive discovery - includes both processes.json files and all schema node_ids.
struct DiscoveryResult {
    /// (base_path, processes.json node_id)
    processes_json_files: Vec<(String, String)>,
    /// All directory schema node_ids (for watching changes)
    schema_node_ids: Vec<String>,
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

    /// Write current process status to the status file.
    /// Called when processes start, stop, or change state.
    pub fn write_status(&self) {
        let mut status = OrchestratorStatus::new();

        for (name, process) in &self.processes {
            let pid = process.handle.as_ref().and_then(|h| h.id());

            // Try to get CWD from config first, otherwise read from /proc/<pid>/cwd
            // For sandbox processes, this finds the deepest child's CWD
            let cwd = process
                .config
                .cwd
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .or_else(|| pid.and_then(super::status::get_process_cwd));

            let state = match process.state {
                DiscoveredProcessState::Stopped => "Stopped",
                DiscoveredProcessState::Starting => "Starting",
                DiscoveredProcessState::Running => "Running",
                DiscoveredProcessState::Failed => "Failed",
            };

            status.processes.push(ProcessStatus {
                name: name.clone(),
                pid,
                cwd,
                state: state.to_string(),
                document_path: Some(process.document_path.clone()),
                source_path: Some(process.source_path.clone()),
            });
        }

        // Merge with existing status (preserving base processes) and write
        if let Err(e) = status.merge_and_write(false) {
            tracing::warn!("[discovery] Failed to write status file: {}", e);
        }
    }

    /// Add a process to be managed.
    ///
    /// # Arguments
    /// * `name` - Unique name for this process
    /// * `document_path` - Full document path (e.g., "examples/counter.json")
    /// * `config` - Process configuration from `.processes.json`
    pub fn add_process(
        &mut self,
        name: String,
        document_path: String,
        source_path: String,
        config: DiscoveredProcess,
    ) {
        let process = ManagedDiscoveredProcess {
            name: name.clone(),
            document_path,
            source_path,
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
                        // Kill the entire process group
                        unsafe {
                            libc::killpg(pid as i32, libc::SIGTERM);
                        }
                    }
                }

                let timeout = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;

                if timeout.is_err() {
                    // Force kill the entire process group
                    #[cfg(unix)]
                    if let Some(pid) = child.id() {
                        unsafe {
                            libc::killpg(pid as i32, libc::SIGKILL);
                        }
                    }
                    let _ = child.kill().await;
                }
            }
            // Update status after removing process
            self.write_status();
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

        // Build command - either from sandbox-exec or command field
        let mut cmd = if let Some(ref exec_cmd) = config.sandbox_exec {
            // sandbox-exec: construct commonplace-sync --sandbox --exec "<cmd>"
            // Find commonplace-sync in the same directory as the orchestrator
            let sync_path = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.join("commonplace-sync")))
                .unwrap_or_else(|| PathBuf::from("commonplace-sync"));

            let mut cmd = Command::new(&sync_path);
            cmd.args(["--sandbox", "--exec", exec_cmd]);

            // Set cwd if specified, otherwise use current directory
            if let Some(ref cwd) = config.cwd {
                cmd.current_dir(cwd);
            }

            tracing::info!(
                "[discovery] Starting sandbox process '{}' (path: {}, exec: {})",
                name,
                document_path,
                exec_cmd
            );
            cmd
        } else if let Some(ref command_spec) = config.command {
            // Traditional command field
            let program = command_spec.program();
            if program.is_empty() {
                return Err(format!("Empty command for process: {}", name));
            }

            let mut cmd = Command::new(program);
            cmd.args(command_spec.args());

            // cwd is required for command-style processes
            let cwd = config
                .cwd
                .as_ref()
                .ok_or_else(|| format!("Process '{}' with command requires cwd", name))?;
            cmd.current_dir(cwd);

            tracing::info!(
                "[discovery] Starting process '{}' (path: {}, cwd: {:?})",
                name,
                document_path,
                cwd
            );
            cmd
        } else {
            return Err(format!(
                "Process '{}' must have either 'command' or 'sandbox-exec'",
                name
            ));
        };

        // Set environment variables for both modes
        cmd.env("COMMONPLACE_PATH", document_path);
        cmd.env("COMMONPLACE_MQTT", &mqtt_broker);
        cmd.env("COMMONPLACE_SERVER", &self.server_url);
        cmd.env("COMMONPLACE_INITIAL_SYNC", "server");

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

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

        // Update status file
        self.write_status();

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
        // Write final status after all started
        self.write_status();
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
                            process.state = DiscoveredProcessState::Failed;
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

                // Update status to reflect the failed state
                self.write_status();

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
    /// - Stops processes that were removed from the config (only from this source)
    /// - Starts processes that were added to the config
    /// - Restarts processes where the command/cwd changed
    ///
    /// # Arguments
    /// * `config` - The new ProcessesConfig to reconcile against
    /// * `base_path` - Base path for constructing document paths (also used as source identifier)
    pub async fn reconcile_config(
        &mut self,
        config: &ProcessesConfig,
        base_path: &str,
    ) -> Result<(), String> {
        // Only consider processes that came from this same source (base_path)
        let current_names_from_source: HashSet<String> = self
            .processes
            .iter()
            .filter(|(_, p)| p.source_path == base_path)
            .map(|(name, _)| name.clone())
            .collect();

        let new_names: HashSet<String> = config.processes.keys().cloned().collect();

        // Find processes to remove (in current from this source but not in new)
        let to_remove: Vec<String> = current_names_from_source
            .difference(&new_names)
            .cloned()
            .collect();

        // Find processes to add (in new but not currently managed from this source)
        let to_add: Vec<String> = new_names
            .difference(&current_names_from_source)
            .cloned()
            .collect();

        // Find processes that exist in both and may need restart
        let to_check: Vec<String> = current_names_from_source
            .intersection(&new_names)
            .cloned()
            .collect();

        // Remove processes that were in this source but are no longer defined
        for name in &to_remove {
            tracing::info!(
                "[discovery] Removing process '{}' (no longer in config at {})",
                name,
                base_path
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
                // Priority: explicit path > owns > base_path (processes.json location)
                // sandbox-exec processes sync at the directory containing their processes.json
                let document_path = if let Some(ref explicit_path) = new_config.path {
                    if explicit_path == "/" || explicit_path.is_empty() {
                        base_path.to_string()
                    } else {
                        format!("{}/{}", base_path, explicit_path.trim_start_matches('/'))
                    }
                } else if let Some(ref owns) = new_config.owns {
                    format!("{}/{}", base_path, owns)
                } else {
                    // sandbox-exec and command processes sync at base_path
                    base_path.to_string()
                };

                self.add_process(
                    name.clone(),
                    document_path,
                    base_path.to_string(),
                    new_config.clone(),
                );

                // Start it
                if let Err(e) = self.spawn_process(name).await {
                    tracing::error!("[discovery] Failed to restart '{}': {}", name, e);
                }
            }
        }

        // Add new processes
        for name in &to_add {
            let new_config = &config.processes[name];
            // Priority: explicit path > owns > base_path (processes.json location)
            // sandbox-exec processes sync at the directory containing their processes.json
            let document_path = if let Some(ref explicit_path) = new_config.path {
                if explicit_path == "/" || explicit_path.is_empty() {
                    base_path.to_string()
                } else {
                    format!("{}/{}", base_path, explicit_path.trim_start_matches('/'))
                }
            } else if let Some(ref owns) = new_config.owns {
                format!("{}/{}", base_path, owns)
            } else {
                // sandbox-exec and command processes sync at base_path
                base_path.to_string()
            };

            tracing::info!("[discovery] Adding new process '{}' from config", name);
            self.add_process(
                name.clone(),
                document_path,
                base_path.to_string(),
                new_config.clone(),
            );

            if let Err(e) = self.spawn_process(name).await {
                tracing::error!("[discovery] Failed to start new process '{}': {}", name, e);
            }
        }

        Ok(())
    }

    /// Check if a process configuration has changed in a way that requires restart.
    fn process_config_changed(current: &DiscoveredProcess, new: &DiscoveredProcess) -> bool {
        // Check if command changed
        let current_cmd = current
            .command
            .as_ref()
            .map(|c| (c.program().to_string(), c.args().join(" ")));
        let new_cmd = new
            .command
            .as_ref()
            .map(|c| (c.program().to_string(), c.args().join(" ")));
        if current_cmd != new_cmd {
            return true;
        }

        // Check if sandbox-exec changed
        if current.sandbox_exec != new.sandbox_exec {
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

    // Note: find_processes_json_recursive was replaced by discover_all_processes_json
    // which handles async fetching of subdirectory schemas.

    /// Recursively discover all processes.json files from a root schema.
    ///
    /// This fetches schemas for subdirectories with node_ids and finds all processes.json.
    /// Also returns all schema node_ids encountered for recursive watching.
    async fn discover_all_processes_json(
        client: &Client,
        server_url: &str,
        root_id: &str,
    ) -> Result<DiscoveryResult, String> {
        let mut processes_json_files = Vec::new();
        let mut schema_node_ids = Vec::new();
        let mut to_visit: Vec<(String, String)> = vec![("/".to_string(), root_id.to_string())];

        while let Some((path, node_id)) = to_visit.pop() {
            // Track this schema node_id for watching
            schema_node_ids.push(node_id.clone());

            // Fetch this node's schema
            let url = format!("{}/docs/{}/head", server_url, node_id);
            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("Failed to fetch {}: {}", url, e))?;

            if !resp.status().is_success() {
                tracing::warn!(
                    "[discovery] Failed to fetch schema for {}: HTTP {}",
                    path,
                    resp.status()
                );
                continue;
            }

            #[derive(Deserialize)]
            struct HeadResponse {
                content: String,
            }

            let head: HeadResponse = resp
                .json()
                .await
                .map_err(|e| format!("Failed to parse response: {}", e))?;

            let schema: serde_json::Value = serde_json::from_str(&head.content)
                .map_err(|e| format!("Failed to parse schema: {}", e))?;

            // Check for processes.json at this level
            if let Some(root) = schema.get("root") {
                if let Some(entries) = root.get("entries").and_then(|e| e.as_object()) {
                    for (name, entry) in entries {
                        let entry_type = entry.get("type").and_then(|t| t.as_str()).unwrap_or("");

                        if name == "processes.json" && entry_type == "doc" {
                            if let Some(pj_node_id) = entry.get("node_id").and_then(|n| n.as_str())
                            {
                                processes_json_files.push((path.clone(), pj_node_id.to_string()));
                            }
                        } else if entry_type == "dir" {
                            // Subdirectory - check if it has a node_id to recurse into
                            if let Some(sub_node_id) = entry.get("node_id").and_then(|n| n.as_str())
                            {
                                let sub_path = if path == "/" {
                                    format!("/{}", name)
                                } else {
                                    format!("{}/{}", path, name)
                                };
                                to_visit.push((sub_path, sub_node_id.to_string()));
                            }
                        }
                    }
                }
            }
        }

        Ok(DiscoveryResult {
            processes_json_files,
            schema_node_ids,
        })
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

    /// Run with recursive discovery of all processes.json files.
    ///
    /// This discovers all processes.json files in the filesystem tree starting from
    /// the given fs-root, and watches each one for changes. Each processes.json
    /// controls processes that sync at its containing directory.
    ///
    /// Watches ALL schema documents (directory node_ids) for changes, not just fs-root.
    pub async fn run_with_recursive_watch(
        &mut self,
        client: &Client,
        fs_root_id: &str,
    ) -> Result<(), String> {
        tracing::info!(
            "[discovery] Starting recursive discovery from fs-root: {}",
            fs_root_id
        );

        // Discover all processes.json files and schema node_ids
        let discovery =
            Self::discover_all_processes_json(client, &self.server_url, fs_root_id).await?;

        if discovery.processes_json_files.is_empty() {
            tracing::warn!("[discovery] No processes.json files found in filesystem tree");
            // Still watch the root for changes
        } else {
            tracing::info!(
                "[discovery] Found {} processes.json file(s): {:?}",
                discovery.processes_json_files.len(),
                discovery
                    .processes_json_files
                    .iter()
                    .map(|(p, _)| p)
                    .collect::<Vec<_>>()
            );
        }

        tracing::info!(
            "[discovery] Watching {} schema document(s) for structural changes",
            discovery.schema_node_ids.len()
        );

        // Track watched documents: node_id -> base_path
        // Only successfully loaded processes.json files are added to this map
        let mut watched: HashMap<String, String> = HashMap::new();

        // Fetch and reconcile each processes.json
        for (base_path, pj_node_id) in &discovery.processes_json_files {
            let url = format!("{}/docs/{}/head", self.server_url, pj_node_id);
            match self.fetch_and_reconcile(client, &url, base_path).await {
                Ok(_) => {
                    tracing::info!(
                        "[discovery] Loaded processes from {}/processes.json",
                        base_path
                    );
                    // Only add to watched if successfully loaded
                    watched.insert(pj_node_id.clone(), base_path.clone());
                }
                Err(e) => tracing::warn!(
                    "[discovery] Failed to load {}/processes.json: {} (will retry)",
                    base_path,
                    e
                ),
            }
        }

        // Track current schema node_ids for detecting new directories
        let mut current_schema_ids: HashSet<String> =
            discovery.schema_node_ids.iter().cloned().collect();

        // Monitor interval for checking process health
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));

        // Re-discovery interval (check for new/removed processes.json files)
        let mut discovery_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            // Build SSE URL from current watched state (schemas + processes.json files)
            // Rebuilt on every reconnect to ensure we don't miss newly discovered docs
            let mut all_ids: Vec<String> = current_schema_ids.iter().cloned().collect();
            for node_id in watched.keys() {
                all_ids.push(node_id.clone());
            }
            let all_ids_param = all_ids.join(",");
            let sse_url = format!(
                "{}/documents/stream?doc_ids={}",
                self.server_url, all_ids_param
            );

            let request_builder = client.get(&sse_url);

            let mut es = match EventSource::new(request_builder) {
                Ok(es) => es,
                Err(e) => {
                    tracing::error!(
                        "[discovery] Failed to create EventSource for schemas: {}",
                        e
                    );
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

                    // Periodic re-discovery of processes.json files
                    _ = discovery_interval.tick() => {
                        match Self::discover_all_processes_json(client, &self.server_url, fs_root_id).await {
                            Ok(new_discovery) => {
                                // Check for new or previously-failed processes.json files
                                let mut added_new_pj = false;
                                for (base_path, node_id) in &new_discovery.processes_json_files {
                                    if !watched.contains_key(node_id) {
                                        tracing::info!("[discovery] Trying processes.json at {}", base_path);
                                        let url = format!("{}/docs/{}/head", self.server_url, node_id);
                                        match self.fetch_and_reconcile(client, &url, base_path).await {
                                            Ok(_) => {
                                                tracing::info!("[discovery] Loaded processes from {}/processes.json", base_path);
                                                // Only add to watched if successfully loaded
                                                watched.insert(node_id.clone(), base_path.clone());
                                                added_new_pj = true;
                                            }
                                            Err(e) => {
                                                tracing::warn!("[discovery] Failed to load {}: {} (will retry)", base_path, e);
                                            }
                                        }
                                    }
                                }

                                // Check for removed processes.json files
                                let new_ids: HashSet<_> = new_discovery.processes_json_files.iter().map(|(_, id)| id.clone()).collect();
                                let removed: Vec<_> = watched.keys()
                                    .filter(|id| !new_ids.contains(*id))
                                    .cloned()
                                    .collect();
                                for node_id in removed {
                                    if let Some(base_path) = watched.remove(&node_id) {
                                        tracing::info!("[discovery] processes.json removed at {}", base_path);
                                        // Remove processes that were from this base_path
                                        let to_remove: Vec<_> = self.processes.iter()
                                            .filter(|(_, p)| p.document_path.starts_with(&base_path))
                                            .map(|(n, _)| n.clone())
                                            .collect();
                                        for name in to_remove {
                                            self.remove_process(&name).await;
                                        }
                                    }
                                }

                                // Check for new schema node_ids (new directories added)
                                let new_schema_ids: HashSet<String> = new_discovery.schema_node_ids.iter().cloned().collect();
                                let schema_changed = new_schema_ids != current_schema_ids;
                                if schema_changed {
                                    let added: Vec<_> = new_schema_ids.difference(&current_schema_ids).collect();
                                    let removed: Vec<_> = current_schema_ids.difference(&new_schema_ids).collect();
                                    tracing::info!(
                                        "[discovery] Schema set changed: +{} -{} directories",
                                        added.len(),
                                        removed.len()
                                    );
                                    current_schema_ids = new_schema_ids;
                                }

                                // Reconnect SSE if schemas changed or new processes.json added
                                // (URL is rebuilt on each loop iteration with current watch set)
                                if schema_changed || added_new_pj {
                                    sse_closed = true;
                                }
                            }
                            Err(e) => {
                                tracing::warn!("[discovery] Re-discovery failed: {}", e);
                            }
                        }
                    }

                    // SSE event from any schema document (structural changes)
                    event = es.next() => {
                        match event {
                            Some(Ok(SseEvent::Open)) => {
                                tracing::info!(
                                    "[discovery] SSE connection opened for {} schema(s)",
                                    current_schema_ids.len()
                                );
                            }
                            Some(Ok(SseEvent::Message(msg))) => {
                                // /documents/stream sends "commit" events with doc_id in payload
                                if msg.event == "commit" {
                                    // Parse the commit event to get doc_id
                                    if let Ok(commit) = serde_json::from_str::<serde_json::Value>(&msg.data) {
                                        if let Some(doc_id) = commit.get("doc_id").and_then(|v| v.as_str()) {
                                            // Check if this is a processes.json file we're watching
                                            if let Some(base_path) = watched.get(doc_id) {
                                                tracing::info!(
                                                    "[discovery] processes.json changed at {}, reconciling...",
                                                    base_path
                                                );
                                                // Fetch and reconcile this specific processes.json
                                                let url = format!("{}/docs/{}/head", self.server_url, doc_id);
                                                let base_path = base_path.clone();
                                                if let Err(e) = self.fetch_and_reconcile(client, &url, &base_path).await {
                                                    tracing::error!(
                                                        "[discovery] Failed to reconcile {}/processes.json: {}",
                                                        base_path, e
                                                    );
                                                }
                                            } else {
                                                // It's a schema change - trigger re-discovery
                                                tracing::info!(
                                                    "[discovery] Schema {} changed, re-discovering...",
                                                    doc_id
                                                );
                                                discovery_interval.reset();
                                            }
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("[discovery] SSE error: {}", e);
                                sse_closed = true;
                            }
                            None => {
                                tracing::info!("[discovery] SSE stream ended");
                                sse_closed = true;
                            }
                        }

                        if sse_closed {
                            break;
                        }
                    }
                }
            }

            if sse_closed {
                tracing::info!("[discovery] Reconnecting to schema stream SSE...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
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
                            // Kill the entire process group (negative pid)
                            // This ensures all descendants are terminated
                            tracing::debug!("[discovery] Sending SIGTERM to process group {}", pid);
                            unsafe {
                                libc::killpg(pid as i32, libc::SIGTERM);
                            }
                        }
                    }

                    // Give processes 10 seconds to shutdown gracefully
                    // This allows sync to properly terminate its exec child
                    let timeout = tokio::time::timeout(Duration::from_secs(10), child.wait()).await;

                    match timeout {
                        Ok(Ok(_)) => {
                            tracing::info!("[discovery] '{}' stopped gracefully", name);
                        }
                        _ => {
                            tracing::warn!(
                                "[discovery] '{}' didn't stop gracefully after 10s, force killing process group",
                                name
                            );
                            // Force kill the entire process group
                            #[cfg(unix)]
                            if let Some(pid) = child.id() {
                                tracing::debug!(
                                    "[discovery] Sending SIGKILL to process group {}",
                                    pid
                                );
                                unsafe {
                                    libc::killpg(pid as i32, libc::SIGKILL);
                                }
                            }
                            let _ = child.kill().await;
                        }
                    }

                    process.handle = None;
                    process.state = DiscoveredProcessState::Stopped;
                }
            }
        }

        // Remove status file on shutdown
        if let Err(e) = OrchestratorStatus::remove() {
            tracing::warn!("[discovery] Failed to remove status file: {}", e);
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
            command: Some(CommandSpec::Simple("python test.py".to_string())),
            sandbox_exec: None,
            path: None,
            owns: Some("test.json".to_string()),
            cwd: Some(PathBuf::from("/tmp")),
        };

        manager.add_process(
            "test".to_string(),
            "examples/test.json".to_string(),
            "examples".to_string(),
            config,
        );

        assert_eq!(manager.processes().len(), 1);
        let process = manager.processes().get("test").unwrap();
        assert_eq!(process.name, "test");
        assert_eq!(process.document_path, "examples/test.json");
        assert_eq!(process.source_path, "examples");
        assert_eq!(process.state, DiscoveredProcessState::Stopped);
    }

    #[test]
    fn test_add_directory_attached_process() {
        let mut manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:3000".to_string(),
        );

        let config = DiscoveredProcess {
            command: Some(CommandSpec::Simple("sync --sandbox".to_string())),
            sandbox_exec: None,
            path: None,
            owns: None,
            cwd: Some(PathBuf::from("/tmp")),
        };

        // For directory-attached, document_path is just the directory
        manager.add_process(
            "sandbox".to_string(),
            "examples".to_string(),
            "examples".to_string(),
            config,
        );

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
