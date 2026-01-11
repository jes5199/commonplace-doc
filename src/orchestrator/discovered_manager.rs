//! Process lifecycle management for discovered processes.
//!
//! This module handles the state machine and lifecycle management for processes
//! discovered from `__processes.json` files. For config parsing and discovery logic,
//! see the `discovery` module.

use super::discovery::{DiscoveredProcess, ProcessesConfig};
use super::process_utils::stop_process_gracefully;
use super::spawn::spawn_managed_process;
use super::status::OrchestratorStatus;
use crate::sync::encode_path;
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
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
    /// Used to track which __processes.json file defined this process
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

/// Manager for processes discovered from `__processes.json` files.
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

/// Result of recursive discovery - includes both __processes.json files and all schema node_ids.
struct DiscoveryResult {
    /// (base_path, __processes.json node_id)
    processes_json_files: Vec<(String, String)>,
    /// All directory schema node_ids (for watching changes)
    schema_node_ids: Vec<String>,
}

/// Mapping from script document UUID to the process names that use it.
/// Used to restart evaluate processes when their script changes.
/// Multiple processes can share the same script.
type ScriptWatchMap = HashMap<String, Vec<String>>;

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

            let state = match process.state {
                DiscoveredProcessState::Stopped => "Stopped",
                DiscoveredProcessState::Starting => "Starting",
                DiscoveredProcessState::Running => "Running",
                DiscoveredProcessState::Failed => "Failed",
            };

            status.processes.push(super::status::build_process_status(
                name.clone(),
                pid,
                process.config.cwd.as_deref(),
                state,
                Some(process.document_path.clone()),
                Some(process.source_path.clone()),
            ));
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
    /// * `config` - Process configuration from `__processes.json`
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
                stop_process_gracefully(child, name, "[discovery]", Duration::from_secs(5)).await;
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

        // Build command - either from sandbox-exec, command, or evaluate field
        let mut cmd = if let Some(ref exec_cmd) = config.sandbox_exec {
            // sandbox-exec: construct commonplace-sync --sandbox --exec "<cmd>"
            // Find commonplace-sync in the same directory as the orchestrator
            let sync_path = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.join("commonplace-sync")))
                .unwrap_or_else(|| PathBuf::from("commonplace-sync"));

            let mut cmd = Command::new(&sync_path);
            cmd.args(["--sandbox", "--name", name, "--exec", exec_cmd]);

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
        } else if let Some(ref script) = config.evaluate {
            // evaluate: spawn Deno with the script URL from the server
            let server = self.server_url.clone();
            let broker = self.mqtt_broker.clone();

            // Validate script path - reject potentially dangerous patterns
            if script.contains("..")
                || script.contains('?')
                || script.contains('#')
                || script.starts_with('/')
            {
                tracing::error!(
                    "Invalid evaluate script path '{}': must be a simple filename",
                    script
                );
                return Err(format!(
                    "Invalid evaluate script path '{}': must be a simple filename",
                    script
                ));
            }

            // Construct the script URL: http://{server}/files/{dir}/{script}
            // URL-encode the path segments to handle spaces and special characters
            let full_path = format!("{}/{}", document_path, script);
            // Strip leading slash to avoid double-slash in URL
            let full_path = full_path.trim_start_matches('/');
            let script_url = format!("{}/files/{}", server, encode_path(full_path));

            // Generate a unique client ID for this process instance
            let client_id = format!("{}-{}", name, &uuid::Uuid::new_v4().to_string()[..8]);

            // Extract host:port from server URL for --allow-net
            let server_host = server.replace("http://", "").replace("https://", "");

            let mut cmd = Command::new("deno");
            cmd.arg("run")
                // Allow npm registry for package downloads, plus server and broker
                .arg(format!(
                    "--allow-net={},{},registry.npmjs.org,cdn.jsdelivr.net,esm.sh",
                    server_host, broker
                ))
                // Allow all env for npm package compatibility (readable-stream, etc.)
                .arg("--allow-env")
                .arg(&script_url);

            // Set environment variables for the SDK
            cmd.env("COMMONPLACE_SERVER", &server);
            cmd.env("COMMONPLACE_BROKER", &broker);
            cmd.env("COMMONPLACE_CLIENT_ID", &client_id);

            // If this process owns an output file, set COMMONPLACE_OUTPUT
            if let Some(ref output) = config.owns {
                let output_path = format!("{}/{}", document_path, output);
                cmd.env("COMMONPLACE_OUTPUT", output_path);
            }

            // Set cwd if specified
            if let Some(ref cwd) = config.cwd {
                cmd.current_dir(cwd);
            }

            tracing::info!(
                "[discovery] Starting evaluate process '{}' (script: {}, url: {})",
                name,
                script,
                script_url
            );
            cmd
        } else {
            return Err(format!(
                "Process '{}' must have 'command', 'sandbox-exec', or 'evaluate'",
                name
            ));
        };

        // Set environment variables for both modes
        cmd.env("COMMONPLACE_PATH", document_path);
        cmd.env("COMMONPLACE_MQTT", &mqtt_broker);
        cmd.env("COMMONPLACE_SERVER", &self.server_url);
        cmd.env("COMMONPLACE_INITIAL_SYNC", "server");

        let child = spawn_managed_process(cmd, name)?;
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
                // Priority: explicit path > owns > base_path (__processes.json location)
                // sandbox-exec processes sync at the directory containing their __processes.json
                // NOTE: evaluate processes use base_path for script URL, NOT owns (owns is just output)
                let document_path = if let Some(ref explicit_path) = new_config.path {
                    if explicit_path == "/" || explicit_path.is_empty() {
                        base_path.to_string()
                    } else {
                        format!("{}/{}", base_path, explicit_path.trim_start_matches('/'))
                    }
                } else if new_config.evaluate.is_some() {
                    // evaluate processes: script is relative to base_path, not owns
                    base_path.to_string()
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
            // NOTE: evaluate processes use base_path for script URL, NOT owns (owns is just output)
            let document_path = if let Some(ref explicit_path) = new_config.path {
                if explicit_path == "/" || explicit_path.is_empty() {
                    base_path.to_string()
                } else {
                    format!("{}/{}", base_path, explicit_path.trim_start_matches('/'))
                }
            } else if new_config.evaluate.is_some() {
                // evaluate processes: script is relative to base_path, not owns
                base_path.to_string()
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

        // Check if evaluate changed
        if current.evaluate != new.evaluate {
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

    /// Recursively discover all __processes.json files from a root schema.
    ///
    /// This fetches schemas for subdirectories with node_ids and finds all __processes.json.
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

            // Check for __processes.json at this level
            if let Some(root) = schema.get("root") {
                if let Some(entries) = root.get("entries").and_then(|e| e.as_object()) {
                    for (name, entry) in entries {
                        let entry_type = entry.get("type").and_then(|t| t.as_str()).unwrap_or("");

                        if name == "__processes.json" && entry_type == "doc" {
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

    /// Resolve a document path to its UUID by traversing the schema tree.
    ///
    /// For example, "bartleby/script.js" would:
    /// 1. Fetch fs-root, find bartleby's node_id
    /// 2. Fetch bartleby's schema, find script.js's node_id
    async fn resolve_path_to_uuid(
        client: &Client,
        server_url: &str,
        fs_root_id: &str,
        path: &str,
    ) -> Result<String, String> {
        use crate::sync::resolve_path_to_uuid_http;

        resolve_path_to_uuid_http(client, server_url, fs_root_id, path)
            .await
            .map_err(|e| e.to_string())
    }

    /// Resolve script paths for all evaluate processes and build a watch map.
    ///
    /// Returns a map of script_uuid -> process_name for all evaluate processes
    /// that have scripts that can be resolved.
    async fn resolve_script_watches(&self, client: &Client, fs_root_id: &str) -> ScriptWatchMap {
        let mut script_watches = ScriptWatchMap::new();

        for (name, process) in &self.processes {
            if let Some(ref script) = process.config.evaluate {
                // Construct the full script path: document_path/script
                let script_path = format!("{}/{}", process.document_path, script);

                match Self::resolve_path_to_uuid(client, &self.server_url, fs_root_id, &script_path)
                    .await
                {
                    Ok(script_uuid) => {
                        tracing::info!(
                            "[discovery] Watching script '{}' (uuid: {}) for process '{}'",
                            script_path,
                            script_uuid,
                            name
                        );
                        script_watches
                            .entry(script_uuid)
                            .or_default()
                            .push(name.clone());
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[discovery] Could not resolve script '{}' for process '{}': {}",
                            script_path,
                            name,
                            e
                        );
                    }
                }
            }
        }

        script_watches
    }

    /// Restart a process by name.
    ///
    /// This is used when a script file changes - we stop and restart the evaluate process.
    async fn restart_process(&mut self, name: &str) -> Result<(), String> {
        // Scope the mutable borrow to stop the process
        {
            let process = self
                .processes
                .get_mut(name)
                .ok_or_else(|| format!("Process '{}' not found", name))?;

            // Stop the process if running
            if let Some(ref mut child) = process.handle {
                tracing::info!("[discovery] Stopping process '{}' for restart", name);
                stop_process_gracefully(child, name, "[discovery]", Duration::from_secs(5)).await;
                process.handle = None;
            }

            // Reset failure count since this is an intentional restart
            process.consecutive_failures = 0;
            process.state = DiscoveredProcessState::Stopped;
        }

        // Respawn (borrow is released)
        self.spawn_process(name).await
    }

    /// Watch a __processes.json document for changes and dynamically update processes.
    ///
    /// This method subscribes to the SSE stream for a document and calls
    /// `reconcile_config` whenever the document changes.
    ///
    /// # Arguments
    /// * `client` - HTTP client for making requests
    /// * `document_path` - The document path to watch (e.g., "examples/__processes.json")
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
            .map_err(|e| format!("Failed to parse __processes.json: {}", e))?;

        // Reconcile
        self.reconcile_config(&config, base_path).await
    }

    /// Run the manager loop with document watching.
    ///
    /// This is an alternative to `run()` that watches a __processes.json document
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

    /// Run with recursive discovery of all __processes.json files.
    ///
    /// This discovers all __processes.json files in the filesystem tree starting from
    /// the given fs-root, and watches each one for changes. Each __processes.json
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

        // Discover all __processes.json files and schema node_ids
        let discovery =
            Self::discover_all_processes_json(client, &self.server_url, fs_root_id).await?;

        if discovery.processes_json_files.is_empty() {
            tracing::warn!("[discovery] No __processes.json files found in filesystem tree");
            // Still watch the root for changes
        } else {
            tracing::info!(
                "[discovery] Found {} __processes.json file(s): {:?}",
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
        // Only successfully loaded __processes.json files are added to this map
        let mut watched: HashMap<String, String> = HashMap::new();

        // Fetch and reconcile each __processes.json
        for (base_path, pj_node_id) in &discovery.processes_json_files {
            let url = format!("{}/docs/{}/head", self.server_url, pj_node_id);
            match self.fetch_and_reconcile(client, &url, base_path).await {
                Ok(_) => {
                    tracing::info!(
                        "[discovery] Loaded processes from {}/__processes.json",
                        base_path
                    );
                    // Only add to watched if successfully loaded
                    watched.insert(pj_node_id.clone(), base_path.clone());
                }
                Err(e) => tracing::warn!(
                    "[discovery] Failed to load {}/__processes.json: {} (will retry)",
                    base_path,
                    e
                ),
            }
        }

        // Track current schema node_ids for detecting new directories
        let mut current_schema_ids: HashSet<String> =
            discovery.schema_node_ids.iter().cloned().collect();

        // Resolve and track script files for evaluate processes
        // script_uuid -> process_name
        let mut script_watches: ScriptWatchMap =
            self.resolve_script_watches(client, fs_root_id).await;

        // Monitor interval for checking process health
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));

        // Re-discovery interval (check for new/removed __processes.json files)
        let mut discovery_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            // Build SSE URL from current watched state (schemas + __processes.json files + scripts)
            // Rebuilt on every reconnect to ensure we don't miss newly discovered docs
            let mut all_ids: Vec<String> = current_schema_ids.iter().cloned().collect();
            for node_id in watched.keys() {
                all_ids.push(node_id.clone());
            }
            // Add script UUIDs to watch list
            for script_uuid in script_watches.keys() {
                all_ids.push(script_uuid.clone());
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

                    // Periodic re-discovery of __processes.json files
                    _ = discovery_interval.tick() => {
                        match Self::discover_all_processes_json(client, &self.server_url, fs_root_id).await {
                            Ok(new_discovery) => {
                                // Check for new or previously-failed __processes.json files
                                let mut added_new_pj = false;
                                for (base_path, node_id) in &new_discovery.processes_json_files {
                                    if !watched.contains_key(node_id) {
                                        tracing::info!("[discovery] Trying __processes.json at {}", base_path);
                                        let url = format!("{}/docs/{}/head", self.server_url, node_id);
                                        match self.fetch_and_reconcile(client, &url, base_path).await {
                                            Ok(_) => {
                                                tracing::info!("[discovery] Loaded processes from {}/__processes.json", base_path);
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

                                // Check for removed __processes.json files
                                let new_ids: HashSet<_> = new_discovery.processes_json_files.iter().map(|(_, id)| id.clone()).collect();
                                let removed_pj: Vec<_> = watched.keys()
                                    .filter(|id| !new_ids.contains(*id))
                                    .cloned()
                                    .collect();
                                let _had_removed_pj = !removed_pj.is_empty();
                                for node_id in removed_pj {
                                    if let Some(base_path) = watched.remove(&node_id) {
                                        tracing::info!("[discovery] __processes.json removed at {}", base_path);
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

                                // Update script watches - always rebuild to catch any changes
                                // (not just when files are added/removed, but also when existing
                                // __processes.json files are edited to add/change evaluate entries)
                                let new_script_watches = self.resolve_script_watches(client, fs_root_id).await;
                                let scripts_changed = new_script_watches != script_watches;
                                script_watches = new_script_watches;

                                // Reconnect SSE if schemas changed, new __processes.json added, or scripts changed
                                // (URL is rebuilt on each loop iteration with current watch set)
                                if schema_changed || added_new_pj || scripts_changed {
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
                                            // Check if this is a __processes.json file we're watching
                                            if let Some(base_path) = watched.get(doc_id) {
                                                tracing::info!(
                                                    "[discovery] __processes.json changed at {}, reconciling...",
                                                    base_path
                                                );
                                                // Fetch and reconcile this specific __processes.json
                                                let url = format!("{}/docs/{}/head", self.server_url, doc_id);
                                                let base_path = base_path.clone();
                                                if let Err(e) = self.fetch_and_reconcile(client, &url, &base_path).await {
                                                    tracing::error!(
                                                        "[discovery] Failed to reconcile {}/__processes.json: {}",
                                                        base_path, e
                                                    );
                                                }
                                                // After reconciling, update script watches
                                                // (processes may have been added/removed/changed)
                                                script_watches = self.resolve_script_watches(client, fs_root_id).await;
                                            } else if let Some(process_names) = script_watches.get(doc_id) {
                                                // This is a script file change - restart all associated processes
                                                for process_name in process_names.clone() {
                                                    tracing::info!(
                                                        "[discovery] Script {} changed, restarting process '{}'",
                                                        doc_id,
                                                        process_name
                                                    );
                                                    if let Err(e) = self.restart_process(&process_name).await {
                                                        tracing::error!(
                                                            "[discovery] Failed to restart '{}' after script change: {}",
                                                            process_name, e
                                                        );
                                                    }
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

                    // Give processes 10 seconds to shutdown gracefully
                    // This allows sync to properly terminate its exec child
                    stop_process_gracefully(child, &name, "[discovery]", Duration::from_secs(10))
                        .await;

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
            "http://localhost:5199".to_string(),
        );
        assert_eq!(manager.mqtt_broker(), "localhost:1883");
        assert_eq!(manager.server_url(), "http://localhost:5199");
        assert!(manager.processes().is_empty());
    }

    #[test]
    fn test_add_process() {
        let mut manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:5199".to_string(),
        );

        let config = DiscoveredProcess {
            command: Some(CommandSpec::Simple("python test.py".to_string())),
            sandbox_exec: None,
            path: None,
            owns: Some("test.json".to_string()),
            cwd: Some(PathBuf::from("/tmp")),
            evaluate: None,
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
            "http://localhost:5199".to_string(),
        );

        let config = DiscoveredProcess {
            command: Some(CommandSpec::Simple("sync --sandbox".to_string())),
            sandbox_exec: None,
            path: None,
            owns: None,
            cwd: Some(PathBuf::from("/tmp")),
            evaluate: None,
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
            "http://localhost:5199".to_string(),
        );

        assert_eq!(manager.server_url(), "http://localhost:5199");
    }

    #[test]
    fn test_add_evaluate_process() {
        let mut manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:5199".to_string(),
        );

        let config = DiscoveredProcess {
            command: None,
            sandbox_exec: None,
            path: None,
            owns: Some("output.json".to_string()),
            cwd: None,
            evaluate: Some("script.js".to_string()),
        };

        manager.add_process(
            "evaluator".to_string(),
            "workspace/myapp".to_string(),
            "workspace/myapp".to_string(),
            config,
        );

        assert_eq!(manager.processes().len(), 1);
        let process = manager.processes().get("evaluator").unwrap();
        assert_eq!(process.name, "evaluator");
        assert_eq!(process.document_path, "workspace/myapp");
        assert_eq!(process.config.evaluate, Some("script.js".to_string()));
    }
}
