//! Process lifecycle management for discovered processes.
//!
//! This module handles the state machine and lifecycle management for processes
//! discovered from `__processes.json` files. For config parsing and discovery logic,
//! see the `discovery` module.

use super::config_reconciler::{ConfigReconciler, ReconcileResult};
use super::discovery::{DiscoveredProcess, ProcessesConfig};
use super::mqtt_watcher::{MqttDocumentWatcher, WatchEvent};
use super::process_discovery::ProcessDiscoveryService;
use super::process_utils::stop_process_gracefully;
use super::script_resolver::{ScriptResolver, ScriptWatchMap};
use super::spawn::spawn_managed_process_with_logging;
use super::status::OrchestratorStatus;
use crate::mqtt::client::MqttClient;
use crate::sync::encode_path;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
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
    /// Path to the log file for this process
    pub log_file: Option<String>,
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
    /// Path to the status file (scoped to config)
    status_file_path: PathBuf,
    /// Service for discovering __processes.json files
    discovery_service: ProcessDiscoveryService,
    /// Resolver for script UUIDs (evaluate processes)
    script_resolver: ScriptResolver,
    /// MQTT client for watching documents (required for run_with_recursive_watch)
    mqtt_client: Option<Arc<MqttClient>>,
    /// Workspace name for MQTT topics
    mqtt_workspace: Option<String>,
}

impl DiscoveredProcessManager {
    /// Create a new discovered process manager.
    pub fn new(mqtt_broker: String, server_url: String, status_file_path: PathBuf) -> Self {
        let client = Client::new();
        let discovery_service = ProcessDiscoveryService::new(client.clone(), server_url.clone());
        let script_resolver = ScriptResolver::new(client, server_url.clone());

        Self {
            mqtt_broker,
            server_url,
            processes: HashMap::new(),
            initial_backoff_ms: 500,
            max_backoff_ms: 10_000,
            reset_after_secs: 30,
            status_file_path,
            discovery_service,
            script_resolver,
            mqtt_client: None,
            mqtt_workspace: None,
        }
    }

    /// Set the MQTT client for document watching.
    ///
    /// Required for `run_with_recursive_watch()` to work.
    pub fn with_mqtt_client(mut self, client: Arc<MqttClient>, workspace: String) -> Self {
        self.mqtt_client = Some(client);
        self.mqtt_workspace = Some(workspace);
        self
    }

    /// Set the MQTT client for document watching (mutable reference version).
    pub fn set_mqtt_client(&mut self, client: Arc<MqttClient>, workspace: String) {
        self.mqtt_client = Some(client);
        self.mqtt_workspace = Some(workspace);
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
                process.log_file.clone(),
            ));
        }

        // Merge with existing status (preserving base processes) and write
        if let Err(e) = status.merge_and_write(&self.status_file_path, false) {
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
            log_file: None,
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
            cmd.args([
                "--sandbox",
                "--name",
                name,
                "--path",
                document_path,
                "--exec",
                exec_cmd,
            ]);

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

            // Loader script URL - injects 'commonplace' global then imports user script
            let loader_url = format!("{}/sdk/loader.ts", server);
            // Import map URL for simplified imports within the SDK
            let import_map_url = format!("{}/sdk/import-map.json", server);

            let mut cmd = Command::new("deno");
            cmd.arg("run")
                // Force re-fetch script to ensure we run latest version after changes
                .arg("--reload")
                // Import map for SDK internal imports
                .arg(format!("--import-map={}", import_map_url))
                // Allow npm registry for package downloads, plus server and broker
                .arg(format!(
                    "--allow-net={},{},registry.npmjs.org,cdn.jsdelivr.net,esm.sh",
                    server_host, broker
                ))
                // Allow all env for npm package compatibility (readable-stream, etc.)
                .arg("--allow-env")
                // Run the loader script (which imports the user script via env var)
                .arg(&loader_url);

            // Set environment variables for the SDK
            cmd.env("COMMONPLACE_SERVER", &server);
            cmd.env("COMMONPLACE_BROKER", &broker);
            cmd.env("COMMONPLACE_CLIENT_ID", &client_id);
            // Pass the user script URL to the loader
            cmd.env("COMMONPLACE_SCRIPT", &script_url);

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
                "[discovery] Starting evaluate process '{}' (script: {}, url: {}, loader: {})",
                name,
                script,
                script_url,
                loader_url
            );
            cmd
        } else if let Some(ref listen_path) = config.log_listener {
            // log-listener: construct commonplace-sync --sandbox --log-listener "<path>"
            // This process subscribes to stdout/stderr events at the given path
            // and writes them to the file it owns
            let sync_path = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.join("commonplace-sync")))
                .unwrap_or_else(|| PathBuf::from("commonplace-sync"));

            let mut cmd = Command::new(&sync_path);
            cmd.args([
                "--sandbox",
                "--name",
                name,
                "--path",
                document_path,
                "--log-listener",
                listen_path,
            ]);

            // Set cwd if specified
            if let Some(ref cwd) = config.cwd {
                cmd.current_dir(cwd);
            }

            tracing::info!(
                "[discovery] Starting log-listener process '{}' (path: {}, listens: {})",
                name,
                document_path,
                listen_path
            );
            cmd
        } else {
            return Err(format!(
                "Process '{}' must have 'command', 'sandbox-exec', 'evaluate', or 'log-listener'",
                name
            ));
        };

        // Set environment variables for both modes
        cmd.env("COMMONPLACE_PATH", document_path);
        cmd.env("COMMONPLACE_MQTT", &mqtt_broker);
        cmd.env("COMMONPLACE_SERVER", &self.server_url);
        cmd.env("COMMONPLACE_INITIAL_SYNC", "server");

        let result = spawn_managed_process_with_logging(cmd, name)?;
        process.handle = Some(result.child);
        process.log_file = result.log_file.map(|p| p.to_string_lossy().to_string());
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
        // Build a map of running processes from this source for reconciliation
        let running_from_source: HashMap<String, DiscoveredProcess> = self
            .processes
            .iter()
            .filter(|(_, p)| p.source_path == base_path)
            .map(|(name, p)| (name.clone(), p.config.clone()))
            .collect();

        // Compute what actions are needed
        let result = ConfigReconciler::reconcile(&running_from_source, config, Some(base_path));

        // Apply the reconciliation result
        self.apply_reconcile_result(result, base_path).await
    }

    /// Apply the result of a reconciliation computation.
    ///
    /// This method performs the actual mutations: stopping, starting, and restarting processes.
    async fn apply_reconcile_result(
        &mut self,
        result: ReconcileResult,
        base_path: &str,
    ) -> Result<(), String> {
        // Stop processes that were removed
        for name in &result.to_stop {
            tracing::info!(
                "[discovery] Removing process '{}' (no longer in config at {})",
                name,
                base_path
            );
            self.remove_process(name).await;
        }

        // Restart processes with changed configuration
        for (name, new_config) in &result.to_restart {
            tracing::info!(
                "[discovery] Restarting process '{}' (configuration changed)",
                name
            );

            // Stop the old process
            self.remove_process(name).await;

            // Add with new config
            let document_path = ConfigReconciler::compute_document_path(new_config, base_path);
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

        // Start new processes
        for (name, new_config) in &result.to_start {
            let document_path = ConfigReconciler::compute_document_path(new_config, base_path);

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

    /// Resolve script paths for all evaluate processes and build a watch map.
    ///
    /// Returns a map of script_uuid -> process_name for all evaluate processes
    /// that have scripts that can be resolved.
    async fn resolve_script_watches(&self, fs_root_id: &str) -> ScriptWatchMap {
        // Build iterator of (process_name, document_path, script_name) for evaluate processes
        let evaluate_processes: Vec<(&str, &str, &str)> = self
            .processes
            .iter()
            .filter_map(|(name, process)| {
                process.config.evaluate.as_ref().map(|script| {
                    (
                        name.as_str(),
                        process.document_path.as_str(),
                        script.as_str(),
                    )
                })
            })
            .collect();

        self.script_resolver
            .build_script_watches(fs_root_id, evaluate_processes)
            .await
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

    /// Run with recursive discovery of all __processes.json files.
    ///
    /// This discovers all __processes.json files in the filesystem tree starting from
    /// the given fs-root, and watches each one for changes. Each __processes.json
    /// controls processes that sync at its containing directory.
    ///
    /// Watches ALL schema documents (directory node_ids) for changes, not just fs-root.
    ///
    /// Requires MQTT client to be configured via `with_mqtt_client()` or `set_mqtt_client()`.
    pub async fn run_with_recursive_watch(
        &mut self,
        client: &Client,
        fs_root_id: &str,
    ) -> Result<(), String> {
        // Require MQTT client to be configured
        let mqtt_client = self.mqtt_client.clone().ok_or_else(|| {
            "MQTT client not configured. Call with_mqtt_client() or set_mqtt_client() before run_with_recursive_watch()".to_string()
        })?;
        let workspace = self.mqtt_workspace.clone().ok_or_else(|| {
            "MQTT workspace not configured. Call with_mqtt_client() or set_mqtt_client() before run_with_recursive_watch()".to_string()
        })?;

        tracing::info!(
            "[discovery] Starting recursive discovery from fs-root: {} (using MQTT)",
            fs_root_id
        );

        // Discover all __processes.json files and schema node_ids
        let discovery = self
            .discovery_service
            .discover_all(fs_root_id)
            .await
            .map_err(|e| e.to_string())?;

        if discovery.processes_json_files.is_empty() {
            tracing::warn!("[discovery] No __processes.json files found in filesystem tree");
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
        let mut script_watches: ScriptWatchMap = self.resolve_script_watches(fs_root_id).await;

        // Create MQTT document watcher
        let watcher = MqttDocumentWatcher::new(mqtt_client.clone(), workspace.clone());

        // Build initial watch set: schemas + __processes.json files + scripts
        let initial_watch_ids: HashSet<String> = {
            let mut ids = current_schema_ids.clone();
            for node_id in watched.keys() {
                ids.insert(node_id.clone());
            }
            for script_uuid in script_watches.keys() {
                ids.insert(script_uuid.clone());
            }
            ids
        };

        // Set up initial subscriptions
        if let Err(e) = watcher.update_watches(initial_watch_ids).await {
            tracing::error!("[discovery] Failed to set up initial MQTT watches: {}", e);
        }

        // Get event receiver
        let mut event_rx = watcher.events();

        // Spawn the watcher's event loop as a background task
        let watcher = Arc::new(watcher);
        let watcher_handle = {
            let watcher = watcher.clone();
            tokio::spawn(async move {
                if let Err(e) = watcher.run().await {
                    tracing::error!("[discovery] MQTT watcher error: {}", e);
                }
            })
        };

        // Monitor interval for checking process health
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(500));

        // Re-discovery interval (check for new/removed __processes.json files)
        let mut discovery_interval = tokio::time::interval(Duration::from_secs(30));

        // Flag to trigger immediate discovery (set when schema changes)
        let mut need_immediate_discovery = false;

        loop {
            // Check if immediate discovery was requested (e.g., schema change)
            if need_immediate_discovery {
                need_immediate_discovery = false;
                tracing::info!("[discovery] Running immediate discovery after schema change");
                match self.discovery_service.discover_all(fs_root_id).await {
                    Ok(new_discovery) => {
                        let mut watches_changed = false;

                        // Check for new or previously-failed __processes.json files
                        for (base_path, node_id) in &new_discovery.processes_json_files {
                            if !watched.contains_key(node_id) {
                                tracing::info!(
                                    "[discovery] Trying __processes.json at {}",
                                    base_path
                                );
                                let url = format!("{}/docs/{}/head", self.server_url, node_id);
                                match self.fetch_and_reconcile(client, &url, base_path).await {
                                    Ok(_) => {
                                        tracing::info!(
                                            "[discovery] Loaded processes from {}/__processes.json",
                                            base_path
                                        );
                                        watched.insert(node_id.clone(), base_path.clone());
                                        watches_changed = true;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "[discovery] Failed to load {}: {} (will retry)",
                                            base_path,
                                            e
                                        );
                                    }
                                }
                            }
                        }

                        // Check for removed __processes.json files
                        let new_ids: HashSet<_> = new_discovery
                            .processes_json_files
                            .iter()
                            .map(|(_, id)| id.clone())
                            .collect();
                        let removed_pj: Vec<_> = watched
                            .keys()
                            .filter(|id| !new_ids.contains(*id))
                            .cloned()
                            .collect();
                        for node_id in removed_pj {
                            if let Some(base_path) = watched.remove(&node_id) {
                                tracing::info!(
                                    "[discovery] __processes.json removed at {}",
                                    base_path
                                );
                                let to_remove: Vec<_> = self
                                    .processes
                                    .iter()
                                    .filter(|(_, p)| p.document_path.starts_with(&base_path))
                                    .map(|(n, _)| n.clone())
                                    .collect();
                                for name in to_remove {
                                    self.remove_process(&name).await;
                                }
                                watches_changed = true;
                            }
                        }

                        // Check for new schema node_ids (new directories added)
                        let new_schema_ids: HashSet<String> =
                            new_discovery.schema_node_ids.iter().cloned().collect();
                        if new_schema_ids != current_schema_ids {
                            let added: Vec<_> =
                                new_schema_ids.difference(&current_schema_ids).collect();
                            let removed: Vec<_> =
                                current_schema_ids.difference(&new_schema_ids).collect();
                            tracing::info!(
                                "[discovery] Schema set changed: +{} -{} directories",
                                added.len(),
                                removed.len()
                            );
                            current_schema_ids = new_schema_ids;
                            watches_changed = true;
                        }

                        // Update script watches
                        let new_script_watches = self.resolve_script_watches(fs_root_id).await;
                        if new_script_watches != script_watches {
                            script_watches = new_script_watches;
                            watches_changed = true;
                        }

                        // Update MQTT subscriptions if watch set changed
                        if watches_changed {
                            let mut new_watch_ids: HashSet<String> = current_schema_ids.clone();
                            for node_id in watched.keys() {
                                new_watch_ids.insert(node_id.clone());
                            }
                            for script_uuid in script_watches.keys() {
                                new_watch_ids.insert(script_uuid.clone());
                            }
                            if let Err(e) = watcher.update_watches(new_watch_ids).await {
                                tracing::error!("[discovery] Failed to update MQTT watches: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[discovery] Immediate re-discovery failed: {}", e);
                    }
                }
            }

            tokio::select! {
                // Periodic health check for process restarts
                _ = monitor_interval.tick() => {
                    self.check_and_restart().await;
                }

                // Periodic re-discovery of __processes.json files
                _ = discovery_interval.tick() => {
                    match self.discovery_service.discover_all(fs_root_id).await {
                        Ok(new_discovery) => {
                            let mut watches_changed = false;

                            // Check for new or previously-failed __processes.json files
                            for (base_path, node_id) in &new_discovery.processes_json_files {
                                if !watched.contains_key(node_id) {
                                    tracing::info!("[discovery] Trying __processes.json at {}", base_path);
                                    let url = format!("{}/docs/{}/head", self.server_url, node_id);
                                    match self.fetch_and_reconcile(client, &url, base_path).await {
                                        Ok(_) => {
                                            tracing::info!("[discovery] Loaded processes from {}/__processes.json", base_path);
                                            watched.insert(node_id.clone(), base_path.clone());
                                            watches_changed = true;
                                        }
                                        Err(e) => {
                                            tracing::warn!("[discovery] Failed to load {}: {} (will retry)", base_path, e);
                                        }
                                    }
                                }
                            }

                            // Re-read already-watched __processes.json files to detect content changes.
                            // MQTT should handle this in real-time via DocumentChanged events, but
                            // as a fallback, we re-read on each periodic discovery cycle.
                            for (base_path, node_id) in &new_discovery.processes_json_files {
                                if watched.contains_key(node_id) {
                                    let url = format!("{}/docs/{}/head", self.server_url, node_id);
                                    if let Err(e) = self.fetch_and_reconcile(client, &url, base_path).await {
                                        tracing::debug!("[discovery] Failed to re-read {}/__processes.json: {}", base_path, e);
                                    }
                                }
                            }

                            // Check for removed __processes.json files
                            let new_ids: HashSet<_> = new_discovery.processes_json_files.iter().map(|(_, id)| id.clone()).collect();
                            let removed_pj: Vec<_> = watched.keys()
                                .filter(|id| !new_ids.contains(*id))
                                .cloned()
                                .collect();
                            for node_id in removed_pj {
                                if let Some(base_path) = watched.remove(&node_id) {
                                    tracing::info!("[discovery] __processes.json removed at {}", base_path);
                                    let to_remove: Vec<_> = self.processes.iter()
                                        .filter(|(_, p)| p.document_path.starts_with(&base_path))
                                        .map(|(n, _)| n.clone())
                                        .collect();
                                    for name in to_remove {
                                        self.remove_process(&name).await;
                                    }
                                    watches_changed = true;
                                }
                            }

                            // Check for new schema node_ids (new directories added)
                            let new_schema_ids: HashSet<String> = new_discovery.schema_node_ids.iter().cloned().collect();
                            if new_schema_ids != current_schema_ids {
                                let added: Vec<_> = new_schema_ids.difference(&current_schema_ids).collect();
                                let removed: Vec<_> = current_schema_ids.difference(&new_schema_ids).collect();
                                tracing::info!(
                                    "[discovery] Schema set changed: +{} -{} directories",
                                    added.len(),
                                    removed.len()
                                );
                                current_schema_ids = new_schema_ids;
                                watches_changed = true;
                            }

                            // Update script watches
                            let new_script_watches = self.resolve_script_watches(fs_root_id).await;
                            if new_script_watches != script_watches {
                                script_watches = new_script_watches;
                                watches_changed = true;
                            }

                            // Update MQTT subscriptions if watch set changed
                            if watches_changed {
                                let mut new_watch_ids: HashSet<String> = current_schema_ids.clone();
                                for node_id in watched.keys() {
                                    new_watch_ids.insert(node_id.clone());
                                }
                                for script_uuid in script_watches.keys() {
                                    new_watch_ids.insert(script_uuid.clone());
                                }
                                if let Err(e) = watcher.update_watches(new_watch_ids).await {
                                    tracing::error!("[discovery] Failed to update MQTT watches: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[discovery] Re-discovery failed: {}", e);
                        }
                    }
                }

                // MQTT document change event
                event = event_rx.recv() => {
                    match event {
                        Ok(WatchEvent::DocumentChanged(doc_id)) => {
                            // Check if this is a __processes.json file we're watching
                            if let Some(base_path) = watched.get(&doc_id) {
                                tracing::info!(
                                    "[discovery] __processes.json changed at {}, reconciling...",
                                    base_path
                                );
                                let url = format!("{}/docs/{}/head", self.server_url, doc_id);
                                let base_path = base_path.clone();
                                if let Err(e) = self.fetch_and_reconcile(client, &url, &base_path).await {
                                    tracing::error!(
                                        "[discovery] Failed to reconcile {}/__processes.json: {}",
                                        base_path, e
                                    );
                                }
                                // Update script watches after reconciling
                                let new_script_watches = self.resolve_script_watches(fs_root_id).await;
                                if new_script_watches != script_watches {
                                    script_watches = new_script_watches;
                                    // Update MQTT subscriptions with new script watches
                                    let mut new_watch_ids: HashSet<String> = current_schema_ids.clone();
                                    for node_id in watched.keys() {
                                        new_watch_ids.insert(node_id.clone());
                                    }
                                    for script_uuid in script_watches.keys() {
                                        new_watch_ids.insert(script_uuid.clone());
                                    }
                                    if let Err(e) = watcher.update_watches(new_watch_ids).await {
                                        tracing::error!("[discovery] Failed to update MQTT watches: {}", e);
                                    }
                                }
                            } else if let Some(process_names) = script_watches.get(&doc_id) {
                                // Script file change - restart associated processes
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
                            } else if current_schema_ids.contains(&doc_id) {
                                // Schema change - trigger immediate re-discovery
                                tracing::info!(
                                    "[discovery] Schema {} changed, triggering immediate re-discovery",
                                    doc_id
                                );
                                need_immediate_discovery = true;
                            } else {
                                // Unknown document change - might be a new schema we don't know about yet
                                tracing::debug!(
                                    "[discovery] Unknown document {} changed (not in current watches)",
                                    doc_id
                                );
                            }
                        }
                        Ok(WatchEvent::Disconnected) => {
                            tracing::warn!("[discovery] MQTT connection lost");
                        }
                        Ok(WatchEvent::Reconnected) => {
                            tracing::info!("[discovery] MQTT connection restored");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("[discovery] MQTT events lagged by {} messages, triggering re-discovery", n);
                            need_immediate_discovery = true;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::error!("[discovery] MQTT event channel closed");
                            break;
                        }
                    }
                }
            }
        }

        // Clean up
        watcher_handle.abort();
        Ok(())
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
        if let Err(e) = OrchestratorStatus::remove(&self.status_file_path) {
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
            PathBuf::from("/tmp/test-status.json"),
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
            PathBuf::from("/tmp/test-status.json"),
        );

        let config = DiscoveredProcess {
            comment: None,
            command: Some(CommandSpec::Simple("python test.py".to_string())),
            sandbox_exec: None,
            path: None,
            owns: Some("test.json".to_string()),
            cwd: Some(PathBuf::from("/tmp")),
            evaluate: None,
            log_listener: None,
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
            PathBuf::from("/tmp/test-status.json"),
        );

        let config = DiscoveredProcess {
            comment: None,
            command: Some(CommandSpec::Simple("sync --sandbox".to_string())),
            sandbox_exec: None,
            path: None,
            owns: None,
            cwd: Some(PathBuf::from("/tmp")),
            evaluate: None,
            log_listener: None,
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
            PathBuf::from("/tmp/test-status.json"),
        );

        assert_eq!(manager.server_url(), "http://localhost:5199");
    }

    #[test]
    fn test_add_evaluate_process() {
        let mut manager = DiscoveredProcessManager::new(
            "localhost:1883".to_string(),
            "http://localhost:5199".to_string(),
            PathBuf::from("/tmp/test-status.json"),
        );

        let config = DiscoveredProcess {
            comment: None,
            command: None,
            sandbox_exec: None,
            path: None,
            owns: Some("output.json".to_string()),
            cwd: None,
            evaluate: Some("script.js".to_string()),
            log_listener: None,
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
