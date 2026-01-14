use super::process_utils::stop_process_gracefully;
use super::spawn::spawn_managed_process;
use super::status::OrchestratorStatus;
use super::{OrchestratorConfig, ProcessConfig, RestartMode};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

#[derive(Debug)]
pub struct ManagedProcess {
    pub config: ProcessConfig,
    pub handle: Option<Child>,
    pub state: ProcessState,
    pub consecutive_failures: u32,
    pub last_start: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessState {
    Stopped,
    Starting,
    Running,
    Failed,
}

pub struct ProcessManager {
    config: OrchestratorConfig,
    processes: HashMap<String, ManagedProcess>,
    mqtt_broker_override: Option<String>,
    disabled: Vec<String>,
    workspace: String,
}

impl ProcessManager {
    pub fn new(
        config: OrchestratorConfig,
        mqtt_broker_override: Option<String>,
        disabled: Vec<String>,
    ) -> Self {
        let workspace = config.workspace.clone();
        let processes = config
            .processes
            .iter()
            .map(|(name, cfg)| {
                (
                    name.clone(),
                    ManagedProcess {
                        config: cfg.clone(),
                        handle: None,
                        state: ProcessState::Stopped,
                        consecutive_failures: 0,
                        last_start: None,
                    },
                )
            })
            .collect();

        Self {
            config,
            processes,
            mqtt_broker_override,
            disabled,
            workspace,
        }
    }

    pub fn mqtt_broker(&self) -> &str {
        self.mqtt_broker_override
            .as_deref()
            .unwrap_or(&self.config.mqtt_broker)
    }

    /// Write current process status to the status file.
    pub fn write_status(&self) {
        let mut status = OrchestratorStatus::new();

        for (name, process) in &self.processes {
            let pid = process.handle.as_ref().and_then(|h| h.id());

            let state = match process.state {
                ProcessState::Stopped => "Stopped",
                ProcessState::Starting => "Starting",
                ProcessState::Running => "Running",
                ProcessState::Failed => "Failed",
            };

            status.processes.push(super::status::build_process_status(
                name.clone(),
                pid,
                process.config.cwd.as_deref(),
                state,
                None,
                None,
            ));
        }

        // Merge with existing status (preserving discovered processes) and write
        if let Err(e) = status.merge_and_write(true) {
            tracing::warn!("[orchestrator] Failed to write status file: {}", e);
        }
    }

    pub async fn spawn_process(&mut self, name: &str) -> Result<(), String> {
        if self.disabled.contains(&name.to_string()) {
            tracing::info!("[orchestrator] Skipping disabled process: {}", name);
            return Ok(());
        }

        // Clone values before getting mutable reference to process
        let mqtt_broker = self.mqtt_broker().to_string();
        let workspace = self.workspace.clone();

        let process = self
            .processes
            .get_mut(name)
            .ok_or_else(|| format!("Unknown process: {}", name))?;

        let config = &process.config;

        // Build command with args
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args);

        // Inject --database for server process (from top-level config)
        if name == "server" {
            cmd.arg("--database");
            cmd.arg(&self.config.database);
        }

        // Set working directory if specified
        if let Some(ref cwd) = config.cwd {
            cmd.current_dir(cwd);
        }

        // Set MQTT_WORKSPACE environment variable for child processes
        cmd.env("MQTT_WORKSPACE", &workspace);

        // Add mqtt-broker arg if the process likely needs it and doesn't already have it
        let already_has_mqtt_broker = config.args.iter().any(|arg| arg == "--mqtt-broker");
        if config.command.contains("commonplace") && !already_has_mqtt_broker {
            cmd.arg("--mqtt-broker");
            cmd.arg(&mqtt_broker);
        }

        tracing::info!("[orchestrator] Starting process: {}", name);

        let child = spawn_managed_process(cmd, name)?;
        process.handle = Some(child);
        process.state = ProcessState::Running;
        process.last_start = Some(Instant::now());

        // Update status file
        self.write_status();

        Ok(())
    }

    pub async fn start_all(&mut self) -> Result<(), String> {
        let order = self.config.startup_order()?;

        // Track effectively disabled processes (explicitly disabled + those with disabled deps)
        let mut effectively_disabled: Vec<String> = self.disabled.clone();

        for name in order {
            if self.disabled.contains(&name) {
                tracing::info!("[orchestrator] Skipping disabled process: {}", name);
                continue;
            }

            // Check if any dependency is disabled
            if let Some(proc_config) = self.config.processes.get(&name) {
                let has_disabled_dep = proc_config
                    .depends_on
                    .iter()
                    .any(|dep| effectively_disabled.contains(dep));

                if has_disabled_dep {
                    tracing::warn!(
                        "[orchestrator] Skipping '{}' because a dependency is disabled",
                        name
                    );
                    effectively_disabled.push(name.clone());
                    continue;
                }
            }

            self.spawn_process(&name).await?;
            // Small delay to let process initialize
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), String> {
        // Start all processes
        self.start_all().await?;

        // Monitor loop
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;

            let names: Vec<String> = self.processes.keys().cloned().collect();
            for name in names {
                if self.disabled.contains(&name) {
                    continue;
                }

                let should_restart = {
                    let process = match self.processes.get_mut(&name) {
                        Some(p) => p,
                        None => continue,
                    };

                    if let Some(ref mut child) = process.handle {
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                let success = status.success();
                                tracing::warn!(
                                    "[orchestrator] Process '{}' exited with status: {}",
                                    name,
                                    status
                                );

                                process.handle = None;

                                match process.config.restart.policy {
                                    RestartMode::Always => true,
                                    RestartMode::OnFailure => !success,
                                    RestartMode::Never => {
                                        process.state = ProcessState::Stopped;
                                        false
                                    }
                                }
                            }
                            Ok(None) => {
                                // Still running - reset failure count if running long enough
                                if let Some(start) = process.last_start {
                                    if start.elapsed() > Duration::from_secs(30)
                                        && process.consecutive_failures > 0
                                    {
                                        process.consecutive_failures = 0;
                                    }
                                }
                                false
                            }
                            Err(e) => {
                                tracing::error!(
                                    "[orchestrator] Error checking process '{}': {}",
                                    name,
                                    e
                                );
                                false
                            }
                        }
                    } else {
                        false
                    }
                };

                if should_restart {
                    let process = self.processes.get_mut(&name).unwrap();
                    process.consecutive_failures += 1;
                    process.state = ProcessState::Failed;

                    let backoff = std::cmp::min(
                        process.config.restart.backoff_ms
                            * 2u64.pow(process.consecutive_failures.saturating_sub(1)),
                        process.config.restart.max_backoff_ms,
                    );

                    tracing::info!(
                        "[orchestrator] Restarting '{}' in {}ms (attempt {})",
                        name,
                        backoff,
                        process.consecutive_failures
                    );

                    tokio::time::sleep(Duration::from_millis(backoff)).await;

                    if let Err(e) = self.spawn_process(&name).await {
                        tracing::error!("[orchestrator] Failed to restart '{}': {}", name, e);
                    }
                }
            }
        }
    }

    /// Check all processes and restart any that have exited (if policy allows)
    pub async fn check_and_restart(&mut self) {
        let names: Vec<String> = self.processes.keys().cloned().collect();
        for name in names {
            if self.disabled.contains(&name) {
                continue;
            }

            let should_restart = {
                let process = match self.processes.get_mut(&name) {
                    Some(p) => p,
                    None => continue,
                };

                if let Some(ref mut child) = process.handle {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            let success = status.success();
                            tracing::warn!(
                                "[orchestrator] Process '{}' exited with status: {}",
                                name,
                                status
                            );

                            process.handle = None;

                            match process.config.restart.policy {
                                RestartMode::Always => true,
                                RestartMode::OnFailure => !success,
                                RestartMode::Never => {
                                    process.state = ProcessState::Stopped;
                                    false
                                }
                            }
                        }
                        Ok(None) => {
                            // Still running - reset failure count if running long enough
                            if let Some(start) = process.last_start {
                                if start.elapsed() > Duration::from_secs(30)
                                    && process.consecutive_failures > 0
                                {
                                    process.consecutive_failures = 0;
                                }
                            }
                            false
                        }
                        Err(e) => {
                            tracing::error!(
                                "[orchestrator] Error checking process '{}': {}",
                                name,
                                e
                            );
                            false
                        }
                    }
                } else {
                    false
                }
            };

            if should_restart {
                let (backoff, failures) = {
                    let process = self.processes.get_mut(&name).unwrap();
                    process.consecutive_failures += 1;
                    process.state = ProcessState::Failed;

                    let backoff = std::cmp::min(
                        process.config.restart.backoff_ms
                            * 2u64.pow(process.consecutive_failures.saturating_sub(1)),
                        process.config.restart.max_backoff_ms,
                    );
                    (backoff, process.consecutive_failures)
                };

                // Update status to reflect the failed state
                self.write_status();

                tracing::info!(
                    "[orchestrator] Restarting '{}' in {}ms (attempt {})",
                    name,
                    backoff,
                    failures
                );

                tokio::time::sleep(Duration::from_millis(backoff)).await;

                if let Err(e) = self.spawn_process(&name).await {
                    tracing::error!("[orchestrator] Failed to restart '{}': {}", name, e);
                }
            }
        }
    }

    /// Stop a specific process by name
    pub async fn stop_process(&mut self, name: &str) {
        if let Some(process) = self.processes.get_mut(name) {
            if let Some(ref mut child) = process.handle {
                tracing::info!("[orchestrator] Stopping '{}'", name);

                stop_process_gracefully(child, name, "[orchestrator]", Duration::from_secs(5))
                    .await;

                process.handle = None;
                process.state = ProcessState::Stopped;
            }
        }
        self.write_status();
    }

    /// Reload configuration, stopping removed processes and starting new ones
    pub async fn reload_config(&mut self, new_config: OrchestratorConfig) {
        tracing::info!("[orchestrator] Reloading configuration...");

        let old_processes: std::collections::HashSet<_> =
            self.config.processes.keys().cloned().collect();
        let new_processes: std::collections::HashSet<_> =
            new_config.processes.keys().cloned().collect();

        // Find removed processes
        let removed: Vec<_> = old_processes.difference(&new_processes).cloned().collect();
        // Find added processes
        let added: Vec<_> = new_processes.difference(&old_processes).cloned().collect();
        // Find processes with changed config
        let mut changed: Vec<String> = Vec::new();
        for name in old_processes.intersection(&new_processes) {
            let old_cfg = &self.config.processes[name];
            let new_cfg = &new_config.processes[name];
            // Compare key fields
            if old_cfg.command != new_cfg.command
                || old_cfg.args != new_cfg.args
                || old_cfg.cwd != new_cfg.cwd
            {
                changed.push(name.clone());
            }
        }

        // Stop removed processes
        for name in &removed {
            tracing::info!("[orchestrator] Config reload: removing process '{}'", name);
            self.stop_process(name).await;
            self.processes.remove(name);
        }

        // Stop and remove changed processes (will be re-added with new config)
        for name in &changed {
            tracing::info!(
                "[orchestrator] Config reload: restarting changed process '{}'",
                name
            );
            self.stop_process(name).await;
            self.processes.remove(name);
        }

        // Update config and workspace
        self.workspace = new_config.workspace.clone();
        self.config = new_config;

        // Update config for unchanged processes (fields other than command/args/cwd may have changed)
        for name in old_processes.intersection(&new_processes) {
            if !changed.contains(name) {
                if let (Some(cfg), Some(proc)) = (
                    self.config.processes.get(name),
                    self.processes.get_mut(name),
                ) {
                    proc.config = cfg.clone();
                }
            }
        }

        // Add new and changed processes to the map
        for name in added.iter().chain(changed.iter()) {
            if let Some(cfg) = self.config.processes.get(name) {
                self.processes.insert(
                    name.clone(),
                    ManagedProcess {
                        config: cfg.clone(),
                        handle: None,
                        state: ProcessState::Stopped,
                        consecutive_failures: 0,
                        last_start: None,
                    },
                );
            }
        }

        // Start added and changed processes in dependency order
        let to_start: std::collections::HashSet<_> =
            added.iter().chain(changed.iter()).cloned().collect();
        let startup_order = self.config.startup_order().unwrap_or_default();
        for name in startup_order {
            if !to_start.contains(&name) {
                continue;
            }
            if self.disabled.contains(&name) {
                continue;
            }
            let action = if added.contains(&name) {
                "starting new"
            } else {
                "restarting changed"
            };
            tracing::info!(
                "[orchestrator] Config reload: {} process '{}'",
                action,
                name
            );
            if let Err(e) = self.spawn_process(&name).await {
                tracing::error!("[orchestrator] Failed to start '{}': {}", name, e);
            }
        }

        tracing::info!(
            "[orchestrator] Config reload complete: {} removed, {} added, {} changed",
            removed.len(),
            added.len(),
            changed.len()
        );
    }

    pub async fn shutdown(&mut self) {
        tracing::info!("[orchestrator] Shutting down...");

        let order = self.config.startup_order().unwrap_or_default();

        for name in order.iter().rev() {
            if let Some(process) = self.processes.get_mut(name) {
                if let Some(ref mut child) = process.handle {
                    tracing::info!("[orchestrator] Stopping '{}'", name);

                    // Give processes 10 seconds to shutdown gracefully
                    // This allows sync to properly terminate its exec child
                    stop_process_gracefully(child, name, "[orchestrator]", Duration::from_secs(10))
                        .await;

                    process.handle = None;
                    process.state = ProcessState::Stopped;
                }
            }
        }

        // Remove status file on shutdown
        if let Err(e) = OrchestratorStatus::remove() {
            tracing::warn!("[orchestrator] Failed to remove status file: {}", e);
        }

        tracing::info!("[orchestrator] Shutdown complete");
    }
}
