use super::status::{OrchestratorStatus, ProcessStatus};
use super::{OrchestratorConfig, ProcessConfig, RestartMode};
use std::collections::HashMap;
#[cfg(unix)]
#[allow(unused_imports)]
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
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
}

impl ProcessManager {
    pub fn new(
        config: OrchestratorConfig,
        mqtt_broker_override: Option<String>,
        disabled: Vec<String>,
    ) -> Self {
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

            // Try to get CWD from config first, otherwise read from /proc/<pid>/cwd
            // For sandbox processes, this finds the deepest child's CWD
            let cwd = process
                .config
                .cwd
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .or_else(|| pid.and_then(super::status::get_process_cwd));

            let state = match process.state {
                ProcessState::Stopped => "Stopped",
                ProcessState::Starting => "Starting",
                ProcessState::Running => "Running",
                ProcessState::Failed => "Failed",
            };

            status.processes.push(ProcessStatus {
                name: name.clone(),
                pid,
                cwd,
                state: state.to_string(),
                document_path: None,
                source_path: None,
            });
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

        // Clone the broker string before getting mutable reference to process
        let mqtt_broker = self.mqtt_broker().to_string();

        let process = self
            .processes
            .get_mut(name)
            .ok_or_else(|| format!("Unknown process: {}", name))?;

        let config = &process.config;

        // Build command with args
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args);

        // Set working directory if specified
        if let Some(ref cwd) = config.cwd {
            cmd.current_dir(cwd);
        }

        // Add mqtt-broker arg if the process likely needs it
        if config.command.contains("commonplace") {
            cmd.arg("--mqtt-broker");
            cmd.arg(&mqtt_broker);
        }

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

        tracing::info!("[orchestrator] Starting process: {}", name);

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
            let name = name_clone.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[{}] {}", name, line);
                }
            });
        }

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

    pub async fn shutdown(&mut self) {
        tracing::info!("[orchestrator] Shutting down...");

        let order = self.config.startup_order().unwrap_or_default();

        for name in order.iter().rev() {
            if let Some(process) = self.processes.get_mut(name) {
                if let Some(ref mut child) = process.handle {
                    tracing::info!("[orchestrator] Stopping '{}'", name);

                    #[cfg(unix)]
                    {
                        if let Some(pid) = child.id() {
                            // Kill the entire process group (negative pid)
                            // This ensures all descendants are terminated
                            tracing::debug!(
                                "[orchestrator] Sending SIGTERM to process group {}",
                                pid
                            );
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
                            tracing::info!("[orchestrator] '{}' stopped gracefully", name);
                        }
                        _ => {
                            tracing::warn!(
                                "[orchestrator] '{}' didn't stop gracefully after 10s, force killing process group",
                                name
                            );
                            // Force kill the entire process group
                            #[cfg(unix)]
                            if let Some(pid) = child.id() {
                                tracing::debug!(
                                    "[orchestrator] Sending SIGKILL to process group {}",
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
