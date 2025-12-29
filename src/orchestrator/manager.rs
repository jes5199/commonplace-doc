use super::{OrchestratorConfig, ProcessConfig, RestartMode};
use std::collections::HashMap;
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

        // Add mqtt-broker arg if the process likely needs it
        if config.command.contains("commonplace") {
            cmd.arg("--mqtt-broker");
            cmd.arg(&mqtt_broker);
        }

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

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

        Ok(())
    }

    pub async fn start_all(&mut self) -> Result<(), String> {
        let order = self.config.startup_order()?;

        for name in order {
            if self.disabled.contains(&name) {
                continue;
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
                            unsafe {
                                libc::kill(pid as i32, libc::SIGTERM);
                            }
                        }
                    }

                    let timeout =
                        tokio::time::timeout(Duration::from_secs(5), child.wait()).await;

                    match timeout {
                        Ok(Ok(_)) => {
                            tracing::info!("[orchestrator] '{}' stopped gracefully", name);
                        }
                        _ => {
                            tracing::warn!(
                                "[orchestrator] '{}' didn't stop gracefully, killing",
                                name
                            );
                            let _ = child.kill().await;
                        }
                    }

                    process.handle = None;
                    process.state = ProcessState::Stopped;
                }
            }
        }

        tracing::info!("[orchestrator] Shutdown complete");
    }
}
