use super::{OrchestratorConfig, ProcessConfig};
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Instant;
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
}
