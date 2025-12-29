# Orchestrator Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a process supervisor binary that starts and manages commonplace-store and commonplace-http with restart-on-failure.

**Architecture:** JSON config defines processes with dependencies and restart policies. Orchestrator starts processes in topological order, captures stdout/stderr with prefixes, and restarts failed processes with exponential backoff. External mosquitto assumed via systemd.

**Tech Stack:** Rust, tokio::process, serde_json, clap (all existing deps)

---

### Task 1: CLI Arguments

**Files:**
- Modify: `src/cli.rs` (add OrchestratorArgs struct)
- Create: `src/bin/orchestrator.rs`

**Step 1: Add OrchestratorArgs to cli.rs**

```rust
/// Arguments for the orchestrator binary
#[derive(Parser, Debug)]
#[command(name = "commonplace-orchestrator")]
#[command(about = "Process supervisor for commonplace services")]
pub struct OrchestratorArgs {
    /// Path to config file
    #[arg(long, default_value = "commonplace.json")]
    pub config: PathBuf,

    /// Override MQTT broker address
    #[arg(long)]
    pub mqtt_broker: Option<String>,

    /// Disable a specific process
    #[arg(long)]
    pub disable: Vec<String>,

    /// Run only a specific process (skip dependencies)
    #[arg(long)]
    pub only: Option<String>,
}
```

**Step 2: Create minimal orchestrator.rs**

```rust
//! commonplace-orchestrator: Process supervisor for commonplace services

use clap::Parser;
use commonplace_doc::cli::OrchestratorArgs;

#[tokio::main]
async fn main() {
    let args = OrchestratorArgs::parse();
    println!("Config: {:?}", args.config);
}
```

**Step 3: Add binary to Cargo.toml**

Add to `[[bin]]` section:
```toml
[[bin]]
name = "commonplace-orchestrator"
path = "src/bin/orchestrator.rs"
```

**Step 4: Build and verify**

Run: `cargo build --bin commonplace-orchestrator`
Expected: Compiles successfully

Run: `cargo run --bin commonplace-orchestrator -- --help`
Expected: Shows help with --config, --mqtt-broker, --disable, --only flags

**Step 5: Commit**

```bash
git add src/cli.rs src/bin/orchestrator.rs Cargo.toml
git commit -m "CP-okn: Add orchestrator CLI skeleton"
```

---

### Task 2: Config Parsing

**Files:**
- Create: `src/orchestrator/mod.rs`
- Create: `src/orchestrator/config.rs`
- Modify: `src/lib.rs` (add module)

**Step 1: Create config types in src/orchestrator/config.rs**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    #[serde(default = "default_mqtt_broker")]
    pub mqtt_broker: String,
    pub processes: HashMap<String, ProcessConfig>,
}

fn default_mqtt_broker() -> String {
    "localhost:1883".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub restart: RestartPolicy,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartPolicy {
    #[serde(default = "default_policy")]
    pub policy: RestartMode,
    #[serde(default = "default_backoff")]
    pub backoff_ms: u64,
    #[serde(default = "default_max_backoff")]
    pub max_backoff_ms: u64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            policy: RestartMode::Always,
            backoff_ms: 500,
            max_backoff_ms: 10000,
        }
    }
}

fn default_policy() -> RestartMode {
    RestartMode::Always
}

fn default_backoff() -> u64 {
    500
}

fn default_max_backoff() -> u64 {
    10000
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RestartMode {
    Always,
    OnFailure,
    Never,
}

impl OrchestratorConfig {
    pub fn load(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&content)?;
        Ok(config)
    }
}
```

**Step 2: Create src/orchestrator/mod.rs**

```rust
mod config;

pub use config::{OrchestratorConfig, ProcessConfig, RestartMode, RestartPolicy};
```

**Step 3: Add module to src/lib.rs**

Add line: `pub mod orchestrator;`

**Step 4: Build and verify**

Run: `cargo build`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/orchestrator/ src/lib.rs
git commit -m "CP-okn: Add orchestrator config types"
```

---

### Task 3: Config Loading Tests

**Files:**
- Create: `src/orchestrator/config.rs` (add tests module)

**Step 1: Write failing test for config parsing**

Add to bottom of `src/orchestrator/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let json = r#"{
            "processes": {
                "store": {
                    "command": "commonplace-store"
                }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.mqtt_broker, "localhost:1883");
        assert_eq!(config.processes.len(), 1);
        assert_eq!(config.processes["store"].command, "commonplace-store");
        assert_eq!(config.processes["store"].restart.policy, RestartMode::Always);
    }

    #[test]
    fn test_parse_full_config() {
        let json = r#"{
            "mqtt_broker": "localhost:1884",
            "processes": {
                "store": {
                    "command": "commonplace-store",
                    "args": ["--database", "./data.redb"],
                    "restart": { "policy": "on_failure", "backoff_ms": 1000, "max_backoff_ms": 30000 }
                },
                "http": {
                    "command": "commonplace-http",
                    "args": ["--port", "3000"],
                    "depends_on": ["store"]
                }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.mqtt_broker, "localhost:1884");
        assert_eq!(config.processes["store"].restart.policy, RestartMode::OnFailure);
        assert_eq!(config.processes["store"].restart.backoff_ms, 1000);
        assert_eq!(config.processes["http"].depends_on, vec!["store"]);
    }
}
```

**Step 2: Run tests**

Run: `cargo test test_parse`
Expected: PASS

**Step 3: Commit**

```bash
git add src/orchestrator/config.rs
git commit -m "CP-okn: Add config parsing tests"
```

---

### Task 4: Dependency Ordering

**Files:**
- Modify: `src/orchestrator/config.rs`

**Step 1: Write failing test for topological sort**

Add to tests module:

```rust
#[test]
fn test_dependency_order() {
    let json = r#"{
        "processes": {
            "http": { "command": "http", "depends_on": ["store"] },
            "store": { "command": "store", "depends_on": ["broker"] },
            "broker": { "command": "broker" }
        }
    }"#;
    let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
    let order = config.startup_order().unwrap();
    assert_eq!(order, vec!["broker", "store", "http"]);
}

#[test]
fn test_dependency_cycle_detected() {
    let json = r#"{
        "processes": {
            "a": { "command": "a", "depends_on": ["b"] },
            "b": { "command": "b", "depends_on": ["a"] }
        }
    }"#;
    let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
    assert!(config.startup_order().is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_dependency`
Expected: FAIL - startup_order method not found

**Step 3: Implement startup_order**

Add to `impl OrchestratorConfig`:

```rust
/// Returns process names in dependency order (dependencies first)
pub fn startup_order(&self) -> Result<Vec<String>, String> {
    let mut order = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut visiting = std::collections::HashSet::new();

    fn visit(
        name: &str,
        processes: &HashMap<String, ProcessConfig>,
        visited: &mut std::collections::HashSet<String>,
        visiting: &mut std::collections::HashSet<String>,
        order: &mut Vec<String>,
    ) -> Result<(), String> {
        if visited.contains(name) {
            return Ok(());
        }
        if visiting.contains(name) {
            return Err(format!("Dependency cycle detected involving '{}'", name));
        }
        visiting.insert(name.to_string());

        if let Some(config) = processes.get(name) {
            for dep in &config.depends_on {
                if !processes.contains_key(dep) {
                    return Err(format!("Unknown dependency '{}' for process '{}'", dep, name));
                }
                visit(dep, processes, visited, visiting, order)?;
            }
        }

        visiting.remove(name);
        visited.insert(name.to_string());
        order.push(name.to_string());
        Ok(())
    }

    for name in self.processes.keys() {
        visit(name, &self.processes, &mut visited, &mut visiting, &mut order)?;
    }

    Ok(order)
}
```

**Step 4: Run tests**

Run: `cargo test test_dependency`
Expected: PASS

**Step 5: Commit**

```bash
git add src/orchestrator/config.rs
git commit -m "CP-okn: Add dependency ordering with cycle detection"
```

---

### Task 5: Process Manager Structure

**Files:**
- Create: `src/orchestrator/manager.rs`
- Modify: `src/orchestrator/mod.rs`

**Step 1: Create ProcessManager skeleton**

Create `src/orchestrator/manager.rs`:

```rust
use super::{OrchestratorConfig, ProcessConfig, RestartMode};
use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

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
}
```

**Step 2: Export from mod.rs**

Add to `src/orchestrator/mod.rs`:

```rust
mod manager;

pub use manager::{ManagedProcess, ProcessManager, ProcessState};
```

**Step 3: Build**

Run: `cargo build`
Expected: Compiles (warnings about unused fields OK)

**Step 4: Commit**

```bash
git add src/orchestrator/manager.rs src/orchestrator/mod.rs
git commit -m "CP-okn: Add ProcessManager structure"
```

---

### Task 6: Process Spawning

**Files:**
- Modify: `src/orchestrator/manager.rs`

**Step 1: Add spawn_process method**

Add to `impl ProcessManager`:

```rust
pub async fn spawn_process(&mut self, name: &str) -> Result<(), String> {
    if self.disabled.contains(&name.to_string()) {
        tracing::info!("[orchestrator] Skipping disabled process: {}", name);
        return Ok(());
    }

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
        cmd.arg(self.mqtt_broker());
    }

    // Capture stdout/stderr
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    tracing::info!("[orchestrator] Starting process: {}", name);

    let mut child = cmd.spawn().map_err(|e| format!("Failed to spawn {}: {}", name, e))?;

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
```

**Step 2: Build**

Run: `cargo build`
Expected: Compiles

**Step 3: Commit**

```bash
git add src/orchestrator/manager.rs
git commit -m "CP-okn: Add process spawning with output capture"
```

---

### Task 7: Startup Sequence

**Files:**
- Modify: `src/orchestrator/manager.rs`

**Step 1: Add start_all method**

Add to `impl ProcessManager`:

```rust
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
```

**Step 2: Build**

Run: `cargo build`
Expected: Compiles

**Step 3: Commit**

```bash
git add src/orchestrator/manager.rs
git commit -m "CP-okn: Add start_all with dependency ordering"
```

---

### Task 8: Process Monitoring Loop

**Files:**
- Modify: `src/orchestrator/manager.rs`

**Step 1: Add run method for monitoring**

Add to `impl ProcessManager`:

```rust
pub async fn run(&mut self) -> Result<(), String> {
    // Start all processes
    self.start_all().await?;

    // Monitor loop
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;

        for (name, process) in &mut self.processes {
            if self.disabled.contains(name) {
                continue;
            }

            if let Some(ref mut child) = process.handle {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        // Process exited
                        let success = status.success();
                        tracing::warn!(
                            "[orchestrator] Process '{}' exited with status: {}",
                            name,
                            status
                        );

                        process.handle = None;

                        // Check restart policy
                        let should_restart = match process.config.restart.policy {
                            RestartMode::Always => true,
                            RestartMode::OnFailure => !success,
                            RestartMode::Never => false,
                        };

                        if should_restart {
                            process.consecutive_failures += 1;
                            process.state = ProcessState::Failed;

                            // Calculate backoff
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

                            // Respawn - need to clone name to avoid borrow issues
                            let name_clone = name.clone();
                            drop(process); // Release borrow
                            if let Err(e) = self.spawn_process(&name_clone).await {
                                tracing::error!("[orchestrator] Failed to restart '{}': {}", name_clone, e);
                            }
                            break; // Restart the monitoring loop
                        } else {
                            process.state = ProcessState::Stopped;
                        }
                    }
                    Ok(None) => {
                        // Still running - reset failure count if running long enough
                        if let Some(start) = process.last_start {
                            if start.elapsed() > Duration::from_secs(30) && process.consecutive_failures > 0 {
                                process.consecutive_failures = 0;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[orchestrator] Error checking process '{}': {}", name, e);
                    }
                }
            }
        }
    }
}
```

**Step 2: Build**

Run: `cargo build`
Expected: Compiles

**Step 3: Commit**

```bash
git add src/orchestrator/manager.rs
git commit -m "CP-okn: Add process monitoring with restart logic"
```

---

### Task 9: Graceful Shutdown

**Files:**
- Modify: `src/orchestrator/manager.rs`

**Step 1: Add shutdown method**

Add to `impl ProcessManager`:

```rust
pub async fn shutdown(&mut self) {
    tracing::info!("[orchestrator] Shutting down...");

    // Get reverse dependency order
    let order = self.config.startup_order().unwrap_or_default();

    // Kill in reverse order
    for name in order.iter().rev() {
        if let Some(process) = self.processes.get_mut(name) {
            if let Some(ref mut child) = process.handle {
                tracing::info!("[orchestrator] Stopping '{}'", name);

                // Try graceful termination first
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    if let Some(pid) = child.id() {
                        unsafe {
                            libc::kill(pid as i32, libc::SIGTERM);
                        }
                    }
                }

                // Wait up to 5 seconds for graceful shutdown
                let timeout = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;

                match timeout {
                    Ok(Ok(_)) => {
                        tracing::info!("[orchestrator] '{}' stopped gracefully", name);
                    }
                    _ => {
                        tracing::warn!("[orchestrator] '{}' didn't stop gracefully, killing", name);
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
```

**Step 2: Build**

Run: `cargo build`
Expected: Compiles

**Step 3: Commit**

```bash
git add src/orchestrator/manager.rs
git commit -m "CP-okn: Add graceful shutdown"
```

---

### Task 10: Wire Up Main Binary

**Files:**
- Modify: `src/bin/orchestrator.rs`

**Step 1: Implement full main function**

Replace `src/bin/orchestrator.rs`:

```rust
//! commonplace-orchestrator: Process supervisor for commonplace services
//!
//! Starts and manages child processes (store, http) with automatic restart on failure.

use clap::Parser;
use commonplace_doc::cli::OrchestratorArgs;
use commonplace_doc::orchestrator::{OrchestratorConfig, ProcessManager};
use std::net::TcpStream;
use std::time::Duration;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Parse CLI arguments
    let args = OrchestratorArgs::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("[orchestrator] Starting commonplace-orchestrator");
    tracing::info!("[orchestrator] Config file: {:?}", args.config);

    // Load config
    let config = match OrchestratorConfig::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("[orchestrator] Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // Determine broker address
    let broker = args
        .mqtt_broker
        .as_ref()
        .unwrap_or(&config.mqtt_broker);

    // Check MQTT broker is reachable
    tracing::info!("[orchestrator] Checking MQTT broker at {}", broker);
    match TcpStream::connect_timeout(
        &broker.parse().expect("Invalid broker address"),
        Duration::from_secs(5),
    ) {
        Ok(_) => {
            tracing::info!("[orchestrator] MQTT broker is reachable");
        }
        Err(e) => {
            tracing::error!(
                "[orchestrator] Cannot connect to MQTT broker at {}: {}",
                broker,
                e
            );
            tracing::error!("[orchestrator] Make sure mosquitto is running (systemctl status mosquitto)");
            std::process::exit(1);
        }
    }

    // Create process manager
    let mut manager = ProcessManager::new(config, args.mqtt_broker.clone(), args.disable.clone());

    // Handle only mode
    if let Some(only) = &args.only {
        tracing::info!("[orchestrator] Running only: {}", only);
        if let Err(e) = manager.spawn_process(only).await {
            tracing::error!("[orchestrator] Failed to start '{}': {}", only, e);
            std::process::exit(1);
        }
    } else {
        // Spawn the main run loop
        let run_handle = tokio::spawn(async move {
            if let Err(e) = manager.run().await {
                tracing::error!("[orchestrator] Run error: {}", e);
            }
            manager
        });

        // Wait for shutdown signal
        tokio::select! {
            _ = signal::ctrl_c() => {
                tracing::info!("[orchestrator] Received Ctrl+C");
            }
        }

        // Shutdown
        let mut manager = run_handle.await.expect("Run task panicked");
        manager.shutdown().await;
    }
}
```

**Step 2: Build and verify**

Run: `cargo build --bin commonplace-orchestrator`
Expected: Compiles

**Step 3: Commit**

```bash
git add src/bin/orchestrator.rs
git commit -m "CP-okn: Wire up orchestrator main binary"
```

---

### Task 11: Create Example Config

**Files:**
- Create: `commonplace.json`

**Step 1: Create example config file**

```json
{
  "mqtt_broker": "localhost:1883",
  "processes": {
    "store": {
      "command": "commonplace-store",
      "args": ["--database", "./data.redb", "--fs-root", "fs-root.json"],
      "restart": {
        "policy": "always",
        "backoff_ms": 500,
        "max_backoff_ms": 10000
      }
    },
    "http": {
      "command": "commonplace-http",
      "args": ["--port", "3000"],
      "restart": {
        "policy": "always",
        "backoff_ms": 500,
        "max_backoff_ms": 10000
      },
      "depends_on": ["store"]
    }
  }
}
```

**Step 2: Commit**

```bash
git add commonplace.json
git commit -m "CP-okn: Add example orchestrator config"
```

---

### Task 12: Integration Test

**Files:**
- Manual testing

**Step 1: Start mosquitto if not running**

Run: `systemctl status mosquitto`
If not running: `sudo systemctl start mosquitto`

**Step 2: Build all binaries**

Run: `cargo build --release`

**Step 3: Run orchestrator**

Run: `cargo run --bin commonplace-orchestrator`
Expected:
- Logs show broker check passes
- store process starts
- http process starts after store
- Prefixed logs from child processes appear

**Step 4: Test restart (in another terminal)**

Run: `pkill commonplace-store`
Expected:
- Orchestrator detects exit
- Waits backoff period
- Restarts store

**Step 5: Test shutdown**

Press Ctrl+C
Expected:
- Orchestrator sends SIGTERM to children
- Children stop gracefully
- Orchestrator exits cleanly

**Step 6: Final commit**

```bash
git add -A
git commit -m "CP-okn: Complete orchestrator implementation"
```

---

## Summary

11 implementation tasks + 1 integration test:
1. CLI arguments
2. Config parsing types
3. Config parsing tests
4. Dependency ordering
5. Process manager structure
6. Process spawning
7. Startup sequence
8. Process monitoring loop
9. Graceful shutdown
10. Wire up main binary
11. Example config
12. Integration test
