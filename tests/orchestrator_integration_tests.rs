//! Integration tests for the orchestrator.
//!
//! These tests verify that the orchestrator correctly starts and manages
//! the server and sync processes.
//!
//! NOTE: These tests must run serially (--test-threads=1) because they share
//! the orchestrator status file at /tmp/commonplace-orchestrator-status.json.

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

/// Find an available port
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Clean up child processes on drop
struct ProcessGuard {
    children: Vec<Child>,
}

impl ProcessGuard {
    fn new() -> Self {
        Self {
            children: Vec::new(),
        }
    }

    fn add(&mut self, child: Child) {
        self.children.push(child);
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        for mut child in self.children.drain(..) {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Clean up the status file
        let _ = std::fs::remove_file("/tmp/commonplace-orchestrator-status.json");
    }
}

/// Create a minimal orchestrator config for testing
fn create_test_config(
    temp_dir: &std::path::Path,
    port: u16,
    db_path: &std::path::Path,
    workspace_dir: &std::path::Path,
) -> String {
    let server_bin = env!("CARGO_BIN_EXE_commonplace-server");
    let sync_bin = env!("CARGO_BIN_EXE_commonplace-sync");
    let server_url = format!("http://127.0.0.1:{}", port);

    serde_json::json!({
        "database": db_path.to_str().unwrap(),
        "http_server": &server_url,
        "managed_paths": [],
        "processes": {
            "server": {
                "command": server_bin,
                "args": ["--port", port.to_string(), "--fs-root", "workspace"],
                "cwd": temp_dir.to_str().unwrap(),
                "restart": {
                    "policy": "never"
                }
            },
            "sync": {
                "command": sync_bin,
                "args": [
                    "--server", &server_url,
                    "--node", "workspace",
                    "--directory", workspace_dir.to_str().unwrap(),
                    "--initial-sync", "local"
                ],
                "restart": {
                    "policy": "never"
                },
                "depends_on": ["server"]
            }
        }
    })
    .to_string()
}

/// Wait for the orchestrator status file to exist and have the expected processes
fn wait_for_orchestrator_ready(timeout: Duration) -> Result<serde_json::Value, String> {
    let start = std::time::Instant::now();
    let status_path = "/tmp/commonplace-orchestrator-status.json";

    loop {
        if start.elapsed() > timeout {
            return Err("Timeout waiting for orchestrator to be ready".to_string());
        }

        // Check if status file exists
        if let Ok(content) = std::fs::read_to_string(status_path) {
            if let Ok(status) = serde_json::from_str::<serde_json::Value>(&content) {
                // Check if we have both server and sync running
                if let Some(processes) = status.get("processes").and_then(|p| p.as_array()) {
                    let server_running = processes.iter().any(|p| {
                        p.get("name").and_then(|n| n.as_str()) == Some("server")
                            && p.get("state").and_then(|s| s.as_str()) == Some("Running")
                            && p.get("pid").and_then(|p| p.as_u64()).is_some()
                    });
                    let sync_running = processes.iter().any(|p| {
                        p.get("name").and_then(|n| n.as_str()) == Some("sync")
                            && p.get("state").and_then(|s| s.as_str()) == Some("Running")
                            && p.get("pid").and_then(|p| p.as_u64()).is_some()
                    });

                    if server_running && sync_running {
                        return Ok(status);
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

/// CP-02vo: Test that orchestrator starts and base processes (server, sync) run correctly.
///
/// Verifies:
/// - P1: Orchestrator is running (commonplace-ps shows orchestrator PID)
/// - P2: Server is running with database persistence
/// - P3: Workspace sync is running
#[test]
fn test_orchestrator_starts_base_processes() {
    // Clean up any stale status file from previous runs
    let _ = std::fs::remove_file("/tmp/commonplace-orchestrator-status.json");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let workspace_dir = temp_dir.path().join("workspace");
    let config_path = temp_dir.path().join("commonplace.json");
    let port = get_available_port();

    // Create workspace directory
    std::fs::create_dir_all(&workspace_dir).unwrap();

    // Create a minimal .commonplace.json schema in the workspace
    let schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&schema).unwrap(),
    )
    .unwrap();

    // Write config file
    let config = create_test_config(temp_dir.path(), port, &db_path, &workspace_dir);
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator with explicit --server for health checking
    let server_url = format!("http://127.0.0.1:{}", port);
    let orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--server",
            &server_url,
        ])
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn orchestrator");

    guard.add(orchestrator);

    // Wait for orchestrator to be ready with server and sync running
    let status = wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    // P1: Verify orchestrator PID is set
    let orchestrator_pid = status
        .get("orchestrator_pid")
        .and_then(|p| p.as_u64())
        .expect("orchestrator_pid should be set");
    assert!(orchestrator_pid > 0, "orchestrator_pid should be positive");

    // P2: Verify server is running
    let processes = status
        .get("processes")
        .and_then(|p| p.as_array())
        .expect("processes should be an array");

    let server = processes
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("server"))
        .expect("server process should exist");

    assert_eq!(
        server.get("state").and_then(|s| s.as_str()),
        Some("Running"),
        "server should be Running"
    );
    let server_pid = server
        .get("pid")
        .and_then(|p| p.as_u64())
        .expect("server should have a PID");
    assert!(server_pid > 0, "server PID should be positive");

    // P3: Verify sync is running
    let sync = processes
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("sync"))
        .expect("sync process should exist");

    assert_eq!(
        sync.get("state").and_then(|s| s.as_str()),
        Some("Running"),
        "sync should be Running"
    );
    let sync_pid = sync
        .get("pid")
        .and_then(|p| p.as_u64())
        .expect("sync should have a PID");
    assert!(sync_pid > 0, "sync PID should be positive");

    // Verify server is responding to health checks
    let client = reqwest::blocking::Client::new();
    let health_resp = client
        .get(format!("http://127.0.0.1:{}/health", port))
        .send()
        .expect("Failed to send health check");
    assert!(
        health_resp.status().is_success(),
        "Server health check should succeed"
    );

    // Verify database file was created (P2: database persistence)
    assert!(db_path.exists(), "Database file should be created");
}

/// Test that commonplace-ps correctly reports orchestrator status
#[test]
fn test_commonplace_ps_reports_status() {
    // Clean up any stale status file from previous runs
    let _ = std::fs::remove_file("/tmp/commonplace-orchestrator-status.json");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let workspace_dir = temp_dir.path().join("workspace");
    let config_path = temp_dir.path().join("commonplace.json");
    let port = get_available_port();

    // Create workspace directory with schema
    std::fs::create_dir_all(&workspace_dir).unwrap();
    let schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&schema).unwrap(),
    )
    .unwrap();

    // Write config file
    let config = create_test_config(temp_dir.path(), port, &db_path, &workspace_dir);
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator with explicit --server for health checking
    let server_url = format!("http://127.0.0.1:{}", port);
    let orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--server",
            &server_url,
        ])
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn orchestrator");

    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    // Run commonplace-ps --json and verify output
    let ps_output = Command::new(env!("CARGO_BIN_EXE_commonplace-ps"))
        .arg("--json")
        .output()
        .expect("Failed to run commonplace-ps");

    assert!(
        ps_output.status.success(),
        "commonplace-ps should succeed: {}",
        String::from_utf8_lossy(&ps_output.stderr)
    );

    let ps_json: serde_json::Value = serde_json::from_slice(&ps_output.stdout)
        .expect("commonplace-ps output should be valid JSON");

    // Verify orchestrator_pid is present
    assert!(
        ps_json.get("orchestrator_pid").is_some(),
        "commonplace-ps should report orchestrator_pid"
    );

    // Verify processes are listed
    let processes = ps_json
        .get("processes")
        .and_then(|p| p.as_array())
        .expect("processes should be an array");

    assert!(
        processes
            .iter()
            .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("server")),
        "server should be in process list"
    );
    assert!(
        processes
            .iter()
            .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("sync")),
        "sync should be in process list"
    );
}
