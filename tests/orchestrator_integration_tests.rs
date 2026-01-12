//! Integration tests for the orchestrator.
//!
//! These tests verify that the orchestrator correctly starts and manages
//! the server and sync processes.
//!
//! NOTE: These tests must run serially (--test-threads=1) because they share
//! the orchestrator status file at /tmp/commonplace-orchestrator-status.json.
//!
//! NOTE: These tests require MQTT broker (mosquitto) running on localhost:1883.
//! They are skipped automatically if MQTT is not available.

use std::collections::HashSet;
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

/// Check if MQTT broker is available on localhost:1883
fn mqtt_available() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:1883".parse().unwrap(),
        Duration::from_millis(100),
    )
    .is_ok()
}

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
    create_test_config_with_discovery(temp_dir, port, db_path, workspace_dir, false)
}

/// Create orchestrator config with optional process discovery
fn create_test_config_with_discovery(
    temp_dir: &std::path::Path,
    port: u16,
    db_path: &std::path::Path,
    workspace_dir: &std::path::Path,
    enable_discovery: bool,
) -> String {
    let server_bin = env!("CARGO_BIN_EXE_commonplace-server");
    let sync_bin = env!("CARGO_BIN_EXE_commonplace-sync");
    let server_url = format!("http://127.0.0.1:{}", port);

    let managed_paths: Vec<&str> = if enable_discovery { vec!["/"] } else { vec![] };

    serde_json::json!({
        "database": db_path.to_str().unwrap(),
        "http_server": &server_url,
        "managed_paths": managed_paths,
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

/// Wait for a discovered process to appear in the orchestrator status
fn wait_for_discovered_process(process_name: &str, timeout: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let status_path = "/tmp/commonplace-orchestrator-status.json";

    loop {
        if start.elapsed() > timeout {
            // Return what we did find for debugging
            if let Ok(content) = std::fs::read_to_string(status_path) {
                eprintln!("Status file contents at timeout: {}", content);
            }
            return Err(format!(
                "Timeout waiting for process '{}' to appear",
                process_name
            ));
        }

        if let Ok(content) = std::fs::read_to_string(status_path) {
            if let Ok(status) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(processes) = status.get("processes").and_then(|p| p.as_array()) {
                    let found = processes
                        .iter()
                        .any(|p| p.get("name").and_then(|n| n.as_str()) == Some(process_name));
                    if found {
                        return Ok(());
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(200));
    }
}

/// CP-02vo: Test that orchestrator starts and base processes (server, sync) run correctly.
///
/// Verifies:
/// - P1: Orchestrator is running (commonplace-ps shows orchestrator PID)
/// - P2: Server is running with database persistence
/// - P3: Workspace sync is running
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_orchestrator_starts_base_processes() {
    if !mqtt_available() {
        eprintln!("Skipping test: MQTT broker not available on localhost:1883");
        return;
    }

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
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_commonplace_ps_reports_status() {
    if !mqtt_available() {
        eprintln!("Skipping test: MQTT broker not available on localhost:1883");
        return;
    }

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

/// CP-qf7t: Test that sandbox processes launch in the sandbox working directory.
///
/// Verifies:
/// - P5: Sandbox process runs in /tmp/commonplace-sandbox-* cwd
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_sandbox_process_runs_in_sandbox_cwd() {
    if !mqtt_available() {
        eprintln!("Skipping test: MQTT broker not available on localhost:1883");
        return;
    }

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

    // Create a subdirectory with __processes.json containing a sandbox process
    let sandbox_test_dir = workspace_dir.join("sandbox-test");
    std::fs::create_dir_all(&sandbox_test_dir).unwrap();

    // Create schema for the subdirectory
    let subdir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        sandbox_test_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&subdir_schema).unwrap(),
    )
    .unwrap();

    // Create __processes.json with a sandbox process that writes pwd to a file
    // The process writes to a known location so we can verify the cwd
    let output_marker = temp_dir.path().join("sandbox-pwd-output.txt");
    let processes_json = serde_json::json!({
        "processes": {
            "pwd-test": {
                "sandbox-exec": format!("sh -c 'pwd > {}'", output_marker.to_str().unwrap())
            }
        }
    });
    std::fs::write(
        sandbox_test_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Write config file with process discovery enabled
    let config =
        create_test_config_with_discovery(temp_dir.path(), port, &db_path, &workspace_dir, true);
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator with process discovery enabled
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

    // Wait for orchestrator to be ready with base processes
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    // Wait for the sandbox process to be discovered and appear in status
    // This requires:
    // 1. sync to push the sandbox-test/.commonplace.json and __processes.json to server
    // 2. orchestrator discovery to find the __processes.json
    // 3. orchestrator to spawn the sandbox process
    wait_for_discovered_process("pwd-test", Duration::from_secs(60))
        .expect("Sandbox process 'pwd-test' was not discovered");

    // Now wait for the sandbox process to actually write its output
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout {
            panic!(
                "Timeout waiting for sandbox process to write output to {:?}",
                output_marker
            );
        }

        if output_marker.exists() {
            break;
        }

        std::thread::sleep(Duration::from_millis(200));
    }

    // Read the output and verify it's a sandbox directory
    let pwd_output = std::fs::read_to_string(&output_marker)
        .expect("Failed to read pwd output")
        .trim()
        .to_string();

    // P5: Verify sandbox process runs in /tmp/commonplace-sandbox-* cwd
    assert!(
        pwd_output.starts_with("/tmp/commonplace-sandbox-"),
        "Sandbox process should run in /tmp/commonplace-sandbox-*, got: {}",
        pwd_output
    );
}

/// Wait for a discovered process to disappear from the orchestrator status
/// TODO(CP-y1cu): Use this function once sync modification propagation is fixed
#[allow(dead_code)]
fn wait_for_process_removed(process_name: &str, timeout: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let status_path = "/tmp/commonplace-orchestrator-status.json";

    loop {
        if start.elapsed() > timeout {
            if let Ok(content) = std::fs::read_to_string(status_path) {
                eprintln!("Status file contents at timeout: {}", content);
            }
            return Err(format!(
                "Timeout waiting for process '{}' to be removed",
                process_name
            ));
        }

        if let Ok(content) = std::fs::read_to_string(status_path) {
            if let Ok(status) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(processes) = status.get("processes").and_then(|p| p.as_array()) {
                    let found = processes
                        .iter()
                        .any(|p| p.get("name").and_then(|n| n.as_str()) == Some(process_name));
                    if !found {
                        return Ok(());
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(200));
    }
}

/// CP-n6sd: Test that adding/removing processes from __processes.json dynamically starts/stops them.
///
/// Verifies:
/// - H3: Add new process to __processes.json
/// - H4: Verify new process appears in commonplace-ps within 10 seconds
/// - H5: Remove process from __processes.json
/// - H6: Verify process stopped and removed from commonplace-ps within 10 seconds
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_processes_json_add_remove_starts_stops_processes() {
    if !mqtt_available() {
        eprintln!("Skipping test: MQTT broker not available on localhost:1883");
        return;
    }

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

    // Create a subdirectory with __processes.json containing an initial process
    let test_dir = workspace_dir.join("dynamic-test");
    std::fs::create_dir_all(&test_dir).unwrap();

    let subdir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        test_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&subdir_schema).unwrap(),
    )
    .unwrap();

    // Initial __processes.json with one process
    let initial_marker = temp_dir.path().join("initial-process-ran.txt");
    let initial_processes = serde_json::json!({
        "processes": {
            "initial-proc": {
                "sandbox-exec": format!("sh -c 'echo started > {}'", initial_marker.to_str().unwrap())
            }
        }
    });
    let processes_json_path = test_dir.join("__processes.json");
    std::fs::write(
        &processes_json_path,
        serde_json::to_string_pretty(&initial_processes).unwrap(),
    )
    .unwrap();

    // Write config file with process discovery enabled
    let config =
        create_test_config_with_discovery(temp_dir.path(), port, &db_path, &workspace_dir, true);
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator
    let server_url = format!("http://127.0.0.1:{}", port);
    let orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--server",
            &server_url,
        ])
        .current_dir(temp_dir.path())
        .stdout(Stdio::null()) // Use null to avoid pipe buffer blocking
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn orchestrator");

    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    // Wait for the initial process to be discovered
    wait_for_discovered_process("initial-proc", Duration::from_secs(60))
        .expect("Initial process 'initial-proc' was not discovered");

    // H3: Add a new process to __processes.json
    let added_marker = temp_dir.path().join("added-process-ran.txt");
    let updated_processes = serde_json::json!({
        "processes": {
            "initial-proc": {
                "sandbox-exec": format!("sh -c 'echo started > {}'", initial_marker.to_str().unwrap())
            },
            "added-proc": {
                "sandbox-exec": format!("sh -c 'echo added > {}'", added_marker.to_str().unwrap())
            }
        }
    });
    std::fs::write(
        &processes_json_path,
        serde_json::to_string_pretty(&updated_processes).unwrap(),
    )
    .unwrap();

    // H4: Verify new process appears within 10 seconds (using 30 to be safe in CI)
    wait_for_discovered_process("added-proc", Duration::from_secs(30))
        .expect("Added process 'added-proc' should appear after updating __processes.json");

    // Verify the added process actually ran
    let start = std::time::Instant::now();
    while !added_marker.exists() && start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        added_marker.exists(),
        "Added process should have written its marker file"
    );

    // H5: Modify __processes.json again to REMOVE added-proc
    eprintln!("Writing __processes.json with removed added-proc...");
    let modified_config2 = serde_json::json!({
        "processes": {
            "initial-proc": {
                "sandbox-exec": format!("sh -c 'echo started > {}/initial-process-ran.txt'", temp_dir.path().display())
            }
            // added-proc is removed
        }
    });
    // Write directly to the __processes.json file (simulating an edit)
    std::fs::write(
        &processes_json_path,
        serde_json::to_string_pretty(&modified_config2).unwrap(),
    )
    .expect("Failed to write modified __processes.json");

    // Give the sync client time to detect the change and upload
    std::thread::sleep(Duration::from_secs(3));

    // Check server content to verify sync happened
    // Get the __processes.json content directly
    let client = reqwest::blocking::Client::new();
    let processes_url = format!("{}/files/dynamic-test/__processes.json", server_url);
    let head_resp = client.get(&processes_url).send().unwrap();
    let server_text = head_resp.text().unwrap();
    eprintln!("Server __processes.json content: {}", server_text);

    let server_content: serde_json::Value = serde_json::from_str(&server_text).unwrap();
    let server_has_added = server_content["processes"]
        .as_object()
        .map(|p| p.contains_key("added-proc"))
        .unwrap_or(false);

    // Wait for orchestrator to reconcile (with timeout)
    let start = std::time::Instant::now();
    let mut removed = false;
    while start.elapsed() < Duration::from_secs(15) {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(status_content) =
            std::fs::read_to_string("/tmp/commonplace-orchestrator-status.json")
        {
            if let Ok(status) = serde_json::from_str::<serde_json::Value>(&status_content) {
                if let Some(processes) = status.get("processes").and_then(|p| p.as_array()) {
                    let added_still_running = processes
                        .iter()
                        .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("added-proc"));
                    if !added_still_running {
                        removed = true;
                        break;
                    }
                }
            }
        }
    }

    // H6: Verify added-proc was removed
    let status_content =
        std::fs::read_to_string("/tmp/commonplace-orchestrator-status.json").unwrap();
    let status: serde_json::Value = serde_json::from_str(&status_content).unwrap();
    let processes = status.get("processes").and_then(|p| p.as_array()).unwrap();
    let initial_running = processes
        .iter()
        .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("initial-proc"));
    let added_running = processes
        .iter()
        .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("added-proc"));

    eprintln!(
        "Final status: {}",
        serde_json::to_string_pretty(&status).unwrap()
    );

    assert!(initial_running, "initial-proc should still be running");
    assert!(
        !server_has_added,
        "Server should NOT have added-proc (sync worked)"
    );
    assert!(
        removed && !added_running,
        "Process 'added-proc' should have been removed after updating __processes.json \
         (server_has_added={}, removed={}, added_running={})",
        server_has_added,
        removed,
        added_running
    );
}

/// Get all current sandbox directories (those matching /tmp/commonplace-sandbox-*).
fn get_sandbox_dirs() -> HashSet<std::path::PathBuf> {
    let mut dirs = HashSet::new();
    if let Ok(entries) = std::fs::read_dir("/tmp") {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("commonplace-sandbox-") && path.is_dir() {
                    dirs.insert(path);
                }
            }
        }
    }
    dirs
}

/// Wait for a new sandbox directory to appear that wasn't in the initial set.
fn wait_for_new_sandbox_dir(
    initial_dirs: &HashSet<std::path::PathBuf>,
    timeout: Duration,
) -> Result<std::path::PathBuf, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err("Timeout waiting for new sandbox directory".to_string());
        }
        let current_dirs = get_sandbox_dirs();
        for dir in &current_dirs {
            if !initial_dirs.contains(dir) {
                // Found a new sandbox directory
                // Check if it has a .pid file (indicates it's active)
                if dir.join(".pid").exists() {
                    return Ok(dir.clone());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait for a file to exist with optional content check.
fn wait_for_file(
    path: &std::path::Path,
    expected_content: Option<&str>,
    timeout: Duration,
) -> Result<String, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!("Timeout waiting for file {:?}", path));
        }
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Some(expected) = expected_content {
                    if content.trim() == expected.trim() {
                        return Ok(content);
                    }
                } else {
                    return Ok(content);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait for a file to be deleted.
fn wait_for_file_deleted(path: &std::path::Path, timeout: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!("Timeout waiting for file {:?} to be deleted", path));
        }
        if !path.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// CP-s7sx: Test workspace <-> sandbox file sync for create/edit/delete operations.
///
/// Verifies:
/// - C1-C3: Create file in workspace, verify it appears in sandbox with matching content
/// - E1-E2: Edit file in workspace, verify sandbox reflects the change
/// - E3-E4: Edit file in sandbox, verify workspace reflects the change
/// - D1-D2: Delete file in workspace, verify it's removed from sandbox
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_workspace_sandbox_file_sync_create_edit_delete() {
    if !mqtt_available() {
        eprintln!("Skipping test: MQTT broker not available on localhost:1883");
        return;
    }

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

    // Create a subdirectory for the sandbox test
    let sync_test_dir = workspace_dir.join("sync-test");
    std::fs::create_dir_all(&sync_test_dir).unwrap();

    // Create schema for the subdirectory
    let subdir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        sync_test_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&subdir_schema).unwrap(),
    )
    .unwrap();

    // Create __processes.json with a long-running sandbox process that keeps the sandbox alive
    // We use a sleep command that runs long enough for us to do our file sync tests
    let processes_json = serde_json::json!({
        "processes": {
            "sync-test-proc": {
                "sandbox-exec": "sleep 120"
            }
        }
    });
    std::fs::write(
        sync_test_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Write config file with process discovery enabled
    let config =
        create_test_config_with_discovery(temp_dir.path(), port, &db_path, &workspace_dir, true);
    std::fs::write(&config_path, &config).unwrap();

    // Record existing sandbox directories before starting orchestrator
    let initial_sandbox_dirs = get_sandbox_dirs();
    eprintln!(
        "Initial sandbox directories: {:?}",
        initial_sandbox_dirs.len()
    );

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator
    let server_url = format!("http://127.0.0.1:{}", port);
    let orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--server",
            &server_url,
        ])
        .current_dir(temp_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn orchestrator");

    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    // Wait for the sandbox process to be discovered and appear in status
    wait_for_discovered_process("sync-test-proc", Duration::from_secs(60))
        .expect("Sandbox process 'sync-test-proc' was not discovered");

    // Wait for the sandbox directory to be created (commonplace-sync creates it)
    let sandbox_dir = wait_for_new_sandbox_dir(&initial_sandbox_dirs, Duration::from_secs(30))
        .expect("Failed to find new sandbox directory");

    eprintln!("Sandbox directory: {:?}", sandbox_dir);

    // === C1-C3: File Creation Propagation ===
    eprintln!("=== Testing file creation propagation ===");

    // C1: Create a new file in workspace
    let test_file_name = "test-file.txt";
    let workspace_test_file = sync_test_dir.join(test_file_name);
    std::fs::write(&workspace_test_file, "hello").expect("Failed to write test file");

    // C2-C3: Verify file appears in sandbox with matching content
    let sandbox_test_file = sandbox_dir.join(test_file_name);
    let content = wait_for_file(&sandbox_test_file, Some("hello"), Duration::from_secs(10))
        .expect("File should appear in sandbox with content 'hello'");
    assert_eq!(content.trim(), "hello", "Sandbox file content should match");
    eprintln!("C1-C3: File creation propagation PASSED");

    // === E1-E2: Edit Propagation (workspace -> sandbox) ===
    eprintln!("=== Testing edit propagation: workspace -> sandbox ===");

    // E1: Edit file in workspace
    std::fs::write(&workspace_test_file, "hello world").expect("Failed to edit test file");

    // E2: Verify sandbox reflects the change
    let content = wait_for_file(
        &sandbox_test_file,
        Some("hello world"),
        Duration::from_secs(10),
    )
    .expect("Sandbox file should be updated to 'hello world'");
    assert_eq!(
        content.trim(),
        "hello world",
        "Sandbox file should show edit"
    );
    eprintln!("E1-E2: Edit propagation (workspace -> sandbox) PASSED");

    // === E3-E4: Edit Propagation (sandbox -> workspace) ===
    eprintln!("=== Testing edit propagation: sandbox -> workspace ===");

    // Wait for CRDT states to fully synchronize before making sandbox edit
    // This gives time for all pending sync operations to complete
    std::thread::sleep(Duration::from_secs(2));

    // Verify both sides have the same content before sandbox edit
    let workspace_pre = std::fs::read_to_string(&workspace_test_file).unwrap();
    let sandbox_pre = std::fs::read_to_string(&sandbox_test_file).unwrap();
    eprintln!("Pre-edit workspace: {:?}", workspace_pre);
    eprintln!("Pre-edit sandbox: {:?}", sandbox_pre);
    assert_eq!(
        workspace_pre.trim(),
        sandbox_pre.trim(),
        "Files should be in sync before sandbox edit"
    );

    // E3: Edit file in sandbox - append "!" to existing content
    // Use a completely different content to avoid CRDT merge confusion
    let new_content = "sandbox edit test";
    std::fs::write(&sandbox_test_file, new_content).expect("Failed to edit sandbox file");
    eprintln!("Sandbox file written: {:?}", new_content);

    // E4: Verify workspace reflects the change
    let content = wait_for_file(
        &workspace_test_file,
        Some(new_content),
        Duration::from_secs(30),
    )
    .expect("Workspace file should be updated with sandbox edit");
    assert_eq!(
        content.trim(),
        new_content,
        "Workspace file should show sandbox edit"
    );
    eprintln!("E3-E4: Edit propagation (sandbox -> workspace) PASSED");

    // === D1-D2: File Deletion Propagation ===
    eprintln!("=== Testing file deletion propagation ===");

    // D1: Delete file in workspace
    std::fs::remove_file(&workspace_test_file).expect("Failed to delete test file");

    // D2: Verify file is removed from sandbox
    wait_for_file_deleted(&sandbox_test_file, Duration::from_secs(10))
        .expect("Sandbox file should be deleted when workspace file is deleted");
    eprintln!("D1-D2: File deletion propagation PASSED");

    eprintln!("=== All sync tests PASSED ===");
}
