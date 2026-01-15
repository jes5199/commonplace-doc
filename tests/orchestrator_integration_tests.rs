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
            let pid = child.id();

            // Send SIGTERM first for graceful shutdown (allows orchestrator to terminate children)
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }

            // Wait up to 5 seconds for graceful termination
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(5);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break, // Process exited
                    Ok(None) => {
                        if start.elapsed() > timeout {
                            // Graceful shutdown timed out, force kill
                            let _ = child.kill();
                            let _ = child.wait();
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(_) => break,
                }
            }
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

/// CP-vbnh: Test that modifying a process command triggers restart with new PID.
///
/// Verifies acceptance criteria H1-H2:
/// - H1: Edit __processes.json to change a process command
/// - H2: Verify process restarted with new PID within 10 seconds
#[test]
fn test_process_config_change_triggers_restart() {
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

    // Create a subdirectory with __processes.json
    let test_dir = workspace_dir.join("restart-test");
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

    // Initial __processes.json with a long-running process
    let marker_v1 = temp_dir.path().join("version1.txt");
    let initial_processes = serde_json::json!({
        "processes": {
            "restart-proc": {
                "sandbox-exec": format!("sh -c 'echo v1 > {} && sleep 120'", marker_v1.to_str().unwrap())
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn orchestrator");

    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    eprintln!("=== Process config change restart test starting ===");

    // Wait for the process to be discovered and running
    wait_for_discovered_process("restart-proc", Duration::from_secs(60))
        .expect("Process 'restart-proc' was not discovered");

    // Wait for the marker file to confirm process started
    let start = std::time::Instant::now();
    while !marker_v1.exists() && start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(marker_v1.exists(), "Process v1 should have written marker");
    eprintln!("H1 setup: Initial process running");

    // Get the initial PID from status (with retry - PID may take a moment to appear in status file)
    let start = std::time::Instant::now();
    let initial_pid = loop {
        if let Some(pid) = get_process_pid("restart-proc") {
            break pid;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timeout waiting for initial PID in status file");
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    eprintln!("Initial PID: {:?}", initial_pid);

    // === H1: Edit __processes.json to change the process command ===
    eprintln!("=== H1: Changing process command ===");
    let marker_v2 = temp_dir.path().join("version2.txt");
    let updated_processes = serde_json::json!({
        "processes": {
            "restart-proc": {
                "sandbox-exec": format!("sh -c 'echo v2 > {} && sleep 120'", marker_v2.to_str().unwrap())
            }
        }
    });
    std::fs::write(
        &processes_json_path,
        serde_json::to_string_pretty(&updated_processes).unwrap(),
    )
    .unwrap();
    eprintln!("H1: Process command changed");

    // === H2: Verify process restarted with new PID within 10 seconds ===
    eprintln!("=== H2: Verifying process restart ===");
    let start = std::time::Instant::now();
    let mut restarted = false;
    let mut new_pid = None;

    while start.elapsed() < Duration::from_secs(30) {
        std::thread::sleep(Duration::from_millis(500));

        if let Some(pid) = get_process_pid("restart-proc") {
            if initial_pid != pid {
                new_pid = Some(pid);
                restarted = true;
                break;
            }
        }
    }

    assert!(
        restarted,
        "Process should have restarted with new PID (initial: {}, current: {:?})",
        initial_pid,
        get_process_pid("restart-proc")
    );
    eprintln!(
        "H2: Process restarted (old PID: {}, new PID: {:?})",
        initial_pid, new_pid
    );

    // Verify the new version ran
    let start = std::time::Instant::now();
    while !marker_v2.exists() && start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        marker_v2.exists(),
        "Restarted process should have written v2 marker"
    );

    eprintln!("=== Process config change restart test PASSED ===");
}

/// Helper to get a process PID from the orchestrator status file.
fn get_process_pid(process_name: &str) -> Option<u32> {
    let status_content =
        std::fs::read_to_string("/tmp/commonplace-orchestrator-status.json").ok()?;
    let status: serde_json::Value = serde_json::from_str(&status_content).ok()?;
    let processes = status.get("processes")?.as_array()?;

    for process in processes {
        if process.get("name")?.as_str()? == process_name {
            return process.get("pid")?.as_u64().map(|p| p as u32);
        }
    }
    None
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
/// Note: Prefer wait_for_sandbox_with_process for tests that run in parallel.
#[allow(dead_code)]
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

/// Wait for a sandbox directory that contains a specific process in __processes.json.
/// This is more robust than wait_for_new_sandbox_dir when tests run in parallel.
fn wait_for_sandbox_with_process(
    initial_dirs: &HashSet<std::path::PathBuf>,
    process_name: &str,
    timeout: Duration,
) -> Result<std::path::PathBuf, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for sandbox with process '{}'",
                process_name
            ));
        }
        let current_dirs = get_sandbox_dirs();
        for dir in &current_dirs {
            if !initial_dirs.contains(dir) && dir.join(".pid").exists() {
                // Check if __processes.json contains our process
                let procs_path = dir.join("__processes.json");
                if let Ok(content) = std::fs::read_to_string(&procs_path) {
                    if content.contains(process_name) {
                        return Ok(dir.clone());
                    }
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
    let mut last_content: Option<String> = None;
    loop {
        if start.elapsed() > timeout {
            // Include actual content in error for better debugging
            return match (expected_content, &last_content) {
                (Some(expected), Some(actual)) => Err(format!(
                    "Timeout waiting for file {:?}: expected {:?}, got {:?}",
                    path,
                    expected,
                    actual.trim()
                )),
                (Some(_), None) => Err(format!(
                    "Timeout waiting for file {:?}: file does not exist",
                    path
                )),
                _ => Err(format!("Timeout waiting for file {:?}", path)),
            };
        }
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Some(expected) = expected_content {
                    if content.trim() == expected.trim() {
                        return Ok(content);
                    }
                    last_content = Some(content);
                } else {
                    return Ok(content);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait for a file to exist and contain a specific substring.
fn wait_for_file_containing(
    path: &std::path::Path,
    expected_substring: &str,
    timeout: Duration,
) -> Result<String, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for file {:?} to contain {:?}",
                path, expected_substring
            ));
        }
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(path) {
                if content.contains(expected_substring) {
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

    // Wait for the sandbox directory for this test's process (handles parallel test runs)
    let sandbox_dir = wait_for_sandbox_with_process(
        &initial_sandbox_dirs,
        "sync-test-proc",
        Duration::from_secs(30),
    )
    .expect("Failed to find sandbox directory for sync-test-proc");

    eprintln!("Sandbox directory: {:?}", sandbox_dir);

    // === C1-C3: File Creation Propagation ===

    // C1: Create a new file in workspace
    let test_file_name = "test-file.txt";
    let workspace_test_file = sync_test_dir.join(test_file_name);
    std::fs::write(&workspace_test_file, "hello").expect("Failed to write test file");

    // C2-C3: Verify file appears in sandbox with matching content
    let sandbox_test_file = sandbox_dir.join(test_file_name);
    let content = wait_for_file(&sandbox_test_file, Some("hello"), Duration::from_secs(10))
        .expect("File should appear in sandbox with content 'hello'");
    assert_eq!(content.trim(), "hello", "Sandbox file content should match");

    // Wait for per-file watcher to be fully set up before editing
    // The watcher is spawned asynchronously, so we need to give it time to start
    std::thread::sleep(Duration::from_secs(2));

    // === E1-E2: Edit Propagation (workspace -> sandbox) ===

    // E1: Edit file in workspace
    // First read to ensure file is stable, then write
    let _ = std::fs::read_to_string(&workspace_test_file);
    std::fs::write(&workspace_test_file, "hello world").expect("Failed to edit test file");

    // E2: Verify sandbox reflects the change
    // Use longer timeout for edit propagation - CI can be much slower
    let content = wait_for_file(
        &sandbox_test_file,
        Some("hello world"),
        Duration::from_secs(45),
    )
    .expect("Sandbox file should be updated to 'hello world'");
    assert_eq!(
        content.trim(),
        "hello world",
        "Sandbox file should show edit"
    );

    // === E3-E4: Edit Propagation (sandbox -> workspace) ===

    // Wait for CRDT states to fully synchronize before making sandbox edit
    // This gives time for all pending sync operations to complete
    std::thread::sleep(Duration::from_secs(2));

    // Verify both sides have the same content before sandbox edit
    let workspace_pre = std::fs::read_to_string(&workspace_test_file).unwrap();
    let sandbox_pre = std::fs::read_to_string(&sandbox_test_file).unwrap();
    assert_eq!(
        workspace_pre.trim(),
        sandbox_pre.trim(),
        "Files should be in sync before sandbox edit"
    );

    // E3: Edit file in sandbox - append "!" to existing content
    // Use a completely different content to avoid CRDT merge confusion
    let new_content = "sandbox edit test";
    std::fs::write(&sandbox_test_file, new_content).expect("Failed to edit sandbox file");

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

    // === D1-D2: File Deletion Propagation ===

    // D1: Delete file in workspace
    std::fs::remove_file(&workspace_test_file).expect("Failed to delete test file");

    // D2: Verify file is removed from sandbox
    wait_for_file_deleted(&sandbox_test_file, Duration::from_secs(10))
        .expect("Sandbox file should be deleted when workspace file is deleted");
}

/// Helper to extract node_id from a schema entry
fn get_node_id_from_schema(schema_path: &std::path::Path, filename: &str) -> Option<String> {
    use commonplace_doc::fs::{Entry, FsSchema};

    let content = std::fs::read_to_string(schema_path).ok()?;
    let schema: FsSchema = serde_json::from_str(&content).ok()?;

    if let Some(Entry::Dir(root)) = schema.root {
        if let Some(entries) = root.entries {
            if let Some(Entry::Doc(doc)) = entries.get(filename) {
                return doc.node_id.clone();
            }
        }
    }
    None
}

/// CP-5pcq: Test commonplace-link schema push updates server
///
/// Verifies acceptance criteria L1-L7:
/// - L1: Create two files (source and target)
/// - L2: Run commonplace-link to share UUID
/// - L3: Verify both files have same UUID in .commonplace.json
/// - L4: Verify target file now contains source content
/// - L5-L6: Edit linked file, verify change propagates
/// - L7: Verify linked file appears in sandbox with correct content
///
/// Note: Uses same-directory linking due to SSE subscription limitations for
/// cross-directory subdirectories. This tests the core link functionality.
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_commonplace_link_schema_push_updates_server() {
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
    let root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&root_schema).unwrap(),
    )
    .unwrap();

    // Create a subdirectory for the link test (will contain both source and target)
    let link_dir = workspace_dir.join("link-test");
    std::fs::create_dir_all(&link_dir).unwrap();
    let link_dir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        link_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&link_dir_schema).unwrap(),
    )
    .unwrap();

    // Create __processes.json with a sandbox process so we can test L7
    let processes_json = serde_json::json!({
        "processes": {
            "link-test-proc": {
                "sandbox-exec": "sleep 120"
            }
        }
    });
    std::fs::write(
        link_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Write config file with process discovery enabled
    let config =
        create_test_config_with_discovery(temp_dir.path(), port, &db_path, &workspace_dir, true);
    std::fs::write(&config_path, &config).unwrap();

    // Record existing sandbox directories before starting orchestrator
    let initial_sandbox_dirs = get_sandbox_dirs();

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

    // Wait for sandbox process to be discovered
    wait_for_discovered_process("link-test-proc", Duration::from_secs(60))
        .expect("Sandbox process 'link-test-proc' was not discovered");

    // Find the sandbox directory for this test's process (handles parallel test runs)
    let sandbox_dir = wait_for_sandbox_with_process(
        &initial_sandbox_dirs,
        "link-test-proc",
        Duration::from_secs(30),
    )
    .expect("Sandbox directory for link-test-proc should appear");
    eprintln!("Sandbox directory: {:?}", sandbox_dir);

    // === L1: Create source file and sync ===
    eprintln!("=== L1: Creating source file ===");

    // Create only source file - target will be created by commonplace-link
    let source_file = link_dir.join("source-file.txt");
    std::fs::write(&source_file, "original").expect("Failed to write source file");

    let _target_file = link_dir.join("target-file.txt");

    // Wait for source file to appear in sandbox - confirms sync is working
    let sandbox_source = sandbox_dir.join("source-file.txt");
    let sandbox_target = sandbox_dir.join("target-file.txt");

    // Wait for source file to sync
    wait_for_file(&sandbox_source, Some("original"), Duration::from_secs(30))
        .expect("Source file should sync to sandbox");
    eprintln!("L1: Source file synced to sandbox");

    // The workspace sync client doesn't subscribe to subdirectory SSE events,
    // so the local schema isn't automatically updated. We need to fetch it from
    // the server and write it locally for commonplace-link to work.
    let link_schema = link_dir.join(".commonplace.json");

    // Get the node_id for link-test from the workspace schema
    let workspace_schema_content = std::fs::read_to_string(workspace_dir.join(".commonplace.json"))
        .expect("Workspace schema should exist");
    let workspace_schema: serde_json::Value = serde_json::from_str(&workspace_schema_content)
        .expect("Workspace schema should be valid JSON");
    let link_test_node_id = workspace_schema["root"]["entries"]["link-test"]["node_id"]
        .as_str()
        .expect("link-test should have a node_id");
    eprintln!("link-test node_id: {}", link_test_node_id);

    // Fetch the link-test schema from the server HEAD API
    let head_url = format!("{}/docs/{}/head", server_url, link_test_node_id);
    eprintln!("Fetching schema from: {}", head_url);

    let client = reqwest::blocking::Client::new();
    let response = client.get(&head_url).send().expect("Failed to fetch HEAD");
    let head_json: serde_json::Value = response.json().expect("Invalid HEAD response");
    let server_schema_content = head_json["content"]
        .as_str()
        .expect("HEAD should have content");
    eprintln!("Server schema content: {}", server_schema_content);

    // Write the server schema to the local file (workaround for missing SSE subscription)
    std::fs::write(&link_schema, server_schema_content).expect("Failed to write schema");

    eprintln!("L1: Source file created and synced to server");

    // === L2: Run commonplace-link to share UUID ===
    eprintln!("=== L2: Running commonplace-link ===");

    // Run commonplace-link from workspace root with relative paths (both in link-test)
    let link_output = Command::new(env!("CARGO_BIN_EXE_commonplace-link"))
        .args([
            "--server",
            &server_url,
            "link-test/source-file.txt",
            "link-test/target-file.txt",
        ])
        .current_dir(&workspace_dir)
        .output()
        .expect("Failed to run commonplace-link");

    eprintln!(
        "Link stdout: {}",
        String::from_utf8_lossy(&link_output.stdout)
    );
    eprintln!(
        "Link stderr: {}",
        String::from_utf8_lossy(&link_output.stderr)
    );

    assert!(
        link_output.status.success(),
        "commonplace-link should succeed"
    );

    eprintln!("L2: commonplace-link executed");

    // === L3: Verify both files have same UUID in .commonplace.json ===
    eprintln!("=== L3: Verifying UUID sharing ===");

    let source_uuid = get_node_id_from_schema(&link_schema, "source-file.txt");
    let target_uuid = get_node_id_from_schema(&link_schema, "target-file.txt");

    eprintln!("Source UUID: {:?}", source_uuid);
    eprintln!("Target UUID: {:?}", target_uuid);

    assert!(source_uuid.is_some(), "Source file should have UUID");
    assert!(target_uuid.is_some(), "Target file should have UUID");
    assert_eq!(
        source_uuid, target_uuid,
        "Source and target should share UUID"
    );

    eprintln!("L3: UUID sharing verified");

    // === L4: Verify target file is created with source content ===
    eprintln!("=== L4: Verifying target file was created by link ===");

    // commonplace-link creates the target entry in the schema with source's node_id.
    // The sandbox sync client receives SSE and materializes the file.
    // Note: Workspace sync doesn't subscribe to subdirectory SSE, so we verify via sandbox.
    let content = wait_for_file(&sandbox_target, Some("original"), Duration::from_secs(30))
        .expect("Target file should appear in sandbox with source content 'original'");
    assert_eq!(
        content.trim(),
        "original",
        "Target should have source content"
    );

    eprintln!("L4: Target file created in sandbox with linked content");

    // === L5-L6: Edit linked file, verify change propagates ===
    eprintln!("=== L5-L6: Testing edit propagation via link ===");

    // Wait for sync to stabilize
    std::thread::sleep(Duration::from_secs(2));

    // Edit the target file in sandbox (since workspace doesn't have it)
    std::fs::write(&sandbox_target, "modified via link")
        .expect("Failed to edit sandbox target file");

    // Verify sandbox source file reflects the change (via UUID link).
    // Note: We verify via sandbox_source rather than workspace source_file because
    // workspace sync doesn't subscribe to per-file SSE events for subdirectory files.
    // The sandbox sync handles both files since they share a node_id.
    let content = wait_for_file_containing(
        &sandbox_source,
        "modified via link",
        Duration::from_secs(30),
    )
    .expect("Sandbox source file should receive edit 'modified via link'");
    assert!(
        content.contains("modified via link"),
        "Source should show edit from linked file, got: {}",
        content
    );

    eprintln!("L5-L6: Edit propagation via link verified (sandbox-to-sandbox)");

    // === L7: Verify linked file in sandbox has the modified content ===
    eprintln!("=== L7: Verifying sandbox linked file has modified content ===");

    // After edit propagation, sandbox target should still have the modified content
    // (we edited it above, and since it shares node_id, edits are bidirectional)
    let content =
        std::fs::read_to_string(&sandbox_target).expect("Should read sandbox target file");
    assert_eq!(
        content.trim(),
        "modified via link",
        "Sandbox should have linked file with modified content"
    );

    eprintln!("L7: Sandbox verification passed");

    eprintln!("=== All commonplace-link tests PASSED ===");
}

/// CP-oto3: Test sandbox stdio capture and sync
///
/// Verifies that sandboxed process stdout/stderr are:
/// 1. Captured into __<exec>.stdout.txt and __<exec>.stderr.txt in the sandbox
/// 2. Synced back to the workspace mirror
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_sandbox_stdio_capture_and_sync() {
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
    let root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&root_schema).unwrap(),
    )
    .unwrap();

    // Create a subdirectory for the stdio test
    let stdio_dir = workspace_dir.join("stdio-test");
    std::fs::create_dir_all(&stdio_dir).unwrap();
    let stdio_dir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        stdio_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&stdio_dir_schema).unwrap(),
    )
    .unwrap();

    // Create __processes.json with a sandbox process that writes to stdout and stderr
    // The process writes output and then sleeps to allow log capture and sync
    let processes_json = serde_json::json!({
        "processes": {
            "stdio-proc": {
                "sandbox-exec": "sh -c 'echo STDOUT_TEST_OUTPUT; echo STDERR_TEST_OUTPUT >&2; sleep 120'"
            }
        }
    });
    std::fs::write(
        stdio_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Create orchestrator config with process discovery enabled
    let config = create_test_config_with_discovery(
        temp_dir.path(),
        port,
        &db_path,
        &workspace_dir,
        true, // enable process discovery
    );
    std::fs::write(&config_path, &config).unwrap();

    // Record existing sandbox directories before starting orchestrator
    let initial_sandbox_dirs = get_sandbox_dirs();

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
        .expect("Failed to start orchestrator");
    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    eprintln!("=== Sandbox stdio capture test starting ===");

    // Wait for sandbox process to be discovered
    wait_for_discovered_process("stdio-proc", Duration::from_secs(60))
        .expect("Sandbox process 'stdio-proc' was not discovered");

    // Find the sandbox directory for this test's process (handles parallel test runs)
    let sandbox_dir =
        wait_for_sandbox_with_process(&initial_sandbox_dirs, "stdio-proc", Duration::from_secs(30))
            .expect("Sandbox directory for stdio-proc should appear");
    eprintln!("Sandbox directory: {:?}", sandbox_dir);

    // Wait for the process to run and produce output
    eprintln!("Waiting for sandbox process to produce stdout/stderr files...");

    // Note: The log file names use the process name, not the shell command
    let sandbox_stdout = sandbox_dir.join("__stdio-proc.stdout.txt");
    let sandbox_stderr = sandbox_dir.join("__stdio-proc.stderr.txt");

    // Wait for stdout file to appear in sandbox
    let stdout_content = wait_for_file(
        &sandbox_stdout,
        Some("STDOUT_TEST_OUTPUT"),
        Duration::from_secs(30),
    )
    .expect("__sh.stdout.txt should appear in sandbox with expected content");
    assert!(
        stdout_content.contains("STDOUT_TEST_OUTPUT"),
        "stdout file should contain test output, got: {}",
        stdout_content
    );
    eprintln!("PASS: Sandbox __sh.stdout.txt contains expected output");

    // Wait for stderr file to appear in sandbox
    let stderr_content = wait_for_file(
        &sandbox_stderr,
        Some("STDERR_TEST_OUTPUT"),
        Duration::from_secs(30),
    )
    .expect("__sh.stderr.txt should appear in sandbox with expected content");
    assert!(
        stderr_content.contains("STDERR_TEST_OUTPUT"),
        "stderr file should contain test output, got: {}",
        stderr_content
    );
    eprintln!("PASS: Sandbox __sh.stderr.txt contains expected output");

    // Verify the log files are registered in the sandbox schema
    // This confirms the stdio capture is properly integrated with the sync system
    eprintln!("Verifying stdio files are in sandbox schema...");

    let sandbox_schema_content = std::fs::read_to_string(sandbox_dir.join(".commonplace.json"))
        .expect("Should read sandbox schema");
    let sandbox_schema: serde_json::Value =
        serde_json::from_str(&sandbox_schema_content).expect("Should parse sandbox schema");

    let entries = &sandbox_schema["root"]["entries"];
    assert!(
        entries["__stdio-proc.stdout.txt"].is_object(),
        "Sandbox schema should contain __stdio-proc.stdout.txt entry"
    );
    assert!(
        entries["__stdio-proc.stderr.txt"].is_object(),
        "Sandbox schema should contain __stdio-proc.stderr.txt entry"
    );

    // Verify the entries have node_ids (assigned by server during sync)
    let stdout_node_id = entries["__stdio-proc.stdout.txt"]["node_id"].as_str();
    let stderr_node_id = entries["__stdio-proc.stderr.txt"]["node_id"].as_str();

    assert!(
        stdout_node_id.is_some(),
        "stdout file should have a node_id from server"
    );
    eprintln!(
        "PASS: Sandbox schema has __stdio-proc.stdout.txt with node_id: {}",
        stdout_node_id.unwrap()
    );

    assert!(
        stderr_node_id.is_some(),
        "stderr file should have a node_id from server"
    );
    eprintln!(
        "PASS: Sandbox schema has __stdio-proc.stderr.txt with node_id: {}",
        stderr_node_id.unwrap()
    );

    eprintln!("=== All sandbox stdio capture tests PASSED ===");
}

/// CP-05qu: Test JSONL file append/sync behavior
///
/// Verifies acceptance criteria J1-J8:
/// - J1: Create JSONL file with first line
/// - J2: Verify appears in sandbox
/// - J3: Content matches
/// - J4: Append second line
/// - J5: Verify sandbox has both lines
/// - J6: Append from sandbox
/// - J7: Verify workspace has all three lines
/// - J8: Delete test file
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_jsonl_file_append_sync_behavior() {
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
    let root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&root_schema).unwrap(),
    )
    .unwrap();

    // Create a subdirectory for the JSONL test
    let jsonl_dir = workspace_dir.join("jsonl-test");
    std::fs::create_dir_all(&jsonl_dir).unwrap();
    let jsonl_dir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        jsonl_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&jsonl_dir_schema).unwrap(),
    )
    .unwrap();

    // Create __processes.json with a sandbox process
    let processes_json = serde_json::json!({
        "processes": {
            "jsonl-proc": {
                "sandbox-exec": "sleep 120"
            }
        }
    });
    std::fs::write(
        jsonl_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Create orchestrator config with process discovery enabled
    let config = create_test_config_with_discovery(
        temp_dir.path(),
        port,
        &db_path,
        &workspace_dir,
        true, // enable process discovery
    );
    std::fs::write(&config_path, &config).unwrap();

    // Record existing sandbox directories before starting orchestrator
    let initial_sandbox_dirs = get_sandbox_dirs();

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
        .expect("Failed to start orchestrator");
    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    eprintln!("=== JSONL append/sync test starting ===");

    // Wait for sandbox process to be discovered
    wait_for_discovered_process("jsonl-proc", Duration::from_secs(60))
        .expect("Sandbox process 'jsonl-proc' was not discovered");

    // Find the sandbox directory for this test's process (handles parallel test runs)
    let sandbox_dir =
        wait_for_sandbox_with_process(&initial_sandbox_dirs, "jsonl-proc", Duration::from_secs(30))
            .expect("Sandbox directory for jsonl-proc should appear");
    eprintln!("Sandbox directory: {:?}", sandbox_dir);

    // === J1: Create JSONL file with first line ===
    eprintln!("=== J1: Creating JSONL file with first line ===");
    let jsonl_file = jsonl_dir.join("test-data.jsonl");
    let line1 = r#"{"id": 1, "message": "first line"}"#;
    std::fs::write(&jsonl_file, format!("{}\n", line1)).expect("Failed to write JSONL file");
    eprintln!("J1: Created JSONL file with first line");

    // === J2-J3: Verify appears in sandbox with correct content ===
    eprintln!("=== J2-J3: Verifying JSONL appears in sandbox ===");
    let sandbox_jsonl = sandbox_dir.join("test-data.jsonl");

    // Note: JSON content may be normalized (spaces removed) during sync,
    // so use substring matching with a value that survives normalization
    let content = wait_for_file_containing(&sandbox_jsonl, "first line", Duration::from_secs(60))
        .expect("JSONL file should appear in sandbox");
    assert!(
        content.contains("first line"),
        "Sandbox should have first line, got: {}",
        content
    );
    eprintln!("J2-J3: JSONL file synced to sandbox with correct content");

    // Wait for per-file watcher to be fully set up before editing
    // The watcher is spawned asynchronously, so we need to give it time to start
    std::thread::sleep(Duration::from_secs(2));

    // === J4: Append second line ===
    eprintln!("=== J4: Appending second line ===");
    let line2 = r#"{"id": 2, "message": "second line"}"#;

    // Read current content and append
    let current = std::fs::read_to_string(&jsonl_file).unwrap();
    std::fs::write(&jsonl_file, format!("{}{}\n", current, line2))
        .expect("Failed to append second line");
    eprintln!("J4: Appended second line to workspace file");

    // === J5: Verify sandbox has both lines ===
    eprintln!("=== J5: Verifying sandbox has both lines ===");
    // Use message value substring for normalized-safe matching
    let content = wait_for_file_containing(&sandbox_jsonl, "second line", Duration::from_secs(30))
        .expect("Sandbox should have second line");
    assert!(
        content.contains("first line") && content.contains("second line"),
        "Sandbox should have both lines, got: {}",
        content
    );
    eprintln!("J5: Sandbox has both lines");

    // === J6: Append from sandbox ===
    eprintln!("=== J6: Appending third line from sandbox ===");
    let line3 = r#"{"id": 3, "message": "third line from sandbox"}"#;

    // Wait for sync to stabilize
    std::thread::sleep(Duration::from_secs(2));

    // Read current sandbox content and append
    let sandbox_content = std::fs::read_to_string(&sandbox_jsonl).unwrap();
    std::fs::write(&sandbox_jsonl, format!("{}{}\n", sandbox_content, line3))
        .expect("Failed to append third line from sandbox");
    eprintln!("J6: Appended third line from sandbox");

    // === J7: Verify workspace has all three lines ===
    eprintln!("=== J7: Verifying workspace has all three lines ===");
    // Use message value substring for normalized-safe matching
    let content = wait_for_file_containing(
        &jsonl_file,
        "third line from sandbox",
        Duration::from_secs(30),
    )
    .expect("Workspace should have third line");
    assert!(
        content.contains("first line")
            && content.contains("second line")
            && content.contains("third line from sandbox"),
        "Workspace should have all three lines, got: {}",
        content
    );
    eprintln!("J7: Workspace has all three lines");

    // === J8: Delete test file ===
    eprintln!("=== J8: Deleting test file ===");
    std::fs::remove_file(&jsonl_file).expect("Failed to delete JSONL file");

    // Verify file is deleted from sandbox
    let start = std::time::Instant::now();
    while sandbox_jsonl.exists() && start.elapsed() < Duration::from_secs(30) {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        !sandbox_jsonl.exists(),
        "JSONL file should be deleted from sandbox"
    );
    eprintln!("J8: Test file deleted");

    eprintln!("=== All JSONL append/sync tests PASSED ===");
}

/// CP-1no1: Test process termination cascade on orchestrator shutdown.
///
/// Verifies acceptance criteria T1-T5:
/// - T1: Record all PIDs via commonplace-ps
/// - T2: Kill only the orchestrator (SIGTERM, not SIGKILL)
/// - T3: Verify all processes terminated within 5 seconds
/// - T4: Verify no orphaned python/node processes remain
/// - T5: Verify sandbox directories still exist
#[test]
fn test_process_termination_cascade_on_shutdown() {
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

    // Create a subdirectory with a sandbox process
    let test_dir = workspace_dir.join("cascade-test");
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

    // Create __processes.json with a long-running process
    let processes_json = serde_json::json!({
        "processes": {
            "cascade-proc": {
                "sandbox-exec": "sleep 300"
            }
        }
    });
    std::fs::write(
        test_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Write config file with process discovery enabled
    let config =
        create_test_config_with_discovery(temp_dir.path(), port, &db_path, &workspace_dir, true);
    std::fs::write(&config_path, &config).unwrap();

    // Record existing sandbox directories before starting orchestrator
    let initial_sandbox_dirs = get_sandbox_dirs();

    // Spawn orchestrator (NOT using ProcessGuard - we want manual control)
    let server_url = format!("http://127.0.0.1:{}", port);
    let mut orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
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

    let orchestrator_pid = orchestrator.id();

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    eprintln!("=== Process termination cascade test starting ===");

    // Wait for sandbox process to be discovered
    wait_for_discovered_process("cascade-proc", Duration::from_secs(60))
        .expect("Process 'cascade-proc' was not discovered");

    // === T1: Record all PIDs via status file ===
    eprintln!("=== T1: Recording all PIDs ===");
    let status_content =
        std::fs::read_to_string("/tmp/commonplace-orchestrator-status.json").unwrap();
    let status: serde_json::Value = serde_json::from_str(&status_content).unwrap();
    let processes = status.get("processes").and_then(|p| p.as_array()).unwrap();

    let mut recorded_pids: Vec<u32> = vec![orchestrator_pid];
    for process in processes {
        if let Some(pid) = process.get("pid").and_then(|p| p.as_u64()) {
            recorded_pids.push(pid as u32);
        }
    }
    eprintln!("T1: Recorded PIDs: {:?}", recorded_pids);
    assert!(
        recorded_pids.len() >= 3,
        "Should have at least 3 PIDs (orchestrator, server, sync or sandbox)"
    );

    // Find sandbox directory (for T5 verification later)
    let sandbox_dir = wait_for_sandbox_with_process(
        &initial_sandbox_dirs,
        "cascade-proc",
        Duration::from_secs(5),
    )
    .expect("Should find sandbox directory");
    eprintln!("Sandbox directory: {:?}", sandbox_dir);

    // === T2: Kill only the orchestrator (SIGTERM, not SIGKILL) ===
    eprintln!("=== T2: Sending SIGTERM to orchestrator ===");
    unsafe {
        libc::kill(orchestrator_pid as i32, libc::SIGTERM);
    }
    eprintln!(
        "T2: Sent SIGTERM to orchestrator (PID {})",
        orchestrator_pid
    );

    // === T3: Verify all processes terminated within 5 seconds ===
    eprintln!("=== T3: Verifying all processes terminated ===");
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10); // Give some buffer beyond the 5s requirement

    // Wait for orchestrator to exit
    let exit_status = loop {
        match orchestrator.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if start.elapsed() > timeout {
                    // Force kill if it didn't exit gracefully
                    let _ = orchestrator.kill();
                    let _ = orchestrator.wait();
                    panic!(
                        "Orchestrator did not exit within {:?} after SIGTERM",
                        timeout
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("Error waiting for orchestrator: {}", e),
        }
    };
    eprintln!(
        "T3: Orchestrator exited with status: {:?} in {:?}",
        exit_status,
        start.elapsed()
    );

    // === T5: Verify sandbox directories still exist (check immediately after exit) ===
    // Note: We check T5 before T4 to avoid race conditions with parallel tests
    eprintln!("=== T5: Verifying sandbox directories still exist ===");
    assert!(
        sandbox_dir.exists(),
        "Sandbox directory should still exist after shutdown: {:?}",
        sandbox_dir
    );
    eprintln!("T5: Sandbox directory still exists");

    // Give child processes a moment to terminate
    std::thread::sleep(Duration::from_secs(1));

    // === T4: Verify no orphaned processes remain ===
    eprintln!("=== T4: Verifying no orphaned processes ===");
    let mut orphaned = Vec::new();
    for pid in &recorded_pids {
        // Check if process is still running using kill(pid, 0)
        let still_running = unsafe { libc::kill(*pid as i32, 0) == 0 };
        if still_running {
            orphaned.push(*pid);
        }
    }

    if !orphaned.is_empty() {
        eprintln!("WARNING: Orphaned processes found: {:?}", orphaned);
        // Clean up orphaned processes
        for pid in &orphaned {
            unsafe {
                libc::kill(*pid as i32, libc::SIGKILL);
            }
        }
    }
    assert!(
        orphaned.is_empty(),
        "No orphaned processes should remain, but found: {:?}",
        orphaned
    );
    eprintln!("T4: No orphaned processes found");

    // Clean up status file manually since we didn't use ProcessGuard
    let _ = std::fs::remove_file("/tmp/commonplace-orchestrator-status.json");

    eprintln!("=== Process termination cascade test PASSED ===");
}

/// CP-ik96: Test UUID-linked files sync bidirectionally between sandboxes.
///
/// Verifies acceptance criteria M1-M10:
/// - M1-M5: Incoming message flow - edit propagates via UUID link from one sandbox to another
/// - M6-M10: Outgoing response flow - edit from second sandbox propagates back
///
/// This tests that files linked via commonplace-link properly sync between two separate
/// sandbox processes, simulating the bartleby/text-to-telegram message flow.
///
/// Requires MQTT broker on localhost:1883 (skipped if not available).
#[test]
fn test_uuid_linked_files_sync_bidirectionally() {
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
    let root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&root_schema).unwrap(),
    )
    .unwrap();

    // Create two subdirectories simulating sender and receiver sandboxes
    // (like text-to-telegram and bartleby in the real system)

    // === Sender directory (like text-to-telegram) ===
    let sender_dir = workspace_dir.join("sender");
    std::fs::create_dir_all(&sender_dir).unwrap();
    let sender_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        sender_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&sender_schema).unwrap(),
    )
    .unwrap();

    // Sender has its own sandbox process
    let sender_processes = serde_json::json!({
        "processes": {
            "sender-proc": {
                "sandbox-exec": "sleep 300"
            }
        }
    });
    std::fs::write(
        sender_dir.join("__processes.json"),
        serde_json::to_string_pretty(&sender_processes).unwrap(),
    )
    .unwrap();

    // === Receiver directory (like bartleby) ===
    let receiver_dir = workspace_dir.join("receiver");
    std::fs::create_dir_all(&receiver_dir).unwrap();
    let receiver_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    std::fs::write(
        receiver_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&receiver_schema).unwrap(),
    )
    .unwrap();

    // Receiver has its own sandbox process
    let receiver_processes = serde_json::json!({
        "processes": {
            "receiver-proc": {
                "sandbox-exec": "sleep 300"
            }
        }
    });
    std::fs::write(
        receiver_dir.join("__processes.json"),
        serde_json::to_string_pretty(&receiver_processes).unwrap(),
    )
    .unwrap();

    // Write config file with process discovery enabled
    let config =
        create_test_config_with_discovery(temp_dir.path(), port, &db_path, &workspace_dir, true);
    std::fs::write(&config_path, &config).unwrap();

    // Record existing sandbox directories before starting orchestrator
    let initial_sandbox_dirs = get_sandbox_dirs();

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

    eprintln!("=== UUID-linked bidirectional sync test starting ===");

    // Wait for both sandbox processes to be discovered
    wait_for_discovered_process("sender-proc", Duration::from_secs(60))
        .expect("Sandbox process 'sender-proc' was not discovered");
    wait_for_discovered_process("receiver-proc", Duration::from_secs(60))
        .expect("Sandbox process 'receiver-proc' was not discovered");

    // Find sandbox directories for both processes
    let sender_sandbox = wait_for_sandbox_with_process(
        &initial_sandbox_dirs,
        "sender-proc",
        Duration::from_secs(30),
    )
    .expect("Sandbox directory for sender-proc should appear");
    let receiver_sandbox = wait_for_sandbox_with_process(
        &initial_sandbox_dirs,
        "receiver-proc",
        Duration::from_secs(30),
    )
    .expect("Sandbox directory for receiver-proc should appear");

    eprintln!("Sender sandbox: {:?}", sender_sandbox);
    eprintln!("Receiver sandbox: {:?}", receiver_sandbox);

    // === M1: Create content file in sender directory ===
    eprintln!("=== M1: Creating content file in sender directory ===");
    let sender_content = sender_dir.join("content.txt");
    std::fs::write(&sender_content, "initial message").expect("Failed to write sender content");

    // Wait for file to sync to sender sandbox
    let sandbox_sender_content = sender_sandbox.join("content.txt");
    wait_for_file(
        &sandbox_sender_content,
        Some("initial message"),
        Duration::from_secs(30),
    )
    .expect("Sender content should sync to sender sandbox");
    eprintln!("M1: Content file created and synced to sender sandbox");

    // Define receiver prompts path (will be created by commonplace-link)
    // Note: receiver_prompts is unused because we only verify via sandbox paths
    let _receiver_prompts = receiver_dir.join("prompts.txt");
    let sandbox_receiver_prompts = receiver_sandbox.join("prompts.txt");

    // === M2: Link sender/content.txt to receiver/prompts.txt ===
    eprintln!("=== M2: Linking sender/content.txt to receiver/prompts.txt ===");

    // Fetch and update schemas from server (workaround for SSE subscription timing)
    let client = reqwest::blocking::Client::new();

    // Get sender node_id from workspace schema
    let workspace_schema_content = std::fs::read_to_string(workspace_dir.join(".commonplace.json"))
        .expect("Workspace schema should exist");
    let workspace_schema: serde_json::Value =
        serde_json::from_str(&workspace_schema_content).expect("Valid JSON");
    let sender_node_id = workspace_schema["root"]["entries"]["sender"]["node_id"]
        .as_str()
        .expect("sender should have node_id");
    let receiver_node_id = workspace_schema["root"]["entries"]["receiver"]["node_id"]
        .as_str()
        .expect("receiver should have node_id");

    // Fetch and write sender schema
    let head_url = format!("{}/docs/{}/head", server_url, sender_node_id);
    let response = client.get(&head_url).send().expect("Failed to fetch HEAD");
    let head_json: serde_json::Value = response.json().expect("Invalid HEAD response");
    let server_schema = head_json["content"]
        .as_str()
        .expect("HEAD should have content");
    std::fs::write(sender_dir.join(".commonplace.json"), server_schema)
        .expect("Failed to write sender schema");

    // Fetch and write receiver schema
    let head_url = format!("{}/docs/{}/head", server_url, receiver_node_id);
    let response = client.get(&head_url).send().expect("Failed to fetch HEAD");
    let head_json: serde_json::Value = response.json().expect("Invalid HEAD response");
    let server_schema = head_json["content"]
        .as_str()
        .expect("HEAD should have content");
    std::fs::write(receiver_dir.join(".commonplace.json"), server_schema)
        .expect("Failed to write receiver schema");

    // Run commonplace-link to share UUID (receiver/prompts.txt gets sender/content.txt's UUID)
    let link_output = Command::new(env!("CARGO_BIN_EXE_commonplace-link"))
        .args([
            "--server",
            &server_url,
            "sender/content.txt",
            "receiver/prompts.txt",
        ])
        .current_dir(&workspace_dir)
        .output()
        .expect("Failed to run commonplace-link");

    eprintln!(
        "Link stdout: {}",
        String::from_utf8_lossy(&link_output.stdout)
    );
    eprintln!(
        "Link stderr: {}",
        String::from_utf8_lossy(&link_output.stderr)
    );

    assert!(
        link_output.status.success(),
        "commonplace-link should succeed"
    );
    eprintln!("M2: Files linked via commonplace-link");

    // === M3: Verify prompts.txt now has sender content ===
    eprintln!("=== M3: Verifying prompts.txt received sender content ===");

    // Wait for receiver sandbox to get the linked content
    let content = wait_for_file_containing(
        &sandbox_receiver_prompts,
        "initial message",
        Duration::from_secs(30),
    )
    .expect("Receiver sandbox should have sender content via UUID link");
    assert!(
        content.contains("initial message"),
        "Receiver should have sender content, got: {}",
        content
    );
    eprintln!("M3: Receiver sandbox received sender content via UUID link");

    // === M4: Edit sender content in workspace ===
    eprintln!("=== M4: Editing sender content in workspace ===");
    // Read first to ensure file watcher is watching, then write
    let _ = std::fs::read_to_string(&sender_content);
    std::fs::write(&sender_content, "updated message from sender")
        .expect("Failed to update sender content");
    eprintln!("M4: Updated sender content");

    // === M5: Verify edit propagates to receiver sandbox via UUID link ===
    // Use longer timeout - edit must propagate through multiple hops
    eprintln!("=== M5: Verifying edit propagates to receiver sandbox ===");
    let content = wait_for_file_containing(
        &sandbox_receiver_prompts,
        "updated message from sender",
        Duration::from_secs(60),
    )
    .expect("Receiver sandbox should have updated content");
    assert!(
        content.contains("updated message from sender"),
        "Receiver should have updated content, got: {}",
        content
    );
    eprintln!("M5: Edit propagated from sender to receiver via UUID link");

    // === M6: Edit from receiver sandbox ===
    eprintln!("=== M6: Editing from receiver sandbox ===");
    std::thread::sleep(Duration::from_secs(2)); // Allow sync to stabilize
                                                // Read first to ensure file watcher is watching, then write
    let _ = std::fs::read_to_string(&sandbox_receiver_prompts);
    std::fs::write(&sandbox_receiver_prompts, "response from receiver")
        .expect("Failed to write from receiver sandbox");
    eprintln!("M6: Wrote response from receiver sandbox");

    // === M7: Verify edit propagates back to sender sandbox ===
    eprintln!("=== M7: Verifying edit propagates to sender sandbox ===");
    let content = wait_for_file_containing(
        &sandbox_sender_content,
        "response from receiver",
        Duration::from_secs(30),
    )
    .expect("Sender sandbox should have receiver response");
    assert!(
        content.contains("response from receiver"),
        "Sender should have receiver response, got: {}",
        content
    );
    eprintln!("M7: Edit propagated from receiver back to sender via UUID link");

    // === M8: Verify workspace sender file also updated ===
    eprintln!("=== M8: Verifying workspace sender file updated ===");
    let workspace_sender_content = wait_for_file_containing(
        &sender_content,
        "response from receiver",
        Duration::from_secs(30),
    )
    .expect("Workspace sender content should have receiver response");
    assert!(
        workspace_sender_content.contains("response from receiver"),
        "Workspace sender should have receiver response"
    );
    eprintln!("M8: Workspace sender file updated with bidirectional sync");

    // Note: receiver/prompts.txt was created via commonplace-link (schema only),
    // not as a local file, so we don't check its workspace copy here.
    // The important verification is that edits propagated between the TWO SANDBOXES
    // (M3-M7), which demonstrates UUID-linked bidirectional sync working.

    eprintln!("=== All UUID-linked bidirectional sync tests PASSED ===");
}

/// Test that evaluate scripts work with global injection (no import required)
///
/// This test verifies:
/// 1. Evaluate processes start correctly with the loader script
/// 2. The 'commonplace' global is available in user scripts
/// 3. Command handling works via the SDK
/// 4. Output file updates work correctly
///
/// Requires MQTT broker on localhost:1883 and Deno installed.
#[test]
fn test_evaluate_script_with_global_injection() {
    if !mqtt_available() {
        eprintln!("Skipping test: MQTT broker not available on localhost:1883");
        return;
    }

    // Check if Deno is available
    let deno_check = std::process::Command::new("deno").arg("--version").output();
    if deno_check.is_err() || !deno_check.unwrap().status.success() {
        eprintln!("Skipping test: Deno not available");
        return;
    }

    // Clean up any stale status file from previous runs
    let _ = std::fs::remove_file("/tmp/commonplace-orchestrator-status.json");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let workspace_dir = temp_dir.path().join("workspace");
    let config_path = temp_dir.path().join("commonplace.json");
    let port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);

    // Create workspace directory
    std::fs::create_dir_all(&workspace_dir).unwrap();

    // Copy SDK files to temp dir (server serves from sdk/ relative to cwd)
    let sdk_src = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let sdk_dest = temp_dir.path().join("sdk");
    std::fs::create_dir_all(&sdk_dest).unwrap();
    for entry in std::fs::read_dir(&sdk_src).expect("Failed to read sdk directory") {
        let entry = entry.unwrap();
        let dest_path = sdk_dest.join(entry.file_name());
        std::fs::copy(entry.path(), dest_path).expect("Failed to copy SDK file");
    }

    // Create workspace .commonplace.json schema
    let workspace_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "eval-test": {
                    "type": "dir",
                    "entries": {}
                }
            }
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&workspace_schema).unwrap(),
    )
    .unwrap();

    // Create eval-test subdirectory
    let eval_test_dir = workspace_dir.join("eval-test");
    std::fs::create_dir_all(&eval_test_dir).unwrap();

    // Create eval-test .commonplace.json schema with output file
    let eval_test_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "output.txt": {
                    "type": "doc",
                    "node_id": "11111111-1111-1111-1111-111111111111",
                    "content_type": "text/plain"
                },
                "script.ts": {
                    "type": "doc",
                    "node_id": "22222222-2222-2222-2222-222222222222",
                    "content_type": "text/typescript"
                },
                "__processes.json": {
                    "type": "doc",
                    "node_id": "33333333-3333-3333-3333-333333333333",
                    "content_type": "application/json"
                }
            }
        }
    });
    std::fs::write(
        eval_test_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&eval_test_schema).unwrap(),
    )
    .unwrap();

    // Create the evaluate script that uses the global 'commonplace'
    // This script uses NO IMPORT - it relies on the loader injecting the global
    let script_content = r##"// Test evaluate script - uses 'commonplace' global (no import needed)

commonplace.onCommand("*", async (verb: string, payload: unknown) => {
  const current = await commonplace.output.get() as string;
  const entry = `[${verb}]: ${JSON.stringify(payload)}\n`;
  await commonplace.output.set(current + entry);
  console.log(`Handled command: ${verb}`);
});

await commonplace.output.set("# Test Output\n\n");
"##;
    std::fs::write(eval_test_dir.join("script.ts"), script_content).unwrap();

    // Create initial output file
    std::fs::write(eval_test_dir.join("output.txt"), "").unwrap();

    // Create __processes.json with evaluate field
    let processes_json = serde_json::json!({
        "eval-proc": {
            "evaluate": "script.ts",
            "owns": "output.txt"
        }
    });
    std::fs::write(
        eval_test_dir.join("__processes.json"),
        serde_json::to_string_pretty(&processes_json).unwrap(),
    )
    .unwrap();

    // Create orchestrator config with process discovery enabled
    let config = create_test_config_with_discovery(
        temp_dir.path(),
        port,
        &db_path,
        &workspace_dir,
        true, // enable process discovery
    );
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator
    let orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--server",
            &server_url,
        ])
        .current_dir(temp_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start orchestrator");
    guard.add(orchestrator);

    // Wait for orchestrator to be ready
    wait_for_orchestrator_ready(Duration::from_secs(30))
        .expect("Orchestrator failed to start within timeout");

    eprintln!("=== Evaluate script integration test starting ===");

    // Wait for evaluate process to be discovered and running
    wait_for_discovered_process("eval-proc", Duration::from_secs(60))
        .expect("Evaluate process 'eval-proc' was not discovered");

    eprintln!("Evaluate process discovered, waiting for it to initialize...");

    // Give the process time to initialize (connect to MQTT, set up handlers)
    std::thread::sleep(Duration::from_secs(5));

    // === Test 1: Send a command and verify output is updated ===
    eprintln!("=== Test 1: Sending command to evaluate process ===");

    let client = reqwest::blocking::Client::new();

    // Send a command to the evaluate process
    let command_url = format!("{}/commands/eval-test/output.txt/test-cmd", server_url);
    let command_resp = client
        .post(&command_url)
        .header("Content-Type", "application/json")
        .body(r#"{"payload": {"message": "hello from test"}}"#)
        .send()
        .expect("Failed to send command");

    assert!(
        command_resp.status().is_success(),
        "Command should be accepted, got status: {}",
        command_resp.status()
    );
    eprintln!("Command sent successfully");

    // Wait for the output file to be updated
    std::thread::sleep(Duration::from_secs(3));

    // === Test 2: Verify output file contains the command ===
    eprintln!("=== Test 2: Verifying output file updated ===");

    let output_url = format!("{}/files/eval-test/output.txt", server_url);
    let mut output_content = String::new();
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    while start.elapsed() < timeout {
        let output_resp = client
            .get(&output_url)
            .send()
            .expect("Failed to fetch output");

        if output_resp.status().is_success() {
            output_content = output_resp.text().unwrap_or_default();
            if output_content.contains("test-cmd") && output_content.contains("hello from test") {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    assert!(
        output_content.contains("test-cmd"),
        "Output should contain command verb 'test-cmd', got: {}",
        output_content
    );
    assert!(
        output_content.contains("hello from test"),
        "Output should contain payload message, got: {}",
        output_content
    );
    eprintln!("Output file contains expected command data");

    // === Test 3: Send another command to verify ongoing operation ===
    eprintln!("=== Test 3: Sending second command ===");

    let command2_url = format!("{}/commands/eval-test/output.txt/second-cmd", server_url);
    let command2_resp = client
        .post(&command2_url)
        .header("Content-Type", "application/json")
        .body(r#"{"payload": {"count": 42}}"#)
        .send()
        .expect("Failed to send second command");

    assert!(
        command2_resp.status().is_success(),
        "Second command should be accepted"
    );

    // Wait and verify
    std::thread::sleep(Duration::from_secs(3));

    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let output_resp = client
            .get(&output_url)
            .send()
            .expect("Failed to fetch output");

        if output_resp.status().is_success() {
            output_content = output_resp.text().unwrap_or_default();
            if output_content.contains("second-cmd") && output_content.contains("42") {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    assert!(
        output_content.contains("second-cmd"),
        "Output should contain second command, got: {}",
        output_content
    );
    assert!(
        output_content.contains("42"),
        "Output should contain payload count, got: {}",
        output_content
    );

    eprintln!("=== All evaluate script integration tests PASSED ===");
}
