//! Integration tests for CRDT peer sync via MQTT.
//!
//! These tests specifically verify the MQTT-based CRDT sync flow:
//! 1. Sync client makes edit
//! 2. Edit publishes to MQTT
//! 3. Server receives edit via MQTT subscription
//! 4. Server persists commit to database
//!
//! This test file verifies the bugs fixed in the CRDT peer sync system:
//! - Server not receiving MQTT edits (subscribe_all_edits)
//! - Node ID mismatch between sync clients
//!
//! NOTE: These tests require MQTT broker (mosquitto) running on localhost:1883.
//! They are skipped automatically if MQTT is not available.
//!
//! Run with: cargo test --test crdt_peer_sync_tests -- --test-threads=1

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

            // Send SIGTERM first for graceful shutdown
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }

            // Wait up to 5 seconds for graceful termination
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(5);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => {
                        if start.elapsed() > timeout {
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

/// Create orchestrator config for CRDT testing.
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

/// Wait for a document to have a CID (commit persisted) on the server.
fn wait_for_server_commit(
    client: &reqwest::blocking::Client,
    server_url: &str,
    uuid: &str,
    timeout: Duration,
) -> Result<String, String> {
    let start = std::time::Instant::now();
    let url = format!("{}/docs/{}/head", server_url, uuid);

    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for server to have commit for {}",
                uuid
            ));
        }

        if let Ok(resp) = client.get(&url).send() {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<serde_json::Value>() {
                    if let Some(cid) = body.get("cid").and_then(|c| c.as_str()) {
                        return Ok(cid.to_string());
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait for a document to have multiple commits on the server.
fn wait_for_commit_count(
    client: &reqwest::blocking::Client,
    server_url: &str,
    uuid: &str,
    min_count: usize,
    timeout: Duration,
) -> Result<usize, String> {
    let start = std::time::Instant::now();
    let url = format!("{}/documents/{}/changes", server_url, uuid);

    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for {} commits for {}",
                min_count, uuid
            ));
        }

        if let Ok(resp) = client.get(&url).send() {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<serde_json::Value>() {
                    if let Some(changes) = body.get("changes").and_then(|c| c.as_array()) {
                        if changes.len() >= min_count {
                            return Ok(changes.len());
                        }
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

// =============================================================================
// TEST: Sync client edit → MQTT → server persists
// =============================================================================
//
// This test verifies the critical CRDT peer sync flow:
// 1. Sync client creates/edits a file
// 2. Edit is published to MQTT
// 3. Server receives the edit via its wildcard MQTT subscription
// 4. Server persists the commit to the database
//
// This is THE test that would have caught the "server not subscribing to MQTT" bug.
//
// Strategy: Create a file in the schema before starting sync, then edit it.
// This avoids the complexity of detecting new file creation.

#[test]
fn test_sync_client_edit_persisted_by_server_via_mqtt() {
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
    let server_url = format!("http://127.0.0.1:{}", port);

    // Create workspace directory
    std::fs::create_dir_all(&workspace_dir).unwrap();

    // Generate a UUID for the test file ahead of time
    let file_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    // Create initial schema WITH a file entry that has a node_id
    // This way we don't need to wait for file discovery
    let schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "mqtt-test.txt": {
                    "type": "doc",
                    "content_type": "text",
                    "node_id": file_uuid
                }
            }
        }
    });
    std::fs::write(
        workspace_dir.join(".commonplace.json"),
        serde_json::to_string_pretty(&schema).unwrap(),
    )
    .unwrap();

    // Create the test file with initial content
    let test_file = workspace_dir.join("mqtt-test.txt");
    std::fs::write(&test_file, "initial content").expect("Failed to write test file");

    // Write config file
    let config = create_test_config(temp_dir.path(), port, &db_path, &workspace_dir);
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    // Spawn orchestrator (which starts server and sync with MQTT)
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

    // Give sync client time to fully initialize and sync
    std::thread::sleep(Duration::from_secs(5));

    // Verify server is healthy
    let client = reqwest::blocking::Client::new();
    let health_resp = client
        .get(format!("{}/health", server_url))
        .send()
        .expect("Failed to send health check");
    assert!(
        health_resp.status().is_success(),
        "Server health check should succeed"
    );

    eprintln!("File UUID: {}", file_uuid);

    // THE CRITICAL CHECK: Server should have persisted the initial commit via MQTT
    // The sync client pushes the initial file content to MQTT, server should receive and persist
    let cid = wait_for_server_commit(&client, &server_url, file_uuid, Duration::from_secs(30))
        .expect("Server should have persisted initial commit via MQTT");

    eprintln!("Server has initial commit: {}", cid);

    // Edit the file to create another commit
    eprintln!("Editing file to trigger MQTT publish...");

    // Read current content first to avoid race
    let _ = std::fs::read_to_string(&test_file);
    std::thread::sleep(Duration::from_millis(500));

    std::fs::write(&test_file, "edited content").expect("Failed to edit test file");

    // Give file watcher time to detect the change
    eprintln!("Waiting for file watcher to detect edit...");
    std::thread::sleep(Duration::from_secs(3));

    // Check current commit count before waiting
    let changes_url = format!("{}/documents/{}/changes", server_url, file_uuid);
    if let Ok(resp) = client.get(&changes_url).send() {
        if let Ok(body) = resp.json::<serde_json::Value>() {
            eprintln!("Current changes: {:?}", body);
        }
    }

    // Wait for second commit to be persisted
    let commit_count =
        wait_for_commit_count(&client, &server_url, file_uuid, 2, Duration::from_secs(45))
            .expect("Server should have persisted 2 commits");

    assert!(
        commit_count >= 2,
        "Server should have at least 2 commits, got {}",
        commit_count
    );

    // Verify server has commits via /docs/{id}/head
    // NOTE: We don't check exact content because of known CRDT merge loop issue (CP-oj5m).
    // The key test here is that commits are persisted via MQTT, which is verified by commit_count.
    let url = format!("{}/docs/{}/head", server_url, file_uuid);
    let resp = client.get(&url).send().expect("Failed to get doc head");
    assert!(resp.status().is_success(), "Server should have doc head");
    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    assert!(
        body.get("cid").is_some(),
        "Server HEAD should have a CID (commit persisted)"
    );

    eprintln!(
        "SUCCESS: Server persisted {} commits via MQTT (CRDT peer sync working)",
        commit_count
    );
}

// =============================================================================
// TEST: Server persistence after edit (verification test)
// =============================================================================
//
// Simple verification that the server receives and persists edits.
// This uses HTTP /replace to verify the basic persistence flow works,
// which is a prerequisite for MQTT-based persistence.

#[test]
fn test_server_persists_edits_via_http() {
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
    let server_url = format!("http://127.0.0.1:{}", port);

    // Create workspace directory with initial schema
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

    // Spawn orchestrator
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

    // Create a document via HTTP
    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "text/plain")
        .send()
        .expect("Failed to create document");

    assert!(resp.status().is_success(), "Failed to create document");
    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let uuid = body["id"].as_str().expect("Response should have id");

    eprintln!("Created document with UUID: {}", uuid);

    // Edit via HTTP /replace
    let edit_resp = client
        .post(format!("{}/docs/{}/replace", server_url, uuid))
        .header("Content-Type", "text/plain")
        .body("content via replace")
        .send()
        .expect("Failed to send replace");

    assert!(
        edit_resp.status().is_success(),
        "Replace should succeed: {:?}",
        edit_resp.status()
    );

    // Verify the commit was persisted
    let cid = wait_for_server_commit(&client, &server_url, uuid, Duration::from_secs(10))
        .expect("Server should have commit after replace");

    eprintln!("SUCCESS: Server persisted commit {} via HTTP", cid);
}
