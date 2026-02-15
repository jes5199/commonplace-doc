//! Integration tests for server + sync client.
//!
//! These tests spawn actual processes and verify end-to-end behavior.

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

/// Helper to spawn mosquitto MQTT broker on a given port
fn spawn_mqtt_broker(port: u16) -> Child {
    let child = Command::new("mosquitto")
        .args(["-p", &port.to_string(), "-v"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn mosquitto");

    // Wait for mosquitto to be ready by checking if port is open
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Mosquitto failed to start within 5 seconds");
        }
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    child
}

/// Helper to spawn the server and wait for it to be ready
fn spawn_server(port: u16, db_path: &std::path::Path) -> Child {
    spawn_server_with_mqtt(port, db_path, None)
}

fn spawn_server_with_mqtt(
    port: u16,
    db_path: &std::path::Path,
    mqtt_broker: Option<&str>,
) -> Child {
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--database".to_string(),
        db_path.to_str().unwrap().to_string(),
    ];

    if let Some(broker) = mqtt_broker {
        args.push("--mqtt-broker".to_string());
        args.push(broker.to_string());
    }

    let child = Command::new(env!("CARGO_BIN_EXE_commonplace-server"))
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn server");

    // Wait for server to be ready
    let client = reqwest::blocking::Client::new();
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Server failed to start within 10 seconds");
        }
        match client
            .get(format!("http://127.0.0.1:{}/health", port))
            .send()
        {
            Ok(resp) if resp.status().is_success() => break,
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }

    child
}

/// Helper to spawn sync client in file mode
fn spawn_sync_file(
    server_url: &str,
    mqtt_broker: &str,
    file_path: &std::path::Path,
    node_id: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_commonplace-sync"))
        .args([
            "--server",
            server_url,
            "--mqtt-broker",
            mqtt_broker,
            "--node",
            node_id,
            "--file",
            file_path.to_str().unwrap(),
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn sync client")
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
    }
}

#[test]
fn test_server_starts_and_responds_to_health_check() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let port = get_available_port();

    let mut guard = ProcessGuard::new();
    let server = spawn_server(port, &db_path);
    guard.add(server);

    // If we got here, the server started and responded to health check
    // (spawn_server waits for health check)
}

#[test]
fn test_create_document_via_http() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let port = get_available_port();

    let mut guard = ProcessGuard::new();
    let server = spawn_server(port, &db_path);
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create a document
    let resp = client
        .post(format!("http://127.0.0.1:{}/docs", port))
        .header("Content-Type", "text/plain")
        .send()
        .expect("Failed to create document");

    assert!(resp.status().is_success(), "Failed to create document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    assert!(
        body["id"].is_string(),
        "Response should contain document id"
    );
}

#[test]
fn test_sync_client_pulls_content_to_local_file() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_file = temp_dir.path().join("synced.txt");
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create a document with content
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "text/plain")
        .send()
        .expect("Failed to create document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let doc_id = body["id"].as_str().expect("No id in response");

    // Set content via replace
    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, doc_id))
        .header("Content-Type", "text/plain")
        .body("Hello from server!")
        .send()
        .expect("Failed to replace content");

    assert!(resp.status().is_success(), "Failed to replace content");

    // Start sync client pointing at this document
    let sync = spawn_sync_file(&server_url, &mqtt_broker, &sync_file, doc_id);
    guard.add(sync);

    // Wait for sync to create file (give it a few seconds)
    let start = std::time::Instant::now();
    while !sync_file.exists() && start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(100));
    }

    // Check file was created with correct content
    // Note: sync adds trailing newline to text files
    assert!(sync_file.exists(), "Sync should create the file");
    let content = std::fs::read_to_string(&sync_file).expect("Failed to read file");
    assert_eq!(content.trim(), "Hello from server!", "Content should match");
}

/// CP-v5f7: Test that local file content survives sync client restart when server
/// has only default/empty content.
///
/// Scenario:
/// 1. Create a document on server, sync to local file
/// 2. Kill sync client
/// 3. Edit local file with substantive content
/// 4. Wipe server content to default ("{}" for JSON)
/// 5. Restart sync client
/// 6. Verify local content is preserved (not overwritten by server's "{}")
#[test]
fn test_local_content_survives_restart_with_default_server_content() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_file = temp_dir.path().join("data.json");
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create a JSON document
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .send()
        .expect("Failed to create document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let doc_id = body["id"].as_str().expect("No id in response");

    // Set initial content on server
    let initial_content = r#"{"status": "initial"}"#;
    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, doc_id))
        .header("Content-Type", "application/json")
        .body(initial_content)
        .send()
        .expect("Failed to replace content");
    assert!(resp.status().is_success(), "Failed to replace content");

    // Start sync client - it should pull the initial content
    let sync = spawn_sync_file(&server_url, &mqtt_broker, &sync_file, doc_id);
    guard.add(sync);

    // Wait for sync to create file
    let start = std::time::Instant::now();
    while !sync_file.exists() && start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(sync_file.exists(), "Sync should create the file");

    // Wait a bit more for content to settle
    std::thread::sleep(Duration::from_millis(500));

    // Kill the sync client (remove from guard and kill it)
    let mut sync_child = guard.children.pop().unwrap();
    let _ = sync_child.kill();
    let _ = sync_child.wait();

    // Now edit the local file with substantive content
    let local_content = r#"{"status": "edited_locally", "important_data": true}"#;
    std::fs::write(&sync_file, local_content).expect("Failed to write local file");

    // Wipe server content back to default (empty JSON object)
    // This simulates the bug scenario where server has lost the content
    // First, get the current HEAD CID (required for replace)
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, doc_id))
        .send()
        .expect("Failed to get HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let parent_cid = head["cid"].as_str().expect("No cid in HEAD");

    let resp = client
        .post(format!(
            "{}/docs/{}/replace?parent_cid={}",
            server_url, doc_id, parent_cid
        ))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to replace content");
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    assert!(
        status.is_success(),
        "Failed to replace to default: {} - {}",
        status,
        body
    );

    // Restart sync client - with the fix, it should NOT overwrite local content
    let sync = spawn_sync_file(&server_url, &mqtt_broker, &sync_file, doc_id);
    guard.add(sync);

    // Wait for sync client to connect and potentially process server content
    std::thread::sleep(Duration::from_secs(2));

    // Verify local content is preserved (not overwritten by "{}")
    let final_content = std::fs::read_to_string(&sync_file).expect("Failed to read file");
    assert!(
        final_content.contains("important_data"),
        "Local content should be preserved, not overwritten by server's default. Got: {}",
        final_content
    );
    assert!(
        final_content.contains("edited_locally"),
        "Local edit should survive. Got: {}",
        final_content
    );
}

/// CP-fbk3: Test that per-file CIDs are persisted and loaded across sync restarts.
///
/// Scenario:
/// 1. Create a document on server, sync to local file
/// 2. Gracefully stop sync client (SIGTERM so state file is saved)
/// 3. Verify state file exists and contains the CID
/// 4. Make a local edit while sync is stopped
/// 5. Restart sync client
/// 6. Verify local edit is pushed to server (ancestry check using persisted CID)
#[test]
fn test_cid_persistence_survives_sync_restart() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_file = temp_dir.path().join("data.txt");
    // DirectorySyncState is stored per-directory, not per-file
    let state_file = temp_dir.path().join(".commonplace-sync.json");
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create a text document
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "text/plain")
        .send()
        .expect("Failed to create document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let doc_id = body["id"].as_str().expect("No id in response");

    // Set initial content on server
    let initial_content = "Initial server content";
    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, doc_id))
        .header("Content-Type", "text/plain")
        .body(initial_content)
        .send()
        .expect("Failed to replace content");
    assert!(resp.status().is_success(), "Failed to replace content");

    // Start sync client - it should pull the initial content
    let mut sync = spawn_sync_file(&server_url, &mqtt_broker, &sync_file, doc_id);

    // Wait for sync to create file
    let start = std::time::Instant::now();
    while !sync_file.exists() && start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(sync_file.exists(), "Sync should create the file");

    // Wait a bit more for sync to establish and CID to be tracked
    std::thread::sleep(Duration::from_millis(1000));

    // Gracefully stop the sync client with SIGTERM (so it saves state file)
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(sync.id() as i32, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = sync.kill();
    }
    let _ = sync.wait();

    // Verify state file was created
    assert!(
        state_file.exists(),
        "State file should exist after graceful shutdown: {}",
        state_file.display()
    );

    // Read and verify state file contains CID (DirectorySyncState format)
    let state_content = std::fs::read_to_string(&state_file).expect("Failed to read state file");
    let state: serde_json::Value =
        serde_json::from_str(&state_content).expect("Failed to parse state file");

    // DirectorySyncState has: version, schema, files
    assert!(
        state["version"].is_number(),
        "State file should contain version: {}",
        state_content
    );

    // The files map should have an entry for data.txt with a head_cid
    let files = &state["files"];
    assert!(
        files.is_object() && !files.as_object().unwrap().is_empty(),
        "State file should have files map: {}",
        state_content
    );

    let file_entry = &files["data.txt"];
    assert!(
        file_entry.is_object(),
        "State file should have entry for data.txt: {}",
        state_content
    );

    assert!(
        file_entry["head_cid"].is_string(),
        "File entry should have head_cid: {}",
        state_content
    );

    // Make a local edit while sync is stopped
    let edited_content = "Edited locally while sync was stopped";
    std::fs::write(&sync_file, edited_content).expect("Failed to write local file");

    // Restart sync client
    let sync2 = spawn_sync_file(&server_url, &mqtt_broker, &sync_file, doc_id);
    guard.add(sync2);

    // Wait for sync to process and push the local edit
    std::thread::sleep(Duration::from_secs(2));

    // Verify server received the local edit
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, doc_id))
        .send()
        .expect("Failed to get HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let server_content = head["content"].as_str().expect("No content in HEAD");

    assert!(
        server_content.contains("Edited locally"),
        "Server should have received local edit. Server content: {}",
        server_content
    );
}

/// Helper to spawn sync client in directory mode
fn spawn_sync_directory(
    server_url: &str,
    mqtt_broker: &str,
    directory: &std::path::Path,
    node_id: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_commonplace-sync"))
        .args([
            "--server",
            server_url,
            "--mqtt-broker",
            mqtt_broker,
            "--node",
            node_id,
            "--directory",
            directory.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn sync client")
}

/// Spawn sync client with stderr inherited for debugging
#[allow(dead_code)]
fn spawn_sync_directory_debug(
    server_url: &str,
    mqtt_broker: &str,
    directory: &std::path::Path,
    node_id: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_commonplace-sync"))
        .args([
            "--server",
            server_url,
            "--mqtt-broker",
            mqtt_broker,
            "--node",
            node_id,
            "--directory",
            directory.to_str().unwrap(),
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to spawn sync client")
}

/// CP-q52x: Test that newly created node-backed subdirectories propagate without restart.
///
/// Scenario:
/// 1. Create a fs-root document on server with initial schema
/// 2. Start sync client watching the directory
/// 3. Create a new node-backed subdirectory via HTTP API
/// 4. Verify sync client discovers and watches the new subdir (via SSE)
/// 5. Create a file in the subdirectory
/// 6. Verify the file appears locally without restarting sync
#[test]
#[ignore = "CP-5dii: times out on CI"]
fn test_node_backed_subdir_propagates_without_restart() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_dir = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&sync_dir).unwrap();
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create fs-root document with initial schema (empty directory)
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create fs-root document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let fs_root_id = body["id"].as_str().expect("No id in response");

    // Start sync client in directory mode
    let sync = spawn_sync_directory(&server_url, &mqtt_broker, &sync_dir, fs_root_id);
    guard.add(sync);

    // Wait for initial sync to complete
    std::thread::sleep(Duration::from_secs(2));

    // Create a node-backed subdirectory by updating the schema
    // First, create a document for the subdir
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create subdir document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let subdir_id = body["id"].as_str().expect("No id in response");

    // Update fs-root schema to include the node-backed subdirectory
    // First fetch HEAD to get parent CID
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, fs_root_id))
        .send()
        .expect("Failed to get fs-root HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let parent_cid = head["cid"].as_str().expect("No cid in HEAD");

    let schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "newsubdir": {
                    "type": "dir",
                    "node_id": subdir_id
                }
            }
        }
    });

    let resp = client
        .post(format!(
            "{}/docs/{}/replace?parent_cid={}",
            server_url, fs_root_id, parent_cid
        ))
        .header("Content-Type", "application/json")
        .body(schema.to_string())
        .send()
        .expect("Failed to update fs-root schema");
    assert!(
        resp.status().is_success(),
        "Failed to update schema: {}",
        resp.text().unwrap_or_default()
    );

    // Wait for sync to discover the new subdir via SSE
    std::thread::sleep(Duration::from_secs(2));

    // Verify the subdirectory was created locally
    let subdir_path = sync_dir.join("newsubdir");
    assert!(
        subdir_path.exists(),
        "Subdirectory should be created locally: {}",
        subdir_path.display()
    );

    // Create a file in the subdirectory on the server
    let file_content = "Hello from new subdir!";

    // Create the file document
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "text/plain")
        .send()
        .expect("Failed to create file document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let file_id = body["id"].as_str().expect("No id in response");

    // Set file content - for new documents with no commits, we don't need parent_cid
    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, file_id))
        .header("Content-Type", "text/plain")
        .body(file_content)
        .send()
        .expect("Failed to set file content");
    assert!(
        resp.status().is_success(),
        "Failed to set file content: {}",
        resp.text().unwrap_or_default()
    );

    // Update subdir schema with file's node_id - subdir is new, no parent_cid needed
    let file_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "test.txt": {
                    "type": "doc",
                    "node_id": file_id
                }
            }
        }
    });

    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, subdir_id))
        .header("Content-Type", "application/json")
        .body(file_schema.to_string())
        .send()
        .expect("Failed to update subdir schema with file");
    assert!(
        resp.status().is_success(),
        "Failed to update subdir schema: {}",
        resp.text().unwrap_or_default()
    );

    // Wait for sync to propagate the file
    std::thread::sleep(Duration::from_secs(3));

    // Verify the file was created locally
    let file_path = subdir_path.join("test.txt");
    assert!(
        file_path.exists(),
        "File should be created in subdirectory: {}",
        file_path.display()
    );

    let local_content = std::fs::read_to_string(&file_path).expect("Failed to read file");
    assert!(
        local_content.contains("Hello from new subdir"),
        "File content should match. Got: {}",
        local_content
    );
}

/// CP-k51z: Test that offline edits sync on reconnect.
///
/// Scenario (acceptance criteria O1-O5):
/// O1: Stop workspace sync process
/// O2: Edit file locally while sync is down
/// O3: Verify server content has NOT changed
/// O4: Restart sync process
/// O5: Verify server content now includes local edit
#[test]
#[ignore = "CP-5dii: times out on CI"]
fn test_offline_edits_sync_on_reconnect() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_dir = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&sync_dir).unwrap();
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create fs-root document with a file
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create fs-root document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let fs_root_id = body["id"].as_str().expect("No id in response");

    // Create a file document with initial content
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "text/plain")
        .send()
        .expect("Failed to create file document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let file_id = body["id"].as_str().expect("No id in response");

    // Set initial file content
    let initial_content = "Initial server content";
    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, file_id))
        .header("Content-Type", "text/plain")
        .body(initial_content)
        .send()
        .expect("Failed to set file content");
    assert!(resp.status().is_success());

    // Update fs-root schema to include the file
    // fs-root is new, so no parent_cid needed
    let schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "notes.txt": {
                    "type": "doc",
                    "node_id": file_id
                }
            }
        }
    });

    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, fs_root_id))
        .header("Content-Type", "application/json")
        .body(schema.to_string())
        .send()
        .expect("Failed to update fs-root schema");
    assert!(resp.status().is_success());

    // Start sync client in directory mode
    let mut sync = spawn_sync_directory(&server_url, &mqtt_broker, &sync_dir, fs_root_id);

    // Wait for initial sync to complete
    std::thread::sleep(Duration::from_secs(2));

    // Verify local file was created with initial content
    let local_file = sync_dir.join("notes.txt");
    assert!(
        local_file.exists(),
        "Local file should exist after initial sync"
    );
    let content = std::fs::read_to_string(&local_file).expect("Failed to read local file");
    assert!(
        content.contains("Initial server content"),
        "Local file should have initial content. Got: {}",
        content
    );

    // O1: Stop sync process
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(sync.id() as i32, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = sync.kill();
    }
    let _ = sync.wait();

    // Wait for sync to fully stop
    std::thread::sleep(Duration::from_millis(500));

    // O2: Edit file locally while sync is down
    let offline_edit = "Edited while offline - important changes!";
    std::fs::write(&local_file, offline_edit).expect("Failed to write local file");

    // O3: Verify server content has NOT changed
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, file_id))
        .send()
        .expect("Failed to get file HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let server_content = head["content"].as_str().expect("No content in HEAD");
    assert!(
        server_content.contains("Initial server content"),
        "Server should still have initial content while sync is down. Got: {}",
        server_content
    );
    assert!(
        !server_content.contains("offline"),
        "Server should NOT have offline edit yet. Got: {}",
        server_content
    );

    // O4: Restart sync process
    let sync = spawn_sync_directory(&server_url, &mqtt_broker, &sync_dir, fs_root_id);
    guard.add(sync);

    // Wait for sync to reconnect and push local changes
    std::thread::sleep(Duration::from_secs(3));

    // O5: Verify server content now includes local edit
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, file_id))
        .send()
        .expect("Failed to get file HEAD after restart");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let server_content = head["content"].as_str().expect("No content in HEAD");
    assert!(
        server_content.contains("Edited while offline"),
        "Server should have received offline edit after sync restart. Got: {}",
        server_content
    );
}

/// CP-7dvs: Test that locally created files in existing subdirectories update server schema.
///
/// Scenario:
/// 1. Create a fs-root with a node-backed subdirectory on server
/// 2. Start sync client
/// 3. Create a new file locally in the subdirectory
/// 4. Verify the subdirectory schema on server includes the new file entry
#[test]
#[ignore = "CP-5dii: times out on CI"]
fn test_local_file_in_existing_subdir_updates_server_schema() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_dir = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&sync_dir).unwrap();
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create fs-root document
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create fs-root document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let fs_root_id = body["id"].as_str().expect("No id in response");

    // Create a document for the subdirectory
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create subdir document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let subdir_id = body["id"].as_str().expect("No id in response");

    // Initialize the subdirectory with an empty schema
    let subdir_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, subdir_id))
        .header("Content-Type", "application/json")
        .body(subdir_schema.to_string())
        .send()
        .expect("Failed to set subdir schema");
    assert!(resp.status().is_success());

    // Update fs-root schema to include the node-backed subdirectory
    let schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "mysubdir": {
                    "type": "dir",
                    "node_id": subdir_id
                }
            }
        }
    });

    let resp = client
        .post(format!("{}/docs/{}/replace", server_url, fs_root_id))
        .header("Content-Type", "application/json")
        .body(schema.to_string())
        .send()
        .expect("Failed to update fs-root schema");
    assert!(resp.status().is_success());

    // Start sync client
    let sync = spawn_sync_directory_debug(&server_url, &mqtt_broker, &sync_dir, fs_root_id);
    guard.add(sync);

    // Wait for initial sync and watcher setup
    std::thread::sleep(Duration::from_secs(15));

    // Verify subdirectory was created locally
    let subdir_path = sync_dir.join("mysubdir");
    assert!(
        subdir_path.exists(),
        "Subdirectory should be created locally"
    );

    // Create a new file locally in the subdirectory
    let new_file_path = subdir_path.join("newfile.txt");
    std::fs::write(&new_file_path, "Hello from local!").expect("Failed to write local file");

    // Wait for sync to push the file and update schema
    std::thread::sleep(Duration::from_secs(5));

    // Verify: subdirectory schema on server should include the new file
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, subdir_id))
        .send()
        .expect("Failed to get subdir HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let schema_content = head["content"].as_str().expect("No content in HEAD");
    let schema: serde_json::Value =
        serde_json::from_str(schema_content).expect("Failed to parse schema");

    // Check that entries contains newfile.txt with a node_id
    let entries = schema["root"]["entries"]
        .as_object()
        .expect("Schema should have entries object");
    assert!(
        entries.contains_key("newfile.txt"),
        "Server schema should contain newfile.txt entry. Schema: {}",
        schema_content
    );

    let file_entry = &entries["newfile.txt"];
    assert!(
        file_entry["node_id"].is_string(),
        "File entry should have node_id. Entry: {}",
        file_entry
    );

    // Verify the file document exists and has correct content
    let file_node_id = file_entry["node_id"].as_str().unwrap();
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, file_node_id))
        .send()
        .expect("Failed to get file HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let file_content = head["content"].as_str().expect("No content in HEAD");
    assert!(
        file_content.contains("Hello from local"),
        "Server file content should match local. Got: {}",
        file_content
    );
}

/// CP-7dvs: Test that locally created subdirectory with file updates server schemas.
///
/// Scenario:
/// 1. Create a fs-root on server (empty)
/// 2. Start sync client
/// 3. Create a new subdirectory locally with a file inside
/// 4. Verify:
///    - fs-root schema includes the new subdirectory with node_id
///    - subdirectory schema includes the file with node_id
///    - file document has correct content
#[test]
#[ignore = "CP-5dii: times out on CI"]
fn test_local_new_subdir_with_file_updates_server_schemas() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_dir = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&sync_dir).unwrap();
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create fs-root document (empty schema)
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create fs-root document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let fs_root_id = body["id"].as_str().expect("No id in response");

    // Start sync client (using debug version to see logs)
    let sync = spawn_sync_directory_debug(&server_url, &mqtt_broker, &sync_dir, fs_root_id);
    guard.add(sync);

    // Wait for initial sync to complete and watcher to start
    // This needs to be long enough for the sync client to:
    // 1. Start up and parse args
    // 2. Acquire sync lock
    // 3. Run initial sync (sync_schema, scan_directory_with_contents, etc.)
    //    Note: initial sync includes a 30-attempt retry loop (3+ seconds) for files
    // 4. Start the watcher event loop
    // Only THEN should we create files so they go through the watcher path
    // We need at least 10 seconds to ensure watcher is started before file creation
    std::thread::sleep(Duration::from_secs(15));

    // Create a new subdirectory locally
    let new_subdir = sync_dir.join("brandnew");
    std::fs::create_dir_all(&new_subdir).expect("Failed to create subdirectory");

    // Wait a bit for the watcher to register the new subdirectory
    // The notify crate needs time to add the new directory to its watch list
    std::thread::sleep(Duration::from_millis(500));

    // Create a file in the new subdirectory
    let new_file = new_subdir.join("test.txt");
    std::fs::write(&new_file, "Created locally in new subdir!").expect("Failed to write file");

    // Wait for sync to:
    // 1. Create subdir document on server
    // 2. Update fs-root schema with subdir entry
    // 3. Create file document on server
    // 4. Update subdir schema with file entry
    std::thread::sleep(Duration::from_secs(5));

    // Verify: directory and file still exist locally (regression test for CP-4s24)
    assert!(
        new_subdir.exists(),
        "Sync should NOT delete newly created directories"
    );
    assert!(
        new_file.exists(),
        "Sync should NOT delete files in newly created directories"
    );

    // Verify: fs-root schema should include the new subdirectory
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, fs_root_id))
        .send()
        .expect("Failed to get fs-root HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let schema_content = head["content"].as_str().expect("No content in HEAD");
    let schema: serde_json::Value =
        serde_json::from_str(schema_content).expect("Failed to parse fs-root schema");

    let entries = schema["root"]["entries"]
        .as_object()
        .expect("fs-root schema should have entries");
    assert!(
        entries.contains_key("brandnew"),
        "fs-root schema should contain brandnew subdir. Schema: {}",
        schema_content
    );

    let subdir_entry = &entries["brandnew"];
    assert!(
        subdir_entry["node_id"].is_string(),
        "Subdir entry should have node_id. Entry: {}",
        subdir_entry
    );

    let subdir_node_id = subdir_entry["node_id"].as_str().unwrap();

    // Verify: subdirectory schema should include the file
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, subdir_node_id))
        .send()
        .expect("Failed to get subdir HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let subdir_schema_content = head["content"].as_str().expect("No content in subdir HEAD");
    let subdir_schema: serde_json::Value =
        serde_json::from_str(subdir_schema_content).expect("Failed to parse subdir schema");

    let subdir_entries = subdir_schema["root"]["entries"]
        .as_object()
        .expect("Subdir schema should have entries");
    assert!(
        subdir_entries.contains_key("test.txt"),
        "Subdir schema should contain test.txt. Schema: {}",
        subdir_schema_content
    );

    let file_entry = &subdir_entries["test.txt"];
    assert!(
        file_entry["node_id"].is_string(),
        "File entry should have node_id. Entry: {}",
        file_entry
    );

    // Verify: file document has correct content
    let file_node_id = file_entry["node_id"].as_str().unwrap();
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, file_node_id))
        .send()
        .expect("Failed to get file HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let file_content = head["content"].as_str().expect("No content in file HEAD");
    assert!(
        file_content.contains("Created locally in new subdir"),
        "File content should match local. Got: {}",
        file_content
    );
}

/// CP-rwaw: Test that initial sync assigns UUIDs directly (not via reconciler).
///
/// Per RECURSIVE_SYNC_THEORY.md:
/// - Schemas pushed to server must have valid UUIDs for all entries
/// - The sync client creates documents BEFORE referencing them in schemas
/// - Schemas with node_id: null are never pushed to the server
///
/// Scenario:
/// 1. Create a directory with files and subdirectories BEFORE starting sync
/// 2. Start sync client
/// 3. Verify schemas on server have valid UUIDs (not null)
#[test]
#[ignore = "CP-5dii: times out on CI"]
fn test_initial_sync_creates_documents_before_schema() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let sync_dir = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&sync_dir).unwrap();
    let port = get_available_port();
    let mqtt_port = get_available_port();
    let server_url = format!("http://127.0.0.1:{}", port);
    let mqtt_broker = format!("mqtt://127.0.0.1:{}", mqtt_port);

    let mut guard = ProcessGuard::new();
    let mqtt = spawn_mqtt_broker(mqtt_port);
    guard.add(mqtt);
    let server = spawn_server_with_mqtt(port, &db_path, Some(&mqtt_broker));
    guard.add(server);

    let client = reqwest::blocking::Client::new();

    // Create fs-root document (empty)
    let resp = client
        .post(format!("{}/docs", server_url))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .expect("Failed to create fs-root document");

    let body: serde_json::Value = resp.json().expect("Failed to parse response");
    let fs_root_id = body["id"].as_str().expect("No id in response");

    // Create files and subdirectory BEFORE starting sync
    // This forces the initial sync path (not the watcher path)
    let subdir = sync_dir.join("preexisting");
    std::fs::create_dir_all(&subdir).expect("Failed to create subdirectory");
    std::fs::write(sync_dir.join("root_file.txt"), "File in root").expect("Failed to write file");
    std::fs::write(subdir.join("nested_file.txt"), "File in subdir")
        .expect("Failed to write nested file");

    // Start sync client
    let sync = spawn_sync_directory_debug(&server_url, &mqtt_broker, &sync_dir, fs_root_id);
    guard.add(sync);

    // Wait for initial sync to complete
    std::thread::sleep(Duration::from_secs(10));

    // Verify: fs-root schema should have valid UUIDs (not null)
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, fs_root_id))
        .send()
        .expect("Failed to get fs-root HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let schema_content = head["content"].as_str().expect("No content in HEAD");
    let schema: serde_json::Value =
        serde_json::from_str(schema_content).expect("Failed to parse schema");

    let entries = schema["root"]["entries"]
        .as_object()
        .expect("Schema should have entries");

    // Check root_file.txt has a valid node_id
    assert!(
        entries.contains_key("root_file.txt"),
        "Schema should contain root_file.txt. Schema: {}",
        schema_content
    );
    let file_entry = &entries["root_file.txt"];
    assert!(
        file_entry["node_id"].is_string() && !file_entry["node_id"].as_str().unwrap().is_empty(),
        "root_file.txt should have a non-null, non-empty node_id. Entry: {}",
        file_entry
    );

    // Check preexisting subdir has a valid node_id
    assert!(
        entries.contains_key("preexisting"),
        "Schema should contain preexisting subdir. Schema: {}",
        schema_content
    );
    let subdir_entry = &entries["preexisting"];
    assert!(
        subdir_entry["node_id"].is_string()
            && !subdir_entry["node_id"].as_str().unwrap().is_empty(),
        "preexisting subdir should have a non-null, non-empty node_id. Entry: {}",
        subdir_entry
    );

    // Check subdirectory schema also has valid UUIDs
    let subdir_node_id = subdir_entry["node_id"].as_str().unwrap();
    let resp = client
        .get(format!("{}/docs/{}/head", server_url, subdir_node_id))
        .send()
        .expect("Failed to get subdir HEAD");
    let head: serde_json::Value = resp.json().expect("Failed to parse HEAD");
    let subdir_schema_content = head["content"].as_str().expect("No content in subdir HEAD");
    let subdir_schema: serde_json::Value =
        serde_json::from_str(subdir_schema_content).expect("Failed to parse subdir schema");

    let subdir_entries = subdir_schema["root"]["entries"]
        .as_object()
        .expect("Subdir schema should have entries");

    assert!(
        subdir_entries.contains_key("nested_file.txt"),
        "Subdir schema should contain nested_file.txt. Schema: {}",
        subdir_schema_content
    );
    let nested_file_entry = &subdir_entries["nested_file.txt"];
    assert!(
        nested_file_entry["node_id"].is_string()
            && !nested_file_entry["node_id"].as_str().unwrap().is_empty(),
        "nested_file.txt should have a non-null, non-empty node_id. Entry: {}",
        nested_file_entry
    );
}

/// CP-29n3.1: Test timeline milestone ordering assertions.
///
/// This test validates that the TimelineMilestone enum has the correct ordering
/// for sandbox sync readiness. The expected ordering is:
///
/// 1. UUID_READY - when file UUID is known (from schema)
/// 2. CRDT_INIT_COMPLETE - when CRDT state is initialized from server
/// 3. TASK_SPAWN - when sync tasks are spawned for a file
/// 4. EXEC_START - when sandbox exec process starts
/// 5. FIRST_WRITE - when first file write occurs (optional)
///
/// This is a unit test that validates the timeline constants and parsing.
#[test]
fn test_timeline_milestone_ordering_invariants() {
    use commonplace_doc::sync::{trace_timeline, TimelineMilestone};
    use std::fs;
    use std::io::{BufRead, BufReader};

    // Clear trace log before test
    let trace_path = "/tmp/sandbox-trace.log";
    let _ = fs::remove_file(trace_path);

    // Emit timeline events in correct order
    trace_timeline(TimelineMilestone::UuidReady, "test.txt", Some("uuid-123"));
    trace_timeline(
        TimelineMilestone::CrdtInitComplete,
        "test.txt",
        Some("uuid-123"),
    );
    trace_timeline(TimelineMilestone::TaskSpawn, "test.txt", Some("uuid-123"));
    trace_timeline(TimelineMilestone::ExecStart, "test_exec", None);
    trace_timeline(TimelineMilestone::FirstWrite, "test.txt", Some("uuid-123"));

    // Read and parse the trace log
    let file = fs::File::open(trace_path).expect("Failed to open trace log");
    let reader = BufReader::new(file);

    let mut milestones: Vec<(u128, String)> = Vec::new();
    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        if line.contains("[TIMELINE]") {
            // Parse: [TIMELINE] milestone=X path=Y uuid=Z timestamp=T pid=P
            let parts: Vec<&str> = line.split_whitespace().collect();
            let mut milestone = String::new();
            let mut timestamp: u128 = 0;
            for part in parts {
                if let Some(m) = part.strip_prefix("milestone=") {
                    milestone = m.to_string();
                }
                if let Some(t) = part.strip_prefix("timestamp=") {
                    timestamp = t.parse().unwrap_or(0);
                }
            }
            if !milestone.is_empty() {
                milestones.push((timestamp, milestone));
            }
        }
    }

    // Verify we captured all 5 milestones
    assert_eq!(
        milestones.len(),
        5,
        "Expected 5 timeline milestones, got: {:?}",
        milestones
    );

    // Verify ordering by milestone name (not timestamp since they may be same)
    let milestone_names: Vec<&str> = milestones.iter().map(|(_, m)| m.as_str()).collect();
    assert_eq!(milestone_names[0], "UUID_READY");
    assert_eq!(milestone_names[1], "CRDT_INIT_COMPLETE");
    assert_eq!(milestone_names[2], "TASK_SPAWN");
    assert_eq!(milestone_names[3], "EXEC_START");
    assert_eq!(milestone_names[4], "FIRST_WRITE");

    // Verify timestamps are non-decreasing (milestones emitted in order)
    for i in 1..milestones.len() {
        assert!(
            milestones[i].0 >= milestones[i - 1].0,
            "Timestamp ordering violated: {} < {} for milestones {} and {}",
            milestones[i].0,
            milestones[i - 1].0,
            milestones[i].1,
            milestones[i - 1].1
        );
    }

    // Cleanup
    let _ = fs::remove_file(trace_path);
}
