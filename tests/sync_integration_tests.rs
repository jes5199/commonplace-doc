//! Integration tests for server + sync client.
//!
//! These tests spawn actual processes and verify end-to-end behavior.

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

/// Helper to spawn the server and wait for it to be ready
fn spawn_server(port: u16, db_path: &std::path::Path) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_commonplace-server"))
        .args([
            "--port",
            &port.to_string(),
            "--database",
            db_path.to_str().unwrap(),
        ])
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
fn spawn_sync_file(server_url: &str, file_path: &std::path::Path, node_id: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_commonplace-sync"))
        .args([
            "--server",
            server_url,
            "--node",
            node_id,
            "--file",
            file_path.to_str().unwrap(),
        ])
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
    let server_url = format!("http://127.0.0.1:{}", port);

    let mut guard = ProcessGuard::new();
    let server = spawn_server(port, &db_path);
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
    let sync = spawn_sync_file(&server_url, &sync_file, doc_id);
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
    let server_url = format!("http://127.0.0.1:{}", port);

    let mut guard = ProcessGuard::new();
    let server = spawn_server(port, &db_path);
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
    let sync = spawn_sync_file(&server_url, &sync_file, doc_id);
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
    let sync = spawn_sync_file(&server_url, &sync_file, doc_id);
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
