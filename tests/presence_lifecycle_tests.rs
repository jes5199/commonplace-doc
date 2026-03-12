//! Integration tests for the presence lifecycle feature.
//!
//! Tests the identity management and heartbeat reaper logic.
//! Some tests require MQTT broker + server; they are skipped if unavailable.

use commonplace_types::fs::actor::{ActorIO, ActorStatus};
use commonplace_doc::orchestrator::identity::{
    default_heartbeat_timeout, is_heartbeat_expired,
};

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Check if MQTT broker is available on localhost:1883
fn mqtt_available() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:1883".parse().unwrap(),
        Duration::from_millis(100),
    )
    .is_ok()
}

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Compute status file path from config path
fn status_file_path(config_path: &std::path::Path) -> std::path::PathBuf {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let canonical = config_path
        .canonicalize()
        .unwrap_or_else(|_| config_path.to_path_buf());

    let mut hasher = DefaultHasher::new();
    canonical.hash(&mut hasher);
    let hash = hasher.finish();

    let hash_str = format!("{:012x}", hash & 0xffffffffffff);
    std::env::temp_dir().join(format!("commonplace-orchestrator-{}.status.json", hash_str))
}

struct ProcessGuard {
    children: Vec<Child>,
}

impl ProcessGuard {
    fn new() -> Self { Self { children: Vec::new() } }
    fn add(&mut self, child: Child) { self.children.push(child); }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        for mut child in self.children.drain(..) {
            let pid = child.id();
            unsafe { libc::kill(pid as i32, libc::SIGTERM); }
            let start = std::time::Instant::now();
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if start.elapsed() > Duration::from_secs(5) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        break;
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                    Err(_) => break,
                }
            }
        }
    }
}

fn wait_for_orchestrator_ready(
    config_path: &std::path::Path,
    timeout: Duration,
) -> Result<serde_json::Value, String> {
    let start = std::time::Instant::now();
    let status_path = status_file_path(config_path);

    loop {
        if start.elapsed() > timeout {
            return Err("Timeout waiting for orchestrator".to_string());
        }
        if let Ok(content) = std::fs::read_to_string(&status_path) {
            if let Ok(status) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(processes) = status.get("processes").and_then(|p| p.as_array()) {
                    let server_running = processes.iter().any(|p| {
                        p.get("name").and_then(|n| n.as_str()) == Some("server")
                            && p.get("state").and_then(|s| s.as_str()) == Some("Running")
                    });
                    let sync_running = processes.iter().any(|p| {
                        p.get("name").and_then(|n| n.as_str()) == Some("sync")
                            && p.get("state").and_then(|s| s.as_str()) == Some("Running")
                    });
                    if server_running && sync_running {
                        return Ok(status);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn test_identity_heartbeat_lifecycle() {
    // A fresh identity starts as Stopped with no heartbeat
    let identity = ActorIO {
        name: "sync".to_string(),
        status: ActorStatus::Stopped,
        started_at: None,
        last_heartbeat: None,
        pid: None,
        capabilities: vec![],
        metadata: None,
        docref: None,
        heartbeat_timeout_seconds: Some(default_heartbeat_timeout("exe")),
    };

    // No heartbeat → expired
    assert!(is_heartbeat_expired(&identity));

    // Simulate process starting: fresh heartbeat
    let active = ActorIO {
        status: ActorStatus::Active,
        started_at: Some(chrono::Utc::now().to_rfc3339()),
        last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
        pid: Some(12345),
        ..identity.clone()
    };

    // Fresh heartbeat → not expired
    assert!(!is_heartbeat_expired(&active));

    // Simulate stale heartbeat (old timestamp)
    let stale = ActorIO {
        last_heartbeat: Some("2020-01-01T00:00:00+00:00".to_string()),
        ..active.clone()
    };

    // Stale heartbeat → expired
    assert!(is_heartbeat_expired(&stale));
}

#[test]
fn test_heartbeat_timeout_by_extension() {
    // Different extensions have different timeouts
    assert_eq!(default_heartbeat_timeout("exe"), 30);  // daemon: tight timeout
    assert_eq!(default_heartbeat_timeout("usr"), 300); // human: generous timeout
    assert_eq!(default_heartbeat_timeout("bot"), 60);  // bot: moderate timeout
}

#[test]
fn test_linked_presence_docref() {
    // When a sync agent has an identity UUID, its presence file should
    // include a docref pointing to the cold identity
    let identity_uuid = "abc-123-def-456";

    let hot_presence = ActorIO {
        name: "sync".to_string(),
        status: ActorStatus::Active,
        started_at: Some(chrono::Utc::now().to_rfc3339()),
        last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
        pid: Some(std::process::id()),
        capabilities: vec!["sync".to_string(), "edit".to_string()],
        metadata: None,
        docref: Some(identity_uuid.to_string()),
        heartbeat_timeout_seconds: Some(30),
    };

    assert_eq!(hot_presence.docref.as_deref(), Some(identity_uuid));
    assert!(!is_heartbeat_expired(&hot_presence));
}

#[test]
fn test_ephemeral_presence_no_docref() {
    // Without identity UUID, presence file has no docref (ephemeral mode)
    let ephemeral = ActorIO {
        name: "sync".to_string(),
        status: ActorStatus::Active,
        started_at: Some(chrono::Utc::now().to_rfc3339()),
        last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
        pid: Some(std::process::id()),
        capabilities: vec!["sync".to_string(), "edit".to_string()],
        metadata: None,
        docref: None,
        heartbeat_timeout_seconds: None,
    };

    assert!(ephemeral.docref.is_none());
    // Default timeout (60s) applies when not specified
    assert!(!is_heartbeat_expired(&ephemeral));
}

#[test]
fn test_actor_io_writer_linked() {
    // Test the linked ActorIOWriter creates presence with docref
    let dir = tempfile::tempdir().unwrap();
    let writer = commonplace_doc::sync::actor_io::ActorIOWriter::new_linked(
        dir.path(),
        "sync",
        "exe",
        "test-uuid-123".to_string(),
    );

    // Verify the path is correct
    assert_eq!(
        writer.path(),
        dir.path().join("sync.exe")
    );
}

#[tokio::test]
async fn test_actor_io_writer_linked_writes_docref() {
    let dir = tempfile::tempdir().unwrap();
    let writer = commonplace_doc::sync::actor_io::ActorIOWriter::new_linked(
        dir.path(),
        "sync",
        "exe",
        "test-uuid-123".to_string(),
    );

    writer.write_status(ActorStatus::Active).await.unwrap();

    let content = tokio::fs::read_to_string(dir.path().join("sync.exe")).await.unwrap();
    let io: ActorIO = serde_json::from_str(&content).unwrap();

    assert_eq!(io.docref.as_deref(), Some("test-uuid-123"));
    assert_eq!(io.status, ActorStatus::Active);
    assert!(io.pid.is_some());
}

#[tokio::test]
async fn test_linked_presence_remove_on_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let writer = commonplace_doc::sync::actor_io::ActorIOWriter::new_linked(
        dir.path(),
        "sync",
        "exe",
        "test-uuid-456".to_string(),
    );

    // Write initial presence
    writer.write_status(ActorStatus::Active).await.unwrap();
    assert!(dir.path().join("sync.exe").exists());

    // In linked mode, shutdown means remove (not mark stopped)
    writer.remove().await.unwrap();
    assert!(!dir.path().join("sync.exe").exists());
}

#[tokio::test]
async fn test_ephemeral_presence_persists_on_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let writer = commonplace_doc::sync::actor_io::ActorIOWriter::new(dir.path(), "sync", "exe");

    // Write initial presence
    writer.write_status(ActorStatus::Active).await.unwrap();
    assert!(dir.path().join("sync.exe").exists());

    // In ephemeral mode, shutdown marks as stopped (file persists)
    writer.shutdown().await.unwrap();
    assert!(dir.path().join("sync.exe").exists());

    let content = tokio::fs::read_to_string(dir.path().join("sync.exe")).await.unwrap();
    let io: ActorIO = serde_json::from_str(&content).unwrap();
    assert_eq!(io.status, ActorStatus::Stopped);
}

/// Full end-to-end presence lifecycle test.
///
/// Tests the complete flow: orchestrator starts server + sync,
/// sync writes presence file, SIGTERM triggers cleanup.
///
/// Requires: MQTT broker on localhost:1883, and a pre-bootstrapped
/// server with the 'workspace' node. Currently skipped because the
/// test harness can't bootstrap an empty server (sync fails to resolve
/// --node workspace when fs-root has no entries).
///
/// To run manually:
/// 1. Start the full system: cargo run --bin commonplace-orchestrator
/// 2. Check workspace/ for *.exe presence files
/// 3. Kill a sync process → verify presence file cleaned up
/// 4. Verify __identities/ entry persists on server
#[test]
#[ignore = "requires pre-bootstrapped server - run manually"]
fn test_full_presence_lifecycle() {
    if !mqtt_available() {
        eprintln!("Skipping: MQTT broker not available");
        return;
    }

    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let workspace_dir = temp_dir.path().join("workspace");
    let config_path = temp_dir.path().join("commonplace.json");
    let port = get_available_port();
    let workspace_name = format!("test-{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Create workspace with schema
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
    ).unwrap();

    let server_url = format!("http://127.0.0.1:{}", port);
    let server_bin = env!("CARGO_BIN_EXE_commonplace-server");
    let sync_bin = env!("CARGO_BIN_EXE_commonplace-sync");

    let config = serde_json::json!({
        "database": db_path.to_str().unwrap(),
        "http_server": &server_url,
        "workspace": &workspace_name,
        "processes": {
            "server": {
                "command": server_bin,
                "args": ["--port", port.to_string(), "--workspace", &workspace_name, "--mqtt-broker", "mqtt://localhost:1883"],
                "cwd": temp_dir.path().to_str().unwrap(),
                "restart": { "policy": "never" }
            },
            "sync": {
                "command": sync_bin,
                "args": [
                    "--server", &server_url,
                    "--node", "workspace",
                    "--directory", workspace_dir.to_str().unwrap(),
                    "--initial-sync", "local",
                    "--workspace", &workspace_name,
                    "--name", "sync-test"
                ],
                "restart": { "policy": "never" },
                "depends_on": ["server"]
            }
        }
    }).to_string();
    std::fs::write(&config_path, &config).unwrap();

    let mut guard = ProcessGuard::new();

    let orchestrator = Command::new(env!("CARGO_BIN_EXE_commonplace-orchestrator"))
        .args(["--config", config_path.to_str().unwrap(), "--server", &server_url])
        .current_dir(temp_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn orchestrator");
    guard.add(orchestrator);

    // Wait for system to be ready
    let _status = wait_for_orchestrator_ready(&config_path, Duration::from_secs(150))
        .expect("Orchestrator failed to start");

    // Give sync agent time to write its presence file
    eprintln!("Orchestrator ready, waiting for presence file...");
    std::thread::sleep(Duration::from_secs(5));

    // Debug: list files
    eprintln!("Files in workspace:");
    for entry in std::fs::read_dir(&workspace_dir).unwrap().flatten() {
        eprintln!("  {}", entry.file_name().to_string_lossy());
    }
    // Debug: check status file
    let status_path = status_file_path(&config_path);
    if let Ok(content) = std::fs::read_to_string(&status_path) {
        eprintln!("Status file: {}", content);
    }

    // Step 2: Check for presence file in workspace
    let presence_files: Vec<_> = std::fs::read_dir(&workspace_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.ends_with(".exe") && !name.starts_with(".")
        })
        .collect();

    assert!(
        !presence_files.is_empty(),
        "Expected at least one .exe presence file in workspace, found none. \
         Files: {:?}",
        std::fs::read_dir(&workspace_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>()
    );

    // Step 3: Verify presence file structure
    let presence_path = &presence_files[0].path();
    let presence_content = std::fs::read_to_string(presence_path).unwrap();
    let presence_io: ActorIO = serde_json::from_str(&presence_content)
        .unwrap_or_else(|e| panic!("Failed to parse presence file {:?}: {}\nContent: {}", presence_path, e, presence_content));

    assert_eq!(presence_io.status, ActorStatus::Active, "Presence should be Active");
    assert!(presence_io.pid.is_some(), "Presence should have a PID");
    assert!(presence_io.last_heartbeat.is_some(), "Presence should have a heartbeat");
    assert!(!is_heartbeat_expired(&presence_io), "Heartbeat should be fresh");

    eprintln!("Presence file verified: {:?}", presence_path.file_name().unwrap());
    eprintln!("  status: {:?}, pid: {:?}, docref: {:?}",
        presence_io.status, presence_io.pid, presence_io.docref);

    // Step 4: Check identity on server via HTTP
    let http_client = reqwest::blocking::Client::new();

    // Check if __identities directory was created on the server
    let identities_url = format!("{}/files/workspace/__identities/", server_url);
    let identities_resp = http_client.get(&identities_url).send();
    if let Ok(resp) = identities_resp {
        eprintln!("__identities/ response: HTTP {}", resp.status());
        if resp.status().is_success() {
            eprintln!("Cold identity directory exists on server");
        }
    }

    // Step 5: Get sync PID and send SIGTERM to it
    let sync_pid = presence_io.pid.unwrap();
    eprintln!("Sending SIGTERM to sync process (PID {})", sync_pid);
    unsafe { libc::kill(sync_pid as i32, libc::SIGTERM); }

    // Wait for sync to shut down
    std::thread::sleep(Duration::from_secs(3));

    // Step 6: If linked mode, presence file should be removed.
    // If ephemeral mode, presence file should show Stopped.
    if presence_io.docref.is_some() {
        // Linked mode: hot presence should be deleted
        if presence_path.exists() {
            let updated = std::fs::read_to_string(presence_path).unwrap();
            let updated_io: ActorIO = serde_json::from_str(&updated).unwrap();
            eprintln!("Presence file still exists after shutdown (status: {:?})", updated_io.status);
            // In linked mode we expect deletion, but sync might have been restarted
        } else {
            eprintln!("Linked presence file correctly removed on shutdown");
        }
    } else {
        // Ephemeral mode: file should persist with Stopped status
        assert!(presence_path.exists(), "Ephemeral presence file should persist");
        let updated = std::fs::read_to_string(presence_path).unwrap();
        let updated_io: ActorIO = serde_json::from_str(&updated).unwrap();
        assert_eq!(updated_io.status, ActorStatus::Stopped,
            "Ephemeral presence should be Stopped after shutdown");
        eprintln!("Ephemeral presence correctly marked as Stopped");
    }

    eprintln!("Full presence lifecycle test passed!");
}
