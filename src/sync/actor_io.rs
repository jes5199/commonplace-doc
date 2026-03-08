//! Actor IO document management for sync agents.
//!
//! Writes and updates presence files in the sync directory to
//! advertise the actor's presence and status. Presence files use
//! observer presence extensions (e.g., `sync.exe`, `jes.usr`).

use commonplace_types::fs::actor::{ActorIO, ActorStatus, ACTOR_IO_FILENAME};
use std::path::{Path, PathBuf};
use tokio::fs;

/// Manages the actor IO document for a sync agent.
pub struct ActorIOWriter {
    /// Path to the presence file (e.g., sync.exe)
    path: PathBuf,
    /// Actor name (e.g., "sync")
    name: String,
    /// Process ID
    pid: u32,
}

impl ActorIOWriter {
    /// Create a new IO writer with observer presence extension naming.
    ///
    /// The file will be named `{name}.{extension}` in the given directory.
    /// For example, `ActorIOWriter::new(dir, "sync", "exe")` creates `sync.exe`.
    pub fn new(directory: &Path, name: &str, extension: &str) -> Self {
        let filename = format!("{}.{}", name, extension);
        Self {
            path: directory.join(filename),
            name: name.to_string(),
            pid: std::process::id(),
        }
    }

    /// Create a new IO writer with collision detection.
    ///
    /// Checks if `{name}.{extension}` already exists and belongs to a different
    /// PID. If so, appends a short hash suffix: `{name}-{hash}.{extension}`.
    /// If the file doesn't exist or belongs to our PID (restart), claims the
    /// plain name.
    pub async fn with_collision_check(directory: &Path, name: &str, extension: &str) -> Self {
        let plain_path = directory.join(format!("{}.{}", name, extension));
        let our_pid = std::process::id();

        let needs_suffix = match fs::read_to_string(&plain_path).await {
            Ok(content) => {
                // File exists — check if it belongs to a different PID
                match serde_json::from_str::<ActorIO>(&content) {
                    Ok(io) => io.pid != Some(our_pid),
                    Err(_) => true, // Can't parse — treat as taken
                }
            }
            Err(_) => false, // File doesn't exist — name is available
        };

        if needs_suffix {
            // Generate a short hash from our PID for disambiguation
            let hash = format!("{:x}", our_pid);
            let short_hash = &hash[..hash.len().min(3)];
            let filename = format!("{}-{}.{}", name, short_hash, extension);
            Self {
                path: directory.join(filename),
                name: name.to_string(),
                pid: our_pid,
            }
        } else {
            Self {
                path: plain_path,
                name: name.to_string(),
                pid: our_pid,
            }
        }
    }

    /// Create a new IO writer using the legacy `__io.json` filename.
    #[allow(dead_code)]
    pub fn new_legacy(directory: &Path, name: &str) -> Self {
        Self {
            path: directory.join(ACTOR_IO_FILENAME),
            name: name.to_string(),
            pid: std::process::id(),
        }
    }

    /// Write the IO document with the given status.
    pub async fn write_status(&self, status: ActorStatus) -> Result<(), std::io::Error> {
        let now = chrono::Utc::now().to_rfc3339();
        let io = ActorIO {
            name: self.name.clone(),
            status,
            started_at: Some(now.clone()),
            last_heartbeat: Some(now),
            pid: Some(self.pid),
            capabilities: vec!["sync".to_string(), "edit".to_string()],
            metadata: None,
            docref: None,
        };

        let json = serde_json::to_string_pretty(&io)
            .map_err(std::io::Error::other)?;
        fs::write(&self.path, json).await
    }

    /// Update just the status and heartbeat (reads existing doc, updates fields).
    pub async fn update_status(&self, status: ActorStatus) -> Result<(), std::io::Error> {
        let now = chrono::Utc::now().to_rfc3339();

        // Try to read existing doc to preserve started_at
        let existing = match fs::read_to_string(&self.path).await {
            Ok(content) => serde_json::from_str::<ActorIO>(&content).ok(),
            Err(_) => None,
        };

        let io = ActorIO {
            name: self.name.clone(),
            status,
            started_at: existing
                .as_ref()
                .and_then(|e| e.started_at.clone())
                .or_else(|| Some(now.clone())),
            last_heartbeat: Some(now),
            pid: Some(self.pid),
            capabilities: vec!["sync".to_string(), "edit".to_string()],
            metadata: existing.as_ref().and_then(|e| e.metadata.clone()),
            docref: existing.and_then(|e| e.docref),
        };

        let json = serde_json::to_string_pretty(&io)
            .map_err(std::io::Error::other)?;
        fs::write(&self.path, json).await
    }

    /// Set status to Stopped on clean shutdown (persistent — file remains).
    pub async fn shutdown(&self) -> Result<(), std::io::Error> {
        self.update_status(ActorStatus::Stopped).await
    }

    /// Remove the IO document entirely.
    pub async fn remove(&self) -> Result<(), std::io::Error> {
        match fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Path to the IO document.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_presence_file_uses_extension_naming() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "sync", "exe");

        writer.write_status(ActorStatus::Starting).await.unwrap();

        // File should be named {name}.{ext}, not __io.json
        assert!(dir.path().join("sync.exe").exists());
        assert!(!dir.path().join(ACTOR_IO_FILENAME).exists());

        let content = fs::read_to_string(dir.path().join("sync.exe"))
            .await
            .unwrap();
        let io: ActorIO = serde_json::from_str(&content).unwrap();

        assert_eq!(io.name, "sync");
        assert_eq!(io.status, ActorStatus::Starting);
        assert!(io.pid.is_some());
        assert!(io.started_at.is_some());
    }

    #[tokio::test]
    async fn test_presence_file_different_extensions() {
        let dir = tempfile::tempdir().unwrap();

        let exe_writer = ActorIOWriter::new(dir.path(), "bartleby", "exe");
        exe_writer.write_status(ActorStatus::Active).await.unwrap();
        assert!(dir.path().join("bartleby.exe").exists());

        let usr_writer = ActorIOWriter::new(dir.path(), "jes", "usr");
        usr_writer.write_status(ActorStatus::Active).await.unwrap();
        assert!(dir.path().join("jes.usr").exists());

        let bot_writer = ActorIOWriter::new(dir.path(), "claude", "bot");
        bot_writer.write_status(ActorStatus::Active).await.unwrap();
        assert!(dir.path().join("claude.bot").exists());
    }

    #[tokio::test]
    async fn test_update_status_preserves_started_at() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "sync", "exe");

        // Write initial status
        writer.write_status(ActorStatus::Starting).await.unwrap();

        let content1 = fs::read_to_string(writer.path()).await.unwrap();
        let io1: ActorIO = serde_json::from_str(&content1).unwrap();
        let started_at = io1.started_at.clone().unwrap();

        // Small delay to ensure different heartbeat timestamp
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Update status
        writer.update_status(ActorStatus::Active).await.unwrap();

        let content2 = fs::read_to_string(writer.path()).await.unwrap();
        let io2: ActorIO = serde_json::from_str(&content2).unwrap();

        assert_eq!(io2.status, ActorStatus::Active);
        // started_at should be preserved from the original write
        assert_eq!(io2.started_at.unwrap(), started_at);
    }

    #[tokio::test]
    async fn test_shutdown_sets_stopped_status() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "sync", "exe");

        writer.write_status(ActorStatus::Active).await.unwrap();
        assert!(dir.path().join("sync.exe").exists());

        // Shutdown should set status to Stopped, not delete the file
        writer.shutdown().await.unwrap();
        assert!(dir.path().join("sync.exe").exists());

        let content = fs::read_to_string(writer.path()).await.unwrap();
        let io: ActorIO = serde_json::from_str(&content).unwrap();
        assert_eq!(io.status, ActorStatus::Stopped);
    }

    #[tokio::test]
    async fn test_remove_deletes_presence_file() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "sync", "exe");

        writer.write_status(ActorStatus::Active).await.unwrap();
        assert!(dir.path().join("sync.exe").exists());

        writer.remove().await.unwrap();
        assert!(!dir.path().join("sync.exe").exists());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "sync", "exe");

        // Should not error when file doesn't exist
        writer.remove().await.unwrap();
    }

    #[tokio::test]
    async fn test_collision_suffix_when_name_taken_by_different_pid() {
        let dir = tempfile::tempdir().unwrap();

        // First writer claims sync.exe
        let writer1 = ActorIOWriter::new(dir.path(), "sync", "exe");
        writer1.write_status(ActorStatus::Active).await.unwrap();

        // Simulate a different PID by writing a fake presence file with PID 99999
        let fake_io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: None,
            pid: Some(99999),
            capabilities: vec![],
            metadata: None,
            docref: None,
        };
        let fake_json = serde_json::to_string_pretty(&fake_io).unwrap();
        fs::write(dir.path().join("sync.exe"), &fake_json).await.unwrap();

        // Second writer should get a suffixed name since sync.exe is taken by PID 99999
        let writer2 = ActorIOWriter::with_collision_check(dir.path(), "sync", "exe").await;
        writer2.write_status(ActorStatus::Active).await.unwrap();

        // The suffixed file should exist and NOT be sync.exe
        assert_ne!(writer2.path(), dir.path().join("sync.exe"));
        let filename = writer2.path().file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("sync-"), "Expected sync-{{hash}}.exe, got {}", filename);
        assert!(filename.ends_with(".exe"), "Expected sync-{{hash}}.exe, got {}", filename);
    }

    #[tokio::test]
    async fn test_no_collision_suffix_when_name_taken_by_same_pid() {
        let dir = tempfile::tempdir().unwrap();

        // Write a presence file with our own PID
        let our_pid = std::process::id();
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Stopped,
            started_at: None,
            last_heartbeat: None,
            pid: Some(our_pid),
            capabilities: vec![],
            metadata: None,
            docref: None,
        };
        let json = serde_json::to_string_pretty(&io).unwrap();
        fs::write(dir.path().join("sync.exe"), &json).await.unwrap();

        // Same PID should reclaim the name (e.g., restart scenario)
        let writer = ActorIOWriter::with_collision_check(dir.path(), "sync", "exe").await;
        assert_eq!(writer.path(), dir.path().join("sync.exe"));
    }

    #[tokio::test]
    async fn test_no_collision_suffix_when_name_available() {
        let dir = tempfile::tempdir().unwrap();

        // No existing file — should get the plain name
        let writer = ActorIOWriter::with_collision_check(dir.path(), "sync", "exe").await;
        assert_eq!(writer.path(), dir.path().join("sync.exe"));
    }

    #[tokio::test]
    async fn test_reroot_reclaims_presence_file() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate first run: start → active → shutdown (Stopped)
        let writer1 = ActorIOWriter::with_collision_check(dir.path(), "sync", "exe").await;
        writer1.write_status(ActorStatus::Starting).await.unwrap();
        writer1.update_status(ActorStatus::Active).await.unwrap();
        writer1.shutdown().await.unwrap();

        // Verify file exists with Stopped status
        let content = fs::read_to_string(writer1.path()).await.unwrap();
        let io: ActorIO = serde_json::from_str(&content).unwrap();
        assert_eq!(io.status, ActorStatus::Stopped);

        // Simulate re-root: same PID reclaims the name
        let writer2 = ActorIOWriter::with_collision_check(dir.path(), "sync", "exe").await;
        assert_eq!(writer2.path(), dir.path().join("sync.exe")); // Same name, no suffix
        writer2.write_status(ActorStatus::Starting).await.unwrap();

        // Verify it overwrites with Starting status
        let content2 = fs::read_to_string(writer2.path()).await.unwrap();
        let io2: ActorIO = serde_json::from_str(&content2).unwrap();
        assert_eq!(io2.status, ActorStatus::Starting);
    }
}
