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
            metadata: existing.and_then(|e| e.metadata),
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
}
