//! Actor IO document management for sync agents.
//!
//! Writes and updates the `__io.json` file in the sync directory to
//! advertise the sync agent's presence and status.

use commonplace_types::fs::actor::{ActorIO, ActorStatus, ACTOR_IO_FILENAME};
use std::path::{Path, PathBuf};
use tokio::fs;

/// Manages the actor IO document for a sync agent.
pub struct ActorIOWriter {
    /// Path to the __io.json file
    path: PathBuf,
    /// Actor name (e.g., "sync-client")
    name: String,
    /// Process ID
    pid: u32,
}

impl ActorIOWriter {
    /// Create a new IO writer for the given sync directory and actor name.
    pub fn new(directory: &Path, name: &str) -> Self {
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

    /// Remove the IO document on clean shutdown.
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
    async fn test_write_status_creates_io_file() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "test-sync");

        writer.write_status(ActorStatus::Starting).await.unwrap();

        let content = fs::read_to_string(dir.path().join(ACTOR_IO_FILENAME))
            .await
            .unwrap();
        let io: ActorIO = serde_json::from_str(&content).unwrap();

        assert_eq!(io.name, "test-sync");
        assert_eq!(io.status, ActorStatus::Starting);
        assert!(io.pid.is_some());
        assert!(io.started_at.is_some());
    }

    #[tokio::test]
    async fn test_update_status_preserves_started_at() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "test-sync");

        // Write initial status
        writer.write_status(ActorStatus::Starting).await.unwrap();

        let content1 = fs::read_to_string(dir.path().join(ACTOR_IO_FILENAME))
            .await
            .unwrap();
        let io1: ActorIO = serde_json::from_str(&content1).unwrap();
        let started_at = io1.started_at.clone().unwrap();

        // Small delay to ensure different heartbeat timestamp
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Update status
        writer.update_status(ActorStatus::Active).await.unwrap();

        let content2 = fs::read_to_string(dir.path().join(ACTOR_IO_FILENAME))
            .await
            .unwrap();
        let io2: ActorIO = serde_json::from_str(&content2).unwrap();

        assert_eq!(io2.status, ActorStatus::Active);
        // started_at should be preserved from the original write
        assert_eq!(io2.started_at.unwrap(), started_at);
    }

    #[tokio::test]
    async fn test_remove_deletes_io_file() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "test-sync");

        writer.write_status(ActorStatus::Active).await.unwrap();
        assert!(dir.path().join(ACTOR_IO_FILENAME).exists());

        writer.remove().await.unwrap();
        assert!(!dir.path().join(ACTOR_IO_FILENAME).exists());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let writer = ActorIOWriter::new(dir.path(), "test-sync");

        // Should not error when file doesn't exist
        writer.remove().await.unwrap();
    }
}
