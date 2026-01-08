//! Integration tests for flock-aware sync.
//!
//! These tests verify the coordination between flock acquisition,
//! PathState tracking, and FlockSyncState management.

#[cfg(unix)]
mod tests {
    use bytes::Bytes;
    use commonplace_doc::sync::flock::{try_flock_exclusive, FlockResult};
    use commonplace_doc::sync::flock_state::{FlockSyncState, PathState};
    use std::fs::File;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    /// Test that flock acquisition and PathState work together correctly.
    /// This validates the integration between the flock module and state tracking.
    #[tokio::test]
    async fn test_flock_with_state_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        // Create test file
        std::fs::write(&file_path, "initial content").unwrap();

        // Create state tracking
        let state = FlockSyncState::new();
        let path = file_path.to_path_buf();

        // Record an outbound commit
        state.record_outbound(&path, "commit-1".to_string()).await;
        assert!(state.has_pending_outbound(&path).await);

        // Acquire flock - should succeed since no one else has it
        let result = try_flock_exclusive(&file_path, Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(matches!(result, FlockResult::Acquired(_)));

        // Confirm the outbound commit
        state
            .confirm_outbound(&path, &["commit-1".to_string()])
            .await;
        assert!(!state.has_pending_outbound(&path).await);
    }

    /// Test that flock timeout is respected when file is locked.
    /// This verifies the timeout behavior integrates correctly.
    #[tokio::test]
    async fn test_flock_timeout_with_locked_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("locked.txt");

        // Create and lock the file
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"locked content").unwrap();
        file.sync_all().unwrap();

        // Hold an exclusive lock to simulate agent holding the file
        let lock_holder = File::open(&file_path).unwrap();
        let result = unsafe { libc::flock(lock_holder.as_raw_fd(), libc::LOCK_EX) };
        assert_eq!(result, 0, "Should acquire initial lock");

        // Try to acquire with short timeout
        let start = Instant::now();
        let result = try_flock_exclusive(&file_path, Some(Duration::from_millis(150)))
            .await
            .unwrap();

        // Should timeout since the lock is held
        assert!(matches!(result, FlockResult::Timeout));
        // Should have waited approximately the timeout duration
        assert!(start.elapsed() >= Duration::from_millis(150));
        assert!(start.elapsed() < Duration::from_millis(500));

        // Release the lock
        unsafe {
            libc::flock(lock_holder.as_raw_fd(), libc::LOCK_UN);
        }
    }

    /// Test FlockSyncState concurrent access from multiple async contexts.
    /// This validates the Arc<RwLock> design works correctly.
    #[tokio::test]
    async fn test_flock_sync_state_concurrent_access() {
        let state = FlockSyncState::new();
        let path = std::path::PathBuf::from("/test/concurrent.txt");

        // Clone the state for concurrent access
        let state1 = state.clone();
        let state2 = state.clone();
        let path1 = path.clone();
        let path2 = path.clone();

        // Spawn multiple tasks that modify state concurrently
        let handle1 = tokio::spawn(async move {
            for i in 0..10 {
                state1
                    .record_outbound(&path1, format!("commit-a{}", i))
                    .await;
            }
        });

        let handle2 = tokio::spawn(async move {
            for i in 0..10 {
                state2
                    .record_outbound(&path2, format!("commit-b{}", i))
                    .await;
            }
        });

        // Wait for both tasks
        handle1.await.unwrap();
        handle2.await.unwrap();

        // Should have all 20 commits pending
        let pending = state.get_pending_outbound(&path).await;
        assert_eq!(pending.len(), 20);
    }

    /// Test the full inbound queueing and processing workflow.
    /// This validates PathState inbound queue behavior.
    #[tokio::test]
    async fn test_inbound_queue_workflow() {
        let state = FlockSyncState::new();
        let path = std::path::PathBuf::from("/test/workflow.txt");

        // 1. Record outbound commit (simulating local edit uploaded)
        state
            .record_outbound(&path, "local-commit".to_string())
            .await;
        assert!(state.has_pending_outbound(&path).await);

        // 2. Queue inbound write (simulating server update while local is pending)
        state
            .queue_inbound(
                &path,
                Bytes::from("server content"),
                "server-commit".to_string(),
            )
            .await;

        // 3. Verify inbound is queued
        let inbound = state.take_pending_inbound(&path).await;
        assert!(inbound.is_some());
        let inbound = inbound.unwrap();
        assert_eq!(inbound.commit_id, "server-commit");
        assert_eq!(&*inbound.content, b"server content");

        // 4. Taking again should return None
        assert!(state.take_pending_inbound(&path).await.is_none());
    }

    /// Test that PathState correctly tracks multiple outbound commits.
    #[tokio::test]
    async fn test_multiple_outbound_tracking() {
        let mut path_state = PathState::new(12345);

        // Add multiple outbound commits
        path_state.add_pending_outbound("cid-1".to_string());
        path_state.add_pending_outbound("cid-2".to_string());
        path_state.add_pending_outbound("cid-3".to_string());

        assert_eq!(path_state.pending_outbound.len(), 3);
        assert!(path_state.has_pending_outbound());

        // Confirm some
        path_state.confirm_outbound(&["cid-1".to_string(), "cid-2".to_string()]);
        assert_eq!(path_state.pending_outbound.len(), 1);
        assert!(path_state.pending_outbound.contains("cid-3"));

        // Confirm remaining
        path_state.confirm_outbound(&["cid-3".to_string()]);
        assert!(!path_state.has_pending_outbound());
    }

    /// Test that newer inbound writes replace older queued ones.
    #[tokio::test]
    async fn test_inbound_replacement() {
        let state = FlockSyncState::new();
        let path = std::path::PathBuf::from("/test/replace.txt");

        // Queue first inbound
        state
            .queue_inbound(&path, Bytes::from("old content"), "old-commit".to_string())
            .await;

        // Queue second inbound (should replace first)
        state
            .queue_inbound(&path, Bytes::from("new content"), "new-commit".to_string())
            .await;

        // Take should return the newer one
        let inbound = state.take_pending_inbound(&path).await.unwrap();
        assert_eq!(inbound.commit_id, "new-commit");
        assert_eq!(&*inbound.content, b"new content");
    }

    /// Test that removing state clears all tracking for a path.
    #[tokio::test]
    async fn test_state_removal_clears_all() {
        let state = FlockSyncState::new();
        let path = std::path::PathBuf::from("/test/remove.txt");

        // Set up various state
        state.record_outbound(&path, "commit-1".to_string()).await;
        state
            .queue_inbound(&path, Bytes::from("content"), "commit-2".to_string())
            .await;
        state.set_inode(&path, 99999).await;

        // Remove all state for path
        state.remove(&path).await;

        // Everything should be gone
        assert!(!state.has_pending_outbound(&path).await);
        assert!(state.take_pending_inbound(&path).await.is_none());
    }

    /// Test flock acquisition on a file that gets deleted and recreated.
    #[tokio::test]
    async fn test_flock_handles_file_recreation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("recreate.txt");

        // Create initial file
        std::fs::write(&file_path, "original").unwrap();

        // Acquire lock
        let result = try_flock_exclusive(&file_path, Some(Duration::from_secs(1)))
            .await
            .unwrap();
        let guard = match result {
            FlockResult::Acquired(g) => g,
            FlockResult::Timeout => panic!("Should acquire lock"),
        };

        // Delete file while we have the lock
        std::fs::remove_file(&file_path).unwrap();

        // Drop the guard (releases lock)
        drop(guard);

        // Recreate the file
        std::fs::write(&file_path, "recreated").unwrap();

        // Should be able to acquire lock on new file
        let result = try_flock_exclusive(&file_path, Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(matches!(result, FlockResult::Acquired(_)));
    }
}
