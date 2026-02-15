//! Integration tests for inode shadow sync.
//!
//! These tests verify that when a file is atomically replaced, writes to
//! the old inode are captured via shadow hardlinks and can be merged.

#[cfg(unix)]
mod tests {
    use commonplace_doc::sync::{
        atomic_write_with_shadow, InodeKey, InodeTracker, ShadowWriteEvent,
    };
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    /// Test that atomic_write_with_shadow creates a shadow hardlink when
    /// the inode is tracked.
    #[tokio::test]
    async fn test_shadow_hardlink_creation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let shadow_dir = temp_dir.path().join("shadows");

        // Create initial file with some content
        std::fs::write(&file_path, "initial content").unwrap();

        // Get the initial inode
        let initial_inode = InodeKey::from_path(&file_path).unwrap();
        eprintln!(
            "Initial inode: {:x}-{:x}",
            initial_inode.dev, initial_inode.ino
        );

        // Create tracker and track the initial inode
        let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_dir.clone())));
        {
            let mut t = tracker.write().await;
            t.track(
                initial_inode,
                "commit-123".to_string(),
                file_path.clone(),
                None,
            );
            eprintln!("Tracker states: {:?}", t.states.keys().collect::<Vec<_>>());
        }

        // Do an atomic write - this should create a shadow hardlink
        let new_content = b"new content from server";
        let result = atomic_write_with_shadow(
            &file_path,
            new_content,
            Some("commit-456".to_string()),
            &tracker,
        )
        .await;

        eprintln!("atomic_write_with_shadow result: {:?}", result);
        let new_inode = result.unwrap();

        // Verify the file has new content
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "new content from server");

        // Verify the inode changed (atomic write creates new inode)
        eprintln!("New inode: {:x}-{:x}", new_inode.dev, new_inode.ino);
        assert_ne!(
            new_inode, initial_inode,
            "Inode should change after atomic write"
        );

        // Verify the shadow hardlink was created
        let shadow_path = shadow_dir.join(initial_inode.shadow_filename());
        eprintln!("Shadow path: {:?}", shadow_path);
        eprintln!(
            "Shadow dir contents: {:?}",
            std::fs::read_dir(&shadow_dir).map(|d| d.collect::<Vec<_>>())
        );
        assert!(shadow_path.exists(), "Shadow hardlink should exist");

        // Verify the shadow has the OLD content (the content before atomic write)
        let shadow_content = std::fs::read_to_string(&shadow_path).unwrap();
        assert_eq!(
            shadow_content, "initial content",
            "Shadow should have old content"
        );

        // Verify the tracker was updated
        let t = tracker.read().await;

        // New inode should be tracked
        let new_state = t.get(&new_inode).expect("New inode should be tracked");
        assert_eq!(new_state.commit_id, "commit-456");

        // Old inode should be marked as shadowed
        let old_state = t
            .get(&initial_inode)
            .expect("Old inode should still be tracked");
        assert!(
            old_state.shadow_path.is_some(),
            "Old inode should have shadow_path"
        );
    }

    /// Test that writes to a shadowed inode can be detected.
    #[tokio::test]
    async fn test_shadow_write_detection() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let shadow_dir = temp_dir.path().join("shadows");

        // Create initial file
        std::fs::write(&file_path, "initial").unwrap();

        // Open the file and hold the fd (simulating a slow writer)
        let mut old_fd = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        // Get the initial inode
        let initial_inode = InodeKey::from_file(&old_fd).unwrap();

        // Create tracker and track the initial inode
        let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_dir.clone())));
        {
            let mut t = tracker.write().await;
            t.track(
                initial_inode,
                "commit-123".to_string(),
                file_path.clone(),
                None,
            );
        }

        // Do an atomic write - this replaces the file with a new inode
        atomic_write_with_shadow(
            &file_path,
            b"server update",
            Some("commit-456".to_string()),
            &tracker,
        )
        .await
        .unwrap();

        // The old fd still points to the old inode (now shadowed)
        // Write through the old fd
        old_fd.write_all(b"slow writer append").unwrap();
        old_fd.flush().unwrap();

        // The main file should have server content
        let main_content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(main_content, "server update");

        // The shadow should have the slow writer's content
        let shadow_path = shadow_dir.join(initial_inode.shadow_filename());
        let shadow_content = std::fs::read_to_string(&shadow_path).unwrap();
        // Note: the slow writer overwrote from the beginning due to seek position
        assert!(
            shadow_content.contains("slow writer append"),
            "Shadow should contain slow writer's content"
        );
    }

    /// Test InodeKey parsing from shadow filename.
    #[test]
    fn test_inode_key_shadow_filename_roundtrip() {
        let key = InodeKey::new(0xfe01, 0x12345678);
        let filename = key.shadow_filename();
        assert_eq!(filename, "fe01-12345678");

        let parsed = InodeKey::from_shadow_filename(&filename).unwrap();
        assert_eq!(parsed, key);
    }

    /// Test that shadow_watcher_task detects writes to shadow files.
    #[tokio::test]
    async fn test_shadow_watcher_detects_writes() {
        use commonplace_doc::sync::watcher::shadow_watcher_task;
        use tokio::sync::mpsc;
        use tokio::time::{sleep, Duration};

        let temp_dir = TempDir::new().unwrap();
        let shadow_dir = temp_dir.path().join("shadows");
        std::fs::create_dir_all(&shadow_dir).unwrap();

        // Create a shadow file with a valid inode key name
        let inode_key = InodeKey::new(0xabc, 0xdef);
        let shadow_path = shadow_dir.join(inode_key.shadow_filename());
        std::fs::write(&shadow_path, "initial").unwrap();

        // Start shadow watcher
        let (tx, mut rx) = mpsc::channel::<ShadowWriteEvent>(10);
        let watcher_handle = tokio::spawn(shadow_watcher_task(shadow_dir.clone(), tx));

        // Give watcher time to start
        sleep(Duration::from_millis(200)).await;

        // Write to the shadow file
        std::fs::write(&shadow_path, "modified content").unwrap();

        // Wait for event with timeout
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("Should receive event within timeout")
            .expect("Channel should not be closed");

        assert_eq!(event.inode_key, inode_key);
        assert_eq!(event.content, b"modified content");

        watcher_handle.abort();
    }
}
