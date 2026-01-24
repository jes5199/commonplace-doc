//! Stage 0 watcher stability tests.
//!
//! These tests validate that the file watcher reads stable content for
//! create/modify events and honors write completion before reading.

use commonplace_doc::sync::watcher::FILE_DEBOUNCE_MS;
use commonplace_doc::sync::{
    file_watcher_task, wait_for_file_stability, FileEvent, STABILITY_CHECK_INTERVAL_MS,
};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

const ADVANCE_STEP_MS: u64 = 10;
const MAX_ADVANCE_MS: u64 = 2_000;

fn spawn_file_watcher(path: PathBuf) -> (mpsc::Receiver<FileEvent>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(4);
    let handle = tokio::spawn(file_watcher_task(path, tx));
    (rx, handle)
}

async fn wait_for_modified(rx: &mut mpsc::Receiver<FileEvent>, max_advance: Duration) -> Vec<u8> {
    let step = Duration::from_millis(ADVANCE_STEP_MS);
    let mut elapsed = Duration::ZERO;

    loop {
        match rx.try_recv() {
            Ok(FileEvent::Modified(content)) => return content,
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                panic!("watcher task ended before sending an event");
            }
        }

        if elapsed >= max_advance {
            panic!("timed out waiting for watcher event");
        }

        tokio::task::yield_now().await;
        let remaining = max_advance - elapsed;
        let advance_by = if remaining < step { remaining } else { step };
        tokio::time::advance(advance_by).await;
        elapsed += advance_by;
    }
}

async fn assert_no_event(rx: &mut mpsc::Receiver<FileEvent>, duration: Duration) {
    let step = Duration::from_millis(ADVANCE_STEP_MS);
    let mut elapsed = Duration::ZERO;

    while elapsed < duration {
        if let Ok(event) = rx.try_recv() {
            panic!("unexpected watcher event: {:?}", event);
        }

        tokio::task::yield_now().await;
        let remaining = duration - elapsed;
        let advance_by = if remaining < step { remaining } else { step };
        tokio::time::advance(advance_by).await;
        elapsed += advance_by;
    }

    if let Ok(event) = rx.try_recv() {
        panic!("unexpected watcher event: {:?}", event);
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn watcher_reads_full_content_on_create() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("create.txt");

    let (mut rx, handle) = spawn_file_watcher(file_path.clone());
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    let part1 = b"hello ";
    let part2 = b"world";
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        file.write_all(part1).unwrap();
        file.sync_all().unwrap();
        file.write_all(part2).unwrap();
        file.sync_all().unwrap();
    }

    let content = wait_for_modified(&mut rx, Duration::from_millis(MAX_ADVANCE_MS)).await;
    let mut expected = Vec::new();
    expected.extend_from_slice(part1);
    expected.extend_from_slice(part2);

    assert_eq!(content, expected);

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn watcher_reads_full_content_on_modify() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("modify.txt");

    std::fs::write(&file_path, "initial content").unwrap();

    let (mut rx, handle) = spawn_file_watcher(file_path.clone());
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    let part1 = vec![b'a'; 256 * 1024];
    let part2 = vec![b'b'; 256 * 1024];
    {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&file_path)
            .unwrap();
        file.write_all(&part1).unwrap();
        file.sync_all().unwrap();
        file.write_all(&part2).unwrap();
        file.sync_all().unwrap();
    }

    let content = wait_for_modified(&mut rx, Duration::from_millis(MAX_ADVANCE_MS)).await;
    let mut expected = Vec::with_capacity(part1.len() + part2.len());
    expected.extend_from_slice(&part1);
    expected.extend_from_slice(&part2);

    assert_eq!(content, expected);
    assert_ne!(content, b"initial content");

    handle.abort();
    let _ = handle.await;
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn watcher_waits_for_write_completion() {
    use std::os::unix::io::AsRawFd;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("locked.txt");

    std::fs::write(&file_path, "seed").unwrap();

    let (mut rx, handle) = spawn_file_watcher(file_path.clone());
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();

    unsafe {
        libc::flock(file.as_raw_fd(), libc::LOCK_EX);
    }

    let part1 = b"partial ";
    let part2 = b"complete";

    file.write_all(part1).unwrap();
    file.sync_all().unwrap();

    assert_no_event(&mut rx, Duration::from_millis(FILE_DEBOUNCE_MS + 50)).await;

    file.write_all(part2).unwrap();
    file.sync_all().unwrap();

    assert_no_event(&mut rx, Duration::from_millis(150)).await;

    unsafe {
        libc::flock(file.as_raw_fd(), libc::LOCK_UN);
    }
    drop(file);

    let content = wait_for_modified(&mut rx, Duration::from_millis(MAX_ADVANCE_MS)).await;
    let mut expected = Vec::new();
    expected.extend_from_slice(part1);
    expected.extend_from_slice(part2);

    assert_eq!(content, expected);

    handle.abort();
    let _ = handle.await;
}

// ============================================================================
// Tests for wait_for_file_stability function
// ============================================================================

/// Test that wait_for_file_stability returns quickly for a stable file.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stability_check_returns_for_stable_file() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("stable.txt");

    // Create a file and wait a bit for it to "settle"
    std::fs::write(&file_path, "stable content").unwrap();

    // Advance time slightly past the stability interval
    tokio::time::advance(Duration::from_millis(STABILITY_CHECK_INTERVAL_MS + 10)).await;

    // Stability check should succeed
    let path_buf = file_path.clone();
    let result = wait_for_file_stability(&path_buf).await;
    assert!(
        result.is_ok(),
        "Stability check should succeed for stable file"
    );
}

/// Test that wait_for_file_stability waits for file size to stabilize.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stability_check_waits_for_size_to_stabilize() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("growing.txt");

    // Create initial file
    std::fs::write(&file_path, "initial").unwrap();

    let path_buf = file_path.clone();

    // Spawn the stability check
    let stability_handle = tokio::spawn(async move { wait_for_file_stability(&path_buf).await });

    // Advance time, but write more data before stability interval completes
    tokio::time::advance(Duration::from_millis(STABILITY_CHECK_INTERVAL_MS / 2)).await;
    tokio::task::yield_now().await;

    // Append more data - this resets the stability window
    {
        let mut file = OpenOptions::new().append(true).open(&file_path).unwrap();
        file.write_all(b" more data").unwrap();
        file.sync_all().unwrap();
    }

    // Advance past the stability interval
    tokio::time::advance(Duration::from_millis(STABILITY_CHECK_INTERVAL_MS + 20)).await;
    tokio::task::yield_now().await;

    // Now stability check should complete
    tokio::time::advance(Duration::from_millis(STABILITY_CHECK_INTERVAL_MS + 20)).await;

    let result = stability_handle.await.unwrap();
    assert!(result.is_ok(), "Stability check should eventually succeed");
}

/// Test that wait_for_file_stability returns error if file disappears.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stability_check_fails_if_file_disappears() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("vanishing.txt");

    // Create file
    std::fs::write(&file_path, "temporary").unwrap();

    let path_buf = file_path.clone();

    // Spawn the stability check
    let stability_handle = tokio::spawn(async move { wait_for_file_stability(&path_buf).await });

    // Give it a moment to start
    tokio::task::yield_now().await;

    // Delete the file during stability check
    std::fs::remove_file(&file_path).unwrap();

    // Advance time past the stability interval
    tokio::time::advance(Duration::from_millis(STABILITY_CHECK_INTERVAL_MS + 10)).await;
    tokio::task::yield_now().await;

    let result = stability_handle.await.unwrap();
    assert!(
        result.is_err(),
        "Stability check should fail when file disappears"
    );
}

/// Test that stability check catches atomic writes (delete + create pattern).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stability_check_handles_atomic_write() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("atomic.txt");
    let temp_file = temp_dir.path().join("atomic.txt.tmp");

    // Create initial file
    std::fs::write(&file_path, "original content").unwrap();

    let path_buf = file_path.clone();

    // Spawn stability check
    let stability_handle = tokio::spawn(async move { wait_for_file_stability(&path_buf).await });

    // Give it a moment
    tokio::task::yield_now().await;

    // Simulate atomic write: write to temp, rename to target
    std::fs::write(&temp_file, "new content via atomic write").unwrap();
    std::fs::rename(&temp_file, &file_path).unwrap();

    // Advance time for stability
    tokio::time::advance(Duration::from_millis(STABILITY_CHECK_INTERVAL_MS * 2 + 20)).await;
    tokio::task::yield_now().await;

    let result = stability_handle.await.unwrap();
    assert!(
        result.is_ok(),
        "Stability check should succeed after atomic write"
    );

    // Verify the file has the new content
    let content = std::fs::read_to_string(&file_path).unwrap();
    assert_eq!(content, "new content via atomic write");
}
