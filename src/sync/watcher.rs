//! File and directory watcher tasks for local filesystem monitoring.
//!
//! This module provides async tasks that watch files and directories for changes
//! using the `notify` crate, with debouncing to handle rapid file modifications.

use crate::sync::{DirEvent, FileEvent, ScanOptions};
use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Default debounce duration for file watcher (100ms)
pub const FILE_DEBOUNCE_MS: u64 = 100;

/// Default debounce duration for directory watcher (500ms)
pub const DIR_DEBOUNCE_MS: u64 = 500;

/// Task that watches a single file for modifications.
///
/// This task sets up a file watcher using `notify` and sends [`FileEvent::Modified`]
/// events through the provided channel when the file changes. Events are debounced
/// to avoid sending multiple events for rapid modifications (e.g., text editors
/// that write files in multiple steps).
///
/// # Arguments
///
/// * `file_path` - Path to the file to watch
/// * `tx` - Channel sender for file events
///
/// # Behavior
///
/// - Watches the parent directory to catch atomic write patterns (rename to target)
/// - Filters events to only handle changes to the target file
/// - Debounces events with a 100ms delay
/// - Sends `FileEvent::Modified` after debounce period
/// - Logs errors but continues watching on watcher errors
/// - Exits when the receiver is dropped
///
/// # Atomic Write Support
///
/// Many editors use atomic writes: write to temp file, then rename to target.
/// This replaces the inode, so we watch the parent directory instead of the file
/// itself to reliably detect these changes.
pub async fn file_watcher_task(file_path: PathBuf, tx: mpsc::Sender<FileEvent>) {
    // Get the parent directory - we watch this to catch atomic renames
    let parent_dir = match file_path.parent() {
        Some(p) if p.as_os_str().is_empty() => PathBuf::from("."),
        Some(p) => p.to_path_buf(),
        None => {
            error!(
                "Cannot watch file without parent directory: {}",
                file_path.display()
            );
            return;
        }
    };

    // Canonicalize the file path for reliable comparison
    let canonical_file_path = match file_path.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            // File might not exist yet, use the original path
            file_path.clone()
        }
    };

    // Create a channel for notify events
    let (notify_tx, mut notify_rx) = mpsc::channel::<Result<Event, notify::Error>>(100);

    // Create watcher
    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default().with_poll_interval(Duration::from_millis(FILE_DEBOUNCE_MS)),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create file watcher: {}", e);
            return;
        }
    };

    // Watch the parent directory (non-recursive) to catch atomic renames
    if let Err(e) = watcher.watch(&parent_dir, RecursiveMode::NonRecursive) {
        error!(
            "Failed to watch directory {} for file {}: {}",
            parent_dir.display(),
            file_path.display(),
            e
        );
        return;
    }

    info!(
        "Watching file: {} (via parent dir: {})",
        file_path.display(),
        parent_dir.display()
    );

    // Debounce timer
    let debounce_duration = Duration::from_millis(FILE_DEBOUNCE_MS);
    let mut debounce_timer: Option<tokio::time::Instant> = None;

    // Helper to check if an event path matches our target file
    let matches_target = |event_path: &PathBuf| -> bool {
        // Try canonical comparison first
        if let Ok(canonical) = event_path.canonicalize() {
            if canonical == canonical_file_path {
                return true;
            }
        }
        // Fall back to file name comparison
        event_path.file_name() == file_path.file_name() && event_path.parent() == file_path.parent()
    };

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        // Check if any event path matches our target file
                        let affects_target = event.paths.iter().any(&matches_target);
                        if !affects_target {
                            continue;
                        }

                        // Handle atomic writes: editors often write to temp file then rename
                        let should_trigger = match event.kind {
                            // Explicit rename handling - atomic write pattern
                            EventKind::Modify(ModifyKind::Name(rename_mode)) => {
                                match rename_mode {
                                    RenameMode::To => {
                                        // File was renamed TO this path - atomic write complete
                                        debug!("File rename to (atomic write): {:?}", event);
                                        true
                                    }
                                    RenameMode::From => {
                                        // File was renamed FROM this path - it's gone
                                        debug!("File rename from (file removed): {:?}", event);
                                        false
                                    }
                                    RenameMode::Both | RenameMode::Any | RenameMode::Other => {
                                        // Platform couldn't determine direction - check if file exists
                                        if file_path.exists() {
                                            debug!("File rename (path exists): {:?}", event);
                                            true
                                        } else {
                                            debug!("File rename (path gone): {:?}", event);
                                            false
                                        }
                                    }
                                }
                            }
                            // Handle create events - file may have been deleted and recreated
                            EventKind::Create(_) => {
                                debug!("File created (possible delete+create pattern): {:?}", event);
                                true
                            }
                            // Standard modification
                            _ if event.kind.is_modify() => {
                                debug!("File modified event: {:?}", event);
                                true
                            }
                            _ => false,
                        };

                        if should_trigger {
                            // Reset debounce timer
                            debounce_timer = Some(tokio::time::Instant::now() + debounce_duration);
                        }
                    }
                    Err(e) => {
                        warn!("File watcher error: {}", e);
                    }
                }
            }
            _ = async {
                if let Some(deadline) = debounce_timer {
                    tokio::time::sleep_until(deadline).await;
                    true
                } else {
                    std::future::pending::<bool>().await
                }
            } => {
                // Debounce period elapsed - capture content BEFORE sending event
                // This prevents race conditions where SSE might overwrite the file
                // between event dispatch and upload_task reading the file.
                debounce_timer = None;

                // Read file content immediately to capture user's edit
                let content = match tokio::fs::read(&file_path).await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Failed to read file for upload: {}", e);
                        continue;
                    }
                };

                if tx.send(FileEvent::Modified(content)).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Task that watches a directory recursively for create/modify/delete events.
///
/// This task monitors a directory tree and sends [`DirEvent`] variants through
/// the provided channel. It handles:
/// - File creation
/// - File modification
/// - File deletion
/// - File renames (converted to delete + create events)
///
/// Events are debounced to consolidate rapid changes.
///
/// # Arguments
///
/// * `directory` - Root directory to watch recursively
/// * `tx` - Channel sender for directory events
/// * `options` - Scan options controlling hidden file handling and ignore patterns
///
/// # Behavior
///
/// - Watches recursively for all file system events
/// - Respects `include_hidden` option from ScanOptions
/// - Converts rename events to delete/create pairs
/// - Debounces events with a 500ms delay
/// - Logs errors but continues watching on watcher errors
/// - Exits when the receiver is dropped
pub async fn directory_watcher_task(
    directory: PathBuf,
    tx: mpsc::Sender<DirEvent>,
    options: ScanOptions,
) {
    let (notify_tx, mut notify_rx) = mpsc::channel::<Result<Event, notify::Error>>(100);

    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default().with_poll_interval(Duration::from_millis(DIR_DEBOUNCE_MS)),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create directory watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&directory, RecursiveMode::Recursive) {
        error!("Failed to watch directory {}: {}", directory.display(), e);
        return;
    }

    info!("Watching directory: {}", directory.display());

    let debounce_duration = Duration::from_millis(DIR_DEBOUNCE_MS);
    let mut pending_events: HashMap<PathBuf, DirEvent> = HashMap::new();
    let mut debounce_timer: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        for path in event.paths {
                            // Skip .commonplace.json files to prevent feedback loops
                            // These files are managed by the sync client itself
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy();
                                if name_str == ".commonplace.json" {
                                    debug!("Skipping schema file event: {}", path.display());
                                    continue;
                                }
                                // Skip hidden files if not configured
                                if !options.include_hidden && name_str.starts_with('.') {
                                    continue;
                                }
                            }

                            // Handle rename events specially - treat as delete+create
                            // so that sync tasks are properly stopped/started
                            let dir_event = if event.kind.is_create() {
                                Some(DirEvent::Created(path.clone()))
                            } else if let EventKind::Modify(ModifyKind::Name(rename_mode)) =
                                event.kind
                            {
                                // Rename events: treat as delete (old path) or create (new path)
                                match rename_mode {
                                    RenameMode::From => {
                                        // Source of rename - file moved away
                                        debug!("Rename from (treating as delete): {}", path.display());
                                        Some(DirEvent::Deleted(path.clone()))
                                    }
                                    RenameMode::To => {
                                        // Destination of rename - file moved here
                                        debug!("Rename to (treating as create): {}", path.display());
                                        Some(DirEvent::Created(path.clone()))
                                    }
                                    RenameMode::Both | RenameMode::Any | RenameMode::Other => {
                                        // Platform couldn't determine direction - check if path exists
                                        if path.exists() {
                                            debug!(
                                                "Rename (path exists, treating as create): {}",
                                                path.display()
                                            );
                                            Some(DirEvent::Created(path.clone()))
                                        } else {
                                            debug!(
                                                "Rename (path gone, treating as delete): {}",
                                                path.display()
                                            );
                                            Some(DirEvent::Deleted(path.clone()))
                                        }
                                    }
                                }
                            } else if event.kind.is_modify() {
                                Some(DirEvent::Modified(path.clone()))
                            } else if event.kind.is_remove() {
                                Some(DirEvent::Deleted(path.clone()))
                            } else {
                                None
                            };

                            if let Some(evt) = dir_event {
                                // Don't let Modified overwrite Created - a newly created file
                                // will typically receive Create then Modify events in rapid
                                // succession. We need to preserve Created so the file gets
                                // added to the schema.
                                let should_insert = match (&evt, pending_events.get(&path)) {
                                    (DirEvent::Modified(_), Some(DirEvent::Created(_))) => {
                                        // Keep the existing Created event
                                        debug!("Preserving Created event (not overwriting with Modified)");
                                        false
                                    }
                                    _ => true,
                                };
                                if should_insert {
                                    pending_events.insert(path, evt);
                                }
                                debounce_timer = Some(tokio::time::Instant::now() + debounce_duration);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Directory watcher error: {}", e);
                    }
                }
            }
            _ = async {
                if let Some(deadline) = debounce_timer {
                    tokio::time::sleep_until(deadline).await;
                    true
                } else {
                    std::future::pending::<bool>().await
                }
            } => {
                debounce_timer = None;
                for (_, event) in pending_events.drain() {
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Test that file_watcher_task detects atomic write patterns (write temp, rename to target).
    #[tokio::test]
    async fn test_file_watcher_detects_atomic_write() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let target_path = temp_dir.path().join("target.txt");

        // Create the initial file
        fs::write(&target_path, "initial content").expect("Failed to write initial file");

        let (tx, mut rx) = mpsc::channel::<FileEvent>(10);

        // Start the watcher task
        let target_path_clone = target_path.clone();
        let watcher_handle = tokio::spawn(async move {
            file_watcher_task(target_path_clone, tx).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Perform atomic write: write to temp file, then rename to target
        let temp_file_path = temp_dir.path().join(".target.txt.tmp");
        fs::write(&temp_file_path, "new content via atomic write").expect("Failed to write temp");
        fs::rename(&temp_file_path, &target_path).expect("Failed to rename");

        // Wait for the event with timeout
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

        // Cleanup
        watcher_handle.abort();

        match result {
            Ok(Some(FileEvent::Modified(content))) => {
                // Success - watcher detected the change and captured content
                assert!(!content.is_empty(), "Captured content should not be empty");
            }
            Ok(None) => panic!("Channel closed without receiving event"),
            Err(_) => panic!("Timeout waiting for file modification event after atomic write"),
        }
    }

    /// Test that file_watcher_task detects delete + create pattern.
    #[tokio::test]
    async fn test_file_watcher_detects_delete_create() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let target_path = temp_dir.path().join("target.txt");

        // Create the initial file
        fs::write(&target_path, "initial content").expect("Failed to write initial file");

        let (tx, mut rx) = mpsc::channel::<FileEvent>(10);

        // Start the watcher task
        let target_path_clone = target_path.clone();
        let watcher_handle = tokio::spawn(async move {
            file_watcher_task(target_path_clone, tx).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Perform delete + create
        fs::remove_file(&target_path).expect("Failed to delete");
        fs::write(&target_path, "recreated content").expect("Failed to recreate");

        // Wait for the event with timeout
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

        // Cleanup
        watcher_handle.abort();

        match result {
            Ok(Some(FileEvent::Modified(content))) => {
                // Success - watcher detected the change and captured content
                assert!(!content.is_empty(), "Captured content should not be empty");
            }
            Ok(None) => panic!("Channel closed without receiving event"),
            Err(_) => panic!("Timeout waiting for file modification event after delete+create"),
        }
    }

    /// Test that file_watcher_task continues working after atomic write.
    #[tokio::test]
    async fn test_file_watcher_continues_after_atomic_write() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let target_path = temp_dir.path().join("target.txt");

        // Create the initial file
        fs::write(&target_path, "initial").expect("Failed to write initial file");

        let (tx, mut rx) = mpsc::channel::<FileEvent>(10);

        // Start the watcher task
        let target_path_clone = target_path.clone();
        let watcher_handle = tokio::spawn(async move {
            file_watcher_task(target_path_clone, tx).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // First atomic write
        let temp_file_path = temp_dir.path().join(".target.txt.tmp1");
        fs::write(&temp_file_path, "first update").expect("Failed to write temp");
        fs::rename(&temp_file_path, &target_path).expect("Failed to rename");

        // Wait for first event
        let result1 = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            matches!(result1, Ok(Some(FileEvent::Modified(_)))),
            "First atomic write not detected"
        );

        // Give debounce time to reset
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Second atomic write - watcher should still be working
        let temp_file_path2 = temp_dir.path().join(".target.txt.tmp2");
        fs::write(&temp_file_path2, "second update").expect("Failed to write temp2");
        fs::rename(&temp_file_path2, &target_path).expect("Failed to rename2");

        // Wait for second event
        let result2 = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

        // Cleanup
        watcher_handle.abort();

        assert!(
            matches!(result2, Ok(Some(FileEvent::Modified(_)))),
            "Second atomic write not detected - watcher may have lost track of file"
        );
    }

    /// Test that file_watcher_task detects regular modifications.
    #[tokio::test]
    async fn test_file_watcher_detects_regular_modify() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let target_path = temp_dir.path().join("target.txt");

        // Create the initial file
        fs::write(&target_path, "initial content").expect("Failed to write initial file");

        let (tx, mut rx) = mpsc::channel::<FileEvent>(10);

        // Start the watcher task
        let target_path_clone = target_path.clone();
        let watcher_handle = tokio::spawn(async move {
            file_watcher_task(target_path_clone, tx).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Regular in-place modification
        fs::write(&target_path, "modified content").expect("Failed to modify file");

        // Wait for the event with timeout
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

        // Cleanup
        watcher_handle.abort();

        match result {
            Ok(Some(FileEvent::Modified(content))) => {
                // Success - watcher detected the change and captured content
                assert!(!content.is_empty(), "Captured content should not be empty");
            }
            Ok(None) => panic!("Channel closed without receiving event"),
            Err(_) => panic!("Timeout waiting for file modification event"),
        }
    }
}
