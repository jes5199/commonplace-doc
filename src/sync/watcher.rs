//! File and directory watcher tasks for local filesystem monitoring.
//!
//! This module provides async tasks that watch files and directories for changes
//! using the `notify` crate, with debouncing to handle rapid file modifications.

#[cfg(unix)]
use crate::sync::flock::{try_flock_shared, FlockResult};
use crate::sync::{DirEvent, FileEvent, ScanOptions};
use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Default debounce duration for file watcher (100ms)
pub const FILE_DEBOUNCE_MS: u64 = 100;

/// Default debounce duration for directory watcher (500ms)
pub const DIR_DEBOUNCE_MS: u64 = 500;

/// Create a notify watcher with a channel for receiving events.
///
/// This helper encapsulates the common watcher setup pattern:
/// - Creates an mpsc channel for events
/// - Creates a RecommendedWatcher with the callback wired to the channel
/// - Configures the poll interval
///
/// Returns the watcher and receiver, or None if watcher creation failed.
fn create_watcher(
    poll_interval_ms: u64,
    watcher_name: &str,
) -> Option<(
    RecommendedWatcher,
    mpsc::Receiver<Result<Event, notify::Error>>,
)> {
    let (notify_tx, notify_rx) = mpsc::channel::<Result<Event, notify::Error>>(100);

    let watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default().with_poll_interval(Duration::from_millis(poll_interval_ms)),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create {} watcher: {}", watcher_name, e);
            return None;
        }
    };

    Some((watcher, notify_rx))
}

/// Check if a filename matches common temp file patterns used by atomic writes.
///
/// Many applications write to a temp file first, then rename to the target.
/// These patterns help identify and ignore such intermediate files.
fn is_temp_file(name: &str) -> bool {
    // Python tempfile patterns (e.g., tmpXXXXXX)
    if name.starts_with("tmp") && name.len() >= 6 {
        let suffix = &name[3..];
        // Check if remaining chars look like tempfile random suffix
        if suffix.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return true;
        }
    }
    // Common temp file suffixes
    if name.ends_with(".tmp")
        || name.ends_with(".temp")
        || name.ends_with(".swp")
        || name.ends_with(".swx")
        || name.ends_with("~")
    {
        return true;
    }
    // Vim swap files
    if name.starts_with(".") && name.ends_with(".swp") {
        return true;
    }
    // Emacs backup files
    if name.starts_with("#") && name.ends_with("#") {
        return true;
    }
    false
}

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

    // Create watcher with channel
    let (mut watcher, mut notify_rx) = match create_watcher(FILE_DEBOUNCE_MS, "file") {
        Some(w) => w,
        None => return,
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
                // Use shared lock to signal to agents that a read is in progress
                #[cfg(unix)]
                let content = {
                    // Acquire shared lock to prevent agents from starting writes
                    let _guard = match try_flock_shared(&file_path, None).await {
                        Ok(FlockResult::Acquired(g)) => Some(g),
                        Ok(FlockResult::Timeout) => {
                            debug!("Shared lock timeout on {}, reading anyway", file_path.display());
                            None
                        }
                        Err(e) => {
                            debug!("Could not acquire shared lock on {}: {}", file_path.display(), e);
                            None
                        }
                    };
                    match tokio::fs::read(&file_path).await {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("Failed to read file for upload: {}", e);
                            continue;
                        }
                    }
                };
                #[cfg(not(unix))]
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
///
/// If `written_schemas` is provided, the watcher will detect user edits to
/// `.commonplace.json` files (changes that differ from what the sync client wrote)
/// and emit `DirEvent::SchemaModified` events for them. Without this parameter,
/// schema file changes are ignored entirely.
pub async fn directory_watcher_task(
    directory: PathBuf,
    tx: mpsc::Sender<DirEvent>,
    options: ScanOptions,
    written_schemas: Option<crate::sync::WrittenSchemas>,
) {
    // Create watcher with channel
    let (mut watcher, mut notify_rx) = match create_watcher(DIR_DEBOUNCE_MS, "directory") {
        Some(w) => w,
        None => return,
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
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy();

                                // Handle .commonplace.json files specially
                                if name_str == ".commonplace.json" {
                                    // If we have written_schemas tracking, check for user edits
                                    if let Some(ref ws) = written_schemas {
                                        // Read the file content
                                        if let Ok(content) = std::fs::read_to_string(&path) {
                                            let canonical = path.canonicalize().unwrap_or(path.clone());
                                            // Use try_read() instead of blocking_read() to avoid panic in async context
                                            let is_our_write = match ws.try_read() {
                                                Ok(ws_guard) => {
                                                    if let Some(last_written) = ws_guard.get(&canonical) {
                                                        // Compare as JSON to ignore whitespace differences
                                                        if let (Ok(last_json), Ok(new_json)) = (
                                                            serde_json::from_str::<serde_json::Value>(last_written),
                                                            serde_json::from_str::<serde_json::Value>(&content),
                                                        ) {
                                                            last_json == new_json
                                                        } else {
                                                            // If JSON parsing fails, compare raw strings
                                                            last_written == &content
                                                        }
                                                    } else {
                                                        // Not in our tracking map - could be initial load or user edit
                                                        // Treat as user edit to be safe
                                                        false
                                                    }
                                                }
                                                Err(_) => {
                                                    // Lock is held - treat as potential user edit to be safe
                                                    // This could cause extra resyncs but won't miss user edits
                                                    false
                                                }
                                            };

                                            if is_our_write {
                                                debug!("Skipping schema file event (our write): {}", path.display());
                                                continue;
                                            } else {
                                                info!("Detected user edit to schema file: {}", path.display());
                                                // Queue SchemaModified event (will be sent after debounce)
                                                pending_events.insert(
                                                    path.clone(),
                                                    DirEvent::SchemaModified(path.clone(), content),
                                                );
                                                if debounce_timer.is_none() {
                                                    debounce_timer = Some(tokio::time::Instant::now() + debounce_duration);
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                    // No written_schemas tracking - skip entirely (old behavior)
                                    debug!("Skipping schema file event: {}", path.display());
                                    continue;
                                }

                                // Skip hidden files if not configured
                                if !options.include_hidden && name_str.starts_with('.') {
                                    continue;
                                }
                                // Skip temp files used by atomic write patterns
                                // These are intermediate files that will be renamed to the target
                                if is_temp_file(&name_str) {
                                    debug!("Skipping temp file event: {}", path.display());
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
                                // Coalesce events to handle atomic writes and other patterns:
                                //
                                // 1. Modified after Created -> keep Created (new file gets modify events)
                                // 2. Created after Deleted -> convert to Modified (atomic write pattern)
                                //    This handles temp file + rename patterns where a file is replaced
                                // 3. All other cases -> replace with new event
                                let coalesced_event = match (&evt, pending_events.get(&path)) {
                                    (DirEvent::Modified(_), Some(DirEvent::Created(_))) => {
                                        // Keep the existing Created event
                                        debug!("Preserving Created event (not overwriting with Modified)");
                                        None
                                    }
                                    (DirEvent::Created(_), Some(DirEvent::Deleted(_))) => {
                                        // Atomic write pattern: file was deleted then created/renamed-to
                                        // Treat as Modified to avoid re-creating server node
                                        debug!(
                                            "Atomic write detected (Deleted + Created = Modified): {}",
                                            path.display()
                                        );
                                        Some(DirEvent::Modified(path.clone()))
                                    }
                                    _ => Some(evt),
                                };
                                if let Some(final_event) = coalesced_event {
                                    pending_events.insert(path, final_event);
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
                for (_path, event) in pending_events.drain() {
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Event from shadow directory watcher.
#[derive(Debug)]
pub struct ShadowWriteEvent {
    /// The inode key parsed from the shadow filename.
    pub inode_key: crate::sync::InodeKey,
    /// Path to the shadow file.
    pub shadow_path: PathBuf,
    /// Content of the shadow file.
    pub content: Vec<u8>,
}

/// Task that watches the shadow directory for writes to old inodes.
///
/// When a shadowed inode receives a write, this task reads its content and
/// sends a [`ShadowWriteEvent`] so the caller can create a CRDT update.
///
/// # Arguments
///
/// * `shadow_dir` - Directory containing shadow hardlinks
/// * `tx` - Channel sender for shadow write events
///
/// # Behavior
///
/// - Watches shadow directory non-recursively
/// - Parses shadow filenames ({devHex}-{inoHex}) to get inode keys
/// - On IN_MODIFY or IN_CLOSE_WRITE, reads content and sends event
/// - Debounces with 100ms delay to handle rapid writes
/// - Exits when receiver is dropped
#[cfg(unix)]
pub async fn shadow_watcher_task(shadow_dir: PathBuf, tx: mpsc::Sender<ShadowWriteEvent>) {
    use crate::sync::InodeKey;

    // Ensure shadow directory exists
    if let Err(e) = tokio::fs::create_dir_all(&shadow_dir).await {
        error!(
            "Failed to create shadow directory {}: {}",
            shadow_dir.display(),
            e
        );
        return;
    }

    // Create watcher with channel
    let (mut watcher, mut notify_rx) = match create_watcher(FILE_DEBOUNCE_MS, "shadow") {
        Some(w) => w,
        None => return,
    };

    // Watch shadow directory non-recursively
    if let Err(e) = watcher.watch(&shadow_dir, RecursiveMode::NonRecursive) {
        error!(
            "Failed to watch shadow directory {}: {}",
            shadow_dir.display(),
            e
        );
        return;
    }

    info!("Watching shadow directory: {}", shadow_dir.display());

    let debounce_duration = Duration::from_millis(FILE_DEBOUNCE_MS);
    let mut pending_events: std::collections::HashMap<PathBuf, tokio::time::Instant> =
        std::collections::HashMap::new();

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        // Only handle modify events (writes to shadow files)
                        if !event.kind.is_modify() {
                            continue;
                        }

                        for path in event.paths {
                            // Skip temp files
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy();
                                if is_temp_file(&name_str) {
                                    continue;
                                }
                            }

                            debug!("Shadow write detected: {}", path.display());
                            pending_events.insert(path, tokio::time::Instant::now() + debounce_duration);
                        }
                    }
                    Err(e) => {
                        warn!("Shadow watcher error: {}", e);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                // Check for debounced events ready to process
                let now = tokio::time::Instant::now();
                let ready: Vec<PathBuf> = pending_events
                    .iter()
                    .filter(|(_, deadline)| now >= **deadline)
                    .map(|(path, _)| path.clone())
                    .collect();

                for path in ready {
                    pending_events.remove(&path);

                    // Parse inode key from filename
                    let filename = match path.file_name() {
                        Some(name) => name.to_string_lossy().to_string(),
                        None => continue,
                    };

                    let inode_key = match InodeKey::from_shadow_filename(&filename) {
                        Some(key) => key,
                        None => {
                            debug!("Could not parse inode key from shadow filename: {}", filename);
                            continue;
                        }
                    };

                    // Read content
                    let content = match tokio::fs::read(&path).await {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("Failed to read shadow file {}: {}", path.display(), e);
                            continue;
                        }
                    };

                    debug!(
                        "Shadow write event: inode {:x}-{:x}, {} bytes",
                        inode_key.dev,
                        inode_key.ino,
                        content.len()
                    );

                    let event = ShadowWriteEvent {
                        inode_key,
                        shadow_path: path,
                        content,
                    };

                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// GC interval for shadow inode cleanup (5 minutes)
pub const SHADOW_GC_INTERVAL: Duration = Duration::from_secs(300);

/// Periodic garbage collection task for shadow inodes.
///
/// This task runs every SHADOW_GC_INTERVAL (5 minutes) and:
/// 1. Calls tracker.gc() to find stale shadow entries
/// 2. Removes the returned shadow hardlinks from disk
///
/// Exits when the tracker Arc is the last reference (sync shutdown).
#[cfg(unix)]
pub async fn shadow_gc_task(tracker: Arc<RwLock<crate::sync::InodeTracker>>) {
    use tokio::time::interval;

    let mut gc_interval = interval(SHADOW_GC_INTERVAL);
    gc_interval.tick().await; // Skip immediate first tick

    loop {
        gc_interval.tick().await;

        // Run GC and get shadow paths to clean up
        let cleaned_paths = {
            let mut t = tracker.write().await;
            t.gc()
        };

        if !cleaned_paths.is_empty() {
            info!(
                "Shadow GC: cleaning up {} stale shadow links",
                cleaned_paths.len()
            );

            for path in cleaned_paths {
                if let Err(e) = std::fs::remove_file(&path) {
                    // ENOENT is fine - file might already be gone
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!("Failed to remove shadow hardlink {}: {}", path.display(), e);
                    }
                } else {
                    debug!("Removed stale shadow hardlink: {}", path.display());
                }
            }
        }
    }
}

/// Spawn all shadow watcher tasks and return their handles.
///
/// This helper encapsulates the common pattern of starting:
/// 1. shadow_watcher_task - watches shadow directory for write events
/// 2. shadow_write_handler_task - processes write events and pushes to server
/// 3. shadow_gc_task - periodically cleans up stale shadow entries
///
/// Returns the three task handles in a tuple.
#[cfg(unix)]
pub fn spawn_shadow_tasks(
    shadow_dir: PathBuf,
    client: reqwest::Client,
    server: String,
    tracker: Arc<RwLock<crate::sync::InodeTracker>>,
    use_paths: bool,
    author: String,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    use crate::sync::sse::shadow_write_handler_task;
    use crate::sync::ShadowWriteEvent;

    // Create channel for shadow write events
    let (shadow_tx, shadow_rx) = mpsc::channel::<ShadowWriteEvent>(100);

    // Start shadow watcher
    let watcher = tokio::spawn(shadow_watcher_task(shadow_dir, shadow_tx));

    // Start shadow write handler
    let handler = tokio::spawn(shadow_write_handler_task(
        client,
        server,
        shadow_rx,
        tracker.clone(),
        use_paths,
        author,
    ));

    // Start periodic shadow GC task
    let gc = tokio::spawn(shadow_gc_task(tracker));

    (watcher, handler, gc)
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

    /// Test that directory_watcher_task coalesces Deleted+Created into Modified.
    /// This is the atomic write pattern: delete target, then create/rename to target.
    #[tokio::test]
    async fn test_directory_watcher_coalesces_delete_create_to_modified() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let target_path = temp_dir.path().join("target.txt");

        // Create the initial file
        fs::write(&target_path, "initial content").expect("Failed to write initial file");

        let (tx, mut rx) = mpsc::channel::<DirEvent>(10);
        let options = ScanOptions {
            include_hidden: false,
            ignore_patterns: vec![],
        };

        // Start the watcher task
        let dir_clone = temp_dir.path().to_path_buf();
        let watcher_handle = tokio::spawn(async move {
            directory_watcher_task(dir_clone, tx, options, None).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Simulate atomic write pattern: delete then create
        fs::remove_file(&target_path).expect("Failed to delete");
        fs::write(&target_path, "new content via delete+create").expect("Failed to create");

        // Wait for the event with timeout (directory watcher has 500ms debounce)
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

        // Cleanup
        watcher_handle.abort();

        match result {
            Ok(Some(DirEvent::Modified(path))) => {
                // Success - events were coalesced into Modified
                assert_eq!(
                    path.file_name().unwrap().to_str().unwrap(),
                    "target.txt",
                    "Modified event should be for target file"
                );
            }
            Ok(Some(DirEvent::Created(path))) => {
                panic!(
                    "Expected Modified but got Created for {} - coalescing failed",
                    path.display()
                );
            }
            Ok(Some(DirEvent::Deleted(path))) => {
                panic!(
                    "Expected Modified but got Deleted for {} - events not coalesced",
                    path.display()
                );
            }
            Ok(Some(DirEvent::SchemaModified(path, _))) => {
                panic!("Unexpected SchemaModified event for {}", path.display());
            }
            Ok(None) => panic!("Channel closed without receiving event"),
            Err(_) => panic!("Timeout waiting for directory event"),
        }
    }

    /// Test that directory_watcher_task ignores temp files.
    #[tokio::test]
    async fn test_directory_watcher_ignores_temp_files() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let (tx, mut rx) = mpsc::channel::<DirEvent>(10);
        let options = ScanOptions {
            include_hidden: false,
            ignore_patterns: vec![],
        };

        // Start the watcher task
        let dir_clone = temp_dir.path().to_path_buf();
        let watcher_handle = tokio::spawn(async move {
            directory_watcher_task(dir_clone, tx, options, None).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Create various temp files that should be ignored
        let temp_files = [
            "tmpXXXabc123", // Python tempfile pattern
            "file.tmp",     // .tmp suffix
            "file.temp",    // .temp suffix
            "file~",        // Backup file
        ];
        for name in &temp_files {
            let path = temp_dir.path().join(name);
            fs::write(&path, "temp content").expect("Failed to write temp file");
        }

        // Also create a normal file that should trigger an event
        let normal_path = temp_dir.path().join("normal.txt");
        fs::write(&normal_path, "normal content").expect("Failed to write normal file");

        // Wait for events (directory watcher has 500ms debounce)
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

        // Cleanup
        watcher_handle.abort();

        match result {
            Ok(Some(event)) => {
                // Should only receive event for normal.txt
                let path = match &event {
                    DirEvent::Created(p) | DirEvent::Modified(p) | DirEvent::Deleted(p) => p,
                    DirEvent::SchemaModified(p, _) => p,
                };
                let name = path.file_name().unwrap().to_str().unwrap();
                assert_eq!(
                    name, "normal.txt",
                    "Only normal.txt should trigger event, got: {}",
                    name
                );
            }
            Ok(None) => panic!("Channel closed without receiving event"),
            Err(_) => panic!("Timeout waiting for directory event - maybe normal.txt was missed?"),
        }
    }

    /// Test is_temp_file function.
    #[test]
    fn test_is_temp_file_detection() {
        // Should match temp file patterns
        assert!(is_temp_file("tmpXXXabc"), "Python tempfile pattern");
        assert!(is_temp_file("tmp123456"), "Python tempfile numeric");
        assert!(is_temp_file("file.tmp"), ".tmp suffix");
        assert!(is_temp_file("file.temp"), ".temp suffix");
        assert!(is_temp_file("file.swp"), ".swp suffix");
        assert!(is_temp_file("file~"), "Backup suffix");
        assert!(is_temp_file("#autosave#"), "Emacs autosave");

        // Should NOT match regular files
        assert!(!is_temp_file("file.txt"), "Regular text file");
        assert!(!is_temp_file("tmp"), "Just 'tmp' is too short");
        assert!(!is_temp_file("template.txt"), "Contains 'tmp' but not temp");
        assert!(!is_temp_file("input.txt"), "Normal input file");
    }

    /// Test that file_watcher_task detects MULTIPLE consecutive modifications.
    /// This verifies the watcher continues working after the first event.
    #[tokio::test]
    async fn test_file_watcher_detects_multiple_modifications() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let target_path = temp_dir.path().join("target.txt");

        // Create initial file
        fs::write(&target_path, "initial content").expect("Failed to write initial file");

        let (tx, mut rx) = mpsc::channel::<FileEvent>(10);

        // Start the watcher task
        let target_path_clone = target_path.clone();
        let watcher_handle = tokio::spawn(async move {
            file_watcher_task(target_path_clone, tx).await;
        });

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // First modification
        fs::write(&target_path, "content v1").expect("Failed to modify file");

        // Wait for the first event
        let result1 = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            matches!(result1, Ok(Some(FileEvent::Modified(_)))),
            "First modification not detected: {:?}",
            result1
        );

        // Wait for debounce to fully settle
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Second modification
        fs::write(&target_path, "content v2").expect("Failed to modify file again");

        // Wait for the second event
        let result2 = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            matches!(result2, Ok(Some(FileEvent::Modified(_)))),
            "Second modification not detected: {:?}",
            result2
        );

        // Wait for debounce
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Third modification
        fs::write(&target_path, "content v3").expect("Failed to modify file third time");

        // Wait for the third event
        let result3 = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            matches!(result3, Ok(Some(FileEvent::Modified(_)))),
            "Third modification not detected: {:?}",
            result3
        );

        watcher_handle.abort();
    }
}
