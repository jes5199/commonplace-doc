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
/// - Watches for modify events on the file
/// - Debounces events with a 100ms delay
/// - Sends `FileEvent::Modified` after debounce period
/// - Logs errors but continues watching on watcher errors
/// - Exits when the receiver is dropped
pub async fn file_watcher_task(file_path: PathBuf, tx: mpsc::Sender<FileEvent>) {
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

    // Watch the file
    if let Err(e) = watcher.watch(&file_path, RecursiveMode::NonRecursive) {
        error!("Failed to watch file {}: {}", file_path.display(), e);
        return;
    }

    info!("Watching file: {}", file_path.display());

    // Debounce timer
    let debounce_duration = Duration::from_millis(FILE_DEBOUNCE_MS);
    let mut debounce_timer: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        // Check if this is a modify event
                        if event.kind.is_modify() {
                            debug!("File modified event: {:?}", event);
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
                // Debounce period elapsed, send event
                debounce_timer = None;
                if tx.send(FileEvent::Modified).await.is_err() {
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
                            // Skip hidden files if not configured
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy();
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
                                pending_events.insert(path, evt);
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
