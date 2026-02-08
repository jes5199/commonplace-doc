//! File and directory watcher tasks for local filesystem monitoring.
//!
//! Re-exports from the `commonplace-watcher` crate.
//! `spawn_shadow_tasks` remains here because it depends on transport
//! (`shadow_write_handler_task`).

pub use commonplace_watcher::watcher::*;

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
    shadow_dir: std::path::PathBuf,
    client: reqwest::Client,
    server: String,
    tracker: std::sync::Arc<tokio::sync::RwLock<super::state::InodeTracker>>,
    use_paths: bool,
    author: String,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    use crate::sync::shadow::shadow_write_handler_task;

    // Create channel for shadow write events
    let (shadow_tx, shadow_rx) = tokio::sync::mpsc::channel::<ShadowWriteEvent>(100);

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
