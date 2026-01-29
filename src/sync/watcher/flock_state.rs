//! Per-path state for flock-aware sync.
//!
//! Tracks pending outbound commits and queued inbound writes to ensure
//! we never overwrite local edits with stale server state.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// State tracked per synced file path
#[derive(Debug)]
pub struct PathState {
    /// Current inode for this path (for shadow tracking)
    pub current_inode: u64,

    /// Commits uploaded but not yet confirmed in server response.
    /// We must verify all of these are ancestors of incoming commits
    /// before writing inbound updates.
    pub pending_outbound: HashSet<String>,

    /// Server update waiting to be written.
    /// Blocked by pending_outbound or flock contention.
    pub pending_inbound: Option<InboundWrite>,
}

/// A queued inbound write from the server
#[derive(Debug, Clone)]
pub struct InboundWrite {
    /// Content to write
    pub content: Bytes,

    /// Commit ID of this update
    pub commit_id: String,

    /// When we received this update
    pub received_at: Instant,
}

impl PathState {
    /// Create new state for a path
    pub fn new(inode: u64) -> Self {
        Self {
            current_inode: inode,
            pending_outbound: HashSet::new(),
            pending_inbound: None,
        }
    }

    /// Check if we have any pending outbound commits
    pub fn has_pending_outbound(&self) -> bool {
        !self.pending_outbound.is_empty()
    }

    /// Record that we uploaded a commit
    pub fn add_pending_outbound(&mut self, commit_id: String) {
        self.pending_outbound.insert(commit_id);
    }

    /// Remove confirmed commits from pending_outbound
    pub fn confirm_outbound(&mut self, commit_ids: &[String]) {
        for cid in commit_ids {
            self.pending_outbound.remove(cid);
        }
    }

    /// Queue an inbound write (replaces any existing queued write)
    pub fn queue_inbound(&mut self, content: Bytes, commit_id: String) {
        self.pending_inbound = Some(InboundWrite {
            content,
            commit_id,
            received_at: Instant::now(),
        });
    }

    /// Take the pending inbound write if any
    pub fn take_pending_inbound(&mut self) -> Option<InboundWrite> {
        self.pending_inbound.take()
    }
}

/// Shared manager for flock-aware sync state across all file paths.
///
/// This wraps a map of PathState instances, one per synced file path.
/// It provides thread-safe access for both the upload path (to record
/// pending outbound commits) and the SSE path (to check/confirm ancestry).
#[derive(Debug, Clone)]
pub struct FlockSyncState {
    /// Map of file path to its PathState
    inner: Arc<RwLock<HashMap<PathBuf, PathState>>>,
}

impl FlockSyncState {
    /// Create a new empty FlockSyncState manager
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create PathState for a path.
    ///
    /// If the path doesn't have state yet, creates one with inode 0.
    /// The caller should update the inode if they have it.
    pub async fn get_or_create(&self, path: &Path) -> PathState {
        let mut states = self.inner.write().await;
        states
            .entry(path.to_path_buf())
            .or_insert_with(|| PathState::new(0))
            .clone()
    }

    /// Record that we uploaded a commit for a path.
    ///
    /// This is called by the upload path after a successful upload.
    /// The commit_id is added to pending_outbound for this path.
    pub async fn record_outbound(&self, path: &PathBuf, commit_id: String) {
        let mut states = self.inner.write().await;
        let state = states
            .entry(path.clone())
            .or_insert_with(|| PathState::new(0));
        state.add_pending_outbound(commit_id.clone());
        tracing::debug!(
            ?path,
            commit_id,
            pending_count = state.pending_outbound.len(),
            "recorded pending outbound commit"
        );
    }

    /// Confirm outbound commits for a path.
    ///
    /// This is called when SSE receives a commit that includes our pending
    /// commits as ancestors. Removes the confirmed commits from pending_outbound.
    pub async fn confirm_outbound(&self, path: &PathBuf, commit_ids: &[String]) {
        let mut states = self.inner.write().await;
        if let Some(state) = states.get_mut(path) {
            state.confirm_outbound(commit_ids);
            tracing::debug!(
                ?path,
                ?commit_ids,
                remaining = state.pending_outbound.len(),
                "confirmed outbound commits"
            );
        }
    }

    /// Check if a path has any pending outbound commits.
    pub async fn has_pending_outbound(&self, path: &PathBuf) -> bool {
        let states = self.inner.read().await;
        states
            .get(path)
            .map(|s| s.has_pending_outbound())
            .unwrap_or(false)
    }

    /// Get the set of pending outbound commits for a path.
    pub async fn get_pending_outbound(&self, path: &PathBuf) -> HashSet<String> {
        let states = self.inner.read().await;
        states
            .get(path)
            .map(|s| s.pending_outbound.clone())
            .unwrap_or_default()
    }

    /// Queue an inbound write for a path.
    ///
    /// This is called when we can't write immediately (e.g., pending outbound
    /// commits aren't confirmed yet).
    pub async fn queue_inbound(&self, path: &PathBuf, content: Bytes, commit_id: String) {
        let mut states = self.inner.write().await;
        let state = states
            .entry(path.clone())
            .or_insert_with(|| PathState::new(0));
        state.queue_inbound(content, commit_id.clone());
        tracing::debug!(?path, commit_id, "queued inbound write");
    }

    /// Take any pending inbound write for a path.
    ///
    /// Returns None if there's no pending inbound write.
    pub async fn take_pending_inbound(&self, path: &PathBuf) -> Option<InboundWrite> {
        let mut states = self.inner.write().await;
        states.get_mut(path).and_then(|s| s.take_pending_inbound())
    }

    /// Execute a function with mutable access to a path's state.
    ///
    /// This is useful when you need to perform multiple operations atomically.
    pub async fn with_state<F, R>(&self, path: &Path, f: F) -> R
    where
        F: FnOnce(&mut PathState) -> R,
    {
        let mut states = self.inner.write().await;
        let state = states
            .entry(path.to_path_buf())
            .or_insert_with(|| PathState::new(0));
        f(state)
    }

    /// Remove state for a path (e.g., when file is deleted).
    pub async fn remove(&self, path: &Path) {
        let mut states = self.inner.write().await;
        states.remove(path);
    }

    /// Update the current inode for a path.
    pub async fn set_inode(&self, path: &Path, inode: u64) {
        let mut states = self.inner.write().await;
        let state = states
            .entry(path.to_path_buf())
            .or_insert_with(|| PathState::new(inode));
        state.current_inode = inode;
    }
}

impl Default for FlockSyncState {
    fn default() -> Self {
        Self::new()
    }
}

/// Record an upload result in the flock state.
///
/// This is a convenience function for integration into upload paths.
/// It records the commit_id as pending outbound, which will be confirmed
/// when SSE receives a commit that includes it as an ancestor.
///
/// If `flock_state` is None, this is a no-op (for backward compatibility
/// with code that doesn't have flock state available).
pub async fn record_upload_result(
    flock_state: Option<&FlockSyncState>,
    path: &PathBuf,
    commit_id: &str,
) {
    if let Some(state) = flock_state {
        state.record_outbound(path, commit_id.to_string()).await;
    }
}

/// Process pending inbound writes after outbound commits are confirmed.
///
/// This should be called after SSE confirms that pending outbound commits
/// are ancestors of an incoming commit. If there's a queued inbound write,
/// it can now be processed.
///
/// Returns the pending inbound content and commit_id if there was one queued.
pub async fn process_pending_inbound_after_confirm(
    flock_state: &FlockSyncState,
    path: &PathBuf,
) -> Option<(Bytes, String)> {
    if let Some(inbound) = flock_state.take_pending_inbound(path).await {
        Some((inbound.content, inbound.commit_id))
    } else {
        None
    }
}

// Make PathState Clone for easier manipulation
impl Clone for PathState {
    fn clone(&self) -> Self {
        Self {
            current_inode: self.current_inode,
            pending_outbound: self.pending_outbound.clone(),
            pending_inbound: self.pending_inbound.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state_has_no_pending() {
        let state = PathState::new(12345);
        assert!(!state.has_pending_outbound());
        assert!(state.pending_inbound.is_none());
    }

    #[test]
    fn test_add_and_confirm_outbound() {
        let mut state = PathState::new(12345);

        state.add_pending_outbound("cid1".to_string());
        state.add_pending_outbound("cid2".to_string());

        assert!(state.has_pending_outbound());
        assert_eq!(state.pending_outbound.len(), 2);

        state.confirm_outbound(&["cid1".to_string()]);
        assert!(state.has_pending_outbound());
        assert_eq!(state.pending_outbound.len(), 1);

        state.confirm_outbound(&["cid2".to_string()]);
        assert!(!state.has_pending_outbound());
    }

    #[test]
    fn test_queue_inbound_replaces_existing() {
        let mut state = PathState::new(12345);

        state.queue_inbound(Bytes::from("content1"), "cid1".to_string());
        assert_eq!(state.pending_inbound.as_ref().unwrap().commit_id, "cid1");

        state.queue_inbound(Bytes::from("content2"), "cid2".to_string());
        assert_eq!(state.pending_inbound.as_ref().unwrap().commit_id, "cid2");
    }

    #[test]
    fn test_take_pending_inbound() {
        let mut state = PathState::new(12345);

        state.queue_inbound(Bytes::from("content"), "cid".to_string());

        let taken = state.take_pending_inbound();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().commit_id, "cid");

        // Second take returns None
        assert!(state.take_pending_inbound().is_none());
    }

    // Tests for FlockSyncState manager

    #[tokio::test]
    async fn test_flock_sync_state_record_outbound() {
        let manager = FlockSyncState::new();
        let path = PathBuf::from("/test/file.txt");

        // Initially no pending
        assert!(!manager.has_pending_outbound(&path).await);

        // Record outbound
        manager.record_outbound(&path, "cid1".to_string()).await;
        assert!(manager.has_pending_outbound(&path).await);

        let pending = manager.get_pending_outbound(&path).await;
        assert_eq!(pending.len(), 1);
        assert!(pending.contains("cid1"));
    }

    #[tokio::test]
    async fn test_flock_sync_state_confirm_outbound() {
        let manager = FlockSyncState::new();
        let path = PathBuf::from("/test/file.txt");

        manager.record_outbound(&path, "cid1".to_string()).await;
        manager.record_outbound(&path, "cid2".to_string()).await;
        assert_eq!(manager.get_pending_outbound(&path).await.len(), 2);

        // Confirm one
        manager.confirm_outbound(&path, &["cid1".to_string()]).await;
        assert!(manager.has_pending_outbound(&path).await);
        assert_eq!(manager.get_pending_outbound(&path).await.len(), 1);

        // Confirm the other
        manager.confirm_outbound(&path, &["cid2".to_string()]).await;
        assert!(!manager.has_pending_outbound(&path).await);
    }

    #[tokio::test]
    async fn test_flock_sync_state_queue_inbound() {
        let manager = FlockSyncState::new();
        let path = PathBuf::from("/test/file.txt");

        manager
            .queue_inbound(&path, Bytes::from("content"), "cid1".to_string())
            .await;

        let taken = manager.take_pending_inbound(&path).await;
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().commit_id, "cid1");

        // Second take returns None
        assert!(manager.take_pending_inbound(&path).await.is_none());
    }

    #[tokio::test]
    async fn test_flock_sync_state_with_state() {
        let manager = FlockSyncState::new();
        let path = PathBuf::from("/test/file.txt");

        // Use with_state for atomic operations
        manager
            .with_state(&path, |state| {
                state.add_pending_outbound("cid1".to_string());
                state.add_pending_outbound("cid2".to_string());
            })
            .await;

        assert_eq!(manager.get_pending_outbound(&path).await.len(), 2);
    }

    #[tokio::test]
    async fn test_flock_sync_state_clone() {
        let manager1 = FlockSyncState::new();
        let path = PathBuf::from("/test/file.txt");

        manager1.record_outbound(&path, "cid1".to_string()).await;

        // Clone shares the same underlying state
        let manager2 = manager1.clone();
        assert!(manager2.has_pending_outbound(&path).await);

        // Changes through one are visible to the other
        manager2.record_outbound(&path, "cid2".to_string()).await;
        assert_eq!(manager1.get_pending_outbound(&path).await.len(), 2);
    }

    #[tokio::test]
    async fn test_flock_sync_state_remove() {
        let manager = FlockSyncState::new();
        let path = PathBuf::from("/test/file.txt");

        manager.record_outbound(&path, "cid1".to_string()).await;
        assert!(manager.has_pending_outbound(&path).await);

        manager.remove(&path).await;
        assert!(!manager.has_pending_outbound(&path).await);
    }
}
