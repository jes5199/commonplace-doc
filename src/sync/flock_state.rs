//! Per-path state for flock-aware sync.
//!
//! Tracks pending outbound commits and queued inbound writes to ensure
//! we never overwrite local edits with stale server state.

use bytes::Bytes;
use std::collections::HashSet;
use std::time::Instant;

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
}
