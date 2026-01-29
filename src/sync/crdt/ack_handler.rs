//! Client-side ack message handling for the cyan sync protocol.
//!
//! This module handles `Ack` messages received from the server after
//! publishing commits to the edits topic.
//!
//! See docs/plans/2026-01-27-cyan-sync-protocol-design.md for protocol details.
//!
//! ## Ack Processing
//!
//! When the server persists a commit, it sends an ack to the client's sync topic:
//! - `{workspace}/sync/{document_id}/{client-id}`
//!
//! ### On `accepted: true`:
//! - Prune the commit from pending queue (it's now durable on server)
//! - If `head_advanced: false`, the client may need to sync and merge (server diverged)
//!
//! ### On `accepted: false`:
//! Check `error.code` and recover:
//! - `missing_parents` → Push the parent commits first, then retry
//! - `cid_mismatch` → Log as bug, don't retry
//! - `invalid_commit` → Log as bug, fix and retry
//! - `no_common_ancestor` → Retry with deeper have set

use super::crdt_state::CrdtPeerState;
use super::sync_state_machine::SyncEvent;
use crate::mqtt::messages::{SyncError, SyncMessage};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Default timeout for pending commits (before considering them lost).
pub const PENDING_COMMIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum number of retries for recoverable errors.
pub const MAX_RETRIES: u32 = 3;

/// A commit that has been published and is awaiting server acknowledgment.
#[derive(Debug, Clone)]
pub struct PendingCommit {
    /// Correlation ID (req field from EditMessage)
    pub req_id: String,
    /// The commit CID
    pub cid: String,
    /// Parent commit CIDs
    pub parents: Vec<String>,
    /// Base64-encoded Yjs update
    pub update_b64: String,
    /// Author identifier
    pub author: String,
    /// When the commit was published
    pub published_at: Instant,
    /// Number of times this commit has been retried
    pub retry_count: u32,
    /// Whether this is a retry of a failed commit
    pub is_retry: bool,
}

impl PendingCommit {
    /// Create a new pending commit.
    pub fn new(
        req_id: String,
        cid: String,
        parents: Vec<String>,
        update_b64: String,
        author: String,
    ) -> Self {
        Self {
            req_id,
            cid,
            parents,
            update_b64,
            author,
            published_at: Instant::now(),
            retry_count: 0,
            is_retry: false,
        }
    }

    /// Check if this commit has timed out.
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.published_at.elapsed() > timeout
    }

    /// Increment retry count and mark as retry.
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.is_retry = true;
        self.published_at = Instant::now();
    }

    /// Check if we should retry this commit.
    pub fn should_retry(&self) -> bool {
        self.retry_count < MAX_RETRIES
    }
}

/// Result of processing an ack message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckResult {
    /// Commit was accepted and HEAD advanced (fast-forward)
    AcceptedAdvanced,
    /// Commit was accepted but HEAD did not advance (divergent branch)
    /// Client should initiate sync to merge with server HEAD.
    AcceptedDiverged,
    /// Commit was rejected due to missing parents.
    /// Contains the list of parent CIDs that need to be pushed first.
    MissingParents(Vec<String>),
    /// Commit was rejected due to CID mismatch (bug).
    /// Contains expected and computed CIDs.
    CidMismatch { expected: String, computed: String },
    /// Commit was rejected due to invalid commit data (bug).
    /// Contains field name and error message.
    InvalidCommit { field: String, message: String },
    /// Commit was rejected due to no common ancestor found.
    /// Client should retry with deeper have set.
    NoCommonAncestor {
        have: Vec<String>,
        server_head: String,
    },
    /// Unknown error (no structured error provided).
    UnknownError(String),
    /// Ack for unknown commit (already pruned or never tracked).
    UnknownCommit,
}

impl AckResult {
    /// Returns true if this result indicates the commit was accepted.
    pub fn is_accepted(&self) -> bool {
        matches!(
            self,
            AckResult::AcceptedAdvanced | AckResult::AcceptedDiverged
        )
    }

    /// Returns true if this result indicates a recoverable error.
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            AckResult::MissingParents(_) | AckResult::NoCommonAncestor { .. }
        )
    }

    /// Returns true if this result indicates a bug (non-recoverable).
    pub fn is_bug(&self) -> bool {
        matches!(
            self,
            AckResult::CidMismatch { .. } | AckResult::InvalidCommit { .. }
        )
    }
}

/// Tracker for pending commits awaiting acknowledgment.
///
/// This structure maintains a queue of commits that have been published
/// but not yet acknowledged by the server.
#[derive(Debug, Default)]
pub struct PendingCommitTracker {
    /// Pending commits keyed by correlation ID (req)
    by_req_id: HashMap<String, PendingCommit>,
    /// Pending commits keyed by CID (for acks without req correlation)
    by_cid: HashMap<String, String>, // cid -> req_id
    /// Order of commits for timeout processing
    order: VecDeque<String>, // req_ids in order
    /// Timeout for pending commits
    timeout: Duration,
}

impl PendingCommitTracker {
    /// Create a new tracker with the default timeout.
    pub fn new() -> Self {
        Self::with_timeout(PENDING_COMMIT_TIMEOUT)
    }

    /// Create a new tracker with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            by_req_id: HashMap::new(),
            by_cid: HashMap::new(),
            order: VecDeque::new(),
            timeout,
        }
    }

    /// Track a newly published commit.
    pub fn track(&mut self, commit: PendingCommit) {
        let req_id = commit.req_id.clone();
        let cid = commit.cid.clone();

        debug!(
            "Tracking pending commit: req_id={}, cid={}, parents={:?}",
            req_id, cid, commit.parents
        );

        self.by_cid.insert(cid, req_id.clone());
        self.order.push_back(req_id.clone());
        self.by_req_id.insert(req_id, commit);
    }

    /// Get a pending commit by correlation ID.
    pub fn get(&self, req_id: &str) -> Option<&PendingCommit> {
        self.by_req_id.get(req_id)
    }

    /// Get a pending commit by CID.
    pub fn get_by_cid(&self, cid: &str) -> Option<&PendingCommit> {
        self.by_cid
            .get(cid)
            .and_then(|req_id| self.by_req_id.get(req_id))
    }

    /// Remove and return a pending commit by correlation ID.
    pub fn remove(&mut self, req_id: &str) -> Option<PendingCommit> {
        if let Some(commit) = self.by_req_id.remove(req_id) {
            self.by_cid.remove(&commit.cid);
            // Note: We don't remove from order queue (lazy cleanup)
            debug!(
                "Removed pending commit: req_id={}, cid={}",
                req_id, commit.cid
            );
            Some(commit)
        } else {
            None
        }
    }

    /// Remove and return a pending commit by CID.
    pub fn remove_by_cid(&mut self, cid: &str) -> Option<PendingCommit> {
        if let Some(req_id) = self.by_cid.remove(cid) {
            if let Some(commit) = self.by_req_id.remove(&req_id) {
                debug!(
                    "Removed pending commit by cid: req_id={}, cid={}",
                    req_id, cid
                );
                return Some(commit);
            }
        }
        None
    }

    /// Check if we have a pending commit with the given correlation ID.
    pub fn has(&self, req_id: &str) -> bool {
        self.by_req_id.contains_key(req_id)
    }

    /// Check if we have a pending commit with the given CID.
    pub fn has_cid(&self, cid: &str) -> bool {
        self.by_cid.contains_key(cid)
    }

    /// Get the number of pending commits.
    pub fn len(&self) -> usize {
        self.by_req_id.len()
    }

    /// Check if there are no pending commits.
    pub fn is_empty(&self) -> bool {
        self.by_req_id.is_empty()
    }

    /// Clean up timed-out commits.
    ///
    /// Returns the list of commits that timed out.
    pub fn cleanup_timed_out(&mut self) -> Vec<PendingCommit> {
        let mut timed_out = Vec::new();

        // Process order queue from front (oldest first)
        while let Some(req_id) = self.order.front() {
            if let Some(commit) = self.by_req_id.get(req_id) {
                if commit.is_timed_out(self.timeout) {
                    let req_id = self.order.pop_front().unwrap();
                    if let Some(commit) = self.remove(&req_id) {
                        warn!(
                            "Commit timed out: req_id={}, cid={} (waited {:?})",
                            commit.req_id,
                            commit.cid,
                            commit.published_at.elapsed()
                        );
                        timed_out.push(commit);
                    }
                } else {
                    // First non-timed-out commit found, stop checking
                    break;
                }
            } else {
                // Commit was already removed, clean up order queue
                self.order.pop_front();
            }
        }

        timed_out
    }

    /// Get all pending commit CIDs (for state inspection).
    pub fn pending_cids(&self) -> Vec<String> {
        self.by_cid.keys().cloned().collect()
    }
}

/// Process an ack message from the server.
///
/// This function handles the ack message and returns the appropriate result.
/// The caller should use the result to update local state and potentially
/// trigger recovery actions.
///
/// # Arguments
/// * `tracker` - The pending commit tracker
/// * `state` - The CRDT peer state for the document
/// * `ack` - The ack message fields
///
/// # Returns
/// The result of processing the ack, indicating what action (if any) to take.
pub fn process_ack(
    tracker: &mut PendingCommitTracker,
    state: &mut CrdtPeerState,
    req: &str,
    commit: &str,
    accepted: bool,
    head_advanced: Option<bool>,
    error: Option<&SyncError>,
) -> AckResult {
    // Try to find the pending commit by req_id or cid
    let pending = if !req.is_empty() {
        tracker.remove(req)
    } else {
        tracker.remove_by_cid(commit)
    };

    // If we don't have this commit tracked, log and return
    if pending.is_none() {
        debug!(
            "Received ack for unknown commit: req={}, cid={}, accepted={}",
            req, commit, accepted
        );
        return AckResult::UnknownCommit;
    }

    let _pending = pending.unwrap();

    if accepted {
        // Commit was accepted
        info!(
            "Commit {} accepted (req={}), head_advanced={:?}",
            commit, req, head_advanced
        );

        // Record this commit as known (it's now durable on server)
        state.record_known_cid(commit);

        match head_advanced {
            Some(true) => {
                // Fast-forward: HEAD moved forward, we're in sync
                debug!("HEAD advanced, client is in sync");

                // If we were in PUSHING state and have no more pending commits,
                // transition to IDLE via AllAcked event
                if tracker.is_empty() {
                    if let Err(e) = state.sync_state_mut().apply(SyncEvent::AllAcked) {
                        debug!("Could not apply AllAcked event: {}", e);
                    }
                }

                AckResult::AcceptedAdvanced
            }
            Some(false) => {
                // Divergent: commit persisted but HEAD didn't move
                // Client should sync with server to merge
                info!(
                    "Commit {} persisted but HEAD did not advance - server diverged",
                    commit
                );
                AckResult::AcceptedDiverged
            }
            None => {
                // Legacy ack without head_advanced field
                debug!(
                    "Commit {} accepted (legacy ack without head_advanced)",
                    commit
                );
                AckResult::AcceptedAdvanced
            }
        }
    } else {
        // Commit was rejected
        match error {
            Some(SyncError::MissingParents { parents }) => {
                warn!("Commit {} rejected: missing parents {:?}", commit, parents);
                AckResult::MissingParents(parents.clone())
            }
            Some(SyncError::CidMismatch { expected, computed }) => {
                error!(
                    "BUG: Commit {} rejected: CID mismatch (expected={}, computed={})",
                    commit, expected, computed
                );
                AckResult::CidMismatch {
                    expected: expected.clone(),
                    computed: computed.clone(),
                }
            }
            Some(SyncError::InvalidCommit { field, message }) => {
                error!(
                    "BUG: Commit {} rejected: invalid commit (field={}, message={})",
                    commit, field, message
                );
                AckResult::InvalidCommit {
                    field: field.clone(),
                    message: message.clone(),
                }
            }
            Some(SyncError::NoCommonAncestor { have, server_head }) => {
                warn!(
                    "Commit {} rejected: no common ancestor (have={:?}, server_head={})",
                    commit, have, server_head
                );
                AckResult::NoCommonAncestor {
                    have: have.clone(),
                    server_head: server_head.clone(),
                }
            }
            Some(SyncError::NotFound { commits }) => {
                warn!(
                    "Commit {} rejected: commits not found {:?}",
                    commit, commits
                );
                // Treat as missing parents for recovery
                AckResult::MissingParents(commits.clone())
            }
            None => {
                warn!("Commit {} rejected with no structured error", commit);
                AckResult::UnknownError("No error details provided".to_string())
            }
        }
    }
}

/// Handle a SyncMessage::Ack received on the sync topic.
///
/// This is the main entry point for processing ack messages.
///
/// # Arguments
/// * `tracker` - The pending commit tracker
/// * `state` - The CRDT peer state for the document
/// * `message` - The sync message (must be an Ack variant)
///
/// # Returns
/// The result of processing the ack, or None if the message is not an Ack.
pub fn handle_ack_message(
    tracker: &mut PendingCommitTracker,
    state: &mut CrdtPeerState,
    message: &SyncMessage,
) -> Option<AckResult> {
    match message {
        SyncMessage::Ack {
            req,
            commit,
            accepted,
            head_advanced,
            error,
        } => Some(process_ack(
            tracker,
            state,
            req,
            commit,
            *accepted,
            *head_advanced,
            error.as_ref(),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_pending_commit_new() {
        let commit = PendingCommit::new(
            "req-123".to_string(),
            "cid-456".to_string(),
            vec!["parent-789".to_string()],
            "base64update".to_string(),
            "author".to_string(),
        );

        assert_eq!(commit.req_id, "req-123");
        assert_eq!(commit.cid, "cid-456");
        assert_eq!(commit.parents, vec!["parent-789"]);
        assert_eq!(commit.retry_count, 0);
        assert!(!commit.is_retry);
        assert!(commit.should_retry());
    }

    #[test]
    fn test_pending_commit_timeout() {
        let commit = PendingCommit::new(
            "req".to_string(),
            "cid".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        );

        // Should not be timed out immediately
        assert!(!commit.is_timed_out(Duration::from_secs(1)));

        // Should be timed out with zero duration
        assert!(commit.is_timed_out(Duration::ZERO));
    }

    #[test]
    fn test_pending_commit_retry() {
        let mut commit = PendingCommit::new(
            "req".to_string(),
            "cid".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        );

        assert!(commit.should_retry());
        assert_eq!(commit.retry_count, 0);

        commit.increment_retry();
        assert!(commit.should_retry());
        assert_eq!(commit.retry_count, 1);
        assert!(commit.is_retry);

        commit.increment_retry();
        commit.increment_retry();
        assert!(!commit.should_retry()); // MAX_RETRIES = 3
        assert_eq!(commit.retry_count, 3);
    }

    #[test]
    fn test_tracker_basic_operations() {
        let mut tracker = PendingCommitTracker::new();

        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);

        let commit = PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        );
        tracker.track(commit);

        assert!(!tracker.is_empty());
        assert_eq!(tracker.len(), 1);
        assert!(tracker.has("req-1"));
        assert!(tracker.has_cid("cid-1"));

        let retrieved = tracker.get("req-1").unwrap();
        assert_eq!(retrieved.cid, "cid-1");

        let by_cid = tracker.get_by_cid("cid-1").unwrap();
        assert_eq!(by_cid.req_id, "req-1");
    }

    #[test]
    fn test_tracker_remove() {
        let mut tracker = PendingCommitTracker::new();

        let commit = PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        );
        tracker.track(commit);

        let removed = tracker.remove("req-1").unwrap();
        assert_eq!(removed.cid, "cid-1");

        assert!(tracker.is_empty());
        assert!(!tracker.has("req-1"));
        assert!(!tracker.has_cid("cid-1"));
    }

    #[test]
    fn test_tracker_remove_by_cid() {
        let mut tracker = PendingCommitTracker::new();

        let commit = PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        );
        tracker.track(commit);

        let removed = tracker.remove_by_cid("cid-1").unwrap();
        assert_eq!(removed.req_id, "req-1");

        assert!(tracker.is_empty());
    }

    #[test]
    fn test_tracker_pending_cids() {
        let mut tracker = PendingCommitTracker::new();

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));
        tracker.track(PendingCommit::new(
            "req-2".to_string(),
            "cid-2".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let cids = tracker.pending_cids();
        assert_eq!(cids.len(), 2);
        assert!(cids.contains(&"cid-1".to_string()));
        assert!(cids.contains(&"cid-2".to_string()));
    }

    #[test]
    fn test_ack_result_properties() {
        assert!(AckResult::AcceptedAdvanced.is_accepted());
        assert!(AckResult::AcceptedDiverged.is_accepted());
        assert!(!AckResult::MissingParents(vec![]).is_accepted());

        assert!(AckResult::MissingParents(vec![]).is_recoverable());
        assert!(AckResult::NoCommonAncestor {
            have: vec![],
            server_head: String::new()
        }
        .is_recoverable());
        assert!(!AckResult::AcceptedAdvanced.is_recoverable());

        assert!(AckResult::CidMismatch {
            expected: String::new(),
            computed: String::new()
        }
        .is_bug());
        assert!(AckResult::InvalidCommit {
            field: String::new(),
            message: String::new()
        }
        .is_bug());
        assert!(!AckResult::AcceptedAdvanced.is_bug());
    }

    #[test]
    fn test_process_ack_accepted_advanced() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let result = process_ack(
            &mut tracker,
            &mut state,
            "req-1",
            "cid-1",
            true,
            Some(true),
            None,
        );

        assert_eq!(result, AckResult::AcceptedAdvanced);
        assert!(tracker.is_empty());
        assert!(state.is_cid_known("cid-1"));
    }

    #[test]
    fn test_process_ack_accepted_diverged() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let result = process_ack(
            &mut tracker,
            &mut state,
            "req-1",
            "cid-1",
            true,
            Some(false),
            None,
        );

        assert_eq!(result, AckResult::AcceptedDiverged);
        assert!(tracker.is_empty());
        assert!(state.is_cid_known("cid-1"));
    }

    #[test]
    fn test_process_ack_missing_parents() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec!["parent-1".to_string()],
            "update".to_string(),
            "author".to_string(),
        ));

        let error = SyncError::MissingParents {
            parents: vec!["parent-1".to_string()],
        };

        let result = process_ack(
            &mut tracker,
            &mut state,
            "req-1",
            "cid-1",
            false,
            None,
            Some(&error),
        );

        assert_eq!(
            result,
            AckResult::MissingParents(vec!["parent-1".to_string()])
        );
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_process_ack_cid_mismatch() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let error = SyncError::CidMismatch {
            expected: "cid-1".to_string(),
            computed: "cid-wrong".to_string(),
        };

        let result = process_ack(
            &mut tracker,
            &mut state,
            "req-1",
            "cid-1",
            false,
            None,
            Some(&error),
        );

        assert_eq!(
            result,
            AckResult::CidMismatch {
                expected: "cid-1".to_string(),
                computed: "cid-wrong".to_string()
            }
        );
    }

    #[test]
    fn test_process_ack_invalid_commit() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let error = SyncError::InvalidCommit {
            field: "update".to_string(),
            message: "invalid base64".to_string(),
        };

        let result = process_ack(
            &mut tracker,
            &mut state,
            "req-1",
            "cid-1",
            false,
            None,
            Some(&error),
        );

        assert_eq!(
            result,
            AckResult::InvalidCommit {
                field: "update".to_string(),
                message: "invalid base64".to_string()
            }
        );
    }

    #[test]
    fn test_process_ack_no_common_ancestor() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let error = SyncError::NoCommonAncestor {
            have: vec!["local-head".to_string()],
            server_head: "server-head".to_string(),
        };

        let result = process_ack(
            &mut tracker,
            &mut state,
            "req-1",
            "cid-1",
            false,
            None,
            Some(&error),
        );

        assert_eq!(
            result,
            AckResult::NoCommonAncestor {
                have: vec!["local-head".to_string()],
                server_head: "server-head".to_string()
            }
        );
    }

    #[test]
    fn test_process_ack_unknown_commit() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Don't track any commit, just process an ack
        let result = process_ack(
            &mut tracker,
            &mut state,
            "unknown-req",
            "unknown-cid",
            true,
            Some(true),
            None,
        );

        assert_eq!(result, AckResult::UnknownCommit);
    }

    #[test]
    fn test_process_ack_by_cid_when_req_empty() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        // Process ack with empty req but valid cid
        let result = process_ack(
            &mut tracker,
            &mut state,
            "", // empty req
            "cid-1",
            true,
            Some(true),
            None,
        );

        assert_eq!(result, AckResult::AcceptedAdvanced);
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_handle_ack_message() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        let message = SyncMessage::Ack {
            req: "req-1".to_string(),
            commit: "cid-1".to_string(),
            accepted: true,
            head_advanced: Some(true),
            error: None,
        };

        let result = handle_ack_message(&mut tracker, &mut state, &message);

        assert_eq!(result, Some(AckResult::AcceptedAdvanced));
    }

    #[test]
    fn test_handle_ack_message_non_ack() {
        let mut tracker = PendingCommitTracker::new();
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        let message = SyncMessage::Head {
            req: "req-1".to_string(),
        };

        let result = handle_ack_message(&mut tracker, &mut state, &message);

        assert!(result.is_none());
    }

    #[test]
    fn test_tracker_cleanup_timed_out() {
        let mut tracker = PendingCommitTracker::with_timeout(Duration::ZERO);

        tracker.track(PendingCommit::new(
            "req-1".to_string(),
            "cid-1".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));
        tracker.track(PendingCommit::new(
            "req-2".to_string(),
            "cid-2".to_string(),
            vec![],
            "update".to_string(),
            "author".to_string(),
        ));

        // All should be timed out with zero timeout
        let timed_out = tracker.cleanup_timed_out();

        assert_eq!(timed_out.len(), 2);
        assert!(tracker.is_empty());
    }
}
