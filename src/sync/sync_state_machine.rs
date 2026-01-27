//! Client sync state machine for the cyan sync protocol.
//!
//! This module implements the state machine defined in docs/plans/2026-01-27-cyan-sync-protocol-design.md.
//!
//! ## State Machine
//!
//! ```text
//!                     ┌─────────────┐
//!                     │   IDLE      │
//!                     └──────┬──────┘
//!                            │ subscribe / HEAD mismatch detected
//!                            ▼
//!                     ┌─────────────┐
//!                     │  COMPARING  │ read retained msg or send head request
//!                     └──────┬──────┘
//!                            │
//!            ┌───────────────┼───────────────┐
//!            ▼               ▼               ▼
//!     ┌──────────┐    ┌──────────┐    ┌──────────┐
//!     │ IN_SYNC  │    │ PULLING  │    │ DIVERGED │
//!     └────┬─────┘    └────┬─────┘    └────┬─────┘
//!          │               │               │
//!          │               │ recv commits  │ send pull, recv commits
//!          │               ▼               ▼
//!          │          ┌──────────┐    ┌──────────┐
//!          │          │ APPLYING │    │ MERGING  │ create merge commit
//!          │          └────┬─────┘    └────┬─────┘
//!          │               │               │
//!          │               ▼               ▼
//!          │          ┌─────────────────────────┐
//!          │          │        PUSHING          │ publish to edits topic
//!          │          └───────────┬─────────────┘
//!          │                      │ recv acks
//!          │                      ▼
//!          │               ┌─────────────┐
//!          └──────────────►│    IDLE     │
//!                          └─────────────┘
//! ```
//!
//! ## State Descriptions
//!
//! | State | Description |
//! |-------|-------------|
//! | IDLE | No sync in progress. Listening for edits. |
//! | COMPARING | Determining relationship between local and server HEAD |
//! | IN_SYNC | HEADs match, nothing to do |
//! | PULLING | Fetching commits from server (client is behind) |
//! | DIVERGED | Both sides have commits the other lacks |
//! | MERGING | Creating merge commit locally (Yjs handles CRDT merge) |
//! | APPLYING | Applying received commits to local Y.Doc |
//! | PUSHING | Publishing local commits, waiting for acks |

use std::fmt;

/// Sync state machine states for the cyan sync protocol.
///
/// Each document being synced has its own state machine tracking
/// its synchronization status with the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ClientSyncState {
    /// No sync in progress. Listening for edits on the edits topic.
    /// Transitions to COMPARING on:
    /// - Initial subscribe to a document
    /// - HEAD mismatch detected (incoming edit with unknown parent)
    #[default]
    Idle,

    /// Determining relationship between local HEAD and server HEAD.
    /// Reads retained message or sends explicit HEAD request.
    /// Transitions to:
    /// - IN_SYNC if HEADs match
    /// - PULLING if server is ahead (server HEAD is descendant of local)
    /// - DIVERGED if neither is ancestor of the other
    Comparing,

    /// HEADs match, nothing to do.
    /// Transitions back to IDLE immediately.
    InSync,

    /// Fetching commits from server (client is behind).
    /// Sends pull request with `have` set (local HEAD + ancestors).
    /// Transitions to APPLYING when commits are received.
    Pulling,

    /// Both client and server have commits the other lacks.
    /// Sends pull request to get server commits.
    /// Transitions to MERGING after receiving all server commits.
    Diverged,

    /// Creating merge commit locally.
    /// Yjs handles the CRDT merge by applying server state to local Y.Doc.
    /// Creates merge commit with both local and server HEADs as parents.
    /// Transitions to PUSHING after merge commit is created.
    Merging,

    /// Applying received commits to local Y.Doc.
    /// For simple pull (non-diverged), applies commits in order.
    /// Transitions to:
    /// - PUSHING if there are local commits to push
    /// - IDLE if no local commits (pure pull)
    Applying,

    /// Publishing local commits to edits topic, waiting for acks.
    /// May need to push multiple commits (merge commit, or parents first).
    /// Transitions to IDLE when all commits are acked.
    /// On `missing_parents` error: push the missing parents first, retry.
    Pushing,
}

impl ClientSyncState {
    /// Returns true if this state represents active sync operations.
    ///
    /// Active states are those where the client is doing work to synchronize.
    /// Used to prevent starting a new sync while one is in progress.
    pub fn is_active(&self) -> bool {
        !matches!(self, ClientSyncState::Idle | ClientSyncState::InSync)
    }

    /// Returns true if this state allows receiving edits.
    ///
    /// In most states, incoming edits should be buffered. Only in IDLE state
    /// are edits processed immediately.
    pub fn should_buffer_edits(&self) -> bool {
        !matches!(self, ClientSyncState::Idle)
    }

    /// Returns true if this state represents a completed sync.
    ///
    /// Both IDLE and IN_SYNC represent completed states where no active
    /// sync is happening.
    pub fn is_complete(&self) -> bool {
        matches!(self, ClientSyncState::Idle | ClientSyncState::InSync)
    }

    /// Returns a short string description suitable for logging.
    pub fn as_str(&self) -> &'static str {
        match self {
            ClientSyncState::Idle => "IDLE",
            ClientSyncState::Comparing => "COMPARING",
            ClientSyncState::InSync => "IN_SYNC",
            ClientSyncState::Pulling => "PULLING",
            ClientSyncState::Diverged => "DIVERGED",
            ClientSyncState::Merging => "MERGING",
            ClientSyncState::Applying => "APPLYING",
            ClientSyncState::Pushing => "PUSHING",
        }
    }
}

impl fmt::Display for ClientSyncState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Events that trigger state transitions in the sync state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncEvent {
    /// Client subscribes to a document or detects HEAD mismatch.
    /// Triggers: IDLE → COMPARING
    StartSync,

    /// Comparison complete: HEADs match.
    /// Triggers: COMPARING → IN_SYNC
    HeadsMatch,

    /// Comparison complete: server is ahead of client.
    /// Triggers: COMPARING → PULLING
    ServerAhead,

    /// Comparison complete: neither is ancestor of other.
    /// Triggers: COMPARING → DIVERGED
    BothDiverged,

    /// All commits received from server (pull complete).
    /// Triggers: PULLING → APPLYING, DIVERGED → MERGING
    CommitsReceived,

    /// Commits applied to local Y.Doc.
    /// Triggers: APPLYING → PUSHING (if local commits) or APPLYING → IDLE (pure pull)
    CommitsApplied {
        /// True if there are local commits to push after applying.
        has_local_commits: bool,
    },

    /// Merge commit created successfully.
    /// Triggers: MERGING → PUSHING
    MergeComplete,

    /// All commits acknowledged by server.
    /// Triggers: PUSHING → IDLE
    AllAcked,

    /// Server returned missing_parents error.
    /// Client should push parent commits and retry.
    /// Stays in PUSHING state, but triggers re-upload of parents.
    MissingParents {
        /// CIDs of commits the server is missing.
        parents: Vec<String>,
    },

    /// Sync aborted due to error or timeout.
    /// Triggers: any state → IDLE
    Abort,

    /// Transition from IN_SYNC back to IDLE.
    /// Triggered immediately after reaching IN_SYNC.
    Resume,
}

impl SyncEvent {
    /// Returns a short string description suitable for logging.
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncEvent::StartSync => "START_SYNC",
            SyncEvent::HeadsMatch => "HEADS_MATCH",
            SyncEvent::ServerAhead => "SERVER_AHEAD",
            SyncEvent::BothDiverged => "BOTH_DIVERGED",
            SyncEvent::CommitsReceived => "COMMITS_RECEIVED",
            SyncEvent::CommitsApplied { .. } => "COMMITS_APPLIED",
            SyncEvent::MergeComplete => "MERGE_COMPLETE",
            SyncEvent::AllAcked => "ALL_ACKED",
            SyncEvent::MissingParents { .. } => "MISSING_PARENTS",
            SyncEvent::Abort => "ABORT",
            SyncEvent::Resume => "RESUME",
        }
    }
}

impl fmt::Display for SyncEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Error returned when a state transition is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidTransition {
    /// The current state before the attempted transition.
    pub from_state: ClientSyncState,
    /// The event that triggered the invalid transition.
    pub event: SyncEvent,
}

impl fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid transition: cannot handle {} in state {}",
            self.event, self.from_state
        )
    }
}

impl std::error::Error for InvalidTransition {}

/// Apply a state transition based on the current state and event.
///
/// Returns the new state, or an error if the transition is invalid.
///
/// # Valid Transitions
///
/// | From State | Event | To State |
/// |------------|-------|----------|
/// | IDLE | StartSync | COMPARING |
/// | COMPARING | HeadsMatch | IN_SYNC |
/// | COMPARING | ServerAhead | PULLING |
/// | COMPARING | BothDiverged | DIVERGED |
/// | IN_SYNC | Resume | IDLE |
/// | PULLING | CommitsReceived | APPLYING |
/// | DIVERGED | CommitsReceived | MERGING |
/// | APPLYING | CommitsApplied(has_local=true) | PUSHING |
/// | APPLYING | CommitsApplied(has_local=false) | IDLE |
/// | MERGING | MergeComplete | PUSHING |
/// | PUSHING | AllAcked | IDLE |
/// | PUSHING | MissingParents | PUSHING (stay, handle parents) |
/// | * | Abort | IDLE |
pub fn transition(
    current: ClientSyncState,
    event: SyncEvent,
) -> Result<ClientSyncState, InvalidTransition> {
    // Abort always returns to IDLE
    if matches!(event, SyncEvent::Abort) {
        return Ok(ClientSyncState::Idle);
    }

    match (current, &event) {
        // IDLE → COMPARING
        (ClientSyncState::Idle, SyncEvent::StartSync) => Ok(ClientSyncState::Comparing),

        // COMPARING → IN_SYNC / PULLING / DIVERGED
        (ClientSyncState::Comparing, SyncEvent::HeadsMatch) => Ok(ClientSyncState::InSync),
        (ClientSyncState::Comparing, SyncEvent::ServerAhead) => Ok(ClientSyncState::Pulling),
        (ClientSyncState::Comparing, SyncEvent::BothDiverged) => Ok(ClientSyncState::Diverged),

        // IN_SYNC → IDLE
        (ClientSyncState::InSync, SyncEvent::Resume) => Ok(ClientSyncState::Idle),

        // PULLING → APPLYING
        (ClientSyncState::Pulling, SyncEvent::CommitsReceived) => Ok(ClientSyncState::Applying),

        // DIVERGED → MERGING
        (ClientSyncState::Diverged, SyncEvent::CommitsReceived) => Ok(ClientSyncState::Merging),

        // APPLYING → PUSHING / IDLE
        (ClientSyncState::Applying, SyncEvent::CommitsApplied { has_local_commits }) => {
            if *has_local_commits {
                Ok(ClientSyncState::Pushing)
            } else {
                Ok(ClientSyncState::Idle)
            }
        }

        // MERGING → PUSHING
        (ClientSyncState::Merging, SyncEvent::MergeComplete) => Ok(ClientSyncState::Pushing),

        // PUSHING → IDLE / stay in PUSHING
        (ClientSyncState::Pushing, SyncEvent::AllAcked) => Ok(ClientSyncState::Idle),
        (ClientSyncState::Pushing, SyncEvent::MissingParents { .. }) => {
            // Stay in PUSHING, but caller should handle pushing parents first
            Ok(ClientSyncState::Pushing)
        }

        // Invalid transition
        _ => Err(InvalidTransition {
            from_state: current,
            event,
        }),
    }
}

/// Sync state machine tracker for a single document.
///
/// This struct wraps the state machine logic and provides a convenient API
/// for managing sync state with logging.
#[derive(Debug, Clone)]
pub struct SyncStateMachine {
    /// Current state
    state: ClientSyncState,
    /// Document identifier for logging
    doc_id: String,
}

impl SyncStateMachine {
    /// Create a new state machine for a document.
    pub fn new(doc_id: impl Into<String>) -> Self {
        Self {
            state: ClientSyncState::Idle,
            doc_id: doc_id.into(),
        }
    }

    /// Get the current state.
    pub fn state(&self) -> ClientSyncState {
        self.state
    }

    /// Get the document ID.
    pub fn doc_id(&self) -> &str {
        &self.doc_id
    }

    /// Apply an event and transition to a new state.
    ///
    /// Logs the transition and returns the new state.
    /// Returns an error if the transition is invalid.
    pub fn apply(&mut self, event: SyncEvent) -> Result<ClientSyncState, InvalidTransition> {
        let old_state = self.state;
        let new_state = transition(self.state, event.clone())?;
        self.state = new_state;

        tracing::debug!(
            doc_id = %self.doc_id,
            from = %old_state,
            to = %new_state,
            event = %event,
            "sync state transition"
        );

        Ok(new_state)
    }

    /// Check if sync is currently active.
    pub fn is_active(&self) -> bool {
        self.state.is_active()
    }

    /// Check if edits should be buffered.
    pub fn should_buffer_edits(&self) -> bool {
        self.state.should_buffer_edits()
    }

    /// Reset to IDLE state (e.g., on disconnect or error).
    pub fn reset(&mut self) {
        if self.state != ClientSyncState::Idle {
            tracing::debug!(
                doc_id = %self.doc_id,
                from = %self.state,
                "sync state reset to IDLE"
            );
            self.state = ClientSyncState::Idle;
        }
    }
}

impl Default for SyncStateMachine {
    fn default() -> Self {
        Self::new("unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idle_to_comparing() {
        let result = transition(ClientSyncState::Idle, SyncEvent::StartSync);
        assert_eq!(result, Ok(ClientSyncState::Comparing));
    }

    #[test]
    fn test_comparing_to_in_sync() {
        let result = transition(ClientSyncState::Comparing, SyncEvent::HeadsMatch);
        assert_eq!(result, Ok(ClientSyncState::InSync));
    }

    #[test]
    fn test_comparing_to_pulling() {
        let result = transition(ClientSyncState::Comparing, SyncEvent::ServerAhead);
        assert_eq!(result, Ok(ClientSyncState::Pulling));
    }

    #[test]
    fn test_comparing_to_diverged() {
        let result = transition(ClientSyncState::Comparing, SyncEvent::BothDiverged);
        assert_eq!(result, Ok(ClientSyncState::Diverged));
    }

    #[test]
    fn test_in_sync_to_idle() {
        let result = transition(ClientSyncState::InSync, SyncEvent::Resume);
        assert_eq!(result, Ok(ClientSyncState::Idle));
    }

    #[test]
    fn test_pulling_to_applying() {
        let result = transition(ClientSyncState::Pulling, SyncEvent::CommitsReceived);
        assert_eq!(result, Ok(ClientSyncState::Applying));
    }

    #[test]
    fn test_diverged_to_merging() {
        let result = transition(ClientSyncState::Diverged, SyncEvent::CommitsReceived);
        assert_eq!(result, Ok(ClientSyncState::Merging));
    }

    #[test]
    fn test_applying_to_pushing_with_local_commits() {
        let result = transition(
            ClientSyncState::Applying,
            SyncEvent::CommitsApplied {
                has_local_commits: true,
            },
        );
        assert_eq!(result, Ok(ClientSyncState::Pushing));
    }

    #[test]
    fn test_applying_to_idle_without_local_commits() {
        let result = transition(
            ClientSyncState::Applying,
            SyncEvent::CommitsApplied {
                has_local_commits: false,
            },
        );
        assert_eq!(result, Ok(ClientSyncState::Idle));
    }

    #[test]
    fn test_merging_to_pushing() {
        let result = transition(ClientSyncState::Merging, SyncEvent::MergeComplete);
        assert_eq!(result, Ok(ClientSyncState::Pushing));
    }

    #[test]
    fn test_pushing_to_idle() {
        let result = transition(ClientSyncState::Pushing, SyncEvent::AllAcked);
        assert_eq!(result, Ok(ClientSyncState::Idle));
    }

    #[test]
    fn test_pushing_with_missing_parents_stays() {
        let result = transition(
            ClientSyncState::Pushing,
            SyncEvent::MissingParents {
                parents: vec!["abc".to_string()],
            },
        );
        assert_eq!(result, Ok(ClientSyncState::Pushing));
    }

    #[test]
    fn test_abort_from_any_state() {
        // Test abort from each state
        for state in [
            ClientSyncState::Idle,
            ClientSyncState::Comparing,
            ClientSyncState::InSync,
            ClientSyncState::Pulling,
            ClientSyncState::Diverged,
            ClientSyncState::Merging,
            ClientSyncState::Applying,
            ClientSyncState::Pushing,
        ] {
            let result = transition(state, SyncEvent::Abort);
            assert_eq!(result, Ok(ClientSyncState::Idle), "Abort from {:?}", state);
        }
    }

    #[test]
    fn test_invalid_transition() {
        // Can't start sync when already comparing
        let result = transition(ClientSyncState::Comparing, SyncEvent::StartSync);
        assert!(result.is_err());

        // Can't receive commits when idle
        let result = transition(ClientSyncState::Idle, SyncEvent::CommitsReceived);
        assert!(result.is_err());

        // Can't complete merge when pulling
        let result = transition(ClientSyncState::Pulling, SyncEvent::MergeComplete);
        assert!(result.is_err());
    }

    #[test]
    fn test_state_is_active() {
        assert!(!ClientSyncState::Idle.is_active());
        assert!(!ClientSyncState::InSync.is_active());
        assert!(ClientSyncState::Comparing.is_active());
        assert!(ClientSyncState::Pulling.is_active());
        assert!(ClientSyncState::Diverged.is_active());
        assert!(ClientSyncState::Merging.is_active());
        assert!(ClientSyncState::Applying.is_active());
        assert!(ClientSyncState::Pushing.is_active());
    }

    #[test]
    fn test_state_should_buffer_edits() {
        assert!(!ClientSyncState::Idle.should_buffer_edits());
        assert!(ClientSyncState::Comparing.should_buffer_edits());
        assert!(ClientSyncState::InSync.should_buffer_edits());
        assert!(ClientSyncState::Pulling.should_buffer_edits());
        assert!(ClientSyncState::Diverged.should_buffer_edits());
        assert!(ClientSyncState::Merging.should_buffer_edits());
        assert!(ClientSyncState::Applying.should_buffer_edits());
        assert!(ClientSyncState::Pushing.should_buffer_edits());
    }

    #[test]
    fn test_state_machine_happy_path() {
        let mut sm = SyncStateMachine::new("test-doc");

        assert_eq!(sm.state(), ClientSyncState::Idle);
        assert!(!sm.is_active());

        // Start sync
        sm.apply(SyncEvent::StartSync).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Comparing);
        assert!(sm.is_active());

        // Server is ahead
        sm.apply(SyncEvent::ServerAhead).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Pulling);

        // Commits received
        sm.apply(SyncEvent::CommitsReceived).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Applying);

        // Commits applied (no local changes)
        sm.apply(SyncEvent::CommitsApplied {
            has_local_commits: false,
        })
        .unwrap();
        assert_eq!(sm.state(), ClientSyncState::Idle);
        assert!(!sm.is_active());
    }

    #[test]
    fn test_state_machine_diverged_path() {
        let mut sm = SyncStateMachine::new("test-doc");

        // Start sync and discover divergence
        sm.apply(SyncEvent::StartSync).unwrap();
        sm.apply(SyncEvent::BothDiverged).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Diverged);

        // Receive commits and merge
        sm.apply(SyncEvent::CommitsReceived).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Merging);

        sm.apply(SyncEvent::MergeComplete).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Pushing);

        sm.apply(SyncEvent::AllAcked).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Idle);
    }

    #[test]
    fn test_state_machine_in_sync_path() {
        let mut sm = SyncStateMachine::new("test-doc");

        sm.apply(SyncEvent::StartSync).unwrap();
        sm.apply(SyncEvent::HeadsMatch).unwrap();
        assert_eq!(sm.state(), ClientSyncState::InSync);

        sm.apply(SyncEvent::Resume).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Idle);
    }

    #[test]
    fn test_state_machine_reset() {
        let mut sm = SyncStateMachine::new("test-doc");

        sm.apply(SyncEvent::StartSync).unwrap();
        sm.apply(SyncEvent::ServerAhead).unwrap();
        assert_eq!(sm.state(), ClientSyncState::Pulling);

        sm.reset();
        assert_eq!(sm.state(), ClientSyncState::Idle);
    }

    #[test]
    fn test_state_display() {
        assert_eq!(ClientSyncState::Idle.to_string(), "IDLE");
        assert_eq!(ClientSyncState::Comparing.to_string(), "COMPARING");
        assert_eq!(ClientSyncState::InSync.to_string(), "IN_SYNC");
        assert_eq!(ClientSyncState::Pulling.to_string(), "PULLING");
        assert_eq!(ClientSyncState::Diverged.to_string(), "DIVERGED");
        assert_eq!(ClientSyncState::Merging.to_string(), "MERGING");
        assert_eq!(ClientSyncState::Applying.to_string(), "APPLYING");
        assert_eq!(ClientSyncState::Pushing.to_string(), "PUSHING");
    }

    #[test]
    fn test_event_display() {
        assert_eq!(SyncEvent::StartSync.to_string(), "START_SYNC");
        assert_eq!(SyncEvent::HeadsMatch.to_string(), "HEADS_MATCH");
        assert_eq!(SyncEvent::AllAcked.to_string(), "ALL_ACKED");
    }

    #[test]
    fn test_invalid_transition_error() {
        let err = InvalidTransition {
            from_state: ClientSyncState::Idle,
            event: SyncEvent::CommitsReceived,
        };
        assert_eq!(
            err.to_string(),
            "Invalid transition: cannot handle COMMITS_RECEIVED in state IDLE"
        );
    }
}
