//! CRDT peer state management for file synchronization.
//!
//! This module implements the "true CRDT peer" architecture where the sync client
//! maintains its own Y.Doc state and merges with the server like any other peer.
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use crate::fs::{Entry, FsSchema};
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::schema_io::SCHEMA_FILENAME;
use crate::sync::sync_state_machine::SyncStateMachine;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, Transact, Update};

/// State file name for per-directory CRDT state.
/// Located inside the directory it tracks (not as a sibling).
pub const CRDT_STATE_FILENAME: &str = ".commonplace-sync.json";

/// Maximum number of edits to queue while waiting for CRDT initialization.
/// Beyond this limit, older edits are dropped with a warning.
pub const MAX_PENDING_EDITS: usize = 100;

// =============================================================================
// Sync Guardrails - Metrics and visibility for diagnosing sync issues
// =============================================================================

/// Global metrics for sync guardrails.
///
/// These counters track events that indicate potential issues:
/// - Queue overflows: edits dropped due to queue capacity limits
/// - Duplicate task spawns: attempts to spawn tasks for already-running files
///
/// Use `SyncGuardrails::global()` to access the singleton instance.
pub struct SyncGuardrails {
    /// Number of edits dropped due to queue overflow
    queue_overflow_count: AtomicU64,
    /// Number of duplicate task spawn attempts blocked
    duplicate_spawn_count: AtomicU64,
    /// Active task spawns (node_id + path combinations currently running)
    /// This is protected by a RwLock for safe concurrent access
    active_spawns: std::sync::RwLock<HashSet<String>>,
}

impl SyncGuardrails {
    /// Create a new SyncGuardrails instance.
    pub fn new() -> Self {
        Self {
            queue_overflow_count: AtomicU64::new(0),
            duplicate_spawn_count: AtomicU64::new(0),
            active_spawns: std::sync::RwLock::new(HashSet::new()),
        }
    }

    /// Get the global singleton instance.
    pub fn global() -> &'static SyncGuardrails {
        static INSTANCE: std::sync::OnceLock<SyncGuardrails> = std::sync::OnceLock::new();
        INSTANCE.get_or_init(SyncGuardrails::new)
    }

    /// Record a queue overflow event.
    ///
    /// Called when an edit is dropped due to queue capacity limits.
    pub fn record_queue_overflow(&self, node_id: &Uuid, queue_size: usize) {
        let count = self.queue_overflow_count.fetch_add(1, Ordering::Relaxed) + 1;
        warn!(
            "[GUARDRAIL] Queue overflow #{}: node_id={} queue_size={} - oldest edit dropped",
            count, node_id, queue_size
        );
    }

    /// Get the total number of queue overflow events.
    pub fn queue_overflow_count(&self) -> u64 {
        self.queue_overflow_count.load(Ordering::Relaxed)
    }

    /// Record a duplicate task spawn attempt.
    ///
    /// Called when an attempt is made to spawn tasks for a file that already has tasks running.
    /// Returns true if this is a duplicate (spawn should be blocked), false if spawn is allowed.
    pub fn check_and_record_spawn(&self, node_id: &Uuid, path: &str) -> bool {
        let key = format!("{}:{}", node_id, path);
        let mut spawns = self.active_spawns.write().unwrap();

        if spawns.contains(&key) {
            let count = self.duplicate_spawn_count.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                "[GUARDRAIL] Duplicate spawn attempt #{}: node_id={} path={} - spawn blocked",
                count, node_id, path
            );
            true // is duplicate
        } else {
            spawns.insert(key);
            debug!(
                "[GUARDRAIL] Task spawn registered: node_id={} path={} (active_count={})",
                node_id,
                path,
                spawns.len()
            );
            false // not duplicate, spawn allowed
        }
    }

    /// Unregister a task spawn when tasks complete or are aborted.
    pub fn unregister_spawn(&self, node_id: &Uuid, path: &str) {
        let key = format!("{}:{}", node_id, path);
        let mut spawns = self.active_spawns.write().unwrap();
        if spawns.remove(&key) {
            debug!(
                "[GUARDRAIL] Task spawn unregistered: node_id={} path={} (active_count={})",
                node_id,
                path,
                spawns.len()
            );
        }
    }

    /// Get the total number of duplicate spawn attempts.
    pub fn duplicate_spawn_count(&self) -> u64 {
        self.duplicate_spawn_count.load(Ordering::Relaxed)
    }

    /// Get the number of currently active task spawns.
    pub fn active_spawn_count(&self) -> usize {
        self.active_spawns.read().unwrap().len()
    }

    /// Log a summary of guardrail metrics.
    pub fn log_summary(&self) {
        let queue_overflows = self.queue_overflow_count();
        let duplicate_spawns = self.duplicate_spawn_count();
        let active_spawns = self.active_spawn_count();

        if queue_overflows > 0 || duplicate_spawns > 0 {
            warn!(
                "[GUARDRAIL SUMMARY] queue_overflows={} duplicate_spawns={} active_spawns={}",
                queue_overflows, duplicate_spawns, active_spawns
            );
        } else {
            info!(
                "[GUARDRAIL SUMMARY] All clear: queue_overflows=0 duplicate_spawns=0 active_spawns={}",
                active_spawns
            );
        }
    }

    /// Reset all counters (for testing).
    ///
    /// This is intended for use in test suites to isolate test cases.
    /// In production, counters accumulate for the lifetime of the process.
    pub fn reset(&self) {
        self.queue_overflow_count.store(0, Ordering::Relaxed);
        self.duplicate_spawn_count.store(0, Ordering::Relaxed);
        self.active_spawns.write().unwrap().clear();
    }
}

impl Default for SyncGuardrails {
    fn default() -> Self {
        Self::new()
    }
}

/// A queued edit message waiting to be applied after CRDT initialization.
#[derive(Debug, Clone)]
pub struct PendingEdit {
    /// The raw MQTT payload (serialized EditMessage)
    pub payload: Vec<u8>,
}

/// An edit buffered during active sync.
///
/// When the client is in an active sync state (PULLING, DIVERGED, MERGING,
/// APPLYING, PUSHING), incoming edits are buffered here instead of being
/// processed immediately. After sync completes, these edits are processed.
#[derive(Debug, Clone)]
pub struct SyncBufferedEdit {
    /// The raw MQTT payload (serialized EditMessage)
    pub payload: Vec<u8>,
    /// Parent CIDs from the edit message (for fast-forward detection)
    pub parents: Vec<String>,
}

/// Configuration for MQTT-only sync mode.
///
/// When enabled, HTTP is deprecated for sync operations.
/// All state initialization comes from MQTT retained messages.
#[derive(Debug, Clone, Copy, Default)]
pub struct MqttOnlySyncConfig {
    /// If true, HTTP calls during sync are deprecated and logged as warnings.
    /// State initialization relies on MQTT retained messages instead of HTTP fetch.
    pub mqtt_only: bool,
}

impl MqttOnlySyncConfig {
    /// Create a new config with MQTT-only mode enabled.
    pub fn mqtt_only() -> Self {
        Self { mqtt_only: true }
    }

    /// Create a new config with HTTP fallback enabled (default behavior).
    pub fn with_http_fallback() -> Self {
        Self { mqtt_only: false }
    }

    /// Check if HTTP calls should be logged as deprecated.
    pub fn http_deprecated(&self) -> bool {
        self.mqtt_only
    }

    /// Log a deprecation warning for HTTP usage if MQTT-only mode is enabled.
    pub fn log_http_deprecation(&self, operation: &str) {
        if self.mqtt_only {
            warn!(
                "[DEPRECATED] HTTP {} called in MQTT-only mode - this may cause race conditions",
                operation
            );
        }
    }
}

/// Reason why edits are being queued (for logging/debugging).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueReason {
    /// CRDT state hasn't been initialized from server yet
    NeedsServerInit,
    /// Receive task hasn't started processing yet
    ReceiveTaskNotReady,
}

impl QueueReason {
    /// Returns a short string description of the queue reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            QueueReason::NeedsServerInit => "needs_server_init",
            QueueReason::ReceiveTaskNotReady => "receive_task_not_ready",
        }
    }
}

/// CRDT peer state for a single document (file content or schema).
///
/// Each tracked entity maintains:
/// - Its node_id (UUID)
/// - The server HEAD we've merged (`head_cid`)
/// - Our latest commit (`local_head_cid`)
/// - The serialized Y.Doc state
/// - A queue of pending edits received before initialization
/// - A flag indicating whether the receive task is ready
/// - A set of known commit IDs (for detecting missing parents)
/// - A sync state machine for cyan sync protocol
/// - A buffer for edits received during active sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtPeerState {
    /// UUID for this document
    pub node_id: Uuid,
    /// Server HEAD we've merged (last known server commit)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_cid: Option<String>,
    /// Our latest commit (may be ahead of server)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_head_cid: Option<String>,
    /// Serialized Y.Doc state (base64 encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yjs_state: Option<String>,
    /// Set of known commit IDs (for detecting missing parents).
    /// This is persisted to survive restarts.
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub known_cids: HashSet<String>,
    /// Queue of edits received before CRDT initialization completed
    /// AND receive task became ready. Edits are queued until both
    /// conditions are met.
    #[serde(skip)]
    pub pending_edits: Vec<PendingEdit>,
    /// Whether the receive task has subscribed and is ready to process edits.
    /// Edits are queued until this is true (even after CRDT init completes).
    #[serde(skip)]
    pub receive_task_ready: bool,
    /// Sync state machine for the cyan sync protocol.
    /// Tracks the current sync state (IDLE, PULLING, DIVERGED, etc.)
    /// Not serialized - resets to IDLE on restart.
    #[serde(skip)]
    pub sync_state_machine: SyncStateMachine,
    /// Buffer for edits received during active sync.
    /// These are processed after sync completes, checking if they
    /// extend the new HEAD (fast-forward) or need another sync cycle.
    #[serde(skip)]
    pub sync_buffered_edits: Vec<SyncBufferedEdit>,
}

impl CrdtPeerState {
    /// Create a new CrdtPeerState with just a node_id.
    pub fn new(node_id: Uuid) -> Self {
        Self {
            node_id,
            head_cid: None,
            local_head_cid: None,
            yjs_state: None,
            known_cids: HashSet::new(),
            pending_edits: Vec::new(),
            receive_task_ready: false,
            sync_state_machine: SyncStateMachine::new(node_id.to_string()),
            sync_buffered_edits: Vec::new(),
        }
    }

    /// Create CrdtPeerState from an existing Y.Doc.
    pub fn from_doc(node_id: Uuid, doc: &Doc) -> Self {
        let txn = doc.transact();
        // Encode full state from empty state vector to get everything
        let update = txn.encode_state_as_update_v1(&yrs::StateVector::default());
        let yjs_state = STANDARD.encode(&update);

        Self {
            node_id,
            head_cid: None,
            local_head_cid: None,
            yjs_state: Some(yjs_state),
            known_cids: HashSet::new(),
            pending_edits: Vec::new(),
            receive_task_ready: false,
            sync_state_machine: SyncStateMachine::new(node_id.to_string()),
            sync_buffered_edits: Vec::new(),
        }
    }

    /// Deserialize the Y.Doc state.
    /// Returns a new Doc with the stored state applied, or an empty Doc if no state.
    /// The "content" text root is always ensured to exist.
    pub fn to_doc(&self) -> SyncResult<Doc> {
        let doc = Doc::new();

        // Note: Don't pre-create any root structure here.
        // The stored state may contain either:
        // - A Text "content" (for file content)
        // - A Map "content" (for schema structure)
        // Pre-creating the wrong type would cause conflicts.

        if let Some(ref state_b64) = self.yjs_state {
            let state_bytes = STANDARD.decode(state_b64)?;

            let update = Update::decode_v1(&state_bytes).map_err(|e| {
                SyncError::yjs_decode(format!("Failed to decode Yrs update: {}", e))
            })?;

            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        Ok(doc)
    }

    /// Update the stored Y.Doc state from a Doc.
    pub fn update_from_doc(&mut self, doc: &Doc) {
        let txn = doc.transact();
        // Encode full state from empty state vector to get everything
        let update = txn.encode_state_as_update_v1(&yrs::StateVector::default());
        self.yjs_state = Some(STANDARD.encode(&update));
    }

    /// Check if this commit CID is already known.
    ///
    /// Returns true if the CID is in the known_cids set, or is the current
    /// head_cid or local_head_cid. This is used to detect echo commits
    /// that we should skip processing.
    pub fn is_cid_known(&self, cid: &str) -> bool {
        self.known_cids.contains(cid)
            || self.head_cid.as_deref() == Some(cid)
            || self.local_head_cid.as_deref() == Some(cid)
    }

    /// Record a commit CID as known.
    ///
    /// This should be called after successfully processing a commit
    /// to track that we have its data in our history.
    pub fn record_known_cid(&mut self, cid: &str) {
        self.known_cids.insert(cid.to_string());
    }

    /// Check which parents of a commit are missing (not known to us).
    ///
    /// Returns a list of parent CIDs that we don't have in our history.
    /// An empty list means all parents are known.
    ///
    /// Note: This only checks our local knowledge. For a commit with no
    /// parents (the initial commit), this returns an empty list.
    pub fn find_missing_parents(&self, parents: &[String]) -> Vec<String> {
        parents
            .iter()
            .filter(|p| !p.is_empty() && !self.is_cid_known(p))
            .cloned()
            .collect()
    }

    /// Initialize this state from server's Yjs state.
    ///
    /// This is critical for CRDT sync to work correctly. Without shared history,
    /// different clients would create independent operation histories that don't
    /// merge properly - deletions from one client wouldn't affect content created
    /// by another client with a different client ID.
    ///
    /// # Arguments
    /// * `server_state_b64` - Base64-encoded Yjs state from server's HEAD
    /// * `server_cid` - The CID of the server's HEAD commit
    pub fn initialize_from_server(&mut self, server_state_b64: &str, server_cid: &str) {
        self.yjs_state = Some(server_state_b64.to_string());
        self.head_cid = Some(server_cid.to_string());
        self.local_head_cid = Some(server_cid.to_string());
        // Record the initial commit as known
        self.record_known_cid(server_cid);
        debug!(
            "Initialized CRDT state from server: cid={}, state_len={}",
            server_cid,
            server_state_b64.len()
        );
    }

    /// Initialize with an empty Y.Doc state.
    ///
    /// This marks the state as "initialized" but with no content.
    /// Used when the server has no state yet (empty document) so that
    /// incoming MQTT edits are processed normally instead of being queued
    /// waiting for server initialization.
    pub fn initialize_empty(&mut self) {
        let doc = Doc::new();
        self.update_from_doc(&doc);
        debug!("Initialized CRDT state as empty for node {}", self.node_id);
    }

    /// Check if this state needs initialization from server.
    ///
    /// Returns true if we have no Yjs state, meaning we haven't synced
    /// with the server yet and need to fetch its state to share history.
    pub fn needs_server_init(&self) -> bool {
        self.yjs_state.is_none()
    }

    /// Check if edits should be queued rather than processed directly.
    ///
    /// Returns `Some(reason)` if edits should be queued, `None` if ready to process.
    /// Edits should be queued if EITHER:
    /// 1. CRDT state hasn't been initialized from server yet
    /// 2. Receive task hasn't signaled it's ready yet
    ///
    /// This prevents the race condition where edits arrive between CRDT init
    /// completion and receive task startup.
    pub fn should_queue_edits(&self) -> Option<QueueReason> {
        if self.yjs_state.is_none() {
            Some(QueueReason::NeedsServerInit)
        } else if !self.receive_task_ready {
            Some(QueueReason::ReceiveTaskNotReady)
        } else {
            None
        }
    }

    /// Mark the receive task as ready to process edits.
    ///
    /// Called by the receive task after it has subscribed to MQTT.
    /// Returns any pending edits that were queued before ready.
    pub fn mark_receive_task_ready(&mut self) -> Vec<PendingEdit> {
        self.receive_task_ready = true;
        debug!(
            "Marked receive task ready for {} (had {} pending edits)",
            self.node_id,
            self.pending_edits.len()
        );
        // Drain pending edits now that we're ready
        std::mem::take(&mut self.pending_edits)
    }

    /// Reset receive task ready state (for resync scenarios).
    ///
    /// Called when resync is needed to ensure edits are queued again
    /// until the task re-subscribes.
    pub fn mark_receive_task_not_ready(&mut self) {
        self.receive_task_ready = false;
        debug!("Marked receive task not ready for {}", self.node_id);
    }

    /// Mark this state as needing resync from server.
    ///
    /// Called after broadcast lag is detected to force re-fetching of server
    /// state. Clears the yjs_state so that `needs_server_init()` returns true
    /// and the next initialization will fetch fresh state from server.
    ///
    /// Also clears head_cid, local_head_cid, and known_cids to allow fresh
    /// ancestry checks. Does NOT reset receive_task_ready since the task is
    /// still running.
    pub fn mark_needs_resync(&mut self) {
        debug!(
            "Marking {} as needing resync - clearing CRDT state",
            self.node_id
        );
        self.yjs_state = None;
        self.head_cid = None;
        self.local_head_cid = None;
        self.known_cids.clear();
        // Keep pending_edits - they may still be applicable after resync
        // Keep receive_task_ready - the task is still running and subscribed
    }

    /// Queue an edit to be applied after CRDT initialization completes.
    ///
    /// If the queue exceeds `MAX_PENDING_EDITS`, the oldest edit is dropped.
    /// This prevents unbounded memory growth while still handling the common
    /// case of a few edits arriving during initialization.
    ///
    /// Returns true if the edit was queued, false if an edit was dropped due to overflow.
    pub fn queue_pending_edit(&mut self, payload: Vec<u8>) -> bool {
        let overflow = self.pending_edits.len() >= MAX_PENDING_EDITS;
        if overflow {
            // Drop the oldest edit to make room
            self.pending_edits.remove(0);
            // Record in global guardrails metrics
            SyncGuardrails::global().record_queue_overflow(&self.node_id, MAX_PENDING_EDITS);
        }
        self.pending_edits.push(PendingEdit { payload });
        debug!(
            "Queued pending edit for {} (queue size: {})",
            self.node_id,
            self.pending_edits.len()
        );
        !overflow
    }

    /// Take all pending edits from the queue.
    ///
    /// This is called after CRDT initialization to drain the queue
    /// and apply all buffered edits.
    pub fn take_pending_edits(&mut self) -> Vec<PendingEdit> {
        std::mem::take(&mut self.pending_edits)
    }

    /// Check if there are pending edits waiting to be applied.
    pub fn has_pending_edits(&self) -> bool {
        !self.pending_edits.is_empty()
    }

    // =========================================================================
    // Sync State Machine Methods
    // =========================================================================

    /// Check if edits should be buffered due to active sync.
    ///
    /// During active sync states (PULLING, DIVERGED, MERGING, APPLYING, PUSHING),
    /// incoming edits should be buffered instead of processed immediately.
    /// This prevents race conditions where incoming edits interfere with sync.
    pub fn should_buffer_for_sync(&self) -> bool {
        self.sync_state_machine.should_buffer_edits()
    }

    /// Buffer an edit received during active sync.
    ///
    /// The edit is stored with its parent CIDs for later fast-forward detection.
    /// If the buffer exceeds MAX_PENDING_EDITS, the oldest edit is dropped.
    ///
    /// # Arguments
    /// * `payload` - The raw MQTT payload (serialized EditMessage)
    /// * `parents` - Parent CIDs from the edit message
    ///
    /// # Returns
    /// True if the edit was buffered, false if an edit was dropped due to overflow.
    pub fn buffer_sync_edit(&mut self, payload: Vec<u8>, parents: Vec<String>) -> bool {
        let overflow = self.sync_buffered_edits.len() >= MAX_PENDING_EDITS;
        if overflow {
            // Drop the oldest edit to make room
            self.sync_buffered_edits.remove(0);
            warn!(
                "Sync buffer overflow for {}: dropped oldest edit (buffer size: {})",
                self.node_id, MAX_PENDING_EDITS
            );
        }
        self.sync_buffered_edits
            .push(SyncBufferedEdit { payload, parents });
        debug!(
            "Buffered sync edit for {} (buffer size: {})",
            self.node_id,
            self.sync_buffered_edits.len()
        );
        !overflow
    }

    /// Check if there are edits buffered during sync.
    pub fn has_sync_buffered_edits(&self) -> bool {
        !self.sync_buffered_edits.is_empty()
    }

    /// Drain buffered sync edits, classifying them by whether they extend the new HEAD.
    ///
    /// After sync completes with a new HEAD, this method returns two groups:
    /// 1. Fast-forward edits: These have the new HEAD as their parent and can be applied
    /// 2. Stale edits: These are based on the old HEAD and need another sync cycle
    ///
    /// # Arguments
    /// * `new_head` - The HEAD CID after sync completed
    ///
    /// # Returns
    /// A tuple of (fast_forward_edits, stale_edits)
    pub fn drain_sync_buffer(
        &mut self,
        new_head: &str,
    ) -> (Vec<SyncBufferedEdit>, Vec<SyncBufferedEdit>) {
        let all_edits = std::mem::take(&mut self.sync_buffered_edits);

        let mut fast_forward = Vec::new();
        let mut stale = Vec::new();

        for edit in all_edits {
            // Check if this edit's parent is the new HEAD (fast-forward case)
            let is_fast_forward = edit.parents.len() == 1
                && edit.parents.first().map(|p| p.as_str()) == Some(new_head);

            if is_fast_forward {
                fast_forward.push(edit);
            } else {
                stale.push(edit);
            }
        }

        if !fast_forward.is_empty() || !stale.is_empty() {
            debug!(
                "Drained sync buffer for {}: {} fast-forward, {} stale",
                self.node_id,
                fast_forward.len(),
                stale.len()
            );
        }

        (fast_forward, stale)
    }

    /// Take all buffered sync edits without classification.
    ///
    /// Used when all buffered edits should be processed regardless of parent.
    pub fn take_sync_buffered_edits(&mut self) -> Vec<SyncBufferedEdit> {
        std::mem::take(&mut self.sync_buffered_edits)
    }

    /// Get a reference to the sync state machine.
    pub fn sync_state(&self) -> &SyncStateMachine {
        &self.sync_state_machine
    }

    /// Get a mutable reference to the sync state machine.
    pub fn sync_state_mut(&mut self) -> &mut SyncStateMachine {
        &mut self.sync_state_machine
    }
}

/// Sync state for an entire directory.
///
/// Each node-backed directory has its own state file containing:
/// - Schema state (the directory's .commonplace.json as a Y.Doc)
/// - File states (each file's content as a Y.Doc)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectorySyncState {
    /// Version for future-proofing migrations
    pub version: u32,
    /// State for this directory's schema
    pub schema: CrdtPeerState,
    /// State for files in this directory (keyed by filename, not path)
    #[serde(default)]
    pub files: HashMap<String, CrdtPeerState>,
}

impl DirectorySyncState {
    /// Current state file version.
    pub const CURRENT_VERSION: u32 = 2;

    /// Create a new DirectorySyncState for a directory with the given node_id.
    pub fn new(schema_node_id: Uuid) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            schema: CrdtPeerState::new(schema_node_id),
            files: HashMap::new(),
        }
    }

    /// Get or create state for a file.
    ///
    /// If the file already exists in state but has a different node_id (e.g., because
    /// the schema was updated), the node_id is updated to match the authoritative value.
    /// This ensures sync tasks subscribe to the correct MQTT topics.
    pub fn get_or_create_file(&mut self, filename: &str, node_id: Uuid) -> &mut CrdtPeerState {
        let state = self
            .files
            .entry(filename.to_string())
            .or_insert_with(|| CrdtPeerState::new(node_id));

        // Update node_id if it changed (schema may have been updated on server)
        if state.node_id != node_id {
            tracing::warn!(
                "CRDT state: updating node_id for '{}' from {} to {} (schema updated)",
                filename,
                state.node_id,
                node_id
            );
            state.node_id = node_id;
            // Clear stale Yjs state since we're now tracking a different document
            state.yjs_state = None;
            state.head_cid = None;
            state.local_head_cid = None;
        }

        state
    }

    /// Get state for a file if it exists.
    pub fn get_file(&self, filename: &str) -> Option<&CrdtPeerState> {
        self.files.get(filename)
    }

    /// Get mutable state for a file if it exists.
    pub fn get_file_mut(&mut self, filename: &str) -> Option<&mut CrdtPeerState> {
        self.files.get_mut(filename)
    }

    /// Remove state for a file.
    pub fn remove_file(&mut self, filename: &str) -> Option<CrdtPeerState> {
        self.files.remove(filename)
    }

    /// Check if a file is tracked.
    pub fn has_file(&self, filename: &str) -> bool {
        self.files.contains_key(filename)
    }

    /// Get the state file path for a directory.
    /// The state file is stored inside the directory.
    pub fn state_file_path(directory: &Path) -> PathBuf {
        directory.join(CRDT_STATE_FILENAME)
    }

    /// Load state from file, or return None if it doesn't exist.
    pub async fn load(directory: &Path) -> io::Result<Option<Self>> {
        let path = Self::state_file_path(directory);
        match fs::read_to_string(&path).await {
            Ok(content) => {
                let state: Self = serde_json::from_str(&content)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Save state to file.
    pub async fn save(&self, directory: &Path) -> io::Result<()> {
        let path = Self::state_file_path(directory);
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Trace log for debugging
        {
            use std::io::Write;
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/sandbox-trace.log")
            {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let pid = std::process::id();
                let yjs_state_len = self.schema.yjs_state.as_ref().map(|s| s.len()).unwrap_or(0);
                let files_count = self.files.len();
                let _ = writeln!(
                    file,
                    "[{} pid={}] DirectorySyncState::save: dir={}, yjs_state_len={}, files_count={}, node_id={}",
                    timestamp, pid, directory.display(), yjs_state_len, files_count, self.schema.node_id
                );
            }
        }

        fs::write(&path, content).await
    }

    /// Load state or create new if it doesn't exist.
    ///
    /// If the loaded state has a mismatched schema node_id (e.g., nil UUID from
    /// corrupted state), it will be updated to the correct value and the Yjs
    /// state will be cleared to ensure consistency.
    ///
    /// When creating new state (or clearing Y.Doc due to mismatch), if a
    /// `.commonplace.json` file exists in the directory, the Y.Doc will be
    /// initialized from that schema to prevent data loss when we later write
    /// the Y.Doc back to disk.
    pub async fn load_or_create(directory: &Path, schema_node_id: Uuid) -> io::Result<Self> {
        match Self::load(directory).await? {
            Some(mut state) => {
                // Trace log for debugging
                {
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("/tmp/sandbox-trace.log")
                    {
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis())
                            .unwrap_or(0);
                        let pid = std::process::id();
                        let yjs_state_len = state
                            .schema
                            .yjs_state
                            .as_ref()
                            .map(|s| s.len())
                            .unwrap_or(0);
                        let files_count = state.files.len();
                        let node_id_match = state.schema.node_id == schema_node_id;
                        let _ = writeln!(
                            file,
                            "[{} pid={}] load_or_create LOADED: dir={}, yjs_state_len={}, files_count={}, loaded_node_id={}, expected_node_id={}, match={}",
                            timestamp, pid, directory.display(), yjs_state_len, files_count, state.schema.node_id, schema_node_id, node_id_match
                        );
                    }
                }

                // Fix corrupted node_id if it doesn't match expected value
                if state.schema.node_id != schema_node_id {
                    tracing::warn!(
                        "DirectorySyncState: fixing schema node_id mismatch: {} -> {}",
                        state.schema.node_id,
                        schema_node_id
                    );
                    state.schema.node_id = schema_node_id;
                    // Clear Yjs state since we're now tracking a different document
                    state.schema.yjs_state = None;
                    state.schema.head_cid = None;
                    state.schema.local_head_cid = None;

                    // NOTE: We no longer initialize from local file here.
                    // This was causing Y.Doc client ID mismatches that broke DELETE operations.
                    // The first MQTT retained message will initialize the state with correct IDs.
                    // See CP-1ual for details.
                }
                Ok(state)
            }
            None => {
                let state = Self::new(schema_node_id);
                // NOTE: We no longer initialize from local file here.
                // This was causing Y.Doc client ID mismatches that broke DELETE operations.
                // The first MQTT retained message will initialize the state with correct IDs.
                // See CP-1ual for details.
                Ok(state)
            }
        }
    }

    /// Initialize the schema Y.Doc from an existing .commonplace.json file.
    ///
    /// This preserves existing schema entries when creating a new state or
    /// recovering from corrupted state, preventing data loss when the Y.Doc
    /// is later written back to disk.
    async fn initialize_schema_from_local_file(state: &mut Self, directory: &Path) {
        use crate::fs::FsSchema;
        use crate::sync::ymap_schema;
        use crate::sync::SCHEMA_FILENAME;

        let schema_path = directory.join(SCHEMA_FILENAME);
        if !schema_path.exists() {
            debug!(
                "No existing .commonplace.json at {}, starting with empty Y.Doc",
                schema_path.display()
            );
            return;
        }

        match fs::read_to_string(&schema_path).await {
            Ok(content) => match serde_json::from_str::<FsSchema>(&content) {
                Ok(fs_schema) => {
                    // Create Y.Doc and initialize from schema
                    let doc = Doc::new();
                    ymap_schema::from_fs_schema(&doc, &fs_schema);

                    // Count entries for logging
                    let entry_count = fs_schema
                        .root
                        .as_ref()
                        .and_then(|e| match e {
                            crate::fs::Entry::Dir(d) => d.entries.as_ref().map(|e| e.len()),
                            _ => None,
                        })
                        .unwrap_or(0);

                    // Update state with the initialized Y.Doc
                    state.schema.update_from_doc(&doc);

                    tracing::info!(
                        "Initialized Y.Doc schema from {} with {} entries",
                        schema_path.display(),
                        entry_count
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to parse {} as FsSchema: {}",
                        schema_path.display(),
                        e
                    );
                }
            },
            Err(e) => {
                warn!("Failed to read {}: {}", schema_path.display(), e);
            }
        }
    }

    /// Reconcile file UUIDs in sync state with the authoritative schema.
    ///
    /// Reads the `.commonplace.json` schema and compares each file's node_id
    /// against the corresponding entry in `self.files`. If they differ, updates
    /// the sync state to use the schema's UUID and clears stale CRDT state.
    ///
    /// This prevents "split-brain" sync where the client writes to a different
    /// server document than what the schema specifies.
    ///
    /// Returns the number of corrections made.
    pub async fn reconcile_with_schema(&mut self, directory: &Path) -> io::Result<usize> {
        let schema_path = directory.join(SCHEMA_FILENAME);
        if !schema_path.exists() {
            debug!(
                "No schema at {} for reconciliation, skipping",
                schema_path.display()
            );
            return Ok(0);
        }

        let content = fs::read_to_string(&schema_path).await?;
        let fs_schema: FsSchema = serde_json::from_str(&content)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Extract file entries from root directory
        let entries = match &fs_schema.root {
            Some(Entry::Dir(dir)) => dir.entries.as_ref(),
            _ => None,
        };

        let Some(entries) = entries else {
            debug!("Schema has no entries to reconcile");
            return Ok(0);
        };

        info!(
            "Reconciling {} schema entries with sync state in {}",
            entries.len(),
            directory.display()
        );

        let mut corrections = 0;

        for (filename, entry) in entries {
            // Only process document entries (not subdirectories)
            let schema_node_id = match entry {
                Entry::Doc(doc) => doc.node_id.as_ref(),
                Entry::Dir(_) => continue, // Skip subdirectories
            };

            let Some(schema_node_id_str) = schema_node_id else {
                continue; // No node_id in schema, skip
            };

            let schema_uuid = match Uuid::parse_str(schema_node_id_str) {
                Ok(uuid) => uuid,
                Err(e) => {
                    warn!(
                        "Invalid UUID '{}' for '{}' in schema: {}",
                        schema_node_id_str, filename, e
                    );
                    continue;
                }
            };

            // Check if we have this file in sync state with a different UUID
            if let Some(file_state) = self.files.get_mut(filename) {
                if file_state.node_id != schema_uuid {
                    warn!(
                        "UUID drift detected for '{}': sync state has {}, schema has {} - correcting",
                        filename, file_state.node_id, schema_uuid
                    );
                    file_state.node_id = schema_uuid;
                    // Clear stale CRDT state since we're now tracking a different document
                    file_state.yjs_state = None;
                    file_state.head_cid = None;
                    file_state.local_head_cid = None;
                    file_state.known_cids.clear();
                    corrections += 1;
                }
            }
            // Note: We don't create new entries here - that's handled by the normal sync flow
        }

        if corrections > 0 {
            info!(
                "Reconciled {} UUID drift(s) with schema in {}",
                corrections,
                directory.display()
            );
            // Save the corrected state
            self.save(directory).await?;
        }

        Ok(corrections)
    }
}

/// Migrate from old SyncStateFile format to new DirectorySyncState.
///
/// The old format stored:
/// - server, node_id, last_synced_cid, files (with hash, last_cid, inode_key)
///
/// The new format stores:
/// - schema state with Y.Doc
/// - file states with Y.Docs
///
/// Migration creates empty Y.Docs but preserves CID information.
pub fn migrate_from_old_state(
    old_state: &crate::sync::state_file::SyncStateFile,
) -> SyncResult<DirectorySyncState> {
    // Parse the node_id as UUID
    let schema_node_id = Uuid::parse_str(&old_state.node_id)?;

    let mut new_state = DirectorySyncState::new(schema_node_id);

    // Preserve the last synced CID as both head_cid and local_head_cid
    // (we assume they're in sync at migration time)
    new_state.schema.head_cid = old_state.last_synced_cid.clone();
    new_state.schema.local_head_cid = old_state.last_synced_cid.clone();

    // Migrate file states
    for (path, file_state) in &old_state.files {
        // For files, we don't have the node_id in the old format
        // We'll need to generate a placeholder or fetch from schema
        // For now, generate a new UUID (will be updated on first sync)
        let file_node_id = Uuid::new_v4();

        let mut peer_state = CrdtPeerState::new(file_node_id);
        peer_state.head_cid = file_state.last_cid.clone();
        peer_state.local_head_cid = file_state.last_cid.clone();
        // Y.Doc state will be populated on first sync

        new_state.files.insert(path.clone(), peer_state);
    }

    debug!(
        "Migrated old state with {} files to new CRDT state",
        new_state.files.len()
    );

    Ok(new_state)
}

/// Attempt to load state, migrating from old format if necessary.
pub async fn load_or_migrate(
    directory: &Path,
    schema_node_id: Uuid,
) -> io::Result<DirectorySyncState> {
    // First try loading new format
    if let Some(state) = DirectorySyncState::load(directory).await? {
        return Ok(state);
    }

    // Check for old format state file (sibling dotfile)
    let old_state_path = crate::sync::state_file::SyncStateFile::state_file_path(directory);
    if old_state_path.exists() {
        if let Some(old_state) =
            crate::sync::state_file::SyncStateFile::load(&old_state_path).await?
        {
            match migrate_from_old_state(&old_state) {
                Ok(new_state) => {
                    // Save the migrated state
                    new_state.save(directory).await?;
                    warn!(
                        "Migrated old sync state to new CRDT format: {}",
                        directory.display()
                    );
                    return Ok(new_state);
                }
                Err(e) => {
                    warn!("Failed to migrate old state: {}", e);
                    // Fall through to create new state
                }
            }
        }
    }

    // No existing state, create new
    Ok(DirectorySyncState::new(schema_node_id))
}

/// Cache for subdirectory CRDT states to prevent race conditions.
///
/// Without this cache, multiple code paths could load DirectorySyncState
/// independently for the same directory, leading to race conditions where
/// one path's changes are overwritten by another's stale state.
pub struct SubdirStateCache {
    cache: std::sync::RwLock<HashMap<PathBuf, Arc<RwLock<DirectorySyncState>>>>,
}

impl SubdirStateCache {
    pub fn new() -> Self {
        Self {
            cache: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a DirectorySyncState for a subdirectory.
    ///
    /// If the state is already cached AND has a matching node_id, returns the existing instance.
    /// If cached but node_id doesn't match (e.g., directory was deleted and recreated),
    /// evicts the stale entry and reloads from disk.
    /// Otherwise, loads from disk (or creates new) and caches it.
    pub async fn get_or_load(
        &self,
        directory: &Path,
        schema_node_id: Uuid,
    ) -> io::Result<Arc<RwLock<DirectorySyncState>>> {
        // First check if cached (clone the Arc without holding lock across await)
        let cached_state = {
            let cache = self.cache.read().unwrap();
            cache.get(directory).cloned()
        };

        // Validate cached state if present
        if let Some(state) = cached_state {
            let state_guard = state.read().await;
            let cached_node_id = state_guard.schema.node_id;
            drop(state_guard);

            if cached_node_id == schema_node_id {
                return Ok(state);
            }
            // node_id mismatch - evict and reload below
            {
                let mut cache = self.cache.write().unwrap();
                cache.remove(directory);
            }
            tracing::debug!(
                "Evicted stale cache entry for {} (expected node_id {}, found {})",
                directory.display(),
                schema_node_id,
                cached_node_id
            );
        }

        // Load from disk (no lock held during async I/O)
        let mut state = DirectorySyncState::load_or_create(directory, schema_node_id).await?;

        // Reconcile UUIDs with schema to fix any drift
        if let Err(e) = state.reconcile_with_schema(directory).await {
            warn!("Failed to reconcile subdir UUIDs with schema: {}", e);
        }

        let arc_state = Arc::new(RwLock::new(state));

        // Insert into cache (acquire write lock again)
        // Another thread might have inserted while we were loading, so check again
        let existing_clone = {
            let cache = self.cache.read().unwrap();
            cache.get(directory).cloned()
        };

        if let Some(existing) = existing_clone {
            // Another thread loaded it while we were loading - validate node_id before using
            let existing_guard = existing.read().await;
            if existing_guard.schema.node_id == schema_node_id {
                drop(existing_guard);
                return Ok(existing);
            }
            // node_id mismatches - use our freshly loaded state and update cache
            drop(existing_guard);
        }

        // Insert our freshly loaded state (either no existing or existing had wrong node_id)
        let mut cache = self.cache.write().unwrap();
        cache.insert(directory.to_path_buf(), arc_state.clone());
        Ok(arc_state)
    }

    /// Remove a subdirectory state from cache (e.g., when directory is deleted).
    pub fn remove(&self, directory: &Path) {
        let mut cache = self.cache.write().unwrap();
        cache.remove(directory);
    }
}

impl Default for SubdirStateCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use yrs::{GetString, Text, WriteTxn};

    #[test]
    fn test_crdt_peer_state_new() {
        let node_id = Uuid::new_v4();
        let state = CrdtPeerState::new(node_id);
        assert_eq!(state.node_id, node_id);
        assert!(state.head_cid.is_none());
        assert!(state.local_head_cid.is_none());
        assert!(state.yjs_state.is_none());
    }

    #[test]
    fn test_crdt_peer_state_from_doc() {
        let node_id = Uuid::new_v4();
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello world");
        }

        let state = CrdtPeerState::from_doc(node_id, &doc);
        assert!(state.yjs_state.is_some());

        // Round-trip test
        let restored_doc = state.to_doc().unwrap();
        let txn = restored_doc.transact();
        let text = txn.get_text("content").unwrap();
        assert_eq!(text.get_string(&txn), "hello world");
    }

    #[test]
    fn test_is_cid_known() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        assert!(!state.is_cid_known("abc123"));

        state.head_cid = Some("abc123".to_string());
        assert!(state.is_cid_known("abc123"));
        assert!(!state.is_cid_known("def456"));

        state.local_head_cid = Some("def456".to_string());
        assert!(state.is_cid_known("abc123"));
        assert!(state.is_cid_known("def456"));
    }

    #[test]
    fn test_directory_sync_state_new() {
        let schema_node_id = Uuid::new_v4();
        let state = DirectorySyncState::new(schema_node_id);
        assert_eq!(state.version, DirectorySyncState::CURRENT_VERSION);
        assert_eq!(state.schema.node_id, schema_node_id);
        assert!(state.files.is_empty());
    }

    #[test]
    fn test_directory_sync_state_files() {
        let mut state = DirectorySyncState::new(Uuid::new_v4());
        let file_node_id = Uuid::new_v4();

        // Get or create
        let file_state = state.get_or_create_file("test.txt", file_node_id);
        assert_eq!(file_state.node_id, file_node_id);

        // Has file
        assert!(state.has_file("test.txt"));
        assert!(!state.has_file("other.txt"));

        // Get file
        assert!(state.get_file("test.txt").is_some());
        assert!(state.get_file("other.txt").is_none());

        // Remove file
        let removed = state.remove_file("test.txt");
        assert!(removed.is_some());
        assert!(!state.has_file("test.txt"));
    }

    #[tokio::test]
    async fn test_directory_sync_state_save_load() {
        let dir = tempdir().unwrap();
        let schema_node_id = Uuid::new_v4();
        let file_node_id = Uuid::new_v4();

        let mut state = DirectorySyncState::new(schema_node_id);
        state.schema.head_cid = Some("abc123".to_string());
        state.get_or_create_file("test.txt", file_node_id);

        // Save
        state.save(dir.path()).await.unwrap();

        // Verify file exists
        let state_path = DirectorySyncState::state_file_path(dir.path());
        assert!(state_path.exists());

        // Load
        let loaded = DirectorySyncState::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.version, DirectorySyncState::CURRENT_VERSION);
        assert_eq!(loaded.schema.node_id, schema_node_id);
        assert_eq!(loaded.schema.head_cid, Some("abc123".to_string()));
        assert!(loaded.has_file("test.txt"));
    }

    #[tokio::test]
    async fn test_load_nonexistent() {
        let dir = tempdir().unwrap();
        let result = DirectorySyncState::load(dir.path()).await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema_node_id = Uuid::new_v4();
        let mut state = DirectorySyncState::new(schema_node_id);

        // Create a doc with content
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "test content");
        }

        let file_node_id = Uuid::new_v4();
        let file_state = state.get_or_create_file("test.txt", file_node_id);
        file_state.update_from_doc(&doc);
        file_state.head_cid = Some("cid123".to_string());
        file_state.local_head_cid = Some("cid456".to_string());

        // Serialize and deserialize
        let json = serde_json::to_string(&state).unwrap();
        let loaded: DirectorySyncState = serde_json::from_str(&json).unwrap();

        // Verify
        let loaded_file = loaded.get_file("test.txt").unwrap();
        assert_eq!(loaded_file.head_cid, Some("cid123".to_string()));
        assert_eq!(loaded_file.local_head_cid, Some("cid456".to_string()));

        // Verify Y.Doc content
        let restored_doc = loaded_file.to_doc().unwrap();
        let txn = restored_doc.transact();
        let text = txn.get_text("content").unwrap();
        assert_eq!(text.get_string(&txn), "test content");
    }

    #[test]
    fn test_needs_server_init() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // New state needs init
        assert!(state.needs_server_init());

        // After adding yjs_state, no longer needs init
        state.yjs_state = Some("some_state".to_string());
        assert!(!state.needs_server_init());
    }

    #[test]
    fn test_initialize_from_server() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        assert!(state.needs_server_init());

        // Initialize from server
        state.initialize_from_server("server_yjs_state_b64", "server_cid_123");

        // Verify state is set
        assert!(!state.needs_server_init());
        assert_eq!(state.yjs_state, Some("server_yjs_state_b64".to_string()));
        assert_eq!(state.head_cid, Some("server_cid_123".to_string()));
        assert_eq!(state.local_head_cid, Some("server_cid_123".to_string()));
    }

    /// Test that initializing from server preserves Yjs operations.
    ///
    /// This is the critical fix for the CRDT sync loop bug: by initializing
    /// from the server's state, our Y.Doc shares the same operation history
    /// as the server and other clients.
    #[test]
    fn test_initialize_from_server_preserves_operations() {
        // Create a "server" doc with some content
        let server_doc = Doc::with_client_id(1);
        let server_update = {
            let mut txn = server_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "server content");
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let server_state_b64 = STANDARD.encode(&server_update);

        // Create client state and initialize from server
        let mut client_state = CrdtPeerState::new(Uuid::new_v4());
        client_state.initialize_from_server(&server_state_b64, "server_cid");

        // Verify client doc has the server's content
        let client_doc = client_state.to_doc().unwrap();
        let txn = client_doc.transact();
        let text = txn.get_text("content").unwrap();
        assert_eq!(text.get_string(&txn), "server content");
    }

    /// Test that a client initialized from server can correctly merge a delete.
    ///
    /// This test verifies the fix for CP-f2fu: when a client starts from the
    /// server's state, it can correctly apply delete operations because it
    /// shares the same operation history.
    #[test]
    fn test_initialized_client_can_merge_deletes() {
        use yrs::updates::decoder::Decode;
        use yrs::Update;

        // Server creates content "hello"
        let server_doc = Doc::with_client_id(1);
        {
            let mut txn = server_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }
        let server_state = {
            let txn = server_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let server_state_b64 = STANDARD.encode(&server_state);

        // Client initializes from server state (the fix!)
        let mut client_state = CrdtPeerState::new(Uuid::new_v4());
        client_state.initialize_from_server(&server_state_b64, "cid1");
        let client_doc = client_state.to_doc().unwrap();

        // Server deletes content
        let delete_update = {
            let mut txn = server_doc.transact_mut();
            let text = txn.get_text("content").unwrap();
            text.remove_range(&mut txn, 0, 5); // Delete "hello"
            txn.encode_update_v1()
        };

        // Client applies the delete
        {
            let update = Update::decode_v1(&delete_update).unwrap();
            let mut txn = client_doc.transact_mut();
            txn.apply_update(update);
        }

        // Verify client doc is now empty
        let txn = client_doc.transact();
        let text = txn.get_text("content").unwrap();
        assert_eq!(text.get_string(&txn), "");
    }

    #[test]
    fn test_pending_edit_queue() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Initially no pending edits
        assert!(!state.has_pending_edits());
        assert_eq!(state.pending_edits.len(), 0);

        // Queue some edits
        state.queue_pending_edit(vec![1, 2, 3]);
        assert!(state.has_pending_edits());
        assert_eq!(state.pending_edits.len(), 1);

        state.queue_pending_edit(vec![4, 5, 6]);
        assert_eq!(state.pending_edits.len(), 2);

        // Take pending edits
        let edits = state.take_pending_edits();
        assert_eq!(edits.len(), 2);
        assert_eq!(edits[0].payload, vec![1, 2, 3]);
        assert_eq!(edits[1].payload, vec![4, 5, 6]);

        // Queue should be empty after take
        assert!(!state.has_pending_edits());
        assert_eq!(state.pending_edits.len(), 0);
    }

    #[test]
    fn test_pending_edit_queue_overflow() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Fill queue to max capacity
        for i in 0..MAX_PENDING_EDITS {
            state.queue_pending_edit(vec![i as u8]);
        }
        assert_eq!(state.pending_edits.len(), MAX_PENDING_EDITS);

        // Queue one more - should drop oldest
        state.queue_pending_edit(vec![255]);
        assert_eq!(state.pending_edits.len(), MAX_PENDING_EDITS);

        // First edit should now be [1] (was [0] but got dropped)
        assert_eq!(state.pending_edits[0].payload, vec![1]);

        // Last edit should be the new one
        assert_eq!(
            state.pending_edits[MAX_PENDING_EDITS - 1].payload,
            vec![255]
        );
    }

    #[test]
    fn test_pending_edits_not_serialized() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.queue_pending_edit(vec![1, 2, 3]);

        // Serialize to JSON
        let json = serde_json::to_string(&state).unwrap();

        // pending_edits should not be in the JSON (due to #[serde(skip)])
        assert!(!json.contains("pending_edits"));

        // When deserialized, pending_edits should be empty
        let loaded: CrdtPeerState = serde_json::from_str(&json).unwrap();
        assert!(!loaded.has_pending_edits());
    }

    #[test]
    fn test_should_queue_edits() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // New state: needs server init AND receive task not ready
        assert_eq!(
            state.should_queue_edits(),
            Some(QueueReason::NeedsServerInit)
        );

        // After server init but before receive task ready
        state.yjs_state = Some("some_state".to_string());
        assert_eq!(
            state.should_queue_edits(),
            Some(QueueReason::ReceiveTaskNotReady)
        );

        // After both conditions met
        state.receive_task_ready = true;
        assert_eq!(state.should_queue_edits(), None);
    }

    #[test]
    fn test_mark_receive_task_ready() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.yjs_state = Some("some_state".to_string());

        // Queue some edits before ready
        state.queue_pending_edit(vec![1, 2, 3]);
        state.queue_pending_edit(vec![4, 5, 6]);

        assert!(!state.receive_task_ready);
        assert_eq!(state.pending_edits.len(), 2);

        // Mark ready - should drain pending edits
        let pending = state.mark_receive_task_ready();

        assert!(state.receive_task_ready);
        assert!(state.pending_edits.is_empty());
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].payload, vec![1, 2, 3]);
        assert_eq!(pending[1].payload, vec![4, 5, 6]);
    }

    #[test]
    fn test_receive_task_ready_not_serialized() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.receive_task_ready = true;

        // Serialize to JSON
        let json = serde_json::to_string(&state).unwrap();

        // receive_task_ready should not be in the JSON (due to #[serde(skip)])
        assert!(!json.contains("receive_task_ready"));

        // When deserialized, receive_task_ready should be false (default)
        let loaded: CrdtPeerState = serde_json::from_str(&json).unwrap();
        assert!(!loaded.receive_task_ready);
    }

    #[test]
    fn test_mark_needs_resync_keeps_receive_ready() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.yjs_state = Some("some_state".to_string());
        state.head_cid = Some("cid".to_string());
        state.receive_task_ready = true;

        // Mark needs resync
        state.mark_needs_resync();

        // Yjs state and CIDs should be cleared
        assert!(state.yjs_state.is_none());
        assert!(state.head_cid.is_none());
        // But receive_task_ready should still be true (task is still running)
        assert!(state.receive_task_ready);
    }

    // =========================================================================
    // Sync Buffering Tests (CP-mlgz)
    // =========================================================================

    #[test]
    fn test_should_buffer_for_sync_in_idle() {
        let state = CrdtPeerState::new(Uuid::new_v4());

        // In IDLE state, should NOT buffer edits
        assert!(!state.should_buffer_for_sync());
    }

    #[test]
    fn test_should_buffer_for_sync_in_active_states() {
        use crate::sync::sync_state_machine::SyncEvent;

        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Transition to COMPARING
        state.sync_state_mut().apply(SyncEvent::StartSync).unwrap();
        assert!(state.should_buffer_for_sync());

        // Transition to PULLING
        state
            .sync_state_mut()
            .apply(SyncEvent::ServerAhead)
            .unwrap();
        assert!(state.should_buffer_for_sync());

        // Transition to APPLYING
        state
            .sync_state_mut()
            .apply(SyncEvent::CommitsReceived)
            .unwrap();
        assert!(state.should_buffer_for_sync());

        // Transition back to IDLE
        state
            .sync_state_mut()
            .apply(SyncEvent::CommitsApplied {
                has_local_commits: false,
            })
            .unwrap();
        assert!(!state.should_buffer_for_sync());
    }

    #[test]
    fn test_buffer_sync_edit() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        assert!(!state.has_sync_buffered_edits());

        // Buffer an edit
        let result = state.buffer_sync_edit(vec![1, 2, 3], vec!["parent1".to_string()]);
        assert!(result); // No overflow
        assert!(state.has_sync_buffered_edits());
        assert_eq!(state.sync_buffered_edits.len(), 1);

        // Buffer another edit
        state.buffer_sync_edit(vec![4, 5, 6], vec!["parent2".to_string()]);
        assert_eq!(state.sync_buffered_edits.len(), 2);
    }

    #[test]
    fn test_sync_buffer_overflow() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Fill buffer to max capacity
        for i in 0..MAX_PENDING_EDITS {
            state.buffer_sync_edit(vec![i as u8], vec!["parent".to_string()]);
        }
        assert_eq!(state.sync_buffered_edits.len(), MAX_PENDING_EDITS);

        // Buffer one more - should drop oldest
        let result = state.buffer_sync_edit(vec![255], vec!["new_parent".to_string()]);
        assert!(!result); // Overflow occurred
        assert_eq!(state.sync_buffered_edits.len(), MAX_PENDING_EDITS);

        // First edit should now be [1] (was [0] but got dropped)
        assert_eq!(state.sync_buffered_edits[0].payload, vec![1]);

        // Last edit should be the new one
        assert_eq!(
            state.sync_buffered_edits[MAX_PENDING_EDITS - 1].payload,
            vec![255]
        );
    }

    #[test]
    fn test_drain_sync_buffer_fast_forward() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Buffer edits with various parents
        state.buffer_sync_edit(vec![1], vec!["new_head".to_string()]); // Fast-forward
        state.buffer_sync_edit(vec![2], vec!["old_head".to_string()]); // Stale
        state.buffer_sync_edit(vec![3], vec!["new_head".to_string()]); // Fast-forward

        let (fast_forward, stale) = state.drain_sync_buffer("new_head");

        // Check fast-forward edits
        assert_eq!(fast_forward.len(), 2);
        assert_eq!(fast_forward[0].payload, vec![1]);
        assert_eq!(fast_forward[1].payload, vec![3]);

        // Check stale edits
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].payload, vec![2]);

        // Buffer should be empty after drain
        assert!(!state.has_sync_buffered_edits());
    }

    #[test]
    fn test_drain_sync_buffer_merge_edits_are_stale() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Merge commit with 2 parents is NOT a fast-forward
        state.buffer_sync_edit(
            vec![1],
            vec!["new_head".to_string(), "other_branch".to_string()],
        );

        let (fast_forward, stale) = state.drain_sync_buffer("new_head");

        // Merge edits should be stale (need full sync)
        assert_eq!(fast_forward.len(), 0);
        assert_eq!(stale.len(), 1);
    }

    #[test]
    fn test_take_sync_buffered_edits() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        state.buffer_sync_edit(vec![1], vec!["p1".to_string()]);
        state.buffer_sync_edit(vec![2], vec!["p2".to_string()]);

        let edits = state.take_sync_buffered_edits();

        assert_eq!(edits.len(), 2);
        assert!(!state.has_sync_buffered_edits());
    }

    #[test]
    fn test_sync_state_machine_access() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Access immutable reference
        assert_eq!(
            state.sync_state().state(),
            crate::sync::sync_state_machine::ClientSyncState::Idle
        );

        // Access mutable reference and make transition
        use crate::sync::sync_state_machine::SyncEvent;
        state.sync_state_mut().apply(SyncEvent::StartSync).unwrap();

        assert_eq!(
            state.sync_state().state(),
            crate::sync::sync_state_machine::ClientSyncState::Comparing
        );
    }

    #[test]
    fn test_sync_state_not_serialized() {
        use crate::sync::sync_state_machine::SyncEvent;

        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Change sync state
        state.sync_state_mut().apply(SyncEvent::StartSync).unwrap();

        // Buffer some edits
        state.buffer_sync_edit(vec![1, 2, 3], vec!["parent".to_string()]);

        // Serialize to JSON
        let json = serde_json::to_string(&state).unwrap();

        // sync_state_machine and sync_buffered_edits should not be in the JSON
        assert!(!json.contains("sync_state_machine"));
        assert!(!json.contains("sync_buffered_edits"));

        // When deserialized, sync state should be IDLE and buffer empty
        let loaded: CrdtPeerState = serde_json::from_str(&json).unwrap();
        assert!(!loaded.should_buffer_for_sync()); // IDLE state
        assert!(!loaded.has_sync_buffered_edits());
    }
}
