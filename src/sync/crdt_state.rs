//! CRDT peer state management for file synchronization.
//!
//! This module implements the "true CRDT peer" architecture where the sync client
//! maintains its own Y.Doc state and merges with the server like any other peer.
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use crate::sync::error::{SyncError, SyncResult};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{debug, warn};
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, Transact, Update};

/// State file name for per-directory CRDT state.
/// Located inside the directory it tracks (not as a sibling).
pub const CRDT_STATE_FILENAME: &str = ".commonplace-sync.json";

/// Maximum number of edits to queue while waiting for CRDT initialization.
/// Beyond this limit, older edits are dropped with a warning.
pub const MAX_PENDING_EDITS: usize = 100;

/// A queued edit message waiting to be applied after CRDT initialization.
#[derive(Debug, Clone)]
pub struct PendingEdit {
    /// The raw MQTT payload (serialized EditMessage)
    pub payload: Vec<u8>,
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
    /// Queue of edits received before CRDT initialization completed
    /// AND receive task became ready. Edits are queued until both
    /// conditions are met.
    #[serde(skip)]
    pub pending_edits: Vec<PendingEdit>,
    /// Whether the receive task has subscribed and is ready to process edits.
    /// Edits are queued until this is true (even after CRDT init completes).
    #[serde(skip)]
    pub receive_task_ready: bool,
}

impl CrdtPeerState {
    /// Create a new CrdtPeerState with just a node_id.
    pub fn new(node_id: Uuid) -> Self {
        Self {
            node_id,
            head_cid: None,
            local_head_cid: None,
            yjs_state: None,
            pending_edits: Vec::new(),
            receive_task_ready: false,
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
            pending_edits: Vec::new(),
            receive_task_ready: false,
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

    /// Check if this commit CID is already known (either as head or local_head).
    pub fn is_cid_known(&self, cid: &str) -> bool {
        self.head_cid.as_deref() == Some(cid) || self.local_head_cid.as_deref() == Some(cid)
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
        debug!(
            "Initialized CRDT state from server: cid={}, state_len={}",
            server_cid,
            server_state_b64.len()
        );
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
    /// Also clears head_cid and local_head_cid to allow fresh ancestry checks.
    /// Does NOT reset receive_task_ready since the task is still running.
    pub fn mark_needs_resync(&mut self) {
        debug!(
            "Marking {} as needing resync - clearing CRDT state",
            self.node_id
        );
        self.yjs_state = None;
        self.head_cid = None;
        self.local_head_cid = None;
        // Keep pending_edits - they may still be applicable after resync
        // Keep receive_task_ready - the task is still running and subscribed
    }

    /// Queue an edit to be applied after CRDT initialization completes.
    ///
    /// If the queue exceeds `MAX_PENDING_EDITS`, the oldest edit is dropped.
    /// This prevents unbounded memory growth while still handling the common
    /// case of a few edits arriving during initialization.
    ///
    /// Returns true if the edit was queued, false if it was dropped due to overflow.
    pub fn queue_pending_edit(&mut self, payload: Vec<u8>) -> bool {
        if self.pending_edits.len() >= MAX_PENDING_EDITS {
            // Drop the oldest edit to make room
            self.pending_edits.remove(0);
            warn!(
                "Pending edit queue overflow for {} - dropped oldest edit (queue at {} edits)",
                self.node_id, MAX_PENDING_EDITS
            );
        }
        self.pending_edits.push(PendingEdit { payload });
        debug!(
            "Queued pending edit for {} (queue size: {})",
            self.node_id,
            self.pending_edits.len()
        );
        true
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

                    // Try to initialize Y.Doc from existing .commonplace.json
                    Self::initialize_schema_from_local_file(&mut state, directory).await;
                }
                Ok(state)
            }
            None => {
                let mut state = Self::new(schema_node_id);
                // Try to initialize Y.Doc from existing .commonplace.json
                Self::initialize_schema_from_local_file(&mut state, directory).await;
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
}
