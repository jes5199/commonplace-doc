//! Commit replay module for reconstructing document content at any historical commit.
//!
//! This module provides functionality to walk the commit DAG and replay Yjs updates
//! in chronological order to reconstruct document state at a specific commit.
//!
//! ## Compaction
//!
//! Long chains of incremental updates can be compacted into a single state update
//! using the `compact_updates` function. This is useful for:
//! - Faster replay of long histories
//! - More compact storage
//! - Creating snapshot commits

use crate::b64;
use crate::commit::Commit;
use crate::document::ContentType;
use crate::store::{CommitStore, StoreError};
use std::collections::HashSet;
use tracing::debug;
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, Transact, Value};

/// Text root name used in Yrs documents (must match DocumentNode)
const TEXT_ROOT_NAME: &str = "content";

/// XML document constants (must match DocumentStore)
const XML_HEADER: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;
const XML_ROOT_START: &str = "<root>";
const XML_ROOT_END: &str = "</root>";

/// Wrap inner XML content with header and root tags
fn wrap_xml_root(inner: &str) -> String {
    if inner.is_empty() {
        return format!("{}<root/>", XML_HEADER);
    }
    format!("{}{}{}{}", XML_HEADER, XML_ROOT_START, inner, XML_ROOT_END)
}

/// Error type for replay operations
#[derive(Debug)]
pub enum ReplayError {
    /// Database/store error
    StoreError(StoreError),
    /// Invalid Yjs update
    InvalidUpdate(String),
    /// Commit not in document history
    CommitNotInHistory(String),
    /// Content type not supported for replay
    UnsupportedContentType(String),
}

impl From<StoreError> for ReplayError {
    fn from(e: StoreError) -> Self {
        ReplayError::StoreError(e)
    }
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::StoreError(e) => write!(f, "Store error: {}", e),
            ReplayError::InvalidUpdate(msg) => write!(f, "Invalid update: {}", msg),
            ReplayError::CommitNotInHistory(cid) => {
                write!(f, "Commit not in document history: {}", cid)
            }
            ReplayError::UnsupportedContentType(t) => {
                write!(f, "Unsupported content type for replace: {}", t)
            }
        }
    }
}

impl std::error::Error for ReplayError {}

/// Configuration for automatic compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum commits since last snapshot to trigger compaction
    pub min_commits: usize,
    /// Minimum ratio of update bytes to content bytes to trigger compaction
    pub min_update_to_content_ratio: f64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_commits: 100,
            min_update_to_content_ratio: 10.0,
        }
    }
}

/// Statistics about a replay operation, useful for compaction decisions
#[derive(Debug, Clone)]
pub struct ReplayStats {
    /// Number of commits replayed
    pub commit_count: usize,
    /// Total bytes of Yjs updates applied
    pub total_update_bytes: usize,
    /// Size of rendered content in bytes
    pub content_bytes: usize,
    /// Whether a snapshot was found and used
    pub started_from_snapshot: bool,
}

impl ReplayStats {
    /// Check if compaction should be performed based on the given config
    pub fn should_compact(&self, config: &CompactionConfig) -> bool {
        if self.commit_count < config.min_commits {
            return false;
        }

        if self.content_bytes == 0 {
            // Avoid division by zero; empty docs don't need compaction
            return false;
        }

        let ratio = self.total_update_bytes as f64 / self.content_bytes as f64;
        ratio >= config.min_update_to_content_ratio
    }
}

/// Compact a sequence of Yjs updates into a single state update.
///
/// This takes multiple incremental updates and produces a single update that
/// represents the same final state. The compacted update can be used to
/// replace a long chain of commits with a single "snapshot" commit.
///
/// # Arguments
/// * `updates` - Base64-encoded Yjs updates in chronological order
/// * `content_type` - Document content type (determines root structure)
///
/// # Returns
/// A base64-encoded Yjs update containing the full compacted state
///
/// # Example
/// ```ignore
/// let updates = vec![update1_b64, update2_b64, update3_b64];
/// let compacted = compact_updates(&updates, &ContentType::Text)?;
/// // compacted contains the same state as applying all three updates
/// ```
pub fn compact_updates(
    updates: &[String],
    content_type: &ContentType,
) -> Result<String, ReplayError> {
    // Create fresh Yrs doc with appropriate root type
    let ydoc = Doc::with_client_id(1);
    match content_type {
        ContentType::Text => {
            ydoc.get_or_insert_text(TEXT_ROOT_NAME);
        }
        ContentType::Json => {
            ydoc.get_or_insert_map(TEXT_ROOT_NAME);
        }
        ContentType::JsonArray | ContentType::Jsonl => {
            ydoc.get_or_insert_array(TEXT_ROOT_NAME);
        }
        ContentType::Xml => {
            ydoc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);
        }
    }

    debug!(
        "Compacting {} updates for {:?}",
        updates.len(),
        content_type
    );

    // Apply all updates
    for (i, update_b64) in updates.iter().enumerate() {
        if update_b64.is_empty() {
            debug!("  [{}]: empty update, skipping", i);
            continue;
        }

        let update_bytes = b64::decode(update_b64)
            .map_err(|e| ReplayError::InvalidUpdate(format!("decode error at {}: {}", i, e)))?;

        if update_bytes.is_empty() {
            debug!("  [{}]: empty after decode, skipping", i);
            continue;
        }

        let update = yrs::Update::decode_v1(&update_bytes)
            .map_err(|e| ReplayError::InvalidUpdate(format!("invalid update at {}: {}", i, e)))?;

        let mut txn = ydoc.transact_mut();
        txn.apply_update(update);
        debug!("  [{}]: applied {} bytes", i, update_bytes.len());
    }

    // Encode full state as a single update
    let txn = ydoc.transact();
    let compacted_bytes = txn.encode_state_as_update_v1(&yrs::StateVector::default());
    let compacted_b64 = b64::encode(&compacted_bytes);

    debug!(
        "Compacted {} updates ({} total chars) into {} bytes",
        updates.len(),
        updates.iter().map(|u| u.len()).sum::<usize>(),
        compacted_bytes.len()
    );

    Ok(compacted_b64)
}

/// Replays commits from a document's history to reconstruct content at a specific commit.
pub struct CommitReplayer<'a> {
    store: &'a CommitStore,
}

impl<'a> CommitReplayer<'a> {
    /// Create a new CommitReplayer with the given commit store.
    pub fn new(store: &'a CommitStore) -> Self {
        Self { store }
    }

    /// Get the content of a document at a specific commit by replaying all updates.
    ///
    /// # Arguments
    /// * `_doc_id` - The document ID (used for validation, currently unused in replay)
    /// * `target_cid` - The commit ID to replay up to
    /// * `content_type` - The content type of the document
    ///
    /// # Returns
    /// The text content at that commit state
    pub async fn get_content_at_commit(
        &self,
        _doc_id: &str,
        target_cid: &str,
        content_type: &ContentType,
    ) -> Result<String, ReplayError> {
        let (content, _) = self
            .get_content_and_state_at_commit(_doc_id, target_cid, content_type)
            .await?;
        Ok(content)
    }

    /// Get both the content and Yjs state at a specific commit.
    ///
    /// This is useful when you need both the text content and the actual Yjs
    /// state bytes (for computing diffs that are compatible with the commit chain).
    ///
    /// # Returns
    /// A tuple of (text content, Yjs state update bytes)
    pub async fn get_content_and_state_at_commit(
        &self,
        _doc_id: &str,
        target_cid: &str,
        content_type: &ContentType,
    ) -> Result<(String, Vec<u8>), ReplayError> {
        let (content, state, _stats) = self
            .get_content_state_and_stats_at_commit(_doc_id, target_cid, content_type)
            .await?;
        Ok((content, state))
    }

    /// Get content, Yjs state, and replay statistics at a specific commit.
    ///
    /// This is the full replay method that also returns statistics useful for
    /// deciding whether compaction should be performed.
    ///
    /// # Returns
    /// A tuple of (text content, Yjs state bytes, replay stats)
    pub async fn get_content_state_and_stats_at_commit(
        &self,
        _doc_id: &str,
        target_cid: &str,
        content_type: &ContentType,
    ) -> Result<(String, Vec<u8>, ReplayStats), ReplayError> {
        // Collect commits from nearest snapshot (or root) to target
        let commits = self.collect_commits_to_target(target_cid).await?;

        // Check if we started from a snapshot
        let started_from_snapshot = commits
            .first()
            .map(|(_, c)| c.is_snapshot())
            .unwrap_or(false);

        // Create fresh Yrs doc with appropriate root type
        let ydoc = Doc::with_client_id(1);
        match content_type {
            ContentType::Text => {
                ydoc.get_or_insert_text(TEXT_ROOT_NAME);
            }
            ContentType::Json => {
                ydoc.get_or_insert_map(TEXT_ROOT_NAME);
            }
            ContentType::JsonArray | ContentType::Jsonl => {
                ydoc.get_or_insert_array(TEXT_ROOT_NAME);
            }
            ContentType::Xml => {
                ydoc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);
            }
        }

        let commit_count = commits.len();
        let mut total_update_bytes: usize = 0;

        debug!(
            "Replaying {} commits to target {}",
            commit_count, target_cid
        );

        for (cid, commit) in &commits {
            // Skip empty updates (e.g., merge commits)
            if commit.update.is_empty() {
                debug!(
                    "  {} (ts={}): EMPTY update (merge commit)",
                    &cid[..8],
                    commit.timestamp
                );
                continue;
            }

            let update_bytes = b64::decode(&commit.update)
                .map_err(|e| ReplayError::InvalidUpdate(e.to_string()))?;

            if update_bytes.is_empty() {
                debug!(
                    "  {} (ts={}): empty after decode",
                    &cid[..8],
                    commit.timestamp
                );
                continue;
            }

            // Track bytes for compaction decisions
            total_update_bytes += update_bytes.len();

            debug!(
                "  {} (ts={}): applying {} bytes",
                &cid[..8],
                commit.timestamp,
                update_bytes.len()
            );

            let update = yrs::Update::decode_v1(&update_bytes)
                .map_err(|e| ReplayError::InvalidUpdate(e.to_string()))?;

            let mut txn = ydoc.transact_mut();
            txn.apply_update(update);
        }

        // Extract final content and state based on content type
        let txn = ydoc.transact();
        let content = match content_type {
            ContentType::Text => {
                let text = txn
                    .get_text(TEXT_ROOT_NAME)
                    .ok_or_else(|| ReplayError::InvalidUpdate("Text root not found".to_string()))?;
                text.get_string(&txn)
            }
            ContentType::Json => {
                let root = txn
                    .root_refs()
                    .find(|(name, _)| *name == TEXT_ROOT_NAME)
                    .map(|(_, value)| value);

                match root {
                    Some(Value::YMap(map)) => {
                        let any = map.to_json(&txn);
                        serde_json::to_string(&any).map_err(|e| {
                            ReplayError::InvalidUpdate(format!("JSON serialization: {}", e))
                        })?
                    }
                    _ => ContentType::Json.default_content(),
                }
            }
            ContentType::JsonArray => {
                let root = txn
                    .root_refs()
                    .find(|(name, _)| *name == TEXT_ROOT_NAME)
                    .map(|(_, value)| value);

                match root {
                    Some(Value::YArray(array)) => {
                        let any = array.to_json(&txn);
                        serde_json::to_string(&any).map_err(|e| {
                            ReplayError::InvalidUpdate(format!("JSON serialization: {}", e))
                        })?
                    }
                    _ => ContentType::JsonArray.default_content(),
                }
            }
            ContentType::Jsonl => {
                let root = txn
                    .root_refs()
                    .find(|(name, _)| *name == TEXT_ROOT_NAME)
                    .map(|(_, value)| value);

                match root {
                    Some(Value::YArray(array)) => {
                        let any = array.to_json(&txn);
                        let json_value = serde_json::to_value(&any).map_err(|e| {
                            ReplayError::InvalidUpdate(format!("JSON serialization: {}", e))
                        })?;
                        if let serde_json::Value::Array(items) = json_value {
                            items
                                .iter()
                                .map(serde_json::to_string)
                                .collect::<Result<Vec<_>, _>>()
                                .map(|lines| lines.join("\n"))
                                .map_err(|e| {
                                    ReplayError::InvalidUpdate(format!(
                                        "JSONL serialization: {}",
                                        e
                                    ))
                                })?
                        } else {
                            ContentType::Jsonl.default_content()
                        }
                    }
                    _ => ContentType::Jsonl.default_content(),
                }
            }
            ContentType::Xml => {
                let fragment = txn.get_xml_fragment(TEXT_ROOT_NAME).ok_or_else(|| {
                    ReplayError::InvalidUpdate("XmlFragment not found".to_string())
                })?;
                let inner = fragment.get_string(&txn);
                wrap_xml_root(&inner)
            }
        };
        let state_bytes = txn.encode_state_as_update_v1(&yrs::StateVector::default());

        let stats = ReplayStats {
            commit_count,
            total_update_bytes,
            content_bytes: content.len(),
            started_from_snapshot,
        };

        Ok((content, state_bytes, stats))
    }

    /// Collect commits from nearest snapshot (or root) to target in chronological order.
    ///
    /// Uses reverse traversal from target back to the first snapshot commit found,
    /// or to roots if no snapshot exists. This optimizes replay by skipping
    /// commits that are already compacted into a snapshot.
    async fn collect_commits_to_target(
        &self,
        target_cid: &str,
    ) -> Result<Vec<(String, Commit)>, ReplayError> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut stack = vec![target_cid.to_string()];
        let mut found_snapshot = false;

        // DFS to collect ancestors, stopping at first snapshot
        while let Some(cid) = stack.pop() {
            if !visited.insert(cid.clone()) {
                continue;
            }

            let commit = self.store.get_commit(&cid).await?;
            let is_snapshot = commit.is_snapshot();
            result.push((cid.clone(), commit.clone()));

            // If this is a snapshot, don't traverse its parents
            // The snapshot already contains the full state up to that point
            if is_snapshot {
                found_snapshot = true;
                debug!(
                    "Found snapshot commit {} - stopping ancestor traversal",
                    &cid[..8]
                );
                continue;
            }

            for parent in &commit.parents {
                if !visited.contains(parent) {
                    stack.push(parent.clone());
                }
            }
        }

        // Sort by timestamp (oldest first) for proper replay order
        // When timestamps are equal, put snapshots first (they contain prior state)
        result.sort_by_key(|(_, c)| (c.timestamp, !c.is_snapshot()));

        debug!(
            "collect_commits_to_target({}) found {} commits{}",
            &target_cid[..8],
            result.len(),
            if found_snapshot {
                " (starting from snapshot)"
            } else {
                ""
            }
        );
        for (i, (cid, commit)) in result.iter().enumerate() {
            debug!(
                "  [{}] {} ts={} parents={:?} update_len={} snapshot={}",
                i,
                &cid[..8],
                commit.timestamp,
                commit.parents.iter().map(|p| &p[..8]).collect::<Vec<_>>(),
                commit.update.len(),
                commit.is_snapshot()
            );
        }

        Ok(result)
    }

    /// Verify that a commit exists and is in the document's history.
    ///
    /// A commit is in the history if it's an ancestor of (or equal to) the current HEAD.
    pub async fn verify_commit_in_history(
        &self,
        doc_id: &str,
        target_cid: &str,
    ) -> Result<bool, ReplayError> {
        // First check if the commit even exists
        if self.store.get_commit(target_cid).await.is_err() {
            return Ok(false);
        }

        let head = self.store.get_document_head(doc_id).await?;

        match head {
            None => {
                // No HEAD means no document history yet
                // The commit exists but isn't in this document's history
                Ok(false)
            }
            Some(head_cid) => {
                // Check if target is ancestor of head (or is head itself)
                Ok(self.store.is_ancestor(target_cid, &head_cid).await?)
            }
        }
    }

    /// Compact all commits from root to target into a single Yjs update.
    ///
    /// This collects all commits in the history leading to `target_cid`,
    /// applies them in order, and returns a single compacted update that
    /// represents the same final state.
    ///
    /// # Arguments
    /// * `target_cid` - The commit to compact up to
    /// * `content_type` - Document content type (determines root structure)
    ///
    /// # Returns
    /// A base64-encoded Yjs update containing the full compacted state
    pub async fn compact_to_commit(
        &self,
        target_cid: &str,
        content_type: &ContentType,
    ) -> Result<String, ReplayError> {
        let commits = self.collect_commits_to_target(target_cid).await?;
        let updates: Vec<String> = commits.into_iter().map(|(_, c)| c.update).collect();
        compact_updates(&updates, content_type)
    }

    /// Check if auto-compaction should be performed and create snapshot if needed.
    ///
    /// This method checks the replay stats against the compaction config and
    /// creates a new snapshot commit if the thresholds are met. The snapshot
    /// is stored in the commit store and the document's HEAD is updated.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID
    /// * `head_cid` - Current HEAD commit ID
    /// * `content_type` - Document content type
    /// * `config` - Compaction configuration thresholds
    ///
    /// # Returns
    /// `Ok(Some(cid))` if a snapshot was created, `Ok(None)` if no compaction needed
    pub async fn maybe_auto_compact(
        &self,
        doc_id: &str,
        head_cid: &str,
        content_type: &ContentType,
        config: &CompactionConfig,
    ) -> Result<Option<String>, ReplayError> {
        // Get stats for the current HEAD
        let (_content, _state, stats) = self
            .get_content_state_and_stats_at_commit(doc_id, head_cid, content_type)
            .await?;

        debug!(
            "Auto-compaction check for doc={}: {} commits, {} update bytes, {} content bytes, started_from_snapshot={}",
            doc_id, stats.commit_count, stats.total_update_bytes, stats.content_bytes, stats.started_from_snapshot
        );

        if !stats.should_compact(config) {
            debug!("Auto-compaction not needed for doc={}", doc_id);
            return Ok(None);
        }

        debug!(
            "Auto-compaction triggered for doc={}: creating snapshot",
            doc_id
        );

        // Create compacted state
        let compacted_update = self.compact_to_commit(head_cid, content_type).await?;

        // Create snapshot commit
        let snapshot =
            Commit::new_snapshot(head_cid.to_string(), compacted_update, "system".to_string());
        let snapshot_cid = self.store.store_commit(&snapshot).await?;

        // Update document HEAD to point to snapshot
        self.store.set_document_head(doc_id, &snapshot_cid).await?;

        debug!(
            "Auto-compaction complete for doc={}: new HEAD={}",
            doc_id,
            &snapshot_cid[..8]
        );

        Ok(Some(snapshot_cid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use yrs::{Text, TextRef, XmlFragment, XmlTextPrelim};

    /// Helper to create a Yjs update that inserts text
    fn create_text_update(text: &str) -> String {
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);
        let update = {
            let mut txn = doc.transact_mut();
            ytext.push(&mut txn, text);
            txn.encode_update_v1()
        };
        b64::encode(&update)
    }

    /// Helper to create a Yjs update that appends to existing content
    fn create_append_update(doc: &Doc, ytext: &TextRef, text: &str) -> String {
        let update = {
            let mut txn = doc.transact_mut();
            ytext.push(&mut txn, text);
            txn.encode_update_v1()
        };
        b64::encode(&update)
    }

    /// Helper to create a Yjs update that inserts XML content (as XmlText in XmlFragment)
    fn create_xml_update(inner_content: &str) -> String {
        let doc = Doc::with_client_id(1);
        let fragment = doc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);
        let update = {
            let mut txn = doc.transact_mut();
            if !inner_content.is_empty() {
                fragment.push_back(&mut txn, XmlTextPrelim::new(inner_content));
            }
            txn.encode_update_v1()
        };
        b64::encode(&update)
    }

    #[tokio::test]
    async fn test_replay_single_commit() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "test-doc";

        // Create initial commit with "hello"
        let update = create_text_update("hello");
        let c1 = Commit::new(vec![], update, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();
        store.set_document_head(doc_id, &cid1).await.unwrap();

        // Replay
        let replayer = CommitReplayer::new(&store);
        let content = replayer
            .get_content_at_commit(doc_id, &cid1, &ContentType::Text)
            .await
            .unwrap();

        assert_eq!(content, "hello");
    }

    #[tokio::test]
    async fn test_replay_commit_chain() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "test-doc";

        // Build a chain of commits using a shared Yrs doc to generate updates
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        // c1: "" -> "hello"
        let u1 = create_append_update(&doc, &ytext, "hello");
        let c1 = Commit::new(vec![], u1, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        // c2: "hello" -> "hello world"
        let u2 = create_append_update(&doc, &ytext, " world");
        let c2 = Commit::new(vec![cid1.clone()], u2, "alice".to_string(), None);
        let cid2 = store.store_commit(&c2).await.unwrap();

        // c3: "hello world" -> "hello world!"
        let u3 = create_append_update(&doc, &ytext, "!");
        let c3 = Commit::new(vec![cid2.clone()], u3, "alice".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();
        store.set_document_head(doc_id, &cid3).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // Replay at c1
        let content1 = replayer
            .get_content_at_commit(doc_id, &cid1, &ContentType::Text)
            .await
            .unwrap();
        assert_eq!(content1, "hello");

        // Replay at c2
        let content2 = replayer
            .get_content_at_commit(doc_id, &cid2, &ContentType::Text)
            .await
            .unwrap();
        assert_eq!(content2, "hello world");

        // Replay at c3 (HEAD)
        let content3 = replayer
            .get_content_at_commit(doc_id, &cid3, &ContentType::Text)
            .await
            .unwrap();
        assert_eq!(content3, "hello world!");
    }

    #[tokio::test]
    async fn test_verify_commit_in_history() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "test-doc";

        // Create commits
        let c1 = Commit::new(
            vec![],
            create_text_update("hello"),
            "alice".to_string(),
            None,
        );
        let cid1 = store.store_commit(&c1).await.unwrap();

        let c2 = Commit::new(
            vec![cid1.clone()],
            create_text_update(" world"),
            "alice".to_string(),
            None,
        );
        let cid2 = store.store_commit(&c2).await.unwrap();
        store.set_document_head(doc_id, &cid2).await.unwrap();

        // Unrelated commit (not in document's history)
        let c3 = Commit::new(vec![], create_text_update("other"), "bob".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // cid1 and cid2 should be in history
        assert!(replayer
            .verify_commit_in_history(doc_id, &cid1)
            .await
            .unwrap());
        assert!(replayer
            .verify_commit_in_history(doc_id, &cid2)
            .await
            .unwrap());

        // cid3 exists but is not in this document's history
        assert!(!replayer
            .verify_commit_in_history(doc_id, &cid3)
            .await
            .unwrap());

        // Nonexistent commit
        assert!(!replayer
            .verify_commit_in_history(doc_id, "nonexistent")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_replay_with_merge_commit() {
        use yrs::updates::decoder::Decode;

        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "test-doc";

        // c1: initial "hello" (client 1)
        let doc1 = Doc::with_client_id(1);
        let text1 = doc1.get_or_insert_text(TEXT_ROOT_NAME);
        let u1 = {
            let mut txn = doc1.transact_mut();
            text1.push(&mut txn, "hello");
            b64::encode(&txn.encode_update_v1())
        };
        let c1 = Commit::new(vec![], u1.clone(), "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        // c2: branch from c1, adds " alice" (client 2)
        // First sync with c1's state, then make changes
        let doc2 = Doc::with_client_id(2);
        doc2.get_or_insert_text(TEXT_ROOT_NAME);
        {
            // Sync c1's update to doc2
            let update_bytes = b64::decode(&u1).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = doc2.transact_mut();
            txn.apply_update(update);
        }
        let text2 = doc2.get_or_insert_text(TEXT_ROOT_NAME);
        let u2 = {
            let mut txn = doc2.transact_mut();
            text2.push(&mut txn, " alice");
            b64::encode(&txn.encode_update_v1())
        };
        let c2 = Commit::new(vec![cid1.clone()], u2, "alice".to_string(), None);
        let cid2 = store.store_commit(&c2).await.unwrap();

        // c3: branch from c1, adds " bob" (client 3)
        // First sync with c1's state, then make changes
        let doc3 = Doc::with_client_id(3);
        doc3.get_or_insert_text(TEXT_ROOT_NAME);
        {
            // Sync c1's update to doc3
            let update_bytes = b64::decode(&u1).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = doc3.transact_mut();
            txn.apply_update(update);
        }
        let text3 = doc3.get_or_insert_text(TEXT_ROOT_NAME);
        let u3 = {
            let mut txn = doc3.transact_mut();
            text3.push(&mut txn, " bob");
            b64::encode(&txn.encode_update_v1())
        };
        let c3 = Commit::new(vec![cid1.clone()], u3, "bob".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();

        // Merge commit with empty update (CRDT handles convergence)
        let merge = Commit::new(
            vec![cid2.clone(), cid3.clone()],
            String::new(),
            "system".to_string(),
            Some("Merge".to_string()),
        );
        let merge_cid = store.store_commit(&merge).await.unwrap();
        store.set_document_head(doc_id, &merge_cid).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // Replay at merge should include content from both branches
        // The exact result depends on Yjs CRDT merge behavior
        let content = replayer
            .get_content_at_commit(doc_id, &merge_cid, &ContentType::Text)
            .await
            .unwrap();

        // Both " alice" and " bob" should be present (order determined by CRDT)
        assert!(content.contains("hello"));
        assert!(content.contains("alice") || content.contains("bob"));
    }

    #[tokio::test]
    async fn test_xml_content_type() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "xml-doc";

        // Create XML commit with inner content "<hello>world</hello>"
        let update = create_xml_update("<hello>world</hello>");
        let c1 = Commit::new(vec![], update, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();
        store.set_document_head(doc_id, &cid1).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // XML content type should now work
        let content = replayer
            .get_content_at_commit(doc_id, &cid1, &ContentType::Xml)
            .await
            .unwrap();

        // Should have XML header, root, and inner content
        assert_eq!(
            content,
            r#"<?xml version="1.0" encoding="UTF-8"?><root><hello>world</hello></root>"#
        );
    }

    #[tokio::test]
    async fn test_xml_empty_content() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "xml-empty";

        // Create XML commit with empty content
        let update = create_xml_update("");
        let c1 = Commit::new(vec![], update, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();
        store.set_document_head(doc_id, &cid1).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        let content = replayer
            .get_content_at_commit(doc_id, &cid1, &ContentType::Xml)
            .await
            .unwrap();

        // Empty XML should have self-closing root
        assert_eq!(content, r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#);
    }

    // ========================================================================
    // Compaction tests
    // ========================================================================

    #[test]
    fn test_compact_text_updates() {
        // Create a sequence of text updates
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "hello");
        let u2 = create_append_update(&doc, &ytext, " ");
        let u3 = create_append_update(&doc, &ytext, "world");

        // Compact the updates
        let compacted = compact_updates(&[u1, u2, u3], &ContentType::Text).unwrap();

        // Verify the compacted update produces the same content
        let verify_doc = Doc::with_client_id(2);
        verify_doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let update_bytes = b64::decode(&compacted).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = verify_doc.transact();
        let text = txn.get_text(TEXT_ROOT_NAME).unwrap();
        assert_eq!(text.get_string(&txn), "hello world");
    }

    #[test]
    fn test_compact_json_updates() {
        use yrs::Map;

        // Create a sequence of JSON map updates
        let doc = Doc::with_client_id(1);
        let ymap = doc.get_or_insert_map(TEXT_ROOT_NAME);

        // First update: set "name" = "Alice"
        let u1 = {
            let mut txn = doc.transact_mut();
            ymap.insert(&mut txn, "name", "Alice");
            b64::encode(&txn.encode_update_v1())
        };

        // Second update: set "age" = 30
        let u2 = {
            let mut txn = doc.transact_mut();
            ymap.insert(&mut txn, "age", 30i64);
            b64::encode(&txn.encode_update_v1())
        };

        // Third update: set "city" = "NYC"
        let u3 = {
            let mut txn = doc.transact_mut();
            ymap.insert(&mut txn, "city", "NYC");
            b64::encode(&txn.encode_update_v1())
        };

        // Compact the updates
        let compacted = compact_updates(&[u1, u2, u3], &ContentType::Json).unwrap();

        // Verify the compacted update produces the same content
        let verify_doc = Doc::with_client_id(2);
        verify_doc.get_or_insert_map(TEXT_ROOT_NAME);
        {
            let update_bytes = b64::decode(&compacted).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = verify_doc.transact();
        let map = txn.get_map(TEXT_ROOT_NAME).unwrap();
        let json = map.to_json(&txn);

        // Check all values are present
        let obj = match json {
            yrs::Any::Map(m) => m,
            _ => panic!("Expected map"),
        };
        assert_eq!(obj.get("name"), Some(&yrs::Any::String("Alice".into())));
        // Yjs stores integers as f64 internally
        assert_eq!(obj.get("age"), Some(&yrs::Any::Number(30.0)));
        assert_eq!(obj.get("city"), Some(&yrs::Any::String("NYC".into())));
    }

    #[test]
    fn test_compact_empty_updates() {
        // Compacting empty updates should return a valid (empty) state
        let compacted = compact_updates(&[], &ContentType::Text).unwrap();

        // Verify it's a valid update that produces empty content
        let verify_doc = Doc::with_client_id(2);
        verify_doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let update_bytes = b64::decode(&compacted).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = verify_doc.transact();
        let text = txn.get_text(TEXT_ROOT_NAME).unwrap();
        assert_eq!(text.get_string(&txn), "");
    }

    #[test]
    fn test_compact_with_empty_string_updates() {
        // Updates list containing empty strings should be skipped
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "hello");
        let u2 = String::new(); // Empty update (like a merge commit)
        let u3 = create_append_update(&doc, &ytext, " world");

        let compacted = compact_updates(&[u1, u2, u3], &ContentType::Text).unwrap();

        // Verify result
        let verify_doc = Doc::with_client_id(2);
        verify_doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let update_bytes = b64::decode(&compacted).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = verify_doc.transact();
        let text = txn.get_text(TEXT_ROOT_NAME).unwrap();
        assert_eq!(text.get_string(&txn), "hello world");
    }

    #[tokio::test]
    async fn test_compact_to_commit() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "test-compact";

        // Build a chain of commits
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "hello");
        let c1 = Commit::new(vec![], u1, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        let u2 = create_append_update(&doc, &ytext, " ");
        let c2 = Commit::new(vec![cid1.clone()], u2, "alice".to_string(), None);
        let cid2 = store.store_commit(&c2).await.unwrap();

        let u3 = create_append_update(&doc, &ytext, "world");
        let c3 = Commit::new(vec![cid2.clone()], u3, "alice".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();
        store.set_document_head(doc_id, &cid3).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // Compact all commits to HEAD
        let compacted = replayer
            .compact_to_commit(&cid3, &ContentType::Text)
            .await
            .unwrap();

        // Verify compacted state matches replay
        let replay_content = replayer
            .get_content_at_commit(doc_id, &cid3, &ContentType::Text)
            .await
            .unwrap();

        // Apply compacted update to verify
        let verify_doc = Doc::with_client_id(2);
        verify_doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let update_bytes = b64::decode(&compacted).unwrap();
            let update = yrs::Update::decode_v1(&update_bytes).unwrap();
            let mut txn = verify_doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = verify_doc.transact();
        let text = txn.get_text(TEXT_ROOT_NAME).unwrap();
        let compacted_content = text.get_string(&txn);

        assert_eq!(compacted_content, replay_content);
        assert_eq!(compacted_content, "hello world");
    }

    // ========================================================================
    // Replay stats and auto-compaction tests
    // ========================================================================

    #[test]
    fn test_should_compact_below_threshold() {
        let config = CompactionConfig::default();

        // Below commit threshold
        let stats = ReplayStats {
            commit_count: 50,
            total_update_bytes: 10000,
            content_bytes: 100,
            started_from_snapshot: false,
        };
        assert!(!stats.should_compact(&config)); // Only 50 commits, need 100
    }

    #[test]
    fn test_should_compact_above_threshold() {
        let config = CompactionConfig::default();

        // Above both thresholds: 100+ commits and 10x ratio
        let stats = ReplayStats {
            commit_count: 150,
            total_update_bytes: 2000, // 20x the content size
            content_bytes: 100,
            started_from_snapshot: false,
        };
        assert!(stats.should_compact(&config));
    }

    #[test]
    fn test_should_compact_ratio_not_met() {
        let config = CompactionConfig::default();

        // Enough commits but ratio not high enough
        let stats = ReplayStats {
            commit_count: 150,
            total_update_bytes: 500, // Only 5x the content size
            content_bytes: 100,
            started_from_snapshot: false,
        };
        assert!(!stats.should_compact(&config)); // 5x < 10x threshold
    }

    #[test]
    fn test_should_compact_empty_content() {
        let config = CompactionConfig::default();

        // Empty content should not trigger compaction (avoid division by zero)
        let stats = ReplayStats {
            commit_count: 200,
            total_update_bytes: 10000,
            content_bytes: 0,
            started_from_snapshot: false,
        };
        assert!(!stats.should_compact(&config));
    }

    #[tokio::test]
    async fn test_replay_stats_tracking() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "stats-test";

        // Build a chain of commits
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "hello");
        let c1 = Commit::new(vec![], u1, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        let u2 = create_append_update(&doc, &ytext, " world");
        let c2 = Commit::new(vec![cid1.clone()], u2, "alice".to_string(), None);
        let cid2 = store.store_commit(&c2).await.unwrap();
        store.set_document_head(doc_id, &cid2).await.unwrap();

        let replayer = CommitReplayer::new(&store);
        let (content, _state, stats) = replayer
            .get_content_state_and_stats_at_commit(doc_id, &cid2, &ContentType::Text)
            .await
            .unwrap();

        assert_eq!(content, "hello world");
        assert_eq!(stats.commit_count, 2);
        assert!(stats.total_update_bytes > 0);
        assert_eq!(stats.content_bytes, "hello world".len());
        assert!(!stats.started_from_snapshot);
    }

    #[tokio::test]
    async fn test_snapshot_commit_stops_traversal() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "snapshot-test";

        // Build initial commits
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "hello");
        let c1 = Commit::new(vec![], u1, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        let u2 = create_append_update(&doc, &ytext, " ");
        let c2 = Commit::new(vec![cid1.clone()], u2, "alice".to_string(), None);
        let cid2 = store.store_commit(&c2).await.unwrap();

        // Create a snapshot commit containing "hello " state
        let replayer = CommitReplayer::new(&store);
        let compacted = replayer
            .compact_to_commit(&cid2, &ContentType::Text)
            .await
            .unwrap();

        let snapshot = Commit::new_snapshot(cid2.clone(), compacted, "system".to_string());
        let snapshot_cid = store.store_commit(&snapshot).await.unwrap();

        // Add more commits after the snapshot
        let u3 = create_append_update(&doc, &ytext, "world");
        let c3 = Commit::new(vec![snapshot_cid.clone()], u3, "alice".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();
        store.set_document_head(doc_id, &cid3).await.unwrap();

        // Replay should start from snapshot, not from root
        let (content, _state, stats) = replayer
            .get_content_state_and_stats_at_commit(doc_id, &cid3, &ContentType::Text)
            .await
            .unwrap();

        assert_eq!(content, "hello world");
        // Should only have 2 commits: the snapshot and the final commit
        // (not the original c1 and c2)
        assert_eq!(stats.commit_count, 2);
        assert!(stats.started_from_snapshot);
    }

    #[tokio::test]
    async fn test_maybe_auto_compact_triggers() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "auto-compact-test";

        // Use low thresholds for testing
        let config = CompactionConfig {
            min_commits: 3,
            min_update_to_content_ratio: 2.0,
        };

        // Build a chain of commits with small content but larger updates
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "a");
        let c1 = Commit::new(vec![], u1, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        let u2 = create_append_update(&doc, &ytext, "b");
        let c2 = Commit::new(vec![cid1.clone()], u2, "alice".to_string(), None);
        let cid2 = store.store_commit(&c2).await.unwrap();

        let u3 = create_append_update(&doc, &ytext, "c");
        let c3 = Commit::new(vec![cid2.clone()], u3, "alice".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();
        store.set_document_head(doc_id, &cid3).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // Auto-compact should trigger (3 commits, ratio should exceed 2x)
        let result = replayer
            .maybe_auto_compact(doc_id, &cid3, &ContentType::Text, &config)
            .await
            .unwrap();

        assert!(result.is_some(), "Auto-compaction should have triggered");

        let snapshot_cid = result.unwrap();

        // Verify snapshot was stored and is HEAD
        let head = store.get_document_head(doc_id).await.unwrap().unwrap();
        assert_eq!(head, snapshot_cid);

        // Verify the snapshot commit is marked as such
        let snapshot_commit = store.get_commit(&snapshot_cid).await.unwrap();
        assert!(snapshot_commit.is_snapshot());

        // Verify content is preserved after compaction
        let (content, _state, stats) = replayer
            .get_content_state_and_stats_at_commit(doc_id, &snapshot_cid, &ContentType::Text)
            .await
            .unwrap();

        assert_eq!(content, "abc");
        // New HEAD is just the snapshot commit itself
        assert_eq!(stats.commit_count, 1);
    }

    #[tokio::test]
    async fn test_maybe_auto_compact_no_trigger() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "no-compact-test";

        // Use high thresholds that won't be met
        let config = CompactionConfig {
            min_commits: 100,
            min_update_to_content_ratio: 10.0,
        };

        // Just one commit
        let doc = Doc::with_client_id(1);
        let ytext = doc.get_or_insert_text(TEXT_ROOT_NAME);

        let u1 = create_append_update(&doc, &ytext, "hello");
        let c1 = Commit::new(vec![], u1, "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();
        store.set_document_head(doc_id, &cid1).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // Should not trigger compaction (only 1 commit)
        let result = replayer
            .maybe_auto_compact(doc_id, &cid1, &ContentType::Text, &config)
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Auto-compaction should not have triggered"
        );

        // HEAD should remain unchanged
        let head = store.get_document_head(doc_id).await.unwrap().unwrap();
        assert_eq!(head, cid1);
    }
}
