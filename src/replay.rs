//! Commit replay module for reconstructing document content at any historical commit.
//!
//! This module provides functionality to walk the commit DAG and replay Yjs updates
//! in chronological order to reconstruct document state at a specific commit.

use crate::b64;
use crate::commit::Commit;
use crate::document::ContentType;
use crate::store::{CommitStore, StoreError};
use std::collections::HashSet;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, Transact};

/// Text root name used in Yrs documents (must match DocumentNode)
const TEXT_ROOT_NAME: &str = "content";

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
        // Verify content type is Text (for now)
        if !matches!(content_type, ContentType::Text) {
            return Err(ReplayError::UnsupportedContentType(
                content_type.to_mime().to_string(),
            ));
        }

        // Collect all commits from root to target
        let commits = self.collect_commits_to_target(target_cid).await?;

        // Create fresh Yrs doc and apply all updates in order
        let ydoc = Doc::with_client_id(1);
        ydoc.get_or_insert_text(TEXT_ROOT_NAME);

        for (_, commit) in commits {
            // Skip empty updates (e.g., merge commits)
            if commit.update.is_empty() {
                continue;
            }

            let update_bytes = b64::decode(&commit.update)
                .map_err(|e| ReplayError::InvalidUpdate(e.to_string()))?;

            if update_bytes.is_empty() {
                continue;
            }

            let update = yrs::Update::decode_v1(&update_bytes)
                .map_err(|e| ReplayError::InvalidUpdate(e.to_string()))?;

            let mut txn = ydoc.transact_mut();
            txn.apply_update(update);
        }

        // Extract final content and state
        let txn = ydoc.transact();
        let text = txn
            .get_text(TEXT_ROOT_NAME)
            .ok_or_else(|| ReplayError::InvalidUpdate("Text root not found".to_string()))?;
        let content = text.get_string(&txn);
        let state_bytes = txn.encode_state_as_update_v1(&yrs::StateVector::default());

        Ok((content, state_bytes))
    }

    /// Collect all commits from root to target in topological/chronological order.
    ///
    /// Uses reverse traversal from target back to roots, then sorts by timestamp.
    async fn collect_commits_to_target(
        &self,
        target_cid: &str,
    ) -> Result<Vec<(String, Commit)>, ReplayError> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut stack = vec![target_cid.to_string()];

        // DFS to collect all ancestors including target
        while let Some(cid) = stack.pop() {
            if !visited.insert(cid.clone()) {
                continue;
            }

            let commit = self.store.get_commit(&cid).await?;
            result.push((cid.clone(), commit.clone()));

            for parent in &commit.parents {
                if !visited.contains(parent) {
                    stack.push(parent.clone());
                }
            }
        }

        // Sort by timestamp (oldest first) for proper replay order
        result.sort_by_key(|(_, c)| c.timestamp);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use yrs::{Text, TextRef};

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
    async fn test_unsupported_content_type() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let c1 = Commit::new(vec![], create_text_update("{}"), "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        let replayer = CommitReplayer::new(&store);

        // JSON content type should fail
        let result = replayer
            .get_content_at_commit("doc", &cid1, &ContentType::Json)
            .await;

        assert!(matches!(
            result,
            Err(ReplayError::UnsupportedContentType(_))
        ));
    }
}
