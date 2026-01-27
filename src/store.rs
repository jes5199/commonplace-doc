use crate::commit::Commit;
use redb::{Database, TableDefinition};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

// Table definitions
const COMMITS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("commits");
const DOC_HEADS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("doc_heads");

#[derive(Debug, Clone)]
pub enum StoreError {
    DatabaseError(String),
    CommitNotFound(String),
    ParentNotFound(String),
    NotMonotonicDescendent(String),
    InvalidUpdate(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            StoreError::CommitNotFound(cid) => write!(f, "Commit not found: {}", cid),
            StoreError::ParentNotFound(cid) => write!(f, "Parent commit not found: {}", cid),
            StoreError::NotMonotonicDescendent(msg) => {
                write!(f, "Not a monotonic descendent: {}", msg)
            }
            StoreError::InvalidUpdate(msg) => write!(f, "Invalid update: {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}

pub struct CommitStore {
    db: Arc<RwLock<Database>>,
}

impl CommitStore {
    /// Create or open a commit store at the given path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let db = Database::create(path).map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
        })
    }

    /// Store a commit and return its CID
    pub async fn store_commit(&self, commit: &Commit) -> Result<String, StoreError> {
        let cid = commit.calculate_cid();
        let commit_json =
            serde_json::to_string(commit).map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        let db = self.db.write().await;
        let write_txn = db
            .begin_write()
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        {
            let mut table = write_txn
                .open_table(COMMITS_TABLE)
                .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

            table
                .insert(cid.as_str(), commit_json.as_str())
                .map_err(|e| StoreError::DatabaseError(e.to_string()))?;
        }

        write_txn
            .commit()
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        Ok(cid)
    }

    /// Get a commit by CID
    pub async fn get_commit(&self, cid: &str) -> Result<Commit, StoreError> {
        let db = self.db.read().await;
        let read_txn = db
            .begin_read()
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        let table = read_txn
            .open_table(COMMITS_TABLE)
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        let commit_json = table
            .get(cid)
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?
            .ok_or_else(|| StoreError::CommitNotFound(cid.to_string()))?;

        serde_json::from_str(commit_json.value())
            .map_err(|e| StoreError::DatabaseError(e.to_string()))
    }

    /// Store a commit and update the document head atomically.
    ///
    /// Returns (cid, timestamp) for optional broadcast.
    /// This is the preferred method for persisting commits as it keeps
    /// storage and head-update behavior aligned across ingress paths.
    ///
    /// NOTE: This method always advances HEAD. For cyan sync protocol compliance,
    /// use `store_commit_with_head_advancement` which implements the proper
    /// fast-forward/merge rules.
    pub async fn store_commit_and_set_head(
        &self,
        doc_id: &str,
        commit: &Commit,
    ) -> Result<(String, u64), StoreError> {
        let timestamp = commit.timestamp;
        let cid = self.store_commit(commit).await?;
        self.set_document_head(doc_id, &cid).await?;
        Ok((cid, timestamp))
    }

    /// Store a commit and conditionally advance HEAD based on cyan sync protocol rules.
    ///
    /// HEAD advancement rules:
    /// - **Fast-forward**: If the new commit's parents include the current HEAD, advance HEAD
    /// - **Merge**: If the commit has 2+ parents AND current HEAD is an ancestor, advance HEAD
    /// - **Otherwise**: Persist the commit but don't advance HEAD (divergent branch)
    ///
    /// Returns `(cid, timestamp, head_advanced)` where `head_advanced` indicates whether
    /// HEAD was updated. This is useful for ack messages in the sync protocol.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID
    /// * `commit` - The commit to store
    ///
    /// # Returns
    /// * `Ok((cid, timestamp, head_advanced))` on success
    /// * `Err(StoreError)` on failure
    pub async fn store_commit_with_head_advancement(
        &self,
        doc_id: &str,
        commit: &Commit,
    ) -> Result<(String, u64, bool), StoreError> {
        let timestamp = commit.timestamp;
        let cid = commit.calculate_cid();

        // First, store the commit (always persisted regardless of HEAD advancement)
        self.store_commit(commit).await?;

        // Get current HEAD
        let current_head = self.get_document_head(doc_id).await?;

        // Determine if HEAD should be advanced
        let should_advance = match &current_head {
            None => {
                // No HEAD yet - always advance for first commit
                true
            }
            Some(head_cid) => {
                // Check if this is a fast-forward: commit's parents include current HEAD
                let is_fast_forward = commit.parents.contains(head_cid);

                if is_fast_forward {
                    true
                } else if commit.parents.len() >= 2 {
                    // Merge commit: check if current HEAD is an ancestor of this commit
                    // For a merge to advance HEAD, current HEAD must be reachable from the commit
                    self.is_ancestor(head_cid, &cid).await?
                } else {
                    // Single-parent commit that doesn't include current HEAD
                    // This is a divergent branch - don't advance HEAD
                    false
                }
            }
        };

        if should_advance {
            self.set_document_head(doc_id, &cid).await?;
        }

        Ok((cid, timestamp, should_advance))
    }

    /// Set the head commit for a document
    pub async fn set_document_head(&self, doc_id: &str, cid: &str) -> Result<(), StoreError> {
        let db = self.db.write().await;
        let write_txn = db
            .begin_write()
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        {
            let mut table = write_txn
                .open_table(DOC_HEADS_TABLE)
                .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

            table
                .insert(doc_id, cid)
                .map_err(|e| StoreError::DatabaseError(e.to_string()))?;
        }

        write_txn
            .commit()
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    /// Get the head commit for a document
    pub async fn get_document_head(&self, doc_id: &str) -> Result<Option<String>, StoreError> {
        let db = self.db.read().await;
        let read_txn = db
            .begin_read()
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?;

        // Try to open the table - if it doesn't exist, return None
        let table = match read_txn.open_table(DOC_HEADS_TABLE) {
            Ok(t) => t,
            Err(_) => return Ok(None), // Table doesn't exist yet, no head set
        };

        let result = table
            .get(doc_id)
            .map_err(|e| StoreError::DatabaseError(e.to_string()))?
            .map(|v| v.value().to_string());

        Ok(result)
    }

    /// Get all commits for a document with timestamps greater than or equal to `since`
    pub async fn get_commits_since(
        &self,
        doc_id: &str,
        since: u64,
    ) -> Result<Vec<(String, Commit)>, StoreError> {
        let Some(head_cid) = self.get_document_head(doc_id).await? else {
            return Ok(Vec::new());
        };

        let mut stack = vec![head_cid];
        let mut visited = HashSet::new();
        let mut commits = Vec::new();

        while let Some(cid) = stack.pop() {
            if !visited.insert(cid.clone()) {
                continue;
            }

            // Try to get the commit, but skip if not found (broken DAG link)
            let commit = match self.get_commit(&cid).await {
                Ok(c) => c,
                Err(StoreError::CommitNotFound(_)) => {
                    tracing::warn!(
                        "Commit {} not found while traversing DAG for doc {}, skipping",
                        cid,
                        doc_id
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            if commit.timestamp >= since {
                commits.push((cid.clone(), commit.clone()));
            }

            for parent in &commit.parents {
                stack.push(parent.clone());
            }
        }

        commits.sort_by_key(|(_, commit)| commit.timestamp);

        Ok(commits)
    }

    /// Check if a commit is an ancestor of another
    pub fn is_ancestor<'a>(
        &'a self,
        ancestor_cid: &'a str,
        descendent_cid: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, StoreError>> + 'a + Send>>
    {
        Box::pin(async move {
            if ancestor_cid == descendent_cid {
                return Ok(true);
            }

            let descendent = self.get_commit(descendent_cid).await?;

            for parent_cid in &descendent.parents {
                if self.is_ancestor(ancestor_cid, parent_cid).await? {
                    return Ok(true);
                }
            }

            Ok(false)
        })
    }

    /// Validate that a new commit can be added to a document
    /// Returns Ok(()) if the commit is a valid monotonic descendent of the current head
    pub async fn validate_monotonic_descent(
        &self,
        doc_id: &str,
        new_commit_cid: &str,
    ) -> Result<(), StoreError> {
        let current_head = self.get_document_head(doc_id).await?;

        // If there's no current head, any commit is valid
        let Some(current_head_cid) = current_head else {
            return Ok(());
        };

        // The new commit must have the current head as an ancestor
        if !self.is_ancestor(&current_head_cid, new_commit_cid).await? {
            return Err(StoreError::NotMonotonicDescendent(format!(
                "Commit {} is not a descendent of current head {}",
                new_commit_cid, current_head_cid
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_store_and_retrieve_commit() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let commit = Commit::new(
            vec![],
            "test_update".to_string(),
            "alice".to_string(),
            Some("Test".to_string()),
        );

        let cid = store.store_commit(&commit).await.unwrap();
        let retrieved = store.get_commit(&cid).await.unwrap();

        assert_eq!(retrieved.update, "test_update");
        assert_eq!(retrieved.author, "alice");
    }

    #[tokio::test]
    async fn test_document_head() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";
        let cid = "commit_abc123";

        // Initially no head
        assert!(store.get_document_head(doc_id).await.unwrap().is_none());

        // Set head
        store.set_document_head(doc_id, cid).await.unwrap();

        // Retrieve head
        let head = store.get_document_head(doc_id).await.unwrap();
        assert_eq!(head, Some(cid.to_string()));
    }

    #[tokio::test]
    async fn test_is_ancestor() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        // Create a chain: c1 -> c2 -> c3
        let c1 = Commit::new(vec![], "u1".to_string(), "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();

        let c2 = Commit::new(
            vec![cid1.clone()],
            "u2".to_string(),
            "alice".to_string(),
            None,
        );
        let cid2 = store.store_commit(&c2).await.unwrap();

        let c3 = Commit::new(
            vec![cid2.clone()],
            "u3".to_string(),
            "alice".to_string(),
            None,
        );
        let cid3 = store.store_commit(&c3).await.unwrap();

        // Test ancestry
        assert!(store.is_ancestor(&cid1, &cid3).await.unwrap());
        assert!(store.is_ancestor(&cid2, &cid3).await.unwrap());
        assert!(store.is_ancestor(&cid3, &cid3).await.unwrap());
        assert!(!store.is_ancestor(&cid3, &cid1).await.unwrap());
    }

    #[tokio::test]
    async fn test_validate_monotonic_descent() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // Create initial commit
        let c1 = Commit::new(vec![], "u1".to_string(), "alice".to_string(), None);
        let cid1 = store.store_commit(&c1).await.unwrap();
        store.set_document_head(doc_id, &cid1).await.unwrap();

        // Create descendent commit
        let c2 = Commit::new(
            vec![cid1.clone()],
            "u2".to_string(),
            "alice".to_string(),
            None,
        );
        let cid2 = store.store_commit(&c2).await.unwrap();

        // Should be valid
        assert!(store
            .validate_monotonic_descent(doc_id, &cid2)
            .await
            .is_ok());

        // Create non-descendent commit
        let c3 = Commit::new(vec![], "u3".to_string(), "alice".to_string(), None);
        let cid3 = store.store_commit(&c3).await.unwrap();

        // Should be invalid
        assert!(store
            .validate_monotonic_descent(doc_id, &cid3)
            .await
            .is_err());
    }

    // ========================================================================
    // Tests for HEAD advancement logic (cyan sync protocol)
    // ========================================================================

    #[tokio::test]
    async fn test_head_advancement_first_commit() {
        // First commit should always advance HEAD
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // First commit to a document - should advance HEAD
        let c1 = Commit::new(vec![], "u1".to_string(), "alice".to_string(), None);
        let (cid1, _ts, advanced) = store
            .store_commit_with_head_advancement(doc_id, &c1)
            .await
            .unwrap();

        assert!(advanced, "First commit should advance HEAD");
        assert_eq!(store.get_document_head(doc_id).await.unwrap(), Some(cid1));
    }

    #[tokio::test]
    async fn test_head_advancement_fast_forward() {
        // Fast-forward: commit's parents include current HEAD
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // Initial commit
        let c1 = Commit::new(vec![], "u1".to_string(), "alice".to_string(), None);
        let (cid1, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c1)
            .await
            .unwrap();

        // Fast-forward commit (parent is current HEAD)
        let c2 = Commit::new(
            vec![cid1.clone()],
            "u2".to_string(),
            "alice".to_string(),
            None,
        );
        let (cid2, _ts, advanced) = store
            .store_commit_with_head_advancement(doc_id, &c2)
            .await
            .unwrap();

        assert!(advanced, "Fast-forward should advance HEAD");
        assert_eq!(store.get_document_head(doc_id).await.unwrap(), Some(cid2));
    }

    #[tokio::test]
    async fn test_head_advancement_divergent_no_advance() {
        // Divergent commit: single-parent commit that doesn't include current HEAD
        // Should NOT advance HEAD
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // Initial commit A
        let c_a = Commit::new(vec![], "uA".to_string(), "alice".to_string(), None);
        let (cid_a, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_a)
            .await
            .unwrap();

        // Commit B fast-forwards from A
        let c_b = Commit::new(
            vec![cid_a.clone()],
            "uB".to_string(),
            "alice".to_string(),
            None,
        );
        let (cid_b, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_b)
            .await
            .unwrap();

        // HEAD is now B

        // Commit C branches from A (parent is A, not B)
        // This is a divergent branch - should NOT advance HEAD
        let c_c = Commit::new(
            vec![cid_a.clone()],
            "uC".to_string(),
            "bob".to_string(),
            None,
        );
        let (cid_c, _ts, advanced) = store
            .store_commit_with_head_advancement(doc_id, &c_c)
            .await
            .unwrap();

        assert!(!advanced, "Divergent commit should NOT advance HEAD");
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_b.clone()),
            "HEAD should still be B"
        );

        // Verify C was still persisted
        let commit_c = store.get_commit(&cid_c).await;
        assert!(commit_c.is_ok(), "Commit C should still be persisted");
    }

    #[tokio::test]
    async fn test_head_advancement_merge_advances() {
        // Merge commit: 2 parents, and current HEAD is an ancestor
        // Should advance HEAD
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // Initial commit A
        let c_a = Commit::new(vec![], "uA".to_string(), "alice".to_string(), None);
        let (cid_a, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_a)
            .await
            .unwrap();

        // Commit B fast-forwards from A (HEAD = B)
        let c_b = Commit::new(
            vec![cid_a.clone()],
            "uB".to_string(),
            "alice".to_string(),
            None,
        );
        let (cid_b, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_b)
            .await
            .unwrap();

        // Commit C branches from A (persisted but doesn't advance HEAD)
        let c_c = Commit::new(
            vec![cid_a.clone()],
            "uC".to_string(),
            "bob".to_string(),
            None,
        );
        let (cid_c, _, advanced_c) = store
            .store_commit_with_head_advancement(doc_id, &c_c)
            .await
            .unwrap();
        assert!(!advanced_c, "C should not advance HEAD");

        // Merge commit M with parents [B, C]
        // Current HEAD is B, which is a parent of M, so this should advance
        let c_m = Commit::new(
            vec![cid_b.clone(), cid_c.clone()],
            String::new(), // Empty update for merge
            "system".to_string(),
            Some("Merge".to_string()),
        );
        let (cid_m, _ts, advanced) = store
            .store_commit_with_head_advancement(doc_id, &c_m)
            .await
            .unwrap();

        assert!(advanced, "Merge commit should advance HEAD");
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_m),
            "HEAD should be the merge commit"
        );
    }

    #[tokio::test]
    async fn test_head_advancement_merge_from_diverged_no_advance() {
        // Merge commit where current HEAD is NOT an ancestor
        // Example: HEAD=B, merge has parents [C, D] where neither includes B
        // Should NOT advance HEAD
        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // Build a DAG where we have two divergent branches
        // A -> B (HEAD)
        // A -> C -> D
        // Merge [C, D] should NOT advance HEAD since B is not an ancestor

        let c_a = Commit::new(vec![], "uA".to_string(), "alice".to_string(), None);
        let (cid_a, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_a)
            .await
            .unwrap();

        // B from A (this will be HEAD)
        let c_b = Commit::new(
            vec![cid_a.clone()],
            "uB".to_string(),
            "alice".to_string(),
            None,
        );
        let (cid_b, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_b)
            .await
            .unwrap();

        // C from A (divergent, doesn't advance HEAD)
        let c_c = Commit::new(
            vec![cid_a.clone()],
            "uC".to_string(),
            "bob".to_string(),
            None,
        );
        let (cid_c, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_c)
            .await
            .unwrap();

        // D from C (also divergent)
        let c_d = Commit::new(
            vec![cid_c.clone()],
            "uD".to_string(),
            "bob".to_string(),
            None,
        );
        let (cid_d, _, _) = store
            .store_commit_with_head_advancement(doc_id, &c_d)
            .await
            .unwrap();

        // Merge [C, D] - this does NOT include B, so should NOT advance HEAD
        let c_merge_cd = Commit::new(
            vec![cid_c.clone(), cid_d.clone()],
            String::new(),
            "system".to_string(),
            Some("Merge C+D".to_string()),
        );
        let (_cid_merge_cd, _ts, advanced) = store
            .store_commit_with_head_advancement(doc_id, &c_merge_cd)
            .await
            .unwrap();

        assert!(
            !advanced,
            "Merge that doesn't include HEAD as ancestor should NOT advance HEAD"
        );
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_b),
            "HEAD should still be B"
        );
    }

    #[tokio::test]
    async fn test_head_advancement_full_scenario() {
        // Full scenario from the cyan sync protocol example:
        // Initial: HEAD = A
        // Client 1 pushes B (parent: A) -> HEAD = B (fast-forward)
        // Client 2 pushes C (parent: A) -> C persisted, HEAD stays B (divergent)
        // Client 2 pushes M (parents: B, C) -> HEAD = M (merge includes B)

        let temp_file = NamedTempFile::new().unwrap();
        let store = CommitStore::new(temp_file.path()).unwrap();

        let doc_id = "doc1";

        // A: initial commit
        let c_a = Commit::new(vec![], "uA".to_string(), "init".to_string(), None);
        let (cid_a, _, advanced_a) = store
            .store_commit_with_head_advancement(doc_id, &c_a)
            .await
            .unwrap();
        assert!(advanced_a);
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_a.clone())
        );

        // B: fast-forward from A
        let c_b = Commit::new(
            vec![cid_a.clone()],
            "uB".to_string(),
            "client1".to_string(),
            None,
        );
        let (cid_b, _, advanced_b) = store
            .store_commit_with_head_advancement(doc_id, &c_b)
            .await
            .unwrap();
        assert!(advanced_b, "B should fast-forward");
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_b.clone())
        );

        // C: divergent from A (doesn't include B as parent)
        let c_c = Commit::new(
            vec![cid_a.clone()],
            "uC".to_string(),
            "client2".to_string(),
            None,
        );
        let (cid_c, _, advanced_c) = store
            .store_commit_with_head_advancement(doc_id, &c_c)
            .await
            .unwrap();
        assert!(!advanced_c, "C should NOT advance HEAD (divergent)");
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_b.clone()),
            "HEAD should still be B"
        );

        // M: merge commit with parents [B, C]
        let c_m = Commit::new(
            vec![cid_b.clone(), cid_c.clone()],
            String::new(),
            "client2".to_string(),
            Some("Merge".to_string()),
        );
        let (cid_m, _, advanced_m) = store
            .store_commit_with_head_advancement(doc_id, &c_m)
            .await
            .unwrap();
        assert!(advanced_m, "M should advance HEAD (merge includes B)");
        assert_eq!(
            store.get_document_head(doc_id).await.unwrap(),
            Some(cid_m),
            "HEAD should be M"
        );
    }
}
