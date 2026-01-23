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
}
