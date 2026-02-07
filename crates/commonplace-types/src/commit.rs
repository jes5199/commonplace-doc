use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// A commit in the document history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    /// Parent commit IDs (empty for initial commit, one for normal commit, two for merge)
    pub parents: Vec<String>,

    /// Unix timestamp in milliseconds
    pub timestamp: u64,

    /// Yjs update as a base64-encoded string
    pub update: String,

    /// Author identifier
    pub author: String,

    /// Optional commit message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Allow arbitrary extension fields
    #[serde(flatten)]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

impl Commit {
    /// Create a new commit
    pub fn new(
        parents: Vec<String>,
        update: String,
        author: String,
        message: Option<String>,
    ) -> Self {
        Self {
            parents,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            update,
            author,
            message,
            extensions: BTreeMap::new(),
        }
    }

    /// Calculate the content ID (CID) of this commit based on its content
    pub fn calculate_cid(&self) -> String {
        let json = serde_json::to_string(self).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Check if this is a merge commit (has 2 parents)
    pub fn is_merge(&self) -> bool {
        self.parents.len() == 2
    }

    /// Check if this is an initial commit (no parents)
    pub fn is_initial(&self) -> bool {
        self.parents.is_empty()
    }

    /// Create a commit with an explicit timestamp.
    ///
    /// Used when reconstructing a commit from a received message to calculate
    /// the CID (which depends on the original timestamp, not current time).
    pub fn with_timestamp(
        parents: Vec<String>,
        update: String,
        author: String,
        message: Option<String>,
        timestamp: u64,
    ) -> Self {
        Self {
            parents,
            timestamp,
            update,
            author,
            message,
            extensions: BTreeMap::new(),
        }
    }

    /// Check if this is a snapshot/compaction commit
    pub fn is_snapshot(&self) -> bool {
        self.extensions
            .get("is_snapshot")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Mark this commit as a snapshot/compaction commit
    pub fn set_snapshot(&mut self) {
        self.extensions
            .insert("is_snapshot".to_string(), serde_json::Value::Bool(true));
    }

    /// Create a new snapshot commit with compacted state
    pub fn new_snapshot(parent: String, compacted_update: String, author: String) -> Self {
        let mut commit = Self::new(
            vec![parent],
            compacted_update,
            author,
            Some("Compaction snapshot".to_string()),
        );
        commit.set_snapshot();
        commit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_creation() {
        let commit = Commit::new(
            vec!["parent1".to_string()],
            "update_data".to_string(),
            "alice".to_string(),
            Some("Test commit".to_string()),
        );

        assert_eq!(commit.parents.len(), 1);
        assert_eq!(commit.update, "update_data");
        assert_eq!(commit.author, "alice");
        assert_eq!(commit.message, Some("Test commit".to_string()));
        assert!(!commit.is_merge());
        assert!(!commit.is_initial());
    }

    #[test]
    fn test_merge_commit() {
        let commit = Commit::new(
            vec!["parent1".to_string(), "parent2".to_string()],
            String::new(),
            "alice".to_string(),
            None,
        );

        assert!(commit.is_merge());
        assert!(!commit.is_initial());
    }

    #[test]
    fn test_initial_commit() {
        let commit = Commit::new(
            vec![],
            "initial_data".to_string(),
            "alice".to_string(),
            None,
        );

        assert!(commit.is_initial());
        assert!(!commit.is_merge());
    }

    #[test]
    fn test_cid_calculation() {
        let commit = Commit::new(
            vec!["parent1".to_string()],
            "update_data".to_string(),
            "alice".to_string(),
            Some("Test commit".to_string()),
        );

        let cid = commit.calculate_cid();
        assert!(!cid.is_empty());
        assert_eq!(cid.len(), 64); // SHA-256 hex is 64 chars

        // Same commit should produce same CID
        let cid2 = commit.calculate_cid();
        assert_eq!(cid, cid2);
    }
}
