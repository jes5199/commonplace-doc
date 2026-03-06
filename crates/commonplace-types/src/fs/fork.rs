//! Fork manifest types for directory fork operations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tracks the relationship between a forked directory and its source.
///
/// Every document in the forked subtree gets an entry in `document_map`,
/// mapping the new UUID to the original UUID and fork-point commit.
/// This preserves the information needed for future diff/merge operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkManifest {
    /// Unique ID for this manifest
    pub id: String,
    /// UUID of the source directory that was forked
    pub forked_from: String,
    /// Timestamp (ms since epoch) when the fork was created
    pub forked_at: u64,
    /// Map from new UUID -> ForkEntry for every document in the forked subtree
    pub document_map: HashMap<String, ForkEntry>,
}

/// A single document's fork provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkEntry {
    /// UUID of the original document
    pub original_uuid: String,
    /// Commit CID at which the fork was taken
    pub fork_point_commit: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fork_manifest_serialization() {
        let mut document_map = HashMap::new();
        document_map.insert(
            "new-uuid-1".to_string(),
            ForkEntry {
                original_uuid: "orig-uuid-1".to_string(),
                fork_point_commit: "bafy...abc".to_string(),
            },
        );

        let manifest = ForkManifest {
            id: "manifest-uuid".to_string(),
            forked_from: "source-dir-uuid".to_string(),
            forked_at: 1709683200000,
            document_map,
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: ForkManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "manifest-uuid");
        assert_eq!(deserialized.forked_from, "source-dir-uuid");
        assert_eq!(deserialized.forked_at, 1709683200000);
        assert_eq!(deserialized.document_map.len(), 1);
        let entry = deserialized.document_map.get("new-uuid-1").unwrap();
        assert_eq!(entry.original_uuid, "orig-uuid-1");
        assert_eq!(entry.fork_point_commit, "bafy...abc");
    }
}
