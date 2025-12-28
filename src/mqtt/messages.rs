//! MQTT message types for the commonplace protocol.
//!
//! Defines the message formats for all four ports:
//! - Edits: Yjs updates with commit metadata
//! - Sync: Git-like protocol for history catch-up
//! - Events: Node broadcasts
//! - Commands: Commands to nodes

use serde::{Deserialize, Serialize};

/// Message published to the edits port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditMessage {
    /// Base64-encoded Yjs update
    pub update: String,
    /// Parent commit IDs (0 for initial, 1 for linear, 2 for merge)
    pub parents: Vec<String>,
    /// Author identifier
    pub author: String,
    /// Optional commit message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Timestamp in milliseconds since Unix epoch
    pub timestamp: u64,
}

/// Message published to the events port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    /// Event payload (arbitrary JSON)
    pub payload: serde_json::Value,
    /// Source node ID
    pub source: String,
}

/// Message received on the commands port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMessage {
    /// Command payload (arbitrary JSON)
    pub payload: serde_json::Value,
    /// Optional source identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Sync protocol messages.
///
/// All messages include a `req` field for request/response correlation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SyncMessage {
    // ========== Requests (from client) ==========
    /// Get current HEAD commit (request and response use same type)
    /// Request: { "type": "head", "req": "..." }
    /// Response: { "type": "head", "req": "...", "commit": "..." }
    Head {
        /// Request ID for correlation
        req: String,
        /// Current HEAD commit ID (only in response, None if empty)
        #[serde(skip_serializing_if = "Option::is_none")]
        commit: Option<String>,
    },

    /// Fetch specific commits by ID
    Get {
        /// Request ID for correlation
        req: String,
        /// Commit IDs to fetch
        commits: Vec<String>,
    },

    /// Incremental sync (like git pull)
    Pull {
        /// Request ID for correlation
        req: String,
        /// Commit IDs the client already has
        have: Vec<String>,
        /// Target commit (or "HEAD")
        want: String,
    },

    /// Full history (like git clone)
    Ancestors {
        /// Request ID for correlation
        req: String,
        /// Starting commit (or "HEAD")
        commit: String,
        /// Optional depth limit (None = full history)
        #[serde(skip_serializing_if = "Option::is_none")]
        depth: Option<u32>,
    },

    // ========== Responses (from doc store) ==========
    /// Commit data
    Commit {
        /// Request ID for correlation
        req: String,
        /// Commit ID
        id: String,
        /// Parent commit IDs
        parents: Vec<String>,
        /// Base64-encoded Yjs update
        data: String,
        /// Timestamp in milliseconds
        timestamp: u64,
        /// Author
        author: String,
        /// Optional message
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },

    /// All requested commits have been sent
    Done {
        /// Request ID for correlation
        req: String,
        /// List of commit IDs that were sent
        commits: Vec<String>,
    },

    /// Error response
    Error {
        /// Request ID for correlation
        req: String,
        /// Error message
        message: String,
    },
}

impl SyncMessage {
    /// Get the request ID from any sync message.
    pub fn req(&self) -> &str {
        match self {
            SyncMessage::Head { req, .. } => req,
            SyncMessage::Get { req, .. } => req,
            SyncMessage::Pull { req, .. } => req,
            SyncMessage::Ancestors { req, .. } => req,
            SyncMessage::Commit { req, .. } => req,
            SyncMessage::Done { req, .. } => req,
            SyncMessage::Error { req, .. } => req,
        }
    }

    /// Check if this is a request message (from client).
    /// Head requests have commit=None, Head responses have commit=Some.
    pub fn is_request(&self) -> bool {
        match self {
            SyncMessage::Head { commit, .. } => commit.is_none(),
            SyncMessage::Get { .. } | SyncMessage::Pull { .. } | SyncMessage::Ancestors { .. } => {
                true
            }
            _ => false,
        }
    }

    /// Check if this is a response message (from doc store).
    pub fn is_response(&self) -> bool {
        match self {
            SyncMessage::Head { commit, .. } => commit.is_some(),
            SyncMessage::Commit { .. } | SyncMessage::Done { .. } | SyncMessage::Error { .. } => {
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edit_message_serialize() {
        let msg = EditMessage {
            update: "base64data".to_string(),
            parents: vec!["abc123".to_string()],
            author: "user@example.com".to_string(),
            message: Some("Update content".to_string()),
            timestamp: 1704067200000,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"update\":\"base64data\""));
        assert!(json.contains("\"author\":\"user@example.com\""));
    }

    #[test]
    fn test_sync_head_request() {
        let msg = SyncMessage::Head {
            req: "r-001".to_string(),
            commit: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"head\""));
        assert!(json.contains("\"req\":\"r-001\""));
        // commit field should be omitted when None
        assert!(!json.contains("\"commit\""));
    }

    #[test]
    fn test_sync_head_response() {
        let msg = SyncMessage::Head {
            req: "r-001".to_string(),
            commit: Some("abc123".to_string()),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"head\""));
        assert!(json.contains("\"commit\":\"abc123\""));
    }

    #[test]
    fn test_sync_pull_request() {
        let msg = SyncMessage::Pull {
            req: "r-002".to_string(),
            have: vec!["abc123".to_string()],
            want: "HEAD".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"pull\""));
        assert!(json.contains("\"have\":[\"abc123\"]"));
        assert!(json.contains("\"want\":\"HEAD\""));
    }

    #[test]
    fn test_sync_commit_response() {
        let msg = SyncMessage::Commit {
            req: "r-003".to_string(),
            id: "def456".to_string(),
            parents: vec!["abc123".to_string()],
            data: "base64update".to_string(),
            timestamp: 1704067200000,
            author: "user".to_string(),
            message: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"commit\""));
        assert!(json.contains("\"id\":\"def456\""));
    }

    #[test]
    fn test_sync_deserialize_head() {
        let json = r#"{"type":"head","req":"r-001"}"#;
        let msg: SyncMessage = serde_json::from_str(json).unwrap();

        match msg {
            SyncMessage::Head { req, commit } => {
                assert_eq!(req, "r-001");
                assert!(commit.is_none());
            }
            _ => panic!("Expected Head message"),
        }
    }

    #[test]
    fn test_is_request() {
        assert!(SyncMessage::Head {
            req: "r".to_string(),
            commit: None,
        }
        .is_request());
        assert!(!SyncMessage::Head {
            req: "r".to_string(),
            commit: Some("abc".to_string()),
        }
        .is_request());
        assert!(SyncMessage::Pull {
            req: "r".to_string(),
            have: vec![],
            want: "HEAD".to_string()
        }
        .is_request());
        assert!(!SyncMessage::Done {
            req: "r".to_string(),
            commits: vec![]
        }
        .is_request());
    }
}
