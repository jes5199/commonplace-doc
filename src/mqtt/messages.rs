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
    /// Request current HEAD commit
    /// { "type": "head", "req": "..." }
    Head {
        /// Request ID for correlation
        req: String,
    },

    /// Response to HEAD request
    /// { "type": "head_response", "req": "...", "commit": "abc123" }
    /// or { "type": "head_response", "req": "..." } if no commits exist
    #[serde(rename = "head_response")]
    HeadResponse {
        /// Request ID for correlation
        req: String,
        /// Current HEAD commit ID (None if document has no commits)
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

/// Request to create a new document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDocumentRequest {
    /// Request ID for correlation
    pub req: String,
    /// MIME content type (e.g., "text/plain", "application/json")
    pub content_type: String,
}

/// Response with created document UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDocumentResponse {
    /// Request ID for correlation
    pub req: String,
    /// Created document UUID (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    /// Error message (present on failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to delete a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDocumentRequest {
    /// Request ID for correlation
    pub req: String,
    /// Document ID to delete
    pub id: String,
}

/// Response to delete document request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDocumentResponse {
    /// Request ID for correlation
    pub req: String,
    /// Whether the document was deleted
    pub deleted: bool,
    /// Error message (present on failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to get document content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetContentRequest {
    /// Request ID for correlation
    pub req: String,
    /// Document ID to get content for
    pub id: String,
}

/// Response with document content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetContentResponse {
    /// Request ID for correlation
    pub req: String,
    /// Document content (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Content type (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Error message (present on failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to get document info/metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetInfoRequest {
    /// Request ID for correlation
    pub req: String,
    /// Document ID to get info for
    pub id: String,
}

/// Response with document info/metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetInfoResponse {
    /// Request ID for correlation
    pub req: String,
    /// Document ID (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Content type (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Error message (present on failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to replace document content (computes diff).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaceContentRequest {
    /// Request ID for correlation
    pub req: String,
    /// Document ID
    pub id: String,
    /// New content to replace with
    pub content: String,
    /// Optional parent commit ID (for offline sync)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_cid: Option<String>,
    /// Optional author identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

/// Summary of replace operation changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaceSummary {
    /// Number of characters inserted
    pub chars_inserted: usize,
    /// Number of characters deleted
    pub chars_deleted: usize,
    /// Number of diff operations
    pub operations: usize,
}

/// Response to replace content request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaceContentResponse {
    /// Request ID for correlation
    pub req: String,
    /// Commit ID (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<String>,
    /// Edit commit ID for the diff (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edit_cid: Option<String>,
    /// Summary of changes (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<ReplaceSummary>,
    /// Error message (present on failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl SyncMessage {
    /// Get the request ID from any sync message.
    pub fn req(&self) -> &str {
        match self {
            SyncMessage::Head { req } => req,
            SyncMessage::HeadResponse { req, .. } => req,
            SyncMessage::Get { req, .. } => req,
            SyncMessage::Pull { req, .. } => req,
            SyncMessage::Ancestors { req, .. } => req,
            SyncMessage::Commit { req, .. } => req,
            SyncMessage::Done { req, .. } => req,
            SyncMessage::Error { req, .. } => req,
        }
    }

    /// Check if this is a request message (from client).
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            SyncMessage::Head { .. }
                | SyncMessage::Get { .. }
                | SyncMessage::Pull { .. }
                | SyncMessage::Ancestors { .. }
        )
    }

    /// Check if this is a response message (from doc store).
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            SyncMessage::HeadResponse { .. }
                | SyncMessage::Commit { .. }
                | SyncMessage::Done { .. }
                | SyncMessage::Error { .. }
        )
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
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"head\""));
        assert!(json.contains("\"req\":\"r-001\""));
        // No commit field in request
        assert!(!json.contains("\"commit\""));
    }

    #[test]
    fn test_sync_head_response_with_commit() {
        let msg = SyncMessage::HeadResponse {
            req: "r-001".to_string(),
            commit: Some("abc123".to_string()),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"head_response\""));
        assert!(json.contains("\"commit\":\"abc123\""));
    }

    #[test]
    fn test_sync_head_response_empty() {
        let msg = SyncMessage::HeadResponse {
            req: "r-001".to_string(),
            commit: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"head_response\""));
        // commit field should be omitted when None
        assert!(!json.contains("\"commit\""));
        // But it's still clearly a response due to type
        assert!(msg.is_response());
        assert!(!msg.is_request());
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
    fn test_sync_deserialize_head_request() {
        let json = r#"{"type":"head","req":"r-001"}"#;
        let msg: SyncMessage = serde_json::from_str(json).unwrap();

        match msg {
            SyncMessage::Head { req } => {
                assert_eq!(req, "r-001");
            }
            _ => panic!("Expected Head message"),
        }
    }

    #[test]
    fn test_sync_deserialize_head_response() {
        // With commit
        let json = r#"{"type":"head_response","req":"r-001","commit":"abc123"}"#;
        let msg: SyncMessage = serde_json::from_str(json).unwrap();

        match msg {
            SyncMessage::HeadResponse { req, commit } => {
                assert_eq!(req, "r-001");
                assert_eq!(commit, Some("abc123".to_string()));
            }
            _ => panic!("Expected HeadResponse message"),
        }

        // Without commit (empty document)
        let json = r#"{"type":"head_response","req":"r-002"}"#;
        let msg: SyncMessage = serde_json::from_str(json).unwrap();

        match msg {
            SyncMessage::HeadResponse { req, commit } => {
                assert_eq!(req, "r-002");
                assert!(commit.is_none());
            }
            _ => panic!("Expected HeadResponse message"),
        }
    }

    #[test]
    fn test_is_request() {
        assert!(SyncMessage::Head {
            req: "r".to_string(),
        }
        .is_request());
        assert!(!SyncMessage::HeadResponse {
            req: "r".to_string(),
            commit: None,
        }
        .is_request());
        assert!(!SyncMessage::HeadResponse {
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

    #[test]
    fn test_create_document_request_serialize() {
        let req = CreateDocumentRequest {
            req: "r-001".to_string(),
            content_type: "text/plain".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"content_type\":\"text/plain\""));
    }

    #[test]
    fn test_create_document_response_success() {
        let resp = CreateDocumentResponse {
            req: "r-001".to_string(),
            uuid: Some("abc-123".to_string()),
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"uuid\":\"abc-123\""));
        // error should be omitted when None
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_create_document_response_error() {
        let resp = CreateDocumentResponse {
            req: "r-001".to_string(),
            uuid: None,
            error: Some("Invalid content type".to_string()),
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"error\":\"Invalid content type\""));
        // uuid should be omitted when None
        assert!(!json.contains("\"uuid\""));
    }

    #[test]
    fn test_delete_document_request_serialize() {
        let req = DeleteDocumentRequest {
            req: "r-001".to_string(),
            id: "doc-123".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"id\":\"doc-123\""));
    }

    #[test]
    fn test_delete_document_response_success() {
        let resp = DeleteDocumentResponse {
            req: "r-001".to_string(),
            deleted: true,
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"deleted\":true"));
        // error should be omitted when None
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_delete_document_response_not_found() {
        let resp = DeleteDocumentResponse {
            req: "r-001".to_string(),
            deleted: false,
            error: Some("Document not found".to_string()),
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"deleted\":false"));
        assert!(json.contains("\"error\":\"Document not found\""));
    }

    #[test]
    fn test_get_content_request_serialize() {
        let req = GetContentRequest {
            req: "r-001".to_string(),
            id: "doc-123".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"id\":\"doc-123\""));
    }

    #[test]
    fn test_get_content_response_success() {
        let resp = GetContentResponse {
            req: "r-001".to_string(),
            content: Some("Hello, world!".to_string()),
            content_type: Some("text/plain".to_string()),
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"content\":\"Hello, world!\""));
        assert!(json.contains("\"content_type\":\"text/plain\""));
        // error should be omitted when None
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_get_content_response_not_found() {
        let resp = GetContentResponse {
            req: "r-001".to_string(),
            content: None,
            content_type: None,
            error: Some("Document not found".to_string()),
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"error\":\"Document not found\""));
        // content and content_type should be omitted when None
        assert!(!json.contains("\"content\":"));
        assert!(!json.contains("\"content_type\":"));
    }

    #[test]
    fn test_get_info_request_serialize() {
        let req = GetInfoRequest {
            req: "r-001".to_string(),
            id: "doc-123".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"id\":\"doc-123\""));
    }

    #[test]
    fn test_get_info_response_success() {
        let resp = GetInfoResponse {
            req: "r-001".to_string(),
            id: Some("doc-123".to_string()),
            content_type: Some("text/plain".to_string()),
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"id\":\"doc-123\""));
        assert!(json.contains("\"content_type\":\"text/plain\""));
        // error should be omitted when None
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_get_info_response_not_found() {
        let resp = GetInfoResponse {
            req: "r-001".to_string(),
            id: None,
            content_type: None,
            error: Some("Document not found".to_string()),
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"error\":\"Document not found\""));
        // id and content_type should be omitted when None
        assert!(!json.contains("\"id\":"));
        assert!(!json.contains("\"content_type\":"));
    }
}
