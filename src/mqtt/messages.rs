//! MQTT message types for the commonplace protocol.
//!
//! Defines the message formats for all four ports:
//! - Edits: Yjs updates with commit metadata
//! - Sync: Git-like protocol for history catch-up
//! - Events: Node broadcasts
//! - Commands: Commands to nodes

use serde::{Deserialize, Serialize};
use std::time::Duration;

// =============================================================================
// Structured Sync Errors
// =============================================================================

/// Structured error types for sync protocol.
///
/// These errors enable clients to programmatically recover from failures
/// rather than parsing error message strings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum SyncError {
    /// Parent commits are missing - client should push these commits first.
    MissingParents {
        /// List of parent commit IDs that the server doesn't have.
        parents: Vec<String>,
    },

    /// Computed CID doesn't match the claimed CID - indicates a bug.
    CidMismatch {
        /// The CID claimed by the client.
        expected: String,
        /// The CID computed by the server.
        computed: String,
    },

    /// Commit validation failed - malformed commit data.
    InvalidCommit {
        /// The field that failed validation.
        field: String,
        /// Description of the validation error.
        message: String,
    },

    /// Requested commits not found on server.
    NotFound {
        /// List of commit IDs that were not found.
        commits: Vec<String>,
    },

    /// No common ancestor found between client's have set and server's history.
    NoCommonAncestor {
        /// The commit IDs the client reported having.
        have: Vec<String>,
        /// The server's current HEAD commit.
        server_head: String,
    },
}

impl SyncError {
    /// Create a missing_parents error.
    pub fn missing_parents(parents: Vec<String>) -> Self {
        SyncError::MissingParents { parents }
    }

    /// Create a cid_mismatch error.
    pub fn cid_mismatch(expected: String, computed: String) -> Self {
        SyncError::CidMismatch { expected, computed }
    }

    /// Create an invalid_commit error.
    pub fn invalid_commit(field: impl Into<String>, message: impl Into<String>) -> Self {
        SyncError::InvalidCommit {
            field: field.into(),
            message: message.into(),
        }
    }

    /// Create a not_found error.
    pub fn not_found(commits: Vec<String>) -> Self {
        SyncError::NotFound { commits }
    }

    /// Create a no_common_ancestor error.
    pub fn no_common_ancestor(have: Vec<String>, server_head: String) -> Self {
        SyncError::NoCommonAncestor { have, server_head }
    }
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::MissingParents { parents } => {
                write!(f, "missing parents: {}", parents.join(", "))
            }
            SyncError::CidMismatch { expected, computed } => {
                write!(
                    f,
                    "CID mismatch: expected {}, computed {}",
                    expected, computed
                )
            }
            SyncError::InvalidCommit { field, message } => {
                write!(f, "invalid commit field '{}': {}", field, message)
            }
            SyncError::NotFound { commits } => {
                write!(f, "commits not found: {}", commits.join(", "))
            }
            SyncError::NoCommonAncestor { have, server_head } => {
                write!(
                    f,
                    "no common ancestor found (have: {}, server_head: {})",
                    have.join(", "),
                    server_head
                )
            }
        }
    }
}

// =============================================================================
// Peer Fallback Configuration
// =============================================================================

/// Default timeout waiting for primary (doc store) response before peers may respond.
pub const PRIMARY_TIMEOUT_MS: u64 = 100;

/// Maximum random jitter peers add before responding (to prevent thundering herd).
pub const PEER_JITTER_MAX_MS: u64 = 500;

/// Configuration for peer fallback behavior on sync requests.
///
/// When a client sends a sync request (Get, Pull, Ancestors), it normally expects
/// the doc store to respond. If the doc store is slow or unavailable, peers that
/// have the requested commits can respond instead after a timeout.
///
/// ## Protocol Flow
///
/// 1. Client publishes sync request with correlation ID
/// 2. Client and peers both subscribe to response topic
/// 3. Doc store has `primary_timeout` to respond
/// 4. If no response arrives, peers wait random jitter (0 to `peer_jitter_max`)
/// 5. First peer to respond publishes commits
/// 6. Other peers see the response and cancel their pending response
///
/// ## Duplicate Suppression
///
/// Peers watch the response topic for their correlation ID. Once any response
/// arrives (from doc store or another peer), they cancel pending responses.
#[derive(Debug, Clone, Copy)]
pub struct PeerFallbackConfig {
    /// Time to wait for primary (doc store) response before peers may respond.
    pub primary_timeout: Duration,
    /// Maximum random jitter peers add before responding.
    /// Actual delay is random between 0 and this value.
    pub peer_jitter_max: Duration,
    /// Whether peer fallback is enabled.
    pub enabled: bool,
}

impl Default for PeerFallbackConfig {
    fn default() -> Self {
        Self {
            primary_timeout: Duration::from_millis(PRIMARY_TIMEOUT_MS),
            peer_jitter_max: Duration::from_millis(PEER_JITTER_MAX_MS),
            enabled: true,
        }
    }
}

impl PeerFallbackConfig {
    /// Create a new config with custom timeouts.
    pub fn new(primary_timeout: Duration, peer_jitter_max: Duration) -> Self {
        Self {
            primary_timeout,
            peer_jitter_max,
            enabled: true,
        }
    }

    /// Create a disabled config (no peer fallback).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

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
    /// Optional correlation ID for request/response matching.
    /// When present, ack messages will include this ID to correlate with the original edit.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub req: Option<String>,
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

    /// Check if one commit is an ancestor of another
    /// { "type": "is_ancestor", "req": "...", "ancestor": "abc123", "descendant": "def456" }
    #[serde(rename = "is_ancestor")]
    IsAncestor {
        /// Request ID for correlation
        req: String,
        /// The potential ancestor commit ID
        ancestor: String,
        /// The potential descendant commit ID
        descendant: String,
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
        /// Source of this response (None = doc store, Some(client_id) = peer)
        /// Used for peer fallback protocol to identify who provided the data.
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },

    /// All requested commits have been sent
    Done {
        /// Request ID for correlation
        req: String,
        /// List of commit IDs that were sent
        commits: Vec<String>,
        /// Source of this response (None = doc store, Some(client_id) = peer)
        /// Used for peer fallback protocol to identify who provided the data.
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// Common ancestor found during pull (first commit in client's "have" set
        /// that exists in server's history). Only set for pull responses.
        #[serde(skip_serializing_if = "Option::is_none")]
        ancestor: Option<String>,
    },

    /// Response to IsAncestor request
    /// { "type": "is_ancestor_response", "req": "...", "result": true }
    #[serde(rename = "is_ancestor_response")]
    IsAncestorResponse {
        /// Request ID for correlation
        req: String,
        /// True if ancestor is an ancestor of descendant
        result: bool,
    },

    /// Error response
    Error {
        /// Request ID for correlation
        req: String,
        /// Human-readable error message (for backwards compatibility)
        message: String,
        /// Structured error for programmatic recovery (optional for backwards compat)
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<SyncError>,
    },

    // ========== Alerts (broadcast to notify peers of issues) ==========
    /// Alert that parent commits are missing.
    ///
    /// Published when a client receives a commit but doesn't have one or more
    /// of its parent commits. Peers can respond by publishing the missing
    /// commits or triggering a sync request.
    ///
    /// Topic pattern: `{workspace}/sync/{path}/missing`
    #[serde(rename = "missing_parent")]
    MissingParent {
        /// Request ID for correlation (optional - can be used to track recovery)
        req: String,
        /// The commit ID that has missing parents
        commit_id: String,
        /// List of parent commit IDs that are missing
        missing_parents: Vec<String>,
        /// Client ID of the sender (so responders know who to help)
        client_id: String,
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
            SyncMessage::IsAncestor { req, .. } => req,
            SyncMessage::Commit { req, .. } => req,
            SyncMessage::Done { req, .. } => req,
            SyncMessage::IsAncestorResponse { req, .. } => req,
            SyncMessage::Error { req, .. } => req,
            SyncMessage::MissingParent { req, .. } => req,
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
                | SyncMessage::IsAncestor { .. }
        )
    }

    /// Check if this is a response message (from doc store).
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            SyncMessage::HeadResponse { .. }
                | SyncMessage::Commit { .. }
                | SyncMessage::Done { .. }
                | SyncMessage::IsAncestorResponse { .. }
                | SyncMessage::Error { .. }
        )
    }

    /// Check if this is an alert message (broadcast to peers).
    pub fn is_alert(&self) -> bool {
        matches!(self, SyncMessage::MissingParent { .. })
    }

    /// Create an error response with both human-readable message and structured error.
    pub fn error(req: impl Into<String>, error: SyncError) -> Self {
        SyncMessage::Error {
            req: req.into(),
            message: error.to_string(),
            error: Some(error),
        }
    }

    /// Create an error response with only a human-readable message (for backwards compat).
    pub fn error_message(req: impl Into<String>, message: impl Into<String>) -> Self {
        SyncMessage::Error {
            req: req.into(),
            message: message.into(),
            error: None,
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
            req: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"update\":\"base64data\""));
        assert!(json.contains("\"author\":\"user@example.com\""));
        // req should be omitted when None
        assert!(!json.contains("\"req\""));
    }

    #[test]
    fn test_edit_message_with_req() {
        let msg = EditMessage {
            update: "base64data".to_string(),
            parents: vec!["abc123".to_string()],
            author: "user@example.com".to_string(),
            message: None,
            timestamp: 1704067200000,
            req: Some("correlation-123".to_string()),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"req\":\"correlation-123\""));
    }

    #[test]
    fn test_edit_message_deserialize_without_req() {
        // Old messages without req field should still parse
        let json =
            r#"{"update":"base64data","parents":["abc123"],"author":"user","timestamp":123456}"#;
        let msg: EditMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.update, "base64data");
        assert!(msg.req.is_none());
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
            source: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"commit\""));
        assert!(json.contains("\"id\":\"def456\""));
        // source should be omitted when None
        assert!(!json.contains("\"source\""));
    }

    #[test]
    fn test_sync_commit_response_with_source() {
        let msg = SyncMessage::Commit {
            req: "r-003".to_string(),
            id: "def456".to_string(),
            parents: vec!["abc123".to_string()],
            data: "base64update".to_string(),
            timestamp: 1704067200000,
            author: "user".to_string(),
            message: None,
            source: Some("peer-123".to_string()),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"source\":\"peer-123\""));
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
            commits: vec![],
            source: None,
            ancestor: None,
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

    // ========== SyncError Tests ==========

    #[test]
    fn test_sync_error_missing_parents() {
        let err = SyncError::missing_parents(vec!["abc123".to_string(), "def456".to_string()]);
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"missing_parents\""));
        assert!(json.contains("\"parents\":[\"abc123\",\"def456\"]"));

        // Round-trip test
        let parsed: SyncError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, parsed);
    }

    #[test]
    fn test_sync_error_cid_mismatch() {
        let err = SyncError::cid_mismatch("expected123".to_string(), "computed456".to_string());
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"cid_mismatch\""));
        assert!(json.contains("\"expected\":\"expected123\""));
        assert!(json.contains("\"computed\":\"computed456\""));

        // Round-trip test
        let parsed: SyncError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, parsed);
    }

    #[test]
    fn test_sync_error_invalid_commit() {
        let err = SyncError::invalid_commit("parents", "must be an array");
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"invalid_commit\""));
        assert!(json.contains("\"field\":\"parents\""));
        assert!(json.contains("\"message\":\"must be an array\""));

        // Round-trip test
        let parsed: SyncError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, parsed);
    }

    #[test]
    fn test_sync_error_not_found() {
        let err = SyncError::not_found(vec!["nonexistent1".to_string()]);
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"not_found\""));
        assert!(json.contains("\"commits\":[\"nonexistent1\"]"));

        // Round-trip test
        let parsed: SyncError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, parsed);
    }

    #[test]
    fn test_sync_error_no_common_ancestor() {
        let err = SyncError::no_common_ancestor(
            vec!["abc".to_string(), "def".to_string()],
            "xyz".to_string(),
        );
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"no_common_ancestor\""));
        assert!(json.contains("\"have\":[\"abc\",\"def\"]"));
        assert!(json.contains("\"server_head\":\"xyz\""));

        // Round-trip test
        let parsed: SyncError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, parsed);
    }

    #[test]
    fn test_sync_message_error_with_structured_error() {
        let err = SyncError::not_found(vec!["abc123".to_string()]);
        let msg = SyncMessage::error("r-001", err.clone());

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"message\":\"commits not found: abc123\""));
        assert!(json.contains("\"error\":{\"code\":\"not_found\""));

        // Verify it can be deserialized back
        let parsed: SyncMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            SyncMessage::Error {
                req,
                message,
                error,
            } => {
                assert_eq!(req, "r-001");
                assert_eq!(message, "commits not found: abc123");
                assert_eq!(error, Some(err));
            }
            _ => panic!("Expected Error message"),
        }
    }

    #[test]
    fn test_sync_message_error_without_structured_error() {
        // Test backwards compatibility with plain error messages
        let msg = SyncMessage::error_message("r-001", "Something went wrong");

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"req\":\"r-001\""));
        assert!(json.contains("\"message\":\"Something went wrong\""));
        // error field should be omitted when None
        assert!(!json.contains("\"error\":"));

        // Verify it can be deserialized back
        let parsed: SyncMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            SyncMessage::Error {
                req,
                message,
                error,
            } => {
                assert_eq!(req, "r-001");
                assert_eq!(message, "Something went wrong");
                assert!(error.is_none());
            }
            _ => panic!("Expected Error message"),
        }
    }

    #[test]
    fn test_sync_error_display() {
        // Test Display impl for all error types
        assert_eq!(
            SyncError::missing_parents(vec!["a".to_string(), "b".to_string()]).to_string(),
            "missing parents: a, b"
        );
        assert_eq!(
            SyncError::cid_mismatch("expected".to_string(), "computed".to_string()).to_string(),
            "CID mismatch: expected expected, computed computed"
        );
        assert_eq!(
            SyncError::invalid_commit("field", "msg").to_string(),
            "invalid commit field 'field': msg"
        );
        assert_eq!(
            SyncError::not_found(vec!["x".to_string()]).to_string(),
            "commits not found: x"
        );
        assert_eq!(
            SyncError::no_common_ancestor(vec!["a".to_string()], "z".to_string()).to_string(),
            "no common ancestor found (have: a, server_head: z)"
        );
    }
}
