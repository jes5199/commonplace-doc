//! Unified error types for sync operations.
//!
//! This module provides a standardized error type for the sync module,
//! replacing ad-hoc String errors and Box<dyn Error> with structured variants.

use std::io;
use thiserror::Error;

/// Unified error type for sync operations.
#[derive(Error, Debug)]
pub enum SyncError {
    /// IO error (file read/write, network)
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Base64 decoding error
    #[error("Base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    /// Yjs update decoding error
    #[error("Yjs decode error: {0}")]
    YjsDecode(String),

    /// CRDT state error (missing state, invalid state)
    #[error("CRDT state error: {0}")]
    CrdtState(String),

    /// Schema error (parse, validate, missing fields)
    #[error("Schema error: {0}")]
    Schema(String),

    /// HTTP client error
    #[error("HTTP error: {0}")]
    Http(String),

    /// MQTT publish/subscribe error
    #[error("MQTT error: {0}")]
    Mqtt(String),

    /// Content unchanged (not really an error, but used for control flow)
    #[error("Content unchanged")]
    ContentUnchanged,

    /// CRDT state is stale compared to local content
    #[error("CRDT state stale: {0}")]
    StaleCrdtState(String),

    /// Document not found
    #[error("Document not found: {0}")]
    NotFound(String),

    /// Invalid UUID
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),

    /// Generic sync error for cases not covered by specific variants
    #[error("{0}")]
    Other(String),
}

impl SyncError {
    /// Create a CRDT state error
    pub fn crdt_state(msg: impl Into<String>) -> Self {
        Self::CrdtState(msg.into())
    }

    /// Create a schema error
    pub fn schema(msg: impl Into<String>) -> Self {
        Self::Schema(msg.into())
    }

    /// Create an HTTP error
    pub fn http(msg: impl Into<String>) -> Self {
        Self::Http(msg.into())
    }

    /// Create an MQTT error
    pub fn mqtt(msg: impl Into<String>) -> Self {
        Self::Mqtt(msg.into())
    }

    /// Create a Yjs decode error
    pub fn yjs_decode(msg: impl Into<String>) -> Self {
        Self::YjsDecode(msg.into())
    }

    /// Create a stale CRDT state error
    pub fn stale_crdt_state(msg: impl Into<String>) -> Self {
        Self::StaleCrdtState(msg.into())
    }

    /// Create a generic error
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Result type alias for sync operations
pub type SyncResult<T> = Result<T, SyncError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_error_display() {
        let err = SyncError::ContentUnchanged;
        assert_eq!(err.to_string(), "Content unchanged");

        let err = SyncError::yjs_decode("bad update");
        assert_eq!(err.to_string(), "Yjs decode error: bad update");
    }

    #[test]
    fn test_sync_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let sync_err: SyncError = io_err.into();
        assert!(matches!(sync_err, SyncError::Io(_)));
    }

    #[test]
    fn test_sync_error_from_uuid() {
        let uuid_result = uuid::Uuid::parse_str("invalid");
        assert!(uuid_result.is_err());
        let sync_err: SyncError = uuid_result.unwrap_err().into();
        assert!(matches!(sync_err, SyncError::InvalidUuid(_)));
    }
}
