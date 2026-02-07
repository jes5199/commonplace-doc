//! Error types for filesystem operations.

use std::fmt;

/// Errors that can occur during filesystem operations.
#[derive(Debug, Clone)]
pub enum FsError {
    /// JSON parse failed
    ParseError(String),
    /// Schema validation failed
    SchemaError(String),
    /// Invalid entry name (contains /, is . or ..)
    InvalidEntryName(String),
    /// Node operation failed
    NodeError(String),
    /// Version mismatch
    UnsupportedVersion(u32),
}

impl fmt::Display for FsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FsError::ParseError(msg) => write!(f, "JSON parse error: {}", msg),
            FsError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            FsError::InvalidEntryName(name) => write!(f, "Invalid entry name: {}", name),
            FsError::NodeError(msg) => write!(f, "Node error: {}", msg),
            FsError::UnsupportedVersion(v) => write!(f, "Unsupported schema version: {}", v),
        }
    }
}

impl std::error::Error for FsError {}

impl FsError {
    /// Convert to JSON payload for fs.error event.
    pub fn to_event_payload(&self, path: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "message": self.to_string(),
            "path": path.unwrap_or("/"),
        })
    }
}
