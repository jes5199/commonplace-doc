//! Router error types.

use serde_json::{json, Value};
use std::fmt;

/// Errors that can occur during router operations.
#[derive(Debug, Clone)]
pub enum RouterError {
    /// JSON parsing error
    ParseError(String),
    /// Unsupported schema version
    UnsupportedVersion(u32),
    /// Schema validation error
    SchemaError(String),
    /// Missing node referenced in edges
    MissingNode(String),
    /// Invalid edge specification
    InvalidEdge(String),
    /// Wiring would create a cycle
    CycleDetected(String, String),
    /// Node registry error
    NodeError(String),
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::UnsupportedVersion(v) => write!(f, "Unsupported version: {}", v),
            Self::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            Self::MissingNode(id) => write!(f, "Missing node: {}", id),
            Self::InvalidEdge(msg) => write!(f, "Invalid edge: {}", msg),
            Self::CycleDetected(from, to) => write!(f, "Cycle detected: {} -> {}", from, to),
            Self::NodeError(msg) => write!(f, "Node error: {}", msg),
        }
    }
}

impl std::error::Error for RouterError {}

impl RouterError {
    /// Convert to an event payload for router.error events.
    pub fn to_event_payload(&self) -> Value {
        match self {
            Self::ParseError(msg) => json!({
                "type": "parse_error",
                "message": msg
            }),
            Self::UnsupportedVersion(v) => json!({
                "type": "unsupported_version",
                "version": v
            }),
            Self::SchemaError(msg) => json!({
                "type": "schema_error",
                "message": msg
            }),
            Self::MissingNode(id) => json!({
                "type": "missing_node",
                "node_id": id
            }),
            Self::InvalidEdge(msg) => json!({
                "type": "invalid_edge",
                "message": msg
            }),
            Self::CycleDetected(from, to) => json!({
                "type": "cycle_detected",
                "from": from,
                "to": to
            }),
            Self::NodeError(msg) => json!({
                "type": "node_error",
                "message": msg
            }),
        }
    }
}
