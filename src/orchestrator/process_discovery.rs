//! Process discovery service for finding __processes.json files.
//!
//! This module provides a service that traverses the filesystem schema tree
//! to discover all `__processes.json` files that define orchestrated processes.

use super::schema_visitor::{SchemaVisitor, VisitorError};
use crate::fs::Entry;
use crate::mqtt::MqttRequestClient;
use std::fmt;
use std::sync::Arc;

/// Error type for process discovery operations.
#[derive(Debug)]
pub enum DiscoveryError {
    /// Schema visitor error (request, parsing, etc.)
    VisitorError(VisitorError),
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscoveryError::VisitorError(e) => write!(f, "Discovery error: {}", e),
        }
    }
}

impl std::error::Error for DiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DiscoveryError::VisitorError(e) => Some(e),
        }
    }
}

impl From<VisitorError> for DiscoveryError {
    fn from(e: VisitorError) -> Self {
        DiscoveryError::VisitorError(e)
    }
}

/// Result of running process discovery.
///
/// Contains both the discovered `__processes.json` files and all schema
/// node_ids encountered during traversal (useful for SSE watching).
#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    /// List of (document_path, node_id) for each __processes.json found.
    /// The document_path is the directory containing the file (e.g., "/" or "/subdir").
    pub processes_json_files: Vec<(String, String)>,
    /// All schema node_ids encountered during traversal (for SSE watching).
    pub schema_node_ids: Vec<String>,
}

/// Service for discovering __processes.json files in the filesystem schema tree.
///
/// Uses `SchemaVisitor` to traverse the tree and find all process configuration files.
pub struct ProcessDiscoveryService {
    visitor: SchemaVisitor,
}

impl ProcessDiscoveryService {
    /// Create a new ProcessDiscoveryService.
    ///
    /// # Arguments
    /// * `request_client` - MQTT request client for fetching documents
    pub fn new(request_client: Arc<MqttRequestClient>) -> Self {
        Self {
            visitor: SchemaVisitor::new(request_client),
        }
    }

    /// Discover all __processes.json files starting from the filesystem root.
    ///
    /// This traverses the entire schema tree, collecting:
    /// - All `__processes.json` file entries with their paths and node_ids
    /// - All schema node_ids encountered (for watching directory changes)
    ///
    /// # Arguments
    /// * `fs_root_id` - The node_id of the filesystem root schema
    ///
    /// # Returns
    /// A `DiscoveryResult` containing the discovered files and schema node_ids.
    pub async fn discover_all(&self, fs_root_id: &str) -> Result<DiscoveryResult, DiscoveryError> {
        let mut processes_json_files = Vec::new();

        // Find all __processes.json entries using the visitor
        let entries = self
            .visitor
            .find_entries(fs_root_id, |_path, name, entry| {
                name == "__processes.json" && matches!(entry, Entry::Doc(_))
            })
            .await?;

        // Convert found entries to (path, node_id) pairs
        for (path, _name, node_id) in entries {
            if let Some(id) = node_id {
                processes_json_files.push((path, id));
            }
        }

        // Collect all schema node_ids for watching
        let schema_node_ids = self.visitor.collect_schema_ids(fs_root_id).await?;

        Ok(DiscoveryResult {
            processes_json_files,
            schema_node_ids,
        })
    }

    /// Get a reference to the underlying SchemaVisitor.
    ///
    /// Useful when additional schema operations are needed beyond discovery.
    pub fn visitor(&self) -> &SchemaVisitor {
        &self.visitor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_error_display() {
        let visitor_err = VisitorError::RequestError("connection refused".to_string());
        let discovery_err = DiscoveryError::from(visitor_err);
        assert!(discovery_err.to_string().contains("Discovery error"));
        assert!(discovery_err.to_string().contains("Request error"));
    }

    #[test]
    fn test_discovery_result_clone() {
        let result = DiscoveryResult {
            processes_json_files: vec![("/".to_string(), "uuid-1".to_string())],
            schema_node_ids: vec!["root-uuid".to_string()],
        };
        let cloned = result.clone();
        assert_eq!(cloned.processes_json_files.len(), 1);
        assert_eq!(cloned.schema_node_ids.len(), 1);
    }

    #[test]
    fn test_discovery_result_empty() {
        let result = DiscoveryResult {
            processes_json_files: Vec::new(),
            schema_node_ids: Vec::new(),
        };

        assert!(result.processes_json_files.is_empty());
        assert!(result.schema_node_ids.is_empty());

        let cloned = result.clone();
        assert!(cloned.processes_json_files.is_empty());
        assert!(cloned.schema_node_ids.is_empty());
    }

    #[test]
    fn test_discovery_result_multiple_processes_files() {
        let result = DiscoveryResult {
            processes_json_files: vec![
                ("/".to_string(), "uuid-root".to_string()),
                ("/subdir".to_string(), "uuid-subdir".to_string()),
                ("/subdir/nested".to_string(), "uuid-nested".to_string()),
            ],
            schema_node_ids: vec![
                "schema-root".to_string(),
                "schema-subdir".to_string(),
                "schema-nested".to_string(),
            ],
        };

        assert_eq!(result.processes_json_files.len(), 3);
        assert_eq!(result.schema_node_ids.len(), 3);

        assert!(result
            .processes_json_files
            .iter()
            .any(|(path, _)| path == "/"));
        assert!(result
            .processes_json_files
            .iter()
            .any(|(path, _)| path == "/subdir"));
        assert!(result
            .processes_json_files
            .iter()
            .any(|(path, _)| path == "/subdir/nested"));
    }

    #[test]
    fn test_discovery_error_from_visitor_error() {
        let req_err = VisitorError::RequestError("timeout".to_string());
        let discovery_err: DiscoveryError = req_err.into();

        assert!(discovery_err.to_string().contains("timeout"));
        assert!(discovery_err.to_string().contains("Request error"));
    }

    #[test]
    fn test_discovery_error_from_parse_error() {
        let parse_err = VisitorError::ParseError("invalid schema JSON".to_string());
        let discovery_err: DiscoveryError = parse_err.into();

        assert!(discovery_err.to_string().contains("invalid schema JSON"));
    }

    #[test]
    fn test_discovery_error_from_schema_error() {
        let schema_err = VisitorError::SchemaError("missing root".to_string());
        let discovery_err: DiscoveryError = schema_err.into();

        assert!(discovery_err.to_string().contains("missing root"));
    }

    #[test]
    fn test_discovery_error_source() {
        let visitor_err = VisitorError::RequestError("source error".to_string());
        let discovery_err = DiscoveryError::VisitorError(visitor_err);

        let source = std::error::Error::source(&discovery_err);
        assert!(source.is_some());
        assert!(source.unwrap().to_string().contains("source error"));
    }

    #[test]
    fn test_discovery_result_debug() {
        let result = DiscoveryResult {
            processes_json_files: vec![("/test".to_string(), "uuid-123".to_string())],
            schema_node_ids: vec!["schema-456".to_string()],
        };

        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("DiscoveryResult"));
        assert!(debug_str.contains("uuid-123"));
        assert!(debug_str.contains("schema-456"));
    }

    #[test]
    fn test_discovery_error_debug() {
        let err = DiscoveryError::VisitorError(VisitorError::RequestError("test".to_string()));

        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("VisitorError"));
        assert!(debug_str.contains("RequestError"));
    }
}
