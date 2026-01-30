//! Schema visitor for traversing the filesystem schema tree.
//!
//! This module encapsulates the logic for fetching schemas from the server
//! and traversing the directory structure to find entries of interest.

use crate::fs::{DirEntry, Entry, FsSchema};
use crate::mqtt::MqttRequestClient;
use std::fmt;
use std::sync::Arc;

/// Error type for schema visitor operations.
#[derive(Debug)]
pub enum VisitorError {
    /// MQTT request failed
    RequestError(String),
    /// Failed to parse response
    ParseError(String),
    /// Schema validation failed
    SchemaError(String),
}

impl fmt::Display for VisitorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VisitorError::RequestError(msg) => write!(f, "Request error: {}", msg),
            VisitorError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            VisitorError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
        }
    }
}

impl std::error::Error for VisitorError {}

/// Visit all entries in a schema's root directory.
///
/// Calls the visitor function for each entry in the schema's root.
/// Does NOT recurse into subdirectories — use `SchemaVisitor::visit_tree` for that.
///
/// This is a free function since it doesn't require any client state.
pub fn visit_entries<F>(schema: &FsSchema, mut visitor: F)
where
    F: FnMut(&str, &Entry),
{
    if let Some(Entry::Dir(ref root)) = schema.root {
        if let Some(ref entries) = root.entries {
            for (name, entry) in entries {
                visitor(name, entry);
            }
        }
    }
}

/// Visitor for traversing filesystem schemas.
///
/// This struct provides methods for fetching schemas from the server
/// and recursively visiting all entries in the schema tree.
pub struct SchemaVisitor {
    request_client: Arc<MqttRequestClient>,
}

impl SchemaVisitor {
    /// Create a new SchemaVisitor.
    ///
    /// # Arguments
    /// * `request_client` - MQTT request client for fetching documents
    pub fn new(request_client: Arc<MqttRequestClient>) -> Self {
        Self { request_client }
    }

    /// Fetch and parse a schema from the server.
    ///
    /// # Arguments
    /// * `node_id` - The UUID of the schema document to fetch
    ///
    /// # Returns
    /// The parsed FsSchema, or an error if the fetch or parse failed.
    pub async fn fetch_schema(&self, node_id: &str) -> Result<FsSchema, VisitorError> {
        let response = self
            .request_client
            .get_content(node_id)
            .await
            .map_err(|e| {
                VisitorError::RequestError(format!("MQTT request failed for {}: {}", node_id, e))
            })?;

        if let Some(error) = response.error {
            return Err(VisitorError::RequestError(format!(
                "get-content error for {}: {}",
                node_id, error
            )));
        }

        let content = response.content.ok_or_else(|| {
            VisitorError::RequestError(format!("No content in response for {}", node_id))
        })?;

        let schema: FsSchema = serde_json::from_str(&content)
            .map_err(|e| VisitorError::ParseError(format!("Failed to parse schema: {}", e)))?;

        Ok(schema)
    }

    /// Recursively collect all schema node_ids in the tree.
    ///
    /// Starting from a root schema, this traverses all node-backed
    /// subdirectories and collects every schema node_id encountered.
    ///
    /// # Arguments
    /// * `root_id` - The node_id of the root schema to start from
    ///
    /// # Returns
    /// A vector of all schema node_ids in the tree (including the root).
    pub async fn collect_schema_ids(&self, root_id: &str) -> Result<Vec<String>, VisitorError> {
        let mut schema_ids = Vec::new();
        let mut to_visit = vec![root_id.to_string()];

        while let Some(node_id) = to_visit.pop() {
            schema_ids.push(node_id.clone());

            let schema = self.fetch_schema(&node_id).await?;

            if let Some(Entry::Dir(ref root)) = schema.root {
                if let Some(ref entries) = root.entries {
                    for entry in entries.values() {
                        if let Entry::Dir(DirEntry {
                            node_id: Some(sub_id),
                            ..
                        }) = entry
                        {
                            to_visit.push(sub_id.clone());
                        }
                    }
                }
            }
        }

        Ok(schema_ids)
    }

    /// Recursively visit all entries in the schema tree.
    ///
    /// Starting from a root schema, this traverses all directories (including
    /// node-backed subdirectories) and calls the visitor for each entry found.
    ///
    /// # Arguments
    /// * `root_id` - The node_id of the root schema to start from
    /// * `visitor` - Callback invoked with (path, entry_name, entry, schema_node_id)
    ///
    /// The path is the directory path containing the entry (e.g., "/" for root,
    /// "/subdir" for a subdirectory). The schema_node_id is the UUID of the
    /// schema document containing this entry.
    pub async fn visit_tree<F>(&self, root_id: &str, mut visitor: F) -> Result<(), VisitorError>
    where
        F: FnMut(&str, &str, &Entry, &str),
    {
        let mut to_visit: Vec<(String, String)> = vec![("/".to_string(), root_id.to_string())];

        while let Some((path, node_id)) = to_visit.pop() {
            let schema = self.fetch_schema(&node_id).await?;

            if let Some(Entry::Dir(ref root)) = schema.root {
                if let Some(ref entries) = root.entries {
                    for (name, entry) in entries {
                        visitor(&path, name, entry, &node_id);

                        // Queue subdirectories with node_ids for recursion
                        if let Entry::Dir(DirEntry {
                            node_id: Some(sub_id),
                            ..
                        }) = entry
                        {
                            let sub_path = if path == "/" {
                                format!("/{}", name)
                            } else {
                                format!("{}/{}", path, name)
                            };
                            to_visit.push((sub_path, sub_id.clone()));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Find all entries matching a predicate in the schema tree.
    ///
    /// This is a convenience wrapper around `visit_tree` that collects
    /// matching entries into a vector.
    ///
    /// # Arguments
    /// * `root_id` - The node_id of the root schema to start from
    /// * `predicate` - Function that returns true for entries to collect
    ///
    /// # Returns
    /// A vector of (path, entry_name, node_id) tuples for matching entries.
    /// The node_id is from the entry itself (for docs/node-backed dirs).
    pub async fn find_entries<F>(
        &self,
        root_id: &str,
        mut predicate: F,
    ) -> Result<Vec<(String, String, Option<String>)>, VisitorError>
    where
        F: FnMut(&str, &str, &Entry) -> bool,
    {
        let mut results = Vec::new();
        let mut to_visit: Vec<(String, String)> = vec![("/".to_string(), root_id.to_string())];

        while let Some((path, schema_node_id)) = to_visit.pop() {
            let schema = self.fetch_schema(&schema_node_id).await?;

            if let Some(Entry::Dir(ref root)) = schema.root {
                if let Some(ref entries) = root.entries {
                    for (name, entry) in entries {
                        if predicate(&path, name, entry) {
                            let entry_node_id = match entry {
                                Entry::Doc(doc) => doc.node_id.clone(),
                                Entry::Dir(dir) => dir.node_id.clone(),
                            };
                            results.push((path.clone(), name.clone(), entry_node_id));
                        }

                        // Queue subdirectories with node_ids for recursion
                        if let Entry::Dir(DirEntry {
                            node_id: Some(sub_id),
                            ..
                        }) = entry
                        {
                            let sub_path = if path == "/" {
                                format!("/{}", name)
                            } else {
                                format!("{}/{}", path, name)
                            };
                            to_visit.push((sub_path, sub_id.clone()));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visit_entries_empty_schema() {
        let schema = FsSchema {
            version: 1,
            root: None,
        };

        let mut visited = Vec::new();
        visit_entries(&schema, |name, _entry| {
            visited.push(name.to_string());
        });

        assert!(visited.is_empty());
    }

    #[test]
    fn test_visit_entries_with_files() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file1.txt": { "type": "doc", "node_id": "uuid-1" },
                    "file2.txt": { "type": "doc", "node_id": "uuid-2" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut visited = Vec::new();
        visit_entries(&schema, |name, entry| {
            if let Entry::Doc(doc) = entry {
                visited.push((name.to_string(), doc.node_id.clone()));
            }
        });

        visited.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(visited.len(), 2);
        assert_eq!(
            visited[0],
            ("file1.txt".to_string(), Some("uuid-1".to_string()))
        );
        assert_eq!(
            visited[1],
            ("file2.txt".to_string(), Some("uuid-2".to_string()))
        );
    }

    #[test]
    fn test_visitor_error_display() {
        let req_err = VisitorError::RequestError("connection refused".to_string());
        assert!(req_err.to_string().contains("Request error"));
        assert!(req_err.to_string().contains("connection refused"));

        let parse_err = VisitorError::ParseError("invalid JSON".to_string());
        assert!(parse_err.to_string().contains("Parse error"));

        let schema_err = VisitorError::SchemaError("missing root".to_string());
        assert!(schema_err.to_string().contains("Schema error"));
    }

    #[test]
    fn test_visit_entries_with_nested_directories() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file1.txt": { "type": "doc", "node_id": "uuid-1" },
                    "subdir": { "type": "dir", "node_id": "subdir-uuid" },
                    "file2.txt": { "type": "doc", "node_id": "uuid-2" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut docs = Vec::new();
        let mut dirs = Vec::new();
        visit_entries(&schema, |name, entry| match entry {
            Entry::Doc(_) => docs.push(name.to_string()),
            Entry::Dir(_) => dirs.push(name.to_string()),
        });

        docs.sort();
        dirs.sort();
        assert_eq!(docs, vec!["file1.txt", "file2.txt"]);
        assert_eq!(dirs, vec!["subdir"]);
    }

    #[test]
    fn test_visit_entries_with_root_but_no_entries() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir"
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut count = 0;
        visit_entries(&schema, |_name, _entry| {
            count += 1;
        });

        assert_eq!(count, 0);
    }

    #[test]
    fn test_visit_entries_with_doc_as_root() {
        // This is an invalid schema (root should be dir), but visit_entries handles it gracefully
        let json = r#"{
            "version": 1,
            "root": {
                "type": "doc",
                "node_id": "some-id"
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut count = 0;
        visit_entries(&schema, |_name, _entry| {
            count += 1;
        });

        // Should not panic, just returns without visiting anything
        assert_eq!(count, 0);
    }

    #[test]
    fn test_visit_entries_extracts_node_ids_from_dirs() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "project-a": { "type": "dir", "node_id": "uuid-project-a" },
                    "project-b": { "type": "dir", "node_id": "uuid-project-b" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut dir_node_ids: Vec<(String, Option<String>)> = Vec::new();
        visit_entries(&schema, |name, entry| {
            if let Entry::Dir(dir) = entry {
                dir_node_ids.push((name.to_string(), dir.node_id.clone()));
            }
        });

        dir_node_ids.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(dir_node_ids.len(), 2);
        assert_eq!(
            dir_node_ids[0],
            ("project-a".to_string(), Some("uuid-project-a".to_string()))
        );
        assert_eq!(
            dir_node_ids[1],
            ("project-b".to_string(), Some("uuid-project-b".to_string()))
        );
    }

    #[test]
    fn test_visit_entries_mixed_content_types() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "config.json": { "type": "doc", "node_id": "uuid-1", "content_type": "application/json" },
                    "readme.txt": { "type": "doc", "node_id": "uuid-2", "content_type": "text/plain" },
                    "data": { "type": "dir", "node_id": "uuid-3", "content_type": "application/json" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut entries_found = Vec::new();
        visit_entries(&schema, |name, _entry| {
            entries_found.push(name.to_string());
        });

        entries_found.sort();
        assert_eq!(entries_found, vec!["config.json", "data", "readme.txt"]);
    }

    #[test]
    fn test_visitor_error_is_std_error() {
        // Verify VisitorError implements std::error::Error
        let err = VisitorError::RequestError("test".to_string());
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_visit_entries_with_special_characters_in_names() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "__processes.json": { "type": "doc", "node_id": "uuid-1" },
                    ".commonplace.json": { "type": "doc", "node_id": "uuid-2" },
                    "file-with-dash.txt": { "type": "doc", "node_id": "uuid-3" },
                    "file_with_underscore.txt": { "type": "doc", "node_id": "uuid-4" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        let mut found_processes_json = false;
        let mut found_commonplace_json = false;
        visit_entries(&schema, |name, _entry| {
            if name == "__processes.json" {
                found_processes_json = true;
            }
            if name == ".commonplace.json" {
                found_commonplace_json = true;
            }
        });

        assert!(found_processes_json);
        assert!(found_commonplace_json);
    }
}
