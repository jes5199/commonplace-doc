//! Schema types for filesystem JSON documents.

use super::FsError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root schema for filesystem JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsSchema {
    /// Schema version (currently only version 1 is supported)
    pub version: u32,
    /// Root directory entry
    #[serde(default)]
    pub root: Option<Entry>,
}

/// An entry in the filesystem (dir or doc).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Entry {
    Dir(DirEntry),
    Doc(DocEntry),
}

/// A directory entry.
///
/// Directories can be either:
/// - **Inline**: has `entries` map containing child entries
/// - **Node-backed**: has `node_id` pointing to another JSON document
///
/// These two forms are mutually exclusive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    /// Inline entries (mutually exclusive with node_id)
    #[serde(default)]
    pub entries: Option<HashMap<String, Entry>>,
    /// Node-backed directory reference (mutually exclusive with entries)
    #[serde(default)]
    pub node_id: Option<String>,
    /// Content type for node-backed directories
    #[serde(default)]
    pub content_type: Option<String>,
}

/// A document entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocEntry {
    /// Explicit node ID (stable across renames)
    #[serde(default)]
    pub node_id: Option<String>,
    /// MIME type for new nodes (default: application/json)
    #[serde(default)]
    pub content_type: Option<String>,
}

impl Entry {
    /// Validate entry name rules: no `/`, not `.` or `..`.
    pub fn validate_name(name: &str) -> Result<(), FsError> {
        if name.contains('/') {
            return Err(FsError::InvalidEntryName(format!(
                "'{}' contains '/'",
                name
            )));
        }
        if name == "." || name == ".." {
            return Err(FsError::InvalidEntryName(format!("'{}' is reserved", name)));
        }
        if name.is_empty() {
            return Err(FsError::InvalidEntryName("empty name".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_schema() {
        let json = r#"{"version": 1}"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.version, 1);
        assert!(schema.root.is_none());
    }

    #[test]
    fn test_parse_with_entries() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "notes": {
                        "type": "dir",
                        "entries": {
                            "ideas.txt": { "type": "doc" }
                        }
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(schema.root.is_some());

        if let Some(Entry::Dir(dir)) = &schema.root {
            assert!(dir.entries.is_some());
            let entries = dir.entries.as_ref().unwrap();
            assert!(entries.contains_key("notes"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    #[test]
    fn test_parse_doc_with_explicit_id() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "stable.txt": {
                        "type": "doc",
                        "node_id": "my-stable-id"
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        if let Some(Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();
            if let Some(Entry::Doc(doc)) = entries.get("stable.txt") {
                assert_eq!(doc.node_id.as_deref(), Some("my-stable-id"));
            } else {
                panic!("Expected Doc entry");
            }
        }
    }

    #[test]
    fn test_parse_node_backed_dir() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "external": {
                        "type": "dir",
                        "node_id": "external-fs-node"
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();
            if let Some(Entry::Dir(external)) = entries.get("external") {
                assert_eq!(external.node_id.as_deref(), Some("external-fs-node"));
                assert!(external.entries.is_none());
            } else {
                panic!("Expected Dir entry");
            }
        }
    }

    #[test]
    fn test_validate_entry_name() {
        assert!(Entry::validate_name("valid").is_ok());
        assert!(Entry::validate_name("with-dash").is_ok());
        assert!(Entry::validate_name("file.txt").is_ok());
        assert!(Entry::validate_name(".hidden").is_ok());

        assert!(Entry::validate_name(".").is_err());
        assert!(Entry::validate_name("..").is_err());
        assert!(Entry::validate_name("with/slash").is_err());
        assert!(Entry::validate_name("").is_err());
    }
}
