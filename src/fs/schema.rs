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
/// **All directories must be node-backed** (have a `node_id` pointing to another JSON document).
///
/// The `entries` field is only used for the **root directory** of a schema document
/// to define its immediate children. Non-root directories (subdirectories) must
/// always have `entries: None` and a `node_id` pointing to their own schema document.
///
/// **DEPRECATED**: Inline subdirectories (non-root directories with `entries` but no `node_id`)
/// are no longer supported and will be rejected during schema validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    /// Child entries - only valid for the root directory of a schema document.
    /// Subdirectories must have `entries: None` and use `node_id` instead.
    #[serde(default)]
    pub entries: Option<HashMap<String, Entry>>,
    /// Node ID pointing to this directory's schema document.
    /// Required for all subdirectories (non-root directories).
    #[serde(default)]
    pub node_id: Option<String>,
    /// Content type for node-backed directories (default: application/json)
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

impl FsSchema {
    /// Check if a path exists in the schema.
    ///
    /// Returns `true` if the path is present in the schema's entries.
    /// For nested paths (e.g., "subdir/file.txt"), this only checks inline
    /// entries. Node-backed directories return `false` because their contents
    /// are defined in separate schema documents.
    ///
    /// # Examples
    /// ```
    /// use commonplace_doc::fs::FsSchema;
    /// let json = r#"{"version": 1, "root": {"type": "dir", "entries": {"file.txt": {"type": "doc"}}}}"#;
    /// let schema: FsSchema = serde_json::from_str(json).unwrap();
    /// assert!(schema.has_path("file.txt"));
    /// assert!(!schema.has_path("missing.txt"));
    /// ```
    pub fn has_path(&self, path: &str) -> bool {
        let Some(Entry::Dir(ref root)) = self.root else {
            return false;
        };

        let Some(ref entries) = root.entries else {
            return false;
        };

        // Handle simple single-level paths
        if !path.contains('/') {
            return entries.contains_key(path);
        }

        // Handle nested paths (e.g., "subdir/file.txt")
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() != 2 {
            return entries.contains_key(path);
        }

        let (first, rest) = (parts[0], parts[1]);
        if let Some(Entry::Dir(subdir)) = entries.get(first) {
            // For node-backed directories, the files are defined in a separate schema
            // document, not in the root schema. Return false here because we can't
            // determine from this schema alone whether the nested path exists.
            if subdir.node_id.is_some() {
                return false;
            }
            // Inline directory entries (deprecated but may still exist)
            if let Some(ref sub_entries) = subdir.entries {
                return sub_entries.contains_key(rest);
            }
        }

        false
    }

    /// Validate the schema structure.
    ///
    /// This checks:
    /// - Version is supported (currently only version 1)
    /// - Root entry (if present) is a directory
    /// - No inline subdirectories exist (all subdirectories must be node-backed)
    /// - All entry names are valid
    pub fn validate(&self) -> Result<(), FsError> {
        if self.version != 1 {
            return Err(FsError::SchemaError(format!(
                "Unsupported schema version: {} (only version 1 is supported)",
                self.version
            )));
        }

        if let Some(ref root) = self.root {
            match root {
                Entry::Dir(dir) => {
                    // Root directory can have entries - validate its children
                    if let Some(ref entries) = dir.entries {
                        for (name, entry) in entries {
                            Entry::validate_name(name)?;
                            Self::validate_entry(entry, name)?;
                        }
                    }
                }
                Entry::Doc(_) => {
                    return Err(FsError::SchemaError(
                        "Root entry must be a directory".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validate a non-root entry recursively.
    ///
    /// Subdirectories must be node-backed (have `node_id`, not `entries`).
    fn validate_entry(entry: &Entry, path: &str) -> Result<(), FsError> {
        match entry {
            Entry::Doc(_) => Ok(()),
            Entry::Dir(dir) => {
                // Check for inline subdirectory (entries without node_id)
                if dir.entries.is_some() && dir.node_id.is_none() {
                    return Err(FsError::SchemaError(format!(
                        "Inline subdirectory '{}' is not supported. All subdirectories must be node-backed (have node_id).",
                        path
                    )));
                }

                // Check for invalid combination (both entries and node_id)
                if dir.entries.is_some() && dir.node_id.is_some() {
                    return Err(FsError::SchemaError(format!(
                        "Directory '{}' has both node_id and entries (mutually exclusive)",
                        path
                    )));
                }

                Ok(())
            }
        }
    }
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
    fn test_has_path_simple_file() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file.txt": { "type": "doc" },
                    "other.json": { "type": "doc" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(schema.has_path("file.txt"));
        assert!(schema.has_path("other.json"));
        assert!(!schema.has_path("missing.txt"));
    }

    #[test]
    fn test_has_path_no_root() {
        let schema = FsSchema {
            version: 1,
            root: None,
        };
        assert!(!schema.has_path("anything"));
    }

    #[test]
    fn test_has_path_empty_entries() {
        let json = r#"{
            "version": 1,
            "root": { "type": "dir" }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(!schema.has_path("file.txt"));
    }

    #[test]
    fn test_has_path_node_backed_subdir_returns_false() {
        // Node-backed directories have their contents in a separate schema,
        // so has_path should return false for nested paths
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "subdir": {
                        "type": "dir",
                        "node_id": "some-uuid"
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(schema.has_path("subdir")); // The directory itself exists
        assert!(!schema.has_path("subdir/file.txt")); // But nested paths return false
    }

    #[test]
    fn test_has_path_directory_entry() {
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "mydir": {
                        "type": "dir",
                        "node_id": "uuid-123"
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(schema.has_path("mydir"));
        assert!(!schema.has_path("otherdir"));
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

    #[test]
    fn test_validate_schema_version() {
        let schema = FsSchema {
            version: 2,
            root: None,
        };
        let result = schema.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("version"));
    }

    #[test]
    fn test_validate_schema_root_must_be_dir() {
        let json = r#"{
            "version": 1,
            "root": { "type": "doc" }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        let result = schema.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("directory"));
    }

    #[test]
    fn test_validate_inline_subdirectory_rejected() {
        // Inline subdirectory (entries but no node_id) should be rejected
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "subdir": {
                        "type": "dir",
                        "entries": {
                            "file.txt": { "type": "doc" }
                        }
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        let result = schema.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Inline subdirectory"),
            "Expected inline subdirectory error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_node_backed_subdirectory_accepted() {
        // Node-backed subdirectory (node_id, no entries) should be accepted
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "subdir": {
                        "type": "dir",
                        "node_id": "some-uuid"
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn test_validate_both_entries_and_node_id_rejected() {
        // Directory with both entries and node_id should be rejected
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "subdir": {
                        "type": "dir",
                        "node_id": "some-uuid",
                        "entries": {
                            "file.txt": { "type": "doc" }
                        }
                    }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        let result = schema.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("mutually exclusive"),
            "Expected mutually exclusive error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_root_with_entries_accepted() {
        // Root directory can have entries - this is normal
        let json = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file.txt": { "type": "doc", "node_id": "uuid-1" }
                }
            }
        }"#;
        let schema: FsSchema = serde_json::from_str(json).unwrap();
        assert!(schema.validate().is_ok());
    }
}
