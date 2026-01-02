//! Directory scanning and FS JSON generation.

use crate::fs::{DirEntry, DocEntry, Entry, FsSchema};
use crate::sync::content_type::{detect_from_path, is_allowed_extension, is_binary_content};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use thiserror::Error;

/// Schema filename for preserving node_ids
const SCHEMA_FILENAME: &str = ".commonplace.json";

/// Extract node_ids from an existing schema, building a path -> node_id map.
fn extract_node_ids(entry: &Entry, prefix: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();

    match entry {
        Entry::Doc(doc) => {
            if let Some(ref node_id) = doc.node_id {
                result.insert(prefix.to_string(), node_id.clone());
            }
        }
        Entry::Dir(dir) => {
            // Also capture directory node_ids
            if let Some(ref node_id) = dir.node_id {
                result.insert(prefix.to_string(), node_id.clone());
            }
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    result.extend(extract_node_ids(child, &child_path));
                }
            }
        }
    }

    result
}

/// Load existing node_ids from .commonplace.json if present.
fn load_existing_node_ids(directory: &Path) -> HashMap<String, String> {
    let schema_path = directory.join(SCHEMA_FILENAME);
    if !schema_path.exists() {
        return HashMap::new();
    }

    match fs::read_to_string(&schema_path) {
        Ok(content) => match serde_json::from_str::<FsSchema>(&content) {
            Ok(schema) => {
                if let Some(ref root) = schema.root {
                    extract_node_ids(root, "")
                } else {
                    HashMap::new()
                }
            }
            Err(_) => HashMap::new(),
        },
        Err(_) => HashMap::new(),
    }
}

/// Normalize a path to use forward slashes regardless of OS.
///
/// Schema paths always use forward slashes, so relative paths must be
/// normalized for consistency across platforms.
fn normalize_path(path: &str) -> String {
    path.replace('\\', "/")
}

fn json_content_type_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    match serde_json::from_slice::<Value>(bytes).ok()? {
        Value::Array(_) => Some("application/json;root=array"),
        Value::Object(_) => Some("application/json"),
        _ => None,
    }
}

/// Error type for directory scanning operations.
#[derive(Debug, Error)]
pub enum ScanError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Path is not a directory: {0}")]
    NotDirectory(String),

    #[error("Failed to read directory entry: {0}")]
    ReadEntry(String),
}

/// Options for directory scanning.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Include hidden files (starting with '.')
    pub include_hidden: bool,
    /// Custom ignore patterns (glob-style)
    pub ignore_patterns: Vec<String>,
}

/// Result of scanning a file.
#[derive(Debug, Clone)]
pub struct ScannedFile {
    /// Relative path from scan root
    pub relative_path: String,
    /// MIME type
    pub content_type: String,
    /// Whether file is binary
    pub is_binary: bool,
    /// File content (text or base64-encoded binary)
    pub content: String,
}

/// Scan a directory and build a filesystem schema.
///
/// This walks the directory tree recursively, skipping symlinks,
/// and builds an FsSchema that can be serialized to JSON.
///
/// If a `.commonplace.json` file exists in the directory, existing
/// node_ids will be preserved for files that still exist.
pub fn scan_directory(path: &Path, options: &ScanOptions) -> Result<FsSchema, ScanError> {
    if !path.is_dir() {
        return Err(ScanError::NotDirectory(path.display().to_string()));
    }

    // Load existing node_ids to preserve them
    let existing_node_ids = load_existing_node_ids(path);

    let root_entry = scan_dir_recursive(path, path, options, &existing_node_ids)?;

    Ok(FsSchema {
        version: 1,
        root: Some(root_entry),
    })
}

/// Recursively scan a directory and build an Entry tree.
fn scan_dir_recursive(
    root: &Path,
    current: &Path,
    options: &ScanOptions,
    existing_node_ids: &HashMap<String, String>,
) -> Result<Entry, ScanError> {
    let mut entries: HashMap<String, Entry> = HashMap::new();

    // Compute relative path for this directory (for looking up node_ids)
    let dir_relative = current
        .strip_prefix(root)
        .map(|p| normalize_path(&p.to_string_lossy()))
        .unwrap_or_default();

    let read_dir = fs::read_dir(current)?;

    for entry_result in read_dir {
        let entry = entry_result?;
        let file_type = entry.file_type()?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy().to_string();

        // Skip symlinks
        if file_type.is_symlink() {
            continue;
        }

        // Skip hidden files unless configured to include them
        if !options.include_hidden && name.starts_with('.') {
            continue;
        }

        // Skip ignored patterns
        if should_ignore(&name, &options.ignore_patterns) {
            continue;
        }

        let entry_path = entry.path();

        // Compute relative path for this entry
        let relative_path = if dir_relative.is_empty() {
            name.clone()
        } else {
            format!("{}/{}", dir_relative, name)
        };

        if file_type.is_dir() {
            // Check if this subdirectory has an existing node_id (already node-backed on server)
            if let Some(existing_node_id) = existing_node_ids.get(&relative_path) {
                // Subdirectory is already node-backed - preserve the reference
                // Don't scan inline, as the server has a separate document for this directory
                entries.insert(
                    name,
                    Entry::Dir(DirEntry {
                        entries: None,
                        node_id: Some(existing_node_id.clone()),
                        content_type: Some("application/json".to_string()),
                    }),
                );
            } else {
                // No existing node_id - scan inline as usual
                let sub_entry = scan_dir_recursive(root, &entry_path, options, existing_node_ids)?;
                entries.insert(name, sub_entry);
            }
        } else if file_type.is_file() {
            // Skip files with disallowed extensions
            if !is_allowed_extension(&entry_path) {
                continue;
            }

            // Create doc entry for file
            let content_info = detect_from_path(&entry_path);
            let content_type = if content_info.mime_type == "application/json" {
                if let Ok(raw) = fs::read(&entry_path) {
                    json_content_type_from_bytes(&raw)
                        .unwrap_or("application/json")
                        .to_string()
                } else {
                    content_info.mime_type.clone()
                }
            } else {
                content_info.mime_type.clone()
            };

            // Look up existing node_id for this file
            let node_id = existing_node_ids.get(&relative_path).cloned();

            let doc_entry = Entry::Doc(DocEntry {
                node_id,
                content_type: Some(content_type),
            });
            entries.insert(name, doc_entry);
        }
        // Skip other types (sockets, devices, etc.)
    }

    // The root entry returned here is the content of THIS document
    // Subdirectories with existing node_ids are preserved as node-backed references above
    Ok(Entry::Dir(DirEntry {
        entries: Some(entries),
        node_id: None,
        content_type: None,
    }))
}

/// Check if a filename matches any ignore pattern.
fn should_ignore(name: &str, patterns: &[String]) -> bool {
    for pattern in patterns {
        if pattern_matches(pattern, name) {
            return true;
        }
    }
    false
}

/// Simple glob pattern matching (supports * and ?).
fn pattern_matches(pattern: &str, name: &str) -> bool {
    // Simple implementation: exact match or wildcard patterns
    if pattern == name {
        return true;
    }

    // Handle simple * wildcards
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let prefix = parts[0];
            let suffix = parts[1];
            return name.starts_with(prefix) && name.ends_with(suffix);
        }
    }

    false
}

/// Scan a directory and collect all file contents.
///
/// Returns a list of scanned files with their contents ready for upload.
pub fn scan_directory_with_contents(
    path: &Path,
    options: &ScanOptions,
) -> Result<Vec<ScannedFile>, ScanError> {
    if !path.is_dir() {
        return Err(ScanError::NotDirectory(path.display().to_string()));
    }

    let mut files = Vec::new();
    scan_files_recursive(path, path, options, &mut files)?;
    Ok(files)
}

/// Recursively scan files and collect their contents.
fn scan_files_recursive(
    root: &Path,
    current: &Path,
    options: &ScanOptions,
    files: &mut Vec<ScannedFile>,
) -> Result<(), ScanError> {
    let read_dir = fs::read_dir(current)?;

    for entry_result in read_dir {
        let entry = entry_result?;
        let file_type = entry.file_type()?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy().to_string();

        // Skip symlinks
        if file_type.is_symlink() {
            continue;
        }

        // Skip hidden files unless configured
        if !options.include_hidden && name.starts_with('.') {
            continue;
        }

        // Skip ignored patterns
        if should_ignore(&name, &options.ignore_patterns) {
            continue;
        }

        let entry_path = entry.path();

        if file_type.is_dir() {
            scan_files_recursive(root, &entry_path, options, files)?;
        } else if file_type.is_file() {
            // Skip files with disallowed extensions
            if !is_allowed_extension(&entry_path) {
                continue;
            }

            // Normalize to forward slashes for cross-platform consistency
            let relative = entry_path
                .strip_prefix(root)
                .map(|p| normalize_path(&p.to_string_lossy()))
                .unwrap_or_else(|_| name.clone());

            let content_info = detect_from_path(&entry_path);
            let raw_content = fs::read(&entry_path)?;

            // Check actual content for binary detection
            let is_binary = content_info.is_binary || is_binary_content(&raw_content);

            let content_type = if content_info.mime_type == "application/json" && !is_binary {
                json_content_type_from_bytes(&raw_content)
                    .unwrap_or("application/json")
                    .to_string()
            } else {
                content_info.mime_type.clone()
            };

            let content = if is_binary {
                use base64::{engine::general_purpose::STANDARD, Engine};
                STANDARD.encode(&raw_content)
            } else {
                String::from_utf8_lossy(&raw_content).to_string()
            };

            files.push(ScannedFile {
                relative_path: relative,
                content_type,
                is_binary,
                content,
            });
        }
    }

    Ok(())
}

/// Convert an FsSchema to JSON string.
pub fn schema_to_json(schema: &FsSchema) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_directory() -> TempDir {
        let temp = TempDir::new().unwrap();

        // Create files
        File::create(temp.path().join("readme.txt"))
            .unwrap()
            .write_all(b"Hello")
            .unwrap();
        File::create(temp.path().join("data.json"))
            .unwrap()
            .write_all(b"{}")
            .unwrap();

        // Create subdirectory
        fs::create_dir(temp.path().join("notes")).unwrap();
        File::create(temp.path().join("notes/idea.md"))
            .unwrap()
            .write_all(b"# Idea")
            .unwrap();

        // Create hidden file (with allowed extension)
        File::create(temp.path().join(".hidden.txt"))
            .unwrap()
            .write_all(b"secret")
            .unwrap();

        temp
    }

    #[test]
    fn test_scan_directory_basic() {
        let temp = create_test_directory();
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        assert_eq!(schema.version, 1);
        assert!(schema.root.is_some());

        let root = schema.root.as_ref().unwrap();
        if let Entry::Dir(dir) = root {
            let entries = dir.entries.as_ref().unwrap();
            assert!(entries.contains_key("readme.txt"));
            assert!(entries.contains_key("data.json"));
            assert!(entries.contains_key("notes"));
            // Hidden files excluded by default
            assert!(!entries.contains_key(".hidden.txt"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    #[test]
    fn test_scan_directory_with_hidden() {
        let temp = create_test_directory();
        let options = ScanOptions {
            include_hidden: true,
            ..Default::default()
        };
        let schema = scan_directory(temp.path(), &options).unwrap();

        if let Some(Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert!(entries.contains_key(".hidden.txt"));
        }
    }

    #[test]
    fn test_scan_nested_directory() {
        let temp = create_test_directory();
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();
            if let Some(Entry::Dir(notes)) = entries.get("notes") {
                let note_entries = notes.entries.as_ref().unwrap();
                assert!(note_entries.contains_key("idea.md"));
            } else {
                panic!("Expected notes to be a Dir");
            }
        }
    }

    #[test]
    fn test_scan_with_ignore_patterns() {
        let temp = create_test_directory();
        let options = ScanOptions {
            ignore_patterns: vec!["*.txt".to_string()],
            ..Default::default()
        };
        let schema = scan_directory(temp.path(), &options).unwrap();

        if let Some(Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert!(!entries.contains_key("readme.txt"));
            assert!(entries.contains_key("data.json"));
        }
    }

    #[test]
    fn test_scan_directory_with_contents() {
        let temp = create_test_directory();
        let files = scan_directory_with_contents(temp.path(), &ScanOptions::default()).unwrap();

        assert!(!files.is_empty());

        // Check that we have the expected files
        let paths: Vec<&str> = files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(paths.contains(&"readme.txt"));
        assert!(paths.contains(&"data.json"));
        assert!(paths.iter().any(|p| p.contains("idea.md")));
    }

    #[test]
    fn test_content_types() {
        let temp = create_test_directory();
        let files = scan_directory_with_contents(temp.path(), &ScanOptions::default()).unwrap();

        for file in &files {
            match file.relative_path.as_str() {
                "readme.txt" => assert_eq!(file.content_type, "text/plain"),
                "data.json" => assert_eq!(file.content_type, "application/json"),
                path if path.ends_with("idea.md") => {
                    assert_eq!(file.content_type, "text/markdown")
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_schema_to_json() {
        let temp = create_test_directory();
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();
        let json = schema_to_json(&schema).unwrap();

        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["version"], 1);
        assert!(parsed["root"]["type"] == "dir");
    }

    #[test]
    fn test_pattern_matching() {
        assert!(pattern_matches("*.txt", "readme.txt"));
        assert!(pattern_matches("*.txt", "notes.txt"));
        assert!(!pattern_matches("*.txt", "readme.md"));
        assert!(pattern_matches("exact", "exact"));
        assert!(!pattern_matches("exact", "inexact"));
    }

    #[test]
    fn test_not_a_directory_error() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("file.txt");
        File::create(&file_path).unwrap();

        let result = scan_directory(&file_path, &ScanOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_scan_filters_disallowed_extensions() {
        let temp = TempDir::new().unwrap();

        // Create allowed files
        File::create(temp.path().join("allowed.txt"))
            .unwrap()
            .write_all(b"text")
            .unwrap();
        File::create(temp.path().join("data.json"))
            .unwrap()
            .write_all(b"{}")
            .unwrap();
        File::create(temp.path().join("doc.xml"))
            .unwrap()
            .write_all(b"<root/>")
            .unwrap();
        File::create(temp.path().join("page.xhtml"))
            .unwrap()
            .write_all(b"<html/>")
            .unwrap();
        File::create(temp.path().join("data.bin"))
            .unwrap()
            .write_all(b"\x00\x01\x02")
            .unwrap();
        File::create(temp.path().join("readme.md"))
            .unwrap()
            .write_all(b"# Readme")
            .unwrap();

        // Create disallowed files
        File::create(temp.path().join("code.rs"))
            .unwrap()
            .write_all(b"fn main() {}")
            .unwrap();
        File::create(temp.path().join("script.py"))
            .unwrap()
            .write_all(b"print('hi')")
            .unwrap();
        File::create(temp.path().join("image.png"))
            .unwrap()
            .write_all(b"PNG")
            .unwrap();

        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();

            // Allowed files should be present
            assert!(entries.contains_key("allowed.txt"));
            assert!(entries.contains_key("data.json"));
            assert!(entries.contains_key("doc.xml"));
            assert!(entries.contains_key("page.xhtml"));
            assert!(entries.contains_key("data.bin"));
            assert!(entries.contains_key("readme.md"));

            // Disallowed files should NOT be present
            assert!(!entries.contains_key("code.rs"));
            assert!(!entries.contains_key("script.py"));
            assert!(!entries.contains_key("image.png"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    #[test]
    fn test_scan_with_contents_filters_disallowed_extensions() {
        let temp = TempDir::new().unwrap();

        // Create allowed file
        File::create(temp.path().join("allowed.txt"))
            .unwrap()
            .write_all(b"text content")
            .unwrap();

        // Create disallowed file
        File::create(temp.path().join("code.rs"))
            .unwrap()
            .write_all(b"fn main() {}")
            .unwrap();

        let files = scan_directory_with_contents(temp.path(), &ScanOptions::default()).unwrap();

        let paths: Vec<&str> = files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(paths.contains(&"allowed.txt"));
        assert!(!paths.contains(&"code.rs"));
    }

    #[test]
    fn test_scan_preserves_existing_node_ids() {
        let temp = TempDir::new().unwrap();

        // Create files
        File::create(temp.path().join("file1.txt"))
            .unwrap()
            .write_all(b"content1")
            .unwrap();
        File::create(temp.path().join("file2.txt"))
            .unwrap()
            .write_all(b"content2")
            .unwrap();

        // Create subdirectory with file
        fs::create_dir(temp.path().join("subdir")).unwrap();
        File::create(temp.path().join("subdir/nested.txt"))
            .unwrap()
            .write_all(b"nested content")
            .unwrap();

        // Create existing .commonplace.json with node_ids
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file1.txt": {
                        "type": "doc",
                        "node_id": "uuid-for-file1",
                        "content_type": "text/plain"
                    },
                    "subdir": {
                        "type": "dir",
                        "entries": {
                            "nested.txt": {
                                "type": "doc",
                                "node_id": "uuid-for-nested",
                                "content_type": "text/plain"
                            }
                        }
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Scan should preserve existing node_ids
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // file1.txt should have preserved node_id
            if let Some(Entry::Doc(doc)) = entries.get("file1.txt") {
                assert_eq!(doc.node_id.as_deref(), Some("uuid-for-file1"));
            } else {
                panic!("Expected file1.txt to be a Doc");
            }

            // file2.txt should have None (not in existing schema)
            if let Some(Entry::Doc(doc)) = entries.get("file2.txt") {
                assert_eq!(doc.node_id, None);
            } else {
                panic!("Expected file2.txt to be a Doc");
            }

            // nested.txt should have preserved node_id
            if let Some(Entry::Dir(subdir)) = entries.get("subdir") {
                let sub_entries = subdir.entries.as_ref().unwrap();
                if let Some(Entry::Doc(doc)) = sub_entries.get("nested.txt") {
                    assert_eq!(doc.node_id.as_deref(), Some("uuid-for-nested"));
                } else {
                    panic!("Expected nested.txt to be a Doc");
                }
            } else {
                panic!("Expected subdir to be a Dir");
            }
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[test]
    fn test_scan_preserves_shared_node_ids() {
        let temp = TempDir::new().unwrap();

        // Create two files that should share a node_id (link scenario)
        File::create(temp.path().join("file_a.txt"))
            .unwrap()
            .write_all(b"content a")
            .unwrap();
        File::create(temp.path().join("file_b.txt"))
            .unwrap()
            .write_all(b"content b")
            .unwrap();

        // Create existing .commonplace.json with shared node_id
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file_a.txt": {
                        "type": "doc",
                        "node_id": "shared-uuid",
                        "content_type": "text/plain"
                    },
                    "file_b.txt": {
                        "type": "doc",
                        "node_id": "shared-uuid",
                        "content_type": "text/plain"
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Scan should preserve both node_ids (even though they're the same)
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            if let Some(Entry::Doc(doc_a)) = entries.get("file_a.txt") {
                assert_eq!(doc_a.node_id.as_deref(), Some("shared-uuid"));
            } else {
                panic!("Expected file_a.txt to be a Doc");
            }

            if let Some(Entry::Doc(doc_b)) = entries.get("file_b.txt") {
                assert_eq!(doc_b.node_id.as_deref(), Some("shared-uuid"));
            } else {
                panic!("Expected file_b.txt to be a Doc");
            }
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[test]
    fn test_scan_preserves_node_backed_directory() {
        let temp = TempDir::new().unwrap();

        // Create subdirectory with files
        fs::create_dir(temp.path().join("subdir")).unwrap();
        File::create(temp.path().join("subdir/file.txt"))
            .unwrap()
            .write_all(b"content")
            .unwrap();

        // Create existing .commonplace.json where subdir is node-backed (server-side document)
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "subdir": {
                        "type": "dir",
                        "node_id": "server-side-subdir-uuid",
                        "content_type": "application/json"
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Scan should preserve the node-backed directory reference
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // subdir should be node-backed (node_id, no entries)
            if let Some(Entry::Dir(subdir)) = entries.get("subdir") {
                // Should have node_id preserved
                assert_eq!(
                    subdir.node_id.as_deref(),
                    Some("server-side-subdir-uuid"),
                    "Expected subdir to preserve its node_id"
                );
                // Should NOT have inline entries (node-backed means entries is None)
                assert!(
                    subdir.entries.is_none(),
                    "Expected subdir to be node-backed (entries should be None)"
                );
            } else {
                panic!("Expected subdir to be a Dir");
            }
        } else {
            panic!("Expected root to be a Dir");
        }
    }
}
