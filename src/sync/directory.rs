//! Directory scanning and FS JSON generation.

use crate::fs::{DirEntry, DocEntry, Entry, FsSchema};
use crate::sync::content_type::{detect_from_path, is_binary_content};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use thiserror::Error;

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
pub fn scan_directory(path: &Path, options: &ScanOptions) -> Result<FsSchema, ScanError> {
    if !path.is_dir() {
        return Err(ScanError::NotDirectory(path.display().to_string()));
    }

    let root_entry = scan_dir_recursive(path, options)?;

    Ok(FsSchema {
        version: 1,
        root: Some(root_entry),
    })
}

/// Recursively scan a directory and build an Entry tree.
fn scan_dir_recursive(current: &Path, options: &ScanOptions) -> Result<Entry, ScanError> {
    let mut entries: HashMap<String, Entry> = HashMap::new();

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

        if file_type.is_dir() {
            // Recursively scan subdirectory
            let sub_entry = scan_dir_recursive(&entry_path, options)?;
            entries.insert(name, sub_entry);
        } else if file_type.is_file() {
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
            let doc_entry = Entry::Doc(DocEntry {
                node_id: None, // Use derived ID
                content_type: Some(content_type),
            });
            entries.insert(name, doc_entry);
        }
        // Skip other types (sockets, devices, etc.)
    }

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

        // Create hidden file
        File::create(temp.path().join(".hidden"))
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
            assert!(!entries.contains_key(".hidden"));
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
            assert!(entries.contains_key(".hidden"));
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
}
