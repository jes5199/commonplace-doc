//! Directory scanning and FS JSON generation.

use crate::fs::{DirEntry, DocEntry, Entry, FsSchema};
use crate::sync::content_type::{detect_from_path, is_allowed_extension, is_binary_content};
use crate::sync::schema_io::SCHEMA_FILENAME;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use thiserror::Error;
use tracing::warn;

/// Load existing node_ids from .commonplace.json if present.
/// Recursively loads from nested .commonplace.json files for node-backed directories.
fn load_existing_node_ids(directory: &Path) -> HashMap<String, String> {
    load_existing_node_ids_recursive(directory, directory, "")
}

/// Recursively load node_ids from a directory and its subdirectories.
fn load_existing_node_ids_recursive(
    _root: &Path,
    current: &Path,
    prefix: &str,
) -> HashMap<String, String> {
    let schema_path = current.join(SCHEMA_FILENAME);
    if !schema_path.exists() {
        return HashMap::new();
    }

    let mut result = HashMap::new();

    match fs::read_to_string(&schema_path) {
        Ok(content) => match serde_json::from_str::<FsSchema>(&content) {
            Ok(schema) => {
                if let Some(ref root_entry) = schema.root {
                    // Extract node_ids from this schema level
                    result.extend(extract_node_ids_with_prefix(root_entry, prefix));

                    // For node-backed directories, recursively load from their .commonplace.json
                    load_nested_node_ids(root_entry, current, prefix, &mut result);
                }
            }
            Err(_) => {}
        },
        Err(_) => {}
    }

    result
}

/// Extract node_ids with a given prefix.
fn extract_node_ids_with_prefix(entry: &Entry, prefix: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();

    match entry {
        Entry::Doc(doc) => {
            if let Some(ref node_id) = doc.node_id {
                result.insert(prefix.to_string(), node_id.clone());
            }
        }
        Entry::Dir(dir) => {
            // Capture directory node_id
            if let Some(ref node_id) = dir.node_id {
                result.insert(prefix.to_string(), node_id.clone());
            }
            // For inline entries, recursively extract
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    result.extend(extract_node_ids_with_prefix(child, &child_path));
                }
            }
        }
    }

    result
}

/// Recursively load node_ids from nested .commonplace.json files.
fn load_nested_node_ids(
    entry: &Entry,
    current_dir: &Path,
    prefix: &str,
    result: &mut HashMap<String, String>,
) {
    if let Entry::Dir(dir) = entry {
        // For node-backed directories, check for nested .commonplace.json
        if dir.node_id.is_some() && dir.entries.is_none() {
            // This is a node-backed directory - don't recurse here as we handle
            // it at the schema level by looking for .commonplace.json in subdirs
        }

        // For inline directories (shouldn't exist after fix, but handle for compatibility)
        if let Some(ref entries) = dir.entries {
            for (name, child) in entries {
                let child_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", prefix, name)
                };
                let child_dir = current_dir.join(name);

                // If this is a node-backed subdirectory, load its nested schema
                if let Entry::Dir(child_dir_entry) = child {
                    if child_dir_entry.node_id.is_some() {
                        let nested_schema_path = child_dir.join(SCHEMA_FILENAME);
                        if nested_schema_path.exists() {
                            if let Ok(content) = fs::read_to_string(&nested_schema_path) {
                                if let Ok(nested_schema) =
                                    serde_json::from_str::<FsSchema>(&content)
                                {
                                    if let Some(ref nested_root) = nested_schema.root {
                                        result.extend(extract_node_ids_with_prefix(
                                            nested_root,
                                            &child_path,
                                        ));
                                        // Recursively load from deeper levels
                                        load_nested_node_ids(
                                            nested_root,
                                            &child_dir,
                                            &child_path,
                                            result,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                // Continue recursing into inline entries
                load_nested_node_ids(child, &child_dir, &child_path, result);
            }
        }
    }
}

/// Load linked entries from the existing schema.
///
/// A "linked" entry is one where:
/// - The file doesn't exist locally
/// - But the entry has a node_id that is shared with another file that does exist
///
/// This supports `commonplace-link` which creates entries pointing to the same document.
fn load_linked_entries(
    directory: &Path,
    scanned_entries: &HashMap<String, Entry>,
) -> HashMap<String, Entry> {
    let schema_path = directory.join(SCHEMA_FILENAME);
    if !schema_path.exists() {
        return HashMap::new();
    }

    let content = match fs::read_to_string(&schema_path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };

    let schema: FsSchema = match serde_json::from_str(&content) {
        Ok(s) => s,
        Err(_) => return HashMap::new(),
    };

    let existing_entries = match &schema.root {
        Some(Entry::Dir(dir)) => match &dir.entries {
            Some(e) => e.clone(),
            None => return HashMap::new(),
        },
        _ => return HashMap::new(),
    };

    // Build a set of node_ids that exist in the scanned entries
    let mut scanned_node_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for entry in scanned_entries.values() {
        if let Entry::Doc(doc) = entry {
            if let Some(ref node_id) = doc.node_id {
                scanned_node_ids.insert(node_id.clone());
            }
        }
    }

    // Find linked entries: entries in existing schema that:
    // - Are not in scanned entries (file doesn't exist locally)
    // - Have a node_id that IS in scanned entries (linked to an existing file)
    let mut linked = HashMap::new();
    for (name, entry) in &existing_entries {
        // Skip if this entry was scanned (file exists locally)
        if scanned_entries.contains_key(name) {
            continue;
        }

        // Check if this is a linked entry (node_id shared with another file)
        if let Entry::Doc(doc) = entry {
            if let Some(ref node_id) = doc.node_id {
                if scanned_node_ids.contains(node_id) {
                    // This is a linked entry - preserve it
                    linked.insert(name.clone(), entry.clone());
                }
            }
        }
    }

    linked
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

    #[error("Failed to serialize schema: {0}")]
    Serialization(String),
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
///
/// Linked entries (from `commonplace-link`) are also preserved even if
/// the local file doesn't exist, as long as the linked target exists.
pub fn scan_directory(path: &Path, options: &ScanOptions) -> Result<FsSchema, ScanError> {
    if !path.is_dir() {
        return Err(ScanError::NotDirectory(path.display().to_string()));
    }

    // Load existing node_ids to preserve them
    let existing_node_ids = load_existing_node_ids(path);

    let mut root_entry = scan_dir_recursive(path, path, options, &existing_node_ids)?;

    // Preserve linked entries that don't have local files but share node_ids
    // with files that do exist. This supports commonplace-link functionality.
    if let Entry::Dir(ref mut dir) = root_entry {
        if let Some(ref mut entries) = dir.entries {
            let linked_entries = load_linked_entries(path, entries);
            for (name, entry) in linked_entries {
                entries.insert(name, entry);
            }
        }
    }

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

        // Handle symlinks - convert to commonplace-linked files
        if file_type.is_symlink() {
            // Apply the same hidden/ignore filters as for regular files
            if !options.include_hidden && name.starts_with('.') {
                continue;
            }
            if should_ignore(&name, &options.ignore_patterns) {
                continue;
            }

            let entry_path = entry.path();

            // Compute relative path for this entry
            let symlink_relative = if dir_relative.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", dir_relative, name)
            };

            match resolve_symlink(root, &entry_path) {
                SymlinkResolution::WithinWorkspace { target_relative } => {
                    // Look up the target's node_id
                    if let Some(target_node_id) = existing_node_ids.get(&target_relative) {
                        // Skip files with disallowed extensions
                        if !is_allowed_extension(&entry_path) {
                            continue;
                        }

                        // Use the same node_id as the target (commonplace-link behavior)
                        let content_info = detect_from_path(&entry_path);
                        let doc_entry = Entry::Doc(DocEntry {
                            node_id: Some(target_node_id.clone()),
                            content_type: Some(content_info.mime_type.clone()),
                        });
                        entries.insert(name, doc_entry);
                        tracing::info!(
                            symlink = %symlink_relative,
                            target = %target_relative,
                            node_id = %target_node_id,
                            "Converted symlink to commonplace-link"
                        );
                    } else {
                        // Target doesn't have a node_id yet - skip for now
                        // Next scan will pick it up once the target has been synced
                        warn!(
                            symlink = %symlink_relative,
                            target = %target_relative,
                            "Skipping symlink: target not yet synced (will retry on next scan)"
                        );
                    }
                }
                SymlinkResolution::OutsideWorkspace => {
                    warn!(
                        symlink = %symlink_relative,
                        "Skipping symlink: target is outside workspace"
                    );
                }
                SymlinkResolution::IsDirectory => {
                    warn!(
                        symlink = %symlink_relative,
                        "Skipping symlink: directory symlinks not supported"
                    );
                }
                SymlinkResolution::Failed(reason) => {
                    warn!(
                        symlink = %symlink_relative,
                        reason = %reason,
                        "Skipping symlink: resolution failed"
                    );
                }
            }
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
            // If no existing node_id, use None - the server will generate a UUID when we push.
            // This ensures all sync clients get the same server-generated UUIDs.
            let node_id = existing_node_ids.get(&relative_path).cloned();

            // Always create node-backed directory references
            // Recursively scan the subdirectory to build its schema
            let sub_entry = scan_dir_recursive(root, &entry_path, options, existing_node_ids)?;

            // Write the subdirectory schema to .commonplace.json
            let sub_schema = FsSchema {
                version: 1,
                root: Some(sub_entry),
            };
            let sub_schema_json = serde_json::to_string(&sub_schema)
                .map_err(|e| ScanError::Serialization(e.to_string()))?;
            let sub_schema_path = entry_path.join(SCHEMA_FILENAME);
            fs::write(&sub_schema_path, &sub_schema_json)?;

            // Create node-backed reference in parent
            entries.insert(
                name,
                Entry::Dir(DirEntry {
                    entries: None,
                    node_id,
                    content_type: Some("application/json".to_string()),
                }),
            );
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

            // Look up existing node_id for this file.
            // If no existing node_id, use None - the server will generate a UUID when we push.
            // This ensures all sync clients get the same server-generated UUIDs.
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

/// Result of resolving a symlink.
enum SymlinkResolution {
    /// Target is within workspace, use this relative path for node_id lookup
    WithinWorkspace { target_relative: String },
    /// Target is outside workspace, skip this symlink
    OutsideWorkspace,
    /// Target is a directory (not supported for auto-linking)
    IsDirectory,
    /// Failed to resolve the symlink
    Failed(String),
}

/// Resolve a symlink and check if its target is within the workspace.
fn resolve_symlink(root: &Path, symlink_path: &Path) -> SymlinkResolution {
    // Read the symlink target
    let target = match fs::read_link(symlink_path) {
        Ok(t) => t,
        Err(e) => return SymlinkResolution::Failed(format!("Failed to read symlink: {}", e)),
    };

    // Resolve to absolute path
    let absolute_target = if target.is_absolute() {
        target
    } else {
        // Relative symlink - resolve from symlink's parent directory
        let parent = symlink_path.parent().unwrap_or(Path::new("."));
        parent.join(&target)
    };

    // Canonicalize to resolve any .. or . components
    let canonical_target = match absolute_target.canonicalize() {
        Ok(c) => c,
        Err(e) => {
            return SymlinkResolution::Failed(format!("Failed to canonicalize target: {}", e))
        }
    };

    // Canonicalize root for comparison
    let canonical_root = match root.canonicalize() {
        Ok(c) => c,
        Err(e) => return SymlinkResolution::Failed(format!("Failed to canonicalize root: {}", e)),
    };

    // Check if target is within the workspace root
    if !canonical_target.starts_with(&canonical_root) {
        return SymlinkResolution::OutsideWorkspace;
    }

    // Check if target is a directory
    if canonical_target.is_dir() {
        return SymlinkResolution::IsDirectory;
    }

    // Get relative path from root
    let target_relative = canonical_target
        .strip_prefix(&canonical_root)
        .map(|p| normalize_path(&p.to_string_lossy()))
        .unwrap_or_default();

    SymlinkResolution::WithinWorkspace { target_relative }
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

        // Skip symlinks - content is synced through the target file
        // The schema scan (scan_dir_recursive) handles symlink-to-link conversion
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
                // Subdirectories are node-backed (no inline entries).
                // node_id is None for new directories - server assigns UUIDs.
                assert!(
                    notes.entries.is_none(),
                    "notes should not have inline entries"
                );

                // Verify nested schema was written to .commonplace.json
                let nested_schema_path = temp.path().join("notes").join(".commonplace.json");
                assert!(
                    nested_schema_path.exists(),
                    "Nested .commonplace.json should be created"
                );

                // Read and verify the nested schema
                let nested_content = std::fs::read_to_string(&nested_schema_path).unwrap();
                let nested_schema: FsSchema = serde_json::from_str(&nested_content).unwrap();
                if let Some(Entry::Dir(nested_root)) = &nested_schema.root {
                    let note_entries = nested_root.entries.as_ref().unwrap();
                    assert!(note_entries.contains_key("idea.md"));
                } else {
                    panic!("Expected nested schema root to be a Dir");
                }
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

        // Create existing .commonplace.json with node_ids (node-backed subdirectory)
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
                        "node_id": "uuid-for-subdir",
                        "content_type": "application/json"
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Create nested .commonplace.json for subdir with nested file node_id
        let nested_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "nested.txt": {
                        "type": "doc",
                        "node_id": "uuid-for-nested",
                        "content_type": "text/plain"
                    }
                }
            }
        }"#;
        File::create(temp.path().join("subdir/.commonplace.json"))
            .unwrap()
            .write_all(nested_schema.as_bytes())
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

            // file2.txt should have None node_id (not in existing schema).
            // Server will assign UUID when schema is pushed.
            if let Some(Entry::Doc(doc)) = entries.get("file2.txt") {
                assert!(
                    doc.node_id.is_none(),
                    "New files should have None node_id (server assigns UUID)"
                );
            } else {
                panic!("Expected file2.txt to be a Doc");
            }

            // subdir should be node-backed with preserved node_id
            if let Some(Entry::Dir(subdir)) = entries.get("subdir") {
                assert_eq!(
                    subdir.node_id.as_deref(),
                    Some("uuid-for-subdir"),
                    "Subdirectory node_id should be preserved"
                );
                assert!(
                    subdir.entries.is_none(),
                    "Subdirectory should be node-backed (no inline entries)"
                );

                // Read the nested .commonplace.json to verify nested.txt node_id
                let nested_schema_path = temp.path().join("subdir/.commonplace.json");
                let nested_content = std::fs::read_to_string(&nested_schema_path).unwrap();
                let nested_schema: FsSchema = serde_json::from_str(&nested_content).unwrap();
                if let Some(Entry::Dir(nested_root)) = &nested_schema.root {
                    let nested_entries = nested_root.entries.as_ref().unwrap();
                    if let Some(Entry::Doc(doc)) = nested_entries.get("nested.txt") {
                        assert_eq!(doc.node_id.as_deref(), Some("uuid-for-nested"));
                    } else {
                        panic!("Expected nested.txt to be a Doc");
                    }
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

    #[cfg(unix)]
    #[test]
    fn test_scan_converts_symlink_to_commonplace_link() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();

        // Create target file
        File::create(temp.path().join("target.txt"))
            .unwrap()
            .write_all(b"target content")
            .unwrap();

        // Create symlink pointing to target
        symlink(temp.path().join("target.txt"), temp.path().join("link.txt")).unwrap();

        // Create existing .commonplace.json with node_id for target
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "target.txt": {
                        "type": "doc",
                        "node_id": "target-uuid",
                        "content_type": "text/plain"
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Scan should convert symlink to commonplace-link (same node_id)
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // target.txt should have its node_id preserved
            if let Some(Entry::Doc(target)) = entries.get("target.txt") {
                assert_eq!(target.node_id.as_deref(), Some("target-uuid"));
            } else {
                panic!("Expected target.txt to be a Doc");
            }

            // link.txt (symlink) should use target's node_id
            if let Some(Entry::Doc(link)) = entries.get("link.txt") {
                assert_eq!(
                    link.node_id.as_deref(),
                    Some("target-uuid"),
                    "Symlink should share node_id with target"
                );
            } else {
                panic!("Expected link.txt to be converted to a Doc");
            }
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_scan_skips_symlink_to_unsynced_target() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();

        // Create target file (no existing node_id in schema)
        File::create(temp.path().join("target.txt"))
            .unwrap()
            .write_all(b"target content")
            .unwrap();

        // Create symlink pointing to target
        symlink(temp.path().join("target.txt"), temp.path().join("link.txt")).unwrap();

        // No existing .commonplace.json - target has no node_id yet

        // Scan should skip the symlink since target has no node_id
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // target.txt should be present with a generated UUID
            assert!(entries.contains_key("target.txt"));

            // link.txt should NOT be present (skipped because target not synced)
            assert!(
                !entries.contains_key("link.txt"),
                "Symlink should be skipped until target is synced"
            );
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_scan_skips_hidden_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();

        // Create target file
        File::create(temp.path().join("target.txt"))
            .unwrap()
            .write_all(b"target content")
            .unwrap();

        // Create hidden symlink pointing to target
        symlink(
            temp.path().join("target.txt"),
            temp.path().join(".hidden-link.txt"),
        )
        .unwrap();

        // Create existing .commonplace.json with node_id for target
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "target.txt": {
                        "type": "doc",
                        "node_id": "target-uuid",
                        "content_type": "text/plain"
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Scan with include_hidden=false (default) should skip hidden symlink
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // Hidden symlink should NOT be present
            assert!(
                !entries.contains_key(".hidden-link.txt"),
                "Hidden symlink should be skipped by default"
            );
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_scan_skips_symlink_outside_workspace() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();

        // Create file outside workspace
        File::create(external.path().join("external.txt"))
            .unwrap()
            .write_all(b"external content")
            .unwrap();

        // Create symlink pointing outside workspace
        symlink(
            external.path().join("external.txt"),
            temp.path().join("link.txt"),
        )
        .unwrap();

        // Scan should skip the symlink since target is outside workspace
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // link.txt should NOT be present
            assert!(
                !entries.contains_key("link.txt"),
                "Symlink to external file should be skipped"
            );
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[test]
    fn test_scan_preserves_linked_entries() {
        // Test that commonplace-link entries are preserved even when
        // the local file doesn't exist, as long as the target file exists.
        let temp = TempDir::new().unwrap();

        // Create source file (the linked target)
        File::create(temp.path().join("source.txt"))
            .unwrap()
            .write_all(b"shared content")
            .unwrap();

        // Create .commonplace.json with both source and a link pointing to it
        // The link file doesn't exist locally, but should be preserved
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "source.txt": {
                        "type": "doc",
                        "node_id": "shared-uuid",
                        "content_type": "text/plain"
                    },
                    "link.txt": {
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

        // Scan should preserve the linked entry even though link.txt doesn't exist
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // source.txt should be present with preserved node_id
            if let Some(Entry::Doc(doc)) = entries.get("source.txt") {
                assert_eq!(doc.node_id.as_deref(), Some("shared-uuid"));
            } else {
                panic!("Expected source.txt to be a Doc");
            }

            // link.txt should be preserved because it shares node_id with source.txt
            assert!(
                entries.contains_key("link.txt"),
                "Linked entry should be preserved even without local file"
            );
            if let Some(Entry::Doc(doc)) = entries.get("link.txt") {
                assert_eq!(
                    doc.node_id.as_deref(),
                    Some("shared-uuid"),
                    "Linked entry should have same node_id as source"
                );
            } else {
                panic!("Expected link.txt to be a Doc");
            }
        } else {
            panic!("Expected root to be a Dir");
        }
    }

    #[test]
    fn test_scan_does_not_preserve_unlinked_orphan_entries() {
        // Test that entries with node_ids that don't match any local file
        // are NOT preserved (they're not linked, just orphaned)
        let temp = TempDir::new().unwrap();

        // Create a real file
        File::create(temp.path().join("real.txt"))
            .unwrap()
            .write_all(b"real content")
            .unwrap();

        // Create .commonplace.json with real file AND an orphan (different node_id)
        let existing_schema = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "real.txt": {
                        "type": "doc",
                        "node_id": "real-uuid",
                        "content_type": "text/plain"
                    },
                    "orphan.txt": {
                        "type": "doc",
                        "node_id": "orphan-uuid",
                        "content_type": "text/plain"
                    }
                }
            }
        }"#;
        File::create(temp.path().join(".commonplace.json"))
            .unwrap()
            .write_all(existing_schema.as_bytes())
            .unwrap();

        // Scan should NOT preserve the orphan entry (node_id doesn't match any local file)
        let schema = scan_directory(temp.path(), &ScanOptions::default()).unwrap();

        if let Some(Entry::Dir(root)) = &schema.root {
            let entries = root.entries.as_ref().unwrap();

            // real.txt should be present
            assert!(entries.contains_key("real.txt"));

            // orphan.txt should NOT be present (not linked to any existing file)
            assert!(
                !entries.contains_key("orphan.txt"),
                "Orphan entry (no matching local file) should not be preserved"
            );
        } else {
            panic!("Expected root to be a Dir");
        }
    }
}
