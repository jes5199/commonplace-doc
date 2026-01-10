//! Workspace path resolution utilities for CLI tools.
//!
//! Provides utilities for resolving file paths to UUIDs within a synced workspace.

use crate::fs::{Entry, FsSchema};
use crate::sync::schema_io::SCHEMA_FILENAME;
use chrono::TimeZone;
use std::fs;
use std::path::{Path, PathBuf};

/// Error type for workspace operations
#[derive(Debug)]
pub enum WorkspaceError {
    /// Not in a commonplace sync directory
    NotInWorkspace(String),
    /// Path is not under workspace
    PathNotUnderWorkspace { path: String, workspace: String },
    /// Empty path provided
    EmptyPath,
    /// Directory not found in schema
    DirNotFound(String),
    /// Root is not a directory
    RootNotDirectory,
    /// Directory has no entries
    NoEntries,
    /// File not found in schema
    FileNotFound(String),
    /// Path refers to a directory, not a file
    IsDirectory(String),
    /// File has no UUID assigned
    NoUuid(String),
    /// Node-backed directory missing schema file
    MissingSchema(String),
    /// IO error
    Io(std::io::Error),
    /// JSON parse error
    Json(serde_json::Error),
}

impl std::fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkspaceError::NotInWorkspace(msg) => write!(f, "{}", msg),
            WorkspaceError::PathNotUnderWorkspace { path, workspace } => {
                write!(f, "Path {} is not under workspace {}", path, workspace)
            }
            WorkspaceError::EmptyPath => write!(f, "Empty path"),
            WorkspaceError::DirNotFound(name) => {
                write!(f, "Directory not found in schema: {}", name)
            }
            WorkspaceError::RootNotDirectory => write!(f, "Root is not a directory"),
            WorkspaceError::NoEntries => write!(f, "Directory has no entries"),
            WorkspaceError::FileNotFound(name) => write!(f, "File not found in schema: {}", name),
            WorkspaceError::IsDirectory(name) => {
                write!(f, "{} is a directory, not a file", name)
            }
            WorkspaceError::NoUuid(name) => write!(f, "File {} has no UUID assigned", name),
            WorkspaceError::MissingSchema(dir) => {
                write!(
                    f,
                    "Node-backed directory {} has no {} file",
                    dir, SCHEMA_FILENAME
                )
            }
            WorkspaceError::Io(e) => write!(f, "IO error: {}", e),
            WorkspaceError::Json(e) => write!(f, "JSON parse error: {}", e),
        }
    }
}

impl std::error::Error for WorkspaceError {}

impl From<std::io::Error> for WorkspaceError {
    fn from(e: std::io::Error) -> Self {
        WorkspaceError::Io(e)
    }
}

impl From<serde_json::Error> for WorkspaceError {
    fn from(e: serde_json::Error) -> Self {
        WorkspaceError::Json(e)
    }
}

/// Find the workspace root by traversing up from start directory.
///
/// Returns (workspace_root_path, schema_file_path).
pub fn find_workspace_root(start: &Path) -> Result<(PathBuf, PathBuf), WorkspaceError> {
    let mut current = start.to_path_buf();
    loop {
        let schema_path = current.join(SCHEMA_FILENAME);
        if schema_path.exists() {
            return Ok((current, schema_path));
        }
        if !current.pop() {
            return Err(WorkspaceError::NotInWorkspace(format!(
                "Not in a commonplace sync directory: {} not found",
                SCHEMA_FILENAME
            )));
        }
    }
}

/// Normalize a path to be relative to the workspace root.
pub fn normalize_path(
    path: &Path,
    cwd: &Path,
    workspace_root: &Path,
) -> Result<String, WorkspaceError> {
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };

    let rel_path = abs_path.strip_prefix(workspace_root).map_err(|_| {
        WorkspaceError::PathNotUnderWorkspace {
            path: path.display().to_string(),
            workspace: workspace_root.display().to_string(),
        }
    })?;

    Ok(rel_path.to_string_lossy().to_string())
}

/// Split a path into directory components and filename.
pub fn split_path(path: &str) -> Result<(Vec<String>, String), WorkspaceError> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return Err(WorkspaceError::EmptyPath);
    }
    let filename = parts.last().unwrap().to_string();
    let dirs: Vec<String> = parts[..parts.len() - 1]
        .iter()
        .map(|s| s.to_string())
        .collect();
    Ok((dirs, filename))
}

/// Resolve a file path to its UUID by walking the schema tree.
pub fn resolve_uuid(
    workspace_root: &Path,
    dirs: &[String],
    filename: &str,
) -> Result<String, WorkspaceError> {
    let root_schema_path = workspace_root.join(SCHEMA_FILENAME);
    let root_schema_content = fs::read_to_string(&root_schema_path)?;
    let mut current_schema: FsSchema = serde_json::from_str(&root_schema_content)?;
    let mut current_dir_path = workspace_root.to_path_buf();

    let mut remaining_dirs = dirs.to_vec();
    let mut i = 0;

    while i < remaining_dirs.len() {
        let dir_name = &remaining_dirs[i];

        let root = current_schema
            .root
            .as_ref()
            .ok_or(WorkspaceError::RootNotDirectory)?;
        let entries = match root {
            Entry::Dir(d) => d.entries.as_ref().ok_or(WorkspaceError::NoEntries)?,
            _ => return Err(WorkspaceError::RootNotDirectory),
        };

        let entry = entries
            .get(dir_name)
            .ok_or_else(|| WorkspaceError::DirNotFound(dir_name.clone()))?;

        match entry {
            Entry::Dir(dir) => {
                if dir.entries.is_none() && dir.node_id.is_some() {
                    current_dir_path = current_dir_path.join(dir_name);
                    let subdir_schema_path = current_dir_path.join(SCHEMA_FILENAME);

                    if subdir_schema_path.exists() {
                        let content = fs::read_to_string(&subdir_schema_path)?;
                        current_schema = serde_json::from_str(&content)?;
                        remaining_dirs = remaining_dirs[i + 1..].to_vec();
                        i = 0;
                        continue;
                    } else {
                        return Err(WorkspaceError::MissingSchema(dir_name.clone()));
                    }
                }
                current_dir_path = current_dir_path.join(dir_name);
            }
            _ => return Err(WorkspaceError::DirNotFound(dir_name.clone())),
        }
        i += 1;
    }

    let root = current_schema
        .root
        .as_ref()
        .ok_or(WorkspaceError::RootNotDirectory)?;

    let mut entries = match root {
        Entry::Dir(d) => d.entries.as_ref().ok_or(WorkspaceError::NoEntries)?,
        _ => return Err(WorkspaceError::RootNotDirectory),
    };

    for dir_name in &remaining_dirs {
        let entry = entries
            .get(dir_name)
            .ok_or_else(|| WorkspaceError::DirNotFound(dir_name.clone()))?;
        entries = match entry {
            Entry::Dir(d) => d.entries.as_ref().ok_or(WorkspaceError::NoEntries)?,
            _ => return Err(WorkspaceError::DirNotFound(dir_name.clone())),
        };
    }

    let file_entry = entries
        .get(filename)
        .ok_or_else(|| WorkspaceError::FileNotFound(filename.to_string()))?;

    match file_entry {
        Entry::Doc(doc) => doc
            .node_id
            .clone()
            .ok_or_else(|| WorkspaceError::NoUuid(filename.to_string())),
        Entry::Dir(_) => Err(WorkspaceError::IsDirectory(filename.to_string())),
    }
}

/// Format a timestamp (milliseconds since epoch) as a human-readable datetime.
pub fn format_timestamp(ts: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let time = UNIX_EPOCH + Duration::from_millis(ts);
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Format a timestamp in short format (for --oneline output).
pub fn format_timestamp_short(ts: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let time = UNIX_EPOCH + Duration::from_millis(ts);
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M").to_string()
}

/// Parse a date string into a timestamp (milliseconds since epoch).
///
/// Supports formats: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or raw timestamp.
pub fn parse_date(s: &str) -> Option<u64> {
    // Try as raw timestamp first
    if let Ok(ts) = s.parse::<u64>() {
        return Some(ts);
    }

    // Try YYYY-MM-DD HH:MM:SS
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        let local = chrono::Local
            .from_local_datetime(&dt)
            .single()
            .map(|d| d.timestamp_millis() as u64);
        if local.is_some() {
            return local;
        }
    }

    // Try YYYY-MM-DD (start of day)
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0)?;
        return chrono::Local
            .from_local_datetime(&dt)
            .single()
            .map(|d| d.timestamp_millis() as u64);
    }

    None
}

/// Resolve a file path to its UUID, given a file path (relative or absolute).
///
/// This is a convenience function that handles finding the workspace root,
/// normalizing the path, and resolving the UUID in one call.
pub fn resolve_path_to_uuid(file_path: &Path) -> Result<(String, PathBuf, String), WorkspaceError> {
    let cwd = std::env::current_dir()?;
    let abs_path = if file_path.is_absolute() {
        file_path.to_path_buf()
    } else {
        cwd.join(file_path)
    };
    let file_dir = abs_path.parent().ok_or(WorkspaceError::EmptyPath)?;
    let (workspace_root, _) = find_workspace_root(file_dir)?;
    let rel_path = normalize_path(file_path, &cwd, &workspace_root)?;
    let (dirs, filename) = split_path(&rel_path)?;
    let uuid = resolve_uuid(&workspace_root, &dirs, &filename)?;
    Ok((uuid, workspace_root, rel_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date_timestamp() {
        let ts = parse_date("1704067200000");
        assert_eq!(ts, Some(1704067200000));
    }

    #[test]
    fn test_parse_date_ymd() {
        let ts = parse_date("2024-01-01");
        assert!(ts.is_some());
    }

    #[test]
    fn test_parse_date_invalid() {
        assert!(parse_date("not a date").is_none());
    }

    #[test]
    fn test_split_path() {
        let (dirs, filename) = split_path("foo/bar/baz.txt").unwrap();
        assert_eq!(dirs, vec!["foo", "bar"]);
        assert_eq!(filename, "baz.txt");
    }

    #[test]
    fn test_split_path_no_dirs() {
        let (dirs, filename) = split_path("file.txt").unwrap();
        assert!(dirs.is_empty());
        assert_eq!(filename, "file.txt");
    }
}
