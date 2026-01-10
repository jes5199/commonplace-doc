//! commonplace-uuid: Resolve a synced file path to its UUID
//!
//! Usage:
//!   commonplace-uuid path/to/file.txt
//!   commonplace-uuid --json path/to/file.txt
//!
//! Prints the UUID that the file is linked to, or an error if not found.

use clap::Parser;
use commonplace_doc::{
    cli::UuidArgs,
    fs::{Entry, FsSchema},
    sync::SCHEMA_FILENAME,
};
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = UuidArgs::parse();

    // Find the workspace root by searching up for .commonplace.json
    let cwd = std::env::current_dir()?;
    let (workspace_root, _) = find_workspace_root(&cwd)?;

    // Convert path to workspace-relative
    let rel_path = normalize_path(&args.path, &cwd, &workspace_root)?;

    // Split into directory components and filename
    let (dirs, filename) = split_path(&rel_path)?;

    // Resolve to the correct schema and find the UUID
    let uuid = resolve_uuid(&workspace_root, &dirs, &filename)?;

    if args.json {
        println!(
            "{{\"path\":\"{}\",\"uuid\":\"{}\"}}",
            rel_path.replace('\\', "\\\\").replace('"', "\\\""),
            uuid
        );
    } else {
        println!("{}", uuid);
    }

    Ok(())
}

/// Find the workspace root by searching up the directory tree for .commonplace.json
fn find_workspace_root(start: &Path) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let mut current = start.to_path_buf();
    loop {
        let schema_path = current.join(SCHEMA_FILENAME);
        if schema_path.exists() {
            return Ok((current, schema_path));
        }
        if !current.pop() {
            return Err(format!(
                "Not in a commonplace sync directory: {} not found in any parent directory",
                SCHEMA_FILENAME
            )
            .into());
        }
    }
}

/// Normalize a path to be relative to the workspace root
fn normalize_path(
    path: &Path,
    cwd: &Path,
    workspace_root: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };

    let rel_path = abs_path.strip_prefix(workspace_root).map_err(|_| {
        format!(
            "Path {} is not under workspace {}",
            path.display(),
            workspace_root.display()
        )
    })?;

    Ok(rel_path.to_string_lossy().to_string())
}

/// Split a path into directory components and filename
fn split_path(path: &str) -> Result<(Vec<String>, String), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return Err("Empty path".into());
    }
    let filename = parts.last().unwrap().to_string();
    let dirs: Vec<String> = parts[..parts.len() - 1]
        .iter()
        .map(|s| s.to_string())
        .collect();
    Ok((dirs, filename))
}

/// Resolve a path to its UUID by walking the schema hierarchy
fn resolve_uuid(
    workspace_root: &Path,
    dirs: &[String],
    filename: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let root_schema_path = workspace_root.join(SCHEMA_FILENAME);
    let root_schema_content = fs::read_to_string(&root_schema_path)?;
    let mut current_schema: FsSchema = serde_json::from_str(&root_schema_content)?;
    let mut current_dir_path = workspace_root.to_path_buf();

    // Navigate through directories, switching schemas at node-backed boundaries
    let mut remaining_dirs = dirs.to_vec();
    let mut i = 0;

    while i < remaining_dirs.len() {
        let dir_name = &remaining_dirs[i];

        let root = current_schema
            .root
            .as_ref()
            .ok_or("Schema has no root directory")?;
        let entries = match root {
            Entry::Dir(d) => d.entries.as_ref().ok_or("Directory has no entries")?,
            _ => return Err("Root is not a directory".into()),
        };

        let entry = entries
            .get(dir_name)
            .ok_or_else(|| format!("Directory not found in schema: {}", dir_name))?;

        match entry {
            Entry::Dir(dir) => {
                if dir.entries.is_none() && dir.node_id.is_some() {
                    // Node-backed directory - switch to its schema
                    current_dir_path = current_dir_path.join(dir_name);
                    let subdir_schema_path = current_dir_path.join(SCHEMA_FILENAME);

                    if subdir_schema_path.exists() {
                        let content = fs::read_to_string(&subdir_schema_path)?;
                        current_schema = serde_json::from_str(&content)?;
                        remaining_dirs = remaining_dirs[i + 1..].to_vec();
                        i = 0;
                        continue;
                    } else {
                        return Err(format!(
                            "Node-backed directory {} has no {} file",
                            dir_name, SCHEMA_FILENAME
                        )
                        .into());
                    }
                }
                current_dir_path = current_dir_path.join(dir_name);
            }
            _ => return Err(format!("{} is not a directory", dir_name).into()),
        }
        i += 1;
    }

    // Now find the file in the current schema
    let root = current_schema
        .root
        .as_ref()
        .ok_or("Schema has no root directory")?;

    let mut entries = match root {
        Entry::Dir(d) => d.entries.as_ref().ok_or("Directory has no entries")?,
        _ => return Err("Root is not a directory".into()),
    };

    // Navigate to the file through remaining dirs
    for dir_name in &remaining_dirs {
        let entry = entries
            .get(dir_name)
            .ok_or_else(|| format!("Directory not found: {}", dir_name))?;
        entries = match entry {
            Entry::Dir(d) => d.entries.as_ref().ok_or("Directory has no entries")?,
            _ => return Err(format!("{} is not a directory", dir_name).into()),
        };
    }

    // Get the file entry
    let file_entry = entries
        .get(filename)
        .ok_or_else(|| format!("File not found in schema: {}", filename))?;

    match file_entry {
        Entry::Doc(doc) => doc
            .node_id
            .clone()
            .ok_or_else(|| format!("File {} has no UUID assigned", filename).into()),
        Entry::Dir(_) => Err(format!("{} is a directory, not a file", filename).into()),
    }
}
