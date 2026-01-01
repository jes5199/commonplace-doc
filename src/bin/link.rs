//! commonplace-link: Create links to commonplace documents (like ln)
//!
//! Usage:
//!   commonplace-link source.txt link.txt
//!   commonplace-link dir1/file.txt dir2/link.txt
//!
//! Creates a link in the schema so both files point to the same document.
//! The link file is NOT created on disk - sync will materialize it on next pull.
//! Supports cross-directory linking within the same workspace.

use clap::Parser;
use commonplace_doc::{
    cli::LinkArgs,
    fs::{DocEntry, Entry, FsSchema},
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Schema filename used to identify synced directories
const SCHEMA_FILENAME: &str = ".commonplace.json";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = LinkArgs::parse();

    // Find the workspace root by searching up for .commonplace.json
    let cwd = std::env::current_dir()?;
    let (workspace_root, schema_path) = find_workspace_root(&cwd)?;

    // Convert source and target to workspace-relative paths
    let source_path = normalize_path(&args.source, &cwd, &workspace_root)?;
    let target_path = normalize_path(&args.target, &cwd, &workspace_root)?;

    // Read and parse the schema
    let schema_content = fs::read_to_string(&schema_path)?;
    let mut schema: FsSchema = serde_json::from_str(&schema_content)
        .map_err(|e| format!("Failed to parse schema: {}", e))?;

    // Split paths into directory components and filename
    let (source_dirs, source_name) = split_path(&source_path)?;
    let (target_dirs, target_name) = split_path(&target_path)?;

    // Get the source entry and its node_id
    let (node_id, content_type) = get_or_create_node_id(&mut schema, &source_dirs, &source_name)?;

    // Create/update the target entry with the same node_id
    set_entry_node_id(
        &mut schema,
        &target_dirs,
        &target_name,
        &node_id,
        content_type,
    )?;

    // Write schema atomically (temp file + rename)
    let temp_path = schema_path.with_extension("json.tmp");
    let new_schema_content = serde_json::to_string_pretty(&schema)?;
    fs::write(&temp_path, &new_schema_content)?;
    fs::rename(&temp_path, &schema_path)?;

    println!(
        "Created link: {} -> {} (node_id: {})",
        target_path, source_path, node_id
    );
    println!("Run sync to materialize the file on disk.");

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
    // Make the path absolute
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };

    // Strip the workspace root to get relative path
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

/// Navigate to a directory in the schema, returning its entries map
fn navigate_to_dir<'a>(
    schema: &'a mut FsSchema,
    dirs: &[String],
) -> Result<&'a mut HashMap<String, Entry>, Box<dyn std::error::Error>> {
    let root = schema.root.as_mut().ok_or("Schema has no root directory")?;

    let mut current_entries = match root {
        Entry::Dir(dir) => dir
            .entries
            .as_mut()
            .ok_or("Root directory has no entries")?,
        _ => return Err("Root is not a directory".into()),
    };

    for dir_name in dirs {
        let entry = current_entries
            .get_mut(dir_name)
            .ok_or_else(|| format!("Directory not found in schema: {}", dir_name))?;

        current_entries = match entry {
            Entry::Dir(dir) => dir
                .entries
                .as_mut()
                .ok_or_else(|| format!("Directory {} has no entries (node-backed?)", dir_name))?,
            _ => return Err(format!("{} is not a directory", dir_name).into()),
        };
    }

    Ok(current_entries)
}

/// Get or create a node_id for a source file
fn get_or_create_node_id(
    schema: &mut FsSchema,
    dirs: &[String],
    filename: &str,
) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let entries = navigate_to_dir(schema, dirs)?;

    let entry = entries
        .get_mut(filename)
        .ok_or_else(|| format!("Source file not found in schema: {}", filename))?;

    match entry {
        Entry::Doc(doc) => {
            let content_type = doc.content_type.clone();
            let node_id = if let Some(id) = &doc.node_id {
                id.clone()
            } else {
                // Generate a new UUID for the source
                let new_id = uuid::Uuid::new_v4().to_string();
                doc.node_id = Some(new_id.clone());
                new_id
            };
            Ok((node_id, content_type))
        }
        Entry::Dir(_) => Err("Cannot link to a directory, only documents".into()),
    }
}

/// Set a node_id on a target file (creating the entry if needed)
fn set_entry_node_id(
    schema: &mut FsSchema,
    dirs: &[String],
    filename: &str,
    node_id: &str,
    content_type: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = navigate_to_dir(schema, dirs)?;

    if let Some(existing) = entries.get(filename) {
        match existing {
            Entry::Doc(doc) => {
                if doc.node_id.is_some() && doc.node_id.as_deref() != Some(node_id) {
                    return Err(format!(
                        "Target {} already exists with different node_id",
                        filename
                    )
                    .into());
                }
            }
            Entry::Dir(_) => {
                return Err(format!("Target {} is a directory", filename).into());
            }
        }
    }

    // Insert or update the entry
    entries.insert(
        filename.to_string(),
        Entry::Doc(DocEntry {
            node_id: Some(node_id.to_string()),
            content_type,
        }),
    );

    Ok(())
}
