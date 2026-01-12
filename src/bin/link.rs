//! commonplace-link: Create links to commonplace documents (like ln)
//!
//! Usage:
//!   commonplace-link source.txt link.txt
//!   commonplace-link dir1/file.txt dir2/link.txt
//!
//! Creates a link in the schema so both files point to the same document.
//! The link file is NOT created on disk - sync will materialize it on next pull.
//! Supports cross-directory linking within the same workspace, including
//! node-backed subdirectories.

use clap::Parser;
use commonplace_doc::{
    cli::LinkArgs,
    fs::{DocEntry, Entry, FsSchema},
    sync::{client::push_schema_to_server, SCHEMA_FILENAME},
    workspace::{find_workspace_root, normalize_path, split_path},
};
use reqwest::Client;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Represents which schema file a path resolves to
struct ResolvedPath {
    /// Path to the .commonplace.json file
    schema_path: PathBuf,
    /// Path components within that schema (after any node-backed directory boundary)
    dirs_within_schema: Vec<String>,
    /// The filename
    filename: String,
    /// The document ID for this schema (node_id of the containing directory)
    #[allow(dead_code)]
    doc_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = LinkArgs::parse();

    // Find the workspace root by searching up for .commonplace.json
    let cwd = std::env::current_dir()?;
    let (workspace_root, _root_schema_path) = find_workspace_root(&cwd)?;

    // Convert source and target to workspace-relative paths
    let source_path = normalize_path(&args.source, &cwd, &workspace_root)?;
    let target_path = normalize_path(&args.target, &cwd, &workspace_root)?;

    // Split paths into directory components and filename
    let (source_dirs, source_name) = split_path(&source_path)?;
    let (target_dirs, target_name) = split_path(&target_path)?;

    // Resolve which schema files contain source and target
    let source_resolved = resolve_schema_path(&workspace_root, &source_dirs, &source_name)?;
    let target_resolved = resolve_schema_path(&workspace_root, &target_dirs, &target_name)?;

    // Track which schemas need to be saved
    let mut schemas_to_save: HashMap<PathBuf, FsSchema> = HashMap::new();

    // Get the source entry and its node_id
    let (node_id, content_type) = {
        let schema = get_or_load_schema(&mut schemas_to_save, &source_resolved.schema_path)?;
        get_or_create_node_id(
            schema,
            &source_resolved.dirs_within_schema,
            &source_resolved.filename,
        )?
    };

    // Create/update the target entry with the same node_id
    {
        let schema = get_or_load_schema(&mut schemas_to_save, &target_resolved.schema_path)?;
        set_entry_node_id(
            schema,
            &target_resolved.dirs_within_schema,
            &target_resolved.filename,
            &node_id,
            content_type,
        )?;
    }

    // Write all modified schemas atomically and collect for server push
    let mut schemas_to_push: Vec<(String, String)> = Vec::new(); // (doc_id, json_content)

    for (schema_path, schema) in &schemas_to_save {
        let temp_path = schema_path.with_extension("json.tmp");
        let new_schema_content = serde_json::to_string_pretty(schema)?;
        fs::write(&temp_path, &new_schema_content)?;
        fs::rename(&temp_path, schema_path)?;

        // Determine the doc_id for this schema
        let doc_id = get_doc_id_for_schema(&workspace_root, schema_path)?;
        schemas_to_push.push((doc_id, new_schema_content));
    }

    println!(
        "Created link: {} -> {} (node_id: {})",
        target_path, source_path, node_id
    );

    // Push schemas to server
    let client = Client::new();
    for (doc_id, schema_json) in &schemas_to_push {
        match push_schema_to_server(
            &client,
            &args.server,
            doc_id,
            schema_json,
            "commonplace-link",
        )
        .await
        {
            Ok(()) => println!("Pushed schema to server ({})", doc_id),
            Err(e) => eprintln!("Warning: Failed to push schema to server: {}", e),
        }
    }

    Ok(())
}

/// Determine the document ID for a schema file
fn get_doc_id_for_schema(
    workspace_root: &Path,
    schema_path: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let schema_dir = schema_path
        .parent()
        .ok_or("Schema has no parent directory")?;

    if schema_dir == workspace_root {
        // This is the root schema - use the workspace directory name as fs_root_id
        let dir_name = workspace_root
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("workspace");
        return Ok(dir_name.to_string());
    }

    // This is a subdirectory schema - find its node_id from the parent schema
    let parent_schema_path = find_parent_schema(workspace_root, schema_dir)?;
    let parent_schema_content = fs::read_to_string(&parent_schema_path)?;
    let parent_schema: FsSchema = serde_json::from_str(&parent_schema_content)?;

    let subdir_name = schema_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("Cannot get subdirectory name")?;

    // Look up the node_id for this subdirectory in the parent schema
    if let Some(Entry::Dir(root)) = &parent_schema.root {
        if let Some(entries) = &root.entries {
            if let Some(Entry::Dir(dir)) = entries.get(subdir_name) {
                if let Some(node_id) = &dir.node_id {
                    return Ok(node_id.clone());
                }
            }
        }
    }

    Err(format!(
        "Could not find node_id for subdirectory {} in parent schema",
        subdir_name
    )
    .into())
}

/// Find the parent schema file for a given directory
fn find_parent_schema(
    workspace_root: &Path,
    dir: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut current = dir.parent().ok_or("Directory has no parent")?;

    loop {
        let schema_path = current.join(SCHEMA_FILENAME);
        if schema_path.exists() {
            return Ok(schema_path);
        }
        if current == workspace_root {
            return Err("Could not find parent schema".into());
        }
        current = current
            .parent()
            .ok_or("Reached filesystem root without finding schema")?;
    }
}

/// Resolve which schema file a path belongs to, handling node-backed directories
fn resolve_schema_path(
    workspace_root: &Path,
    dirs: &[String],
    filename: &str,
) -> Result<ResolvedPath, Box<dyn std::error::Error>> {
    let root_schema_path = workspace_root.join(SCHEMA_FILENAME);

    if dirs.is_empty() {
        // File is directly in workspace root
        return Ok(ResolvedPath {
            schema_path: root_schema_path,
            dirs_within_schema: vec![],
            filename: filename.to_string(),
            doc_id: None, // Computed later via get_doc_id_for_schema
        });
    }

    // Load root schema to check for node-backed directories
    let root_schema_content = fs::read_to_string(&root_schema_path)?;
    let root_schema: FsSchema = serde_json::from_str(&root_schema_content)
        .map_err(|e| format!("Failed to parse root schema: {}", e))?;

    // Walk through directories to find if any is node-backed
    let mut current_dir_path = workspace_root.to_path_buf();
    let mut current_schema = root_schema;
    let mut schema_path = root_schema_path;
    let mut remaining_dirs: Vec<String> = dirs.to_vec();

    let root = current_schema
        .root
        .as_ref()
        .ok_or("Schema has no root directory")?;
    let mut current_entries = match root {
        Entry::Dir(dir) => dir.entries.as_ref(),
        _ => return Err("Root is not a directory".into()),
    };

    let mut i = 0;
    while i < remaining_dirs.len() {
        let dir_name = &remaining_dirs[i];

        let entries = current_entries.ok_or_else(|| {
            format!(
                "Directory entries are None but we haven't found .commonplace.json for {}",
                dir_name
            )
        })?;

        let entry = entries
            .get(dir_name)
            .ok_or_else(|| format!("Directory not found in schema: {}", dir_name))?;

        match entry {
            Entry::Dir(dir) => {
                if dir.entries.is_none() && dir.node_id.is_some() {
                    // This is a node-backed directory!
                    // Check if it has its own .commonplace.json
                    current_dir_path = current_dir_path.join(dir_name);
                    let subdir_schema_path = current_dir_path.join(SCHEMA_FILENAME);

                    if subdir_schema_path.exists() {
                        // Switch to this schema
                        let subdir_schema_content = fs::read_to_string(&subdir_schema_path)?;
                        current_schema = serde_json::from_str(&subdir_schema_content)
                            .map_err(|e| format!("Failed to parse {} schema: {}", dir_name, e))?;

                        schema_path = subdir_schema_path;
                        remaining_dirs = remaining_dirs[i + 1..].to_vec();
                        i = 0;

                        let new_root = current_schema
                            .root
                            .as_ref()
                            .ok_or("Subdir schema has no root")?;
                        current_entries = match new_root {
                            Entry::Dir(d) => d.entries.as_ref(),
                            _ => return Err("Subdir root is not a directory".into()),
                        };
                        continue;
                    } else {
                        return Err(format!(
                            "Node-backed directory {} has no {} file",
                            dir_name, SCHEMA_FILENAME
                        )
                        .into());
                    }
                } else {
                    // Regular directory with inline entries
                    current_dir_path = current_dir_path.join(dir_name);
                    current_entries = dir.entries.as_ref();
                }
            }
            _ => return Err(format!("{} is not a directory", dir_name).into()),
        }
        i += 1;
    }

    Ok(ResolvedPath {
        schema_path,
        dirs_within_schema: remaining_dirs,
        filename: filename.to_string(),
        doc_id: None, // Computed later via get_doc_id_for_schema
    })
}

/// Get a schema from cache or load it from disk
fn get_or_load_schema<'a>(
    cache: &'a mut HashMap<PathBuf, FsSchema>,
    schema_path: &PathBuf,
) -> Result<&'a mut FsSchema, Box<dyn std::error::Error>> {
    if !cache.contains_key(schema_path) {
        let content = fs::read_to_string(schema_path)?;
        let schema: FsSchema = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse schema at {:?}: {}", schema_path, e))?;
        cache.insert(schema_path.clone(), schema);
    }
    Ok(cache.get_mut(schema_path).unwrap())
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
                .ok_or_else(|| format!("Directory {} has no entries", dir_name))?,
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
