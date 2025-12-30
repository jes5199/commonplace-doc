//! commonplace-link: Create links to commonplace documents (like ln)
//!
//! Usage:
//!   commonplace-link source.txt link.txt
//!
//! Creates a link in the schema so both files point to the same document.
//! The link file is NOT created on disk - sync will materialize it on next pull.

use clap::Parser;
use commonplace_doc::{cli::LinkArgs, fs::FsSchema};
use std::fs;
use std::path::Path;

/// Schema filename used to identify synced directories
const SCHEMA_FILENAME: &str = ".commonplace.json";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = LinkArgs::parse();

    // Get the directory containing the source file
    let source_dir = args.source.parent().unwrap_or(Path::new(".")).to_path_buf();

    let source_name = args
        .source
        .file_name()
        .ok_or("Source path has no filename")?
        .to_string_lossy()
        .to_string();

    let target_name = args
        .target
        .file_name()
        .ok_or("Target path has no filename")?
        .to_string_lossy()
        .to_string();

    // Check that source and target are in the same directory
    let target_dir = args.target.parent().unwrap_or(Path::new(".")).to_path_buf();

    // Normalize the directories for comparison
    let source_dir_canonical = if source_dir.as_os_str().is_empty() {
        std::env::current_dir()?
    } else {
        source_dir.canonicalize().unwrap_or(source_dir.clone())
    };

    let target_dir_canonical = if target_dir.as_os_str().is_empty() {
        std::env::current_dir()?
    } else {
        target_dir.canonicalize().unwrap_or(target_dir.clone())
    };

    if source_dir_canonical != target_dir_canonical {
        return Err(
            "Cannot link across directories: source and target must be in the same directory"
                .into(),
        );
    }

    // Find the .commonplace.json file
    let schema_path = source_dir_canonical.join(SCHEMA_FILENAME);
    if !schema_path.exists() {
        return Err(format!(
            "Not in a commonplace sync directory: {} not found",
            schema_path.display()
        )
        .into());
    }

    // Read and parse the schema
    let schema_content = fs::read_to_string(&schema_path)?;
    let mut schema: FsSchema = serde_json::from_str(&schema_content)
        .map_err(|e| format!("Failed to parse schema: {}", e))?;

    // Get the root directory entries
    let root = schema.root.as_mut().ok_or("Schema has no root directory")?;

    let entries = match root {
        commonplace_doc::fs::Entry::Dir(dir) => dir
            .entries
            .as_mut()
            .ok_or("Root directory has no entries")?,
        _ => return Err("Root is not a directory".into()),
    };

    // Find the source entry
    let source_entry = entries
        .get(&source_name)
        .ok_or_else(|| format!("Source file not found in schema: {}", source_name))?;

    // Get the source node_id (or generate one if needed)
    let node_id = match source_entry {
        commonplace_doc::fs::Entry::Doc(doc) => {
            if let Some(id) = &doc.node_id {
                id.clone()
            } else {
                // Generate a new UUID for the source
                uuid::Uuid::new_v4().to_string()
            }
        }
        commonplace_doc::fs::Entry::Dir(_) => {
            return Err("Cannot link to a directory, only documents".into());
        }
    };

    // Check that target doesn't already exist
    if entries.contains_key(&target_name) {
        return Err(format!("Target already exists: {}", target_name).into());
    }

    // Get content_type from source (if any)
    let content_type = match source_entry {
        commonplace_doc::fs::Entry::Doc(doc) => doc.content_type.clone(),
        _ => None,
    };

    // Update source entry with node_id if it didn't have one
    if let Some(commonplace_doc::fs::Entry::Doc(doc)) = entries.get_mut(&source_name) {
        if doc.node_id.is_none() {
            doc.node_id = Some(node_id.clone());
        }
    }

    // Create the target entry with the same node_id
    let target_entry = commonplace_doc::fs::Entry::Doc(commonplace_doc::fs::DocEntry {
        node_id: Some(node_id.clone()),
        content_type,
    });

    // Add target to entries
    entries.insert(target_name.clone(), target_entry);

    // Write schema atomically (temp file + rename)
    let temp_path = schema_path.with_extension("json.tmp");
    let new_schema_content = serde_json::to_string_pretty(&schema)?;
    fs::write(&temp_path, &new_schema_content)?;
    fs::rename(&temp_path, &schema_path)?;

    println!(
        "Created link: {} -> {} (node_id: {})",
        target_name, source_name, node_id
    );
    println!("Run sync to materialize the file on disk.");

    Ok(())
}
