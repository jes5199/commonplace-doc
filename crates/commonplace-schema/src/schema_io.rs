//! Schema I/O operations for reading and writing .commonplace.json files.
//!
//! This module handles writing schema files to disk with deduplication
//! and feedback loop prevention.

use commonplace_types::fs::{Entry, FsSchema};
use commonplace_watcher::WrittenSchemas;
use std::path::Path;
use tracing::{debug, info, warn};

pub use commonplace_types::SCHEMA_FILENAME;

/// Result of fetching and validating a schema from the server.
pub struct FetchedSchema {
    /// The parsed schema.
    pub schema: FsSchema,
    /// The raw JSON content from the server.
    pub content: String,
    /// The commit ID from the HEAD response (if any).
    pub cid: Option<String>,
}

/// Write a schema file to disk with deduplication.
///
/// Compares the new schema with the existing file (if any) as parsed JSON
/// to avoid unnecessary writes that could cause feedback loops.
pub async fn write_schema_file(
    directory: &Path,
    schema_json: &str,
    written_schemas: Option<&WrittenSchemas>,
) -> Result<(), std::io::Error> {
    let schema_path = directory.join(SCHEMA_FILENAME);

    // Check if existing schema is the same (prevents feedback loops)
    if schema_path.exists() {
        if let Ok(existing) = tokio::fs::read_to_string(&schema_path).await {
            // Compare as parsed JSON to ignore whitespace/formatting differences
            let existing_parsed: Result<serde_json::Value, _> = serde_json::from_str(&existing);
            let new_parsed: Result<serde_json::Value, _> = serde_json::from_str(schema_json);
            if let (Ok(existing_json), Ok(new_json)) = (existing_parsed, new_parsed) {
                if existing_json == new_json {
                    debug!(
                        "Schema unchanged, skipping write to {}",
                        schema_path.display()
                    );
                    // Still record in written_schemas even if we skip the write,
                    // in case the file was modified externally to match what we have
                    if let Some(ws) = written_schemas {
                        let canonical = schema_path.canonicalize().unwrap_or(schema_path.clone());
                        ws.write().await.insert(canonical, schema_json.to_string());
                    }
                    return Ok(());
                }
            }
        }
    }

    tokio::fs::write(&schema_path, schema_json).await?;
    info!("Wrote schema to {}", schema_path.display());

    // Record what we wrote for echo detection
    if let Some(ws) = written_schemas {
        let canonical = schema_path.canonicalize().unwrap_or(schema_path.clone());
        ws.write().await.insert(canonical, schema_json.to_string());
    }

    Ok(())
}

/// Create directories for node-backed subdirectories.
///
/// Traverses the schema tree and creates directories for each node-backed
/// subdirectory. Does NOT fetch or write child schemas - each subdirectory
/// manages its own `.commonplace-sync.json` state via its own sync task.
///
/// This implements the "independent directory sync" model where the parent
/// only knows the child's node_id, not its contents.
pub async fn write_nested_schemas(
    directory: &Path,
    schema: &FsSchema,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(ref root) = schema.root {
        create_nested_directories(directory, root, directory).await?;
    }
    Ok(())
}

/// Recursively create directories for node-backed subdirectories.
///
/// Only creates the directory structure - does not fetch or write schemas.
/// The subdirectory's own sync task handles its schema via `.commonplace-sync.json`.
#[async_recursion::async_recursion]
pub async fn create_nested_directories(
    _base_dir: &Path,
    entry: &Entry,
    current_dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Entry::Dir(dir) = entry {
        if let Some(ref entries) = dir.entries {
            for (name, child_entry) in entries {
                if let Entry::Dir(child_dir) = child_entry {
                    if child_dir.node_id.is_some() {
                        // Node-backed subdirectory: create the directory
                        let subdir_path = current_dir.join(name);
                        if !subdir_path.exists() {
                            debug!(
                                "Creating directory for node-backed subdirectory: {:?}",
                                subdir_path
                            );
                            if let Err(e) = tokio::fs::create_dir_all(&subdir_path).await {
                                warn!("Failed to create directory {:?}: {}", subdir_path, e);
                            }
                        }
                        // Note: We do NOT write the child's schema here.
                        // The child's own sync task handles its .commonplace-sync.json
                    }
                }
                // Recurse into directories (node-backed or not) to find nested subdirs
                if let Entry::Dir(_) = child_entry {
                    let subdir_path = current_dir.join(name);
                    if subdir_path.exists() {
                        create_nested_directories(_base_dir, child_entry, &subdir_path).await?;
                    }
                }
            }
        }
    }
    Ok(())
}
