//! Schema I/O operations for reading and writing .commonplace.json files.
//!
//! This module handles writing schema files to disk with deduplication
//! and feedback loop prevention.

use crate::fs::{Entry, FsSchema};
use crate::sync::{encode_node_id, HeadResponse, WrittenSchemas};
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

/// Schema filename constant.
pub const SCHEMA_FILENAME: &str = ".commonplace.json";

/// Result of fetching and validating a schema from the server.
pub struct FetchedSchema {
    /// The parsed schema.
    pub schema: FsSchema,
    /// The raw JSON content from the server.
    pub content: String,
    /// The commit ID from the HEAD response (if any).
    pub cid: Option<String>,
}

/// Fetch a schema from the server, parse it, and optionally validate it.
///
/// Returns `None` if:
/// - The fetch fails
/// - The content is empty
/// - Parsing fails
/// - `require_valid_root` is true and schema doesn't have a valid root
///
/// A valid root is a directory with either entries or a node_id.
pub async fn fetch_and_validate_schema(
    client: &Client,
    server: &str,
    node_id: &str,
    require_valid_root: bool,
) -> Option<FetchedSchema> {
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(node_id));
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to fetch schema for {}: {}", node_id, e);
            return None;
        }
    };

    if !resp.status().is_success() {
        warn!(
            "Failed to fetch schema HEAD for {}: status {}",
            node_id,
            resp.status()
        );
        return None;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            warn!("Failed to parse HEAD response for {}: {}", node_id, e);
            return None;
        }
    };

    if head.content.is_empty() {
        debug!("Schema content is empty for {}", node_id);
        return None;
    }

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse schema for {}: {}", node_id, e);
            return None;
        }
    };

    if require_valid_root {
        let has_valid_root = match &schema.root {
            Some(Entry::Dir(dir)) => dir.entries.is_some() || dir.node_id.is_some(),
            _ => false,
        };
        if !has_valid_root {
            warn!(
                "Schema for {} has no valid root (missing entries or node_id)",
                node_id
            );
            return None;
        }
    }

    Some(FetchedSchema {
        schema,
        content: head.content,
        cid: head.cid,
    })
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

/// Write nested schemas for node-backed directories.
///
/// Traverses the schema tree and writes .commonplace.json files for each
/// node-backed subdirectory, fetching their content from the server.
pub async fn write_nested_schemas(
    client: &Client,
    server: &str,
    directory: &Path,
    schema: &FsSchema,
    written_schemas: Option<&WrittenSchemas>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref root) = schema.root {
        // Track processed node_ids with their content hashes to prevent redundant fetches
        let mut processed_hashes: HashMap<String, String> = HashMap::new();
        write_nested_schemas_recursive(
            client,
            server,
            directory,
            root,
            directory,
            &mut processed_hashes,
            written_schemas,
        )
        .await?;
    }
    Ok(())
}

/// Recursively traverse the schema and write nested schemas for node-backed directories.
///
/// Uses a hash map to track already-processed node_ids and their content hashes,
/// preventing redundant fetches and writes within a single traversal.
#[async_recursion::async_recursion]
async fn write_nested_schemas_recursive(
    client: &Client,
    server: &str,
    _base_dir: &Path,
    entry: &Entry,
    current_dir: &Path,
    processed_hashes: &mut HashMap<String, String>,
    written_schemas: Option<&WrittenSchemas>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "write_nested_schemas_recursive: current_dir={:?}, entry_type={:?}",
        current_dir,
        match entry {
            Entry::Dir(_) => "dir",
            Entry::Doc(_) => "doc",
        }
    );

    if let Entry::Dir(dir) = entry {
        // If this directory has entries, iterate over them to find node-backed subdirectories
        if let Some(ref entries) = dir.entries {
            info!(
                "write_nested_schemas_recursive: processing {} entries in {:?}",
                entries.len(),
                current_dir
            );
            for (name, child_entry) in entries {
                if let Entry::Dir(child_dir) = child_entry {
                    if let Some(ref node_id) = child_dir.node_id {
                        info!(
                            "write_nested_schemas_recursive: found node-backed dir '{}' with node_id={}",
                            name, node_id
                        );

                        // Skip if we've already processed this node_id in this traversal
                        if processed_hashes.contains_key(node_id) {
                            debug!(
                                "Skipping already-processed subdirectory schema: {}",
                                node_id
                            );
                            continue;
                        }

                        // Calculate the subdirectory path
                        let subdir_path = current_dir.join(name);

                        // Fetch the directory's schema from server
                        let head_url = format!("{}/docs/{}/head", server, encode_node_id(node_id));
                        info!(
                            "write_nested_schemas_recursive: fetching schema from {}",
                            head_url
                        );
                        match client.get(&head_url).send().await {
                            Ok(resp) => {
                                if resp.status().is_success() {
                                    match resp.json::<HeadResponse>().await {
                                        Ok(head) => {
                                            // Compute content hash and check if content has changed
                                            let content_hash =
                                                crate::sync::state_file::compute_content_hash(
                                                    head.content.as_bytes(),
                                                );

                                            // Mark as processed with current hash
                                            processed_hashes.insert(node_id.clone(), content_hash);

                                            // Create directory for node-backed subdirectory (even if schema is empty)
                                            if !subdir_path.exists() {
                                                info!(
                                                    "Creating directory for node-backed subdirectory: {:?}",
                                                    subdir_path
                                                );
                                                if let Err(e) =
                                                    tokio::fs::create_dir_all(&subdir_path).await
                                                {
                                                    warn!(
                                                        "Failed to create directory for nested schema {:?}: {}",
                                                        subdir_path, e
                                                    );
                                                }
                                            } else {
                                                debug!(
                                                    "Directory already exists: {:?}",
                                                    subdir_path
                                                );
                                            }

                                            // Only write schema if content is valid (not just "{}")
                                            if !head.content.is_empty() && head.content != "{}" {
                                                info!(
                                                    "write_nested_schemas_recursive: parsing schema for {} (content len: {})",
                                                    node_id, head.content.len()
                                                );
                                                match serde_json::from_str::<FsSchema>(
                                                    &head.content,
                                                ) {
                                                    Ok(sub_schema) => {
                                                        info!(
                                                            "write_nested_schemas_recursive: parsed schema, root={:?}",
                                                            sub_schema.root.is_some()
                                                        );
                                                        // Write the schema to the subdirectory (write_schema_file has its own dedup)
                                                        if subdir_path.exists() {
                                                            if let Err(e) = write_schema_file(
                                                                &subdir_path,
                                                                &head.content,
                                                                written_schemas,
                                                            )
                                                            .await
                                                            {
                                                                warn!(
                                                                    "Failed to write nested schema to {:?}: {}",
                                                                    subdir_path, e
                                                                );
                                                            }
                                                        }
                                                        // Recursively handle any nested node-backed directories
                                                        if let Some(ref sub_root) = sub_schema.root
                                                        {
                                                            info!(
                                                                "write_nested_schemas_recursive: recursing into {:?}",
                                                                subdir_path
                                                            );
                                                            write_nested_schemas_recursive(
                                                                client,
                                                                server,
                                                                _base_dir,
                                                                sub_root,
                                                                &subdir_path,
                                                                processed_hashes,
                                                                written_schemas,
                                                            )
                                                            .await?;
                                                        } else {
                                                            info!(
                                                                "write_nested_schemas_recursive: no root in schema for {}",
                                                                node_id
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        warn!(
                                                            "Failed to parse schema for {}: {} (content: {})",
                                                            node_id, e, &head.content[..100.min(head.content.len())]
                                                        );
                                                    }
                                                }
                                            } else {
                                                debug!(
                                                    "Schema for {} is empty, skipping recursion",
                                                    node_id
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to parse HEAD response for {}: {}",
                                                node_id, e
                                            );
                                        }
                                    }
                                } else {
                                    warn!(
                                        "Failed to fetch schema for {}: status {}",
                                        node_id,
                                        resp.status()
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Failed to fetch schema for {}: {}", node_id, e);
                            }
                        }
                    }
                }
            }
        } else {
            debug!(
                "write_nested_schemas_recursive: no entries in {:?}",
                current_dir
            );
        }
    }
    Ok(())
}
