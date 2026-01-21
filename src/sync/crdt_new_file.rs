//! Local UUID generation and new file handling for CRDT peer sync.
//!
//! This module implements local file creation without server round-trips:
//! - Generate UUIDv4 locally for new files
//! - Update schema Y.Doc with new file entry
//! - Create and publish commits via MQTT
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use crate::commit::Commit;
use crate::fs::{DirEntry, DocEntry, Entry, FsSchema};
use crate::mqtt::{EditMessage, MqttClient, Topic};
use crate::sync::crdt_state::{CrdtPeerState, DirectorySyncState};
use base64::{engine::general_purpose::STANDARD, Engine};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};
use uuid::Uuid;
use yrs::{Doc, GetString, Text, Transact, WriteTxn};

/// Result of creating a new file locally.
#[derive(Debug)]
pub struct NewFileResult {
    /// The generated UUID for the file
    pub uuid: Uuid,
    /// The schema commit CID
    pub schema_cid: String,
    /// The file content commit CID
    pub file_cid: String,
}

/// Generate a new UUIDv4 for a file.
///
/// This replaces the server round-trip for UUID generation.
pub fn generate_file_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Create a new file locally with a generated UUID.
///
/// This implements the new file creation flow:
/// 1. Generate UUIDv4 locally
/// 2. Update schema state with new file entry
/// 3. Create file state with initial content
/// 4. Publish schema commit via MQTT
/// 5. Publish file content commit via MQTT
///
/// Returns the new file's UUID and commit CIDs.
pub async fn create_new_file(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    dir_state: &mut DirectorySyncState,
    filename: &str,
    content: &str,
    author: &str,
) -> Result<NewFileResult, String> {
    // 1. Generate UUID locally
    let file_uuid = generate_file_uuid();
    let file_uuid_str = file_uuid.to_string();

    info!(
        "Creating new file '{}' with local UUID: {}",
        filename, file_uuid_str
    );

    // 2. Update schema state - add new file entry
    let schema_cid = update_schema_with_new_file(
        mqtt_client,
        workspace,
        &mut dir_state.schema,
        filename,
        &file_uuid_str,
        author,
    )
    .await?;

    // 3. Create file state with initial content
    let file_state = dir_state.get_or_create_file(filename, file_uuid);

    // 4. Publish file content commit
    let file_cid = publish_file_content(
        mqtt_client,
        workspace,
        &file_uuid_str,
        file_state,
        content,
        author,
    )
    .await?;

    info!(
        "New file '{}' created: uuid={}, schema_cid={}, file_cid={}",
        filename, file_uuid_str, schema_cid, file_cid
    );

    Ok(NewFileResult {
        uuid: file_uuid,
        schema_cid,
        file_cid,
    })
}

/// Update schema Y.Doc with a new file entry and publish commit.
async fn update_schema_with_new_file(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    schema_state: &mut CrdtPeerState,
    filename: &str,
    file_uuid: &str,
    author: &str,
) -> Result<String, String> {
    // Load schema Y.Doc
    let doc = schema_state.to_doc()?;

    // Update schema content
    // For now, we store schema as JSON text in a "content" field
    // In a full implementation, this would use Yjs Map/Array types
    let update_bytes = {
        let mut txn = doc.transact_mut();
        let text = txn.get_or_insert_text("content");

        // Get current schema JSON
        let current_json = text.get_string(&txn);
        let mut schema: FsSchema = if current_json.is_empty() {
            FsSchema {
                version: 1,
                root: None,
            }
        } else {
            serde_json::from_str(&current_json)
                .map_err(|e| format!("Failed to parse schema: {}", e))?
        };

        // Add new file entry to schema
        add_file_to_schema(&mut schema, filename, file_uuid);

        // Serialize updated schema
        let new_json = serde_json::to_string_pretty(&schema)
            .map_err(|e| format!("Failed to serialize schema: {}", e))?;

        // Update Y.Doc
        let len = text.len(&txn);
        if len > 0 {
            text.remove_range(&mut txn, 0, len);
        }
        text.insert(&mut txn, 0, &new_json);

        txn.encode_update_v1()
    };

    // Create and publish commit
    let parents = match &schema_state.local_head_cid {
        Some(cid) => vec![cid.clone()],
        None => vec![],
    };

    let update_b64 = STANDARD.encode(&update_bytes);
    let commit = Commit::new(
        parents.clone(),
        update_b64.clone(),
        author.to_string(),
        None,
    );
    let cid = commit.calculate_cid();

    // Update state
    schema_state.local_head_cid = Some(cid.clone());
    schema_state.update_from_doc(&doc);

    // Publish via MQTT
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: Some(format!("Add file: {}", filename)),
        timestamp,
    };

    let schema_node_id = schema_state.node_id.to_string();
    let topic = Topic::edits(workspace, &schema_node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)
        .map_err(|e| format!("Failed to serialize schema edit: {}", e))?;

    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("Failed to publish schema edit: {}", e))?;

    debug!(
        "Published schema commit {} for new file '{}'",
        cid, filename
    );

    Ok(cid)
}

/// Add a file entry to the schema.
fn add_file_to_schema(schema: &mut FsSchema, filename: &str, file_uuid: &str) {
    // Ensure root directory exists
    if schema.root.is_none() {
        schema.root = Some(Entry::Dir(DirEntry {
            node_id: None,
            entries: Some(std::collections::HashMap::new()),
            content_type: None,
        }));
    }

    // Add file to root entries
    if let Some(Entry::Dir(ref mut dir)) = schema.root {
        let entries = dir
            .entries
            .get_or_insert_with(std::collections::HashMap::new);
        entries.insert(
            filename.to_string(),
            Entry::Doc(DocEntry {
                node_id: Some(file_uuid.to_string()),
                content_type: None,
            }),
        );
    }
}

/// Publish file content as initial commit.
async fn publish_file_content(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    file_uuid: &str,
    file_state: &mut CrdtPeerState,
    content: &str,
    author: &str,
) -> Result<String, String> {
    // Create Y.Doc with content
    let doc = Doc::new();
    let update_bytes = {
        let mut txn = doc.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, content);
        txn.encode_update_v1()
    };

    // Create commit (no parents for initial commit)
    let update_b64 = STANDARD.encode(&update_bytes);
    let commit = Commit::new(vec![], update_b64.clone(), author.to_string(), None);
    let cid = commit.calculate_cid();

    // Update state
    file_state.local_head_cid = Some(cid.clone());
    file_state.update_from_doc(&doc);

    // Publish via MQTT
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: update_b64,
        parents: vec![],
        author: author.to_string(),
        message: Some("Initial content".to_string()),
        timestamp,
    };

    let topic = Topic::edits(workspace, file_uuid).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)
        .map_err(|e| format!("Failed to serialize file edit: {}", e))?;

    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("Failed to publish file edit: {}", e))?;

    debug!(
        "Published initial content commit {} for file {}",
        cid, file_uuid
    );

    Ok(cid)
}

/// Remove a file from the schema and publish the update.
pub async fn remove_file_from_schema(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    dir_state: &mut DirectorySyncState,
    filename: &str,
    author: &str,
) -> Result<String, String> {
    // Load schema Y.Doc
    let doc = dir_state.schema.to_doc()?;

    // Update schema content
    let update_bytes = {
        let mut txn = doc.transact_mut();
        let text = txn.get_or_insert_text("content");

        // Get current schema JSON
        let current_json = text.get_string(&txn);
        let mut schema: FsSchema = if current_json.is_empty() {
            return Err("Schema is empty, cannot remove file".to_string());
        } else {
            serde_json::from_str(&current_json)
                .map_err(|e| format!("Failed to parse schema: {}", e))?
        };

        // Remove file from schema
        if let Some(Entry::Dir(ref mut dir)) = schema.root {
            if let Some(ref mut entries) = dir.entries {
                entries.remove(filename);
            }
        }

        // Serialize updated schema
        let new_json = serde_json::to_string_pretty(&schema)
            .map_err(|e| format!("Failed to serialize schema: {}", e))?;

        // Update Y.Doc
        let len = text.len(&txn);
        if len > 0 {
            text.remove_range(&mut txn, 0, len);
        }
        text.insert(&mut txn, 0, &new_json);

        txn.encode_update_v1()
    };

    // Create and publish commit
    let parents = match &dir_state.schema.local_head_cid {
        Some(cid) => vec![cid.clone()],
        None => vec![],
    };

    let update_b64 = STANDARD.encode(&update_bytes);
    let commit = Commit::new(
        parents.clone(),
        update_b64.clone(),
        author.to_string(),
        None,
    );
    let cid = commit.calculate_cid();

    // Update schema state
    dir_state.schema.local_head_cid = Some(cid.clone());
    dir_state.schema.update_from_doc(&doc);

    // Get schema node_id before removing file
    let schema_node_id = dir_state.schema.node_id.to_string();

    // Also remove from file states
    dir_state.remove_file(filename);

    // Publish via MQTT
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: Some(format!("Remove file: {}", filename)),
        timestamp,
    };

    let topic = Topic::edits(workspace, &schema_node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)
        .map_err(|e| format!("Failed to serialize schema edit: {}", e))?;

    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("Failed to publish schema edit: {}", e))?;

    info!(
        "Published schema commit {} to remove file '{}'",
        cid, filename
    );

    Ok(cid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_empty_schema() -> FsSchema {
        FsSchema {
            version: 1,
            root: None,
        }
    }

    #[test]
    fn test_generate_file_uuid() {
        let uuid1 = generate_file_uuid();
        let uuid2 = generate_file_uuid();

        // Should generate valid UUIDs
        assert!(!uuid1.is_nil());
        assert!(!uuid2.is_nil());

        // Should be unique
        assert_ne!(uuid1, uuid2);

        // Should be v4
        assert_eq!(uuid1.get_version_num(), 4);
    }

    #[test]
    fn test_add_file_to_schema_empty() {
        let mut schema = new_empty_schema();
        add_file_to_schema(&mut schema, "test.txt", "uuid-123");

        assert!(schema.root.is_some());
        if let Some(Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert!(entries.contains_key("test.txt"));
            if let Some(Entry::Doc(doc)) = entries.get("test.txt") {
                assert_eq!(doc.node_id, Some("uuid-123".to_string()));
            } else {
                panic!("Expected Doc entry");
            }
        } else {
            panic!("Expected Dir entry");
        }
    }

    #[test]
    fn test_add_file_to_schema_existing() {
        let mut schema = new_empty_schema();

        // Add first file
        add_file_to_schema(&mut schema, "file1.txt", "uuid-1");

        // Add second file
        add_file_to_schema(&mut schema, "file2.txt", "uuid-2");

        if let Some(Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key("file1.txt"));
            assert!(entries.contains_key("file2.txt"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    #[test]
    fn test_schema_serialization_roundtrip() {
        let mut schema = new_empty_schema();
        add_file_to_schema(&mut schema, "test.txt", "uuid-123");

        let json = serde_json::to_string(&schema).unwrap();
        let parsed: FsSchema = serde_json::from_str(&json).unwrap();

        if let Some(Entry::Dir(dir)) = &parsed.root {
            let entries = dir.entries.as_ref().unwrap();
            assert!(entries.contains_key("test.txt"));
        } else {
            panic!("Expected Dir entry");
        }
    }
}
