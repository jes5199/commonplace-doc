//! Local UUID generation and new file handling for CRDT peer sync.
//!
//! This module implements local file creation without server round-trips:
//! - Generate UUIDv4 locally for new files
//! - Update schema Y.Doc with new file entry (using YMap for proper CRDT merge)
//! - Create and publish commits via MQTT
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use super::crdt_state::{CrdtPeerState, DirectorySyncState};
use super::yjs::create_yjs_jsonl_update;
use super::ymap_schema;
use crate::commit::Commit;
use crate::mqtt::{EditMessage, MqttClient, Topic};
use crate::sync::error::{SyncError, SyncResult};
use base64::{engine::general_purpose::STANDARD, Engine};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use uuid::Uuid;
use yrs::{Doc, ReadTxn, Text, Transact, WriteTxn};

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
) -> SyncResult<NewFileResult> {
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

    // 3. Schema-ready barrier: Wait for schema to propagate before sending content.
    // This prevents a race condition where peers receive content edits before they
    // know the file UUID (from schema), causing them to ignore the edit.
    // See: CP-ekqq
    // Increased from 200ms to 3000ms to allow sandbox sync to fully process schema,
    // create the file, and subscribe to MQTT topic before content is published.
    const SCHEMA_PROPAGATION_DELAY_MS: u64 = 3000;
    debug!(
        "Waiting {}ms for schema propagation before content publish (file: {}, uuid: {})",
        SCHEMA_PROPAGATION_DELAY_MS, filename, file_uuid_str
    );
    tokio::time::sleep(tokio::time::Duration::from_millis(
        SCHEMA_PROPAGATION_DELAY_MS,
    ))
    .await;
    debug!(
        "Schema propagation delay complete, publishing content for file: {}",
        filename
    );

    // 4. Create file state with initial content
    let file_state = dir_state.get_or_create_file(filename, file_uuid);

    // 5. Publish file content commit
    let file_cid = publish_file_content(
        mqtt_client,
        workspace,
        &file_uuid_str,
        file_state,
        content,
        author,
        filename,
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
) -> SyncResult<String> {
    // Load schema Y.Doc
    let doc = schema_state.to_doc()?;

    // Migrate from JSON text format if needed
    if ymap_schema::is_json_text_format(&doc) && !ymap_schema::is_ymap_format(&doc) {
        match ymap_schema::migrate_from_json_text(&doc) {
            Ok(true) => {
                info!("Migrated schema from JSON text to YMap format");
            }
            Ok(false) => {
                // No migration needed (empty or already migrated)
            }
            Err(e) => {
                warn!(
                    "Failed to migrate schema, will use YMap for new entry: {}",
                    e
                );
            }
        }
    }

    // Add file using YMap operations (provides proper CRDT merge semantics)
    ymap_schema::add_file(&doc, filename, file_uuid);

    // Get the update bytes
    let update_bytes = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
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
        req: None,
    };

    let schema_node_id = schema_state.node_id.to_string();
    let topic = Topic::edits(workspace, &schema_node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)?;

    // Trace log for debugging
    {
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/sandbox-trace.log")
        {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let pid = std::process::id();
            let _ = writeln!(file, "[{} pid={}] PUBLISH schema update for new file: filename={}, cid={}, topic={}, payload_len={}",
                timestamp, pid, filename, cid, topic, payload.len());
        }
    }

    // Use retained message so new subscribers get the latest schema state immediately.
    // This is critical for sync: subscribers may join after schema updates are published,
    // and the retained message ensures they receive the current file mappings.
    mqtt_client
        .publish_retained(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to publish schema edit: {}", e)))?;

    debug!(
        "Published schema commit {} for new file '{}' (retained)",
        cid, filename
    );

    Ok(cid)
}

/// Publish file content as initial commit.
async fn publish_file_content(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    file_uuid: &str,
    file_state: &mut CrdtPeerState,
    content: &str,
    author: &str,
    filename: &str,
) -> SyncResult<String> {
    let is_jsonl = filename.ends_with(".jsonl") || filename.ends_with(".ndjson");

    // Create Y.Doc with content
    // For JSONL files, use YArray (structured CRDT); for all others, use YText.
    let doc = Doc::new();
    let update_bytes = if is_jsonl {
        let update_b64 = create_yjs_jsonl_update(content, None)
            .map_err(|e| SyncError::crdt_state(format!("Failed to create JSONL update: {}", e)))?;
        let decoded = STANDARD
            .decode(&update_b64)
            .map_err(|e| SyncError::crdt_state(format!("Failed to decode JSONL update: {}", e)))?;
        // Apply to our doc so we can update state from it
        {
            use yrs::updates::decoder::Decode;
            let update = yrs::Update::decode_v1(&decoded).map_err(|e| {
                SyncError::crdt_state(format!("Failed to decode Yjs update: {}", e))
            })?;
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }
        decoded
    } else {
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
        req: None,
    };

    let topic = Topic::edits(workspace, file_uuid).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)?;

    // Use retained message so new subscribers get the content immediately.
    // This is critical for the sandbox sync race: the sandbox may subscribe
    // after the content edit is published, and the retained message ensures
    // it still receives the content.
    mqtt_client
        .publish_retained(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to publish file edit: {}", e)))?;

    debug!(
        "Published initial content commit {} for file {} (retained)",
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
) -> SyncResult<String> {
    // Load schema Y.Doc
    let doc = dir_state.schema.to_doc()?;

    // Migrate from JSON text format if needed
    if ymap_schema::is_json_text_format(&doc) && !ymap_schema::is_ymap_format(&doc) {
        match ymap_schema::migrate_from_json_text(&doc) {
            Ok(true) => {
                info!("Migrated schema from JSON text to YMap format");
            }
            Ok(false) => {
                // No migration needed
            }
            Err(e) => {
                warn!("Failed to migrate schema: {}", e);
            }
        }
    }

    // Check if entry exists before removing
    if ymap_schema::get_entry(&doc, filename).is_none() {
        return Err(SyncError::NotFound(format!(
            "File '{}' not found in schema",
            filename
        )));
    }

    // Remove file using YMap operations (provides proper CRDT merge semantics)
    ymap_schema::remove_entry(&doc, filename);

    // Get the update bytes
    let update_bytes = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
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
        req: None,
    };

    let topic = Topic::edits(workspace, &schema_node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)?;

    // Use retained message so new subscribers get the latest schema state immediately.
    // This is critical for sync: subscribers may join after schema updates are published,
    // and the retained message ensures they receive the current file mappings.
    mqtt_client
        .publish_retained(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to publish schema edit: {}", e)))?;

    info!(
        "Published schema commit {} to remove file '{}' (retained)",
        cid, filename
    );

    Ok(cid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::Entry;

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
    fn test_add_file_to_schema_ymap() {
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "test.txt", "uuid-123");

        let entry = ymap_schema::get_entry(&doc, "test.txt").expect("Entry should exist");
        assert_eq!(entry.entry_type, ymap_schema::SchemaEntryType::Doc);
        assert_eq!(entry.node_id, Some("uuid-123".to_string()));

        // Verify it converts to FsSchema correctly
        let fs_schema = ymap_schema::to_fs_schema(&doc);
        assert!(fs_schema.root.is_some());
        if let Some(Entry::Dir(dir)) = &fs_schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert!(entries.contains_key("test.txt"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    #[test]
    fn test_add_multiple_files_ymap() {
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "file1.txt", "uuid-1");
        ymap_schema::add_file(&doc, "file2.txt", "uuid-2");

        let entries = ymap_schema::list_entries(&doc);
        assert_eq!(entries.len(), 2);
        assert!(entries.contains_key("file1.txt"));
        assert!(entries.contains_key("file2.txt"));
    }

    #[test]
    fn test_remove_file_ymap() {
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "test.txt", "uuid-123");
        assert!(ymap_schema::get_entry(&doc, "test.txt").is_some());

        ymap_schema::remove_entry(&doc, "test.txt");
        assert!(ymap_schema::get_entry(&doc, "test.txt").is_none());
    }

    #[test]
    fn test_ymap_to_fs_schema_roundtrip() {
        let doc = Doc::new();
        ymap_schema::add_file(&doc, "test.txt", "uuid-123");
        ymap_schema::add_directory(&doc, "subdir", Some("uuid-456"));

        let fs_schema = ymap_schema::to_fs_schema(&doc);
        assert!(fs_schema.root.is_some());

        if let Some(Entry::Dir(dir)) = &fs_schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key("test.txt"));
            assert!(entries.contains_key("subdir"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    // ==========================================================================
    // Integration Tests for New File Creation and Deletion
    // ==========================================================================

    /// Test that new file creation with local UUID propagates to other peers.
    ///
    /// Scenario:
    /// 1. Client A creates a new file with local UUID
    /// 2. Client A's schema update is sent to Client B
    /// 3. Client B sees the new file with the same UUID
    #[test]
    fn test_new_file_creation_propagates() {
        use yrs::updates::decoder::Decode;
        use yrs::{Transact, Update};

        // Client A creates a new file with local UUID
        let doc_a = Doc::with_client_id(1);
        let file_uuid = generate_file_uuid();
        ymap_schema::add_file(&doc_a, "newfile.txt", &file_uuid.to_string());

        // Get update from Client A
        let update_a = {
            let txn = doc_a.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        // Client B applies the update
        let doc_b = Doc::with_client_id(2);
        {
            let update = Update::decode_v1(&update_a).expect("Should decode update");
            let mut txn = doc_b.transact_mut();
            txn.apply_update(update);
        }

        // Client B should see the new file with the same UUID
        let entry = ymap_schema::get_entry(&doc_b, "newfile.txt").expect("Entry should exist");
        assert_eq!(entry.entry_type, ymap_schema::SchemaEntryType::Doc);
        assert_eq!(entry.node_id, Some(file_uuid.to_string()));

        // Verify UUID is valid v4
        assert_eq!(file_uuid.get_version_num(), 4);
    }

    /// Test that file deletion propagates to other peers via schema update.
    ///
    /// Scenario:
    /// 1. Both clients have a file in schema
    /// 2. Client A deletes the file (removes from schema)
    /// 3. Client B receives the update
    /// 4. Client B no longer sees the file in schema
    #[test]
    fn test_file_deletion_propagates_via_schema() {
        use yrs::updates::decoder::Decode;
        use yrs::{Transact, Update};

        // Initial state: both clients have a file
        let initial_doc = Doc::with_client_id(0);
        ymap_schema::add_file(&initial_doc, "deleteme.txt", "uuid-to-delete");

        // Sync initial state to Client A
        let initial_update = {
            let txn = initial_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        let doc_a = Doc::with_client_id(1);
        {
            let update = Update::decode_v1(&initial_update).expect("Should decode");
            let mut txn = doc_a.transact_mut();
            txn.apply_update(update);
        }

        // Sync initial state to Client B
        let doc_b = Doc::with_client_id(2);
        {
            let update = Update::decode_v1(&initial_update).expect("Should decode");
            let mut txn = doc_b.transact_mut();
            txn.apply_update(update);
        }

        // Verify both clients have the file
        assert!(ymap_schema::get_entry(&doc_a, "deleteme.txt").is_some());
        assert!(ymap_schema::get_entry(&doc_b, "deleteme.txt").is_some());

        // Client A deletes the file
        ymap_schema::remove_entry(&doc_a, "deleteme.txt");

        // Get delta update from Client A (what changed since initial state)
        let delete_update = {
            let txn = doc_a.transact();
            // Encode only the changes since initial state
            let initial_sv = {
                let initial_txn = initial_doc.transact();
                initial_txn.state_vector()
            };
            txn.encode_state_as_update_v1(&initial_sv)
        };

        // Client B applies the deletion update
        {
            let update = Update::decode_v1(&delete_update).expect("Should decode");
            let mut txn = doc_b.transact_mut();
            txn.apply_update(update);
        }

        // Client B should no longer see the file
        assert!(
            ymap_schema::get_entry(&doc_b, "deleteme.txt").is_none(),
            "File should be deleted in Client B's schema"
        );

        // Client A should also not see it
        assert!(
            ymap_schema::get_entry(&doc_a, "deleteme.txt").is_none(),
            "File should be deleted in Client A's schema"
        );
    }

    /// Test concurrent file creation with different UUIDs (CRDT merge behavior).
    ///
    /// When two clients create a file with the same name concurrently,
    /// CRDT last-writer-wins applies and one UUID should win.
    #[test]
    fn test_concurrent_file_creation_same_name() {
        use yrs::updates::decoder::Decode;
        use yrs::{Transact, Update};

        // Client A creates "conflict.txt" with UUID A
        let doc_a = Doc::with_client_id(1);
        let uuid_a = generate_file_uuid();
        ymap_schema::add_file(&doc_a, "conflict.txt", &uuid_a.to_string());

        // Client B creates "conflict.txt" with UUID B (concurrently)
        let doc_b = Doc::with_client_id(2);
        let uuid_b = generate_file_uuid();
        ymap_schema::add_file(&doc_b, "conflict.txt", &uuid_b.to_string());

        // Get updates from both clients
        let update_a = {
            let txn = doc_a.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let update_b = {
            let txn = doc_b.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        // Client A receives Client B's update
        {
            let update = Update::decode_v1(&update_b).expect("Should decode");
            let mut txn = doc_a.transact_mut();
            txn.apply_update(update);
        }

        // Client B receives Client A's update
        {
            let update = Update::decode_v1(&update_a).expect("Should decode");
            let mut txn = doc_b.transact_mut();
            txn.apply_update(update);
        }

        // Both should have the file with the same UUID (CRDT convergence)
        let entry_a = ymap_schema::get_entry(&doc_a, "conflict.txt").expect("Should exist");
        let entry_b = ymap_schema::get_entry(&doc_b, "conflict.txt").expect("Should exist");

        // Both clients should converge to the same UUID
        assert_eq!(
            entry_a.node_id, entry_b.node_id,
            "Both clients should converge to same UUID after merge"
        );

        // The UUID should be one of the two original UUIDs
        let final_uuid = entry_a.node_id.as_ref().unwrap();
        assert!(
            final_uuid == &uuid_a.to_string() || final_uuid == &uuid_b.to_string(),
            "Final UUID should be one of the original UUIDs"
        );
    }

    /// Test that DirectorySyncState properly tracks file creation.
    #[test]
    fn test_directory_sync_state_tracks_new_file() {
        use super::super::DirectorySyncState;

        let mut dir_state = DirectorySyncState::new(Uuid::new_v4());

        // Generate UUID and create file state
        let file_uuid = generate_file_uuid();
        let file_state = dir_state.get_or_create_file("newfile.txt", file_uuid);

        // Verify file is tracked
        assert_eq!(file_state.node_id, file_uuid);
        assert!(dir_state.has_file("newfile.txt"));

        // Verify UUID is valid v4
        assert_eq!(file_uuid.get_version_num(), 4);

        // Update schema Y.Doc
        let doc = dir_state.schema.to_doc().unwrap();
        ymap_schema::add_file(&doc, "newfile.txt", &file_uuid.to_string());
        dir_state.schema.update_from_doc(&doc);

        // Verify schema reflects the file
        let restored_doc = dir_state.schema.to_doc().unwrap();
        let entry =
            ymap_schema::get_entry(&restored_doc, "newfile.txt").expect("File should be in schema");
        assert_eq!(entry.node_id, Some(file_uuid.to_string()));
    }

    /// Test that DirectorySyncState properly tracks file deletion.
    #[test]
    fn test_directory_sync_state_tracks_file_deletion() {
        use super::super::DirectorySyncState;

        let mut dir_state = DirectorySyncState::new(Uuid::new_v4());

        // Create a file first
        let file_uuid = generate_file_uuid();
        dir_state.get_or_create_file("deleteme.txt", file_uuid);

        // Add to schema
        let doc = dir_state.schema.to_doc().unwrap();
        ymap_schema::add_file(&doc, "deleteme.txt", &file_uuid.to_string());
        dir_state.schema.update_from_doc(&doc);

        // Verify file exists
        assert!(dir_state.has_file("deleteme.txt"));

        // Delete the file
        let removed = dir_state.remove_file("deleteme.txt");
        assert!(removed.is_some());
        assert!(!dir_state.has_file("deleteme.txt"));

        // Update schema to remove file
        let doc = dir_state.schema.to_doc().unwrap();
        ymap_schema::remove_entry(&doc, "deleteme.txt");
        dir_state.schema.update_from_doc(&doc);

        // Verify schema no longer has the file
        let restored_doc = dir_state.schema.to_doc().unwrap();
        assert!(ymap_schema::get_entry(&restored_doc, "deleteme.txt").is_none());
    }

    /// Test that JSONL content stored via create_yjs_jsonl_update round-trips
    /// correctly through get_doc_text_content (the receive-side extraction).
    #[test]
    fn test_publish_jsonl_roundtrips_through_get_doc_text_content() {
        let jsonl_content = r#"{"name":"alice","age":30}
{"name":"bob","age":25}
"#;

        // Simulate what publish_file_content does for JSONL
        let update_b64 =
            create_yjs_jsonl_update(jsonl_content, None).expect("Should create JSONL update");
        let update_bytes = STANDARD.decode(&update_b64).expect("Should decode base64");

        // Apply to a fresh doc (as the receiving peer does)
        let doc = Doc::new();
        {
            use yrs::updates::decoder::Decode;
            let update = yrs::Update::decode_v1(&update_bytes).expect("Should decode Yjs update");
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        // Verify get_text returns empty (YArray items aren't string chunks)
        let txn = doc.transact();
        let text_content = txn
            .get_text("content")
            .map(|t| yrs::GetString::get_string(&t, &txn))
            .unwrap_or_default();
        assert!(
            text_content.is_empty(),
            "YArray content should not be readable as YText"
        );
        drop(txn);

        // Verify the content round-trips via get_doc_text_content
        let extracted = super::super::crdt_merge::get_doc_text_content_for_test(&doc);
        assert!(!extracted.is_empty(), "Should extract JSONL content");
        assert!(extracted.ends_with('\n'), "JSONL should end with newline");

        let lines: Vec<&str> = extracted.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let obj1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(obj1["name"], "alice");
        let obj2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(obj2["name"], "bob");
    }

    /// Test that non-JSONL content stored via YText round-trips correctly.
    #[test]
    fn test_publish_text_roundtrips_through_get_doc_text_content() {
        let content = "Hello, world!";

        // Simulate what publish_file_content does for non-JSONL
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, content);
        }

        let extracted = super::super::crdt_merge::get_doc_text_content_for_test(&doc);
        assert_eq!(extracted, content);
    }
}
