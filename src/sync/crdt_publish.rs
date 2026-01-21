//! CRDT commit publishing for peer sync.
//!
//! This module implements the "publish" side of CRDT peer sync:
//! - Apply local edits to Y.Doc
//! - Create commits with correct parent chain
//! - Publish via MQTT edits port
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use crate::commit::Commit;
use crate::mqtt::{EditMessage, MqttClient, Topic};
use crate::sync::crdt_state::CrdtPeerState;
use base64::{engine::general_purpose::STANDARD, Engine};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};
use yrs::updates::decoder::Decode;
use yrs::{GetString, ReadTxn, Text, Transact, Update, WriteTxn};

/// Result of publishing a local change.
#[derive(Debug)]
pub struct PublishResult {
    /// The commit ID (CID) of the new commit
    pub cid: String,
    /// The Yjs update bytes that were published
    pub update_bytes: Vec<u8>,
}

/// Publish a local text file change as a CRDT commit.
///
/// This implements the new local change flow:
/// 1. Load current Y.Doc from state (or create empty)
/// 2. Compute diff between old content and new content
/// 3. Apply the diff as a Yjs update
/// 4. Create commit with parent = local_head_cid
/// 5. Update local_head_cid in state
/// 6. Publish commit via MQTT
///
/// Returns the new commit ID and the update bytes.
pub async fn publish_text_change(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    old_content: &str,
    new_content: &str,
    author: &str,
) -> Result<PublishResult, String> {
    // Skip if content unchanged
    if old_content == new_content {
        debug!("Content unchanged, skipping publish for {}", node_id);
        return Err("Content unchanged".to_string());
    }

    // Load or create Y.Doc
    let doc = state.to_doc()?;

    // Get or create the text root
    let update_bytes = {
        let mut txn = doc.transact_mut();
        let text = txn.get_or_insert_text("content");

        // Clear existing content and set new content
        // This is a simple approach; could be optimized with diff
        let current = text.get_string(&txn);
        if current != new_content {
            // Delete all existing content
            let len = text.len(&txn);
            if len > 0 {
                text.remove_range(&mut txn, 0, len);
            }
            // Insert new content
            text.insert(&mut txn, 0, new_content);
        }

        // Get the update
        txn.encode_update_v1()
    };

    // Create commit
    let parents = match &state.local_head_cid {
        Some(cid) => vec![cid.clone()],
        None => vec![],
    };

    // Encode update as base64 for the commit
    let update_b64 = STANDARD.encode(&update_bytes);

    // Create commit and calculate CID
    let commit = Commit::new(
        parents.clone(),
        update_b64.clone(),
        author.to_string(),
        None,
    );
    let cid = commit.calculate_cid();

    // Update state
    state.local_head_cid = Some(cid.clone());
    state.update_from_doc(&doc);

    // Publish via MQTT
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: None,
        timestamp,
    };

    let topic = Topic::edits(workspace, node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)
        .map_err(|e| format!("Failed to serialize edit message: {}", e))?;

    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("Failed to publish edit: {}", e))?;

    info!(
        "Published commit {} for {} ({} bytes)",
        cid,
        node_id,
        update_bytes.len()
    );

    Ok(PublishResult { cid, update_bytes })
}

/// Publish a raw Yjs update as a CRDT commit.
///
/// Used when you already have a Yjs update (e.g., from structured content).
pub async fn publish_yjs_update(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    update_bytes: Vec<u8>,
    author: &str,
) -> Result<PublishResult, String> {
    // Apply update to local doc
    let doc = state.to_doc()?;
    {
        let update =
            Update::decode_v1(&update_bytes).map_err(|e| format!("Invalid Yjs update: {}", e))?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    // Create commit
    let parents = match &state.local_head_cid {
        Some(cid) => vec![cid.clone()],
        None => vec![],
    };

    // Encode update as base64 for the commit
    let update_b64 = STANDARD.encode(&update_bytes);

    // Create commit and calculate CID
    let commit = Commit::new(
        parents.clone(),
        update_b64.clone(),
        author.to_string(),
        None,
    );
    let cid = commit.calculate_cid();

    // Update state
    state.local_head_cid = Some(cid.clone());
    state.update_from_doc(&doc);

    // Publish via MQTT
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: None,
        timestamp,
    };

    let topic = Topic::edits(workspace, node_id).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)
        .map_err(|e| format!("Failed to serialize edit message: {}", e))?;

    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("Failed to publish edit: {}", e))?;

    info!(
        "Published commit {} for {} ({} bytes)",
        cid,
        node_id,
        update_bytes.len()
    );

    Ok(PublishResult { cid, update_bytes })
}

/// Check if a commit CID is already known in the state.
///
/// This is used for echo prevention - we skip processing commits
/// that we created ourselves.
pub fn is_commit_known(state: &CrdtPeerState, cid: &str) -> bool {
    state.is_cid_known(cid)
}

/// Apply a received commit to local Y.Doc state.
///
/// This is used when receiving a commit from MQTT that we didn't create.
/// Returns Ok(true) if the update was applied, Ok(false) if it was a duplicate.
pub fn apply_received_commit(
    state: &mut CrdtPeerState,
    cid: &str,
    update_b64: &str,
    _parents: &[String],
) -> Result<bool, String> {
    // Check if we already know this commit
    if state.is_cid_known(cid) {
        debug!("Commit {} already known, skipping", cid);
        return Ok(false);
    }

    // Decode update
    let update_bytes = STANDARD
        .decode(update_b64)
        .map_err(|e| format!("Failed to decode update: {}", e))?;

    let update = Update::decode_v1(&update_bytes)
        .map_err(|e| format!("Failed to decode Yjs update: {}", e))?;

    // Apply to local doc
    let doc = state.to_doc()?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    // Update state
    state.head_cid = Some(cid.to_string());
    state.update_from_doc(&doc);

    debug!("Applied commit {} ({} bytes)", cid, update_bytes.len());
    Ok(true)
}

/// Get the current text content from the Y.Doc state.
pub fn get_text_content(state: &CrdtPeerState) -> Result<String, String> {
    let doc = state.to_doc()?;
    let txn = doc.transact();

    match txn.get_text("content") {
        Some(text) => Ok(text.get_string(&txn)),
        None => Ok(String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_is_commit_known() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        assert!(!is_commit_known(&state, "abc123"));

        state.local_head_cid = Some("abc123".to_string());
        assert!(is_commit_known(&state, "abc123"));

        state.head_cid = Some("def456".to_string());
        assert!(is_commit_known(&state, "def456"));
    }

    #[test]
    fn test_get_text_content_empty() {
        let state = CrdtPeerState::new(Uuid::new_v4());
        let content = get_text_content(&state).unwrap();
        assert_eq!(content, "");
    }

    #[test]
    fn test_apply_received_commit() {
        use yrs::{Doc, Text, WriteTxn};

        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // Create a Yjs update with some content
        let doc = Doc::new();
        let update_bytes = {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello world");
            txn.encode_update_v1()
        };
        let update_b64 = STANDARD.encode(&update_bytes);

        // Apply the commit
        let result = apply_received_commit(&mut state, "cid123", &update_b64, &[]).unwrap();
        assert!(result);

        // Verify state was updated
        assert_eq!(state.head_cid, Some("cid123".to_string()));

        // Verify content
        let content = get_text_content(&state).unwrap();
        assert_eq!(content, "hello world");

        // Applying same commit again should be a no-op
        let result2 = apply_received_commit(&mut state, "cid123", &update_b64, &[]).unwrap();
        assert!(!result2);
    }
}
