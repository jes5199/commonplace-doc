//! CRDT commit publishing for peer sync.
//!
//! This module implements the "publish" side of CRDT peer sync:
//! - Apply local edits to Y.Doc
//! - Create commits with correct parent chain
//! - Publish via MQTT edits port
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use crate::crdt_state::CrdtPeerState;
use base64::{engine::general_purpose::STANDARD, Engine};
use commonplace_types::commit::Commit;
use commonplace_types::diff;
use commonplace_types::mqtt::EditMessage;
use commonplace_types::sync::error::{SyncError, SyncResult};
use commonplace_types::traits::{edits_topic, MqttPublisher};
use std::sync::Arc;
use tracing::debug;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, Transact, Update};

/// Result of publishing a local change.
#[derive(Debug)]
pub struct PublishResult {
    /// The commit ID (CID) of the new commit
    pub cid: String,
    /// The Yjs update bytes that were published
    pub update_bytes: Vec<u8>,
}

/// Ensure the Y.Doc text content matches the expected old content before publish.
fn check_text_publish_preconditions(
    doc: &Doc,
    old_content: &str,
    new_content: &str,
) -> SyncResult<String> {
    let current_content = get_doc_text_content(doc);

    if current_content == new_content {
        return Err(SyncError::ContentUnchanged);
    }

    if current_content != old_content {
        return Err(SyncError::stale_crdt_state(format!(
            "doc_len={} old_len={} new_len={}",
            current_content.len(),
            old_content.len(),
            new_content.len()
        )));
    }

    Ok(current_content)
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
    mqtt_client: &Arc<impl MqttPublisher>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    old_content: &str,
    new_content: &str,
    author: &str,
) -> SyncResult<PublishResult> {
    // Load or create Y.Doc
    let doc = state.to_doc()?;

    // Compute a minimal update against the current Y.Doc state.
    let current_content = check_text_publish_preconditions(&doc, old_content, new_content)?;

    let update_bytes = match compute_text_update(&doc, &current_content, new_content)? {
        Some(update) => update,
        None => {
            debug!("No-op update for {}, skipping publish", node_id);
            return Err(SyncError::ContentUnchanged);
        }
    };

    // Apply the update to our local doc state
    {
        let update = Update::decode_v1(&update_bytes)
            .map_err(|e| SyncError::yjs_decode(format!("Invalid Yjs update: {}", e)))?;
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
    // Update both local_head_cid AND head_cid when we publish.
    // This is critical: when we publish a commit, it becomes the new logical
    // server head. Without updating head_cid, the merge strategy will think
    // we have diverged from the server when we receive edits that build on
    // our published commit, causing LocalAhead instead of FastForward.
    state.local_head_cid = Some(cid.clone());
    state.head_cid = Some(cid.clone());
    state.record_known_cid(&cid);
    state.update_from_doc(&doc);

    // Publish via MQTT — use the commit's timestamp so receivers can reconstruct
    // the same CID for echo detection (is_cid_known).
    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: None,
        timestamp: commit.timestamp,
        req: None,
    };

    let topic = edits_topic(workspace, node_id);
    let payload = serde_json::to_vec(&edit_msg)?;

    // Use retained message so new subscribers get the latest content immediately.
    // This is critical for sync: subscribers may join after messages are published,
    // and the retained message ensures they receive the current content.
    mqtt_client
        .publish_retained(&topic, &payload)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to publish edit: {}", e)))?;

    debug!(
        "Published commit {} for {} ({} bytes, retained)",
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
    mqtt_client: &Arc<impl MqttPublisher>,
    workspace: &str,
    node_id: &str,
    state: &mut CrdtPeerState,
    update_bytes: Vec<u8>,
    author: &str,
) -> SyncResult<PublishResult> {
    if state.needs_server_init() {
        return Err(SyncError::stale_crdt_state(
            "CRDT state not initialized".to_string(),
        ));
    }

    // Apply update to local doc
    let doc = state.to_doc()?;
    {
        let update = Update::decode_v1(&update_bytes)
            .map_err(|e| SyncError::yjs_decode(format!("Invalid Yjs update: {}", e)))?;
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
    // Update both local_head_cid AND head_cid when we publish.
    // This is critical: when we publish a commit, it becomes the new logical
    // server head. Without updating head_cid, the merge strategy will think
    // we have diverged from the server when we receive edits that build on
    // our published commit, causing LocalAhead instead of FastForward.
    state.local_head_cid = Some(cid.clone());
    state.head_cid = Some(cid.clone());
    state.record_known_cid(&cid);
    state.update_from_doc(&doc);

    // Publish via MQTT — use the commit's timestamp so receivers can reconstruct
    // the same CID for echo detection (is_cid_known).
    let edit_msg = EditMessage {
        update: update_b64,
        parents,
        author: author.to_string(),
        message: None,
        timestamp: commit.timestamp,
        req: None,
    };

    let topic = edits_topic(workspace, node_id);
    let payload = serde_json::to_vec(&edit_msg)?;

    // Use retained message so new subscribers get the latest content immediately.
    mqtt_client
        .publish_retained(&topic, &payload)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to publish edit: {}", e)))?;

    debug!(
        "Published commit {} for {} ({} bytes, retained)",
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
) -> SyncResult<bool> {
    // Check if we already know this commit
    if state.is_cid_known(cid) {
        debug!("Commit {} already known, skipping", cid);
        return Ok(false);
    }

    // Decode update
    let update_bytes = STANDARD.decode(update_b64)?;

    let update = Update::decode_v1(&update_bytes)
        .map_err(|e| SyncError::yjs_decode(format!("Failed to decode Yjs update: {}", e)))?;

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
pub fn get_text_content(state: &CrdtPeerState) -> SyncResult<String> {
    let doc = state.to_doc()?;
    let txn = doc.transact();

    match txn.get_text("content") {
        Some(text) => Ok(text.get_string(&txn)),
        None => Ok(String::new()),
    }
}

/// Get the current text content from a Y.Doc.
fn get_doc_text_content(doc: &Doc) -> String {
    let txn = doc.transact();
    match txn.get_text("content") {
        Some(text) => text.get_string(&txn),
        None => String::new(),
    }
}

/// Compute a minimal text update using the current Y.Doc state as base.
///
/// Returns None when no changes are detected.
fn compute_text_update(
    doc: &Doc,
    old_content: &str,
    new_content: &str,
) -> SyncResult<Option<Vec<u8>>> {
    if old_content == new_content {
        return Ok(None);
    }

    let base_state = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };

    let diff = diff::compute_diff_update_with_base(&base_state, old_content, new_content)
        .map_err(|e| SyncError::other(format!("Diff computation failed: {}", e)))?;

    if diff.summary.chars_inserted == 0 && diff.summary.chars_deleted == 0 {
        return Ok(None);
    }

    Ok(Some(diff.update_bytes))
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

    #[test]
    fn test_compute_text_update_noop() {
        use yrs::{Doc, Text, WriteTxn};

        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }

        let current = get_doc_text_content(&doc);
        let update = compute_text_update(&doc, &current, "hello").unwrap();
        assert!(update.is_none());
    }

    #[test]
    fn test_compute_text_update_apply_insert() {
        use yrs::{Doc, Text, Update, WriteTxn};

        let base_doc = Doc::new();
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello");
        }

        let current = get_doc_text_content(&base_doc);
        let update = compute_text_update(&base_doc, &current, "hello world")
            .unwrap()
            .expect("expected update");

        // Apply base state then update to a fresh doc
        let base_state = {
            let txn = base_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let apply_doc = Doc::new();
        {
            let update = Update::decode_v1(&base_state).unwrap();
            let mut txn = apply_doc.transact_mut();
            txn.apply_update(update);
        }
        {
            let update = Update::decode_v1(&update).unwrap();
            let mut txn = apply_doc.transact_mut();
            txn.apply_update(update);
        }

        let content = get_doc_text_content(&apply_doc);
        assert_eq!(content, "hello world");
    }

    #[test]
    fn test_compute_text_update_apply_delete() {
        use yrs::{Doc, Text, Update, WriteTxn};

        let base_doc = Doc::new();
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "hello world");
        }

        let current = get_doc_text_content(&base_doc);
        let update = compute_text_update(&base_doc, &current, "hello")
            .unwrap()
            .expect("expected update");

        // Apply base state then update to a fresh doc
        let base_state = {
            let txn = base_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let apply_doc = Doc::new();
        {
            let update = Update::decode_v1(&base_state).unwrap();
            let mut txn = apply_doc.transact_mut();
            txn.apply_update(update);
        }
        {
            let update = Update::decode_v1(&update).unwrap();
            let mut txn = apply_doc.transact_mut();
            txn.apply_update(update);
        }

        let content = get_doc_text_content(&apply_doc);
        assert_eq!(content, "hello");
    }

    #[test]
    fn test_compute_text_update_preserves_newlines() {
        use yrs::{Doc, Text, Update, WriteTxn};

        // Test that newlines are preserved through CRDT updates
        let base_doc = Doc::new();
        {
            let mut txn = base_doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "");
        }

        let current = get_doc_text_content(&base_doc);
        let new_content = "/typing\nHello World\nLine 2\n/unset typing\n";
        let update = compute_text_update(&base_doc, &current, new_content)
            .unwrap()
            .expect("expected update");

        // Apply to a fresh doc
        let base_state = {
            let txn = base_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let apply_doc = Doc::new();
        {
            let update = Update::decode_v1(&base_state).unwrap();
            let mut txn = apply_doc.transact_mut();
            txn.apply_update(update);
        }
        {
            let update = Update::decode_v1(&update).unwrap();
            let mut txn = apply_doc.transact_mut();
            txn.apply_update(update);
        }

        let content = get_doc_text_content(&apply_doc);
        assert_eq!(
            content, new_content,
            "Newlines should be preserved in CRDT text updates"
        );
        assert!(content.contains('\n'), "Content should contain newlines");
        assert_eq!(content.matches('\n').count(), 4, "Should have 4 newlines");
    }

    #[test]
    fn test_check_text_publish_preconditions_stale() {
        use yrs::{Doc, Text, WriteTxn};

        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("content");
            text.insert(&mut txn, 0, "server");
        }

        let result = check_text_publish_preconditions(&doc, "local", "local updated");
        assert!(matches!(result, Err(SyncError::StaleCrdtState(_))));
    }
}
