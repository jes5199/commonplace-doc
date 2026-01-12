//! Edits port handler.
//!
//! Subscribes to `{path}/edits` topics, persists Yjs updates to the commit store,
//! and applies them to documents.
//!
//! IMPORTANT: The doc store does NOT re-emit edits. MQTT broker handles fanout.

use crate::commit::Commit;
use crate::document::{resolve_path_to_uuid, DocumentStore};
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::EditMessage;
use crate::mqtt::topics::{content_type_for_path, Topic};
use crate::mqtt::{parse_json, MqttError};
use crate::store::CommitStore;
use rumqttc::QoS;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Handler for the edits port.
pub struct EditsHandler {
    client: Arc<MqttClient>,
    document_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    workspace: String,
    /// Cache of fs-root JSON content for path→UUID resolution.
    fs_root_content: RwLock<String>,
    /// The fs-root path, so we can refresh the cache when it's edited.
    fs_root_path: RwLock<Option<String>>,
    subscribed_paths: RwLock<HashSet<String>>,
}

impl EditsHandler {
    /// Create a new edits handler.
    pub fn new(
        client: Arc<MqttClient>,
        document_store: Arc<DocumentStore>,
        commit_store: Option<Arc<CommitStore>>,
        workspace: String,
    ) -> Self {
        Self {
            client,
            document_store,
            commit_store,
            workspace,
            fs_root_content: RwLock::new(String::new()),
            fs_root_path: RwLock::new(None),
            subscribed_paths: RwLock::new(HashSet::new()),
        }
    }

    /// Update the cached fs-root content for path resolution.
    pub async fn set_fs_root_content(&self, content: String) {
        let mut fs_root = self.fs_root_content.write().await;
        *fs_root = content;
    }

    /// Set the fs-root path so we can refresh the cache when it's edited.
    pub async fn set_fs_root_path(&self, path: String) {
        let mut fs_root_path = self.fs_root_path.write().await;
        *fs_root_path = Some(path);
    }

    /// Subscribe to edits for a path.
    pub async fn subscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic = Topic::edits(&self.workspace, path);
        let topic_str = topic.to_topic_string();

        // Use QoS 1 (at least once) for edits - important not to lose them
        self.client.subscribe(&topic_str, QoS::AtLeastOnce).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.insert(path.to_string());

        debug!("Subscribed to edits for path: {}", path);
        Ok(())
    }

    /// Unsubscribe from edits for a path.
    pub async fn unsubscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic = Topic::edits(&self.workspace, path);
        let topic_str = topic.to_topic_string();

        self.client.unsubscribe(&topic_str).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.remove(path);

        debug!("Unsubscribed from edits for path: {}", path);
        Ok(())
    }

    /// Handle an incoming edit message.
    ///
    /// This:
    /// 1. Parses the EditMessage
    /// 2. Creates a Commit and stores it (if commit_store is available)
    /// 3. Applies the Yjs update to the Document via DocumentStore
    ///
    /// IMPORTANT: Does NOT re-emit. MQTT broker handles fanout.
    pub async fn handle_edit(&self, topic: &Topic, payload: &[u8]) -> Result<(), MqttError> {
        // Parse the edit message
        let edit_msg: EditMessage = parse_json(payload)?;

        debug!(
            "Received edit for path: {} from author: {}",
            topic.path, edit_msg.author
        );

        // Check if this is an fs-root edit (special case: path IS the document ID)
        let fs_root_path = self.fs_root_path.read().await;
        let is_fs_root_edit = fs_root_path.as_ref() == Some(&topic.path);
        let fs_root_id = fs_root_path.clone();
        drop(fs_root_path);

        // Resolve path to document ID:
        // - For fs-root: the path IS the document ID
        // - For other documents: resolve via fs-root content
        let document_id = if is_fs_root_edit {
            topic.path.clone()
        } else {
            let fs_root = self.fs_root_content.read().await;
            let fs_root_id = fs_root_id.as_deref().unwrap_or("");
            let uuid =
                resolve_path_to_uuid(&fs_root, &topic.path, fs_root_id).ok_or_else(|| {
                    MqttError::InvalidMessage(format!(
                        "Path not mounted in fs-root: {}",
                        topic.path
                    ))
                })?;
            drop(fs_root);
            uuid
        };

        // Determine parents: use provided parents, or infer from current HEAD if empty
        let parents = if edit_msg.parents.is_empty() {
            // No parents provided - infer from current document HEAD
            if let Some(store) = &self.commit_store {
                if let Ok(Some(head)) = store.get_document_head(&document_id).await {
                    vec![head]
                } else {
                    vec![] // No HEAD yet, this is a root commit
                }
            } else {
                vec![] // No commit store, use empty parents
            }
        } else {
            edit_msg.parents.clone()
        };

        // Create commit from the edit message
        let commit = Commit {
            parents,
            timestamp: edit_msg.timestamp,
            update: edit_msg.update.clone(),
            author: edit_msg.author.clone(),
            message: edit_msg.message.clone(),
            extensions: Default::default(),
        };

        // Store the commit if we have a commit store
        let cid = if let Some(store) = &self.commit_store {
            let (cid, _timestamp) = store
                .store_commit_and_set_head(&document_id, &commit)
                .await?;
            debug!("Stored commit {} for document {}", cid, document_id);
            Some(cid)
        } else {
            None
        };

        // Get the content type and ensure document exists
        let content_type = content_type_for_path(&topic.path)?;
        let _doc = self
            .document_store
            .get_or_create_with_id(&document_id, content_type)
            .await;

        // Decode the base64 update and apply it to the document
        let update_bytes = crate::b64::decode_with_context(&commit.update, "update")
            .map_err(MqttError::InvalidMessage)?;

        self.document_store
            .apply_yjs_update(&document_id, &update_bytes)
            .await
            .map_err(|e| {
                MqttError::InvalidMessage(format!("Failed to apply Yjs update: {:?}", e))
            })?;

        debug!(
            "Applied edit to document {} (path: {}, cid: {:?})",
            document_id,
            topic.path,
            cid.as_deref().unwrap_or("none")
        );

        // If this was an fs-root edit, refresh the fs-root content cache
        if is_fs_root_edit {
            if let Some(doc) = self.document_store.get_document(&document_id).await {
                let mut fs_root = self.fs_root_content.write().await;
                *fs_root = doc.content.clone();
                debug!("Refreshed fs-root content cache after edit");
            }
        }

        Ok(())
    }

    /// Check if a path is subscribed.
    pub async fn is_subscribed(&self, path: &str) -> bool {
        let paths = self.subscribed_paths.read().await;
        paths.contains(path)
    }

    /// Get all subscribed paths.
    pub async fn subscribed_paths(&self) -> Vec<String> {
        let paths = self.subscribed_paths.read().await;
        paths.iter().cloned().collect()
    }
}
