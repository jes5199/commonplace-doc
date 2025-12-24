use super::subscription::{BlueSubscription, RedSubscription, Subscription};
use super::types::{Edit, Event, NodeError, NodeId};
use super::{Node, ObservableNode};
use crate::document::ContentType;
use async_trait::async_trait;
use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{broadcast, RwLock};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::{GetString, Text, Transact, WriteTxn};

/// Configuration for DocumentNode
pub struct DocumentNodeConfig {
    /// Broadcast channel capacity for blue port (edits)
    pub blue_channel_capacity: usize,
    /// Broadcast channel capacity for red port (events)
    pub red_channel_capacity: usize,
}

impl Default for DocumentNodeConfig {
    fn default() -> Self {
        Self {
            blue_channel_capacity: 256,
            red_channel_capacity: 256,
        }
    }
}

/// Internal state for a DocumentNode
struct DocumentState {
    content: String,
    content_type: ContentType,
    ydoc: yrs::Doc,
}

/// A node wrapping a single document with Yjs CRDT support.
///
/// DocumentNode has two ports:
/// - **Blue port**: Emits edits (Yjs commits) when the document changes
/// - **Red port**: Emits events (ephemeral JSON) forwarded from receive_event
pub struct DocumentNode {
    id: NodeId,
    state: RwLock<DocumentState>,
    /// Blue channel for edits (persistent commits)
    blue_tx: broadcast::Sender<Edit>,
    /// Red channel for events (ephemeral)
    red_tx: broadcast::Sender<Event>,
    /// Shutdown flag
    is_shutdown: AtomicBool,
}

impl DocumentNode {
    const TEXT_ROOT_NAME: &'static str = "content";
    const DEFAULT_YDOC_CLIENT_ID: u64 = 1;
    const XML_HEADER: &'static str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

    /// Create a new DocumentNode with the given ID and content type
    pub fn new(id: impl Into<String>, content_type: ContentType) -> Self {
        Self::with_config(id, content_type, DocumentNodeConfig::default())
    }

    /// Create a new DocumentNode with custom configuration
    pub fn with_config(
        id: impl Into<String>,
        content_type: ContentType,
        config: DocumentNodeConfig,
    ) -> Self {
        let (blue_tx, _) = broadcast::channel(config.blue_channel_capacity);
        let (red_tx, _) = broadcast::channel(config.red_channel_capacity);

        let ydoc = yrs::Doc::with_client_id(Self::DEFAULT_YDOC_CLIENT_ID);
        // Initialize the appropriate Yrs root type
        match content_type {
            ContentType::Text => {
                ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);
            }
            ContentType::Json => {
                ydoc.get_or_insert_map(Self::TEXT_ROOT_NAME);
            }
            ContentType::Xml => {
                ydoc.get_or_insert_xml_fragment(Self::TEXT_ROOT_NAME);
            }
        }

        let state = DocumentState {
            content: content_type.default_content(),
            content_type,
            ydoc,
        };

        Self {
            id: NodeId::new(id),
            state: RwLock::new(state),
            blue_tx,
            red_tx,
            is_shutdown: AtomicBool::new(false),
        }
    }

    /// Apply a Yjs update and return the new content
    async fn apply_update(&self, update_bytes: &[u8]) -> Result<String, NodeError> {
        let mut state = self.state.write().await;

        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(NodeError::Shutdown);
        }

        let update = yrs::Update::decode_v1(update_bytes)
            .map_err(|e| NodeError::InvalidEdit(e.to_string()))?;

        // Clone content_type before borrowing ydoc
        let content_type = state.content_type.clone();

        // Apply update and extract content within transaction scope
        let content = {
            let mut txn = state.ydoc.transact_mut();
            txn.apply_update(update);
            Self::extract_content(&mut txn, &content_type)?
        };

        state.content = content.clone();
        Ok(content)
    }

    /// Extract rendered content from a transaction based on content type
    fn extract_content(
        txn: &mut yrs::TransactionMut<'_>,
        content_type: &ContentType,
    ) -> Result<String, NodeError> {
        match content_type {
            ContentType::Text => {
                let text = txn.get_or_insert_text(Self::TEXT_ROOT_NAME);
                Ok(text.get_string(txn))
            }
            ContentType::Json => {
                let map = txn.get_or_insert_map(Self::TEXT_ROOT_NAME);
                let any = map.to_json(txn);
                serde_json::to_string(&any)
                    .map_err(|e| NodeError::InvalidEdit(format!("JSON serialization: {}", e)))
            }
            ContentType::Xml => {
                let fragment = txn.get_or_insert_xml_fragment(Self::TEXT_ROOT_NAME);
                let inner = fragment.get_string(txn);
                Ok(Self::wrap_xml_root(&inner))
            }
        }
    }

    fn wrap_xml_root(inner: &str) -> String {
        if inner.is_empty() {
            format!("{}<root/>", Self::XML_HEADER)
        } else {
            format!("{}<root>{}</root>", Self::XML_HEADER, inner)
        }
    }

    /// Emit an edit to blue port subscribers
    fn emit_edit(&self, edit: Edit) {
        // Ignore send errors (no subscribers)
        let _ = self.blue_tx.send(edit);
    }

    /// Emit an event to red port subscribers
    fn emit_event(&self, event: Event) {
        // Ignore send errors (no subscribers)
        let _ = self.red_tx.send(event);
    }

    /// Get the content type of this document
    pub fn content_type(&self) -> ContentType {
        // Safe to block briefly since we only read
        futures::executor::block_on(async { self.state.read().await.content_type.clone() })
    }

    /// Set the content of this document (for forking)
    pub fn set_content(&self, content: &str) {
        futures::executor::block_on(async {
            let mut state = self.state.write().await;

            // Only support text content type for now
            if matches!(state.content_type, ContentType::Text) {
                {
                    let text = state.ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);
                    let mut txn = state.ydoc.transact_mut();

                    // Clear existing content
                    let len = text.len(&txn);
                    if len > 0 {
                        text.remove_range(&mut txn, 0, len);
                    }

                    // Insert new content
                    text.insert(&mut txn, 0, content);
                    // txn is committed when dropped here
                }
                state.content = content.to_string();
            }
        })
    }
}

#[async_trait]
impl Node for DocumentNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn node_type(&self) -> &'static str {
        "document"
    }

    async fn receive_edit(&self, edit: Edit) -> Result<(), NodeError> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(NodeError::Shutdown);
        }

        // Decode the base64 update from the commit
        let update_bytes = crate::b64::decode(&edit.commit.update)
            .map_err(|e| NodeError::InvalidEdit(e.to_string()))?;

        // Apply the update
        self.apply_update(&update_bytes).await?;

        // Re-emit the edit to blue port subscribers (with our ID as the new source)
        let outgoing_edit = Edit {
            commit: edit.commit,
            source: self.id.clone(),
        };
        self.emit_edit(outgoing_edit);

        Ok(())
    }

    async fn receive_event(&self, event: Event) -> Result<(), NodeError> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(NodeError::Shutdown);
        }

        // Forward events to red port subscribers (documents don't interpret events)
        let outgoing_event = Event {
            source: self.id.clone(),
            ..event
        };
        self.emit_event(outgoing_event);
        Ok(())
    }

    fn subscribe_blue(&self) -> BlueSubscription {
        BlueSubscription::new(self.id.clone(), self.blue_tx.subscribe())
    }

    fn subscribe_red(&self) -> RedSubscription {
        RedSubscription::new(self.id.clone(), self.red_tx.subscribe())
    }

    fn subscribe(&self) -> Subscription {
        Subscription::new(
            self.id.clone(),
            self.blue_tx.subscribe(),
            self.red_tx.subscribe(),
        )
    }

    fn blue_subscriber_count(&self) -> usize {
        self.blue_tx.receiver_count()
    }

    fn red_subscriber_count(&self) -> usize {
        self.red_tx.receiver_count()
    }

    async fn shutdown(&self) -> Result<(), NodeError> {
        self.is_shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.is_shutdown.load(Ordering::Relaxed)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl ObservableNode for DocumentNode {
    async fn get_content(&self) -> Result<String, NodeError> {
        let state = self.state.read().await;
        Ok(state.content.clone())
    }

    fn content_type(&self) -> &str {
        // Note: This is a simplification - we'd need to store the MIME type
        // For now, return a placeholder. The actual MIME type can be retrieved
        // by reading state.content_type.to_mime() which requires async access
        "text/plain"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_document_node_creation() {
        let node = DocumentNode::new("test-doc", ContentType::Text);
        assert_eq!(node.id().0, "test-doc");
        assert_eq!(node.node_type(), "document");
        assert!(node.is_healthy());
    }

    #[tokio::test]
    async fn test_document_node_content() {
        let node = DocumentNode::new("test-doc", ContentType::Json);
        let content = node.get_content().await.unwrap();
        assert_eq!(content, "{}");
    }

    #[tokio::test]
    async fn test_document_node_shutdown() {
        let node = DocumentNode::new("test-doc", ContentType::Text);
        assert!(node.is_healthy());

        node.shutdown().await.unwrap();
        assert!(!node.is_healthy());

        // Operations should fail after shutdown
        let event = Event {
            event_type: "test".to_string(),
            payload: serde_json::json!({}),
            source: NodeId::new("other"),
        };
        let result = node.receive_event(event).await;
        assert!(matches!(result, Err(NodeError::Shutdown)));
    }

    #[tokio::test]
    async fn test_document_node_subscription() {
        let node = DocumentNode::new("test-doc", ContentType::Text);
        assert_eq!(node.blue_subscriber_count(), 0);
        assert_eq!(node.red_subscriber_count(), 0);

        let _blue_sub = node.subscribe_blue();
        assert_eq!(node.blue_subscriber_count(), 1);
        assert_eq!(node.red_subscriber_count(), 0);

        let _red_sub = node.subscribe_red();
        assert_eq!(node.blue_subscriber_count(), 1);
        assert_eq!(node.red_subscriber_count(), 1);

        // Combined subscription adds to both counts
        let _combined = node.subscribe();
        assert_eq!(node.blue_subscriber_count(), 2);
        assert_eq!(node.red_subscriber_count(), 2);
    }
}
