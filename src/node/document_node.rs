use super::subscription::Subscription;
use super::types::{Edit, Event, NodeError, NodeId, NodeMessage};
use super::{Node, ObservableNode};
use crate::document::ContentType;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{broadcast, RwLock};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::{GetString, Transact, WriteTxn};

/// Configuration for DocumentNode
pub struct DocumentNodeConfig {
    /// Broadcast channel capacity for subscribers
    pub channel_capacity: usize,
}

impl Default for DocumentNodeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 256,
        }
    }
}

/// Internal state for a DocumentNode
struct DocumentState {
    content: String,
    content_type: ContentType,
    ydoc: yrs::Doc,
}

/// A node wrapping a single document with Yjs CRDT support
pub struct DocumentNode {
    id: NodeId,
    state: RwLock<DocumentState>,
    /// Broadcast channel for emitting messages to subscribers
    tx: broadcast::Sender<NodeMessage>,
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
        let (tx, _rx) = broadcast::channel(config.channel_capacity);

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
            tx,
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

    /// Emit a message to all subscribers
    fn emit(&self, message: NodeMessage) {
        // Ignore send errors (no subscribers)
        let _ = self.tx.send(message);
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

        // Re-emit the edit to our subscribers (with our ID as the new source)
        let outgoing_edit = Edit {
            commit: edit.commit,
            source: self.id.clone(),
        };
        self.emit(NodeMessage::Edit(outgoing_edit));

        Ok(())
    }

    async fn receive_event(&self, event: Event) -> Result<(), NodeError> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(NodeError::Shutdown);
        }

        // Forward events to subscribers (documents don't interpret events)
        let outgoing_event = Event {
            source: self.id.clone(),
            ..event
        };
        self.emit(NodeMessage::Event(outgoing_event));
        Ok(())
    }

    fn subscribe(&self) -> Subscription {
        Subscription::new(self.id.clone(), self.tx.subscribe())
    }

    fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    async fn shutdown(&self) -> Result<(), NodeError> {
        self.is_shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.is_shutdown.load(Ordering::Relaxed)
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
        assert_eq!(node.subscriber_count(), 0);

        let _sub1 = node.subscribe();
        assert_eq!(node.subscriber_count(), 1);

        let _sub2 = node.subscribe();
        assert_eq!(node.subscriber_count(), 2);
    }
}
