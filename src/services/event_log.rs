//! Event log service for persistent red event streams.
//!
//! Manages JSONL event log documents that record events for files.
//! Event logs are created lazily on first append, with the UUID
//! stored in the file's schema entry.

use crate::document::DocumentStore;
use commonplace_types::content_type::ContentType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;
use yrs::updates::decoder::Decode;
use yrs::{Array, ReadTxn, Transact, WriteTxn};

/// Notification sent when an event is appended to an event log.
#[derive(Debug, Clone)]
pub struct EventNotification {
    /// The event log document ID
    pub log_id: String,
    /// The appended event entry
    pub entry: EventLogEntry,
}

/// Broadcaster for real-time event notifications.
#[derive(Clone)]
pub struct EventBroadcaster {
    sender: Arc<broadcast::Sender<EventNotification>>,
}

impl EventBroadcaster {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(sender),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventNotification> {
        self.sender.subscribe()
    }

    pub fn notify(&self, notification: EventNotification) {
        let _ = self.sender.send(notification);
    }
}

/// Service for managing event log documents.
///
/// Handles lazy creation of JSONL event log docs and appending events.
pub struct EventLogService {
    store: Arc<DocumentStore>,
    broadcaster: Option<EventBroadcaster>,
}

impl EventLogService {
    pub fn new(store: Arc<DocumentStore>) -> Self {
        Self {
            store,
            broadcaster: None,
        }
    }

    pub fn with_broadcaster(mut self, broadcaster: EventBroadcaster) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    /// Append an event to an event log document.
    ///
    /// If `event_log_id` is provided, appends to that doc.
    /// If not, creates a new JSONL doc and returns its UUID.
    ///
    /// Returns the event_log UUID (existing or newly created).
    pub async fn append_event(
        &self,
        event_log_id: Option<&str>,
        entry: &EventLogEntry,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let entry_json = serde_json::to_value(entry)?;

        // Get or create the event log doc
        let log_id = match event_log_id {
            Some(id) => id.to_string(),
            None => {
                // Create a new JSONL doc
                self.store.create_document(ContentType::Jsonl).await
            }
        };

        // Get the current doc state to build the append update
        let doc = self.store.get_document(&log_id).await;
        let base_state = doc.and_then(|d| {
            d.ydoc.as_ref().map(|ydoc| {
                let txn = ydoc.transact();
                commonplace_crdt::base64_encode(&txn.encode_state_as_update_v1(&yrs::StateVector::default()))
            })
        });

        let update_b64 =
            create_yjs_jsonl_append(&entry_json, base_state.as_deref())?;

        // Decode and apply the update to the document
        let update_bytes = commonplace_crdt::base64_decode(&update_b64)?;
        self.store
            .apply_yjs_update(&log_id, &update_bytes)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Failed to apply event log update: {:?}", e).into()
            })?;

        // Notify subscribers
        if let Some(ref broadcaster) = self.broadcaster {
            broadcaster.notify(EventNotification {
                log_id: log_id.clone(),
                entry: entry.clone(),
            });
        }

        Ok(log_id)
    }

    /// Read events from an event log document.
    ///
    /// Returns events with optional filtering by offset (`since`) and `limit`.
    pub async fn read_events(
        &self,
        log_id: &str,
        since: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<EventLogEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let doc = self
            .store
            .get_document(log_id)
            .await
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Event log document not found: {}", log_id).into()
            })?;

        let content = &doc.content;
        let entries: Vec<EventLogEntry> = content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .skip(since.unwrap_or(0))
            .take(limit.unwrap_or(usize::MAX))
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();

        Ok(entries)
    }
}

/// A single entry in an event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogEntry {
    /// ISO 8601 timestamp
    pub timestamp: String,
    /// Type of event (e.g., "red:update", "process:stdout")
    pub event_type: String,
    /// Actor that produced the event (node ID, process name, etc.)
    pub source: String,
    /// Arbitrary JSON payload
    pub payload: Value,
}

impl EventLogEntry {
    /// Create a new event log entry with the current timestamp.
    pub fn from_event(event_type: &str, source: &str, payload: &Value) -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: event_type.to_string(),
            source: source.to_string(),
            payload: payload.clone(),
        }
    }
}

/// Create a Yjs update that appends a single JSON value to a Y.Array document.
///
/// Takes the current document state (base64-encoded) and returns a base64-encoded
/// update that appends the entry. If `base_state` is None, creates a new doc.
pub fn create_yjs_jsonl_append(
    entry: &Value,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    use commonplace_crdt::{base64_decode, base64_encode, json_value_to_any, TEXT_ROOT_NAME};

    let doc = yrs::Doc::with_client_id(1);

    if let Some(state_b64) = base_state {
        let state_bytes = base64_decode(state_b64)?;
        if !state_bytes.is_empty() {
            let update = yrs::Update::decode_v1(&state_bytes)?;
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }
    }

    let update = {
        let mut txn = doc.transact_mut();
        let array = txn.get_or_insert_array(TEXT_ROOT_NAME);
        let any_val = json_value_to_any(entry.clone());
        array.push_back(&mut txn, any_val);
        txn.encode_update_v1()
    };

    Ok(base64_encode(&update))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_event_log_entry_serializes_to_json() {
        let entry = EventLogEntry {
            timestamp: "2026-03-07T21:00:00Z".to_string(),
            event_type: "red:update".to_string(),
            source: "bartleby".to_string(),
            payload: json!({"message": "hello"}),
        };

        let json_str = serde_json::to_string(&entry).unwrap();
        let roundtrip: EventLogEntry = serde_json::from_str(&json_str).unwrap();

        assert_eq!(roundtrip.timestamp, "2026-03-07T21:00:00Z");
        assert_eq!(roundtrip.event_type, "red:update");
        assert_eq!(roundtrip.source, "bartleby");
        assert_eq!(roundtrip.payload, json!({"message": "hello"}));
    }

    #[test]
    fn test_append_to_empty_event_log() {
        let entry = json!({"event_type": "test", "source": "unit-test", "payload": {}});
        let update_b64 = create_yjs_jsonl_append(&entry, None).unwrap();

        // Verify by reading back
        let content = commonplace_crdt::yjs_array_to_jsonl(&update_b64).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["event_type"], "test");
    }

    #[test]
    fn test_append_preserves_existing_entries() {
        use commonplace_crdt::{base64_decode, TEXT_ROOT_NAME};

        // Create initial state with one entry
        let entry1 = json!({"event_type": "first", "source": "test"});
        let state1 = create_yjs_jsonl_append(&entry1, None).unwrap();

        // Append a second entry
        let entry2 = json!({"event_type": "second", "source": "test"});
        let state2 = create_yjs_jsonl_append(&entry2, Some(&state1)).unwrap();

        // Read back — should have both entries
        // Need to merge both updates
        let doc = yrs::Doc::new();
        {
            let bytes1 = base64_decode(&state1).unwrap();
            let bytes2 = base64_decode(&state2).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(yrs::Update::decode_v1(&bytes1).unwrap());
            txn.apply_update(yrs::Update::decode_v1(&bytes2).unwrap());
        }
        let txn = doc.transact();
        let array = txn.get_array(TEXT_ROOT_NAME).unwrap();
        assert_eq!(array.len(&txn), 2);
    }

    #[tokio::test]
    async fn test_append_event_creates_new_log_doc() {
        let store = Arc::new(DocumentStore::new());
        let service = EventLogService::new(store.clone());

        let entry = EventLogEntry::from_event("test:event", "unit-test", &json!({"data": 42}));

        // No existing event_log — should create a new doc
        let log_id = service.append_event(None, &entry).await.unwrap();

        // The returned ID should be a valid UUID
        assert!(uuid::Uuid::parse_str(&log_id).is_ok());

        // The doc should exist and contain our event
        let doc = store.get_document(&log_id).await.unwrap();
        assert_eq!(doc.content_type, ContentType::Jsonl);
        let lines: Vec<&str> = doc.content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1);
        let parsed: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["event_type"], "test:event");
        assert_eq!(parsed["source"], "unit-test");
    }

    #[tokio::test]
    async fn test_append_event_to_existing_log() {
        let store = Arc::new(DocumentStore::new());
        let service = EventLogService::new(store.clone());

        // Create first event
        let entry1 = EventLogEntry::from_event("first", "test", &json!({}));
        let log_id = service.append_event(None, &entry1).await.unwrap();

        // Append second event to same log
        let entry2 = EventLogEntry::from_event("second", "test", &json!({}));
        let log_id2 = service.append_event(Some(&log_id), &entry2).await.unwrap();
        assert_eq!(log_id, log_id2);

        // Should have 2 events
        let doc = store.get_document(&log_id).await.unwrap();
        let lines: Vec<&str> = doc.content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn test_broadcaster_notifies_on_append() {
        let store = Arc::new(DocumentStore::new());
        let broadcaster = EventBroadcaster::new(16);
        let mut receiver = broadcaster.subscribe();
        let service = EventLogService::new(store.clone()).with_broadcaster(broadcaster);

        let entry = EventLogEntry::from_event("test:event", "unit-test", &json!({"data": 1}));
        let log_id = service.append_event(None, &entry).await.unwrap();

        // Should receive the notification
        let notification = receiver.try_recv().unwrap();
        assert_eq!(notification.log_id, log_id);
        assert_eq!(notification.entry.event_type, "test:event");
        assert_eq!(notification.entry.source, "unit-test");
    }

    #[test]
    fn test_event_log_entry_from_event_message() {
        let payload = json!({"key": "value"});
        let entry = EventLogEntry::from_event("red:update", "bartleby", &payload);

        assert_eq!(entry.event_type, "red:update");
        assert_eq!(entry.source, "bartleby");
        assert_eq!(entry.payload, json!({"key": "value"}));
        // Timestamp should be set automatically
        assert!(!entry.timestamp.is_empty());
    }
}
