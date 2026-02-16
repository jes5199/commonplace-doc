use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::GetString;
use yrs::ReadTxn;
use yrs::Transact;
use yrs::Value;
use yrs::WriteTxn;

// Re-export ContentType for backward compatibility
pub use crate::content_type::ContentType;

#[derive(Clone)]
pub struct Document {
    pub content: String,
    pub content_type: ContentType,
    /// Yrs document that powers collaborative edits. This is a `Y.Text` named `content`.
    pub ydoc: Option<yrs::Doc>,
}

pub struct DocumentStore {
    documents: Arc<RwLock<HashMap<String, Document>>>,
}

impl Default for DocumentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentStore {
    const TEXT_ROOT_NAME: &'static str = "content";
    const DEFAULT_YDOC_CLIENT_ID: u64 = 1;
    const XML_HEADER: &'static str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;
    const XML_ROOT_START: &'static str = "<root>";
    const XML_ROOT_END: &'static str = "</root>";

    pub fn new() -> Self {
        Self {
            documents: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_document(&self, content_type: ContentType) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let ydoc = yrs::Doc::with_client_id(Self::DEFAULT_YDOC_CLIENT_ID);
        match content_type {
            ContentType::Text => {
                ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);
            }
            ContentType::Json => {
                ydoc.get_or_insert_map(Self::TEXT_ROOT_NAME);
            }
            ContentType::JsonArray | ContentType::Jsonl => {
                ydoc.get_or_insert_array(Self::TEXT_ROOT_NAME);
            }
            ContentType::Xml => {
                ydoc.get_or_insert_xml_fragment(Self::TEXT_ROOT_NAME);
            }
        }

        let content = content_type.default_content();

        let doc = Document {
            content,
            content_type,
            ydoc: Some(ydoc),
        };

        let mut documents = self.documents.write().await;
        documents.insert(id.clone(), doc);

        id
    }

    /// Create a new document with a specific ID.
    /// If a document with this ID already exists, this is a no-op.
    pub async fn create_document_with_id(&self, id: String, content_type: ContentType) {
        // Check if document already exists
        {
            let documents = self.documents.read().await;
            if documents.contains_key(&id) {
                return;
            }
        }

        let ydoc = yrs::Doc::with_client_id(Self::DEFAULT_YDOC_CLIENT_ID);
        match content_type {
            ContentType::Text => {
                ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);
            }
            ContentType::Json => {
                ydoc.get_or_insert_map(Self::TEXT_ROOT_NAME);
            }
            ContentType::JsonArray | ContentType::Jsonl => {
                ydoc.get_or_insert_array(Self::TEXT_ROOT_NAME);
            }
            ContentType::Xml => {
                ydoc.get_or_insert_xml_fragment(Self::TEXT_ROOT_NAME);
            }
        }

        let content = content_type.default_content();

        let doc = Document {
            content,
            content_type,
            ydoc: Some(ydoc),
        };

        let mut documents = self.documents.write().await;
        documents.insert(id, doc);
    }

    pub async fn get_document(&self, id: &str) -> Option<Document> {
        let documents = self.documents.read().await;
        documents.get(id).cloned()
    }

    /// Get an existing document or create one with the given ID.
    /// Unlike create_document, this uses a specific ID instead of generating a new UUID.
    pub async fn get_or_create_with_id(&self, id: &str, content_type: ContentType) -> Document {
        // First check with read lock
        {
            let documents = self.documents.read().await;
            if let Some(doc) = documents.get(id) {
                return doc.clone();
            }
        }

        // Not found, create with write lock
        let mut documents = self.documents.write().await;

        // Double-check after acquiring write lock
        if let Some(doc) = documents.get(id) {
            return doc.clone();
        }

        // Create new document with this ID
        let ydoc = yrs::Doc::with_client_id(Self::DEFAULT_YDOC_CLIENT_ID);
        match content_type {
            ContentType::Text => {
                ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);
            }
            ContentType::Json => {
                ydoc.get_or_insert_map(Self::TEXT_ROOT_NAME);
            }
            ContentType::JsonArray | ContentType::Jsonl => {
                ydoc.get_or_insert_array(Self::TEXT_ROOT_NAME);
            }
            ContentType::Xml => {
                ydoc.get_or_insert_xml_fragment(Self::TEXT_ROOT_NAME);
            }
        }

        let content = content_type.default_content();

        let doc = Document {
            content,
            content_type,
            ydoc: Some(ydoc),
        };

        documents.insert(id.to_string(), doc.clone());
        doc
    }

    /// Get an existing document or create one with the given ID, without pre-initializing
    /// the Y.Doc root type. Use this when an incoming CRDT update will establish the type.
    ///
    /// This is needed because Yrs allows multiple types with the same name, so if we
    /// pre-create a YMap and then apply an update with YText at the same root name,
    /// they will coexist rather than the YText replacing the YMap.
    pub async fn get_or_create_with_id_uninit(
        &self,
        id: &str,
        content_type: ContentType,
    ) -> Document {
        // First check with read lock
        {
            let documents = self.documents.read().await;
            if let Some(doc) = documents.get(id) {
                return doc.clone();
            }
        }

        // Not found, create with write lock
        let mut documents = self.documents.write().await;

        // Double-check after acquiring write lock
        if let Some(doc) = documents.get(id) {
            return doc.clone();
        }

        // Create new document with this ID but don't initialize the Y.Doc root type
        // The incoming CRDT update will establish what type the root is
        let ydoc = yrs::Doc::with_client_id(Self::DEFAULT_YDOC_CLIENT_ID);
        // Note: we intentionally don't call get_or_insert_* here

        let content = content_type.default_content();

        let doc = Document {
            content,
            content_type,
            ydoc: Some(ydoc),
        };

        documents.insert(id.to_string(), doc.clone());
        doc
    }

    pub async fn delete_document(&self, id: &str) -> bool {
        let mut documents = self.documents.write().await;
        documents.remove(id).is_some()
    }

    pub async fn apply_yjs_update(&self, id: &str, update: &[u8]) -> Result<(), ApplyError> {
        let mut documents = self.documents.write().await;
        let doc = documents.get_mut(id).ok_or(ApplyError::NotFound)?;

        let ydoc = doc.ydoc.as_ref().ok_or(ApplyError::MissingYDoc)?.clone();
        let update =
            yrs::Update::decode_v1(update).map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?;

        let mut txn = ydoc.transact_mut();
        let content_type = doc.content_type;
        txn.apply_update(update);

        // Detect all root variants for "content". Yrs can retain multiple types with
        // the same name if a document was pre-initialized with one type and later
        // receives updates for another. Prefer structured roots when present.
        let mut root_map = None;
        let mut root_array = None;
        let mut root_text = None;
        for (name, value) in txn.root_refs() {
            if name != Self::TEXT_ROOT_NAME {
                continue;
            }
            match value {
                Value::YMap(map) => {
                    if root_map.is_none() {
                        root_map = Some(map);
                    }
                }
                Value::YArray(array) => {
                    if root_array.is_none() {
                        root_array = Some(array);
                    }
                }
                Value::YText(text) => {
                    if root_text.is_none() {
                        root_text = Some(text);
                    }
                }
                _ => {}
            }
        }

        doc.content = match content_type {
            ContentType::Text => {
                // Check if the Y.Doc actually has YMap (schema) instead of YText (file)
                if let Some(map) = root_map {
                    // This is actually a schema document, update content_type and extract as JSON
                    doc.content_type = ContentType::Json;
                    json_content_from_map(&txn, &map)?
                } else if let Some(array) = root_array {
                    // This is a JSONL document (YArray root), update content type.
                    doc.content_type = ContentType::Jsonl;
                    jsonl_content_from_array(&txn, &array)?
                } else if let Some(text) = root_text {
                    let text_content = text.get_string(&txn);
                    if !text_content.is_empty() {
                        text_content
                    } else {
                        // Fallback for mixed-root docs where root_refs selected an empty YText
                        // but structured content exists under the same root name.
                        if let Some(array) = txn.get_array(Self::TEXT_ROOT_NAME) {
                            let array_content = jsonl_content_from_array(&txn, &array)?;
                            if !array_content.is_empty() {
                                doc.content_type = ContentType::Jsonl;
                                array_content
                            } else if let Some(map) = txn.get_map(Self::TEXT_ROOT_NAME) {
                                let map_content = json_content_from_map(&txn, &map)?;
                                if map_content != ContentType::Json.default_content() {
                                    doc.content_type = ContentType::Json;
                                    map_content
                                } else {
                                    text_content
                                }
                            } else {
                                text_content
                            }
                        } else if let Some(map) = txn.get_map(Self::TEXT_ROOT_NAME) {
                            let map_content = json_content_from_map(&txn, &map)?;
                            if map_content != ContentType::Json.default_content() {
                                doc.content_type = ContentType::Json;
                                map_content
                            } else {
                                text_content
                            }
                        } else {
                            text_content
                        }
                    }
                } else {
                    // Try to get text directly
                    let text = txn.get_or_insert_text(Self::TEXT_ROOT_NAME);
                    let text_content = text.get_string(&txn);
                    if !text_content.is_empty() {
                        text_content
                    } else if let Some(array) = txn.get_array(Self::TEXT_ROOT_NAME) {
                        let array_content = jsonl_content_from_array(&txn, &array)?;
                        if !array_content.is_empty() {
                            doc.content_type = ContentType::Jsonl;
                            array_content
                        } else {
                            text_content
                        }
                    } else {
                        text_content
                    }
                }
            }
            ContentType::Json => {
                if let Some(map) = root_map {
                    json_content_from_map(&txn, &map)?
                } else if let Some(text) = root_text {
                    // Handle YText in case document was created as JSON but contains text content
                    // This happens when CRDT edits arrive before the document type is known
                    // Update content_type to reflect the actual type
                    doc.content_type = ContentType::Text;
                    text.get_string(&txn)
                } else {
                    // Try to get text directly in case root_refs doesn't match the expected pattern
                    let text = txn.get_or_insert_text(Self::TEXT_ROOT_NAME);
                    let content = text.get_string(&txn);
                    if !content.is_empty() {
                        content
                    } else {
                        ContentType::Json.default_content()
                    }
                }
            }
            ContentType::JsonArray => {
                if let Some(array) = root_array.or_else(|| txn.get_array(Self::TEXT_ROOT_NAME)) {
                    let any = array.to_json(&txn);
                    serde_json::to_string(&any)
                        .map_err(|e| ApplyError::Serialization(e.to_string()))?
                } else {
                    ContentType::JsonArray.default_content()
                }
            }
            ContentType::Jsonl => {
                if let Some(array) = root_array.or_else(|| txn.get_array(Self::TEXT_ROOT_NAME)) {
                    jsonl_content_from_array(&txn, &array)?
                } else {
                    ContentType::Jsonl.default_content()
                }
            }
            ContentType::Xml => {
                let fragment = txn.get_or_insert_xml_fragment(Self::TEXT_ROOT_NAME);
                let inner = fragment.get_string(&txn);
                Self::wrap_xml_root(&inner)
            }
        };

        Ok(())
    }

    /// Get the current Yjs state as bytes (for syncing).
    /// Returns the full document state encoded as a Yjs update.
    pub async fn get_yjs_state(&self, id: &str) -> Option<Vec<u8>> {
        let documents = self.documents.read().await;
        let doc = documents.get(id)?;
        let ydoc = doc.ydoc.as_ref()?;
        let txn = ydoc.transact();
        Some(txn.encode_state_as_update_v1(&yrs::StateVector::default()))
    }

    /// Set document content directly (for initialization/migration).
    ///
    /// This creates a Yjs update from the current content to the new content
    /// and applies it. Used by the reconciler for schema migration.
    ///
    /// The update method is content-type aware:
    /// - Text/Xml: uses character-level text diff (Y.Text operations)
    /// - Json/JsonArray: uses Y.Map/Y.Array operations to preserve JSON structure
    /// - Jsonl: uses Y.Array with line-based parsing for newline-delimited JSON
    pub async fn set_content(&self, id: &str, new_content: &str) -> Result<(), ApplyError> {
        use crate::sync::yjs::{base64_decode, base64_encode, create_yjs_structured_update};

        let mut documents = self.documents.write().await;
        let doc = documents.get_mut(id).ok_or(ApplyError::NotFound)?;
        let ydoc = doc.ydoc.as_ref().ok_or(ApplyError::MissingYDoc)?.clone();

        // Get current Yjs state as base64 for structured content types
        let base_state_b64 = {
            let txn = ydoc.transact();
            let state_bytes = txn.encode_state_as_update_v1(&yrs::StateVector::default());
            base64_encode(&state_bytes)
        };

        // Create update based on content type
        let update_bytes = match doc.content_type {
            ContentType::Text => {
                // Use text diff for plain text content
                use crate::diff;
                let diff_result = diff::compute_diff_update(&doc.content, new_content)
                    .map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?;
                diff_result.update_bytes
            }
            ContentType::Xml => {
                // Use XML diff for XmlFragment-based content
                use crate::diff;
                let diff_result = diff::compute_xml_diff_update(&doc.content, new_content)
                    .map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?;
                diff_result.update_bytes
            }
            ContentType::Json | ContentType::JsonArray | ContentType::Jsonl => {
                // Use create_yjs_structured_update to handle JSON/JSONL content types
                let update_b64 = create_yjs_structured_update(
                    doc.content_type,
                    new_content,
                    Some(&base_state_b64),
                )
                .map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?;
                base64_decode(&update_b64).map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?
            }
        };

        // Apply the update
        let update = yrs::Update::decode_v1(&update_bytes)
            .map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?;

        let mut txn = ydoc.transact_mut();
        txn.apply_update(update);

        // Update content
        doc.content = new_content.to_string();

        Ok(())
    }

    fn wrap_xml_root(inner: &str) -> String {
        if inner.is_empty() {
            return format!("{}<root/>", Self::XML_HEADER);
        }
        format!(
            "{}{}{}{}",
            Self::XML_HEADER,
            Self::XML_ROOT_START,
            inner,
            Self::XML_ROOT_END
        )
    }
}

fn json_content_from_map(
    txn: &yrs::TransactionMut<'_>,
    map: &yrs::MapRef,
) -> Result<String, ApplyError> {
    let any = map.to_json(txn);
    serde_json::to_string(&any).map_err(|e| ApplyError::Serialization(e.to_string()))
}

fn jsonl_content_from_array(
    txn: &yrs::TransactionMut<'_>,
    array: &yrs::ArrayRef,
) -> Result<String, ApplyError> {
    let any_array = array.to_json(txn);
    let json_value =
        serde_json::to_value(&any_array).map_err(|e| ApplyError::Serialization(e.to_string()))?;
    if let serde_json::Value::Array(items) = json_value {
        let mut content = items
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map(|lines| lines.join("\n"))
            .map_err(|e| ApplyError::Serialization(e.to_string()))?;
        if !content.is_empty() {
            content.push('\n');
        }
        Ok(content)
    } else {
        Ok(ContentType::Jsonl.default_content())
    }
}

/// Resolve a path to a document ID by parsing fs-root JSON content.
///
/// The fs-root uses a versioned schema:
/// ```json
/// {
///   "version": 1,
///   "root": {
///     "type": "dir",
///     "entries": {
///       "notes": { "type": "dir", "entries": { "todo.txt": { "type": "doc", "node_id": "abc-123" } } }
///     }
///   }
/// }
/// ```
///
/// Document ID is either:
/// - Explicit `node_id` in the schema
/// - Derived as `<fs_root_id>:<path>` if not specified
///
/// Returns None if path not found or JSON invalid.
pub fn resolve_path_to_uuid(fs_root_content: &str, path: &str, fs_root_id: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(fs_root_content).ok()?;

    // Parse the versioned schema
    let root = json.get("root")?;
    let root_type = root.get("type")?.as_str()?;
    if root_type != "dir" {
        return None;
    }

    // Navigate through the path
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let mut current = root;

    for (i, part) in parts.iter().enumerate() {
        let entries = current.get("entries")?;
        current = entries.get(*part)?;

        let entry_type = current.get("type")?.as_str()?;

        // If this is the last part and it's a doc, we found it
        if i == parts.len() - 1 {
            if entry_type != "doc" {
                return None; // Path points to a directory, not a document
            }
            // Return node_id if set, otherwise derive from path
            return current
                .get("node_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| Some(format!("{}:{}", fs_root_id, path)));
        }

        // Not the last part, so this should be a directory
        if entry_type != "dir" {
            return None;
        }
    }

    None
}

#[derive(Debug)]
pub enum ApplyError {
    NotFound,
    MissingYDoc,
    InvalidUpdate(String),
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_path_to_uuid() {
        let fs_root = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "notes": {
                        "type": "dir",
                        "entries": {
                            "todo.txt": { "type": "doc", "node_id": "abc-123" },
                            "ideas.md": { "type": "doc" }
                        }
                    },
                    "readme.txt": { "type": "doc", "node_id": "ghi-789" }
                }
            }
        }"#;

        // Explicit node_id is returned
        assert_eq!(
            resolve_path_to_uuid(fs_root, "notes/todo.txt", "fs-root.json"),
            Some("abc-123".to_string())
        );
        assert_eq!(
            resolve_path_to_uuid(fs_root, "readme.txt", "fs-root.json"),
            Some("ghi-789".to_string())
        );

        // Derived document ID when no node_id
        assert_eq!(
            resolve_path_to_uuid(fs_root, "notes/ideas.md", "fs-root.json"),
            Some("fs-root.json:notes/ideas.md".to_string())
        );

        // Path not found
        assert_eq!(
            resolve_path_to_uuid(fs_root, "nonexistent.txt", "fs-root.json"),
            None
        );

        // Path points to directory, not document
        assert_eq!(resolve_path_to_uuid(fs_root, "notes", "fs-root.json"), None);
    }

    #[tokio::test]
    async fn test_create_document_returns_uuid() {
        let store = DocumentStore::new();
        let uuid = store.create_document(ContentType::Text).await;

        // UUID should be valid format (36 chars with dashes)
        assert!(uuid.len() == 36);
        assert!(uuid.contains('-'));

        // Document should be retrievable
        let doc = store.get_document(&uuid).await;
        assert!(doc.is_some());
    }

    #[tokio::test]
    async fn test_set_content_json_preserves_structure() {
        // This test verifies CP-ia7 fix: set_content should use Y.Map for JSON,
        // not text diff which corrupts the version field.
        let store = DocumentStore::new();
        let id = store.create_document(ContentType::Json).await;

        // Set initial JSON content
        let initial = r#"{"version":1,"data":"test"}"#;
        store.set_content(&id, initial).await.unwrap();

        // Update to new content
        let updated = r#"{"version":1,"data":"updated","extra":"field"}"#;
        store.set_content(&id, updated).await.unwrap();

        // Verify the content is correct (not corrupted)
        let doc = store.get_document(&id).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&doc.content).unwrap();

        // The key test: version field should still be integer 1, not corrupted to "ve"
        assert_eq!(
            parsed["version"], 1,
            "version field should be integer 1, not corrupted"
        );
        assert_eq!(parsed["data"], "updated");
        assert_eq!(parsed["extra"], "field");
    }

    #[tokio::test]
    async fn test_set_content_text_uses_diff() {
        let store = DocumentStore::new();
        let id = store.create_document(ContentType::Text).await;

        store.set_content(&id, "hello").await.unwrap();
        store.set_content(&id, "hello world").await.unwrap();

        let doc = store.get_document(&id).await.unwrap();
        assert_eq!(doc.content, "hello world");
    }

    #[tokio::test]
    async fn test_set_content_json_array_preserves_structure() {
        // This test verifies JsonArray uses create_yjs_json_update (not create_yjs_jsonl_update)
        // which correctly handles array roots. Using JSONL would nest the array: [[1,2,3]].
        let store = DocumentStore::new();
        let id = store.create_document(ContentType::JsonArray).await;

        // Set JSON array content
        let initial = r#"[1,2,3]"#;
        store.set_content(&id, initial).await.unwrap();

        // Update to new array content
        let updated = r#"[1,2,3,4,5]"#;
        store.set_content(&id, updated).await.unwrap();

        // Verify the content is correct (not nested as [[1,2,3,4,5]])
        let doc = store.get_document(&id).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&doc.content).unwrap();

        // Should be a flat array, not nested
        assert!(parsed.is_array(), "should be an array");
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 5, "should have 5 elements, not be nested");
        assert_eq!(arr[0], 1);
        assert_eq!(arr[4], 5);
    }

    #[tokio::test]
    async fn test_apply_yjs_update_text_doc_handles_jsonl_array_root() {
        // Reproduces UUID-path ingestion where a document is pre-created as Text
        // and later receives JSONL (YArray) updates.
        let store = DocumentStore::new();
        let id = "jsonl-array-root-doc";
        store
            .create_document_with_id(id.to_string(), ContentType::Text)
            .await;

        let expected = "{\"id\":1}\n{\"id\":2}\n";
        let update_b64 = crate::sync::yjs::create_yjs_jsonl_update(expected, None).unwrap();
        let update_bytes = crate::sync::yjs::base64_decode(&update_b64).unwrap();

        store.apply_yjs_update(id, &update_bytes).await.unwrap();
        let doc = store.get_document(id).await.unwrap();

        assert_eq!(doc.content_type, ContentType::Jsonl);
        assert_eq!(doc.content, expected);
    }

    #[tokio::test]
    async fn test_apply_yjs_update_with_captured_cp_1876_state() {
        // Captured from failing CP-1876 orchestrator run:
        // /docs/{uuid}/head returned content="", but this state should decode to 3 JSONL lines.
        let captured_state = "AQIBAAEBB2NvbnRlbnQDiAECA3YCAmlkegAAAAAAAAABB21lc3NhZ2V3CmZpcnN0IGxpbmV2AgdtZXNzYWdldwtzZWNvbmQgbGluZQJpZHoAAAAAAAAAAnYCAmlkegAAAAAAAAADB21lc3NhZ2V3F3RoaXJkIGxpbmUgZnJvbSBzYW5kYm94AQEBAAM=";
        let update_bytes = crate::sync::yjs::base64_decode(captured_state).unwrap();

        let store = DocumentStore::new();
        let id = "captured-cp-1876-doc";
        store
            .create_document_with_id(id.to_string(), ContentType::Text)
            .await;

        store.apply_yjs_update(id, &update_bytes).await.unwrap();
        let doc = store.get_document(id).await.unwrap();

        assert_eq!(doc.content_type, ContentType::Jsonl);
        assert!(doc.content.contains("first line"));
        assert!(doc.content.contains("second line"));
        assert!(doc.content.contains("third line from sandbox"));
    }
}
