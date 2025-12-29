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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentType {
    Json,
    JsonArray,
    Xml,
    Text,
}

impl ContentType {
    pub fn from_mime(mime: &str) -> Option<Self> {
        let mut parts = mime.split(';').map(|part| part.trim());
        let base = parts.next().unwrap_or_default();
        let params: Vec<&str> = parts.collect();

        match base {
            "application/json" => {
                if params.iter().any(|p| p.eq_ignore_ascii_case("root=array")) {
                    Some(ContentType::JsonArray)
                } else {
                    Some(ContentType::Json)
                }
            }
            "application/xml" | "text/xml" => Some(ContentType::Xml),
            "text/plain" => Some(ContentType::Text),
            _ => None,
        }
    }

    pub fn to_mime(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::JsonArray => "application/json;root=array",
            ContentType::Xml => "application/xml",
            ContentType::Text => "text/plain",
        }
    }

    pub fn default_content(&self) -> String {
        match self {
            ContentType::Json => "{}".to_string(),
            ContentType::JsonArray => "[]".to_string(),
            ContentType::Xml => r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#.to_string(),
            ContentType::Text => String::new(),
        }
    }
}

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
            ContentType::JsonArray => {
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

    pub async fn get_document(&self, id: &str) -> Option<Document> {
        let documents = self.documents.read().await;
        documents.get(id).cloned()
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
        let content_type = doc.content_type.clone();
        txn.apply_update(update);

        doc.content = match content_type {
            ContentType::Text => {
                let text = txn.get_or_insert_text(Self::TEXT_ROOT_NAME);
                text.get_string(&txn)
            }
            ContentType::Json => {
                let root = txn
                    .root_refs()
                    .find(|(name, _)| *name == Self::TEXT_ROOT_NAME)
                    .map(|(_, value)| value);

                match root {
                    Some(Value::YMap(map)) => {
                        let any = map.to_json(&txn);
                        serde_json::to_string(&any)
                            .map_err(|e| ApplyError::Serialization(e.to_string()))?
                    }
                    _ => ContentType::Json.default_content(),
                }
            }
            ContentType::JsonArray => {
                let root = txn
                    .root_refs()
                    .find(|(name, _)| *name == Self::TEXT_ROOT_NAME)
                    .map(|(_, value)| value);

                match root {
                    Some(Value::YArray(array)) => {
                        let any = array.to_json(&txn);
                        serde_json::to_string(&any)
                            .map_err(|e| ApplyError::Serialization(e.to_string()))?
                    }
                    _ => ContentType::JsonArray.default_content(),
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

/// Resolve a path to a UUID by parsing fs-root JSON content.
/// Returns None if path not found or JSON invalid.
pub fn resolve_path_to_uuid(fs_root_content: &str, path: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(fs_root_content).ok()?;

    let parts: Vec<&str> = path.split('/').collect();
    let mut current = &json;

    for part in parts {
        current = current.get(part)?;
    }

    // The leaf should have a _uuid field
    current.get("_uuid")?.as_str().map(|s| s.to_string())
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
            "notes": {
                "todo.txt": {"_uuid": "abc-123"},
                "ideas.md": {"_uuid": "def-456"}
            },
            "readme.txt": {"_uuid": "ghi-789"}
        }"#;

        assert_eq!(
            resolve_path_to_uuid(fs_root, "notes/todo.txt"),
            Some("abc-123".to_string())
        );
        assert_eq!(
            resolve_path_to_uuid(fs_root, "readme.txt"),
            Some("ghi-789".to_string())
        );
        assert_eq!(resolve_path_to_uuid(fs_root, "nonexistent.txt"), None);
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
}
