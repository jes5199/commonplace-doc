use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use yrs::updates::decoder::Decode;
use yrs::GetString;
use yrs::Transact;

#[derive(Clone, Debug)]
pub enum ContentType {
    Json,
    Xml,
    Text,
}

impl ContentType {
    pub fn from_mime(mime: &str) -> Option<Self> {
        match mime {
            "application/json" => Some(ContentType::Json),
            "application/xml" | "text/xml" => Some(ContentType::Xml),
            "text/plain" => Some(ContentType::Text),
            _ => None,
        }
    }

    pub fn to_mime(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::Xml => "application/xml",
            ContentType::Text => "text/plain",
        }
    }

    pub fn default_content(&self) -> String {
        match self {
            ContentType::Json => "{}".to_string(),
            ContentType::Xml => r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#.to_string(),
            ContentType::Text => String::new(),
        }
    }
}

#[derive(Clone)]
pub struct Document {
    pub content: String,
    pub content_type: ContentType,
    /// For `ContentType::Text`, this holds the Yrs document that powers collaborative edits.
    /// For other content types this is `None`.
    pub ydoc: Option<yrs::Doc>,
}

pub struct DocumentStore {
    documents: Arc<RwLock<HashMap<String, Document>>>,
}

impl DocumentStore {
    const TEXT_ROOT_NAME: &'static str = "content";

    pub fn new() -> Self {
        Self {
            documents: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_document(&self, content_type: ContentType) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let doc = match content_type {
            ContentType::Text => {
                let ydoc = yrs::Doc::new();
                ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);

                Document {
                    content: String::new(),
                    content_type: ContentType::Text,
                    ydoc: Some(ydoc),
                }
            }
            other => Document {
                content: other.default_content(),
                content_type: other,
                ydoc: None,
            },
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

    pub async fn apply_text_update(&self, id: &str, update: &[u8]) -> Result<(), ApplyError> {
        let mut documents = self.documents.write().await;
        let doc = documents.get_mut(id).ok_or(ApplyError::NotFound)?;

        if !matches!(doc.content_type, ContentType::Text) {
            return Err(ApplyError::WrongContentType);
        }

        let ydoc = doc.ydoc.as_ref().ok_or(ApplyError::MissingYDoc)?.clone();
        let update =
            yrs::Update::decode_v1(update).map_err(|e| ApplyError::InvalidUpdate(e.to_string()))?;

        let text = ydoc.get_or_insert_text(Self::TEXT_ROOT_NAME);
        let mut txn = ydoc.transact_mut();
        txn.apply_update(update);

        doc.content = text.get_string(&txn);

        Ok(())
    }
}

#[derive(Debug)]
pub enum ApplyError {
    NotFound,
    WrongContentType,
    MissingYDoc,
    InvalidUpdate(String),
}
