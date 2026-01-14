//! Commands port handler.
//!
//! Handles store-level commands like create-document, delete-document, get-content, and get-info.

use crate::document::{ContentType, DocumentStore};
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{
    CreateDocumentRequest, CreateDocumentResponse, DeleteDocumentRequest, DeleteDocumentResponse,
    GetContentRequest, GetContentResponse, GetInfoRequest, GetInfoResponse,
};
use crate::mqtt::{encode_json, parse_json, MqttError};
use rumqttc::QoS;
use std::sync::Arc;
use tracing::{debug, warn};

/// Handler for the commands port.
pub struct CommandsHandler {
    client: Arc<MqttClient>,
    document_store: Arc<DocumentStore>,
    workspace: String,
}

impl CommandsHandler {
    /// Create a new commands handler.
    pub fn new(
        client: Arc<MqttClient>,
        document_store: Arc<DocumentStore>,
        workspace: String,
    ) -> Self {
        Self {
            client,
            document_store,
            workspace,
        }
    }

    /// Publish a response to the {workspace}/responses topic.
    async fn publish_response<T: serde::Serialize>(&self, response: &T) -> Result<(), MqttError> {
        let payload = encode_json(response)?;

        let response_topic = format!("{}/responses", self.workspace);
        self.client
            .publish(&response_topic, &payload, QoS::AtLeastOnce)
            .await
    }

    /// Handle create-document command.
    /// Topic: {workspace}/commands/create-document
    pub async fn handle_create_document(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: CreateDocumentRequest = parse_json(payload)?;

        debug!(
            "Received create-document command: req={}, content_type={}",
            request.req, request.content_type
        );

        let response = match ContentType::from_mime(&request.content_type) {
            Some(content_type) => {
                let uuid = self.document_store.create_document(content_type).await;
                debug!(
                    "Created document with uuid={} for req={}",
                    uuid, request.req
                );
                CreateDocumentResponse {
                    req: request.req,
                    uuid: Some(uuid),
                    error: None,
                }
            }
            None => {
                warn!(
                    "Invalid content type '{}' for req={}",
                    request.content_type, request.req
                );
                CreateDocumentResponse {
                    req: request.req,
                    uuid: None,
                    error: Some(format!("Invalid content type: {}", request.content_type)),
                }
            }
        };

        self.publish_response(&response).await
    }

    /// Handle delete-document command.
    /// Topic: {workspace}/commands/delete-document
    pub async fn handle_delete_document(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: DeleteDocumentRequest = parse_json(payload)?;

        debug!(
            "Received delete-document command: req={}, id={}",
            request.req, request.id
        );

        let deleted = self.document_store.delete_document(&request.id).await;

        let response = DeleteDocumentResponse {
            req: request.req.clone(),
            deleted,
            error: if deleted {
                None
            } else {
                Some(format!("Document '{}' not found", request.id))
            },
        };

        debug!(
            "Delete-document response: deleted={} for req={}",
            deleted, request.req
        );

        self.publish_response(&response).await
    }

    /// Handle get-content command.
    /// Topic: {workspace}/commands/get-content
    pub async fn handle_get_content(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: GetContentRequest = parse_json(payload)?;

        debug!(
            "Received get-content command: req={}, id={}",
            request.req, request.id
        );

        let response = match self.document_store.get_document(&request.id).await {
            Some(doc) => GetContentResponse {
                req: request.req.clone(),
                content: Some(doc.content.clone()),
                content_type: Some(doc.content_type.to_mime().to_string()),
                error: None,
            },
            None => GetContentResponse {
                req: request.req.clone(),
                content: None,
                content_type: None,
                error: Some(format!("Document '{}' not found", request.id)),
            },
        };

        debug!("Get-content response for req={}", request.req);

        self.publish_response(&response).await
    }

    /// Handle get-info command.
    /// Topic: {workspace}/commands/get-info
    pub async fn handle_get_info(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: GetInfoRequest = parse_json(payload)?;

        debug!(
            "Received get-info command: req={}, id={}",
            request.req, request.id
        );

        let response = match self.document_store.get_document(&request.id).await {
            Some(doc) => GetInfoResponse {
                req: request.req.clone(),
                id: Some(request.id.clone()),
                content_type: Some(doc.content_type.to_mime().to_string()),
                error: None,
            },
            None => GetInfoResponse {
                req: request.req.clone(),
                id: None,
                content_type: None,
                error: Some(format!("Document '{}' not found", request.id)),
            },
        };

        debug!("Get-info response for req={}", request.req);

        self.publish_response(&response).await
    }
}
