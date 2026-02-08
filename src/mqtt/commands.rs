//! Commands port handler.
//!
//! Handles store-level commands like create-document, delete-document, get-content,
//! get-info, and fork-document.

use crate::b64;
use crate::commit::Commit;
use crate::document::{ContentType, DocumentStore};
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{
    CreateDocumentRequest, CreateDocumentResponse, DeleteDocumentRequest, DeleteDocumentResponse,
    ForkDocumentRequest, ForkDocumentResponse, GetContentRequest, GetContentResponse,
    GetInfoRequest, GetInfoResponse,
};
use crate::mqtt::{encode_json, parse_json, MqttError};
use crate::replay::CommitReplayer;
use crate::store::CommitStore;
use rumqttc::QoS;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Handler for the commands port.
pub struct CommandsHandler {
    client: Arc<MqttClient>,
    document_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    workspace: String,
}

impl CommandsHandler {
    /// Create a new commands handler.
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
    /// Topic: {workspace}/commands/__system/create-document
    pub async fn handle_create_document(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: CreateDocumentRequest = parse_json(payload)?;

        debug!(
            "Received create-document command: req={}, content_type={}",
            request.req, request.content_type
        );

        let response = match ContentType::from_mime(&request.content_type) {
            Some(content_type) => {
                let uuid = if let Some(ref custom_id) = request.id {
                    self.document_store
                        .create_document_with_id(custom_id.clone(), content_type)
                        .await;
                    custom_id.clone()
                } else {
                    self.document_store.create_document(content_type).await
                };
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
    /// Topic: {workspace}/commands/__system/delete-document
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
    /// Topic: {workspace}/commands/__system/get-content
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
    /// Topic: {workspace}/commands/__system/get-info
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

    /// Handle fork-document command.
    /// Topic: {workspace}/commands/__system/fork-document
    pub async fn handle_fork_document(&self, payload: &[u8]) -> Result<(), MqttError> {
        let request: ForkDocumentRequest = parse_json(payload)?;

        debug!(
            "Received fork-document command: req={}, source_id={}, at_commit={:?}",
            request.req, request.source_id, request.at_commit
        );

        let commit_store = match &self.commit_store {
            Some(cs) => cs,
            None => {
                let response = ForkDocumentResponse {
                    req: request.req,
                    id: None,
                    head: None,
                    error: Some("Fork requires persistence (--database)".to_string()),
                };
                return self.publish_response(&response).await;
            }
        };

        // Get source document
        let source_doc = match self.document_store.get_document(&request.source_id).await {
            Some(doc) => doc,
            None => {
                let response = ForkDocumentResponse {
                    req: request.req,
                    id: None,
                    head: None,
                    error: Some(format!("Source document '{}' not found", request.source_id)),
                };
                return self.publish_response(&response).await;
            }
        };

        // Create new document with same content type
        let new_id = self
            .document_store
            .create_document(source_doc.content_type)
            .await;

        // Get target commit (specified or HEAD)
        let target_cid = match request.at_commit {
            Some(cid) => cid,
            None => match commit_store.get_document_head(&request.source_id).await {
                Ok(Some(cid)) => cid,
                Ok(None) => {
                    let response = ForkDocumentResponse {
                        req: request.req,
                        id: None,
                        head: None,
                        error: Some(format!("No HEAD commit for source '{}'", request.source_id)),
                    };
                    return self.publish_response(&response).await;
                }
                Err(e) => {
                    let response = ForkDocumentResponse {
                        req: request.req,
                        id: None,
                        head: None,
                        error: Some(format!("Failed to get HEAD: {}", e)),
                    };
                    return self.publish_response(&response).await;
                }
            },
        };

        // Replay commits to build state
        let replayer = CommitReplayer::new(commit_store);

        // Verify commit belongs to source document's history
        match replayer
            .verify_commit_in_history(&request.source_id, &target_cid)
            .await
        {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                let response = ForkDocumentResponse {
                    req: request.req,
                    id: None,
                    head: None,
                    error: Some(format!(
                        "Commit {} not in history of {}",
                        target_cid, request.source_id
                    )),
                };
                return self.publish_response(&response).await;
            }
        }

        let (_content, state_bytes) = match replayer
            .get_content_and_state_at_commit(
                &request.source_id,
                &target_cid,
                &source_doc.content_type,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let response = ForkDocumentResponse {
                    req: request.req,
                    id: None,
                    head: None,
                    error: Some(format!("Failed to replay state: {}", e)),
                };
                return self.publish_response(&response).await;
            }
        };

        // Apply replayed state to new document
        if let Err(e) = self
            .document_store
            .apply_yjs_update(&new_id, &state_bytes)
            .await
        {
            let response = ForkDocumentResponse {
                req: request.req,
                id: None,
                head: None,
                error: Some(format!("Failed to apply state: {:?}", e)),
            };
            return self.publish_response(&response).await;
        }

        // Create root commit for forked document
        let update_b64 = b64::encode(&state_bytes);
        let commit = Commit::new(
            vec![],
            update_b64,
            "fork".to_string(),
            Some(format!(
                "Forked from {} at {}",
                request.source_id, target_cid
            )),
        );

        let response = match commit_store
            .store_commit_and_set_head(&new_id, &commit)
            .await
        {
            Ok((new_cid, _timestamp)) => {
                info!(
                    "Forked document {} -> {} (at commit {})",
                    request.source_id,
                    new_id,
                    &target_cid[..8.min(target_cid.len())]
                );
                ForkDocumentResponse {
                    req: request.req,
                    id: Some(new_id),
                    head: Some(new_cid),
                    error: None,
                }
            }
            Err(e) => ForkDocumentResponse {
                req: request.req,
                id: None,
                head: None,
                error: Some(format!("Failed to store commit: {}", e)),
            },
        };

        self.publish_response(&response).await
    }
}
