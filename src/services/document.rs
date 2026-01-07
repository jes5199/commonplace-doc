//! Document service layer.
//!
//! This module contains the business logic for document operations,
//! separating it from HTTP handler concerns. The service orchestrates
//! between DocumentStore, CommitStore, and CommitBroadcaster.

use std::sync::Arc;

use tracing::debug;

use crate::commit::Commit;
use crate::document::{ApplyError, ContentType, Document, DocumentStore};
use crate::events::{CommitBroadcaster, CommitNotification};
use crate::fs::FilesystemReconciler;
use crate::store::CommitStore;
use crate::sync::{base64_decode, create_yjs_json_update, create_yjs_jsonl_update};
use crate::{b64, diff, replay::CommitReplayer};

fn preview_text(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

/// Errors that can occur in service operations.
#[derive(Debug)]
pub enum ServiceError {
    /// Document not found
    NotFound,
    /// Commit store not available (persistence disabled)
    NoPersistence,
    /// Invalid input data
    InvalidInput(String),
    /// Internal error
    Internal(String),
    /// Conflict (e.g., concurrent modification)
    Conflict,
}

impl From<ApplyError> for ServiceError {
    fn from(e: ApplyError) -> Self {
        match e {
            ApplyError::NotFound => ServiceError::NotFound,
            ApplyError::MissingYDoc => ServiceError::Internal("Missing YDoc".to_string()),
            ApplyError::InvalidUpdate(msg) => ServiceError::InvalidInput(msg),
            ApplyError::Serialization(msg) => ServiceError::Internal(msg),
        }
    }
}

/// Result of a document edit operation.
pub struct EditResult {
    /// Commit ID of the created commit
    pub cid: String,
    /// Timestamp of the commit
    pub timestamp: u64,
}

/// Result of creating a commit with optional merge.
pub struct CommitResult {
    /// The main commit CID (edit commit if merging)
    pub cid: String,
    /// The merge commit CID (if a merge was required)
    pub merge_cid: Option<String>,
}

/// Result of a replace operation.
pub struct ReplaceResult {
    /// The merge/head commit CID
    pub cid: String,
    /// The edit commit CID
    pub edit_cid: String,
    /// Summary statistics
    pub chars_inserted: usize,
    pub chars_deleted: usize,
    pub operations: usize,
}

/// Result of a fork operation.
pub struct ForkResult {
    /// ID of the new document
    pub id: String,
    /// HEAD commit of the new document
    pub head: String,
}

/// Document head information.
pub struct HeadInfo {
    /// Current commit ID (if persistence enabled)
    pub cid: Option<String>,
    /// Document content
    pub content: String,
    /// Base64-encoded Yjs state (if available)
    pub state: Option<String>,
}

/// Service for document operations.
///
/// Encapsulates business logic for document CRUD and commit operations,
/// orchestrating between stores and handling broadcasting.
pub struct DocumentService {
    doc_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    commit_broadcaster: Option<CommitBroadcaster>,
    /// Filesystem reconciler (if fs-root is configured)
    reconciler: Option<Arc<FilesystemReconciler>>,
    /// Filesystem root document ID (for triggering reconciliation)
    fs_root_id: Option<String>,
}

impl DocumentService {
    /// Create a new document service.
    pub fn new(
        doc_store: Arc<DocumentStore>,
        commit_store: Option<Arc<CommitStore>>,
        commit_broadcaster: Option<CommitBroadcaster>,
    ) -> Self {
        Self {
            doc_store,
            commit_store,
            commit_broadcaster,
            reconciler: None,
            fs_root_id: None,
        }
    }

    /// Create a new document service with filesystem reconciler.
    pub fn with_reconciler(
        doc_store: Arc<DocumentStore>,
        commit_store: Option<Arc<CommitStore>>,
        commit_broadcaster: Option<CommitBroadcaster>,
        reconciler: Arc<FilesystemReconciler>,
        fs_root_id: String,
    ) -> Self {
        Self {
            doc_store,
            commit_store,
            commit_broadcaster,
            reconciler: Some(reconciler),
            fs_root_id: Some(fs_root_id),
        }
    }

    /// Broadcast a commit notification.
    fn broadcast_commit(&self, doc_id: &str, commit_id: &str, timestamp: u64) {
        if let Some(broadcaster) = self.commit_broadcaster.as_ref() {
            broadcaster.notify(CommitNotification {
                doc_id: doc_id.to_string(),
                commit_id: commit_id.to_string(),
                timestamp,
            });
        }
    }

    /// Broadcast multiple commit notifications.
    fn broadcast_commits(&self, doc_id: &str, commits: &[(String, u64)]) {
        for (commit_id, timestamp) in commits {
            self.broadcast_commit(doc_id, commit_id, *timestamp);
        }
    }

    // ========================================================================
    // Basic CRUD operations
    // ========================================================================

    /// Create a new document with the specified content type.
    pub async fn create_document(&self, content_type: ContentType) -> String {
        self.doc_store.create_document(content_type).await
    }

    /// Get a document by ID.
    pub async fn get_document(&self, id: &str) -> Result<Document, ServiceError> {
        self.doc_store
            .get_document(id)
            .await
            .ok_or(ServiceError::NotFound)
    }

    /// Delete a document by ID.
    pub async fn delete_document(&self, id: &str) -> bool {
        self.doc_store.delete_document(id).await
    }

    // ========================================================================
    // HEAD operations
    // ========================================================================

    /// Get document HEAD information.
    ///
    /// If `at_commit` is specified, replays history to return state at that commit.
    pub async fn get_head(
        &self,
        id: &str,
        at_commit: Option<&str>,
    ) -> Result<HeadInfo, ServiceError> {
        let doc = self.get_document(id).await?;

        // If at_commit is specified, replay commits to get historical state
        if let Some(target_cid) = at_commit {
            let commit_store = self
                .commit_store
                .as_ref()
                .ok_or(ServiceError::NoPersistence)?;
            let replayer = CommitReplayer::new(commit_store);

            // Verify the commit belongs to this document's history
            if !replayer
                .verify_commit_in_history(id, target_cid)
                .await
                .map_err(|_| ServiceError::NotFound)?
            {
                return Err(ServiceError::NotFound);
            }

            let (content, state_bytes) = replayer
                .get_content_and_state_at_commit(id, target_cid, &doc.content_type)
                .await
                .map_err(|_| ServiceError::NotFound)?;

            return Ok(HeadInfo {
                cid: Some(target_cid.to_string()),
                content,
                state: Some(b64::encode(&state_bytes)),
            });
        }

        // Default: return current HEAD
        let cid = if let Some(store) = &self.commit_store {
            store.get_document_head(id).await.ok().flatten()
        } else {
            None
        };

        let state_bytes = self.doc_store.get_yjs_state(id).await;
        let state_b64 = state_bytes.map(|b| b64::encode(&b));

        Ok(HeadInfo {
            cid,
            content: doc.content,
            state: state_b64,
        })
    }

    // ========================================================================
    // Edit operations
    // ========================================================================

    /// Apply an edit to a document.
    ///
    /// Creates a commit, applies the Yjs update, sets HEAD, and broadcasts.
    pub async fn edit_document(
        &self,
        id: &str,
        update_b64: &str,
        author: Option<String>,
        message: Option<String>,
    ) -> Result<EditResult, ServiceError> {
        let commit_store = self
            .commit_store
            .as_ref()
            .ok_or(ServiceError::NoPersistence)?;

        // Verify document exists
        self.get_document(id).await?;

        let update_bytes = b64::decode(update_b64)
            .map_err(|_| ServiceError::InvalidInput("Invalid base64".to_string()))?;

        // Get current head
        let current_head = commit_store
            .get_document_head(id)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        let parents = current_head.into_iter().collect();
        let author = author.unwrap_or_else(|| "anonymous".to_string());

        let commit = Commit::new(parents, update_b64.to_string(), author, message);
        let timestamp = commit.timestamp;

        let cid = commit_store
            .store_commit(&commit)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        // Apply update to document
        self.doc_store.apply_yjs_update(id, &update_bytes).await?;

        // Update head
        commit_store
            .set_document_head(id, &cid)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        self.broadcast_commit(id, &cid, timestamp);

        // Trigger filesystem reconciliation if this is the fs-root document
        self.maybe_reconcile(id).await;

        Ok(EditResult { cid, timestamp })
    }

    /// Trigger filesystem reconciliation if the edited document is the fs-root
    /// or a node-backed subdirectory.
    ///
    /// When a node-backed subdirectory document is updated, we:
    /// 1. Migrate the subdirectory document itself (generate UUIDs for entries)
    /// 2. Re-reconcile the fs-root to discover any new UUIDs
    async fn maybe_reconcile(&self, doc_id: &str) {
        if let (Some(reconciler), Some(fs_root_id)) = (&self.reconciler, &self.fs_root_id) {
            let is_subdirectory = if doc_id == fs_root_id {
                // fs-root was edited directly
                false
            } else {
                // Check if this is a known node-backed directory
                // (created by a previous reconciliation, now being populated with its schema)
                reconciler.is_node_backed_directory(doc_id).await
            };

            let should_reconcile = doc_id == fs_root_id || is_subdirectory;

            if is_subdirectory {
                // First, migrate this subdirectory document to generate UUIDs for its entries
                if let Err(e) = reconciler.migrate_subdirectory_document(doc_id).await {
                    tracing::warn!(
                        "Failed to migrate subdirectory document {}: {:?}",
                        doc_id,
                        e
                    );
                }
            }

            if should_reconcile {
                // Get the fs-root content for reconciliation
                if let Some(content) = reconciler.get_fs_root_content().await {
                    if !content.is_empty() && content != "{}" {
                        if let Err(e) = reconciler.reconcile(&content).await {
                            tracing::warn!("Filesystem reconciliation failed: {}", e);
                        } else if doc_id == fs_root_id {
                            tracing::debug!(
                                "Filesystem reconciliation completed for fs-root {}",
                                doc_id
                            );
                        } else {
                            tracing::info!(
                                "Filesystem reconciliation triggered by subdirectory {} update",
                                doc_id
                            );
                        }
                    }
                }
            }
        }
    }

    /// Create a commit with optional merge handling.
    ///
    /// This handles the complex case where `parent_cid` specifies a non-HEAD parent,
    /// requiring creation of both an edit commit and a merge commit.
    pub async fn create_commit(
        &self,
        id: &str,
        update_b64: &str,
        author: String,
        message: Option<String>,
        parent_cid: Option<String>,
    ) -> Result<CommitResult, ServiceError> {
        let commit_store = self
            .commit_store
            .as_ref()
            .ok_or(ServiceError::NoPersistence)?;

        // Verify document exists
        self.get_document(id).await?;

        let author = if author.is_empty() {
            "anonymous".to_string()
        } else {
            author
        };

        let update_bytes = b64::decode(update_b64)
            .map_err(|_| ServiceError::InvalidInput("Invalid base64".to_string()))?;

        let current_head = commit_store
            .get_document_head(id)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        let mut notifications: Vec<(String, u64)> = Vec::new();

        let (commit_cid, merge_cid) = if let Some(parent) = parent_cid {
            // Case: Create edit commit + merge commit
            let edit_commit = Commit::new(
                vec![parent.clone()],
                update_b64.to_string(),
                author.clone(),
                message.clone(),
            );
            let edit_timestamp = edit_commit.timestamp;

            let edit_cid = commit_store
                .store_commit(&edit_commit)
                .await
                .map_err(|e| ServiceError::Internal(e.to_string()))?;
            notifications.push((edit_cid.clone(), edit_timestamp));

            // Then create a merge commit with both the edit and current head as parents
            let merge_parents = if let Some(head_cid) = current_head.as_ref() {
                vec![edit_cid.clone(), head_cid.clone()]
            } else {
                // No current head - just make the edit commit the head
                self.doc_store.apply_yjs_update(id, &update_bytes).await?;

                commit_store
                    .set_document_head(id, &edit_cid)
                    .await
                    .map_err(|e| ServiceError::Internal(e.to_string()))?;

                self.broadcast_commits(id, &notifications);
                return Ok(CommitResult {
                    cid: edit_cid,
                    merge_cid: None,
                });
            };

            let merge_commit = Commit::new(
                merge_parents,
                String::new(), // Empty update for merge
                author,
                Some("Merge commit".to_string()),
            );
            let merge_timestamp = merge_commit.timestamp;

            let merge_cid = commit_store
                .store_commit(&merge_commit)
                .await
                .map_err(|e| ServiceError::Internal(e.to_string()))?;
            notifications.push((merge_cid.clone(), merge_timestamp));

            // Validate monotonic descent for the merge commit
            commit_store
                .validate_monotonic_descent(id, &merge_cid)
                .await
                .map_err(|_| ServiceError::Conflict)?;

            self.doc_store.apply_yjs_update(id, &update_bytes).await?;

            commit_store
                .set_document_head(id, &merge_cid)
                .await
                .map_err(|e| ServiceError::Internal(e.to_string()))?;

            (edit_cid, Some(merge_cid))
        } else {
            // Case: Simple commit
            let parents = current_head.clone().into_iter().collect();
            let commit = Commit::new(parents, update_b64.to_string(), author, message);
            let commit_timestamp = commit.timestamp;

            let cid = commit_store
                .store_commit(&commit)
                .await
                .map_err(|e| ServiceError::Internal(e.to_string()))?;

            // Validate monotonic descent
            commit_store
                .validate_monotonic_descent(id, &cid)
                .await
                .map_err(|_| ServiceError::Conflict)?;

            self.doc_store.apply_yjs_update(id, &update_bytes).await?;

            commit_store
                .set_document_head(id, &cid)
                .await
                .map_err(|e| ServiceError::Internal(e.to_string()))?;

            notifications.push((cid.clone(), commit_timestamp));
            (cid, None)
        };

        self.broadcast_commits(id, &notifications);

        Ok(CommitResult {
            cid: commit_cid,
            merge_cid,
        })
    }

    /// Fork a document, optionally at a specific commit.
    pub async fn fork_document(
        &self,
        source_id: &str,
        at_commit: Option<String>,
    ) -> Result<ForkResult, ServiceError> {
        let commit_store = self
            .commit_store
            .as_ref()
            .ok_or(ServiceError::NoPersistence)?;

        let source_doc = self.get_document(source_id).await?;

        // Create new document with same content type
        let new_id = self
            .doc_store
            .create_document(source_doc.content_type.clone())
            .await;

        // Get target commit (specified or HEAD)
        let target_cid = if let Some(cid) = at_commit {
            cid
        } else {
            commit_store
                .get_document_head(source_id)
                .await
                .map_err(|e| ServiceError::Internal(e.to_string()))?
                .ok_or(ServiceError::NotFound)?
        };

        // Replay commits to build state
        let replayer = CommitReplayer::new(commit_store);

        // Verify the commit belongs to the source document's history
        if !replayer
            .verify_commit_in_history(source_id, &target_cid)
            .await
            .map_err(|_| ServiceError::NotFound)?
        {
            return Err(ServiceError::NotFound);
        }

        let (_content, state_bytes) = replayer
            .get_content_and_state_at_commit(source_id, &target_cid, &source_doc.content_type)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        // Apply the replayed state to the new document
        self.doc_store
            .apply_yjs_update(&new_id, &state_bytes)
            .await?;

        // Create a root commit for the forked document
        let update_b64 = b64::encode(&state_bytes);
        let commit = Commit::new(
            vec![],
            update_b64,
            "fork".to_string(),
            Some(format!("Forked from {} at {}", source_id, target_cid)),
        );

        let new_cid = commit_store
            .store_commit(&commit)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        commit_store
            .set_document_head(&new_id, &new_cid)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        Ok(ForkResult {
            id: new_id,
            head: new_cid,
        })
    }

    /// Replace document content with diff computation.
    ///
    /// Handles optional parent_cid for offline sync scenarios where the client's
    /// view may have diverged from HEAD.
    pub async fn replace_content(
        &self,
        id: &str,
        new_content: &str,
        parent_cid: Option<String>,
        author: Option<String>,
    ) -> Result<ReplaceResult, ServiceError> {
        let commit_store = self
            .commit_store
            .as_ref()
            .ok_or(ServiceError::NoPersistence)?;

        let doc = self.get_document(id).await?;

        // Get current HEAD to check if parent_cid differs
        let current_head = commit_store
            .get_document_head(id)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        // Check if this is a JSON document type (uses Y.Map/Y.Array instead of Y.Text)
        let is_json_type = matches!(
            doc.content_type,
            ContentType::Json | ContentType::JsonArray | ContentType::Jsonl
        );

        // Check if this is an XML document (uses Y.XmlFragment)
        let is_xml_type = matches!(doc.content_type, ContentType::Xml);

        // Determine parents and compute diff appropriately
        let (diff_result, parents) = if let Some(ref parent) = parent_cid {
            // Check if parent differs from current HEAD
            let parent_differs = current_head.as_ref() != Some(parent);

            if parent_differs {
                // Parent differs from HEAD - use historical state for proper CRDT merge
                debug!(
                    "MERGE PATH: parent {} differs from HEAD {:?} for doc {}",
                    parent,
                    current_head.as_ref().map(|s| &s[..8.min(s.len())]),
                    id
                );
                let replayer = CommitReplayer::new(commit_store);

                // Verify the commit belongs to this document's history
                if !replayer
                    .verify_commit_in_history(id, parent)
                    .await
                    .map_err(|_| ServiceError::NotFound)?
                {
                    return Err(ServiceError::NotFound);
                }

                let (old_content, base_state_bytes) = replayer
                    .get_content_and_state_at_commit(id, parent, &doc.content_type)
                    .await
                    .map_err(|_| ServiceError::NotFound)?;

                debug!(
                    "MERGE: parent content = {:?} ({} bytes), new content = {:?} ({} bytes), base_state = {} bytes",
                    preview_text(&old_content, 50),
                    old_content.len(),
                    preview_text(new_content, 50),
                    new_content.len(),
                    base_state_bytes.len()
                );

                let diff = if is_json_type {
                    // JSON documents use Y.Map/Y.Array - use JSON update with base state
                    let base_state_b64 = b64::encode(&base_state_bytes);
                    let is_jsonl = doc.content_type == ContentType::Jsonl;
                    compute_json_diff(new_content, &old_content, Some(&base_state_b64), is_jsonl)?
                } else if is_xml_type {
                    // XML documents use Y.XmlFragment - use base state for proper CRDT merge
                    diff::compute_xml_diff_update_with_base(
                        &base_state_bytes,
                        &old_content,
                        new_content,
                    )
                    .map_err(|e| ServiceError::Internal(e.to_string()))?
                } else {
                    // Text documents use Y.Text - use character-level diff
                    diff::compute_diff_update_with_base(
                        &base_state_bytes,
                        &old_content,
                        new_content,
                    )
                    .map_err(|e| ServiceError::Internal(e.to_string()))?
                };

                debug!(
                    "MERGE: diff computed - {} inserted, {} deleted, {} ops, update_bytes = {} bytes",
                    diff.summary.chars_inserted,
                    diff.summary.chars_deleted,
                    diff.operation_count,
                    diff.update_bytes.len()
                );

                // Include BOTH parents for merge commit
                let mut merge_parents = vec![parent.clone()];
                if let Some(ref head) = current_head {
                    merge_parents.push(head.clone());
                    debug!(
                        "MERGE: creating merge commit with parents [{}, {}]",
                        &parent[..8.min(parent.len())],
                        &head[..8.min(head.len())]
                    );
                }
                (diff, merge_parents)
            } else {
                // Parent matches HEAD - use current content for diff
                let diff = if is_json_type {
                    // Get current Yjs state for proper CRDT merge
                    let base_state = self
                        .doc_store
                        .get_yjs_state(id)
                        .await
                        .map(|b| b64::encode(&b));
                    let is_jsonl = doc.content_type == ContentType::Jsonl;
                    compute_json_diff(new_content, &doc.content, base_state.as_deref(), is_jsonl)?
                } else if is_xml_type {
                    // XML documents use Y.XmlFragment - use server's actual Yjs state
                    let base_state = self.doc_store.get_yjs_state(id).await;
                    match base_state {
                        Some(state_bytes) => diff::compute_xml_diff_update_with_base(
                            &state_bytes,
                            &doc.content,
                            new_content,
                        )
                        .map_err(|e| ServiceError::Internal(e.to_string()))?,
                        None => {
                            // No Yjs state yet - use fresh doc
                            diff::compute_xml_diff_update(&doc.content, new_content)
                                .map_err(|e| ServiceError::Internal(e.to_string()))?
                        }
                    }
                } else {
                    // Text: use server's actual Yjs state for proper CRDT merge
                    let base_state = self.doc_store.get_yjs_state(id).await;
                    match base_state {
                        Some(state_bytes) => diff::compute_diff_update_with_base(
                            &state_bytes,
                            &doc.content,
                            new_content,
                        )
                        .map_err(|e| ServiceError::Internal(e.to_string()))?,
                        None => {
                            // No Yjs state yet - use fresh doc (initial content)
                            diff::compute_diff_update(&doc.content, new_content)
                                .map_err(|e| ServiceError::Internal(e.to_string()))?
                        }
                    }
                };
                (diff, vec![parent.clone()])
            }
        } else {
            // No parent specified - use current content and HEAD as parent
            let diff = if is_json_type {
                // Get current Yjs state for proper CRDT merge
                let base_state = self
                    .doc_store
                    .get_yjs_state(id)
                    .await
                    .map(|b| b64::encode(&b));
                let is_jsonl = doc.content_type == ContentType::Jsonl;
                compute_json_diff(new_content, &doc.content, base_state.as_deref(), is_jsonl)?
            } else if is_xml_type {
                // XML documents use Y.XmlFragment - use server's actual Yjs state
                let base_state = self.doc_store.get_yjs_state(id).await;
                match base_state {
                    Some(state_bytes) => diff::compute_xml_diff_update_with_base(
                        &state_bytes,
                        &doc.content,
                        new_content,
                    )
                    .map_err(|e| ServiceError::Internal(e.to_string()))?,
                    None => {
                        // No Yjs state yet - use fresh doc
                        diff::compute_xml_diff_update(&doc.content, new_content)
                            .map_err(|e| ServiceError::Internal(e.to_string()))?
                    }
                }
            } else {
                // Text: use server's actual Yjs state for proper CRDT merge
                let base_state = self.doc_store.get_yjs_state(id).await;
                match base_state {
                    Some(state_bytes) => {
                        diff::compute_diff_update_with_base(&state_bytes, &doc.content, new_content)
                            .map_err(|e| ServiceError::Internal(e.to_string()))?
                    }
                    None => {
                        // No Yjs state yet - use fresh doc (initial content)
                        diff::compute_diff_update(&doc.content, new_content)
                            .map_err(|e| ServiceError::Internal(e.to_string()))?
                    }
                }
            };
            (diff, current_head.into_iter().collect())
        };

        // Skip creating commit if no changes (prevents empty Yjs updates)
        if diff_result.summary.chars_inserted == 0 && diff_result.summary.chars_deleted == 0 {
            debug!("SKIP: no changes detected, not creating commit");
            let head_cid = parents.first().cloned().unwrap_or_default();
            return Ok(ReplaceResult {
                cid: head_cid.clone(),
                edit_cid: head_cid,
                chars_inserted: 0,
                chars_deleted: 0,
                operations: 0,
            });
        }

        let author = author.unwrap_or_else(|| "anonymous".to_string());
        let commit = Commit::new(parents, diff_result.update_b64.clone(), author, None);
        let timestamp = commit.timestamp;

        let cid = commit_store
            .store_commit(&commit)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        // Apply update
        let content_before = self
            .doc_store
            .get_document(id)
            .await
            .map(|d| d.content.clone())
            .unwrap_or_default();
        debug!(
            "APPLY: doc content BEFORE update = {:?} ({} bytes)",
            preview_text(&content_before, 50),
            content_before.len()
        );

        self.doc_store
            .apply_yjs_update(id, &diff_result.update_bytes)
            .await?;

        let content_after = self
            .doc_store
            .get_document(id)
            .await
            .map(|d| d.content.clone())
            .unwrap_or_default();
        debug!(
            "APPLY: doc content AFTER update = {:?} ({} bytes)",
            preview_text(&content_after, 50),
            content_after.len()
        );

        commit_store
            .set_document_head(id, &cid)
            .await
            .map_err(|e| ServiceError::Internal(e.to_string()))?;

        self.broadcast_commit(id, &cid, timestamp);

        // Trigger filesystem reconciliation if this is the fs-root document
        self.maybe_reconcile(id).await;

        Ok(ReplaceResult {
            cid: cid.clone(),
            edit_cid: cid,
            chars_inserted: diff_result.summary.chars_inserted,
            chars_deleted: diff_result.summary.chars_deleted,
            operations: diff_result.operation_count,
        })
    }
}

/// Compute a JSON diff using Y.Map/Y.Array updates instead of Y.Text.
///
/// Returns a DiffResult compatible with the text diff output.
/// For JSONL content, set `is_jsonl` to true to parse as newline-delimited JSON
/// and store as Y.Array<Y.Map>.
fn compute_json_diff(
    new_content: &str,
    old_content: &str,
    base_state_b64: Option<&str>,
    is_jsonl: bool,
) -> Result<diff::DiffResult, ServiceError> {
    let update_b64 = if is_jsonl {
        create_yjs_jsonl_update(new_content, base_state_b64)
            .map_err(|e| ServiceError::Internal(format!("JSONL update failed: {}", e)))?
    } else {
        create_yjs_json_update(new_content, base_state_b64)
            .map_err(|e| ServiceError::Internal(format!("JSON update failed: {}", e)))?
    };

    let update_bytes = base64_decode(&update_b64)
        .map_err(|e| ServiceError::Internal(format!("Base64 decode failed: {}", e)))?;

    // Estimate changes based on content length difference
    let old_len = old_content.len();
    let new_len = new_content.len();
    let (chars_inserted, chars_deleted) = if new_len > old_len {
        (new_len - old_len, 0)
    } else {
        (0, old_len - new_len)
    };

    Ok(diff::DiffResult {
        update_bytes,
        update_b64,
        operation_count: 1, // JSON replace is effectively one operation
        summary: diff::DiffSummary {
            chars_inserted,
            chars_deleted,
            unchanged_chars: old_len.min(new_len),
        },
    })
}
