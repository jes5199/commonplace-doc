//! Sync port handler.
//!
//! Implements the git-like merkle tree synchronization protocol.
//! Subscribes to `{path}/sync/+` wildcard and responds to sync requests.

use crate::document::resolve_path_to_uuid;
use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::{SyncError, SyncMessage};
use crate::mqtt::topics::Topic;
use crate::mqtt::MqttError;
use crate::store::CommitStore;
use rumqttc::QoS;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Result of finding commits between have and want.
struct FindCommitsResult {
    /// Commits to send (topological order, oldest first).
    commits: Vec<String>,
    /// The common ancestor found (first commit in client's "have" set
    /// that exists in server's history), or None if no common ancestor found.
    ancestor: Option<String>,
}

/// Handler for the sync port.
pub struct SyncHandler {
    client: Arc<MqttClient>,
    commit_store: Option<Arc<CommitStore>>,
    workspace: String,
    subscribed_paths: RwLock<HashSet<String>>,
    /// Cache of fs-root JSON content for path→document ID resolution.
    fs_root_content: RwLock<String>,
    /// The fs-root path/ID.
    fs_root_path: RwLock<Option<String>>,
}

impl SyncHandler {
    /// Create a new sync handler.
    pub fn new(
        client: Arc<MqttClient>,
        commit_store: Option<Arc<CommitStore>>,
        workspace: String,
    ) -> Self {
        Self {
            client,
            commit_store,
            workspace,
            subscribed_paths: RwLock::new(HashSet::new()),
            fs_root_content: RwLock::new(String::new()),
            fs_root_path: RwLock::new(None),
        }
    }

    /// Update the cached fs-root content for path resolution.
    pub async fn set_fs_root_content(&self, content: String) {
        let mut fs_root = self.fs_root_content.write().await;
        *fs_root = content;
    }

    /// Set the fs-root path.
    pub async fn set_fs_root_path(&self, path: String) {
        let mut fs_root_path = self.fs_root_path.write().await;
        *fs_root_path = Some(path);
    }

    /// Resolve a path to a document ID.
    async fn resolve_document_id(&self, path: &str) -> Option<String> {
        let fs_root_path = self.fs_root_path.read().await;
        let is_fs_root = fs_root_path.as_ref() == Some(&path.to_string());
        let fs_root_id = fs_root_path.clone();
        drop(fs_root_path);

        if is_fs_root {
            Some(path.to_string())
        } else {
            let fs_root = self.fs_root_content.read().await;
            let fs_root_id = fs_root_id.as_deref().unwrap_or("");
            resolve_path_to_uuid(&fs_root, path, fs_root_id)
        }
    }

    /// Subscribe to sync requests for a path.
    /// Uses wildcard: `{workspace}/{path}/sync/+`
    pub async fn subscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::sync_wildcard(&self.workspace, path);

        // Use QoS 0 for sync - ephemeral catch-up
        self.client
            .subscribe(&topic_pattern, QoS::AtMostOnce)
            .await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.insert(path.to_string());

        debug!("Subscribed to sync requests for path: {}", path);
        Ok(())
    }

    /// Unsubscribe from sync requests for a path.
    pub async fn unsubscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::sync_wildcard(&self.workspace, path);

        self.client.unsubscribe(&topic_pattern).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.remove(path);

        debug!("Unsubscribed from sync requests for path: {}", path);
        Ok(())
    }

    /// Get all subscribed paths.
    pub async fn subscribed_paths(&self) -> Vec<String> {
        let paths = self.subscribed_paths.read().await;
        paths.iter().cloned().collect()
    }

    /// Handle an incoming sync request.
    pub async fn handle_sync_request(
        &self,
        topic: &Topic,
        message: SyncMessage,
    ) -> Result<(), MqttError> {
        let client_id = topic.qualifier.as_deref().ok_or_else(|| {
            MqttError::InvalidTopic("Sync topic missing client ID qualifier".to_string())
        })?;

        debug!(
            "Received sync request from {} for path: {}",
            client_id, topic.path
        );

        match message {
            SyncMessage::Head { req } => self.handle_head(&topic.path, client_id, &req).await,
            SyncMessage::Get { req, commits } => {
                self.handle_get(&topic.path, client_id, &req, commits).await
            }
            SyncMessage::Pull { req, have, want } => {
                self.handle_pull(&topic.path, client_id, &req, have, &want)
                    .await
            }
            SyncMessage::Ancestors { req, commit, depth } => {
                self.handle_ancestors(&topic.path, client_id, &req, &commit, depth)
                    .await
            }
            SyncMessage::IsAncestor {
                req,
                ancestor,
                descendant,
            } => {
                self.handle_is_ancestor(&topic.path, client_id, &req, &ancestor, &descendant)
                    .await
            }
            SyncMessage::HeadResponse { .. }
            | SyncMessage::Commit { .. }
            | SyncMessage::Done { .. }
            | SyncMessage::IsAncestorResponse { .. }
            | SyncMessage::Error { .. }
            | SyncMessage::Ack { .. }
            | SyncMessage::MissingParent { .. } => {
                // Response messages shouldn't be received by the doc store
                warn!("Received unexpected sync response message: {:?}", message);
                Ok(())
            }
        }
    }

    /// Handle a HEAD request.
    async fn handle_head(&self, path: &str, client_id: &str, req: &str) -> Result<(), MqttError> {
        // Resolve path to document ID
        let doc_id = self.resolve_document_id(path).await;

        let commit = if let (Some(store), Some(doc_id)) = (&self.commit_store, &doc_id) {
            store.get_document_head(doc_id).await.ok().flatten()
        } else {
            None
        };

        let response = SyncMessage::HeadResponse {
            req: req.to_string(),
            commit,
        };

        self.send_response(path, client_id, &response).await
    }

    /// Handle a GET request - fetch specific commits by ID.
    async fn handle_get(
        &self,
        path: &str,
        client_id: &str,
        req: &str,
        commit_ids: Vec<String>,
    ) -> Result<(), MqttError> {
        let store = self
            .commit_store
            .as_ref()
            .ok_or_else(|| MqttError::Node("Commit store not initialized".to_string()))?;

        let mut sent_commits = Vec::new();
        let mut not_found = Vec::new();

        for cid in commit_ids {
            match store.get_commit(&cid).await {
                Ok(commit) => {
                    let response = SyncMessage::Commit {
                        req: req.to_string(),
                        id: cid.clone(),
                        parents: commit.parents,
                        data: commit.update,
                        timestamp: commit.timestamp,
                        author: commit.author,
                        message: commit.message,
                        source: None, // Doc store response
                    };

                    self.send_response(path, client_id, &response).await?;
                    sent_commits.push(cid);
                }
                Err(e) => {
                    warn!("Commit {} not found: {}", cid, e);
                    not_found.push(cid);
                }
            }
        }

        // If any commits were not found, send a structured error
        if !not_found.is_empty() {
            let error_response = SyncMessage::error(req, SyncError::not_found(not_found));
            self.send_response(path, client_id, &error_response).await?;
        }

        // Send done message with commits that were found
        let done = SyncMessage::Done {
            req: req.to_string(),
            commits: sent_commits,
            source: None,   // Doc store response
            ancestor: None, // No ancestor for get requests (only for pull)
        };

        self.send_response(path, client_id, &done).await
    }

    /// Handle a PULL request - incremental sync.
    async fn handle_pull(
        &self,
        path: &str,
        client_id: &str,
        req: &str,
        have: Vec<String>,
        want: &str,
    ) -> Result<(), MqttError> {
        let store = self
            .commit_store
            .as_ref()
            .ok_or_else(|| MqttError::Node("Commit store not initialized".to_string()))?;

        // Resolve path to document ID
        let doc_id = self
            .resolve_document_id(path)
            .await
            .ok_or_else(|| MqttError::InvalidTopic(format!("Path not mounted: {}", path)))?;

        // Resolve "HEAD" to actual commit ID
        let target_cid = if want == "HEAD" {
            match store.get_document_head(&doc_id).await? {
                Some(cid) => cid,
                None => {
                    // Empty document, send done with no commits
                    let done = SyncMessage::Done {
                        req: req.to_string(),
                        commits: vec![],
                        source: None,   // Doc store response
                        ancestor: None, // No ancestor for empty documents
                    };
                    return self.send_response(path, client_id, &done).await;
                }
            }
        } else {
            want.to_string()
        };

        // Find commits between 'have' and 'want'
        let find_result = self.find_commits_between(store, &have, &target_cid).await?;

        // If have set was non-empty but no common ancestor was found, return error
        if !have.is_empty() && find_result.ancestor.is_none() {
            let error = SyncMessage::error(req, SyncError::no_common_ancestor(have, target_cid));
            return self.send_response(path, client_id, &error).await;
        }

        let mut sent_commits = Vec::new();

        for cid in find_result.commits {
            match store.get_commit(&cid).await {
                Ok(commit) => {
                    let response = SyncMessage::Commit {
                        req: req.to_string(),
                        id: cid.clone(),
                        parents: commit.parents,
                        data: commit.update,
                        timestamp: commit.timestamp,
                        author: commit.author,
                        message: commit.message,
                        source: None, // Doc store response
                    };

                    self.send_response(path, client_id, &response).await?;
                    sent_commits.push(cid);
                }
                Err(e) => {
                    warn!("Error fetching commit {}: {}", cid, e);
                }
            }
        }

        // Send done message with ancestor (if found during pull)
        let done = SyncMessage::Done {
            req: req.to_string(),
            commits: sent_commits,
            source: None, // Doc store response
            ancestor: find_result.ancestor,
        };

        self.send_response(path, client_id, &done).await
    }

    /// Handle an ANCESTORS request - full history from a commit.
    async fn handle_ancestors(
        &self,
        path: &str,
        client_id: &str,
        req: &str,
        commit: &str,
        depth: Option<u32>,
    ) -> Result<(), MqttError> {
        let store = self
            .commit_store
            .as_ref()
            .ok_or_else(|| MqttError::Node("Commit store not initialized".to_string()))?;

        // Resolve path to document ID
        let doc_id = self
            .resolve_document_id(path)
            .await
            .ok_or_else(|| MqttError::InvalidTopic(format!("Path not mounted: {}", path)))?;

        // Resolve "HEAD" to actual commit ID
        let start_cid = if commit == "HEAD" {
            match store.get_document_head(&doc_id).await? {
                Some(cid) => cid,
                None => {
                    // Empty document
                    let done = SyncMessage::Done {
                        req: req.to_string(),
                        commits: vec![],
                        source: None,   // Doc store response
                        ancestor: None, // No ancestor for empty documents
                    };
                    return self.send_response(path, client_id, &done).await;
                }
            }
        } else {
            commit.to_string()
        };

        // Traverse ancestry
        let ancestors = self.collect_ancestors(store, &start_cid, depth).await?;

        let mut sent_commits = Vec::new();

        // Send commits in chronological order (oldest first)
        for cid in ancestors.into_iter().rev() {
            match store.get_commit(&cid).await {
                Ok(commit) => {
                    let response = SyncMessage::Commit {
                        req: req.to_string(),
                        id: cid.clone(),
                        parents: commit.parents,
                        data: commit.update,
                        timestamp: commit.timestamp,
                        author: commit.author,
                        message: commit.message,
                        source: None, // Doc store response
                    };

                    self.send_response(path, client_id, &response).await?;
                    sent_commits.push(cid);
                }
                Err(e) => {
                    warn!("Error fetching commit {}: {}", cid, e);
                }
            }
        }

        // Send done message
        let done = SyncMessage::Done {
            req: req.to_string(),
            commits: sent_commits,
            source: None,   // Doc store response
            ancestor: None, // No ancestor for ancestors request (only for pull)
        };

        self.send_response(path, client_id, &done).await
    }

    /// Handle an IS_ANCESTOR request - check if one commit is an ancestor of another.
    async fn handle_is_ancestor(
        &self,
        path: &str,
        client_id: &str,
        req: &str,
        ancestor: &str,
        descendant: &str,
    ) -> Result<(), MqttError> {
        let store = self
            .commit_store
            .as_ref()
            .ok_or_else(|| MqttError::Node("Commit store not initialized".to_string()))?;

        // Check ancestry relationship
        let result = match store.is_ancestor(ancestor, descendant).await {
            Ok(is_ancestor) => is_ancestor,
            Err(e) => {
                // Send error response - use error_message for generic errors
                let error =
                    SyncMessage::error_message(req, format!("Ancestry check failed: {}", e));
                return self.send_response(path, client_id, &error).await;
            }
        };

        let response = SyncMessage::IsAncestorResponse {
            req: req.to_string(),
            result,
        };

        self.send_response(path, client_id, &response).await
    }

    /// Send a response to a client.
    async fn send_response(
        &self,
        path: &str,
        client_id: &str,
        message: &SyncMessage,
    ) -> Result<(), MqttError> {
        let topic = Topic::sync(&self.workspace, path, client_id);
        let topic_str = topic.to_topic_string();

        let payload = serde_json::to_vec(message)?;

        // Use QoS 0 for sync responses - ephemeral
        self.client
            .publish(&topic_str, &payload, QoS::AtMostOnce)
            .await
    }

    /// Find commits between 'have' set and 'want' commit.
    /// Returns commits in topological order (dependencies first) and the
    /// common ancestor found (if any).
    async fn find_commits_between(
        &self,
        store: &CommitStore,
        have: &[String],
        want: &str,
    ) -> Result<FindCommitsResult, MqttError> {
        let have_set: HashSet<&str> = have.iter().map(|s| s.as_str()).collect();
        let mut result = Vec::new();
        let mut to_visit = vec![want.to_string()];
        let mut visited = HashSet::new();
        // Track the first common ancestor found (first commit in have set we encounter)
        let mut ancestor: Option<String> = None;

        while let Some(cid) = to_visit.pop() {
            if visited.contains(&cid) {
                continue;
            }

            // Check if this commit is in the have set (common ancestor found)
            if have_set.contains(cid.as_str()) {
                // Record the first ancestor found (there may be multiple in divergent DAGs)
                if ancestor.is_none() {
                    ancestor = Some(cid.clone());
                }
                continue;
            }

            visited.insert(cid.clone());

            match store.get_commit(&cid).await {
                Ok(commit) => {
                    result.push(cid);
                    // If this commit has no parents, we've reached the root
                    if commit.parents.is_empty() {
                        // Reaching root without finding have commits means:
                        // - Either we found all commits (have was empty)
                        // - Or there's no common ancestor
                        // For empty have set, we consider root as the implicit ancestor
                        // (no ancestor field needed in response)
                    }
                    for parent in commit.parents {
                        to_visit.push(parent);
                    }
                }
                Err(_) => {
                    // Commit not found, skip
                }
            }
        }

        // Reverse to get oldest first
        result.reverse();
        Ok(FindCommitsResult {
            commits: result,
            ancestor,
        })
    }

    /// Collect ancestors of a commit up to a depth limit.
    async fn collect_ancestors(
        &self,
        store: &CommitStore,
        start: &str,
        depth: Option<u32>,
    ) -> Result<Vec<String>, MqttError> {
        let mut result = Vec::new();
        let mut to_visit = vec![(start.to_string(), 0u32)];
        let mut visited = HashSet::new();

        while let Some((cid, current_depth)) = to_visit.pop() {
            if visited.contains(&cid) {
                continue;
            }

            if let Some(max_depth) = depth {
                if current_depth > max_depth {
                    continue;
                }
            }

            visited.insert(cid.clone());

            match store.get_commit(&cid).await {
                Ok(commit) => {
                    result.push(cid);
                    for parent in commit.parents {
                        to_visit.push((parent, current_depth + 1));
                    }
                }
                Err(_) => {
                    // Commit not found, skip
                }
            }
        }

        Ok(result)
    }
}
