//! Sync port handler.
//!
//! Implements the git-like merkle tree synchronization protocol.
//! Subscribes to `{path}/sync/+` wildcard and responds to sync requests.

use crate::mqtt::client::MqttClient;
use crate::mqtt::messages::SyncMessage;
use crate::mqtt::topics::Topic;
use crate::mqtt::MqttError;
use crate::store::CommitStore;
use rumqttc::QoS;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Handler for the sync port.
pub struct SyncHandler {
    client: Arc<MqttClient>,
    commit_store: Option<Arc<CommitStore>>,
    subscribed_paths: RwLock<HashSet<String>>,
}

impl SyncHandler {
    /// Create a new sync handler.
    pub fn new(client: Arc<MqttClient>, commit_store: Option<Arc<CommitStore>>) -> Self {
        Self {
            client,
            commit_store,
            subscribed_paths: RwLock::new(HashSet::new()),
        }
    }

    /// Subscribe to sync requests for a path.
    /// Uses wildcard: `{path}/sync/+`
    pub async fn subscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::sync_wildcard(path);

        // Use QoS 0 for sync - ephemeral catch-up
        self.client.subscribe(&topic_pattern, QoS::AtMostOnce).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.insert(path.to_string());

        debug!("Subscribed to sync requests for path: {}", path);
        Ok(())
    }

    /// Unsubscribe from sync requests for a path.
    pub async fn unsubscribe_path(&self, path: &str) -> Result<(), MqttError> {
        let topic_pattern = Topic::sync_wildcard(path);

        self.client.unsubscribe(&topic_pattern).await?;

        let mut paths = self.subscribed_paths.write().await;
        paths.remove(path);

        debug!("Unsubscribed from sync requests for path: {}", path);
        Ok(())
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
            SyncMessage::Head { req, commit: None } => {
                self.handle_head(&topic.path, client_id, &req).await
            }
            SyncMessage::Head { commit: Some(_), .. } => {
                // Head response shouldn't be received by doc store
                warn!("Received unexpected Head response message");
                Ok(())
            }
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
            _ => {
                // Response messages shouldn't be received by the doc store
                warn!("Received unexpected sync response message");
                Ok(())
            }
        }
    }

    /// Handle a HEAD request.
    async fn handle_head(
        &self,
        path: &str,
        client_id: &str,
        req: &str,
    ) -> Result<(), MqttError> {
        let commit = if let Some(store) = &self.commit_store {
            store.get_document_head(path).await.ok().flatten()
        } else {
            None
        };

        let response = SyncMessage::Head {
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
        let store = self.commit_store.as_ref().ok_or_else(|| {
            MqttError::Node("Commit store not initialized".to_string())
        })?;

        let mut sent_commits = Vec::new();

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
                    };

                    self.send_response(path, client_id, &response).await?;
                    sent_commits.push(cid);
                }
                Err(e) => {
                    warn!("Commit {} not found: {}", cid, e);
                    // Continue with other commits
                }
            }
        }

        // Send done message
        let done = SyncMessage::Done {
            req: req.to_string(),
            commits: sent_commits,
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
        let store = self.commit_store.as_ref().ok_or_else(|| {
            MqttError::Node("Commit store not initialized".to_string())
        })?;

        // Resolve "HEAD" to actual commit ID
        let target_cid = if want == "HEAD" {
            match store.get_document_head(path).await? {
                Some(cid) => cid,
                None => {
                    // Empty document, send done with no commits
                    let done = SyncMessage::Done {
                        req: req.to_string(),
                        commits: vec![],
                    };
                    return self.send_response(path, client_id, &done).await;
                }
            }
        } else {
            want.to_string()
        };

        // Find commits between 'have' and 'want'
        // This is a simplified implementation - for a proper git-like protocol,
        // we'd do a proper DAG traversal
        let commits_to_send = self.find_commits_between(store, &have, &target_cid).await?;

        let mut sent_commits = Vec::new();

        for cid in commits_to_send {
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
        let store = self.commit_store.as_ref().ok_or_else(|| {
            MqttError::Node("Commit store not initialized".to_string())
        })?;

        // Resolve "HEAD" to actual commit ID
        let start_cid = if commit == "HEAD" {
            match store.get_document_head(path).await? {
                Some(cid) => cid,
                None => {
                    // Empty document
                    let done = SyncMessage::Done {
                        req: req.to_string(),
                        commits: vec![],
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
        };

        self.send_response(path, client_id, &done).await
    }

    /// Send a response to a client.
    async fn send_response(
        &self,
        path: &str,
        client_id: &str,
        message: &SyncMessage,
    ) -> Result<(), MqttError> {
        let topic = Topic::sync(path, client_id);
        let topic_str = topic.to_topic_string();

        let payload = serde_json::to_vec(message)?;

        // Use QoS 0 for sync responses - ephemeral
        self.client
            .publish(&topic_str, &payload, QoS::AtMostOnce)
            .await
    }

    /// Find commits between 'have' set and 'want' commit.
    /// Returns commits in topological order (dependencies first).
    async fn find_commits_between(
        &self,
        store: &CommitStore,
        have: &[String],
        want: &str,
    ) -> Result<Vec<String>, MqttError> {
        let have_set: HashSet<&str> = have.iter().map(|s| s.as_str()).collect();
        let mut result = Vec::new();
        let mut to_visit = vec![want.to_string()];
        let mut visited = HashSet::new();

        while let Some(cid) = to_visit.pop() {
            if visited.contains(&cid) || have_set.contains(cid.as_str()) {
                continue;
            }

            visited.insert(cid.clone());

            match store.get_commit(&cid).await {
                Ok(commit) => {
                    result.push(cid);
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
        Ok(result)
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
