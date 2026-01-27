//! Peer fallback for cyan history/commit requests over MQTT.
//!
//! When a client needs commit history, it publishes a sync request (Get, Pull, Ancestors).
//! The doc store normally responds, but if it's slow or unavailable, peers that have
//! the requested commits can respond instead.
//!
//! ## Protocol Flow
//!
//! 1. Client publishes sync request with correlation ID (req field)
//! 2. Doc store has 100ms to respond (primary timeout)
//! 3. If no response arrives, eligible peers wait random jitter (0-500ms)
//! 4. First peer with the data publishes commits
//! 5. Other peers see the response and cancel their pending response
//!
//! ## Duplicate Suppression
//!
//! - Peers watch response topics for correlation IDs they're tracking
//! - Once any response arrives (from doc store or peer), pending responses are cancelled
//! - Rate limiting prevents spam (30s minimum between serving same commit)
//!
//! See CP-sg6e for design context.

use crate::mqtt::messages::{PeerFallbackConfig, SyncMessage};
use crate::mqtt::{MqttClient, Topic};
use crate::sync::crdt_state::CrdtPeerState;
use crate::sync::error::{SyncError, SyncResult};
use rumqttc::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// A pending sync request that may require peer fallback.
#[derive(Debug)]
pub struct PendingRequest {
    /// Correlation ID for this request
    pub req_id: String,
    /// Document path this request is for
    pub path: String,
    /// Commit IDs requested (for Get requests)
    pub commit_ids: Vec<String>,
    /// When the request was registered
    pub registered_at: Instant,
    /// Deadline for primary (doc store) response
    pub primary_deadline: Instant,
    /// Whether any response has been received
    pub response_received: bool,
    /// Whether we've already responded as a peer
    pub peer_responded: bool,
}

impl PendingRequest {
    /// Create a new pending request.
    pub fn new(
        req_id: String,
        path: String,
        commit_ids: Vec<String>,
        config: &PeerFallbackConfig,
    ) -> Self {
        let now = Instant::now();
        Self {
            req_id,
            path,
            commit_ids,
            registered_at: now,
            primary_deadline: now + config.primary_timeout,
            response_received: false,
            peer_responded: false,
        }
    }

    /// Check if the primary timeout has elapsed.
    pub fn primary_timeout_elapsed(&self) -> bool {
        Instant::now() >= self.primary_deadline
    }

    /// Check if this request is eligible for peer response.
    ///
    /// Returns true if:
    /// - Primary timeout has elapsed
    /// - No response has been received yet
    /// - We haven't already responded as a peer
    pub fn eligible_for_peer_response(&self) -> bool {
        self.primary_timeout_elapsed() && !self.response_received && !self.peer_responded
    }
}

/// Tracks pending sync requests and coordinates peer fallback responses.
///
/// Each sync client maintains one of these to track requests it has sent
/// and to coordinate responding to other clients' requests as a peer.
pub struct PeerFallbackHandler {
    /// Configuration for timing
    config: PeerFallbackConfig,
    /// Pending requests we're tracking (keyed by req_id)
    pending_requests: RwLock<HashMap<String, PendingRequest>>,
    /// Rate limiter for peer responses (commit_id -> last_served_at)
    rate_limiter: RwLock<HashMap<String, Instant>>,
    /// Minimum interval between serving the same commit
    rate_limit_interval: Duration,
    /// Our client ID (for identifying our responses)
    client_id: String,
}

impl PeerFallbackHandler {
    /// Create a new peer fallback handler.
    pub fn new(client_id: String, config: PeerFallbackConfig) -> Self {
        Self {
            config,
            pending_requests: RwLock::new(HashMap::new()),
            rate_limiter: RwLock::new(HashMap::new()),
            rate_limit_interval: Duration::from_secs(30),
            client_id,
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(client_id: String) -> Self {
        Self::new(client_id, PeerFallbackConfig::default())
    }

    /// Check if peer fallback is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the configuration.
    pub fn config(&self) -> &PeerFallbackConfig {
        &self.config
    }

    /// Register a sync request we're sending.
    ///
    /// Call this when you publish a Get/Pull/Ancestors request so we can
    /// track when the primary timeout elapses.
    pub async fn register_request(&self, req_id: &str, path: &str, commit_ids: Vec<String>) {
        if !self.config.enabled {
            return;
        }

        let request = PendingRequest::new(
            req_id.to_string(),
            path.to_string(),
            commit_ids,
            &self.config,
        );

        let mut requests = self.pending_requests.write().await;
        requests.insert(req_id.to_string(), request);

        debug!(
            "Registered pending request {} for path {} ({} commits)",
            req_id,
            path,
            requests.len()
        );
    }

    /// Mark that a response was received for a request.
    ///
    /// Call this when you receive a Commit or Done message matching a request ID.
    /// This cancels any pending peer response.
    pub async fn mark_response_received(&self, req_id: &str) {
        let mut requests = self.pending_requests.write().await;
        if let Some(request) = requests.get_mut(req_id) {
            request.response_received = true;
            debug!("Marked response received for request {}", req_id);
        }
    }

    /// Remove a completed request from tracking.
    ///
    /// Call this when a Done message is received to clean up.
    pub async fn remove_request(&self, req_id: &str) {
        let mut requests = self.pending_requests.write().await;
        if requests.remove(req_id).is_some() {
            debug!("Removed completed request {}", req_id);
        }
    }

    /// Check if we can serve a commit as a peer (rate limiting).
    async fn can_serve_commit(&self, commit_id: &str) -> bool {
        let limiter = self.rate_limiter.read().await;
        match limiter.get(commit_id) {
            None => true,
            Some(last_served) => last_served.elapsed() >= self.rate_limit_interval,
        }
    }

    /// Record that we served a commit.
    async fn record_commit_served(&self, commit_id: &str) {
        let mut limiter = self.rate_limiter.write().await;
        limiter.insert(commit_id.to_string(), Instant::now());
    }

    /// Generate random jitter for peer response delay.
    fn random_jitter(&self) -> Duration {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        );
        let max_ms = self.config.peer_jitter_max.as_millis() as u64;
        if max_ms == 0 {
            return Duration::ZERO;
        }
        let random_ms = hasher.finish() % max_ms;
        Duration::from_millis(random_ms)
    }

    /// Check if we should respond to a sync request as a peer.
    ///
    /// This is called when we receive a sync request from another client.
    /// Returns the commits we should send if we decide to respond.
    ///
    /// # Arguments
    /// * `req_id` - Correlation ID of the request
    /// * `path` - Document path
    /// * `commit_ids` - Commit IDs requested
    /// * `state` - Our CRDT state for this document
    ///
    /// # Returns
    /// * `Ok(commits)` - List of commits we can provide (empty if none or shouldn't respond)
    pub async fn check_and_prepare_peer_response(
        &self,
        req_id: &str,
        path: &str,
        commit_ids: &[String],
        state: &CrdtPeerState,
    ) -> SyncResult<Vec<String>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        // Check which commits we have
        let mut available_commits = Vec::new();
        for cid in commit_ids {
            // Check if we know this commit
            if state.is_cid_known(cid) {
                // Check rate limiting
                if self.can_serve_commit(cid).await {
                    available_commits.push(cid.clone());
                } else {
                    debug!("Rate limited: not serving commit {} again so soon", cid);
                }
            }
        }

        if available_commits.is_empty() {
            debug!("No commits to serve for request {} path {}", req_id, path);
            return Ok(vec![]);
        }

        debug!(
            "Can serve {} commits for request {} path {}: {:?}",
            available_commits.len(),
            req_id,
            path,
            available_commits
        );

        Ok(available_commits)
    }

    /// Wait for jitter and then publish peer response if still valid.
    ///
    /// # Arguments
    /// * `mqtt_client` - MQTT client for publishing
    /// * `workspace` - Workspace name
    /// * `path` - Document path
    /// * `req_id` - Request correlation ID
    /// * `client_id` - Requesting client's ID (who we're responding to)
    /// * `state` - Our CRDT state
    /// * `commit_ids` - Commits we can provide
    #[allow(clippy::too_many_arguments)]
    pub async fn respond_as_peer(
        &self,
        mqtt_client: &Arc<MqttClient>,
        workspace: &str,
        path: &str,
        req_id: &str,
        client_id: &str,
        state: &CrdtPeerState,
        commit_ids: Vec<String>,
    ) -> SyncResult<usize> {
        if !self.config.enabled || commit_ids.is_empty() {
            return Ok(0);
        }

        // Wait for jitter
        let jitter = self.random_jitter();
        debug!(
            "Waiting {:?} jitter before peer response for request {}",
            jitter, req_id
        );
        tokio::time::sleep(jitter).await;

        // Re-check if response was already received during jitter
        {
            let requests = self.pending_requests.read().await;
            if let Some(request) = requests.get(req_id) {
                if request.response_received {
                    debug!(
                        "Response already received during jitter for {}, aborting peer response",
                        req_id
                    );
                    return Ok(0);
                }
            }
        }

        // Publish commits
        let mut sent_count = 0;
        for cid in &commit_ids {
            // Double-check rate limiting
            if !self.can_serve_commit(cid).await {
                continue;
            }

            // Build commit message from state
            // Note: We can only serve commits if we have full data
            // For now, we only support serving HEAD commits
            if state.head_cid.as_deref() != Some(cid.as_str())
                && state.local_head_cid.as_deref() != Some(cid.as_str())
            {
                debug!(
                    "Cannot serve commit {} - not current head (need full commit store)",
                    cid
                );
                continue;
            }

            let yjs_state = match &state.yjs_state {
                Some(s) => s.clone(),
                None => {
                    debug!("Cannot serve commit {} - no Yjs state", cid);
                    continue;
                }
            };

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            // Build response with our client_id as source
            let commit_msg = SyncMessage::Commit {
                req: req_id.to_string(),
                id: cid.clone(),
                parents: vec![], // We don't have parent info without commit store
                data: yjs_state,
                timestamp,
                author: format!("peer-{}", self.client_id),
                message: Some("Peer fallback response".to_string()),
                source: Some(self.client_id.clone()),
            };

            // Publish to the requesting client's sync topic
            let topic = Topic::sync(workspace, path, client_id).to_topic_string();
            let payload = serde_json::to_vec(&commit_msg)?;

            mqtt_client
                .publish(&topic, &payload, QoS::AtMostOnce)
                .await
                .map_err(|e| SyncError::mqtt(format!("Failed to publish peer response: {}", e)))?;

            self.record_commit_served(cid).await;
            sent_count += 1;

            info!(
                "Sent peer response for commit {} to {} (request {})",
                cid, client_id, req_id
            );
        }

        // Send Done message
        if sent_count > 0 {
            let done_msg = SyncMessage::Done {
                req: req_id.to_string(),
                commits: commit_ids.into_iter().take(sent_count).collect(),
                source: Some(self.client_id.clone()),
                ancestor: None, // Peer fallback doesn't track ancestor
            };

            let topic = Topic::sync(workspace, path, client_id).to_topic_string();
            let payload = serde_json::to_vec(&done_msg)?;

            mqtt_client
                .publish(&topic, &payload, QoS::AtMostOnce)
                .await
                .map_err(|e| SyncError::mqtt(format!("Failed to publish peer Done: {}", e)))?;
        }

        Ok(sent_count)
    }

    /// Clean up expired pending requests.
    ///
    /// Call periodically to remove requests that have timed out.
    pub async fn cleanup_expired(&self) {
        let max_age = Duration::from_secs(60);
        let mut requests = self.pending_requests.write().await;
        let before_count = requests.len();

        requests.retain(|_, req| req.registered_at.elapsed() < max_age);

        let removed = before_count - requests.len();
        if removed > 0 {
            debug!("Cleaned up {} expired pending requests", removed);
        }

        // Also clean up rate limiter
        let mut limiter = self.rate_limiter.write().await;
        limiter.retain(|_, last| last.elapsed() < self.rate_limit_interval * 2);
    }

    /// Get statistics about pending requests.
    pub async fn stats(&self) -> PeerFallbackStats {
        let requests = self.pending_requests.read().await;
        let limiter = self.rate_limiter.read().await;

        let mut eligible_count = 0;
        let mut waiting_count = 0;
        let mut received_count = 0;

        for req in requests.values() {
            if req.response_received {
                received_count += 1;
            } else if req.primary_timeout_elapsed() {
                eligible_count += 1;
            } else {
                waiting_count += 1;
            }
        }

        PeerFallbackStats {
            pending_total: requests.len(),
            waiting_for_primary: waiting_count,
            eligible_for_peer: eligible_count,
            response_received: received_count,
            rate_limited_commits: limiter.len(),
        }
    }
}

/// Statistics about the peer fallback handler state.
#[derive(Debug, Clone)]
pub struct PeerFallbackStats {
    /// Total pending requests
    pub pending_total: usize,
    /// Requests still waiting for primary response
    pub waiting_for_primary: usize,
    /// Requests eligible for peer response
    pub eligible_for_peer: usize,
    /// Requests that have received a response
    pub response_received: usize,
    /// Number of commits currently rate limited
    pub rate_limited_commits: usize,
}

/// Thread-safe shared peer fallback handler.
pub type SharedPeerFallbackHandler = Arc<PeerFallbackHandler>;

/// Create a new shared peer fallback handler.
pub fn new_peer_fallback_handler(
    client_id: String,
    config: PeerFallbackConfig,
) -> SharedPeerFallbackHandler {
    Arc::new(PeerFallbackHandler::new(client_id, config))
}

/// Handle an incoming sync request that we might respond to as a peer.
///
/// This is the main entry point for peer fallback. Call this when you receive
/// a sync request (Get, Pull, Ancestors) on a path you're subscribed to.
///
/// The function will:
/// 1. Check if we have the requested commits
/// 2. Wait for primary timeout + random jitter
/// 3. Re-check if a response arrived (cancel if so)
/// 4. Publish our response if we can help
///
/// # Arguments
/// * `handler` - The peer fallback handler
/// * `mqtt_client` - MQTT client for publishing responses
/// * `workspace` - Workspace name
/// * `message` - The sync request message
/// * `requesting_client_id` - Client ID of the requester
/// * `path` - Document path
/// * `state` - Our CRDT state for this document
///
/// # Returns
/// * `Ok(count)` - Number of commits we responded with (0 if none)
pub async fn handle_peer_sync_request(
    handler: &SharedPeerFallbackHandler,
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    message: &SyncMessage,
    requesting_client_id: &str,
    path: &str,
    state: &CrdtPeerState,
) -> SyncResult<usize> {
    if !handler.is_enabled() {
        return Ok(0);
    }

    // Don't respond to our own requests
    if requesting_client_id == handler.client_id {
        return Ok(0);
    }

    // Extract request details based on message type
    let (req_id, commit_ids) = match message {
        SyncMessage::Get { req, commits } => (req.clone(), commits.clone()),
        SyncMessage::Pull { req, want, .. } => {
            // For Pull, we can only help if they want our HEAD
            if want == "HEAD" || state.head_cid.as_deref() == Some(want) {
                let commits = state.head_cid.iter().cloned().collect();
                (req.clone(), commits)
            } else {
                return Ok(0); // Can't help with arbitrary commits
            }
        }
        SyncMessage::Ancestors { req, commit, .. } => {
            // For Ancestors, we can only help if they want our HEAD
            if commit == "HEAD" || state.head_cid.as_deref() == Some(commit) {
                let commits = state.head_cid.iter().cloned().collect();
                (req.clone(), commits)
            } else {
                return Ok(0); // Can't help with arbitrary commits
            }
        }
        _ => return Ok(0), // Not a request we can respond to
    };

    // Check what commits we can provide
    let available_commits = handler
        .check_and_prepare_peer_response(&req_id, path, &commit_ids, state)
        .await?;

    if available_commits.is_empty() {
        return Ok(0);
    }

    // Register this as a "request we're considering responding to"
    // (Use a different key to avoid conflicts with our own requests)
    let tracking_key = format!("peer-{}", req_id);
    handler
        .register_request(&tracking_key, path, available_commits.clone())
        .await;

    // Wait for primary timeout
    tokio::time::sleep(handler.config().primary_timeout).await;

    // Now try to respond (this includes jitter and duplicate checking)
    let result = handler
        .respond_as_peer(
            mqtt_client,
            workspace,
            path,
            &req_id,
            requesting_client_id,
            state,
            available_commits,
        )
        .await;

    // Clean up tracking
    handler.remove_request(&tracking_key).await;

    result
}

/// Subscribe to sync requests for peer fallback on a path.
///
/// This subscribes to the wildcard topic `{workspace}/sync/{path}/+` so we can
/// see requests from other clients and potentially respond as a peer.
///
/// # Returns
/// The subscription topic pattern.
pub async fn subscribe_for_peer_fallback(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    path: &str,
) -> SyncResult<String> {
    let topic_pattern = Topic::sync_wildcard(workspace, path);

    mqtt_client
        .subscribe(&topic_pattern, QoS::AtMostOnce)
        .await
        .map_err(|e| {
            SyncError::mqtt(format!(
                "Failed to subscribe to sync requests for peer fallback: {}",
                e
            ))
        })?;

    debug!(
        "Subscribed to sync requests for peer fallback at: {}",
        topic_pattern
    );
    Ok(topic_pattern)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_peer_fallback_config_default() {
        let config = PeerFallbackConfig::default();
        assert_eq!(config.primary_timeout, Duration::from_millis(100));
        assert_eq!(config.peer_jitter_max, Duration::from_millis(500));
        assert!(config.enabled);
    }

    #[test]
    fn test_peer_fallback_config_disabled() {
        let config = PeerFallbackConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_pending_request_new() {
        let config = PeerFallbackConfig::default();
        let req = PendingRequest::new(
            "req-123".to_string(),
            "test/path".to_string(),
            vec!["commit-1".to_string()],
            &config,
        );

        assert_eq!(req.req_id, "req-123");
        assert_eq!(req.path, "test/path");
        assert!(!req.response_received);
        assert!(!req.peer_responded);
        assert!(!req.primary_timeout_elapsed()); // Shouldn't be elapsed immediately
    }

    #[tokio::test]
    async fn test_handler_register_and_mark_response() {
        let handler = PeerFallbackHandler::with_defaults("client-1".to_string());

        // Register a request
        handler
            .register_request("req-1", "test/path", vec!["commit-1".to_string()])
            .await;

        // Check it's registered
        {
            let requests = handler.pending_requests.read().await;
            assert!(requests.contains_key("req-1"));
            assert!(!requests.get("req-1").unwrap().response_received);
        }

        // Mark response received
        handler.mark_response_received("req-1").await;

        {
            let requests = handler.pending_requests.read().await;
            assert!(requests.get("req-1").unwrap().response_received);
        }

        // Remove the request
        handler.remove_request("req-1").await;

        {
            let requests = handler.pending_requests.read().await;
            assert!(!requests.contains_key("req-1"));
        }
    }

    #[tokio::test]
    async fn test_handler_rate_limiting() {
        let handler = PeerFallbackHandler::with_defaults("client-1".to_string());

        // First check should allow
        assert!(handler.can_serve_commit("commit-1").await);

        // Record serving
        handler.record_commit_served("commit-1").await;

        // Second check should deny (rate limited)
        assert!(!handler.can_serve_commit("commit-1").await);

        // Different commit should still be allowed
        assert!(handler.can_serve_commit("commit-2").await);
    }

    #[test]
    fn test_jitter_bounded() {
        let handler = PeerFallbackHandler::with_defaults("client-1".to_string());

        for _ in 0..100 {
            let jitter = handler.random_jitter();
            assert!(jitter <= handler.config.peer_jitter_max);
        }
    }

    #[tokio::test]
    async fn test_check_and_prepare_peer_response() {
        let handler = PeerFallbackHandler::with_defaults("client-1".to_string());
        let mut state = CrdtPeerState::new(Uuid::new_v4());

        // No known commits, should return empty
        let result = handler
            .check_and_prepare_peer_response(
                "req-1",
                "test/path",
                &["commit-1".to_string()],
                &state,
            )
            .await
            .unwrap();
        assert!(result.is_empty());

        // Add a known commit
        state.record_known_cid("commit-1");

        // Now should return the commit
        let result = handler
            .check_and_prepare_peer_response(
                "req-1",
                "test/path",
                &["commit-1".to_string()],
                &state,
            )
            .await
            .unwrap();
        assert_eq!(result, vec!["commit-1"]);

        // Rate limit it
        handler.record_commit_served("commit-1").await;

        // Should return empty due to rate limiting
        let result = handler
            .check_and_prepare_peer_response(
                "req-1",
                "test/path",
                &["commit-1".to_string()],
                &state,
            )
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let mut config = PeerFallbackConfig::default();
        config.primary_timeout = Duration::from_millis(1); // Very short for testing
        let handler = PeerFallbackHandler::new("client-1".to_string(), config);

        // Register a request
        handler.register_request("req-1", "test/path", vec![]).await;

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Stats before cleanup
        let stats = handler.stats().await;
        assert_eq!(stats.pending_total, 1);

        // Note: cleanup_expired uses a 60s max age, so the request won't be
        // cleaned up yet in this test. This is expected behavior.
        handler.cleanup_expired().await;

        // Request should still exist (not expired yet)
        let stats = handler.stats().await;
        assert_eq!(stats.pending_total, 1);
    }

    #[tokio::test]
    async fn test_disabled_handler() {
        let config = PeerFallbackConfig::disabled();
        let handler = PeerFallbackHandler::new("client-1".to_string(), config);

        assert!(!handler.is_enabled());

        // Register should be a no-op
        handler.register_request("req-1", "test/path", vec![]).await;

        let requests = handler.pending_requests.read().await;
        assert!(requests.is_empty());
    }
}
