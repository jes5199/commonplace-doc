//! Missing parent alert handling for CRDT sync.
//!
//! This module implements:
//! 1. Publishing missing parent alerts when receiving commits with unknown parents
//! 2. Subscribing to missing parent alerts from peers
//! 3. Rebroadcasting commits when we have the missing parents that peers need
//!
//! See CP-vtkn and CP-76h8 for design context.

use crate::mqtt::{EditMessage, MqttClient, SyncMessage, Topic};
use crate::sync::crdt_state::CrdtPeerState;
use crate::sync::error::{SyncError, SyncResult};
use rumqttc::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Minimum interval between rebroadcasting the same commit (to prevent spam).
pub const REBROADCAST_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum jitter to add before rebroadcasting (to prevent thundering herd).
pub const REBROADCAST_MAX_JITTER_MS: u64 = 5000;

/// Rate limiter for commit rebroadcasts.
///
/// Tracks when each commit was last rebroadcast to prevent spam.
/// Multiple clients having the same commit would otherwise all try to
/// rebroadcast simultaneously.
#[derive(Debug, Default)]
pub struct RebroadcastRateLimiter {
    /// Map from commit ID to last rebroadcast time
    last_rebroadcast: HashMap<String, Instant>,
}

impl RebroadcastRateLimiter {
    pub fn new() -> Self {
        Self {
            last_rebroadcast: HashMap::new(),
        }
    }

    /// Check if we can rebroadcast a commit (hasn't been rebroadcast recently).
    ///
    /// Returns true if enough time has passed since the last rebroadcast.
    pub fn can_rebroadcast(&self, cid: &str) -> bool {
        match self.last_rebroadcast.get(cid) {
            None => true,
            Some(last) => last.elapsed() >= REBROADCAST_MIN_INTERVAL,
        }
    }

    /// Record that a commit was rebroadcast.
    pub fn record_rebroadcast(&mut self, cid: &str) {
        self.last_rebroadcast
            .insert(cid.to_string(), Instant::now());
    }

    /// Clean up old entries that are past the rate limit window.
    pub fn cleanup(&mut self) {
        self.last_rebroadcast
            .retain(|_, last| last.elapsed() < REBROADCAST_MIN_INTERVAL * 2);
    }
}

/// Thread-safe wrapper for the rate limiter.
pub type SharedRateLimiter = Arc<RwLock<RebroadcastRateLimiter>>;

/// Create a new shared rate limiter.
pub fn new_rate_limiter() -> SharedRateLimiter {
    Arc::new(RwLock::new(RebroadcastRateLimiter::new()))
}

/// Generate a random jitter duration to prevent thundering herd.
fn random_jitter() -> Duration {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    // Use a simple hash-based random to avoid adding rand dependency
    let hasher = RandomState::new().build_hasher();
    let mut hasher = hasher;
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
    );
    let random_ms = hasher.finish() % REBROADCAST_MAX_JITTER_MS;
    Duration::from_millis(random_ms)
}

/// Publish a missing parent alert via MQTT.
///
/// Call this when you receive a commit but don't have one or more of its parents.
/// Peers who have the missing commits can then rebroadcast them.
pub async fn publish_missing_parent_alert(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    path: &str,
    commit_id: &str,
    missing_parents: Vec<String>,
    client_id: &str,
) -> SyncResult<()> {
    if missing_parents.is_empty() {
        return Ok(()); // No missing parents, nothing to alert
    }

    let req_id = Uuid::new_v4().to_string();

    let alert = SyncMessage::MissingParent {
        req: req_id,
        commit_id: commit_id.to_string(),
        missing_parents: missing_parents.clone(),
        client_id: client_id.to_string(),
    };

    let topic = Topic::sync_missing(workspace, path).to_topic_string();
    let payload = serde_json::to_vec(&alert)?;

    // Use QoS 1 to ensure delivery (alerts are important)
    mqtt_client
        .publish(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to publish missing parent alert: {}", e)))?;

    info!(
        "Published missing parent alert for commit {}: missing {:?}",
        commit_id, missing_parents
    );

    Ok(())
}

/// Handle a received missing parent alert.
///
/// Checks if we have the requested commits and rebroadcasts them if so.
/// Uses rate limiting and jitter to avoid spam and thundering herd.
///
/// # Arguments
/// * `mqtt_client` - MQTT client for publishing
/// * `workspace` - Workspace name
/// * `path` - Document path (node ID)
/// * `alert` - The missing parent alert message
/// * `state` - Our CRDT state (to check if we have the commits)
/// * `rate_limiter` - Rate limiter to prevent spam
///
/// # Returns
/// * `Ok(rebroadcast_count)` - Number of commits rebroadcast
pub async fn handle_missing_parent_alert(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    path: &str,
    alert: &SyncMessage,
    state: &CrdtPeerState,
    rate_limiter: &SharedRateLimiter,
) -> SyncResult<usize> {
    let (_commit_id, missing_parents, client_id) = match alert {
        SyncMessage::MissingParent {
            commit_id,
            missing_parents,
            client_id,
            ..
        } => (commit_id, missing_parents, client_id),
        _ => return Err(SyncError::other("Expected MissingParent message")),
    };

    // Don't respond to our own alerts
    let our_id = state.node_id.to_string();
    if client_id == &our_id {
        debug!("Ignoring our own missing parent alert");
        return Ok(0);
    }

    let mut rebroadcast_count = 0;

    // Check each missing parent to see if we have it
    for missing_cid in missing_parents {
        // Check if we know this commit
        if !state.is_cid_known(missing_cid) {
            debug!(
                "Don't have commit {} requested by peer {}",
                missing_cid, client_id
            );
            continue;
        }

        // Check rate limiter
        {
            let limiter = rate_limiter.read().await;
            if !limiter.can_rebroadcast(missing_cid) {
                debug!("Skipping rebroadcast of {} (rate limited)", missing_cid);
                continue;
            }
        }

        // Add jitter before rebroadcasting to prevent thundering herd
        let jitter = random_jitter();
        debug!(
            "Waiting {:?} jitter before rebroadcasting commit {}",
            jitter, missing_cid
        );
        tokio::time::sleep(jitter).await;

        // Re-check rate limiter after jitter (another client may have rebroadcast)
        {
            let limiter = rate_limiter.read().await;
            if !limiter.can_rebroadcast(missing_cid) {
                debug!(
                    "Skipping rebroadcast of {} (rate limited after jitter)",
                    missing_cid
                );
                continue;
            }
        }

        // We have this commit! Rebroadcast it.
        // Since we don't have a full commit store yet (CP-cj9e), we can only
        // help if the missing commit is our current head_cid or local_head_cid.
        // When we have a proper commit store, we can rebroadcast any commit.
        if state.head_cid.as_deref() == Some(missing_cid)
            || state.local_head_cid.as_deref() == Some(missing_cid)
        {
            // Rebroadcast the current state as a commit
            if let Err(e) =
                rebroadcast_current_state(mqtt_client, workspace, path, state, missing_cid).await
            {
                warn!("Failed to rebroadcast commit {}: {}", missing_cid, e);
                continue;
            }

            // Record the rebroadcast
            {
                let mut limiter = rate_limiter.write().await;
                limiter.record_rebroadcast(missing_cid);
            }

            info!(
                "Rebroadcast commit {} to help peer {} catch up",
                missing_cid, client_id
            );
            rebroadcast_count += 1;
        } else {
            debug!(
                "Have commit {} in known_cids but not as current head, cannot rebroadcast without commit store",
                missing_cid
            );
        }
    }

    // Periodically clean up the rate limiter
    {
        let mut limiter = rate_limiter.write().await;
        limiter.cleanup();
    }

    Ok(rebroadcast_count)
}

/// Rebroadcast the current state as a commit.
///
/// This is a limited implementation until we have a proper commit store (CP-cj9e).
/// It can only rebroadcast commits where we know the full state (current head).
async fn rebroadcast_current_state(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    path: &str,
    state: &CrdtPeerState,
    expected_cid: &str,
) -> SyncResult<()> {
    // Get the current Yjs state
    let yjs_state = state
        .yjs_state
        .as_ref()
        .ok_or_else(|| SyncError::other("No Yjs state to rebroadcast"))?;

    // Build parents list - use local_head_cid's parent if available
    // This is a simplification; with a proper commit store we'd have the exact parents
    let parents = if let Some(ref head) = state.head_cid {
        if head == expected_cid {
            // This is the head commit, parents would be the previous head
            // Without commit store, we don't know the exact parents
            vec![]
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let edit_msg = EditMessage {
        update: yjs_state.clone(),
        parents,
        author: format!("rebroadcast-{}", state.node_id),
        message: Some("Rebroadcast for missing parent recovery".to_string()),
        timestamp,
    };

    let topic = Topic::edits(workspace, path).to_topic_string();
    let payload = serde_json::to_vec(&edit_msg)?;

    // Use retained message so it's immediately available
    mqtt_client
        .publish_retained(&topic, &payload, QoS::AtLeastOnce)
        .await
        .map_err(|e| SyncError::mqtt(format!("Failed to rebroadcast: {}", e)))?;

    debug!(
        "Rebroadcast current state for path {} ({} bytes)",
        path,
        yjs_state.len()
    );

    Ok(())
}

/// Check for missing parents and publish an alert if any are missing.
///
/// This should be called when processing a received commit.
/// Returns the list of missing parent CIDs (empty if all parents are known).
pub async fn check_and_alert_missing_parents(
    mqtt_client: Option<&Arc<MqttClient>>,
    workspace: &str,
    path: &str,
    commit_id: &str,
    parents: &[String],
    state: &CrdtPeerState,
    client_id: &str,
) -> Vec<String> {
    let missing = state.find_missing_parents(parents);

    if !missing.is_empty() {
        debug!(
            "Commit {} has {} missing parents: {:?}",
            commit_id,
            missing.len(),
            missing
        );

        // Publish alert if we have an MQTT client
        if let Some(mqtt) = mqtt_client {
            if let Err(e) = publish_missing_parent_alert(
                mqtt,
                workspace,
                path,
                commit_id,
                missing.clone(),
                client_id,
            )
            .await
            {
                warn!("Failed to publish missing parent alert: {}", e);
            }
        }
    }

    missing
}

/// Subscribe to missing parent alerts for a document.
///
/// Returns the subscription topic for logging/debugging.
pub async fn subscribe_to_missing_parent_alerts(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    path: &str,
) -> SyncResult<String> {
    let topic = Topic::sync_missing_pattern(workspace, path);

    mqtt_client
        .subscribe(&topic, QoS::AtLeastOnce)
        .await
        .map_err(|e| {
            SyncError::mqtt(format!(
                "Failed to subscribe to missing parent alerts: {}",
                e
            ))
        })?;

    debug!("Subscribed to missing parent alerts at: {}", topic);
    Ok(topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_first_broadcast() {
        let limiter = RebroadcastRateLimiter::new();
        assert!(limiter.can_rebroadcast("commit-1"));
    }

    #[test]
    fn test_rate_limiter_blocks_immediate_rebroadcast() {
        let mut limiter = RebroadcastRateLimiter::new();
        limiter.record_rebroadcast("commit-1");
        assert!(!limiter.can_rebroadcast("commit-1"));
    }

    #[test]
    fn test_rate_limiter_different_commits_independent() {
        let mut limiter = RebroadcastRateLimiter::new();
        limiter.record_rebroadcast("commit-1");
        assert!(!limiter.can_rebroadcast("commit-1"));
        assert!(limiter.can_rebroadcast("commit-2"));
    }

    #[tokio::test]
    async fn test_random_jitter_is_bounded() {
        for _ in 0..100 {
            let jitter = random_jitter();
            assert!(jitter <= Duration::from_millis(REBROADCAST_MAX_JITTER_MS));
        }
    }

    #[test]
    fn test_check_missing_parents() {
        let mut state = CrdtPeerState::new(Uuid::new_v4());
        state.record_known_cid("parent-1");

        // All parents known
        let missing = state.find_missing_parents(&["parent-1".to_string()]);
        assert!(missing.is_empty());

        // One parent missing
        let missing = state.find_missing_parents(&["parent-1".to_string(), "parent-2".to_string()]);
        assert_eq!(missing, vec!["parent-2"]);

        // Both parents missing
        let missing = state.find_missing_parents(&["parent-2".to_string(), "parent-3".to_string()]);
        assert_eq!(missing.len(), 2);
    }

    #[test]
    fn test_empty_parents_returns_empty() {
        let state = CrdtPeerState::new(Uuid::new_v4());
        let missing = state.find_missing_parents(&[]);
        assert!(missing.is_empty());
    }
}
