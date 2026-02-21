//! Client helper for checking commit ancestry.

use crate::mqtt::messages::SyncMessage;
use crate::mqtt::{MqttClient, MqttError, Topic};
use reqwest::Client;
use rumqttc::QoS;
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

const MQTT_ANCESTRY_TIMEOUT_SECS: u64 = 5;

/// Process-local MQTT context used by ancestry helpers in sync runtime.
///
/// This is set by `commonplace-sync` startup. When unset, helpers can still
/// use HTTP fallback in non-strict runtimes.
#[derive(Clone)]
pub struct SyncAncestryMqttContext {
    mqtt_client: Arc<MqttClient>,
    workspace: String,
}

impl SyncAncestryMqttContext {
    pub fn new(mqtt_client: Arc<MqttClient>, workspace: impl Into<String>) -> Self {
        Self {
            mqtt_client,
            workspace: workspace.into(),
        }
    }
}

static SYNC_ANCESTRY_MQTT_CONTEXT: OnceLock<RwLock<Option<SyncAncestryMqttContext>>> =
    OnceLock::new();

fn ancestry_mqtt_slot() -> &'static RwLock<Option<SyncAncestryMqttContext>> {
    SYNC_ANCESTRY_MQTT_CONTEXT.get_or_init(|| RwLock::new(None))
}

fn get_sync_ancestry_mqtt_context() -> Option<SyncAncestryMqttContext> {
    let guard = ancestry_mqtt_slot()
        .read()
        .expect("ancestry mqtt context lock poisoned");
    guard.clone()
}

/// Register or clear the MQTT context used by ancestry helpers.
pub fn set_sync_ancestry_mqtt_context(context: Option<SyncAncestryMqttContext>) {
    let mut guard = ancestry_mqtt_slot()
        .write()
        .expect("ancestry mqtt context lock poisoned");
    *guard = context;
}

#[derive(Debug, thiserror::Error)]
pub enum AncestryError {
    #[error("HTTP ancestry request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("MQTT ancestry request failed: {0}")]
    Mqtt(#[from] MqttError),

    #[error("Failed to serialize/parse ancestry message: {0}")]
    Json(#[from] serde_json::Error),

    #[error("MQTT ancestry response timed out (req={req}, topic={topic})")]
    Timeout { req: String, topic: String },

    #[error("MQTT ancestry message channel error: {0}")]
    Channel(String),

    #[error("MQTT ancestry query failed: {0}")]
    Response(String),

    #[error("HTTP disabled in sync runtime and MQTT ancestry context is not configured")]
    HttpDisabledWithoutMqtt,
}

#[derive(Debug, Deserialize)]
struct AncestorResponse {
    is_ancestor: bool,
}

/// Result of comparing two commit CIDs for sync direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    /// Local is ahead of server (server CID is ancestor of local CID) - push local
    Push,
    /// Server is ahead of local (local CID is ancestor of server CID) - pull server
    Pull,
    /// Both have same CID - already in sync
    InSync,
    /// Neither is ancestor of the other - diverged, needs merge
    Diverged,
}

impl SyncDirection {
    /// Returns true if server content should be applied (server is ahead or in sync).
    pub fn should_pull(&self) -> bool {
        matches!(self, SyncDirection::Pull | SyncDirection::InSync)
    }
}

async fn is_ancestor_via_http(
    client: &Client,
    server_url: &str,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, reqwest::Error> {
    // The server's is-ancestor endpoint doesn't use the doc_id (commits are global),
    // so we use a placeholder. URL-encode CIDs for safety.
    let url = format!(
        "{}/docs/any/is-ancestor?ancestor={}&descendant={}",
        server_url,
        urlencoding::encode(ancestor),
        urlencoding::encode(descendant)
    );
    let response: AncestorResponse = client.get(&url).send().await?.json().await?;
    Ok(response.is_ancestor)
}

async fn is_ancestor_via_mqtt(
    context: &SyncAncestryMqttContext,
    doc_id: &str,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, AncestryError> {
    let req = Uuid::new_v4().to_string();
    let sync_client_id = Uuid::new_v4().to_string();
    let sync_topic = Topic::sync(&context.workspace, doc_id, &sync_client_id).to_topic_string();
    let mut message_rx = context.mqtt_client.subscribe_messages();

    context
        .mqtt_client
        .subscribe(&sync_topic, QoS::AtLeastOnce)
        .await?;

    let request = SyncMessage::IsAncestor {
        req: req.clone(),
        ancestor: ancestor.to_string(),
        descendant: descendant.to_string(),
    };
    let payload = serde_json::to_vec(&request)?;
    if let Err(e) = context
        .mqtt_client
        .publish(&sync_topic, &payload, QoS::AtLeastOnce)
        .await
    {
        let _ = context.mqtt_client.unsubscribe(&sync_topic).await;
        return Err(e.into());
    }

    let deadline =
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(MQTT_ANCESTRY_TIMEOUT_SECS);
    let result = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break Err(AncestryError::Timeout {
                req: req.clone(),
                topic: sync_topic.clone(),
            });
        }

        match tokio::time::timeout(remaining, message_rx.recv()).await {
            Ok(Ok(msg)) => {
                if msg.topic != sync_topic {
                    continue;
                }

                let sync_msg: SyncMessage = match serde_json::from_slice(&msg.payload) {
                    Ok(parsed) => parsed,
                    Err(_) => continue,
                };

                match sync_msg {
                    SyncMessage::IsAncestorResponse {
                        req: response_req,
                        result,
                    } if response_req == req => {
                        break Ok(result);
                    }
                    SyncMessage::Error {
                        req: response_req,
                        message,
                        ..
                    } if response_req == req => {
                        break Err(AncestryError::Response(message));
                    }
                    _ => continue,
                }
            }
            Ok(Err(RecvError::Lagged(missed))) => {
                break Err(AncestryError::Channel(format!(
                    "lagged by {} message(s)",
                    missed
                )));
            }
            Ok(Err(RecvError::Closed)) => {
                break Err(AncestryError::Channel("channel closed".to_string()));
            }
            Err(_) => {
                break Err(AncestryError::Timeout {
                    req: req.clone(),
                    topic: sync_topic.clone(),
                });
            }
        }
    };

    if let Err(e) = context.mqtt_client.unsubscribe(&sync_topic).await {
        tracing::debug!(
            topic = %sync_topic,
            error = %e,
            "failed to unsubscribe ancestry sync topic"
        );
    }

    result
}

/// Check if `ancestor` is an ancestor of `descendant` in the commit DAG.
///
/// Prefers MQTT/cyan request-response in sync runtime and falls back to HTTP
/// only when no MQTT ancestry context is configured.
pub async fn is_ancestor(
    client: &Client,
    server_url: &str,
    doc_id: &str,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, AncestryError> {
    if ancestor == descendant {
        return Ok(true);
    }

    if let Some(context) = get_sync_ancestry_mqtt_context() {
        return is_ancestor_via_mqtt(&context, doc_id, ancestor, descendant).await;
    }

    if super::client::is_sync_http_disabled() {
        return Err(AncestryError::HttpDisabledWithoutMqtt);
    }

    Ok(is_ancestor_via_http(client, server_url, ancestor, descendant).await?)
}

fn direction_from_ancestry_checks(
    server_is_ancestor_of_local: bool,
    local_is_ancestor_of_server: bool,
) -> SyncDirection {
    if server_is_ancestor_of_local {
        SyncDirection::Push
    } else if local_is_ancestor_of_server {
        SyncDirection::Pull
    } else {
        SyncDirection::Diverged
    }
}

/// Determine sync direction by comparing local and server CIDs.
///
/// - If local_cid is None: server is ahead (or both new), pull from server
/// - If server_cid is None: local is ahead, push to server
/// - If same: in sync
/// - Otherwise: check ancestry to determine direction
pub async fn determine_sync_direction(
    client: &Client,
    server_url: &str,
    doc_id: &str,
    local_cid: Option<&str>,
    server_cid: Option<&str>,
) -> Result<SyncDirection, AncestryError> {
    match (local_cid, server_cid) {
        // Both have no commits - new document, push local content
        (None, None) => Ok(SyncDirection::Push),
        // Local has no CID but server does - server is ahead
        (None, Some(_)) => Ok(SyncDirection::Pull),
        // Local has CID but server doesn't - local is ahead (shouldn't happen normally)
        (Some(_), None) => Ok(SyncDirection::Push),
        // Both have CIDs - check if they're the same or need ancestry check
        (Some(local), Some(server)) => {
            if local == server {
                return Ok(SyncDirection::InSync);
            }

            let server_is_ancestor_of_local =
                is_ancestor(client, server_url, doc_id, server, local).await?;
            let local_is_ancestor_of_server = if server_is_ancestor_of_local {
                false
            } else {
                is_ancestor(client, server_url, doc_id, local, server).await?
            };

            Ok(direction_from_ancestry_checks(
                server_is_ancestor_of_local,
                local_is_ancestor_of_server,
            ))
        }
    }
}

/// Check if all commits in `pending` are ancestors of `descendant`.
///
/// Returns true only if every pending commit is an ancestor.
/// Used to verify local edits are included in server's merged state.
pub async fn all_are_ancestors(
    client: &Client,
    server_url: &str,
    doc_id: &Uuid,
    pending: &HashSet<String>,
    descendant: &str,
) -> Result<bool, AncestryError> {
    for ancestor in pending {
        if !is_ancestor(
            client,
            server_url,
            &doc_id.to_string(),
            ancestor,
            descendant,
        )
        .await?
        {
            tracing::debug!(
                ancestor,
                descendant,
                "pending commit is not ancestor of incoming"
            );
            return Ok(false);
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    #[test]
    fn direction_from_ancestry_checks_push_when_server_is_ancestor_of_local() {
        let direction = direction_from_ancestry_checks(true, false);
        assert_eq!(direction, SyncDirection::Push);
    }

    #[test]
    fn direction_from_ancestry_checks_pull_when_local_is_ancestor_of_server() {
        let direction = direction_from_ancestry_checks(false, true);
        assert_eq!(direction, SyncDirection::Pull);
    }

    #[test]
    fn direction_from_ancestry_checks_diverged_when_neither_is_ancestor() {
        let direction = direction_from_ancestry_checks(false, false);
        assert_eq!(direction, SyncDirection::Diverged);
    }

    #[tokio::test]
    async fn determine_sync_direction_unknown_when_ancestry_query_errors() {
        set_sync_ancestry_mqtt_context(None);
        super::super::client::set_sync_http_disabled(true);

        let client = Client::new();
        let err = determine_sync_direction(
            &client,
            "http://127.0.0.1:1",
            "doc-id",
            Some("local-cid"),
            Some("server-cid"),
        )
        .await
        .unwrap_err();

        super::super::client::set_sync_http_disabled(false);

        assert!(matches!(err, AncestryError::HttpDisabledWithoutMqtt));
    }

    #[tokio::test]
    async fn is_ancestor_fails_fast_when_http_disabled_without_mqtt_context() {
        set_sync_ancestry_mqtt_context(None);
        super::super::client::set_sync_http_disabled(true);

        let client = Client::new();
        let err = is_ancestor(&client, "http://127.0.0.1:1", "doc-id", "a", "b")
            .await
            .unwrap_err();

        super::super::client::set_sync_http_disabled(false);

        assert!(matches!(err, AncestryError::HttpDisabledWithoutMqtt));
    }
}
