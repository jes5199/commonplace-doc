//! Identity management for the orchestrator.
//!
//! Provides types and logic for managing permanent identity records
//! in the __identities/ directory. The orchestrator ensures identities
//! exist before spawning processes and reaps stale hot presence files.

use commonplace_types::fs::actor::{ActorIO, ActorStatus};
use serde::{Deserialize, Serialize};

/// Request to ensure an identity exists in __identities/.
#[derive(Debug, Serialize, Deserialize)]
pub struct EnsureIdentityRequest {
    pub req: String,
    pub name: String,
    pub extension: String,
    /// Path to the repo root (e.g., "myapp")
    pub repo_path: String,
}

/// Response from identity ensure.
#[derive(Debug, Serialize, Deserialize)]
pub struct EnsureIdentityResponse {
    pub req: String,
    pub uuid: String,
    pub identity_path: String,
}

/// Request to list identities for a repo.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIdentitiesRequest {
    pub req: String,
    pub repo_path: String,
}

/// A single identity entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityEntry {
    pub name: String,
    pub extension: String,
    pub uuid: String,
    pub status: Option<String>,
}

/// Response from identity list.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIdentitiesResponse {
    pub req: String,
    pub identities: Vec<IdentityEntry>,
}

/// Default heartbeat timeout in seconds by extension.
pub fn default_heartbeat_timeout(extension: &str) -> u64 {
    match extension {
        "exe" => 30,
        "usr" => 300,
        "bot" => 60,
        _ => 60,
    }
}

/// Ensure an identity exists in `__identities/` for the given actor.
///
/// Uses HTTP to create the identity document via `/files/path/replace?create=true`.
/// Returns the identity path. The UUID can be resolved later via path resolution.
///
/// This is idempotent — if the identity already exists, it's a no-op (the replace
/// only updates if content differs).
pub async fn ensure_identity_via_http(
    http_client: &reqwest::Client,
    server: &str,
    repo_path: &str,
    name: &str,
    extension: &str,
) -> Result<String, String> {
    let identity_filename = format!("{}.{}.json", name, extension);
    let identity_path = format!(
        "{}/__identities/{}",
        repo_path.trim_start_matches('/'),
        identity_filename
    );

    let timeout = default_heartbeat_timeout(extension);
    let io = ActorIO {
        name: name.to_string(),
        status: ActorStatus::Stopped,
        started_at: None,
        last_heartbeat: None,
        pid: None,
        capabilities: vec![],
        metadata: None,
        docref: None,
        heartbeat_timeout_seconds: Some(timeout),
    };

    let content = serde_json::to_string_pretty(&io)
        .map_err(|e| format!("Failed to serialize identity: {}", e))?;

    let url = format!("{}/files/{}/replace?create=true", server, identity_path);
    let resp = http_client
        .post(&url)
        .body(content)
        .send()
        .await
        .map_err(|e| format!("Failed to create identity: {}", e))?;

    if resp.status().is_success() {
        Ok(identity_path)
    } else {
        Err(format!(
            "Failed to create identity: HTTP {}",
            resp.status()
        ))
    }
}

/// Resolve an identity path to its UUID via MQTT path traversal.
pub async fn resolve_identity_uuid(
    mqtt_client: &crate::mqtt::MqttRequestClient,
    fs_root: &str,
    identity_path: &str,
) -> Result<String, String> {
    mqtt_client
        .resolve_path_to_uuid(fs_root, identity_path)
        .await
        .map_err(|e| format!("Failed to resolve identity UUID: {}", e))
}

/// Check if a presence file's heartbeat has expired.
pub fn is_heartbeat_expired(io: &ActorIO) -> bool {
    let timeout = io.heartbeat_timeout_seconds.unwrap_or(60);

    let last_heartbeat = match &io.last_heartbeat {
        Some(hb) => hb,
        None => return true,
    };

    let hb_time = match chrono::DateTime::parse_from_rfc3339(last_heartbeat) {
        Ok(t) => t.timestamp() as u64,
        Err(_) => return true,
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    now - hb_time > timeout
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expired_heartbeat() {
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: Some("2020-01-01T00:00:00+00:00".to_string()),
            pid: Some(1234),
            capabilities: vec![],
            metadata: None,
            docref: None,
            heartbeat_timeout_seconds: Some(30),
        };
        assert!(is_heartbeat_expired(&io));
    }

    #[test]
    fn test_fresh_heartbeat() {
        let now = chrono::Utc::now().to_rfc3339();
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: Some(now),
            pid: Some(1234),
            capabilities: vec![],
            metadata: None,
            docref: None,
            heartbeat_timeout_seconds: Some(30),
        };
        assert!(!is_heartbeat_expired(&io));
    }

    #[test]
    fn test_no_heartbeat_is_expired() {
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: None,
            pid: None,
            capabilities: vec![],
            metadata: None,
            docref: None,
            heartbeat_timeout_seconds: Some(30),
        };
        assert!(is_heartbeat_expired(&io));
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(default_heartbeat_timeout("exe"), 30);
        assert_eq!(default_heartbeat_timeout("usr"), 300);
        assert_eq!(default_heartbeat_timeout("bot"), 60);
        assert_eq!(default_heartbeat_timeout("unknown"), 60);
    }

    #[test]
    fn test_request_serialization() {
        let req = EnsureIdentityRequest {
            req: "abc-123".to_string(),
            name: "sync".to_string(),
            extension: "exe".to_string(),
            repo_path: "myapp".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: EnsureIdentityRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "sync");
        assert_eq!(parsed.extension, "exe");
    }
}
