//! Actor IO document schema for visible process presence.
//!
//! An IO document is a JSON file owned by an actor (process, sync agent, etc.)
//! that represents its presence in the directory tree. Other actors can read
//! this document to discover capabilities and status.

use serde::{Deserialize, Serialize};

/// Status of an actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorStatus {
    /// Actor is running and processing events
    Active,
    /// Actor is running but idle (no pending work)
    Idle,
    /// Actor has stopped (document may be stale)
    Stopped,
    /// Actor is starting up
    Starting,
}

/// IO document schema for actor presence.
///
/// This is the expected shape of an actor's IO document (typically `__io.json`
/// in the actor's directory). It describes the actor's identity, status,
/// and capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorIO {
    /// Name of the actor (e.g., "sync-client", "bartleby")
    pub name: String,

    /// Current status
    pub status: ActorStatus,

    /// ISO 8601 timestamp when the actor started
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,

    /// ISO 8601 timestamp of the last heartbeat/activity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<String>,

    /// Process ID (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,

    /// What this actor can do (e.g., ["sync", "edit", "evaluate"])
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,

    /// Actor-specific metadata
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Conventional filename for actor IO documents.
pub const ACTOR_IO_FILENAME: &str = "__io.json";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_io_serializes_to_json() {
        let io = ActorIO {
            name: "sync-client".to_string(),
            status: ActorStatus::Active,
            started_at: Some("2026-03-07T23:00:00Z".to_string()),
            last_heartbeat: Some("2026-03-07T23:05:00Z".to_string()),
            pid: Some(12345),
            capabilities: vec!["sync".to_string(), "edit".to_string()],
            metadata: None,
        };

        let json = serde_json::to_string_pretty(&io).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["name"], "sync-client");
        assert_eq!(parsed["status"], "active");
        assert_eq!(parsed["started_at"], "2026-03-07T23:00:00Z");
        assert_eq!(parsed["pid"], 12345);
        assert_eq!(parsed["capabilities"][0], "sync");
        assert_eq!(parsed["capabilities"][1], "edit");
        // metadata should not be present (skip_serializing_if)
        assert!(parsed.get("metadata").is_none());
    }

    #[test]
    fn test_actor_io_deserializes_from_json() {
        let json = r#"{
            "name": "bartleby",
            "status": "idle",
            "pid": 9999
        }"#;

        let io: ActorIO = serde_json::from_str(json).unwrap();
        assert_eq!(io.name, "bartleby");
        assert_eq!(io.status, ActorStatus::Idle);
        assert_eq!(io.pid, Some(9999));
        assert!(io.started_at.is_none());
        assert!(io.capabilities.is_empty());
    }

    #[test]
    fn test_actor_status_roundtrip() {
        for status in [
            ActorStatus::Active,
            ActorStatus::Idle,
            ActorStatus::Stopped,
            ActorStatus::Starting,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let parsed: ActorStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_actor_io_minimal() {
        // Minimum valid IO document
        let json = r#"{"name": "test", "status": "stopped"}"#;
        let io: ActorIO = serde_json::from_str(json).unwrap();
        assert_eq!(io.name, "test");
        assert_eq!(io.status, ActorStatus::Stopped);
    }

    #[test]
    fn test_actor_io_with_metadata() {
        let io = ActorIO {
            name: "sync-client".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: None,
            pid: None,
            capabilities: vec![],
            metadata: Some(serde_json::json!({
                "node_id": "abc-123",
                "directory": "/workspace/main"
            })),
        };

        let json = serde_json::to_string(&io).unwrap();
        let roundtrip: ActorIO = serde_json::from_str(&json).unwrap();
        assert_eq!(
            roundtrip.metadata.unwrap()["node_id"],
            "abc-123"
        );
    }
}
