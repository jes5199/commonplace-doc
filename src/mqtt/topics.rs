//! MQTT topic parsing and construction.
//!
//! Topics follow the pattern: `{workspace}/{path.ext}/{port}` or `{workspace}/{path.ext}/{port}/{qualifier}`
//!
//! Valid extensions: .txt, .json, .xml, .xhtml, .bin

use crate::document::ContentType;
use crate::mqtt::MqttError;

/// MQTT port types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Port {
    /// Persistent Yjs edits
    Edits,
    /// Ephemeral sync channel for history catch-up
    Sync,
    /// Node event broadcasts
    Events,
    /// Commands to a node
    Commands,
}

impl Port {
    /// Parse a port from its string representation.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "edits" => Some(Port::Edits),
            "sync" => Some(Port::Sync),
            "events" => Some(Port::Events),
            "commands" => Some(Port::Commands),
            _ => None,
        }
    }

    /// Get the string representation of the port.
    pub fn as_str(&self) -> &'static str {
        match self {
            Port::Edits => "edits",
            Port::Sync => "sync",
            Port::Events => "events",
            Port::Commands => "commands",
        }
    }
}

/// A parsed MQTT topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    /// The workspace namespace
    pub workspace: String,
    /// The port type
    pub port: Port,
    /// The document path (e.g., "terminal/screen")
    pub path: String,
    /// Optional qualifier (e.g., client-id for sync, event-name for events)
    pub qualifier: Option<String>,
}

impl Topic {
    /// Parse a topic string into its components.
    ///
    /// Topic format: `{workspace}/{port}/{path}` or `{workspace}/{port}/{path}/{qualifier}`
    ///
    /// The port is always the second segment (after workspace), making parsing unambiguous.
    /// No file extension is required in the path.
    pub fn parse(topic_str: &str, expected_workspace: &str) -> Result<Self, MqttError> {
        let segments: Vec<&str> = topic_str.split('/').collect();

        if segments.len() < 3 {
            return Err(MqttError::InvalidTopic(format!(
                "Topic must have at least workspace, port, and path: {}",
                topic_str
            )));
        }

        // First segment is workspace
        let workspace = segments[0];
        if workspace != expected_workspace {
            return Err(MqttError::InvalidTopic(format!(
                "Topic workspace '{}' does not match expected '{}'",
                workspace, expected_workspace
            )));
        }

        // Second segment is port
        let port_str = segments[1];
        let port = Port::parse(port_str).ok_or_else(|| {
            MqttError::InvalidTopic(format!(
                "Invalid port '{}' in topic: {}",
                port_str, topic_str
            ))
        })?;

        // Remaining segments are the path (and possibly qualifier)
        // For now, we include everything as path - qualifier separation
        // is context-dependent and handled by the caller if needed
        let path = segments[2..].join("/");

        Ok(Topic {
            workspace: workspace.to_string(),
            port,
            path,
            qualifier: None, // Caller can extract qualifier from path if needed
        })
    }

    /// Construct an edits topic for a workspace and path.
    pub fn edits(workspace: &str, path: &str) -> Self {
        Topic {
            workspace: workspace.to_string(),
            path: path.to_string(),
            port: Port::Edits,
            qualifier: None,
        }
    }

    /// Construct a sync topic for a workspace, path, and client ID.
    pub fn sync(workspace: &str, path: &str, client_id: &str) -> Self {
        Topic {
            workspace: workspace.to_string(),
            path: path.to_string(),
            port: Port::Sync,
            qualifier: Some(client_id.to_string()),
        }
    }

    /// Construct an events topic for a workspace, path, and event name.
    pub fn events(workspace: &str, path: &str, event_name: &str) -> Self {
        Topic {
            workspace: workspace.to_string(),
            path: path.to_string(),
            port: Port::Events,
            qualifier: Some(event_name.to_string()),
        }
    }

    /// Construct a commands topic for a workspace, path, and verb.
    pub fn commands(workspace: &str, path: &str, verb: &str) -> Self {
        Topic {
            workspace: workspace.to_string(),
            path: path.to_string(),
            port: Port::Commands,
            qualifier: Some(verb.to_string()),
        }
    }

    /// Convert the topic to its string representation.
    ///
    /// Format: `{workspace}/{port}/{path}` or `{workspace}/{port}/{path}/{qualifier}`
    pub fn to_topic_string(&self) -> String {
        match &self.qualifier {
            Some(q) => format!(
                "{}/{}/{}/{}",
                self.workspace,
                self.port.as_str(),
                self.path,
                q
            ),
            None => format!("{}/{}/{}", self.workspace, self.port.as_str(), self.path),
        }
    }

    /// Get the wildcard pattern for subscribing to all edits in a workspace.
    /// Returns `{workspace}/edits/#`
    pub fn edits_wildcard(workspace: &str) -> String {
        format!("{}/edits/#", workspace)
    }

    /// Get the wildcard pattern for subscribing to edits under a path prefix.
    /// Returns `{workspace}/edits/{path}/#`
    pub fn edits_path_wildcard(workspace: &str, path_prefix: &str) -> String {
        format!("{}/edits/{}/#", workspace, path_prefix)
    }

    /// Get the wildcard pattern for subscribing to sync requests for a path.
    /// Returns `{workspace}/sync/{path}/+`
    pub fn sync_wildcard(workspace: &str, path: &str) -> String {
        format!("{}/sync/{}/+", workspace, path)
    }

    /// Get the wildcard pattern for subscribing to all events in a workspace.
    /// Returns `{workspace}/events/#`
    pub fn events_wildcard_all(workspace: &str) -> String {
        format!("{}/events/#", workspace)
    }

    /// Get the wildcard pattern for subscribing to all events for a path.
    /// Returns `{workspace}/events/{path}/#`
    pub fn events_wildcard(workspace: &str, path: &str) -> String {
        format!("{}/events/{}/#", workspace, path)
    }

    /// Get the wildcard pattern for subscribing to all commands in a workspace.
    /// Returns `{workspace}/commands/#`
    pub fn commands_wildcard_all(workspace: &str) -> String {
        format!("{}/commands/#", workspace)
    }

    /// Get the wildcard pattern for subscribing to all commands for a path.
    /// Returns `{workspace}/commands/{path}/#`
    pub fn commands_wildcard(workspace: &str, path: &str) -> String {
        format!("{}/commands/{}/#", workspace, path)
    }
}

/// Validate a workspace name.
/// Valid characters: alphanumeric, hyphens, underscores.
/// Must be at least one character.
pub fn validate_workspace_name(name: &str) -> Result<(), MqttError> {
    if name.is_empty() {
        return Err(MqttError::InvalidTopic(
            "Workspace name cannot be empty".to_string(),
        ));
    }

    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '-' && c != '_' {
            return Err(MqttError::InvalidTopic(format!(
                "Invalid character '{}' in workspace name. Allowed: alphanumeric, hyphen, underscore",
                c
            )));
        }
    }

    Ok(())
}

/// Get the content type for a path based on its extension.
///
/// Uses the shared content type detection from the sync module.
pub fn content_type_for_path(path: &str) -> Result<ContentType, MqttError> {
    use crate::sync::detect_from_path;
    use std::path::Path;

    let content_info = detect_from_path(Path::new(path));

    if content_info.is_binary {
        return Err(MqttError::InvalidTopic(format!(
            "Binary content type not supported: {}",
            content_info.mime_type
        )));
    }

    ContentType::from_mime(&content_info.mime_type).ok_or_else(|| {
        MqttError::InvalidTopic(format!(
            "Unsupported content type for path '{}': {}",
            path, content_info.mime_type
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================
    // Basic parsing tests (new format: workspace/port/path)
    // ========================================

    #[test]
    fn test_parse_edits_topic() {
        let topic = Topic::parse("commonplace/edits/terminal/screen", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.port, Port::Edits);
        assert_eq!(topic.path, "terminal/screen");
    }

    #[test]
    fn test_parse_sync_topic() {
        let topic = Topic::parse("commonplace/sync/terminal/screen", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.port, Port::Sync);
        assert_eq!(topic.path, "terminal/screen");
    }

    #[test]
    fn test_parse_events_topic() {
        let topic = Topic::parse("commonplace/events/astrolabe/clock", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.port, Port::Events);
        assert_eq!(topic.path, "astrolabe/clock");
    }

    #[test]
    fn test_parse_commands_topic() {
        let topic = Topic::parse("commonplace/commands/terminal/screen", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.port, Port::Commands);
        assert_eq!(topic.path, "terminal/screen");
    }

    #[test]
    fn test_parse_nested_path() {
        let topic = Topic::parse("commonplace/edits/deep/nested/path/doc", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.port, Port::Edits);
        assert_eq!(topic.path, "deep/nested/path/doc");
    }

    #[test]
    fn test_parse_path_with_extension() {
        // Extensions are allowed but not required
        let topic = Topic::parse("commonplace/edits/docs/notes.txt", "commonplace").unwrap();
        assert_eq!(topic.path, "docs/notes.txt");
    }

    #[test]
    fn test_parse_path_without_extension() {
        // No extension required anymore!
        let topic = Topic::parse("commonplace/edits/docs/notes", "commonplace").unwrap();
        assert_eq!(topic.path, "docs/notes");
    }

    #[test]
    fn test_reject_invalid_port() {
        let result = Topic::parse("commonplace/invalid/terminal/screen", "commonplace");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_too_few_segments() {
        let result = Topic::parse("commonplace/edits", "commonplace");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_wrong_workspace() {
        let result = Topic::parse("other/edits/terminal/screen", "commonplace");
        assert!(result.is_err());
    }

    // ========================================
    // Construction tests
    // ========================================

    #[test]
    fn test_construct_edits_topic() {
        let topic = Topic::edits("commonplace", "terminal/screen");
        assert_eq!(topic.to_topic_string(), "commonplace/edits/terminal/screen");
    }

    #[test]
    fn test_construct_sync_topic() {
        let topic = Topic::sync("commonplace", "terminal/screen", "client-123");
        assert_eq!(
            topic.to_topic_string(),
            "commonplace/sync/terminal/screen/client-123"
        );
    }

    #[test]
    fn test_construct_events_topic() {
        let topic = Topic::events("commonplace", "astrolabe/clock", "planetary-hour");
        assert_eq!(
            topic.to_topic_string(),
            "commonplace/events/astrolabe/clock/planetary-hour"
        );
    }

    #[test]
    fn test_construct_commands_topic() {
        let topic = Topic::commands("commonplace", "terminal/screen", "clear");
        assert_eq!(
            topic.to_topic_string(),
            "commonplace/commands/terminal/screen/clear"
        );
    }

    #[test]
    fn test_construct_edits_with_workspace() {
        let topic = Topic::edits("myworkspace", "docs/notes");
        assert_eq!(topic.workspace, "myworkspace");
        assert_eq!(topic.port, Port::Edits);
        assert_eq!(topic.path, "docs/notes");
    }

    #[test]
    fn test_construct_sync_with_workspace() {
        let topic = Topic::sync("myspace", "doc", "client-123");
        assert_eq!(topic.workspace, "myspace");
        assert_eq!(topic.port, Port::Sync);
        assert_eq!(topic.qualifier, Some("client-123".to_string()));
    }

    // ========================================
    // Wildcard tests (the key improvement!)
    // ========================================

    #[test]
    fn test_edits_wildcard_all() {
        assert_eq!(Topic::edits_wildcard("workspace"), "workspace/edits/#");
    }

    #[test]
    fn test_edits_path_wildcard() {
        assert_eq!(
            Topic::edits_path_wildcard("workspace", "docs"),
            "workspace/edits/docs/#"
        );
    }

    #[test]
    fn test_sync_wildcard() {
        assert_eq!(
            Topic::sync_wildcard("commonplace", "terminal/screen"),
            "commonplace/sync/terminal/screen/+"
        );
    }

    #[test]
    fn test_events_wildcard_all() {
        assert_eq!(
            Topic::events_wildcard_all("workspace"),
            "workspace/events/#"
        );
    }

    #[test]
    fn test_events_wildcard() {
        assert_eq!(
            Topic::events_wildcard("workspace", "docs/notes"),
            "workspace/events/docs/notes/#"
        );
    }

    #[test]
    fn test_commands_wildcard_all() {
        assert_eq!(
            Topic::commands_wildcard_all("workspace"),
            "workspace/commands/#"
        );
    }

    #[test]
    fn test_commands_wildcard() {
        assert_eq!(
            Topic::commands_wildcard("workspace", "docs/notes"),
            "workspace/commands/docs/notes/#"
        );
    }

    // ========================================
    // Roundtrip tests (construct -> to_string -> parse)
    // ========================================

    #[test]
    fn test_edits_roundtrip() {
        let original = Topic::edits("myworkspace", "docs/notes");
        let topic_str = original.to_topic_string();
        assert_eq!(topic_str, "myworkspace/edits/docs/notes");

        let parsed = Topic::parse(&topic_str, "myworkspace").unwrap();
        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.port, original.port);
        assert_eq!(parsed.path, original.path);
    }

    #[test]
    fn test_sync_roundtrip() {
        let original = Topic::sync("my-workspace", "data/config", "client-abc");
        let topic_str = original.to_topic_string();
        assert_eq!(topic_str, "my-workspace/sync/data/config/client-abc");

        let parsed = Topic::parse(&topic_str, "my-workspace").unwrap();
        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.port, original.port);
        // Note: qualifier is included in path when parsing
        assert_eq!(parsed.path, "data/config/client-abc");
    }

    // ========================================
    // Workspace isolation tests
    // ========================================

    #[test]
    fn test_different_workspaces_different_topics() {
        let t1 = Topic::edits("workspace-a", "doc");
        let t2 = Topic::edits("workspace-b", "doc");

        assert_ne!(t1.to_topic_string(), t2.to_topic_string());
        assert_eq!(t1.to_topic_string(), "workspace-a/edits/doc");
        assert_eq!(t2.to_topic_string(), "workspace-b/edits/doc");
    }

    #[test]
    fn test_workspace_mismatch_fails() {
        let topic = Topic::edits("workspace-a", "doc");
        let topic_str = topic.to_topic_string();

        let result = Topic::parse(&topic_str, "workspace-b");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not match expected"));
    }

    #[test]
    fn test_workspace_isolation() {
        let path = "shared/document";

        let topic_ws1 = Topic::edits("workspace1", path);
        let topic_ws2 = Topic::edits("workspace2", path);

        assert_ne!(topic_ws1.to_topic_string(), topic_ws2.to_topic_string());

        let ws1_str = topic_ws1.to_topic_string();
        assert!(Topic::parse(&ws1_str, "workspace2").is_err());

        let ws2_str = topic_ws2.to_topic_string();
        assert!(Topic::parse(&ws2_str, "workspace1").is_err());
    }

    // ========================================
    // All ports test
    // ========================================

    #[test]
    fn test_all_ports_with_workspace() {
        let workspace = "test-ns";
        let path = "file";

        let edits = Topic::edits(workspace, path);
        assert_eq!(edits.to_topic_string(), "test-ns/edits/file");

        let sync = Topic::sync(workspace, path, "client");
        assert_eq!(sync.to_topic_string(), "test-ns/sync/file/client");

        let events = Topic::events(workspace, path, "event");
        assert_eq!(events.to_topic_string(), "test-ns/events/file/event");

        let commands = Topic::commands(workspace, path, "cmd");
        assert_eq!(commands.to_topic_string(), "test-ns/commands/file/cmd");
    }

    // ========================================
    // Utility function tests
    // ========================================

    #[test]
    fn test_content_type_for_path() {
        assert_eq!(
            content_type_for_path("doc.json").unwrap(),
            ContentType::Json
        );
        assert_eq!(content_type_for_path("doc.txt").unwrap(), ContentType::Text);
        assert_eq!(content_type_for_path("doc.xml").unwrap(), ContentType::Xml);
        assert_eq!(
            content_type_for_path("doc.xhtml").unwrap(),
            ContentType::Xml
        );
        assert_eq!(content_type_for_path("doc.bin").unwrap(), ContentType::Text);
        assert_eq!(content_type_for_path("doc.md").unwrap(), ContentType::Text);
    }

    #[test]
    fn test_validate_workspace_valid() {
        assert!(validate_workspace_name("commonplace").is_ok());
        assert!(validate_workspace_name("my-workspace").is_ok());
        assert!(validate_workspace_name("workspace_1").is_ok());
        assert!(validate_workspace_name("A").is_ok());
    }

    #[test]
    fn test_validate_workspace_invalid() {
        assert!(validate_workspace_name("").is_err());
        assert!(validate_workspace_name("has/slash").is_err());
        assert!(validate_workspace_name("has+plus").is_err());
        assert!(validate_workspace_name("has#hash").is_err());
        assert!(validate_workspace_name("has space").is_err());
    }
}
