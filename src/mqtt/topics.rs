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
    /// The document path (e.g., "terminal/screen.txt")
    pub path: String,
    /// The port type
    pub port: Port,
    /// Optional qualifier (e.g., client-id for sync, event-name for events)
    pub qualifier: Option<String>,
}

impl Topic {
    /// Parse a topic string into its components.
    ///
    /// Topic format: `{workspace}/{path}/{port}` or `{workspace}/{path}/{port}/{qualifier}`
    ///
    /// The path ends at the segment containing a dot (the extension).
    pub fn parse(topic_str: &str, expected_workspace: &str) -> Result<Self, MqttError> {
        let segments: Vec<&str> = topic_str.split('/').collect();

        if segments.len() < 3 {
            return Err(MqttError::InvalidTopic(format!(
                "Topic must have at least workspace, path, and port: {}",
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

        // Find where the path ends (segment with a dot = has extension)
        let mut path_end_idx = None;
        for (i, segment) in segments.iter().enumerate().skip(1) {
            if segment.contains('.') {
                path_end_idx = Some(i);
                break;
            }
        }

        let path_end_idx = path_end_idx.ok_or_else(|| {
            MqttError::InvalidTopic(format!("Path must have an extension: {}", topic_str))
        })?;

        // Path is from index 1 to path_end_idx (skip workspace)
        let path = segments[1..=path_end_idx].join("/");

        // Validate the extension
        validate_extension(&path)?;

        // Next segment should be the port
        if path_end_idx + 1 >= segments.len() {
            return Err(MqttError::InvalidTopic(format!(
                "Missing port in topic: {}",
                topic_str
            )));
        }

        let port_str = segments[path_end_idx + 1];
        let port = Port::parse(port_str).ok_or_else(|| {
            MqttError::InvalidTopic(format!(
                "Invalid port '{}' in topic: {}",
                port_str, topic_str
            ))
        })?;

        // Remaining segments are the qualifier
        let qualifier = if path_end_idx + 2 < segments.len() {
            Some(segments[path_end_idx + 2..].join("/"))
        } else {
            None
        };

        Ok(Topic {
            workspace: workspace.to_string(),
            path,
            port,
            qualifier,
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
    pub fn to_topic_string(&self) -> String {
        match &self.qualifier {
            Some(q) => format!(
                "{}/{}/{}/{}",
                self.workspace,
                self.path,
                self.port.as_str(),
                q
            ),
            None => format!("{}/{}/{}", self.workspace, self.path, self.port.as_str()),
        }
    }

    /// Get the wildcard pattern for subscribing to sync requests.
    /// Returns `{workspace}/{path}/sync/+`
    pub fn sync_wildcard(workspace: &str, path: &str) -> String {
        format!("{}/{}/sync/+", workspace, path)
    }

    /// Get the wildcard pattern for subscribing to all events.
    /// Returns `{workspace}/{path}/events/#`
    pub fn events_wildcard(workspace: &str, path: &str) -> String {
        format!("{}/{}/events/#", workspace, path)
    }

    /// Get the wildcard pattern for subscribing to all commands.
    /// Returns `{workspace}/{path}/commands/#`
    pub fn commands_wildcard(workspace: &str, path: &str) -> String {
        format!("{}/{}/commands/#", workspace, path)
    }
}

/// Allowed file extensions for sync operations.
const ALLOWED_EXTENSIONS: &[&str] = &["txt", "json", "xml", "xhtml", "bin", "md"];

/// Validate that a path has an allowed extension.
pub fn validate_extension(path: &str) -> Result<(), MqttError> {
    let ext = path
        .rsplit('.')
        .next()
        .map(|s| s.to_lowercase())
        .ok_or_else(|| MqttError::InvalidTopic(format!("Path has no extension: {}", path)))?;

    if ALLOWED_EXTENSIONS.contains(&ext.as_str()) {
        Ok(())
    } else {
        Err(MqttError::InvalidTopic(format!(
            "Extension '{}' not allowed. Valid: {:?}",
            ext, ALLOWED_EXTENSIONS
        )))
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
pub fn content_type_for_path(path: &str) -> Result<ContentType, MqttError> {
    let ext = path
        .rsplit('.')
        .next()
        .map(|s| s.to_lowercase())
        .ok_or_else(|| MqttError::InvalidTopic(format!("Path has no extension: {}", path)))?;

    match ext.as_str() {
        "json" => Ok(ContentType::Json),
        "txt" | "bin" | "md" => Ok(ContentType::Text),
        "xml" | "xhtml" => Ok(ContentType::Xml),
        _ => Err(MqttError::InvalidTopic(format!(
            "Unknown extension: {}",
            ext
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_edits_topic() {
        let topic = Topic::parse("commonplace/terminal/screen.txt/edits", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Edits);
        assert_eq!(topic.qualifier, None);
    }

    #[test]
    fn test_parse_sync_topic() {
        let topic = Topic::parse(
            "commonplace/terminal/screen.txt/sync/client-123",
            "commonplace",
        )
        .unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Sync);
        assert_eq!(topic.qualifier, Some("client-123".to_string()));
    }

    #[test]
    fn test_parse_events_topic() {
        let topic = Topic::parse(
            "commonplace/astrolabe/clock.json/events/planetary-hour",
            "commonplace",
        )
        .unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "astrolabe/clock.json");
        assert_eq!(topic.port, Port::Events);
        assert_eq!(topic.qualifier, Some("planetary-hour".to_string()));
    }

    #[test]
    fn test_parse_commands_topic() {
        let topic = Topic::parse(
            "commonplace/terminal/screen.txt/commands/clear",
            "commonplace",
        )
        .unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Commands);
        assert_eq!(topic.qualifier, Some("clear".to_string()));
    }

    #[test]
    fn test_parse_nested_path() {
        let topic =
            Topic::parse("commonplace/deep/nested/path/doc.txt/edits", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "deep/nested/path/doc.txt");
        assert_eq!(topic.port, Port::Edits);
    }

    #[test]
    fn test_reject_no_extension() {
        let result = Topic::parse("commonplace/terminal/screen/edits", "commonplace");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_invalid_extension() {
        let result = Topic::parse("commonplace/code/main.rs/edits", "commonplace");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_invalid_port() {
        let result = Topic::parse("commonplace/terminal/screen.txt/invalid", "commonplace");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_with_workspace() {
        let topic = Topic::parse("commonplace/terminal/screen.txt/edits", "commonplace").unwrap();
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Edits);
    }

    #[test]
    fn test_parse_wrong_workspace() {
        let result = Topic::parse("other/terminal/screen.txt/edits", "commonplace");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_with_qualifier() {
        let topic = Topic::parse("myspace/doc.txt/sync/client-123", "myspace").unwrap();
        assert_eq!(topic.workspace, "myspace");
        assert_eq!(topic.qualifier, Some("client-123".to_string()));
    }

    #[test]
    fn test_construct_edits_topic() {
        let topic = Topic::edits("commonplace", "terminal/screen.txt");
        assert_eq!(
            topic.to_topic_string(),
            "commonplace/terminal/screen.txt/edits"
        );
    }

    #[test]
    fn test_construct_sync_topic() {
        let topic = Topic::sync("commonplace", "terminal/screen.txt", "client-123");
        assert_eq!(
            topic.to_topic_string(),
            "commonplace/terminal/screen.txt/sync/client-123"
        );
    }

    #[test]
    fn test_to_topic_string_with_workspace() {
        let topic = Topic::edits("commonplace", "terminal/screen.txt");
        assert_eq!(
            topic.to_topic_string(),
            "commonplace/terminal/screen.txt/edits"
        );
    }

    #[test]
    fn test_to_topic_string_with_qualifier() {
        let topic = Topic::sync("myspace", "doc.txt", "client-123");
        assert_eq!(topic.to_topic_string(), "myspace/doc.txt/sync/client-123");
    }

    #[test]
    fn test_sync_wildcard() {
        assert_eq!(
            Topic::sync_wildcard("commonplace", "terminal/screen.txt"),
            "commonplace/terminal/screen.txt/sync/+"
        );
    }

    #[test]
    fn test_construct_edits_with_workspace() {
        let topic = Topic::edits("commonplace", "terminal/screen.txt");
        assert_eq!(topic.workspace, "commonplace");
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Edits);
    }

    #[test]
    fn test_construct_sync_with_workspace() {
        let topic = Topic::sync("myspace", "doc.txt", "client-123");
        assert_eq!(topic.workspace, "myspace");
        assert_eq!(topic.qualifier, Some("client-123".to_string()));
    }

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

    #[test]
    fn test_topic_with_workspace() {
        let topic = Topic {
            workspace: "commonplace".to_string(),
            path: "terminal/screen.txt".to_string(),
            port: Port::Edits,
            qualifier: None,
        };
        assert_eq!(topic.workspace, "commonplace");
    }

    // ========================================
    // Workspace namespacing integration tests
    // ========================================

    /// Test that topics roundtrip correctly: construct -> to_topic_string -> parse
    #[test]
    fn test_topic_roundtrip_with_workspace() {
        let original = Topic::edits("myworkspace", "docs/notes.txt");
        let topic_str = original.to_topic_string();
        assert_eq!(topic_str, "myworkspace/docs/notes.txt/edits");

        let parsed = Topic::parse(&topic_str, "myworkspace").unwrap();
        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.path, original.path);
        assert_eq!(parsed.port, original.port);
        assert_eq!(parsed.qualifier, original.qualifier);
    }

    /// Test that different workspaces produce different topic strings
    #[test]
    fn test_different_workspaces_different_topics() {
        let t1 = Topic::edits("workspace-a", "doc.txt");
        let t2 = Topic::edits("workspace-b", "doc.txt");

        assert_ne!(t1.to_topic_string(), t2.to_topic_string());
        assert_eq!(t1.to_topic_string(), "workspace-a/doc.txt/edits");
        assert_eq!(t2.to_topic_string(), "workspace-b/doc.txt/edits");
    }

    /// Test roundtrip for sync topics with qualifiers
    #[test]
    fn test_sync_topic_roundtrip_with_workspace() {
        let original = Topic::sync("my-workspace", "data/config.json", "client-abc");
        let topic_str = original.to_topic_string();
        assert_eq!(topic_str, "my-workspace/data/config.json/sync/client-abc");

        let parsed = Topic::parse(&topic_str, "my-workspace").unwrap();
        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.path, original.path);
        assert_eq!(parsed.port, original.port);
        assert_eq!(parsed.qualifier, original.qualifier);
    }

    /// Test roundtrip for events topics with qualifiers
    #[test]
    fn test_events_topic_roundtrip_with_workspace() {
        let original = Topic::events("test_workspace", "sensors/temp.json", "reading-update");
        let topic_str = original.to_topic_string();
        assert_eq!(
            topic_str,
            "test_workspace/sensors/temp.json/events/reading-update"
        );

        let parsed = Topic::parse(&topic_str, "test_workspace").unwrap();
        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.path, original.path);
        assert_eq!(parsed.port, original.port);
        assert_eq!(parsed.qualifier, original.qualifier);
    }

    /// Test roundtrip for commands topics with qualifiers
    #[test]
    fn test_commands_topic_roundtrip_with_workspace() {
        let original = Topic::commands("prod-workspace", "control/device.json", "reset");
        let topic_str = original.to_topic_string();
        assert_eq!(
            topic_str,
            "prod-workspace/control/device.json/commands/reset"
        );

        let parsed = Topic::parse(&topic_str, "prod-workspace").unwrap();
        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.path, original.path);
        assert_eq!(parsed.port, original.port);
        assert_eq!(parsed.qualifier, original.qualifier);
    }

    /// Test that parsing with wrong workspace fails
    #[test]
    fn test_workspace_mismatch_fails() {
        let topic = Topic::edits("workspace-a", "doc.txt");
        let topic_str = topic.to_topic_string();

        // Try to parse with different workspace
        let result = Topic::parse(&topic_str, "workspace-b");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not match expected"));
    }

    /// Test wildcard patterns include workspace
    #[test]
    fn test_wildcards_include_workspace() {
        let workspace = "my-workspace";
        let path = "docs/notes.txt";

        assert_eq!(
            Topic::sync_wildcard(workspace, path),
            "my-workspace/docs/notes.txt/sync/+"
        );
        assert_eq!(
            Topic::events_wildcard(workspace, path),
            "my-workspace/docs/notes.txt/events/#"
        );
        assert_eq!(
            Topic::commands_wildcard(workspace, path),
            "my-workspace/docs/notes.txt/commands/#"
        );
    }

    /// Test that all port types work with workspace namespacing
    #[test]
    fn test_all_ports_with_workspace() {
        let workspace = "test-ns";
        let path = "file.txt";

        // Edits
        let edits = Topic::edits(workspace, path);
        assert_eq!(edits.to_topic_string(), "test-ns/file.txt/edits");

        // Sync
        let sync = Topic::sync(workspace, path, "client");
        assert_eq!(sync.to_topic_string(), "test-ns/file.txt/sync/client");

        // Events
        let events = Topic::events(workspace, path, "event");
        assert_eq!(events.to_topic_string(), "test-ns/file.txt/events/event");

        // Commands
        let commands = Topic::commands(workspace, path, "cmd");
        assert_eq!(commands.to_topic_string(), "test-ns/file.txt/commands/cmd");
    }

    /// Test workspace isolation: same paths in different workspaces are independent
    #[test]
    fn test_workspace_isolation() {
        let path = "shared/document.txt";

        let topic_ws1 = Topic::edits("workspace1", path);
        let topic_ws2 = Topic::edits("workspace2", path);

        // They should have different topic strings
        assert_ne!(topic_ws1.to_topic_string(), topic_ws2.to_topic_string());

        // Parsing one workspace's topic with the other should fail
        let ws1_str = topic_ws1.to_topic_string();
        assert!(Topic::parse(&ws1_str, "workspace2").is_err());

        let ws2_str = topic_ws2.to_topic_string();
        assert!(Topic::parse(&ws2_str, "workspace1").is_err());
    }
}
