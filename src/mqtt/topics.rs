//! MQTT topic parsing and construction.
//!
//! Topics follow the pattern: `{path.ext}/{port}` or `{path.ext}/{port}/{qualifier}`
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
    /// Topic format: `{path}/{port}` or `{path}/{port}/{qualifier}`
    ///
    /// The path ends at the segment containing a dot (the extension).
    pub fn parse(topic_str: &str) -> Result<Self, MqttError> {
        let segments: Vec<&str> = topic_str.split('/').collect();

        if segments.len() < 2 {
            return Err(MqttError::InvalidTopic(format!(
                "Topic must have at least path and port: {}",
                topic_str
            )));
        }

        // Find where the path ends (segment with a dot = has extension)
        let mut path_end_idx = None;
        for (i, segment) in segments.iter().enumerate() {
            if segment.contains('.') {
                path_end_idx = Some(i);
                break;
            }
        }

        let path_end_idx = path_end_idx.ok_or_else(|| {
            MqttError::InvalidTopic(format!("Path must have an extension: {}", topic_str))
        })?;

        // Path is everything up to and including the segment with the extension
        let path = segments[..=path_end_idx].join("/");

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
            MqttError::InvalidTopic(format!("Invalid port '{}' in topic: {}", port_str, topic_str))
        })?;

        // Remaining segments are the qualifier
        let qualifier = if path_end_idx + 2 < segments.len() {
            Some(segments[path_end_idx + 2..].join("/"))
        } else {
            None
        };

        Ok(Topic {
            path,
            port,
            qualifier,
        })
    }

    /// Construct an edits topic for a path.
    pub fn edits(path: &str) -> Self {
        Topic {
            path: path.to_string(),
            port: Port::Edits,
            qualifier: None,
        }
    }

    /// Construct a sync topic for a path and client ID.
    pub fn sync(path: &str, client_id: &str) -> Self {
        Topic {
            path: path.to_string(),
            port: Port::Sync,
            qualifier: Some(client_id.to_string()),
        }
    }

    /// Construct an events topic for a path and event name.
    pub fn events(path: &str, event_name: &str) -> Self {
        Topic {
            path: path.to_string(),
            port: Port::Events,
            qualifier: Some(event_name.to_string()),
        }
    }

    /// Construct a commands topic for a path and verb.
    pub fn commands(path: &str, verb: &str) -> Self {
        Topic {
            path: path.to_string(),
            port: Port::Commands,
            qualifier: Some(verb.to_string()),
        }
    }

    /// Convert the topic to its string representation.
    pub fn to_topic_string(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{}/{}/{}", self.path, self.port.as_str(), q),
            None => format!("{}/{}", self.path, self.port.as_str()),
        }
    }

    /// Get the wildcard pattern for subscribing to sync requests.
    /// Returns `{path}/sync/+`
    pub fn sync_wildcard(path: &str) -> String {
        format!("{}/sync/+", path)
    }

    /// Get the wildcard pattern for subscribing to all events.
    /// Returns `{path}/events/#`
    pub fn events_wildcard(path: &str) -> String {
        format!("{}/events/#", path)
    }

    /// Get the wildcard pattern for subscribing to all commands.
    /// Returns `{path}/commands/#`
    pub fn commands_wildcard(path: &str) -> String {
        format!("{}/commands/#", path)
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
        let topic = Topic::parse("terminal/screen.txt/edits").unwrap();
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Edits);
        assert_eq!(topic.qualifier, None);
    }

    #[test]
    fn test_parse_sync_topic() {
        let topic = Topic::parse("terminal/screen.txt/sync/client-123").unwrap();
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Sync);
        assert_eq!(topic.qualifier, Some("client-123".to_string()));
    }

    #[test]
    fn test_parse_events_topic() {
        let topic = Topic::parse("astrolabe/clock.json/events/planetary-hour").unwrap();
        assert_eq!(topic.path, "astrolabe/clock.json");
        assert_eq!(topic.port, Port::Events);
        assert_eq!(topic.qualifier, Some("planetary-hour".to_string()));
    }

    #[test]
    fn test_parse_commands_topic() {
        let topic = Topic::parse("terminal/screen.txt/commands/clear").unwrap();
        assert_eq!(topic.path, "terminal/screen.txt");
        assert_eq!(topic.port, Port::Commands);
        assert_eq!(topic.qualifier, Some("clear".to_string()));
    }

    #[test]
    fn test_parse_nested_path() {
        let topic = Topic::parse("deep/nested/path/doc.txt/edits").unwrap();
        assert_eq!(topic.path, "deep/nested/path/doc.txt");
        assert_eq!(topic.port, Port::Edits);
    }

    #[test]
    fn test_reject_no_extension() {
        let result = Topic::parse("terminal/screen/edits");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_invalid_extension() {
        let result = Topic::parse("code/main.rs/edits");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_invalid_port() {
        let result = Topic::parse("terminal/screen.txt/invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_construct_edits_topic() {
        let topic = Topic::edits("terminal/screen.txt");
        assert_eq!(topic.to_topic_string(), "terminal/screen.txt/edits");
    }

    #[test]
    fn test_construct_sync_topic() {
        let topic = Topic::sync("terminal/screen.txt", "client-123");
        assert_eq!(
            topic.to_topic_string(),
            "terminal/screen.txt/sync/client-123"
        );
    }

    #[test]
    fn test_sync_wildcard() {
        assert_eq!(
            Topic::sync_wildcard("terminal/screen.txt"),
            "terminal/screen.txt/sync/+"
        );
    }

    #[test]
    fn test_content_type_for_path() {
        assert_eq!(
            content_type_for_path("doc.json").unwrap(),
            ContentType::Json
        );
        assert_eq!(
            content_type_for_path("doc.txt").unwrap(),
            ContentType::Text
        );
        assert_eq!(content_type_for_path("doc.xml").unwrap(), ContentType::Xml);
        assert_eq!(
            content_type_for_path("doc.xhtml").unwrap(),
            ContentType::Xml
        );
        assert_eq!(
            content_type_for_path("doc.bin").unwrap(),
            ContentType::Text
        );
        assert_eq!(content_type_for_path("doc.md").unwrap(), ContentType::Text);
    }
}
