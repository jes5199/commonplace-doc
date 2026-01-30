//! Script resolution for evaluate processes.
//!
//! This module handles resolving script paths to UUIDs for evaluate processes,
//! enabling the orchestrator to watch script files and restart processes when they change.

use crate::fs::{Entry, FsSchema};
use crate::mqtt::MqttRequestClient;
use std::collections::HashMap;
use std::sync::Arc;

/// Maps script UUID -> list of process names that use that script.
/// Used to restart evaluate processes when their script changes.
/// Multiple processes can share the same script.
pub type ScriptWatchMap = HashMap<String, Vec<String>>;

/// Error type for script resolution operations.
#[derive(Debug)]
pub enum ResolverError {
    /// MQTT request failed
    Request(String),
    /// Path resolution failed
    PathNotFound(String),
}

impl std::fmt::Display for ResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(e) => write!(f, "MQTT request failed: {}", e),
            Self::PathNotFound(msg) => write!(f, "Path not found: {}", msg),
        }
    }
}

impl std::error::Error for ResolverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Resolves script paths to UUIDs for evaluate processes.
///
/// This struct provides methods to:
/// - Resolve a single script path to its UUID
/// - Build a complete ScriptWatchMap from a list of processes
pub struct ScriptResolver {
    request_client: Arc<MqttRequestClient>,
}

impl ScriptResolver {
    /// Create a new ScriptResolver.
    ///
    /// # Arguments
    /// * `request_client` - MQTT request client for fetching documents
    pub fn new(request_client: Arc<MqttRequestClient>) -> Self {
        Self { request_client }
    }

    /// Resolve a script path to its UUID.
    ///
    /// # Arguments
    /// * `fs_root_id` - The UUID of the fs-root document
    /// * `base_path` - The base path of the process (e.g., "workspace/myapp")
    /// * `script_name` - The script filename (e.g., "script.ts")
    ///
    /// # Returns
    /// * `Ok(Some(uuid))` - Script was found and resolved
    /// * `Ok(None)` - Script was not found (not an error, may not exist yet)
    /// * `Err(ResolverError)` - An error occurred during resolution
    pub async fn resolve_script_uuid(
        &self,
        fs_root_id: &str,
        base_path: &str,
        script_name: &str,
    ) -> Result<Option<String>, ResolverError> {
        let script_path = format!("{}/{}", base_path, script_name);

        match resolve_path_to_uuid_mqtt(&self.request_client, fs_root_id, &script_path).await {
            Ok(uuid) => Ok(Some(uuid)),
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("not found") || err_msg.contains("no entry") {
                    Ok(None)
                } else {
                    Err(ResolverError::PathNotFound(err_msg))
                }
            }
        }
    }

    /// Build a ScriptWatchMap from a list of processes with evaluate configs.
    ///
    /// This iterates through the provided processes, resolves the script UUID for each
    /// evaluate process, and builds a map of script_uuid -> process_names.
    ///
    /// # Arguments
    /// * `fs_root_id` - The UUID of the fs-root document
    /// * `processes` - Iterator of (process_name, base_path, script_name) tuples
    ///
    /// # Returns
    /// A ScriptWatchMap mapping each resolved script UUID to the list of process names
    /// that use that script. Processes whose scripts cannot be resolved are skipped
    /// with a warning log.
    pub async fn build_script_watches<'a, I>(
        &self,
        fs_root_id: &str,
        processes: I,
    ) -> ScriptWatchMap
    where
        I: IntoIterator<Item = (&'a str, &'a str, &'a str)>,
    {
        let mut script_watches = ScriptWatchMap::new();

        for (process_name, base_path, script_name) in processes {
            match self
                .resolve_script_uuid(fs_root_id, base_path, script_name)
                .await
            {
                Ok(Some(script_uuid)) => {
                    tracing::info!(
                        "[script_resolver] Watching script '{}/{}' (uuid: {}) for process '{}'",
                        base_path,
                        script_name,
                        script_uuid,
                        process_name
                    );
                    script_watches
                        .entry(script_uuid)
                        .or_default()
                        .push(process_name.to_string());
                }
                Ok(None) => {
                    tracing::warn!(
                        "[script_resolver] Script '{}/{}' not found for process '{}' (will not watch)",
                        base_path,
                        script_name,
                        process_name
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "[script_resolver] Could not resolve script '{}/{}' for process '{}': {}",
                        base_path,
                        script_name,
                        process_name,
                        e
                    );
                }
            }
        }

        script_watches
    }
}

/// Resolve a path to its UUID by walking the schema tree via MQTT.
///
/// This is the MQTT equivalent of `resolve_path_to_uuid_http` in
/// `src/sync/transport/client.rs`. The algorithm is identical:
/// walk path segments, fetch schema at each level, extract node_id.
pub async fn resolve_path_to_uuid_mqtt(
    request_client: &MqttRequestClient,
    fs_root_id: &str,
    path: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return Ok(fs_root_id.to_string());
    }

    let mut current_id = fs_root_id.to_string();

    for (i, segment) in segments.iter().enumerate() {
        let response = request_client.get_content(&current_id).await.map_err(|e| {
            format!(
                "Failed to fetch schema for '{}': {}",
                segments[..=i].join("/"),
                e
            )
        })?;

        if let Some(error) = response.error {
            return Err(format!(
                "Failed to fetch schema for '{}': {}",
                segments[..=i].join("/"),
                error
            )
            .into());
        }

        let content = response
            .content
            .ok_or_else(|| format!("No content for schema '{}'", segments[..=i].join("/")))?;

        let schema: FsSchema = serde_json::from_str(&content).map_err(|e| {
            format!(
                "Failed to parse schema for '{}': {}",
                segments[..=i].join("/"),
                e
            )
        })?;

        // Find the segment in schema entries
        let is_last = i == segments.len() - 1;
        let entry = schema
            .root
            .as_ref()
            .and_then(|root| match root {
                Entry::Dir(dir) => dir.entries.as_ref(),
                _ => None,
            })
            .and_then(|entries| entries.get(*segment));

        match entry {
            Some(Entry::Doc(doc)) if is_last => {
                return doc
                    .node_id
                    .clone()
                    .ok_or_else(|| format!("Entry '{}' has no node_id", segment).into());
            }
            Some(Entry::Dir(dir)) => {
                current_id = dir
                    .node_id
                    .clone()
                    .ok_or_else(|| format!("Directory '{}' has no node_id", segment))?;
            }
            Some(Entry::Doc(doc)) if !is_last => {
                current_id = doc
                    .node_id
                    .clone()
                    .ok_or_else(|| format!("Entry '{}' has no node_id", segment))?;
            }
            _ => {
                return Err(format!(
                    "Entry '{}' not found in schema at '{}'",
                    segment,
                    if i == 0 {
                        "/".to_string()
                    } else {
                        segments[..i].join("/")
                    }
                )
                .into());
            }
        }
    }

    Ok(current_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_watch_map_type() {
        let mut map: ScriptWatchMap = HashMap::new();
        map.insert(
            "uuid-123".to_string(),
            vec!["process1".to_string(), "process2".to_string()],
        );
        assert_eq!(map.get("uuid-123").unwrap().len(), 2);
    }

    #[test]
    fn test_resolver_error_display() {
        let err = ResolverError::PathNotFound("test path".to_string());
        assert!(err.to_string().contains("Path not found"));
        assert!(err.to_string().contains("test path"));
    }

    #[test]
    fn test_resolver_error_request_display() {
        let err = ResolverError::Request("connection refused".to_string());
        assert!(err.to_string().contains("MQTT request failed"));
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn test_script_watch_map_multiple_processes_same_script() {
        let mut map: ScriptWatchMap = HashMap::new();
        let script_uuid = "shared-script-uuid".to_string();

        map.entry(script_uuid.clone())
            .or_default()
            .push("process1".to_string());
        map.entry(script_uuid.clone())
            .or_default()
            .push("process2".to_string());
        map.entry(script_uuid.clone())
            .or_default()
            .push("process3".to_string());

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&script_uuid).unwrap().len(), 3);
    }

    #[test]
    fn test_script_watch_map_multiple_scripts() {
        let mut map: ScriptWatchMap = HashMap::new();

        map.insert("script-1".to_string(), vec!["proc-a".to_string()]);
        map.insert(
            "script-2".to_string(),
            vec!["proc-b".to_string(), "proc-c".to_string()],
        );

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("script-1").unwrap().len(), 1);
        assert_eq!(map.get("script-2").unwrap().len(), 2);
    }

    #[test]
    fn test_resolver_error_source() {
        use std::error::Error;
        let err = ResolverError::PathNotFound("test".to_string());
        assert!(err.source().is_none());
    }

    #[test]
    fn test_script_watch_map_empty() {
        let map: ScriptWatchMap = HashMap::new();
        assert!(map.is_empty());
        assert!(map.get("any-uuid").is_none());
    }
}
