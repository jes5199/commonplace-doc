//! Script resolution for evaluate processes.
//!
//! This module handles resolving script paths to UUIDs for evaluate processes,
//! enabling the orchestrator to watch script files and restart processes when they change.

use reqwest::Client;
use std::collections::HashMap;

/// Maps script UUID -> list of process names that use that script.
/// Used to restart evaluate processes when their script changes.
/// Multiple processes can share the same script.
pub type ScriptWatchMap = HashMap<String, Vec<String>>;

/// Error type for script resolution operations.
#[derive(Debug)]
pub enum ResolverError {
    /// HTTP request failed
    Request(reqwest::Error),
    /// Path resolution failed
    PathNotFound(String),
}

impl std::fmt::Display for ResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(e) => write!(f, "HTTP request failed: {}", e),
            Self::PathNotFound(msg) => write!(f, "Path not found: {}", msg),
        }
    }
}

impl std::error::Error for ResolverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Request(e) => Some(e),
            Self::PathNotFound(_) => None,
        }
    }
}

/// Resolves script paths to UUIDs for evaluate processes.
///
/// This struct provides methods to:
/// - Resolve a single script path to its UUID
/// - Build a complete ScriptWatchMap from a list of processes
pub struct ScriptResolver {
    client: Client,
    server_url: String,
}

impl ScriptResolver {
    /// Create a new ScriptResolver.
    ///
    /// # Arguments
    /// * `client` - HTTP client for making requests
    /// * `server_url` - Base URL of the commonplace server
    pub fn new(client: Client, server_url: String) -> Self {
        Self { client, server_url }
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
        use crate::sync::resolve_path_to_uuid_http;

        // Construct the full script path
        let script_path = format!("{}/{}", base_path, script_name);

        match resolve_path_to_uuid_http(&self.client, &self.server_url, fs_root_id, &script_path)
            .await
        {
            Ok(uuid) => Ok(Some(uuid)),
            Err(e) => {
                // Check if this is a "not found" error vs a real error
                let err_msg = e.to_string();
                if err_msg.contains("not found") || err_msg.contains("no entry") {
                    // Script doesn't exist yet - not an error
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
    fn test_script_resolver_new() {
        let client = Client::new();
        let resolver = ScriptResolver::new(client, "http://localhost:5199".to_string());
        assert_eq!(resolver.server_url, "http://localhost:5199");
    }

    // =========================================================================
    // Additional edge case tests
    // =========================================================================

    #[test]
    fn test_script_watch_map_multiple_processes_same_script() {
        // Multiple processes can share the same script
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

        assert_eq!(map.len(), 1); // Only one script entry
        assert_eq!(map.get(&script_uuid).unwrap().len(), 3); // Three processes
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
    fn test_resolver_error_path_not_found_display() {
        let err = ResolverError::PathNotFound("workspace/missing/script.ts".to_string());
        let display = err.to_string();
        assert!(display.contains("Path not found"));
        assert!(display.contains("workspace/missing/script.ts"));
    }

    #[test]
    fn test_resolver_error_source() {
        use std::error::Error;
        // PathNotFound should have no source error
        let err = ResolverError::PathNotFound("test".to_string());
        assert!(err.source().is_none());
    }

    #[test]
    fn test_script_watch_map_empty() {
        let map: ScriptWatchMap = HashMap::new();
        assert!(map.is_empty());
        assert!(map.get("any-uuid").is_none());
    }

    #[test]
    fn test_script_resolver_with_different_urls() {
        // Test with various URL formats
        let client = Client::new();

        let resolver1 = ScriptResolver::new(client.clone(), "http://localhost:5199".to_string());
        assert_eq!(resolver1.server_url, "http://localhost:5199");

        let resolver2 = ScriptResolver::new(client.clone(), "http://127.0.0.1:8080".to_string());
        assert_eq!(resolver2.server_url, "http://127.0.0.1:8080");

        let resolver3 = ScriptResolver::new(client, "https://example.com".to_string());
        assert_eq!(resolver3.server_url, "https://example.com");
    }

    #[test]
    fn test_script_watch_map_overwrite_processes() {
        let mut map: ScriptWatchMap = HashMap::new();

        // Initial insert
        map.insert("script-uuid".to_string(), vec!["old-process".to_string()]);

        // Overwrite with new processes
        map.insert(
            "script-uuid".to_string(),
            vec!["new-process-1".to_string(), "new-process-2".to_string()],
        );

        assert_eq!(map.len(), 1);
        let processes = map.get("script-uuid").unwrap();
        assert_eq!(processes.len(), 2);
        assert!(!processes.contains(&"old-process".to_string()));
        assert!(processes.contains(&"new-process-1".to_string()));
    }
}
