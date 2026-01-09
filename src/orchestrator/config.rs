use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    /// Path to the redb database file. Required for persistent storage.
    /// The orchestrator will automatically pass this to the server process.
    pub database: PathBuf,
    #[serde(default = "default_workspace")]
    pub workspace: String,
    #[serde(default = "default_mqtt_broker")]
    pub mqtt_broker: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_server: Option<String>,
    /// Filesystem paths managed by this orchestrator.
    /// Used for documentation and potential overlap detection.
    /// Example: ["/workspace/team-a", "/shared/data"]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub managed_paths: Vec<String>,
    pub processes: HashMap<String, ProcessConfig>,
}

fn default_workspace() -> String {
    "commonplace".to_string()
}

fn default_mqtt_broker() -> String {
    "localhost:1883".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub restart: RestartPolicy,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartPolicy {
    #[serde(default = "default_policy")]
    pub policy: RestartMode,
    #[serde(default = "default_backoff")]
    pub backoff_ms: u64,
    #[serde(default = "default_max_backoff")]
    pub max_backoff_ms: u64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            policy: RestartMode::Always,
            backoff_ms: 500,
            max_backoff_ms: 10000,
        }
    }
}

fn default_policy() -> RestartMode {
    RestartMode::Always
}

fn default_backoff() -> u64 {
    500
}

fn default_max_backoff() -> u64 {
    10000
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RestartMode {
    Always,
    OnFailure,
    Never,
}

impl OrchestratorConfig {
    pub fn load(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Compute a scoped lock file path based on the config file path.
    /// This allows multiple orchestrators to run with different configs.
    /// The lock file name is derived from a hash of the canonical config path.
    pub fn lock_file_path(config_path: &std::path::Path) -> PathBuf {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Use canonical path for consistency across relative/absolute paths
        let canonical = config_path
            .canonicalize()
            .unwrap_or_else(|_| config_path.to_path_buf());

        // Hash the path to create a unique but short identifier
        let mut hasher = DefaultHasher::new();
        canonical.hash(&mut hasher);
        let hash = hasher.finish();

        // Use first 12 hex chars of hash for readability
        let hash_str = format!("{:012x}", hash & 0xffffffffffff);

        std::env::temp_dir().join(format!("commonplace-orchestrator-{}.lock", hash_str))
    }

    /// Resolve the HTTP server URL with fallback chain:
    /// 1. Parse --port from http process args
    /// 2. Use http_server config field
    /// 3. Default to http://localhost:3000
    pub fn resolve_server_url(&self) -> String {
        // Try to parse port from http process args
        if let Some(http_config) = self.processes.get("http") {
            let args = &http_config.args;
            for (i, arg) in args.iter().enumerate() {
                if arg == "--port" || arg == "-p" {
                    if let Some(port) = args.get(i + 1) {
                        return format!("http://localhost:{}", port);
                    }
                }
            }
        }

        // Fall back to explicit http_server field
        if let Some(ref url) = self.http_server {
            return url.clone();
        }

        // Default
        "http://localhost:3000".to_string()
    }

    /// Returns process names in dependency order (dependencies first)
    pub fn startup_order(&self) -> Result<Vec<String>, String> {
        let mut order = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut visiting = std::collections::HashSet::new();

        fn visit(
            name: &str,
            processes: &HashMap<String, ProcessConfig>,
            visited: &mut std::collections::HashSet<String>,
            visiting: &mut std::collections::HashSet<String>,
            order: &mut Vec<String>,
        ) -> Result<(), String> {
            if visited.contains(name) {
                return Ok(());
            }
            if visiting.contains(name) {
                return Err(format!("Dependency cycle detected involving '{}'", name));
            }
            visiting.insert(name.to_string());

            if let Some(config) = processes.get(name) {
                for dep in &config.depends_on {
                    if !processes.contains_key(dep) {
                        return Err(format!(
                            "Unknown dependency '{}' for process '{}'",
                            dep, name
                        ));
                    }
                    visit(dep, processes, visited, visiting, order)?;
                }
            }

            visiting.remove(name);
            visited.insert(name.to_string());
            order.push(name.to_string());
            Ok(())
        }

        // Sort keys for deterministic ordering
        let mut keys: Vec<_> = self.processes.keys().collect();
        keys.sort();

        for name in keys {
            visit(
                name,
                &self.processes,
                &mut visited,
                &mut visiting,
                &mut order,
            )?;
        }

        Ok(order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let json = r#"{
            "database": "./data.redb",
            "processes": {
                "store": {
                    "command": "commonplace-store"
                }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.database, PathBuf::from("./data.redb"));
        assert_eq!(config.mqtt_broker, "localhost:1883");
        assert_eq!(config.processes.len(), 1);
        assert_eq!(config.processes["store"].command, "commonplace-store");
        assert_eq!(
            config.processes["store"].restart.policy,
            RestartMode::Always
        );
    }

    #[test]
    fn test_database_required() {
        let json = r#"{
            "processes": {}
        }"#;
        let result: Result<OrchestratorConfig, _> = serde_json::from_str(json);
        assert!(result.is_err(), "database field should be required");
    }

    #[test]
    fn test_parse_full_config() {
        let json = r#"{
            "database": "./data.redb",
            "mqtt_broker": "localhost:1884",
            "processes": {
                "store": {
                    "command": "commonplace-store",
                    "args": ["--database", "./data.redb"],
                    "restart": { "policy": "on_failure", "backoff_ms": 1000, "max_backoff_ms": 30000 }
                },
                "http": {
                    "command": "commonplace-http",
                    "args": ["--port", "3000"],
                    "depends_on": ["store"]
                }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.mqtt_broker, "localhost:1884");
        assert_eq!(
            config.processes["store"].restart.policy,
            RestartMode::OnFailure
        );
        assert_eq!(config.processes["store"].restart.backoff_ms, 1000);
        assert_eq!(config.processes["http"].depends_on, vec!["store"]);
    }

    #[test]
    fn test_dependency_order() {
        let json = r#"{
            "database": "./data.redb",
            "processes": {
                "http": { "command": "http", "depends_on": ["store"] },
                "store": { "command": "store", "depends_on": ["broker"] },
                "broker": { "command": "broker" }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        let order = config.startup_order().unwrap();
        assert_eq!(order, vec!["broker", "store", "http"]);
    }

    #[test]
    fn test_dependency_cycle_detected() {
        let json = r#"{
            "database": "./data.redb",
            "processes": {
                "a": { "command": "a", "depends_on": ["b"] },
                "b": { "command": "b", "depends_on": ["a"] }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert!(config.startup_order().is_err());
    }

    #[test]
    fn test_resolve_server_url_from_http_args() {
        let json = r#"{
            "database": "./data.redb",
            "processes": {
                "http": {
                    "command": "commonplace-http",
                    "args": ["--port", "8080"]
                }
            }
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.resolve_server_url(), "http://localhost:8080");
    }

    #[test]
    fn test_resolve_server_url_from_explicit_field() {
        let json = r#"{
            "database": "./data.redb",
            "http_server": "http://example.com:3000",
            "processes": {}
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.resolve_server_url(), "http://example.com:3000");
    }

    #[test]
    fn test_resolve_server_url_default() {
        let json = r#"{ "database": "./data.redb", "processes": {} }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.resolve_server_url(), "http://localhost:3000");
    }

    #[test]
    fn test_lock_file_path_unique_per_config() {
        use std::path::Path;

        let path_a = Path::new("/tmp/config-a.json");
        let path_b = Path::new("/tmp/config-b.json");

        let lock_a = OrchestratorConfig::lock_file_path(path_a);
        let lock_b = OrchestratorConfig::lock_file_path(path_b);

        // Different configs should have different lock files
        assert_ne!(lock_a, lock_b);

        // Same config should have same lock file
        let lock_a2 = OrchestratorConfig::lock_file_path(path_a);
        assert_eq!(lock_a, lock_a2);
    }

    #[test]
    fn test_lock_file_path_format() {
        use std::path::Path;

        let path = Path::new("/tmp/test-config.json");
        let lock = OrchestratorConfig::lock_file_path(path);

        // Should be in temp dir with expected prefix
        let filename = lock.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("commonplace-orchestrator-"));
        assert!(filename.ends_with(".lock"));
    }

    #[test]
    fn test_managed_paths_field() {
        let json = r#"{
            "database": "./data.redb",
            "managed_paths": ["/workspace/team-a", "/shared"],
            "processes": {}
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.managed_paths.len(), 2);
        assert_eq!(config.managed_paths[0], "/workspace/team-a");
    }

    #[test]
    fn test_managed_paths_optional() {
        let json = r#"{ "database": "./data.redb", "processes": {} }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert!(config.managed_paths.is_empty());
    }

    #[test]
    fn test_workspace_field() {
        let json = r#"{
            "database": "./data.redb",
            "workspace": "myspace",
            "processes": {}
        }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.workspace, "myspace");
    }

    #[test]
    fn test_workspace_default() {
        let json = r#"{ "database": "./data.redb", "processes": {} }"#;
        let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.workspace, "commonplace");
    }
}
