//! Discovery module for `__processes.json` files.
//!
//! This module provides the configuration format and parsing for
//! processes discovered from `__processes.json` files in the filesystem.
//!
//! For process lifecycle management, see the `discovered_manager` module.
//!
//! ## Configuration Format
//!
//! The preferred format uses file paths as keys (the file the process owns):
//!
//! ```json
//! {
//!   "output.txt": {
//!     "sandbox-exec": "deno run script.ts"
//!   }
//! }
//! ```
//!
//! The legacy format with a `processes` wrapper is still supported:
//!
//! ```json
//! {
//!   "processes": {
//!     "my-process": {
//!       "sandbox-exec": "deno run script.ts",
//!       "owns": "output.txt"
//!     }
//!   }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration parsed from `__processes.json` files.
///
/// Supports two formats:
/// 1. New format: Keys are file paths (process identity)
/// 2. Legacy format: Wrapped in `{"processes": {...}}` with optional `owns` field
#[derive(Debug, Clone, Serialize)]
pub struct ProcessesConfig {
    pub processes: HashMap<String, DiscoveredProcess>,
}

// Custom deserializer to support both new and legacy formats
impl<'de> Deserialize<'de> for ProcessesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // First, deserialize as a generic JSON value
        let value: serde_json::Value = serde_json::Value::deserialize(deserializer)?;

        let obj = value
            .as_object()
            .ok_or_else(|| D::Error::custom("expected JSON object"))?;

        // Check if this is the legacy format (has "processes" key at root)
        if obj.contains_key("processes") {
            // Legacy format: { "processes": { "name": {...} } }
            let legacy: LegacyProcessesConfig = serde_json::from_value(value)
                .map_err(|e| D::Error::custom(format!("failed to parse legacy format: {}", e)))?;

            let mut processes = HashMap::new();
            for (name, process) in legacy.processes {
                // If process has 'owns' field in legacy format, warn about deprecation
                if process.owns.is_some() {
                    tracing::warn!(
                        "Deprecated: process '{}' uses 'owns' field. \
                         Migrate to new format where the key IS the file path.",
                        name
                    );
                }
                processes.insert(name, process);
            }
            Ok(ProcessesConfig { processes })
        } else {
            // New format: keys are file paths
            // { "output.txt": { "sandbox-exec": "..." } }
            let mut processes = HashMap::new();
            for (key, process_value) in obj {
                let mut process: DiscoveredProcess = serde_json::from_value(process_value.clone())
                    .map_err(|e| {
                        D::Error::custom(format!("failed to parse process '{}': {}", key, e))
                    })?;

                // In new format, the key IS the file path identity
                // Set 'owns' to the key (the file path)
                if process.owns.is_none() {
                    process.owns = Some(key.clone());
                }
                processes.insert(key.clone(), process);
            }
            Ok(ProcessesConfig { processes })
        }
    }
}

/// Legacy format wrapper (for backwards compatibility parsing)
#[derive(Deserialize)]
struct LegacyProcessesConfig {
    processes: HashMap<String, DiscoveredProcess>,
}

/// A process discovered from a `__processes.json` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredProcess {
    /// Optional comment for documentation purposes.
    /// Ignored by the orchestrator but useful for humans reading the config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Command to run (either a string or array of strings)
    /// Mutually exclusive with sandbox_exec
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandSpec>,

    /// Convenience field: exec command to run in a commonplace-sync sandbox
    /// When set, orchestrator constructs: commonplace-sync --sandbox --exec "<value>"
    /// and sets COMMONPLACE_PATH=<process_name>, COMMONPLACE_SERVER, COMMONPLACE_INITIAL_SYNC
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sandbox-exec"
    )]
    pub sandbox_exec: Option<String>,

    /// Override the path for sandbox-exec (defaults to process name)
    /// Use "/" to mount at root, or "parent/child" for nested paths
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// File path identity for this process.
    ///
    /// In the new format, this is automatically set from the JSON key.
    /// In the legacy format, this can be explicitly set (deprecated).
    /// If absent, process is directory-attached (no file ownership).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owns: Option<String>,

    /// Working directory (optional for sandbox-exec, required for command)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,

    /// Script to evaluate with Deno (for JS evaluator processes).
    /// Path is relative to the process directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluate: Option<String>,

    /// Path to listen for stdout/stderr events from another process.
    /// When set, this process subscribes to events at the given path
    /// and writes them to the file it owns. Useful for capturing
    /// ephemeral output to a persistent log.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "log-listener"
    )]
    pub log_listener: Option<String>,
}

/// Command specification - supports both simple string and array formats.
///
/// Note: The simple string form uses whitespace splitting, not shell-style quoting.
/// For commands with spaces in arguments, use the array form instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CommandSpec {
    /// Simple command string (split on whitespace - no quoting support)
    Simple(String),
    /// Array of command and arguments (recommended for complex commands)
    Array(Vec<String>),
}

impl CommandSpec {
    /// Get the program to execute.
    pub fn program(&self) -> &str {
        match self {
            CommandSpec::Simple(s) => s.split_whitespace().next().unwrap_or(""),
            CommandSpec::Array(arr) => arr.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }

    /// Get the arguments for the command.
    pub fn args(&self) -> Vec<&str> {
        match self {
            CommandSpec::Simple(s) => s.split_whitespace().skip(1).collect(),
            CommandSpec::Array(arr) => arr.iter().skip(1).map(|s| s.as_str()).collect(),
        }
    }
}

impl ProcessesConfig {
    /// Load a `__processes.json` file from the given path.
    pub fn load(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Parse from a JSON string.
    pub fn parse(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_processes_json_with_string_command() {
        let json = r#"{
            "processes": {
                "counter": {
                    "command": "python counter.py",
                    "owns": "counter.json",
                    "cwd": "/home/user/examples"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        let counter = &config.processes["counter"];
        assert!(matches!(counter.command, Some(CommandSpec::Simple(_))));
        assert_eq!(counter.command.as_ref().unwrap().program(), "python");
        assert_eq!(counter.command.as_ref().unwrap().args(), vec!["counter.py"]);
        assert_eq!(counter.owns, Some("counter.json".to_string()));
        assert_eq!(counter.cwd, Some(PathBuf::from("/home/user/examples")));
    }

    #[test]
    fn test_parse_processes_json_with_array_command() {
        let json = r#"{
            "processes": {
                "server": {
                    "command": ["node", "server.js", "--port", "3000"],
                    "owns": "state.json",
                    "cwd": "/opt/app"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        let server = &config.processes["server"];
        assert!(matches!(server.command, Some(CommandSpec::Array(_))));
        assert_eq!(server.command.as_ref().unwrap().program(), "node");
        assert_eq!(
            server.command.as_ref().unwrap().args(),
            vec!["server.js", "--port", "3000"]
        );
        assert_eq!(server.owns, Some("state.json".to_string()));
        assert_eq!(server.cwd, Some(PathBuf::from("/opt/app")));
    }

    #[test]
    fn test_command_spec_deserialization() {
        // Test simple string
        let simple: CommandSpec = serde_json::from_str(r#""echo hello world""#).unwrap();
        assert!(matches!(simple, CommandSpec::Simple(_)));
        assert_eq!(simple.program(), "echo");
        assert_eq!(simple.args(), vec!["hello", "world"]);

        // Test array
        let array: CommandSpec = serde_json::from_str(r#"["echo", "hello", "world"]"#).unwrap();
        assert!(matches!(array, CommandSpec::Array(_)));
        assert_eq!(array.program(), "echo");
        assert_eq!(array.args(), vec!["hello", "world"]);
    }

    #[test]
    fn test_command_spec_serialization() {
        let simple = CommandSpec::Simple("python script.py".to_string());
        let json = serde_json::to_string(&simple).unwrap();
        assert_eq!(json, r#""python script.py""#);

        let array = CommandSpec::Array(vec![
            "python".to_string(),
            "script.py".to_string(),
            "--verbose".to_string(),
        ]);
        let json = serde_json::to_string(&array).unwrap();
        assert_eq!(json, r#"["python","script.py","--verbose"]"#);
    }

    #[test]
    fn test_empty_command() {
        let spec = CommandSpec::Simple("".to_string());
        assert_eq!(spec.program(), "");
        assert!(spec.args().is_empty());

        let spec = CommandSpec::Array(vec![]);
        assert_eq!(spec.program(), "");
        assert!(spec.args().is_empty());
    }

    #[test]
    fn test_parse_directory_attached_process() {
        let json = r#"{
            "processes": {
                "sandbox": {
                    "command": "commonplace-sync --sandbox --exec ./run.sh",
                    "cwd": "/home/user/sandbox"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        let sandbox = &config.processes["sandbox"];
        assert!(sandbox.owns.is_none());
        assert_eq!(sandbox.cwd, Some(PathBuf::from("/home/user/sandbox")));
    }

    #[test]
    fn test_multiple_processes() {
        let json = r#"{
            "processes": {
                "frontend": {
                    "command": "npm start",
                    "owns": "frontend-state.json",
                    "cwd": "/app/frontend"
                },
                "backend": {
                    "command": ["cargo", "run", "--release"],
                    "owns": "backend-state.json",
                    "cwd": "/app/backend"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 2);
        assert!(config.processes.contains_key("frontend"));
        assert!(config.processes.contains_key("backend"));
    }

    #[test]
    fn test_mixed_file_and_directory_attached() {
        let json = r#"{
            "processes": {
                "counter": {
                    "command": "python counter.py",
                    "owns": "counter.json",
                    "cwd": "/app/counter"
                },
                "sandbox": {
                    "command": "commonplace-sync --sandbox --exec ./run.sh",
                    "cwd": "/app/sandbox"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 2);

        let counter = &config.processes["counter"];
        assert_eq!(counter.owns, Some("counter.json".to_string()));

        let sandbox = &config.processes["sandbox"];
        assert!(sandbox.owns.is_none());
    }

    #[test]
    fn test_evaluate_process() {
        let json = r#"{
            "processes": {
                "transform": {
                    "evaluate": "transform.ts",
                    "owns": "output.txt"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let transform = &config.processes["transform"];
        assert_eq!(transform.evaluate, Some("transform.ts".to_string()));
        assert_eq!(transform.owns, Some("output.txt".to_string()));
    }

    #[test]
    fn test_process_with_comment() {
        let json = r#"{
            "processes": {
                "api": {
                    "comment": "Main API server - handles all HTTP requests",
                    "sandbox-exec": "node server.js"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let api = &config.processes["api"];
        assert_eq!(
            api.comment,
            Some("Main API server - handles all HTTP requests".to_string())
        );
        assert_eq!(api.sandbox_exec, Some("node server.js".to_string()));
    }

    #[test]
    fn test_comment_is_optional() {
        let json = r#"{
            "processes": {
                "worker": {
                    "sandbox-exec": "python worker.py"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let worker = &config.processes["worker"];
        assert_eq!(worker.comment, None);
    }

    // =========================================================================
    // New format tests (key is file path)
    // =========================================================================

    #[test]
    fn test_new_format_simple() {
        // New format: key is the file path identity
        let json = r#"{
            "output.txt": {
                "sandbox-exec": "deno run script.ts"
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        // Key is "output.txt" - the file path
        let process = &config.processes["output.txt"];
        assert_eq!(process.sandbox_exec, Some("deno run script.ts".to_string()));
        // 'owns' is automatically set to the key
        assert_eq!(process.owns, Some("output.txt".to_string()));
    }

    #[test]
    fn test_new_format_with_evaluate() {
        let json = r#"{
            "output.txt": {
                "evaluate": "transform.ts"
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let process = &config.processes["output.txt"];
        assert_eq!(process.evaluate, Some("transform.ts".to_string()));
        assert_eq!(process.owns, Some("output.txt".to_string()));
    }

    #[test]
    fn test_new_format_multiple_processes() {
        let json = r#"{
            "output.txt": {
                "evaluate": "script1.ts"
            },
            "data.json": {
                "sandbox-exec": "node processor.js"
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 2);

        assert_eq!(
            config.processes["output.txt"].owns,
            Some("output.txt".to_string())
        );
        assert_eq!(
            config.processes["data.json"].owns,
            Some("data.json".to_string())
        );
    }

    #[test]
    fn test_new_format_nested_path() {
        // Nested paths like "subdir/output.txt" are supported
        let json = r#"{
            "subdir/output.txt": {
                "evaluate": "transform.ts"
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let process = &config.processes["subdir/output.txt"];
        assert_eq!(process.owns, Some("subdir/output.txt".to_string()));
    }

    #[test]
    fn test_new_format_with_all_fields() {
        let json = r#"{
            "results.json": {
                "comment": "Processes input and writes to results.json",
                "sandbox-exec": "python process.py",
                "path": "/custom/path",
                "cwd": "/app/workdir"
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let process = &config.processes["results.json"];
        assert_eq!(
            process.comment,
            Some("Processes input and writes to results.json".to_string())
        );
        assert_eq!(process.sandbox_exec, Some("python process.py".to_string()));
        assert_eq!(process.path, Some("/custom/path".to_string()));
        assert_eq!(process.cwd, Some(PathBuf::from("/app/workdir")));
        assert_eq!(process.owns, Some("results.json".to_string()));
    }

    #[test]
    fn test_legacy_format_still_works() {
        // Ensure legacy format with "processes" wrapper still works
        let json = r#"{
            "processes": {
                "my-process": {
                    "sandbox-exec": "node app.js",
                    "owns": "output.txt"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        assert_eq!(config.processes.len(), 1);

        // In legacy format, key is the process name
        let process = &config.processes["my-process"];
        assert_eq!(process.sandbox_exec, Some("node app.js".to_string()));
        // 'owns' is preserved from the JSON
        assert_eq!(process.owns, Some("output.txt".to_string()));
    }

    #[test]
    fn test_legacy_format_directory_attached() {
        // Legacy format without 'owns' field = directory-attached
        let json = r#"{
            "processes": {
                "sandbox-process": {
                    "sandbox-exec": "node app.js"
                }
            }
        }"#;

        let config = ProcessesConfig::parse(json).unwrap();
        let process = &config.processes["sandbox-process"];
        // No 'owns' field in JSON, so it's None (directory-attached)
        assert!(process.owns.is_none());
    }
}
