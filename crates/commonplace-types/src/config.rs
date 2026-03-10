//! General-purpose three-level config: file < env < CLI.
//!
//! Loads settings from `~/.commonplace/config.json` (or `$COMMONPLACE_CONFIG`).
//! Values from this file serve as defaults that env vars and CLI flags override.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Default config file location relative to home directory.
const CONFIG_DIR: &str = ".commonplace";
const CONFIG_FILE: &str = "config.json";

/// Shared settings across all commonplace binaries.
///
/// Each field is optional — absent means "use hardcoded default."
/// Config file values are overridden by env vars, which are overridden by CLI flags.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommonplaceConfig {
    /// Identity name (e.g., "jes", "sync-client")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Presence extension (e.g., "usr", "exe", "bot")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension: Option<String>,

    /// Server URL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,

    /// MQTT broker URL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mqtt_broker: Option<String>,

    /// Workspace name for MQTT topic namespacing
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,

    /// Identity UUID for presence lifecycle.
    /// When set, the sync agent creates a linked hot presence entry
    /// pointing to this UUID (which should exist in __identities/).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_uuid: Option<String>,
}

impl CommonplaceConfig {
    /// Load config from the default location (`~/.commonplace/config.json`),
    /// or from `$COMMONPLACE_CONFIG` if set.
    ///
    /// Returns `Default` if the file doesn't exist. Errors only on malformed JSON.
    pub fn load() -> Result<Self, ConfigError> {
        let path = Self::config_path();
        match path {
            Some(p) => Self::load_from(&p),
            None => Ok(Self::default()),
        }
    }

    /// Load config from a specific path.
    ///
    /// Returns `Default` if the file doesn't exist.
    pub fn load_from(path: &Path) -> Result<Self, ConfigError> {
        match std::fs::read_to_string(path) {
            Ok(content) => serde_json::from_str(&content)
                .map_err(|e| ConfigError::Parse(path.to_path_buf(), e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(ConfigError::Read(path.to_path_buf(), e.to_string())),
        }
    }

    /// Resolve the config file path.
    ///
    /// Priority: `$COMMONPLACE_CONFIG` > `~/.commonplace/config.json`.
    pub fn config_path() -> Option<PathBuf> {
        if let Ok(path) = std::env::var("COMMONPLACE_CONFIG") {
            return Some(PathBuf::from(path));
        }
        dirs_fallback_home().map(|home| home.join(CONFIG_DIR).join(CONFIG_FILE))
    }

    /// Get a field value, falling back to a default.
    /// Use this to provide defaults to clap.
    pub fn name_or(&self, default: &str) -> String {
        self.name.clone().unwrap_or_else(|| default.to_string())
    }

    pub fn server_or(&self, default: &str) -> String {
        self.server.clone().unwrap_or_else(|| default.to_string())
    }

    pub fn workspace_or(&self, default: &str) -> String {
        self.workspace.clone().unwrap_or_else(|| default.to_string())
    }

    pub fn mqtt_broker_or(&self, default: &str) -> String {
        self.mqtt_broker
            .clone()
            .unwrap_or_else(|| default.to_string())
    }

    pub fn extension_or(&self, default: &str) -> String {
        self.extension
            .clone()
            .unwrap_or_else(|| default.to_string())
    }
}

/// Errors from config loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file {0}: {1}")]
    Read(PathBuf, String),
    #[error("Failed to parse config file {0}: {1}")]
    Parse(PathBuf, String),
}

/// Resolve a single config field with three-level precedence:
/// CLI/env (Some) > config file > hardcoded default.
pub fn resolve_field(cli_value: Option<String>, config_value: Option<&str>, default: &str) -> String {
    cli_value
        .or_else(|| config_value.map(|s| s.to_string()))
        .unwrap_or_else(|| default.to_string())
}

/// Simple home directory fallback (avoids adding the `dirs` crate).
fn dirs_fallback_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_empty() {
        let config = CommonplaceConfig::default();
        assert!(config.name.is_none());
        assert!(config.server.is_none());
        assert!(config.workspace.is_none());
        assert!(config.mqtt_broker.is_none());
        assert!(config.extension.is_none());
    }

    #[test]
    fn test_deserialize_partial_config() {
        let json = r#"{"name": "jes", "extension": "usr"}"#;
        let config: CommonplaceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name.as_deref(), Some("jes"));
        assert_eq!(config.extension.as_deref(), Some("usr"));
        assert!(config.server.is_none());
        assert!(config.workspace.is_none());
    }

    #[test]
    fn test_deserialize_full_config() {
        let json = r#"{
            "name": "jes",
            "extension": "usr",
            "server": "http://myserver:5199",
            "mqtt_broker": "mqtt://mybroker:1883",
            "workspace": "myworkspace"
        }"#;
        let config: CommonplaceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name.as_deref(), Some("jes"));
        assert_eq!(config.extension.as_deref(), Some("usr"));
        assert_eq!(config.server.as_deref(), Some("http://myserver:5199"));
        assert_eq!(config.mqtt_broker.as_deref(), Some("mqtt://mybroker:1883"));
        assert_eq!(config.workspace.as_deref(), Some("myworkspace"));
    }

    #[test]
    fn test_fallback_methods() {
        let config = CommonplaceConfig {
            name: Some("jes".to_string()),
            ..Default::default()
        };
        assert_eq!(config.name_or("default"), "jes");
        assert_eq!(config.server_or("http://localhost:5199"), "http://localhost:5199");
        assert_eq!(config.workspace_or("commonplace"), "commonplace");
    }

    #[test]
    fn test_load_from_nonexistent_returns_default() {
        let config = CommonplaceConfig::load_from(Path::new("/nonexistent/config.json")).unwrap();
        assert!(config.name.is_none());
    }

    #[test]
    fn test_load_from_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"name": "test-user"}"#).unwrap();

        let config = CommonplaceConfig::load_from(&path).unwrap();
        assert_eq!(config.name.as_deref(), Some("test-user"));
    }

    #[test]
    fn test_load_from_malformed_json_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "not json").unwrap();

        let result = CommonplaceConfig::load_from(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("parse"));
    }

    #[test]
    fn test_serialize_skips_none_fields() {
        let config = CommonplaceConfig {
            name: Some("jes".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("name").is_some());
        assert!(parsed.get("server").is_none());
        assert!(parsed.get("workspace").is_none());
    }

    #[test]
    fn test_config_path_from_env() {
        // Save and restore
        let original = std::env::var("COMMONPLACE_CONFIG").ok();
        std::env::set_var("COMMONPLACE_CONFIG", "/custom/path/config.json");
        let path = CommonplaceConfig::config_path();
        assert_eq!(path, Some(PathBuf::from("/custom/path/config.json")));
        match original {
            Some(v) => std::env::set_var("COMMONPLACE_CONFIG", v),
            None => std::env::remove_var("COMMONPLACE_CONFIG"),
        }
    }

    #[test]
    fn test_config_with_identity_uuid() {
        let json = r#"{"identity_uuid": "abc-123-def"}"#;
        let config: CommonplaceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.identity_uuid.as_deref(), Some("abc-123-def"));
    }

    #[test]
    fn test_resolve_field_precedence() {
        let config = CommonplaceConfig {
            server: Some("http://config-server:5199".to_string()),
            ..Default::default()
        };
        // CLI/env wins over config
        assert_eq!(
            resolve_field(Some("http://cli-server:5199".to_string()), config.server.as_deref(), "http://default:5199"),
            "http://cli-server:5199"
        );
        // Config wins over hardcoded default
        assert_eq!(
            resolve_field(None, config.server.as_deref(), "http://default:5199"),
            "http://config-server:5199"
        );
        // Hardcoded default when both are None
        let empty_config = CommonplaceConfig::default();
        assert_eq!(
            resolve_field(None, empty_config.server.as_deref(), "http://default:5199"),
            "http://default:5199"
        );
    }

    #[test]
    fn test_empty_json_object_is_valid() {
        let config: CommonplaceConfig = serde_json::from_str("{}").unwrap();
        assert!(config.name.is_none());
    }
}
