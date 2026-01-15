//! Command subscription for sandbox-synced processes.
//!
//! This module provides functionality for processes to receive commands via MQTT.
//! When a process is running in sandbox mode, it subscribes to commands on its path
//! and writes them to `__commands.jsonl` for the process to consume.

use crate::events::recv_broadcast;
use crate::mqtt::{MqttClient, Topic};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, warn};

/// A command received via MQTT, serialized to __commands.jsonl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEntry {
    /// The command verb (extracted from topic)
    pub verb: String,
    /// The command payload (JSON from message body)
    pub payload: serde_json::Value,
    /// Source identifier (e.g., "mqtt" or client ID if available)
    pub source: String,
    /// Unix timestamp in milliseconds when the command was received
    pub timestamp: u64,
}

/// Spawn a task that subscribes to commands for a path and writes them to __commands.jsonl.
///
/// # Arguments
/// * `mqtt_client` - The MQTT client to use for subscribing
/// * `workspace` - The workspace name
/// * `path` - The document path to subscribe to commands for
/// * `directory` - The local directory where __commands.jsonl will be written
///
/// # Returns
/// A JoinHandle for the spawned task
pub fn spawn_command_listener(
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    path: String,
    directory: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(command_listener_task(
        mqtt_client,
        workspace,
        path,
        directory,
    ))
}

/// The command listener task that subscribes to MQTT commands and writes to __commands.jsonl.
async fn command_listener_task(
    mqtt_client: Arc<MqttClient>,
    workspace: String,
    path: String,
    directory: std::path::PathBuf,
) {
    // Subscribe to all commands for this path: {workspace}/commands/{path}/#
    let topic_pattern = Topic::commands_wildcard(&workspace, &path);

    info!(
        "Subscribing to commands for path '{}' at topic: {}",
        path, topic_pattern
    );

    if let Err(e) = mqtt_client
        .subscribe(&topic_pattern, QoS::AtLeastOnce)
        .await
    {
        error!("Failed to subscribe to commands topic: {}", e);
        return;
    }

    // Get a receiver for incoming messages
    let mut message_rx = mqtt_client.subscribe_messages();

    // The file to append commands to
    let commands_file = directory.join("__commands.jsonl");

    // Topic prefix for matching (e.g., "workspace/commands/path/")
    let topic_prefix = format!("{}/commands/{}/", workspace, path);

    info!("Command listener started, writing to {:?}", commands_file);

    // Process incoming MQTT messages
    let context = format!("commands listener for {}", path);
    while let Some(msg) = recv_broadcast(&mut message_rx, &context).await {
        // Check if this message is for our commands topic
        if !msg.topic.starts_with(&topic_prefix) {
            continue;
        }

        // Extract the verb from the topic (everything after the prefix)
        let verb = msg.topic[topic_prefix.len()..].to_string();
        if verb.is_empty() {
            debug!("Ignoring command with empty verb");
            continue;
        }

        // Parse the payload as JSON (or wrap in null if not valid JSON)
        let payload: serde_json::Value = match serde_json::from_slice(&msg.payload) {
            Ok(v) => v,
            Err(_) => {
                // If not valid JSON, treat it as a string
                match String::from_utf8(msg.payload.clone()) {
                    Ok(s) => serde_json::Value::String(s),
                    Err(_) => serde_json::Value::Null,
                }
            }
        };

        // Create the command entry
        let entry = CommandEntry {
            verb: verb.clone(),
            payload,
            source: "mqtt".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };

        // Serialize and append to file
        match append_command(&commands_file, &entry).await {
            Ok(()) => {
                debug!("Wrote command '{}' to {:?}", verb, commands_file);
            }
            Err(e) => {
                warn!("Failed to write command '{}' to file: {}", verb, e);
            }
        }
    }

    info!("Command listener for '{}' shutting down", path);
}

/// Append a command entry to the __commands.jsonl file.
async fn append_command(file_path: &Path, entry: &CommandEntry) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
        .await?;

    let mut json = serde_json::to_string(entry)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    json.push('\n');

    file.write_all(json.as_bytes()).await?;
    file.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_entry_serialization() {
        let entry = CommandEntry {
            verb: "clear".to_string(),
            payload: serde_json::json!({"lines": 10}),
            source: "mqtt".to_string(),
            timestamp: 1234567890000,
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"verb\":\"clear\""));
        assert!(json.contains("\"lines\":10"));
        assert!(json.contains("\"source\":\"mqtt\""));
        assert!(json.contains("\"timestamp\":1234567890000"));
    }

    #[test]
    fn test_command_entry_deserialization() {
        let json = r#"{"verb":"restart","payload":null,"source":"http","timestamp":9999999999999}"#;
        let entry: CommandEntry = serde_json::from_str(json).unwrap();

        assert_eq!(entry.verb, "restart");
        assert_eq!(entry.payload, serde_json::Value::Null);
        assert_eq!(entry.source, "http");
        assert_eq!(entry.timestamp, 9999999999999);
    }
}
