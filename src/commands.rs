//! HTTP to MQTT command bridge.
//!
//! This module provides `POST /commands/*path` endpoints that publish
//! commands to MQTT topics for document-specific actions.
//!
//! Path format: `/commands/{document_path}/{verb}`
//! - The last segment is the verb
//! - Everything before is the document path
//! - Paths starting with `__` are reserved for system commands

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::mqtt::{CommandMessage, MqttClient, Topic};

/// State for command endpoints.
#[derive(Clone)]
pub struct CommandsState {
    pub mqtt_client: Arc<MqttClient>,
    pub workspace: String,
}

/// Request body for commands (optional).
#[derive(Debug, Deserialize, Default)]
pub struct CommandRequest {
    /// Command payload (arbitrary JSON)
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Optional source identifier
    #[serde(default)]
    pub source: Option<String>,
}

/// Response for successful command.
#[derive(Debug, Serialize)]
pub struct CommandResponse {
    pub ok: bool,
    pub path: String,
    pub verb: String,
}

/// Errors from command operations.
#[derive(Debug)]
pub enum CommandError {
    /// Path is too short (needs at least path + verb)
    InvalidPath(String),
    /// Path starts with reserved prefix
    ReservedPath(String),
    /// MQTT publish failed
    PublishFailed(String),
}

impl IntoResponse for CommandError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            CommandError::InvalidPath(msg) => (StatusCode::BAD_REQUEST, msg),
            CommandError::ReservedPath(msg) => (StatusCode::FORBIDDEN, msg),
            CommandError::PublishFailed(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, message).into_response()
    }
}

/// Create the commands router.
///
/// Returns None if MQTT is not configured.
pub fn router(mqtt_client: Option<Arc<MqttClient>>, workspace: Option<String>) -> Option<Router> {
    let (mqtt_client, workspace) = match (mqtt_client, workspace) {
        (Some(client), Some(ws)) => (client, ws),
        _ => return None,
    };

    let state = CommandsState {
        mqtt_client,
        workspace,
    };

    Some(
        Router::new()
            .route("/commands/*path", post(handle_command))
            .with_state(state),
    )
}

/// Parse a command path into (document_path, verb).
///
/// The last segment is the verb, everything before is the document path.
/// Example: "foo/bar.json/increment" -> ("foo/bar.json", "increment")
fn parse_command_path(path: &str) -> Result<(String, String), CommandError> {
    // Remove leading slash if present
    let path = path.strip_prefix('/').unwrap_or(path);

    // Split on last slash
    match path.rsplit_once('/') {
        Some((doc_path, verb)) if !doc_path.is_empty() && !verb.is_empty() => {
            // Check for reserved prefix
            if doc_path.starts_with("__") {
                return Err(CommandError::ReservedPath(format!(
                    "Paths starting with '__' are reserved: {}",
                    doc_path
                )));
            }
            Ok((doc_path.to_string(), verb.to_string()))
        }
        Some((_, verb)) if !verb.is_empty() => {
            // Single segment - could be just a verb with no path, or a path with no verb
            // We require at least path/verb, so this is invalid
            Err(CommandError::InvalidPath(format!(
                "Path must include document path and verb: got '{}'",
                path
            )))
        }
        _ => Err(CommandError::InvalidPath(format!(
            "Invalid command path format: '{}'. Expected: {{path}}/{{verb}}",
            path
        ))),
    }
}

/// POST /commands/*path - Send a command to a document.
async fn handle_command(
    State(state): State<CommandsState>,
    Path(path): Path<String>,
    body: Option<Json<CommandRequest>>,
) -> Result<Json<CommandResponse>, CommandError> {
    let (doc_path, verb) = parse_command_path(&path)?;

    // Build the command message
    let request = body.map(|b| b.0).unwrap_or_default();
    let message = CommandMessage {
        payload: request.payload,
        source: request.source,
    };

    // Build the MQTT topic
    let topic = Topic::commands(&state.workspace, &doc_path, &verb);
    let topic_str = topic.to_topic_string();

    // Serialize and publish
    let payload_bytes = serde_json::to_vec(&message)
        .map_err(|e| CommandError::PublishFailed(format!("Failed to serialize command: {}", e)))?;

    state
        .mqtt_client
        .publish(&topic_str, &payload_bytes, QoS::AtLeastOnce)
        .await
        .map_err(|e| CommandError::PublishFailed(format!("MQTT publish failed: {}", e)))?;

    tracing::info!("Command sent: {} {} (topic: {})", doc_path, verb, topic_str);

    Ok(Json(CommandResponse {
        ok: true,
        path: doc_path,
        verb,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_path() {
        // Valid paths
        let (path, verb) = parse_command_path("foo.json/increment").unwrap();
        assert_eq!(path, "foo.json");
        assert_eq!(verb, "increment");

        let (path, verb) = parse_command_path("a/b/c.json/reset").unwrap();
        assert_eq!(path, "a/b/c.json");
        assert_eq!(verb, "reset");

        let (path, verb) = parse_command_path("/foo/bar/baz").unwrap();
        assert_eq!(path, "foo/bar");
        assert_eq!(verb, "baz");
    }

    #[test]
    fn test_parse_command_path_invalid() {
        // Single segment - no verb
        assert!(parse_command_path("foo").is_err());

        // Empty
        assert!(parse_command_path("").is_err());

        // Just a slash
        assert!(parse_command_path("/").is_err());
    }

    #[test]
    fn test_parse_command_path_reserved() {
        // Reserved prefix
        assert!(matches!(
            parse_command_path("__system/create-document"),
            Err(CommandError::ReservedPath(_))
        ));

        assert!(matches!(
            parse_command_path("__foo/bar"),
            Err(CommandError::ReservedPath(_))
        ));
    }
}
