//! commonplace-checkout: Switch sync target
//!
//! Sends a re-root MQTT command to the sync agent, causing it to
//! switch from the current root UUID to the target's UUID.
//!
//! Accepts multiple target formats:
//! - Path: `workspace/main/bartleby` (resolved via HTTP schema walk)
//! - UUID: `abc-def-123` (used directly)
//! - Docref: `path:uuid@cid` (UUID extracted)
//!
//! Target must resolve to a directory (schema document), not a file.

use clap::Parser;
use commonplace_doc::cli::CheckoutArgs;
use commonplace_doc::mqtt::{MqttClient, MqttConfig};
use commonplace_doc::sync::client::{discover_fs_root, fetch_head, resolve_path_to_uuid_http};
use commonplace_doc::{DEFAULT_MQTT_BROKER_URL, DEFAULT_SERVER_URL, DEFAULT_WORKSPACE};
use commonplace_types::config::{CommonplaceConfig, resolve_field};
use rumqttc::QoS;
use std::sync::Arc;
use std::time::Duration;

/// Detect what kind of target the user provided and resolve it to a UUID.
enum TargetKind {
    /// Raw UUID (contains hyphens, looks like UUID)
    Uuid(String),
    /// Docref format: path:uuid or path:uuid@cid
    Docref { uuid: String },
    /// Path to resolve via schema walk
    Path(String),
}

fn classify_target(target: &str) -> TargetKind {
    // Docref format: contains ':' separator (path:uuid or path:uuid@cid)
    if let Some(after_colon) = target.split_once(':').map(|(_, rest)| rest) {
        // Extract UUID (strip @cid if present)
        let uuid = after_colon.split_once('@').map_or(after_colon, |(u, _)| u);
        return TargetKind::Docref { uuid: uuid.to_string() };
    }

    // UUID pattern: 8-4-4-4-12 hex with hyphens, or short UUID (8+ hex chars with hyphens)
    let looks_like_uuid = target.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
        && target.contains('-')
        && target.len() >= 8;

    if looks_like_uuid {
        return TargetKind::Uuid(target.to_string());
    }

    // Everything else is a path
    TargetKind::Path(target.to_string())
}

/// Validate that a UUID points to a directory (schema document), not a file.
async fn validate_directory(
    http_client: &reqwest::Client,
    server: &str,
    uuid: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let head = fetch_head(http_client, server, uuid, false)
        .await
        .map_err(|e| format!("Cannot fetch target {}: {}", &uuid[..8.min(uuid.len())], e))?
        .ok_or_else(|| format!("Target {} has no content", &uuid[..8.min(uuid.len())]))?;

    // A directory schema has root.entries; a plain file won't parse as one
    let parsed: serde_json::Value = serde_json::from_str(&head.content)
        .map_err(|_| format!("Target {} is a file, not a directory", &uuid[..8.min(uuid.len())]))?;

    // Check for schema structure: must have root.entries (or root.type == "dir")
    let is_dir = parsed
        .get("root")
        .and_then(|r| {
            // Either has entries map, or type == "dir"
            if r.get("entries").is_some() {
                Some(true)
            } else {
                r.get("type").and_then(|t| t.as_str()).map(|t| t == "dir")
            }
        })
        .unwrap_or(false);

    if !is_dir {
        // Also accept empty schema {} (newly created directory)
        let content = head.content.trim();
        if content != "{}" {
            return Err(format!(
                "Target {} is not a directory. Only directories can be checked out.",
                &uuid[..8.min(uuid.len())]
            ).into());
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CheckoutArgs::parse();

    let config = CommonplaceConfig::load().unwrap_or_default();
    let server = resolve_field(args.server, config.server.as_deref(), DEFAULT_SERVER_URL);
    let mqtt_broker = resolve_field(args.mqtt_broker, config.mqtt_broker.as_deref(), DEFAULT_MQTT_BROKER_URL);
    let workspace = resolve_field(args.workspace, config.workspace.as_deref(), DEFAULT_WORKSPACE);

    let http_client = reqwest::Client::new();

    // Resolve target to UUID
    let (target_uuid, display_name) = match classify_target(&args.target) {
        TargetKind::Uuid(uuid) => {
            println!("Checking out UUID {}...", &uuid[..8.min(uuid.len())]);
            (uuid.clone(), uuid)
        }
        TargetKind::Docref { uuid } => {
            println!("Checking out docref (UUID: {})...", &uuid[..8.min(uuid.len())]);
            (uuid.clone(), args.target.clone())
        }
        TargetKind::Path(path) => {
            let fs_root_id = discover_fs_root(&http_client, &server)
                .await
                .map_err(|e| format!("Cannot discover fs-root: {}. Is the server running?", e))?;

            let uuid = resolve_path_to_uuid_http(&http_client, &server, &fs_root_id, &path)
                .await
                .map_err(|e| format!("Cannot resolve path '{}': {}", path, e))?;

            println!(
                "Checking out '{}' (UUID: {})...",
                path,
                &uuid[..8.min(uuid.len())]
            );
            (uuid, path)
        }
    };

    // Validate it's a directory
    validate_directory(&http_client, &server, &target_uuid).await?;

    let sync_name = &args.sync_name;

    // Connect to MQTT and send re-root command
    let mqtt_config = MqttConfig {
        broker_url: mqtt_broker.clone(),
        client_id: format!("commonplace-checkout-{}", uuid::Uuid::new_v4()),
        workspace: workspace.clone(),
        ..Default::default()
    };

    let client = Arc::new(MqttClient::connect(mqtt_config).await?);
    let client_for_loop = client.clone();
    let loop_handle = tokio::spawn(async move {
        let _ = client_for_loop.run_event_loop().await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish re-root command
    let topic = format!(
        "{}/commands/__sync/{}/re-root",
        workspace, sync_name
    );
    let payload = serde_json::json!({
        "root_uuid": target_uuid,
        "target": display_name,
    });
    let payload_bytes = serde_json::to_vec(&payload)?;

    client
        .publish(&topic, &payload_bytes, QoS::AtLeastOnce)
        .await?;

    // Wait for PUBACK delivery
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!(
        "Sent re-root to sync agent '{}'. Now syncing '{}'.",
        sync_name, display_name
    );

    loop_handle.abort();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_uuid() {
        match classify_target("abc-def-123") {
            TargetKind::Uuid(u) => assert_eq!(u, "abc-def-123"),
            _ => panic!("Expected UUID"),
        }

        match classify_target("0847bb0b-d991-43e3-a960-edc18fc01965") {
            TargetKind::Uuid(u) => assert_eq!(u, "0847bb0b-d991-43e3-a960-edc18fc01965"),
            _ => panic!("Expected UUID"),
        }
    }

    #[test]
    fn test_classify_docref() {
        match classify_target("workspace/sync.exe:abc-def-123") {
            TargetKind::Docref { uuid } => assert_eq!(uuid, "abc-def-123"),
            _ => panic!("Expected Docref"),
        }

        match classify_target("workspace/sync.exe:abc-def-123@commit-id") {
            TargetKind::Docref { uuid } => assert_eq!(uuid, "abc-def-123"),
            _ => panic!("Expected Docref"),
        }
    }

    #[test]
    fn test_classify_path() {
        match classify_target("workspace/main/bartleby") {
            TargetKind::Path(p) => assert_eq!(p, "workspace/main/bartleby"),
            _ => panic!("Expected Path"),
        }

        match classify_target("main") {
            TargetKind::Path(p) => assert_eq!(p, "main"),
            _ => panic!("Expected Path"),
        }
    }
}
