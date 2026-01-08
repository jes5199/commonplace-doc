//! commonplace-mcp: MCP server for firing commands to commonplace paths
//!
//! This MCP server exposes a `fire_command` tool that sends commands
//! to document paths via MQTT, bridging LLM tool-use with commonplace.

use commonplace_doc::mqtt::{CommandMessage, MqttConfig, Topic};
use rmcp::{
    handler::server::ServerHandler,
    model::{
        CallToolResult, Content, ErrorCode, ErrorData, Implementation, InitializeRequestParam,
        InitializeResult, ListToolsResult, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
    },
    schemars, service,
    transport::io::stdio,
    ServiceExt,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::time::Duration;

/// Parameters for the fire_command tool
#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
struct FireCommandParams {
    /// The document path (e.g., "examples/counter.json")
    path: String,
    /// The command verb (e.g., "increment", "reset")
    verb: String,
    /// Optional JSON payload for the command
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

/// The MCP server for commonplace commands
#[derive(Clone)]
struct CommonplaceMcp {
    mqtt_broker: String,
    workspace: String,
}

impl CommonplaceMcp {
    fn new(mqtt_broker: String, workspace: String) -> Self {
        Self {
            mqtt_broker,
            workspace,
        }
    }

    async fn fire_command(&self, params: FireCommandParams) -> Result<String, String> {
        let payload = params.payload.unwrap_or(serde_json::json!({}));

        let message = CommandMessage {
            payload,
            source: Some("commonplace-mcp".to_string()),
        };

        let topic = Topic::commands(&self.workspace, &params.path, &params.verb);
        let topic_str = topic.to_topic_string();

        let config = MqttConfig {
            broker_url: self.mqtt_broker.clone(),
            client_id: format!("commonplace-mcp-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };

        // Parse broker URL
        let broker_url = &config.broker_url;
        let stripped = broker_url
            .strip_prefix("mqtt://")
            .or_else(|| broker_url.strip_prefix("tcp://"))
            .unwrap_or(broker_url);
        let parts: Vec<&str> = stripped.split(':').collect();
        let host = parts.first().unwrap_or(&"localhost").to_string();
        let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(1883);

        let mut options = MqttOptions::new(&config.client_id, host, port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs));
        options.set_clean_session(config.clean_session);

        let (client, mut event_loop) = AsyncClient::new(options, 256);

        // Wait for ConnAck with timeout
        let connect_timeout = Duration::from_secs(5);
        let connect_deadline = tokio::time::Instant::now() + connect_timeout;
        let mut connected = false;

        while tokio::time::Instant::now() < connect_deadline {
            match tokio::time::timeout(Duration::from_millis(100), event_loop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(ack)))) => {
                    if ack.code == rumqttc::ConnectReturnCode::Success {
                        connected = true;
                        break;
                    } else {
                        return Err(format!("MQTT connection rejected: {:?}", ack.code));
                    }
                }
                Ok(Ok(_)) => continue, // Other events, keep polling
                Ok(Err(e)) => return Err(format!("MQTT connection error: {}", e)),
                Err(_) => continue, // Timeout, keep trying
            }
        }

        if !connected {
            return Err("MQTT connection timeout".to_string());
        }

        // Publish the message
        let payload_bytes = serde_json::to_vec(&message)
            .map_err(|e| format!("JSON serialization failed: {}", e))?;

        client
            .publish(&topic_str, QoS::AtLeastOnce, false, payload_bytes)
            .await
            .map_err(|e| format!("MQTT publish failed: {}", e))?;

        // Wait for PubAck with timeout
        let publish_timeout = Duration::from_secs(5);
        let publish_deadline = tokio::time::Instant::now() + publish_timeout;
        let mut acked = false;

        while tokio::time::Instant::now() < publish_deadline {
            match tokio::time::timeout(Duration::from_millis(100), event_loop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => {
                    acked = true;
                    break;
                }
                Ok(Ok(_)) => continue, // Other events, keep polling
                Ok(Err(e)) => return Err(format!("MQTT error while waiting for ack: {}", e)),
                Err(_) => continue, // Timeout, keep trying
            }
        }

        if !acked {
            return Err("MQTT publish ack timeout".to_string());
        }

        // Disconnect gracefully
        let _ = client.disconnect().await;

        Ok(format!("Sent {} to {}", params.verb, params.path))
    }
}

impl ServerHandler for CommonplaceMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities {
                tools: Some(Default::default()),
                ..Default::default()
            },
            server_info: Implementation {
                name: "commonplace-mcp".into(),
                version: "0.1.0".into(),
                title: None,
                website_url: None,
                icons: None,
            },
            instructions: Some(
                "Use the fire_command tool to send commands to commonplace document paths.".into(),
            ),
        }
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParam,
        _context: service::RequestContext<service::RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        Ok(self.get_info())
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParam>,
        _context: service::RequestContext<service::RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema = schemars::schema_for!(FireCommandParams);
        let schema_value = serde_json::to_value(schema).unwrap_or_default();

        Ok(ListToolsResult {
            tools: vec![Tool {
                name: "fire_command".into(),
                description: Some("Send a command to a commonplace document path via MQTT".into()),
                input_schema: serde_json::from_value(schema_value).unwrap_or_default(),
                annotations: None,
                icons: None,
                meta: None,
                output_schema: None,
                title: None,
            }],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParam,
        _context: service::RequestContext<service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        if request.name != "fire_command" {
            return Err(ErrorData {
                code: ErrorCode::METHOD_NOT_FOUND,
                message: Cow::Owned(format!("Unknown tool: {}", request.name)),
                data: None,
            });
        }

        let args = serde_json::Value::Object(request.arguments.unwrap_or_default());
        let params: FireCommandParams = serde_json::from_value(args).map_err(|e| ErrorData {
            code: ErrorCode::INVALID_PARAMS,
            message: Cow::Owned(format!("Invalid parameters: {}", e)),
            data: None,
        })?;

        match self.fire_command(params).await {
            Ok(result) => Ok(CallToolResult::success(vec![Content::text(result)])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e)])),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mqtt_broker =
        std::env::var("MQTT_BROKER").unwrap_or_else(|_| "mqtt://localhost:1883".to_string());
    let workspace = std::env::var("MQTT_WORKSPACE").unwrap_or_else(|_| "commonplace".to_string());

    let server = CommonplaceMcp::new(mqtt_broker, workspace);

    // Start the MCP server on stdio
    let transport = stdio();
    server.serve(transport).await?;

    Ok(())
}
