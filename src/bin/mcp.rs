//! commonplace-mcp: MCP server for firing commands to commonplace paths
//!
//! This MCP server exposes a `fire_command` tool that sends commands
//! to document paths via MQTT, bridging LLM tool-use with commonplace.

use commonplace_doc::mqtt::{CommandMessage, MqttClient, MqttConfig, Topic};
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
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;
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
}

impl CommonplaceMcp {
    fn new(mqtt_broker: String) -> Self {
        Self { mqtt_broker }
    }

    async fn fire_command(&self, params: FireCommandParams) -> Result<String, String> {
        let payload = params.payload.unwrap_or(serde_json::json!({}));

        let message = CommandMessage {
            payload,
            source: Some("commonplace-mcp".to_string()),
        };

        let topic = Topic::commands(&params.path, &params.verb);
        let topic_str = topic.to_topic_string();

        let config = MqttConfig {
            broker_url: self.mqtt_broker.clone(),
            client_id: format!("commonplace-mcp-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };

        let client = MqttClient::connect(config)
            .await
            .map_err(|e| format!("MQTT connection failed: {}", e))?;

        let client = Arc::new(client);
        let client_clone = client.clone();

        let loop_handle = tokio::spawn(async move {
            let _ =
                tokio::time::timeout(Duration::from_secs(2), client_clone.run_event_loop()).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        let payload_bytes = serde_json::to_vec(&message)
            .map_err(|e| format!("JSON serialization failed: {}", e))?;

        client
            .publish(&topic_str, &payload_bytes, QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("MQTT publish failed: {}", e))?;

        tokio::time::sleep(Duration::from_millis(500)).await;
        loop_handle.abort();

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

    let server = CommonplaceMcp::new(mqtt_broker);

    // Start the MCP server on stdio
    let transport = stdio();
    server.serve(transport).await?;

    Ok(())
}
