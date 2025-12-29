use clap::Parser;
use std::path::PathBuf;

/// CLI arguments for the combined server (legacy, for backwards compatibility)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-doc")]
#[clap(about = "A document server with Yjs commit history", long_about = None)]
pub struct Args {
    /// Path to the redb database file
    #[clap(short, long, value_name = "FILE")]
    pub database: Option<PathBuf>,

    /// Port to listen on
    #[clap(short, long, default_value = "3000")]
    pub port: u16,

    /// Host to bind to
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Node ID for filesystem root document
    #[clap(long, value_name = "NODE_ID")]
    pub fs_root: Option<String>,

    /// MQTT broker URL (e.g., mqtt://localhost:1883)
    #[clap(long, value_name = "URL")]
    pub mqtt_broker: Option<String>,

    /// MQTT client ID (defaults to generated UUID)
    #[clap(long, value_name = "ID")]
    pub mqtt_client_id: Option<String>,

    /// Document paths to subscribe via MQTT (repeatable, requires --mqtt-broker)
    /// Paths must include file extensions (e.g., notes/todo.txt, config.json)
    #[clap(long = "mqtt-subscribe", value_name = "PATH")]
    pub mqtt_subscribe: Vec<String>,
}

/// CLI arguments for commonplace-store (document storage, no HTTP)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-store")]
#[clap(about = "Document store with MQTT transport (no HTTP)", long_about = None)]
pub struct StoreArgs {
    /// Path to the redb database file (required)
    #[clap(short, long, value_name = "FILE")]
    pub database: PathBuf,

    /// MQTT broker URL (e.g., mqtt://localhost:1883) (required)
    #[clap(long, value_name = "URL")]
    pub mqtt_broker: String,

    /// MQTT client ID (defaults to "commonplace-store")
    #[clap(long, value_name = "ID", default_value = "commonplace-store")]
    pub mqtt_client_id: String,

    /// Node ID for filesystem root document (required - determines MQTT subscriptions)
    #[clap(long, value_name = "NODE_ID")]
    pub fs_root: String,
}

/// CLI arguments for commonplace-http (HTTP gateway via MQTT)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-http")]
#[clap(about = "HTTP gateway that translates requests to MQTT", long_about = None)]
pub struct HttpArgs {
    /// Port to listen on
    #[clap(short, long, default_value = "3000")]
    pub port: u16,

    /// Host to bind to
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    /// MQTT broker URL (e.g., mqtt://localhost:1883) (required)
    #[clap(long, value_name = "URL")]
    pub mqtt_broker: String,

    /// MQTT client ID (defaults to "commonplace-http")
    #[clap(long, value_name = "ID", default_value = "commonplace-http")]
    pub mqtt_client_id: String,
}

/// CLI arguments for the orchestrator binary
#[derive(Parser, Debug)]
#[clap(name = "commonplace-orchestrator")]
#[clap(about = "Process supervisor for commonplace services", long_about = None)]
pub struct OrchestratorArgs {
    /// Path to config file
    #[clap(long, default_value = "commonplace.json")]
    pub config: PathBuf,

    /// Override MQTT broker address
    #[clap(long, value_name = "URL")]
    pub mqtt_broker: Option<String>,

    /// Disable a specific process
    #[clap(long, value_name = "NAME")]
    pub disable: Vec<String>,

    /// Run only a specific process (skip dependencies)
    #[clap(long, value_name = "NAME")]
    pub only: Option<String>,
}
