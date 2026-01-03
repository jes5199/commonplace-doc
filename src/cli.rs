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

    /// Watch a .processes.json document for dynamic process management
    /// When specified, processes are started/stopped based on document changes
    #[clap(long, value_name = "DOC_PATH")]
    pub watch_processes: Option<String>,

    /// HTTP server URL for document fetching (used with --watch-processes)
    #[clap(long, value_name = "URL", default_value = "http://localhost:3000")]
    pub server: String,

    /// Use path-based endpoints instead of document IDs (used with --watch-processes)
    #[clap(long)]
    pub use_paths: bool,

    /// Recursively discover all processes.json files in the filesystem tree
    /// Uses --server's fs-root as the starting point (default: true)
    /// Use --no-recursive to use static config file instead
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub recursive: bool,
}

/// CLI arguments for commonplace-cmd (send commands to paths)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-cmd")]
#[clap(about = "Send commands to commonplace document paths via MQTT", long_about = None)]
pub struct CmdArgs {
    /// Document path (e.g., "examples/counter.json")
    pub path: String,

    /// Command verb (e.g., "increment", "reset")
    pub verb: String,

    /// JSON payload (optional, defaults to {})
    #[clap(short, long, default_value = "{}")]
    pub payload: String,

    /// MQTT broker URL
    #[clap(long, default_value = "mqtt://localhost:1883")]
    pub mqtt_broker: String,

    /// Source identifier for the command
    #[clap(long, default_value = "commonplace-cmd")]
    pub source: String,
}

/// CLI arguments for commonplace-link (create document aliases)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-link")]
#[clap(about = "Create a link to a commonplace document (like ln)", long_about = None)]
pub struct LinkArgs {
    /// Source file path (must exist in schema)
    pub source: PathBuf,

    /// Target link path (must not exist)
    pub target: PathBuf,

    /// Server URL to push schema changes to (optional, defaults to http://localhost:3000)
    #[clap(short, long, default_value = "http://localhost:3000")]
    pub server: String,
}
