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

    /// Directory containing static HTML/JS/CSS for the document viewer
    /// Enables /view/docs/:id and /view/files/*path routes
    #[clap(long, value_name = "DIR")]
    pub static_dir: Option<PathBuf>,
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

    /// HTTP server URL for recursive process discovery
    #[clap(long, value_name = "URL", default_value = "http://localhost:3000")]
    pub server: String,
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

/// CLI arguments for commonplace-uuid (resolve path to UUID)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-uuid")]
#[clap(about = "Resolve a synced file path to its UUID", long_about = None)]
pub struct UuidArgs {
    /// File path to resolve (relative or absolute)
    pub path: PathBuf,

    /// Output in JSON format
    #[clap(long)]
    pub json: bool,
}

/// CLI arguments for commonplace-ps (list orchestrator processes)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-ps")]
#[clap(about = "List processes managed by the commonplace orchestrator", long_about = None)]
pub struct PsArgs {
    /// Output in JSON format
    #[clap(long)]
    pub json: bool,
}

/// CLI arguments for commonplace-replay (view/replay file edits)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-replay")]
#[clap(about = "View or replay edit history for a synced file", long_about = None)]
pub struct ReplayArgs {
    /// File path to view history for (relative or absolute)
    pub path: PathBuf,

    /// List commits only (don't show content)
    #[clap(long)]
    pub list: bool,

    /// Show content at a specific commit
    #[clap(long)]
    pub at: Option<String>,

    /// Server URL
    #[clap(long, default_value = "http://localhost:3000")]
    pub server: String,

    /// Output in JSON format
    #[clap(long)]
    pub json: bool,
}

/// CLI arguments for commonplace-log (git-log style commit history)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-log")]
#[clap(about = "Show commit history for a synced file (like git log)", long_about = None)]
pub struct LogArgs {
    /// File path to view history for (relative or absolute)
    pub path: PathBuf,

    /// Show one commit per line (compact format)
    #[clap(long)]
    pub oneline: bool,

    /// Show ASCII graph of commit ancestry (default: on)
    #[clap(long, default_value = "true", action = clap::ArgAction::SetTrue)]
    pub graph: bool,

    /// Disable graph output
    #[clap(long)]
    pub no_graph: bool,

    /// Show ref-like decorations (HEAD marker) (default: on)
    #[clap(long, default_value = "true", action = clap::ArgAction::SetTrue)]
    pub decorate: bool,

    /// Disable decorations
    #[clap(long)]
    pub no_decorate: bool,

    /// Show change statistics (lines added/removed)
    #[clap(long)]
    pub stat: bool,

    /// Show diff/patch for each commit (default: on)
    #[clap(short = 'u', visible_short_alias = 'p', long, default_value = "true", action = clap::ArgAction::SetTrue)]
    pub patch: bool,

    /// Suppress diff output (summary only)
    #[clap(long)]
    pub no_patch: bool,

    /// Limit number of commits shown
    #[clap(short = 'n', long = "max-count")]
    pub max_count: Option<usize>,

    /// Show commits after this date (YYYY-MM-DD or timestamp)
    #[clap(long)]
    pub since: Option<String>,

    /// Show commits before this date (YYYY-MM-DD or timestamp)
    #[clap(long)]
    pub until: Option<String>,

    /// Watch for new commits and output them as they arrive
    #[clap(short = 'f', long)]
    pub follow: bool,

    /// Server URL
    #[clap(long, default_value = "http://localhost:3000")]
    pub server: String,

    /// Output in JSON format
    #[clap(long)]
    pub json: bool,

    /// Show raw Yjs update bytes (ASCII-ish representation)
    #[clap(long)]
    pub show_yjs: bool,
}

/// CLI arguments for commonplace-show (git-show style content display)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-show")]
#[clap(about = "Show content at a specific commit (like git show)", long_about = None)]
pub struct ShowArgs {
    /// File path to view (relative or absolute)
    pub path: PathBuf,

    /// Commit ID to show (default: HEAD)
    pub commit: Option<String>,

    /// Show change statistics for the commit
    #[clap(long)]
    pub stat: bool,

    /// Server URL
    #[clap(long, default_value = "http://localhost:3000")]
    pub server: String,

    /// Output in JSON format
    #[clap(long)]
    pub json: bool,
}

/// CLI arguments for commonplace-signal (signal orchestrator process)
#[derive(Parser, Debug)]
#[clap(name = "commonplace-signal")]
#[clap(about = "Send a signal to an orchestrator-managed process", long_about = None)]
pub struct SignalArgs {
    /// Process name to signal
    #[clap(long, short)]
    pub name: String,

    /// Filter by document path (optional)
    #[clap(long, short)]
    pub path: Option<String>,

    /// Signal to send (default: TERM)
    #[clap(long, short, default_value = "TERM")]
    pub signal: String,

    /// Output in JSON format
    #[clap(long)]
    pub json: bool,
}
