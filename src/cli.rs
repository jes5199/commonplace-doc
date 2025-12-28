use clap::Parser;
use std::path::PathBuf;

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

    /// Node IDs for router documents (repeatable)
    #[clap(long = "router", value_name = "NODE_ID")]
    pub routers: Vec<String>,

    /// MQTT broker URL (e.g., mqtt://localhost:1883)
    #[clap(long, value_name = "URL")]
    pub mqtt_broker: Option<String>,

    /// MQTT client ID (defaults to generated UUID)
    #[clap(long, value_name = "ID")]
    pub mqtt_client_id: Option<String>,
}
