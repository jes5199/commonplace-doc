//! commonplace-checkout: Switch active branch
//!
//! Updates the workspace's active branch by sending an MQTT command
//! to the sync agent to switch subscription targets.

use clap::Parser;
use commonplace_doc::cli::CheckoutArgs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CheckoutArgs::parse();

    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    let schema_path = directory.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    // TODO: Implement checkout via MQTT command to sync agent
    // 1. Find the branch's root UUID from the repo schema
    // 2. Send MQTT command to sync agent to switch subscription targets
    // 3. Sync agent tears down old subscriptions, rebuilds from new root
    eprintln!(
        "Checkout not yet fully implemented. Branch '{}' would be activated.",
        args.branch
    );
    eprintln!("Broker: {} Workspace: {}", args.mqtt_broker, args.workspace);
    eprintln!("This requires the sync agent to support subscription switching (CP-ngl0).");

    std::process::exit(1);
}
