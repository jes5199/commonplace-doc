//! commonplace-checkout: Switch active branch
//!
//! Updates the workspace's active branch by changing which root UUID
//! the sync agent subscribes to.

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

    // TODO: Implement checkout
    // 1. Find the branch's root UUID from the repo schema
    // 2. Update .commonplace/config.json with the new active branch
    // 3. Signal the sync agent to switch subscription targets
    eprintln!(
        "Checkout not yet fully implemented. Branch '{}' would be activated.",
        args.branch
    );
    eprintln!("This requires the sync agent to support subscription switching.");

    std::process::exit(1);
}
