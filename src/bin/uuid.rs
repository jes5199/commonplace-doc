//! commonplace-uuid: Resolve a synced file path to its UUID
//!
//! Usage:
//!   commonplace-uuid path/to/file.txt
//!   commonplace-uuid --json path/to/file.txt
//!
//! Prints the UUID that the file is linked to, or an error if not found.

use clap::Parser;
use commonplace_doc::{
    cli::UuidArgs,
    workspace::{find_workspace_root, normalize_path, resolve_uuid, split_path},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = UuidArgs::parse();

    // Find the workspace root by searching up for .commonplace.json
    let cwd = std::env::current_dir()?;
    let (workspace_root, _) = find_workspace_root(&cwd)?;

    // Convert path to workspace-relative
    let rel_path = normalize_path(&args.path, &cwd, &workspace_root)?;

    // Split into directory components and filename
    let (dirs, filename) = split_path(&rel_path)?;

    // Resolve to the correct schema and find the UUID
    let uuid = resolve_uuid(&workspace_root, &dirs, &filename)?;

    if args.json {
        println!(
            "{{\"path\":\"{}\",\"uuid\":\"{}\"}}",
            rel_path.replace('\\', "\\\\").replace('"', "\\\""),
            uuid
        );
    } else {
        println!("{}", uuid);
    }

    Ok(())
}
