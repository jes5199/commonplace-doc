//! commonplace-whoami: Print your sync identity
//!
//! Like Unix `whoami` — prints a single name. Determines identity from:
//! 1. COMMONPLACE_PRESENCE env var (set by --presence flag on sync)
//! 2. If exactly one presence file exists, that's you
//!
//! Usage:
//!   commonplace-whoami         # prints e.g. "jes.usr"
//!   commonplace-whoami --json  # {"file":"jes.usr","name":"jes","type":"usr",...}

use clap::Parser;
use commonplace_doc::cli::WhoamiArgs;
use commonplace_doc::workspace::find_workspace_root;
use commonplace_types::fs::actor::ActorIO;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = WhoamiArgs::parse();

    let cwd = std::env::current_dir()?;
    let (workspace_root, _) = find_workspace_root(&cwd)?;

    // Strategy 1: COMMONPLACE_PRESENCE env var
    if let Ok(presence) = std::env::var("COMMONPLACE_PRESENCE") {
        let path = workspace_root.join(&presence);
        if path.exists() {
            return print_identity(&presence, &path, args.json);
        }
        // Env var set but file doesn't exist — still use the name
        if args.json {
            println!(
                "{}",
                serde_json::json!({
                    "file": presence,
                    "name": presence.rsplit_once('.').map(|(n, _)| n).unwrap_or(&presence),
                    "type": presence.rsplit_once('.').map(|(_, e)| e).unwrap_or("who"),
                })
            );
        } else {
            println!("{}", presence);
        }
        return Ok(());
    }

    // Strategy 2: exactly one presence file
    let mut presence_files = Vec::new();
    for entry in std::fs::read_dir(&workspace_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if matches!(ext, "exe" | "usr" | "bot" | "who") {
            if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                presence_files.push((filename.to_string(), path.clone()));
            }
        }
    }

    match presence_files.len() {
        0 => {
            eprintln!("No presence files found. Set COMMONPLACE_PRESENCE or start sync with --presence.");
            std::process::exit(1);
        }
        1 => {
            let (filename, path) = &presence_files[0];
            print_identity(filename, path, args.json)
        }
        _ => {
            eprintln!(
                "Multiple presence files found ({}). Set COMMONPLACE_PRESENCE to disambiguate:",
                presence_files.len()
            );
            for (f, _) in &presence_files {
                eprintln!("  {}", f);
            }
            std::process::exit(1);
        }
    }
}

fn print_identity(
    filename: &str,
    path: &std::path::Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if json {
        // Try to read full actor data
        if let Ok(content) = std::fs::read_to_string(path) {
            if let Ok(actor) = serde_json::from_str::<ActorIO>(&content) {
                let ext = filename.rsplit_once('.').map(|(_, e)| e).unwrap_or("who");
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "file": filename,
                        "name": actor.name,
                        "type": ext,
                        "status": actor.status,
                        "pid": actor.pid,
                        "docref": actor.docref,
                        "started_at": actor.started_at,
                        "last_heartbeat": actor.last_heartbeat,
                        "capabilities": actor.capabilities,
                    }))?
                );
                return Ok(());
            }
        }
        // Fallback: just the filename
        println!(
            "{}",
            serde_json::json!({
                "file": filename,
                "name": filename.rsplit_once('.').map(|(n, _)| n).unwrap_or(filename),
                "type": filename.rsplit_once('.').map(|(_, e)| e).unwrap_or("who"),
            })
        );
    } else {
        println!("{}", filename);
    }
    Ok(())
}
