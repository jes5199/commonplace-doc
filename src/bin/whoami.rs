//! commonplace-whoami: Show your sync identity
//!
//! Scans the workspace root for presence files (*.exe, *.usr, *.bot, *.who)
//! and displays identity information. Matches by name or shows all.
//!
//! Usage:
//!   commonplace-whoami              # Show all presence files
//!   commonplace-whoami -n jes       # Filter by name
//!   commonplace-whoami --json       # JSON output

use clap::Parser;
use commonplace_doc::cli::WhoamiArgs;
use commonplace_doc::workspace::find_workspace_root;
use commonplace_types::fs::actor::ActorIO;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = WhoamiArgs::parse();

    let cwd = std::env::current_dir()?;
    let (workspace_root, _) = find_workspace_root(&cwd)?;

    // Scan for presence files
    let mut identities = Vec::new();
    for entry in std::fs::read_dir(&workspace_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|f| f.to_str()) {
            Some(f) => f.to_string(),
            None => continue,
        };

        let ext = match path.extension().and_then(|e| e.to_str()) {
            Some(e) => e,
            None => continue,
        };

        if !matches!(ext, "exe" | "usr" | "bot" | "who") {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let actor: ActorIO = match serde_json::from_str(&content) {
            Ok(a) => a,
            Err(_) => continue,
        };

        // Filter by name if specified
        if let Some(ref filter_name) = args.name {
            if actor.name != *filter_name {
                continue;
            }
        }

        identities.push((filename, actor));
    }

    if identities.is_empty() {
        if let Some(ref name) = args.name {
            eprintln!("No presence file found for '{}'", name);
        } else {
            eprintln!("No presence files found in {}", workspace_root.display());
        }
        std::process::exit(1);
    }

    identities.sort_by(|a, b| a.0.cmp(&b.0));

    if args.json {
        let json_out: Vec<serde_json::Value> = identities
            .iter()
            .map(|(filename, actor)| {
                serde_json::json!({
                    "file": filename,
                    "name": actor.name,
                    "status": actor.status,
                    "pid": actor.pid,
                    "docref": actor.docref,
                    "started_at": actor.started_at,
                    "last_heartbeat": actor.last_heartbeat,
                    "capabilities": actor.capabilities,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&json_out)?);
    } else {
        for (filename, actor) in &identities {
            println!("{}", filename);
            println!("  name:      {}", actor.name);
            println!("  status:    {:?}", actor.status);
            if let Some(pid) = actor.pid {
                println!("  pid:       {}", pid);
            }
            if let Some(ref docref) = actor.docref {
                println!("  docref:    {}", docref);
            }
            if let Some(ref started) = actor.started_at {
                println!("  started:   {}", started);
            }
            if let Some(ref heartbeat) = actor.last_heartbeat {
                println!("  heartbeat: {}", heartbeat);
            }
            if !actor.capabilities.is_empty() {
                println!("  caps:      {}", actor.capabilities.join(", "));
            }
            println!();
        }
    }

    Ok(())
}
