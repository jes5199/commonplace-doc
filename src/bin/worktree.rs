//! commonplace-worktree: Manage additional branch checkouts
//!
//! Like `git worktree`, allows multiple branches to be checked out
//! simultaneously in different directories. Each worktree runs its own
//! commonplace-sync agent, registered in `__processes.json` so the
//! orchestrator manages its lifecycle.

use clap::Parser;
use commonplace_doc::cli::{WorktreeArgs, WorktreeCommand};
use commonplace_doc::orchestrator::ProcessesConfig;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = WorktreeArgs::parse();

    let directory = args.directory.canonicalize().unwrap_or(args.directory);

    match args.command {
        WorktreeCommand::Add { branch, path } => {
            add_worktree(&directory, &branch, &path)?;
        }
        WorktreeCommand::Remove { path } => {
            remove_worktree(&directory, &path)?;
        }
        WorktreeCommand::List => {
            list_worktrees(&directory)?;
        }
    }

    Ok(())
}

/// Derive the process name for a worktree sync agent.
fn worktree_process_name(path: &Path) -> String {
    let dir_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "worktree".to_string());
    format!("worktree-sync-{}", dir_name)
}

fn add_worktree(
    workspace_dir: &Path,
    branch: &str,
    worktree_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Find the branch's root UUID from the schema
    let schema_path = workspace_dir.join(".commonplace.json");
    if !schema_path.exists() {
        eprintln!("Not a commonplace workspace (no .commonplace.json found)");
        std::process::exit(1);
    }

    let schema_content = std::fs::read_to_string(&schema_path)?;
    let schema: serde_json::Value = serde_json::from_str(&schema_content)?;

    // Look for the branch as a directory entry in the schema
    let branch_uuid = schema
        .get("root")
        .and_then(|r| r.get("entries"))
        .and_then(|e| e.get(branch))
        .and_then(|entry| entry.get("node_id"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| {
            format!(
                "Branch '{}' not found in schema. Use 'commonplace branch' to list branches.",
                branch
            )
        })?;

    // Check the worktree directory doesn't already exist with content
    let abs_worktree = if worktree_path.is_absolute() {
        worktree_path.to_path_buf()
    } else {
        workspace_dir.join(worktree_path)
    };

    if abs_worktree.exists() && abs_worktree.read_dir()?.next().is_some() {
        eprintln!(
            "Directory '{}' already exists and is not empty",
            abs_worktree.display()
        );
        std::process::exit(1);
    }

    // Create the directory
    std::fs::create_dir_all(&abs_worktree)?;

    // Find commonplace-sync binary
    let sync_binary = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("commonplace-sync")))
        .unwrap_or_else(|| std::path::PathBuf::from("commonplace-sync"));

    let sync_binary_str = sync_binary.to_string_lossy().to_string();
    let worktree_dir_str = abs_worktree.to_string_lossy().to_string();

    // Add sync agent entry to __processes.json
    let processes_path = workspace_dir.join("__processes.json");
    let mut config_json: serde_json::Value = if processes_path.exists() {
        let content = std::fs::read_to_string(&processes_path)?;
        serde_json::from_str(&content)?
    } else {
        serde_json::json!({})
    };

    let process_name = worktree_process_name(&abs_worktree);
    let entry = serde_json::json!({
        "comment": format!("Worktree sync for branch '{}' at {}", branch, abs_worktree.display()),
        "command": [
            sync_binary_str,
            "--server", "http://localhost:5199",
            "--node", branch_uuid,
            "--directory", worktree_dir_str,
            "--initial-sync", "server",
            "--name", &process_name
        ],
        "cwd": workspace_dir.to_string_lossy()
    });

    if let Some(obj) = config_json.as_object_mut() {
        if obj.contains_key("processes") {
            if let Some(processes) = obj.get_mut("processes").and_then(|p| p.as_object_mut()) {
                processes.insert(process_name.clone(), entry);
            }
        } else {
            obj.insert(process_name.clone(), entry);
        }
    }

    let updated = serde_json::to_string_pretty(&config_json)?;

    // Verify parseable before writing
    ProcessesConfig::parse(&updated).map_err(|e| {
        format!("Generated invalid __processes.json: {}", e)
    })?;

    std::fs::write(&processes_path, format!("{}\n", updated))?;

    println!(
        "Worktree added: {} → {} (branch: {})",
        abs_worktree.display(),
        &branch_uuid[..8.min(branch_uuid.len())],
        branch
    );
    println!("Sync agent '{}' registered. Orchestrator will start it automatically.", process_name);

    Ok(())
}

fn remove_worktree(
    workspace_dir: &Path,
    worktree_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let abs_worktree = if worktree_path.is_absolute() {
        worktree_path.to_path_buf()
    } else {
        workspace_dir.join(worktree_path)
    };

    let process_name = worktree_process_name(&abs_worktree);

    // Remove from __processes.json
    let processes_path = workspace_dir.join("__processes.json");
    if !processes_path.exists() {
        eprintln!("No __processes.json found");
        std::process::exit(1);
    }

    let content = std::fs::read_to_string(&processes_path)?;
    let mut config_json: serde_json::Value = serde_json::from_str(&content)?;

    let removed = if let Some(obj) = config_json.as_object_mut() {
        if obj.contains_key("processes") {
            obj.get_mut("processes")
                .and_then(|p| p.as_object_mut())
                .map(|processes| processes.remove(&process_name).is_some())
                .unwrap_or(false)
        } else {
            obj.remove(&process_name).is_some()
        }
    } else {
        false
    };

    if !removed {
        eprintln!(
            "No worktree sync agent '{}' found in __processes.json",
            process_name
        );
        std::process::exit(1);
    }

    let updated = serde_json::to_string_pretty(&config_json)?;
    std::fs::write(&processes_path, format!("{}\n", updated))?;

    println!(
        "Worktree removed: {} (sync agent '{}' will be stopped by orchestrator)",
        abs_worktree.display(),
        process_name
    );

    Ok(())
}

fn list_worktrees(workspace_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let processes_path = workspace_dir.join("__processes.json");
    if !processes_path.exists() {
        println!("No worktrees configured.");
        return Ok(());
    }

    let content = std::fs::read_to_string(&processes_path)?;
    let config = ProcessesConfig::parse(&content)?;

    let mut found = false;
    for (name, process) in &config.processes {
        if name.starts_with("worktree-sync-") || name.starts_with("branch-sync-") {
            let comment = process
                .comment
                .as_deref()
                .unwrap_or("(no description)");
            println!("  {} — {}", name, comment);
            found = true;
        }
    }

    if !found {
        println!("No worktrees configured.");
    }

    Ok(())
}
