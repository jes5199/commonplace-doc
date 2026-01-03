//! commonplace: CLI dispatcher for commonplace subcommands
//!
//! Usage:
//!   commonplace sync ...      → runs commonplace-sync
//!   commonplace log ...       → runs commonplace-log
//!   commonplace show ...      → runs commonplace-show
//!   commonplace help <cmd>    → runs commonplace-<cmd> --help

use std::env;
use std::os::unix::process::CommandExt;
use std::process::Command;

const SUBCOMMANDS: &[(&str, &str)] = &[
    ("server", "Document server with Yjs commit history"),
    ("sync", "Sync files with commonplace server"),
    (
        "orchestrator",
        "Process supervisor for commonplace services",
    ),
    (
        "log",
        "Show commit history for a synced file (like git log)",
    ),
    ("show", "Show content at a specific commit (like git show)"),
    (
        "replay",
        "View or replay edit history (legacy, use log/show)",
    ),
    ("ps", "List processes managed by orchestrator"),
    ("signal", "Send signal to orchestrator-managed process"),
    ("link", "Create a link to a commonplace document"),
    ("uuid", "Resolve a synced file path to its UUID"),
    ("cmd", "Send commands to document paths via MQTT"),
    ("mcp", "MCP server for commonplace"),
    ("store", "Document store with MQTT transport"),
    ("http", "HTTP gateway that translates to MQTT"),
];

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(0);
    }

    let subcommand = &args[1];

    // Handle help
    if subcommand == "help" || subcommand == "--help" || subcommand == "-h" {
        if args.len() > 2 && subcommand == "help" {
            // commonplace help <cmd> → run commonplace-<cmd> --help
            let cmd = &args[2];
            exec_subcommand(cmd, &["--help".to_string()]);
        } else {
            print_usage();
            std::process::exit(0);
        }
    }

    // Handle version
    if subcommand == "--version" || subcommand == "-V" {
        println!("commonplace {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }

    // Run the subcommand
    let subcommand_args: Vec<String> = args[2..].to_vec();
    exec_subcommand(subcommand, &subcommand_args);
}

fn exec_subcommand(name: &str, args: &[String]) -> ! {
    let binary_name = format!("commonplace-{}", name);

    // Try to find the binary in the same directory as this executable
    let current_exe = env::current_exe().ok();
    let exe_dir = current_exe.as_ref().and_then(|p| p.parent());

    let mut cmd = if let Some(dir) = exe_dir {
        let local_path = dir.join(&binary_name);
        if local_path.exists() {
            Command::new(local_path)
        } else {
            Command::new(&binary_name)
        }
    } else {
        Command::new(&binary_name)
    };

    cmd.args(args);

    // On Unix, exec replaces this process
    let err = cmd.exec();

    // If we get here, exec failed
    eprintln!("error: unknown command '{}'\n", name);
    eprintln!("Run 'commonplace help' for available commands.");

    // Check if it's a typo
    let suggestions: Vec<&str> = SUBCOMMANDS
        .iter()
        .filter(|(cmd, _)| {
            cmd.starts_with(&name[..1.min(name.len())]) || levenshtein(cmd, name) <= 2
        })
        .map(|(cmd, _)| *cmd)
        .collect();

    if !suggestions.is_empty() {
        eprintln!("\nDid you mean one of these?");
        for s in suggestions {
            eprintln!("    {}", s);
        }
    }

    // Show the actual error for debugging
    if env::var("COMMONPLACE_DEBUG").is_ok() {
        eprintln!("\nDebug: {}", err);
    }

    std::process::exit(127);
}

fn print_usage() {
    println!("commonplace - Document server with Yjs commit history\n");
    println!("Usage: commonplace <command> [args...]\n");
    println!("Commands:");

    let max_len = SUBCOMMANDS.iter().map(|(c, _)| c.len()).max().unwrap_or(0);
    for (cmd, desc) in SUBCOMMANDS {
        println!("  {:width$}  {}", cmd, desc, width = max_len);
    }

    println!("\nRun 'commonplace help <command>' for more information.");
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let len_a = a.len();
    let len_b = b.len();

    if len_a == 0 {
        return len_b;
    }
    if len_b == 0 {
        return len_a;
    }

    let mut prev: Vec<usize> = (0..=len_b).collect();
    let mut curr = vec![0; len_b + 1];

    for i in 1..=len_a {
        curr[0] = i;
        for j in 1..=len_b {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[len_b]
}
