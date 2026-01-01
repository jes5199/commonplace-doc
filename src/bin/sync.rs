//! Commonplace Sync - Local file synchronization with a Commonplace server
//!
//! This binary syncs a local file or directory with a server-side document node.
//! It watches both directions: local changes push to server,
//! server changes update local files.

use clap::Parser;
use commonplace_doc::sync::state_file::{compute_content_hash, SyncStateFile};
use commonplace_doc::sync::{
    build_replace_url, build_uuid_map_recursive, check_server_has_content, detect_from_path,
    directory_sse_task, directory_watcher_task, encode_node_id, ensure_fs_root_exists,
    file_watcher_task, fork_node, handle_file_created, handle_file_deleted, handle_file_modified,
    handle_schema_change, initial_sync, is_binary_content, scan_directory_with_contents,
    spawn_file_sync_tasks, sse_task, sync_schema, sync_single_file, upload_task, DirEvent,
    FileEvent, FileSyncState, ReplaceResponse, ScanOptions, SyncState, SCHEMA_FILENAME,
};
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Commonplace Sync - Keep a local file or directory in sync with a server document
#[derive(Parser, Debug)]
#[command(name = "commonplace-sync")]
#[command(about = "Sync a local file or directory with a Commonplace document node")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Server URL (also reads from COMMONPLACE_SERVER env var)
    #[arg(
        short,
        long,
        default_value = "http://localhost:3000",
        env = "COMMONPLACE_SERVER"
    )]
    server: String,

    /// Node ID to sync with (reads from COMMONPLACE_NODE or COMMONPLACE_PATH env vars; optional if --fork-from is provided)
    #[arg(short, long, env = "COMMONPLACE_NODE")]
    node: Option<String>,

    /// Local file path to sync (mutually exclusive with --directory)
    #[arg(short, long, conflicts_with = "directory")]
    file: Option<PathBuf>,

    /// Local directory path to sync (mutually exclusive with --file)
    #[arg(short, long, conflicts_with = "file")]
    directory: Option<PathBuf>,

    /// Fork from this node before syncing (also reads from COMMONPLACE_FORK_FROM env var; creates a new node)
    #[arg(long, env = "COMMONPLACE_FORK_FROM")]
    fork_from: Option<String>,

    /// When forking, use this commit instead of HEAD
    #[arg(long, requires = "fork_from")]
    at_commit: Option<String>,

    /// Include hidden files when syncing directories
    #[arg(long, default_value = "false")]
    include_hidden: bool,

    /// Glob patterns to ignore (can be specified multiple times)
    #[arg(long)]
    ignore: Vec<String>,

    /// Initial sync strategy when both sides have content
    #[arg(long, default_value = "skip", value_parser = ["local", "server", "skip"])]
    initial_sync: String,

    /// Use path-based API endpoints (/files/*path) instead of ID-based endpoints
    /// This requires the server to have --fs-root configured
    #[arg(long, default_value = "false")]
    use_paths: bool,

    /// Run a command in the synced directory context.
    /// Sync will continue running while the command executes.
    /// When the command exits, sync shuts down and propagates the exit code.
    /// Use `--` to separate sync args from command args.
    #[arg(long, value_name = "COMMAND")]
    exec: Option<String>,

    /// Additional arguments to pass to the exec command (after --)
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    exec_args: Vec<String>,

    /// Run in sandbox mode: creates a temporary directory, syncs content there,
    /// runs the command in isolation, then cleans up on exit.
    /// Implies --exec and conflicts with --directory (uses temp dir instead).
    #[arg(long, conflicts_with = "directory", requires = "exec")]
    sandbox: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    // COMMONPLACE_PATH is an alias for --node (for conductor compatibility)
    let node = args
        .node
        .clone()
        .or_else(|| std::env::var("COMMONPLACE_PATH").ok());

    // Validate that either --file, --directory, or --sandbox is provided
    if args.file.is_none() && args.directory.is_none() && !args.sandbox {
        error!("Either --file, --directory, or --sandbox must be provided");
        return ExitCode::from(1);
    }

    // Exec mode requires --directory or --sandbox (doesn't make sense with single file)
    if args.exec.is_some() && args.directory.is_none() && !args.sandbox {
        error!(
            "--exec requires --directory or --sandbox (exec mode doesn't support single file sync)"
        );
        return ExitCode::from(1);
    }

    // Create HTTP client
    let client = Client::new();

    // Determine the node ID to sync with
    let node_id = match (&node, &args.fork_from) {
        (Some(n), None) => {
            // Direct sync to existing node
            n.clone()
        }
        (None, Some(source)) => {
            // Fork first, then sync to new node
            info!("Forking from node {}...", source);
            match fork_node(&client, &args.server, source, args.at_commit.as_deref()).await {
                Ok(id) => id,
                Err(e) => {
                    error!("Fork failed: {}", e);
                    return ExitCode::from(1);
                }
            }
        }
        (Some(n), Some(source)) => {
            // Both provided - use --node but warn
            warn!(
                "--node and --fork-from both provided; using --node={} (ignoring --fork-from={})",
                n, source
            );
            n.clone()
        }
        (None, None) => {
            error!("Either --node or --fork-from must be provided");
            return ExitCode::from(1);
        }
    };

    // Route to appropriate mode
    let result = if args.sandbox {
        // Sandbox mode: create temp directory, sync there, run command, clean up
        let sandbox_dir =
            std::env::temp_dir().join(format!("commonplace-sandbox-{}", uuid::Uuid::new_v4()));
        info!("Creating sandbox directory: {}", sandbox_dir.display());

        if let Err(e) = std::fs::create_dir_all(&sandbox_dir) {
            error!("Failed to create sandbox directory: {}", e);
            return ExitCode::from(1);
        }

        // Always ignore the schema file (.commonplace.json) when scanning
        let mut ignore_patterns = args.ignore;
        ignore_patterns.push(SCHEMA_FILENAME.to_string());

        let scan_options = ScanOptions {
            include_hidden: args.include_hidden,
            ignore_patterns,
        };

        // exec is required by clap when sandbox is set
        let exec_cmd = args.exec.expect("--sandbox requires --exec");

        // Sandbox mode defaults to pulling server content (since local sandbox is empty)
        // User can still override with explicit --initial-sync if needed
        let initial_sync = if args.initial_sync == "skip" {
            "server".to_string()
        } else {
            args.initial_sync
        };

        let exec_result = run_exec_mode(
            client,
            args.server,
            node_id,
            sandbox_dir.clone(),
            scan_options,
            initial_sync,
            args.use_paths,
            exec_cmd,
            args.exec_args,
            true, // sandbox mode
        )
        .await;

        // Clean up sandbox directory
        info!("Cleaning up sandbox directory: {}", sandbox_dir.display());
        if let Err(e) = std::fs::remove_dir_all(&sandbox_dir) {
            warn!("Failed to clean up sandbox directory: {}", e);
        }

        exec_result
    } else if let Some(directory) = args.directory {
        // Always ignore the schema file (.commonplace.json) when scanning
        let mut ignore_patterns = args.ignore;
        ignore_patterns.push(SCHEMA_FILENAME.to_string());

        let scan_options = ScanOptions {
            include_hidden: args.include_hidden,
            ignore_patterns,
        };

        if let Some(exec_cmd) = args.exec {
            // Exec mode: sync directory, run command, exit when command exits
            run_exec_mode(
                client,
                args.server,
                node_id,
                directory,
                scan_options,
                args.initial_sync,
                args.use_paths,
                exec_cmd,
                args.exec_args,
                false, // not sandbox mode
            )
            .await
        } else {
            // Normal directory sync mode
            run_directory_mode(
                client,
                args.server,
                node_id,
                directory,
                scan_options,
                args.initial_sync,
                args.use_paths,
            )
            .await
            .map(|_| 0u8)
        }
    } else if let Some(file) = args.file {
        run_file_mode(client, args.server, node_id, file)
            .await
            .map(|_| 0u8)
    } else {
        unreachable!("Validated above")
    };

    match result {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            error!("Error: {}", e);
            ExitCode::from(1)
        }
    }
}

/// Run single-file sync mode (original behavior)
async fn run_file_mode(
    client: Client,
    server: String,
    node_id: String,
    file: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting commonplace-sync (file mode): server={}, node={}, file={}",
        server,
        node_id,
        file.display()
    );

    // Verify document exists
    let doc_url = format!("{}/docs/{}/info", server, encode_node_id(&node_id));
    let resp = client.get(&doc_url).send().await?;
    if !resp.status().is_success() {
        error!("Document {} not found on server", node_id);
        return Err(format!("Document {} not found", node_id).into());
    }
    info!("Connected to document {}", node_id);

    // Load or create state file for offline change detection
    let state_file_path = SyncStateFile::state_file_path(&file);
    let state_file = SyncStateFile::load_or_create(&file, &server, &node_id)
        .await
        .map_err(|e| format!("Failed to load state file: {}", e))?;

    // Check for offline local changes and merge them before initial_sync
    if let Some(last_cid) = &state_file.last_synced_cid {
        if file.exists() {
            let current_content = tokio::fs::read(&file).await?;
            let current_hash = compute_content_hash(&current_content);
            let file_name = file.file_name().unwrap_or_default().to_string_lossy();

            if state_file.has_file_changed(&file_name, &current_hash) {
                info!(
                    "Detected offline local changes (last synced at {})",
                    last_cid
                );

                // Push offline changes using replace endpoint with last_synced_cid as parent.
                // The server computes a diff from historical state and creates a CRDT update,
                // which automatically merges with any concurrent server changes.
                let content_info = detect_from_path(&file);
                let is_binary = content_info.is_binary || is_binary_content(&current_content);
                let content_str = if is_binary {
                    use base64::{engine::general_purpose::STANDARD, Engine};
                    STANDARD.encode(&current_content)
                } else {
                    String::from_utf8_lossy(&current_content).to_string()
                };

                let replace_url = build_replace_url(&server, &node_id, last_cid, false);
                info!("Pushing offline changes via CRDT merge...");

                match client
                    .post(&replace_url)
                    .header("content-type", "text/plain")
                    .body(content_str)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<ReplaceResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Merged offline changes: {} chars inserted, {} deleted (new cid: {})",
                                        result.summary.chars_inserted,
                                        result.summary.chars_deleted,
                                        &result.cid[..8.min(result.cid.len())]
                                    );
                                }
                                Err(e) => {
                                    warn!("Failed to parse replace response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            warn!("Failed to push offline changes: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to push offline changes: {}", e);
                    }
                }
            }
        }
    }

    // Initialize shared state with loaded state file
    let state = Arc::new(RwLock::new(SyncState::with_state_file(
        state_file,
        state_file_path,
    )));

    // Perform initial sync
    initial_sync(&client, &server, &node_id, &file, &state).await?;

    // Create channel for file events
    let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);

    // Start file watcher task
    let watcher_handle = tokio::spawn(file_watcher_task(file.clone(), file_tx));

    // Start file change handler task
    // File mode always uses ID-based API (use_paths=false)
    let upload_handle = tokio::spawn(upload_task(
        client.clone(),
        server.clone(),
        node_id.clone(),
        file.clone(),
        state.clone(),
        file_rx,
        false, // use_paths: file mode uses ID-based API
    ));

    // Start SSE subscription task
    let sse_handle = tokio::spawn(sse_task(
        client.clone(),
        server.clone(),
        node_id.clone(),
        file.clone(),
        state.clone(),
        false, // use_paths: file mode uses ID-based API
    ));

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    // Cancel tasks
    watcher_handle.abort();
    upload_handle.abort();
    sse_handle.abort();

    info!("Goodbye!");
    Ok(())
}

/// Run directory sync mode
async fn run_directory_mode(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    options: ScanOptions,
    initial_sync_strategy: String,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting commonplace-sync (directory mode): server={}, fs-root={}, directory={}, use_paths={}",
        server,
        fs_root_id,
        directory.display(),
        use_paths
    );

    // Verify directory exists
    if !directory.is_dir() {
        error!("Not a directory: {}", directory.display());
        return Err(format!("Not a directory: {}", directory.display()).into());
    }

    // Verify fs-root document exists (or create it)
    ensure_fs_root_exists(&client, &server, &fs_root_id).await?;

    // Check if server has existing schema
    let server_has_content = check_server_has_content(&client, &server, &fs_root_id).await;

    // If strategy is "server" and server has content, pull server files first
    // This creates the temporary file_states that handle_schema_change needs
    let file_states: Arc<RwLock<HashMap<String, FileSyncState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    if initial_sync_strategy == "server" && server_has_content {
        info!("Pulling server schema first (initial-sync=server)...");
        // Don't spawn tasks here - the main loop will spawn them for all files
        handle_schema_change(
            &client,
            &server,
            &fs_root_id,
            &directory,
            &file_states,
            false,
            use_paths,
        )
        .await?;
        info!("Server files pulled to local directory");
    }

    // Synchronize schema between local and server
    sync_schema(
        &client,
        &server,
        &fs_root_id,
        &directory,
        &options,
        &initial_sync_strategy,
        server_has_content,
    )
    .await?;

    // Scan files with contents and push each one
    info!("Syncing file contents...");
    let files = scan_directory_with_contents(&directory, &options)
        .map_err(|e| format!("Scan error: {}", e))?;

    // Wait for reconciler to process the schema and create documents
    sleep(Duration::from_millis(100)).await;

    // When not using paths, fetch the updated schema to get UUIDs assigned by reconciler
    // Build a map of relative_path -> node_id for UUID resolution
    // This recursively follows node-backed directories to get all UUIDs
    let uuid_map = if !use_paths {
        let map = build_uuid_map_recursive(&client, &server, &fs_root_id).await;
        info!(
            "Resolved {} UUIDs from server schema for initial sync",
            map.len()
        );
        for (path, uuid) in &map {
            debug!("  UUID map: {} -> {}", path, uuid);
        }
        map
    } else {
        std::collections::HashMap::new()
    };

    // Sync each file
    for file in &files {
        let file_path = directory.join(&file.relative_path);
        if let Err(e) = sync_single_file(
            &client,
            &server,
            &fs_root_id,
            file,
            &file_path,
            &uuid_map,
            &initial_sync_strategy,
            &file_states,
            use_paths,
        )
        .await
        {
            warn!("Failed to sync file {}: {}", file.relative_path, e);
        }
    }

    info!("Initial sync complete: {} files synced", files.len());

    // Start directory watcher
    let (dir_tx, mut dir_rx) = mpsc::channel::<DirEvent>(100);
    let watcher_handle = tokio::spawn(directory_watcher_task(
        directory.clone(),
        dir_tx,
        options.clone(),
    ));

    // Start SSE task for fs-root (to watch for schema changes)
    let sse_handle = tokio::spawn(directory_sse_task(
        client.clone(),
        server.clone(),
        fs_root_id.clone(),
        directory.clone(),
        file_states.clone(),
        use_paths,
    ));

    // Start file sync tasks for each file and store handles in FileSyncState
    {
        let mut states = file_states.write().await;
        for (relative_path, file_state) in states.iter_mut() {
            let file_path = directory.join(relative_path);

            // Spawn sync tasks and store handles in FileSyncState for cleanup on deletion
            file_state.task_handles = spawn_file_sync_tasks(
                client.clone(),
                server.clone(),
                file_state.identifier.clone(),
                file_path,
                file_state.state.clone(),
                file_state.use_paths,
            );
        }
    }

    // Handle directory-level events (file creation/deletion)
    let dir_event_handle = tokio::spawn({
        let client = client.clone();
        let server = server.clone();
        let fs_root_id = fs_root_id.clone();
        let directory = directory.clone();
        let options = options.clone();
        let file_states = file_states.clone();
        async move {
            while let Some(event) = dir_rx.recv().await {
                match event {
                    DirEvent::Created(path) => {
                        handle_file_created(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &options,
                            &file_states,
                            use_paths,
                        )
                        .await;
                    }
                    DirEvent::Modified(path) => {
                        handle_file_modified(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &options,
                        )
                        .await;
                    }
                    DirEvent::Deleted(path) => {
                        handle_file_deleted(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &options,
                            &file_states,
                        )
                        .await;
                    }
                }
            }
        }
    });

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    // Cancel all tasks
    watcher_handle.abort();
    sse_handle.abort();
    dir_event_handle.abort();

    // Abort all per-file sync tasks
    {
        let states = file_states.read().await;
        for file_state in states.values() {
            for handle in &file_state.task_handles {
                handle.abort();
            }
        }
    }

    info!("Goodbye!");
    Ok(())
}

/// Run exec mode: sync directory, run command, exit when command exits
///
/// This mode is designed for workflows where a user wants to work on synced files
/// with their preferred editor/tool, and have everything tear down cleanly when done.
#[allow(clippy::too_many_arguments)]
async fn run_exec_mode(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    options: ScanOptions,
    initial_sync_strategy: String,
    use_paths: bool,
    exec_cmd: String,
    exec_args: Vec<String>,
    sandbox: bool,
) -> Result<u8, Box<dyn std::error::Error>> {
    info!(
        "Starting commonplace-sync (exec mode): server={}, fs-root={}, directory={}, exec={}",
        server,
        fs_root_id,
        directory.display(),
        exec_cmd
    );

    // Verify directory exists or create it
    if !directory.exists() {
        info!("Creating directory: {}", directory.display());
        tokio::fs::create_dir_all(&directory).await?;
    }
    if !directory.is_dir() {
        error!("Not a directory: {}", directory.display());
        return Err(format!("Not a directory: {}", directory.display()).into());
    }

    // Verify fs-root document exists (or create it)
    ensure_fs_root_exists(&client, &server, &fs_root_id).await?;

    // Check if server has existing schema
    let server_has_content = check_server_has_content(&client, &server, &fs_root_id).await;

    // If strategy is "server" and server has content, pull server files first
    let file_states: Arc<RwLock<HashMap<String, FileSyncState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    if initial_sync_strategy == "server" && server_has_content {
        info!("Pulling server schema first (initial-sync=server)...");
        handle_schema_change(
            &client,
            &server,
            &fs_root_id,
            &directory,
            &file_states,
            false,
            use_paths,
        )
        .await?;
        info!("Server files pulled to local directory");
    }

    // Synchronize schema between local and server
    sync_schema(
        &client,
        &server,
        &fs_root_id,
        &directory,
        &options,
        &initial_sync_strategy,
        server_has_content,
    )
    .await?;

    // Sync file contents
    info!("Syncing file contents...");
    let files = scan_directory_with_contents(&directory, &options)
        .map_err(|e| format!("Scan error: {}", e))?;

    // Wait for reconciler to process the schema and create documents
    sleep(Duration::from_millis(100)).await;

    // When not using paths, fetch the updated schema to get UUIDs assigned by reconciler
    // Build a map of relative_path -> node_id for UUID resolution
    // This recursively follows node-backed directories to get all UUIDs
    let uuid_map = if !use_paths {
        let map = build_uuid_map_recursive(&client, &server, &fs_root_id).await;
        info!(
            "Resolved {} UUIDs from server schema for initial sync",
            map.len()
        );
        for (path, uuid) in &map {
            debug!("  UUID map: {} -> {}", path, uuid);
        }
        map
    } else {
        std::collections::HashMap::new()
    };

    // Sync each file
    for file in &files {
        let file_path = directory.join(&file.relative_path);
        if let Err(e) = sync_single_file(
            &client,
            &server,
            &fs_root_id,
            file,
            &file_path,
            &uuid_map,
            &initial_sync_strategy,
            &file_states,
            use_paths,
        )
        .await
        {
            warn!("Failed to sync file {}: {}", file.relative_path, e);
        }
    }

    info!("Initial sync complete: {} files synced", files.len());

    // Start directory watcher
    let (dir_tx, mut dir_rx) = mpsc::channel::<DirEvent>(100);
    let watcher_handle = tokio::spawn(directory_watcher_task(
        directory.clone(),
        dir_tx,
        options.clone(),
    ));

    // Start SSE task for fs-root
    let sse_handle = tokio::spawn(directory_sse_task(
        client.clone(),
        server.clone(),
        fs_root_id.clone(),
        directory.clone(),
        file_states.clone(),
        use_paths,
    ));

    // Start file sync tasks for each file
    {
        let mut states = file_states.write().await;
        for (relative_path, file_state) in states.iter_mut() {
            let file_path = directory.join(relative_path);
            file_state.task_handles = spawn_file_sync_tasks(
                client.clone(),
                server.clone(),
                file_state.identifier.clone(),
                file_path,
                file_state.state.clone(),
                file_state.use_paths,
            );
        }
    }

    // Start directory event handler (same logic as run_directory_mode)
    let dir_event_handle = tokio::spawn({
        let client = client.clone();
        let server = server.clone();
        let fs_root_id = fs_root_id.clone();
        let directory = directory.clone();
        let options = options.clone();
        let file_states = file_states.clone();
        async move {
            while let Some(event) = dir_rx.recv().await {
                match event {
                    DirEvent::Created(path) => {
                        handle_file_created(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &options,
                            &file_states,
                            use_paths,
                        )
                        .await;
                    }
                    DirEvent::Modified(path) => {
                        handle_file_modified(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &options,
                        )
                        .await;
                    }
                    DirEvent::Deleted(path) => {
                        handle_file_deleted(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &options,
                            &file_states,
                        )
                        .await;
                    }
                }
            }
        }
    });

    // Build the command to execute
    // Parse exec_cmd - if it contains spaces and no exec_args, treat as shell command
    let (program, args) = if exec_args.is_empty() && exec_cmd.contains(' ') {
        // Treat as shell command
        if cfg!(target_os = "windows") {
            ("cmd".to_string(), vec!["/C".to_string(), exec_cmd])
        } else {
            ("sh".to_string(), vec!["-c".to_string(), exec_cmd])
        }
    } else {
        // Direct command with optional args
        (exec_cmd, exec_args)
    };

    info!("Launching: {} {:?}", program, args);

    // Build the command
    let mut cmd = tokio::process::Command::new(&program);
    cmd.args(&args)
        .current_dir(&directory)
        // Inherit stdin/stdout/stderr for interactive programs
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        // Pass through key environment variables
        .env("COMMONPLACE_SERVER", &server)
        .env("COMMONPLACE_NODE", &fs_root_id);

    // In sandbox mode, add commonplace binaries to PATH
    if sandbox {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(bin_dir) = exe_path.parent() {
                let current_path = std::env::var_os("PATH").unwrap_or_default();
                if let Ok(new_path) = std::env::join_paths(
                    std::iter::once(bin_dir.to_path_buf())
                        .chain(std::env::split_paths(&current_path)),
                ) {
                    cmd.env("PATH", &new_path);
                    info!("Added {} to PATH for sandbox", bin_dir.display());
                }
            }
        }
    }

    // Spawn the child process in the synced directory
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn command '{}': {}", program, e))?;

    // Wait for child to exit OR signal
    let exit_code = tokio::select! {
        status = child.wait() => {
            match status {
                Ok(s) => {
                    let code = s.code().unwrap_or(1) as u8;
                    info!("Command exited with code: {}", code);
                    code
                }
                Err(e) => {
                    error!("Failed to wait for command: {}", e);
                    1
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, terminating child process...");
            // Try to kill the child gracefully
            #[cfg(unix)]
            {
                // Send SIGTERM to child
                if let Some(pid) = child.id() {
                    unsafe {
                        libc::kill(pid as i32, libc::SIGTERM);
                    }
                }
                // Wait briefly for graceful shutdown
                tokio::select! {
                    _ = child.wait() => {}
                    _ = sleep(Duration::from_secs(5)) => {
                        // Force kill after timeout
                        let _ = child.kill().await;
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = child.kill().await;
            }
            130 // Standard exit code for Ctrl+C
        }
    };

    info!("Shutting down sync...");

    // Cancel all tasks
    watcher_handle.abort();
    sse_handle.abort();
    dir_event_handle.abort();

    // Abort all per-file sync tasks
    {
        let states = file_states.read().await;
        for file_state in states.values() {
            for handle in &file_state.task_handles {
                handle.abort();
            }
        }
    }

    info!("Goodbye!");
    Ok(exit_code)
}
