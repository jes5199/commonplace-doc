//! Commonplace Sync - Local file synchronization with a Commonplace server
//!
//! This binary syncs a local file or directory with a server-side document node.
//! It watches both directions: local changes push to server,
//! server changes update local files.

use clap::Parser;
use commonplace_doc::fs::{Entry, FsSchema};
use commonplace_doc::sync::state_file::{compute_content_hash, SyncStateFile};
use commonplace_doc::sync::{
    build_head_url, build_replace_url, detect_from_path, directory_watcher_task, encode_node_id,
    file_watcher_task, fork_node, is_allowed_extension, is_binary_content, normalize_path,
    push_file_content, push_json_content, push_schema_to_server, scan_directory,
    scan_directory_with_contents, schema_to_json, sse_task, upload_task, DirEvent, FileEvent,
    HeadResponse, ReplaceResponse, ScanOptions, SyncState,
};
use futures::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
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

/// Filename for the local schema JSON file written during directory sync
const SCHEMA_FILENAME: &str = ".commonplace.json";

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

/// State for a single file in directory sync
#[derive(Debug)]
struct FileSyncState {
    /// Relative path from directory root
    #[allow(dead_code)]
    relative_path: String,
    /// Identifier - either path (for /files/* API) or node ID (for /docs/* API)
    identifier: String,
    /// Sync state for this file
    state: Arc<RwLock<SyncState>>,
    /// Task handles for cleanup on deletion
    task_handles: Vec<JoinHandle<()>>,
    /// Whether to use path-based API (/files/*) or ID-based API (/docs/*)
    use_paths: bool,
    /// Content hash for fork detection (SHA-256 hex)
    content_hash: Option<String>,
}

/// Spawn sync tasks (watcher, upload, SSE) for a single file.
/// Returns the task handles so they can be aborted on file deletion.
fn spawn_file_sync_tasks(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> Vec<JoinHandle<()>> {
    let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);

    vec![
        // File watcher task
        tokio::spawn(file_watcher_task(file_path.clone(), file_tx)),
        // Upload task
        tokio::spawn(upload_task(
            client.clone(),
            server.clone(),
            identifier.clone(),
            file_path.clone(),
            state.clone(),
            file_rx,
            use_paths,
        )),
        // SSE task
        tokio::spawn(sse_task(
            client, server, identifier, file_path, state, use_paths,
        )),
    ]
}

/// Write the schema JSON to the local .commonplace.json file
async fn write_schema_file(
    directory: &std::path::Path,
    schema_json: &str,
) -> Result<(), std::io::Error> {
    let schema_path = directory.join(SCHEMA_FILENAME);
    tokio::fs::write(&schema_path, schema_json).await?;
    info!("Wrote schema to {}", schema_path.display());
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
    let doc_url = format!("{}/docs/{}/info", server, encode_node_id(&fs_root_id));
    let resp = client.get(&doc_url).send().await?;
    if !resp.status().is_success() {
        // Create the document
        info!("Creating fs-root document: {}", fs_root_id);
        let create_url = format!("{}/docs", server);
        let create_resp = client
            .post(&create_url)
            .json(&serde_json::json!({
                "type": "document",
                "id": fs_root_id,
                "content_type": "application/json"
            }))
            .send()
            .await?;
        if !create_resp.status().is_success() {
            let status = create_resp.status();
            let body = create_resp.text().await.unwrap_or_default();
            return Err(format!("Failed to create fs-root document: {} - {}", status, body).into());
        }
    }
    info!("Connected to fs-root document: {}", fs_root_id);

    // Check if server has existing schema FIRST (needed for server strategy)
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(&fs_root_id));
    let head_resp = client.get(&head_url).send().await?;
    let server_has_content = if head_resp.status().is_success() {
        let head: HeadResponse = head_resp.json().await?;
        !head.content.is_empty() && head.content != "{}"
    } else {
        false
    };

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

    // Scan directory and generate FS schema
    info!("Scanning directory...");
    let schema = scan_directory(&directory, &options).map_err(|e| format!("Scan error: {}", e))?;
    let schema_json = schema_to_json(&schema)?;
    info!(
        "Directory scanned: generated {} bytes of schema JSON",
        schema_json.len()
    );

    // Decide whether to push local schema based on strategy
    let should_push_schema = match initial_sync_strategy.as_str() {
        "local" => true,
        "server" => !server_has_content,
        "skip" => !server_has_content,
        _ => !server_has_content,
    };

    if should_push_schema {
        // Push schema to fs-root node
        info!("Pushing filesystem schema to server...");
        push_schema_to_server(&client, &server, &fs_root_id, &schema_json).await?;
        info!("Schema pushed successfully");

        // Write the schema to local .commonplace.json file
        if let Err(e) = write_schema_file(&directory, &schema_json).await {
            warn!("Failed to write schema file: {}", e);
        }
    } else {
        info!(
            "Server already has content, skipping schema push (strategy={})",
            initial_sync_strategy
        );

        // Fetch and write the server's schema to local .commonplace.json file
        let head_url = format!("{}/docs/{}/head", server, encode_node_id(&fs_root_id));
        if let Ok(resp) = client.get(&head_url).send().await {
            if resp.status().is_success() {
                if let Ok(head) = resp.json::<HeadResponse>().await {
                    if let Err(e) = write_schema_file(&directory, &head.content).await {
                        warn!("Failed to write schema file: {}", e);
                    }
                }
            }
        }
    }

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

    // Sync each file (file_states was created earlier for server-first pull)
    for file in &files {
        // When use_paths=true, use relative path directly for /files/* endpoints
        // When use_paths=false, use UUID from schema if available, otherwise derive as fs_root_id:path
        let identifier = if use_paths {
            file.relative_path.clone()
        } else if let Some(uuid) = uuid_map.get(&file.relative_path) {
            info!("Using UUID for {}: {}", file.relative_path, uuid);
            uuid.clone()
        } else {
            let derived = format!("{}:{}", fs_root_id, file.relative_path);
            warn!(
                "No UUID found for {}, using derived ID: {}",
                file.relative_path, derived
            );
            derived
        };
        info!("Syncing file: {} -> {}", file.relative_path, identifier);

        let file_path = directory.join(&file.relative_path);

        // Reuse existing state if handle_schema_change already created one (server-first sync)
        // Otherwise create a new state
        let state = {
            let states = file_states.read().await;
            if let Some(existing) = states.get(&file.relative_path) {
                existing.state.clone()
            } else {
                Arc::new(RwLock::new(SyncState::new()))
            }
        };

        // Push initial content if local wins or server is empty
        let file_head_url = build_head_url(&server, &identifier, use_paths);
        let file_head_resp = client.get(&file_head_url).send().await;

        let should_push_content = match &file_head_resp {
            Ok(resp) if resp.status().is_success() => {
                // Check if server has content
                false // Will be handled below after parsing
            }
            _ => true, // Node doesn't exist yet or error
        };

        if let Ok(resp) = file_head_resp {
            debug!(
                "File head response status: {} for {}",
                resp.status(),
                identifier
            );
            if resp.status().is_success() {
                let head: HeadResponse = resp.json().await?;
                info!(
                    "File head content empty: {}, strategy: {}",
                    head.content.is_empty(),
                    initial_sync_strategy
                );
                if head.content.is_empty() || initial_sync_strategy == "local" {
                    // Push local content (binary files are already base64 encoded)
                    info!(
                        "Pushing initial content for: {} ({} bytes)",
                        identifier,
                        file.content.len()
                    );
                    if !file.is_binary && file.content_type.starts_with("application/json") {
                        push_json_content(
                            &client,
                            &server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    } else {
                        push_file_content(
                            &client,
                            &server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    }
                } else {
                    // Server has content
                    if initial_sync_strategy == "server" {
                        // Pull server content to local
                        // For binary files, decode base64; for text, use as-is
                        if file.is_binary {
                            use base64::{engine::general_purpose::STANDARD, Engine};
                            if let Ok(decoded) = STANDARD.decode(&head.content) {
                                tokio::fs::write(&file_path, &decoded).await?;
                            }
                        } else {
                            tokio::fs::write(&file_path, &head.content).await?;
                        }
                    }
                    // Seed SyncState so SSE updates work and uploads use correct parent CID
                    // For "skip" strategy: use local content for last_written_content so
                    // server edits aren't blocked (we didn't modify local, so local IS
                    // what's "last written"). For "server" strategy: we wrote server
                    // content to local, so use server content as last_written.
                    let mut s = state.write().await;
                    s.last_written_cid = head.cid;
                    if initial_sync_strategy == "skip" {
                        // Local file unchanged, so last_written matches local content
                        s.last_written_content = file.content.clone();
                    } else {
                        // Server content was written to local
                        s.last_written_content = head.content;
                    }
                }
            } else if should_push_content {
                // Node doesn't exist yet or returned non-success - push content with retries
                info!("Node not ready, will push with retries for: {}", identifier);
                if !file.is_binary && file.content_type.starts_with("application/json") {
                    push_json_content(
                        &client,
                        &server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                } else {
                    push_file_content(
                        &client,
                        &server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                }
            }
        }

        // Store state for this file (tasks will be spawned after initial sync)
        {
            // For binary files, decode base64 to get raw bytes for consistent hashing
            // New file detection hashes raw bytes, so we must do the same here
            let content_hash = if file.is_binary {
                use base64::{engine::general_purpose::STANDARD, Engine};
                match STANDARD.decode(&file.content) {
                    Ok(raw_bytes) => compute_content_hash(&raw_bytes),
                    Err(_) => compute_content_hash(file.content.as_bytes()),
                }
            } else {
                compute_content_hash(file.content.as_bytes())
            };
            let mut states = file_states.write().await;
            states.insert(
                file.relative_path.clone(),
                FileSyncState {
                    relative_path: file.relative_path.clone(),
                    identifier: identifier.clone(),
                    state,
                    task_handles: Vec::new(), // Will be populated after initial sync
                    use_paths,
                    content_hash: Some(content_hash),
                },
            );
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
                        debug!("Directory event: file created: {}", path.display());

                        // Calculate relative path (normalized to forward slashes for cross-platform consistency)
                        let relative_path = match path.strip_prefix(&directory) {
                            Ok(rel) => normalize_path(&rel.to_string_lossy()),
                            Err(_) => continue,
                        };

                        // Check if file matches ignore patterns
                        let file_name = path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_default();
                        let should_ignore = options.ignore_patterns.iter().any(|pattern| {
                            if pattern == &file_name {
                                true
                            } else if pattern.contains('*') {
                                let parts: Vec<&str> = pattern.split('*').collect();
                                if parts.len() == 2 {
                                    file_name.starts_with(parts[0]) && file_name.ends_with(parts[1])
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        });
                        if should_ignore {
                            debug!(
                                "Ignoring new file (matches ignore pattern): {}",
                                relative_path
                            );
                            continue;
                        }

                        // Skip hidden files unless configured to include them
                        if !options.include_hidden && file_name.starts_with('.') {
                            debug!("Ignoring hidden file: {}", relative_path);
                            continue;
                        }

                        // Skip files with disallowed extensions
                        if !is_allowed_extension(&path) {
                            debug!("Ignoring file with disallowed extension: {}", relative_path);
                            continue;
                        }

                        // Check if we already have sync tasks for this file
                        let already_tracked = {
                            let states = file_states.read().await;
                            states.contains_key(&relative_path)
                        };

                        if !already_tracked && path.is_file() {
                            // Read file content first (needed for both hash check and push)
                            let raw_content = match tokio::fs::read(&path).await {
                                Ok(c) => c,
                                Err(e) => {
                                    warn!("Failed to read new file {}: {}", path.display(), e);
                                    continue;
                                }
                            };

                            // Compute content hash and check for matching file (fork detection)
                            let content_hash = compute_content_hash(&raw_content);
                            let matching_source = {
                                let states = file_states.read().await;
                                states
                                    .iter()
                                    .find(|(_, state)| {
                                        state.content_hash.as_ref() == Some(&content_hash)
                                    })
                                    .map(|(_, state)| state.identifier.clone())
                            };

                            // Initial identifier - may be updated after schema push if not using paths
                            let mut identifier = if use_paths {
                                relative_path.clone()
                            } else {
                                format!("{}:{}", fs_root_id, relative_path)
                            };
                            let state = Arc::new(RwLock::new(SyncState::new()));

                            // If content matches an existing file, try to fork it
                            let forked_successfully = if let Some(source_id) = matching_source {
                                info!(
                                    "New file {} has identical content to {}, forking...",
                                    relative_path, source_id
                                );

                                // Convert Result to avoid holding Box<dyn Error> across await
                                let fork_result = fork_node(&client, &server, &source_id, None)
                                    .await
                                    .map_err(|e| e.to_string());

                                match fork_result {
                                    Ok(forked_id) => {
                                        info!(
                                            "Forked {} -> {} for new file {}",
                                            source_id, forked_id, relative_path
                                        );
                                        // Update schema to point to forked node
                                        if let Ok(schema) = scan_directory(&directory, &options) {
                                            if let Ok(json) = schema_to_json(&schema) {
                                                if let Err(e) = push_schema_to_server(
                                                    &client,
                                                    &server,
                                                    &fs_root_id,
                                                    &json,
                                                )
                                                .await
                                                {
                                                    warn!("Failed to push updated schema: {}", e);
                                                }
                                            }
                                        }
                                        // Use the forked ID as the identifier
                                        identifier = forked_id;
                                        true
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Fork failed for {}, falling back to new document: {}",
                                            relative_path, e
                                        );
                                        false
                                    }
                                }
                            } else {
                                false
                            };

                            // If fork didn't succeed (no match or fork failed), create document normally
                            if !forked_successfully {
                                // Push updated schema FIRST so server reconciler creates the node
                                if let Ok(schema) = scan_directory(&directory, &options) {
                                    if let Ok(json) = schema_to_json(&schema) {
                                        if let Err(e) = push_schema_to_server(
                                            &client,
                                            &server,
                                            &fs_root_id,
                                            &json,
                                        )
                                        .await
                                        {
                                            warn!("Failed to push updated schema: {}", e);
                                        }
                                    }
                                }

                                // Wait briefly for server to reconcile and create the node
                                sleep(Duration::from_millis(100)).await;

                                // When not using paths, fetch the UUID from the updated schema
                                // The reconciler assigns UUIDs to new entries, so we need to look them up
                                if !use_paths {
                                    if let Some(uuid) = fetch_node_id_from_schema(
                                        &client,
                                        &server,
                                        &fs_root_id,
                                        &relative_path,
                                    )
                                    .await
                                    {
                                        info!(
                                            "Resolved UUID for {}: {} -> {}",
                                            relative_path, identifier, uuid
                                        );
                                        identifier = uuid;
                                    } else {
                                        warn!(
                                            "Could not resolve UUID for {}, using derived ID: {}",
                                            relative_path, identifier
                                        );
                                    }
                                }

                                // Now push initial content (handle binary files)
                                let content_info = detect_from_path(&path);
                                let is_binary =
                                    content_info.is_binary || is_binary_content(&raw_content);

                                let content = if is_binary {
                                    use base64::{engine::general_purpose::STANDARD, Engine};
                                    STANDARD.encode(&raw_content)
                                } else {
                                    String::from_utf8_lossy(&raw_content).to_string()
                                };

                                if let Err(e) =
                                    if !is_binary && content_info.mime_type == "application/json" {
                                        push_json_content(
                                            &client,
                                            &server,
                                            &identifier,
                                            &content,
                                            &state,
                                            use_paths,
                                        )
                                        .await
                                    } else {
                                        push_file_content(
                                            &client,
                                            &server,
                                            &identifier,
                                            &content,
                                            &state,
                                            use_paths,
                                        )
                                        .await
                                    }
                                {
                                    warn!("Failed to push new file content: {}", e);
                                }
                            }

                            // Spawn sync tasks for the new file
                            let task_handles = spawn_file_sync_tasks(
                                client.clone(),
                                server.clone(),
                                identifier.clone(),
                                path.clone(),
                                state.clone(),
                                use_paths,
                            );

                            // Add to file_states with task handles
                            {
                                let mut states = file_states.write().await;
                                states.insert(
                                    relative_path.clone(),
                                    FileSyncState {
                                        relative_path: relative_path.clone(),
                                        identifier,
                                        state,
                                        task_handles,
                                        use_paths,
                                        content_hash: Some(content_hash),
                                    },
                                );
                            }

                            info!("Started sync for new local file: {}", relative_path);
                        }
                    }
                    DirEvent::Modified(path) => {
                        debug!("Directory event: file modified: {}", path.display());
                        // Modified files are handled by per-file watchers
                        // Just update schema in case metadata changed
                        if let Ok(schema) = scan_directory(&directory, &options) {
                            if let Ok(json) = schema_to_json(&schema) {
                                if let Err(e) =
                                    push_schema_to_server(&client, &server, &fs_root_id, &json)
                                        .await
                                {
                                    warn!("Failed to push updated schema: {}", e);
                                }
                            }
                        }
                    }
                    DirEvent::Deleted(path) => {
                        debug!("Directory event: file deleted: {}", path.display());

                        // Calculate relative path (normalized to forward slashes for cross-platform consistency)
                        let relative_path = match path.strip_prefix(&directory) {
                            Ok(rel) => normalize_path(&rel.to_string_lossy()),
                            Err(_) => {
                                warn!("Could not strip prefix from deleted path");
                                continue;
                            }
                        };

                        // Stop sync tasks for this file and remove from file_states
                        {
                            let mut states = file_states.write().await;
                            if let Some(file_state) = states.remove(&relative_path) {
                                info!("Stopping sync tasks for deleted file: {}", relative_path);
                                for handle in file_state.task_handles {
                                    handle.abort();
                                }
                            }
                        }

                        // Rescan and push updated schema
                        if let Ok(schema) = scan_directory(&directory, &options) {
                            if let Ok(json) = schema_to_json(&schema) {
                                if let Err(e) =
                                    push_schema_to_server(&client, &server, &fs_root_id, &json)
                                        .await
                                {
                                    warn!("Failed to push updated schema: {}", e);
                                }
                            }
                        }
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
    let doc_url = format!("{}/docs/{}/info", server, encode_node_id(&fs_root_id));
    let resp = client.get(&doc_url).send().await?;
    if !resp.status().is_success() {
        // Create the document
        info!("Creating fs-root document: {}", fs_root_id);
        let create_url = format!("{}/docs", server);
        let create_resp = client
            .post(&create_url)
            .json(&serde_json::json!({
                "type": "document",
                "id": fs_root_id,
                "content_type": "application/json"
            }))
            .send()
            .await?;
        if !create_resp.status().is_success() {
            let status = create_resp.status();
            let body = create_resp.text().await.unwrap_or_default();
            return Err(format!("Failed to create fs-root document: {} - {}", status, body).into());
        }
    }
    info!("Connected to fs-root document: {}", fs_root_id);

    // Check if server has existing schema FIRST (needed for server strategy)
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(&fs_root_id));
    let head_resp = client.get(&head_url).send().await?;
    let server_has_content = if head_resp.status().is_success() {
        let head: HeadResponse = head_resp.json().await?;
        !head.content.is_empty() && head.content != "{}"
    } else {
        false
    };

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

    // Scan directory and generate FS schema
    info!("Scanning directory...");
    let schema = scan_directory(&directory, &options).map_err(|e| format!("Scan error: {}", e))?;
    let schema_json = schema_to_json(&schema)?;
    info!(
        "Directory scanned: generated {} bytes of schema JSON",
        schema_json.len()
    );

    // Decide whether to push local schema based on strategy
    let should_push_schema = match initial_sync_strategy.as_str() {
        "local" => true,
        "server" => !server_has_content,
        "skip" => !server_has_content,
        _ => !server_has_content,
    };

    if should_push_schema {
        info!("Pushing filesystem schema to server...");
        push_schema_to_server(&client, &server, &fs_root_id, &schema_json).await?;
        info!("Schema pushed successfully");

        if let Err(e) = write_schema_file(&directory, &schema_json).await {
            warn!("Failed to write schema file: {}", e);
        }
    } else {
        info!(
            "Server already has content, skipping schema push (strategy={})",
            initial_sync_strategy
        );

        let head_url = format!("{}/docs/{}/head", server, encode_node_id(&fs_root_id));
        if let Ok(resp) = client.get(&head_url).send().await {
            if resp.status().is_success() {
                if let Ok(head) = resp.json::<HeadResponse>().await {
                    if let Err(e) = write_schema_file(&directory, &head.content).await {
                        warn!("Failed to write schema file: {}", e);
                    }
                }
            }
        }
    }

    // Sync file contents (reusing logic from run_directory_mode)
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

    for file in &files {
        // When use_paths=true, use relative path directly for /files/* endpoints
        // When use_paths=false, use UUID from schema if available, otherwise derive as fs_root_id:path
        let identifier = if use_paths {
            file.relative_path.clone()
        } else if let Some(uuid) = uuid_map.get(&file.relative_path) {
            info!("Using UUID for {}: {}", file.relative_path, uuid);
            uuid.clone()
        } else {
            let derived = format!("{}:{}", fs_root_id, file.relative_path);
            warn!(
                "No UUID found for {}, using derived ID: {}",
                file.relative_path, derived
            );
            derived
        };
        info!("Syncing file: {} -> {}", file.relative_path, identifier);

        let file_path = directory.join(&file.relative_path);

        let state = {
            let states = file_states.read().await;
            if let Some(existing) = states.get(&file.relative_path) {
                existing.state.clone()
            } else {
                Arc::new(RwLock::new(SyncState::new()))
            }
        };

        let file_head_url = build_head_url(&server, &identifier, use_paths);
        let file_head_resp = client.get(&file_head_url).send().await;

        if let Ok(resp) = file_head_resp {
            if resp.status().is_success() {
                let head: HeadResponse = resp.json().await?;
                if head.content.is_empty() || initial_sync_strategy == "local" {
                    info!(
                        "Pushing initial content for: {} ({} bytes)",
                        identifier,
                        file.content.len()
                    );
                    if !file.is_binary && file.content_type.starts_with("application/json") {
                        push_json_content(
                            &client,
                            &server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    } else {
                        push_file_content(
                            &client,
                            &server,
                            &identifier,
                            &file.content,
                            &state,
                            use_paths,
                        )
                        .await?;
                    }
                } else {
                    if initial_sync_strategy == "server" {
                        if file.is_binary {
                            use base64::{engine::general_purpose::STANDARD, Engine};
                            if let Ok(decoded) = STANDARD.decode(&head.content) {
                                tokio::fs::write(&file_path, &decoded).await?;
                            }
                        } else {
                            tokio::fs::write(&file_path, &head.content).await?;
                        }
                    }
                    let mut s = state.write().await;
                    s.last_written_cid = head.cid;
                    if initial_sync_strategy == "skip" {
                        s.last_written_content = file.content.clone();
                    } else {
                        s.last_written_content = head.content;
                    }
                }
            } else {
                info!("Node not ready, will push with retries for: {}", identifier);
                if !file.is_binary && file.content_type.starts_with("application/json") {
                    push_json_content(
                        &client,
                        &server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                } else {
                    push_file_content(
                        &client,
                        &server,
                        &identifier,
                        &file.content,
                        &state,
                        use_paths,
                    )
                    .await?;
                }
            }
        }

        {
            // For binary files, decode base64 to get raw bytes for consistent hashing
            // New file detection hashes raw bytes, so we must do the same here
            let content_hash = if file.is_binary {
                use base64::{engine::general_purpose::STANDARD, Engine};
                match STANDARD.decode(&file.content) {
                    Ok(raw_bytes) => compute_content_hash(&raw_bytes),
                    Err(_) => compute_content_hash(file.content.as_bytes()),
                }
            } else {
                compute_content_hash(file.content.as_bytes())
            };
            let mut states = file_states.write().await;
            states.insert(
                file.relative_path.clone(),
                FileSyncState {
                    relative_path: file.relative_path.clone(),
                    identifier: identifier.clone(),
                    state,
                    task_handles: Vec::new(),
                    use_paths,
                    content_hash: Some(content_hash),
                },
            );
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
                        debug!("Directory event: file created: {}", path.display());

                        // Calculate relative path
                        let relative_path = match path.strip_prefix(&directory) {
                            Ok(rel) => normalize_path(&rel.to_string_lossy()),
                            Err(_) => continue,
                        };

                        // Check if file matches ignore patterns
                        let file_name = path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_default();
                        let should_ignore = options.ignore_patterns.iter().any(|pattern| {
                            if pattern == &file_name {
                                true
                            } else if pattern.contains('*') {
                                let parts: Vec<&str> = pattern.split('*').collect();
                                if parts.len() == 2 {
                                    file_name.starts_with(parts[0]) && file_name.ends_with(parts[1])
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        });
                        if should_ignore {
                            debug!(
                                "Ignoring new file (matches ignore pattern): {}",
                                relative_path
                            );
                            continue;
                        }

                        // Skip hidden files unless configured to include them
                        if !options.include_hidden && file_name.starts_with('.') {
                            debug!("Ignoring hidden file: {}", relative_path);
                            continue;
                        }

                        // Skip files with disallowed extensions
                        if !is_allowed_extension(&path) {
                            debug!("Ignoring file with disallowed extension: {}", relative_path);
                            continue;
                        }

                        // Check if we already have sync tasks for this file
                        let already_tracked = {
                            let states = file_states.read().await;
                            states.contains_key(&relative_path)
                        };

                        if !already_tracked && path.is_file() {
                            // Read file content first (needed for both hash check and push)
                            let raw_content = match tokio::fs::read(&path).await {
                                Ok(c) => c,
                                Err(e) => {
                                    warn!("Failed to read new file {}: {}", path.display(), e);
                                    continue;
                                }
                            };

                            // Compute content hash and check for matching file (fork detection)
                            let content_hash = compute_content_hash(&raw_content);
                            let matching_source = {
                                let states = file_states.read().await;
                                states
                                    .iter()
                                    .find(|(_, state)| {
                                        state.content_hash.as_ref() == Some(&content_hash)
                                    })
                                    .map(|(_, state)| state.identifier.clone())
                            };

                            // Initial identifier - may be updated after schema push if not using paths
                            let mut identifier = if use_paths {
                                relative_path.clone()
                            } else {
                                format!("{}:{}", fs_root_id, relative_path)
                            };
                            let state = Arc::new(RwLock::new(SyncState::new()));

                            // If content matches an existing file, try to fork it
                            let forked_successfully = if let Some(source_id) = matching_source {
                                info!(
                                    "New file {} has identical content to {}, forking...",
                                    relative_path, source_id
                                );

                                // Convert Result to avoid holding Box<dyn Error> across await
                                let fork_result = fork_node(&client, &server, &source_id, None)
                                    .await
                                    .map_err(|e| e.to_string());

                                match fork_result {
                                    Ok(forked_id) => {
                                        info!(
                                            "Forked {} -> {} for new file {}",
                                            source_id, forked_id, relative_path
                                        );
                                        // Update schema to point to forked node
                                        if let Ok(schema) = scan_directory(&directory, &options) {
                                            if let Ok(json) = schema_to_json(&schema) {
                                                if let Err(e) = push_schema_to_server(
                                                    &client,
                                                    &server,
                                                    &fs_root_id,
                                                    &json,
                                                )
                                                .await
                                                {
                                                    warn!("Failed to push updated schema: {}", e);
                                                }
                                            }
                                        }
                                        // Use the forked ID as the identifier
                                        identifier = forked_id;
                                        true
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Fork failed for {}, falling back to new document: {}",
                                            relative_path, e
                                        );
                                        false
                                    }
                                }
                            } else {
                                false
                            };

                            // If fork didn't succeed (no match or fork failed), create document normally
                            if !forked_successfully {
                                // Push updated schema FIRST so server reconciler creates the node
                                if let Ok(schema) = scan_directory(&directory, &options) {
                                    if let Ok(json) = schema_to_json(&schema) {
                                        if let Err(e) = push_schema_to_server(
                                            &client,
                                            &server,
                                            &fs_root_id,
                                            &json,
                                        )
                                        .await
                                        {
                                            warn!("Failed to push updated schema: {}", e);
                                        }
                                    }
                                }

                                // Wait briefly for server to reconcile and create the node
                                sleep(Duration::from_millis(100)).await;

                                // When not using paths, fetch the UUID from the updated schema
                                // The reconciler assigns UUIDs to new entries, so we need to look them up
                                if !use_paths {
                                    if let Some(uuid) = fetch_node_id_from_schema(
                                        &client,
                                        &server,
                                        &fs_root_id,
                                        &relative_path,
                                    )
                                    .await
                                    {
                                        info!(
                                            "Resolved UUID for {}: {} -> {}",
                                            relative_path, identifier, uuid
                                        );
                                        identifier = uuid;
                                    } else {
                                        warn!(
                                            "Could not resolve UUID for {}, using derived ID: {}",
                                            relative_path, identifier
                                        );
                                    }
                                }

                                // Now push initial content (handle binary files)
                                let content_info = detect_from_path(&path);
                                let is_binary =
                                    content_info.is_binary || is_binary_content(&raw_content);

                                let content = if is_binary {
                                    use base64::{engine::general_purpose::STANDARD, Engine};
                                    STANDARD.encode(&raw_content)
                                } else {
                                    String::from_utf8_lossy(&raw_content).to_string()
                                };

                                if let Err(e) =
                                    if !is_binary && content_info.mime_type == "application/json" {
                                        push_json_content(
                                            &client,
                                            &server,
                                            &identifier,
                                            &content,
                                            &state,
                                            use_paths,
                                        )
                                        .await
                                    } else {
                                        push_file_content(
                                            &client,
                                            &server,
                                            &identifier,
                                            &content,
                                            &state,
                                            use_paths,
                                        )
                                        .await
                                    }
                                {
                                    warn!("Failed to push new file content: {}", e);
                                }
                            }

                            // Spawn sync tasks for the new file
                            let task_handles = spawn_file_sync_tasks(
                                client.clone(),
                                server.clone(),
                                identifier.clone(),
                                path.clone(),
                                state.clone(),
                                use_paths,
                            );

                            // Add to file_states with task handles
                            {
                                let mut states = file_states.write().await;
                                states.insert(
                                    relative_path.clone(),
                                    FileSyncState {
                                        relative_path: relative_path.clone(),
                                        identifier,
                                        state,
                                        task_handles,
                                        use_paths,
                                        content_hash: Some(content_hash),
                                    },
                                );
                            }

                            info!("Started sync for new local file: {}", relative_path);
                        }
                    }
                    DirEvent::Modified(path) => {
                        debug!("Directory event: file modified: {}", path.display());
                        // Modified files are handled by per-file watchers
                        // Just update schema in case metadata changed
                        if let Ok(schema) = scan_directory(&directory, &options) {
                            if let Ok(json) = schema_to_json(&schema) {
                                if let Err(e) =
                                    push_schema_to_server(&client, &server, &fs_root_id, &json)
                                        .await
                                {
                                    warn!("Failed to push updated schema: {}", e);
                                }
                            }
                        }
                    }
                    DirEvent::Deleted(path) => {
                        debug!("Directory event: file deleted: {}", path.display());

                        let relative_path = match path.strip_prefix(&directory) {
                            Ok(rel) => normalize_path(&rel.to_string_lossy()),
                            Err(_) => {
                                warn!("Could not strip prefix from deleted path");
                                continue;
                            }
                        };

                        // Stop sync tasks for this file and remove from file_states
                        {
                            let mut states = file_states.write().await;
                            if let Some(file_state) = states.remove(&relative_path) {
                                info!("Stopping sync tasks for deleted file: {}", relative_path);
                                for handle in file_state.task_handles {
                                    handle.abort();
                                }
                            }
                        }

                        // Rescan and push updated schema
                        if let Ok(schema) = scan_directory(&directory, &options) {
                            if let Ok(json) = schema_to_json(&schema) {
                                if let Err(e) =
                                    push_schema_to_server(&client, &server, &fs_root_id, &json)
                                        .await
                                {
                                    warn!("Failed to push updated schema: {}", e);
                                }
                            }
                        }
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

/// SSE task for directory-level events (watching fs-root)
async fn directory_sse_task(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    file_states: Arc<RwLock<HashMap<String, FileSyncState>>>,
    use_paths: bool,
) {
    // fs-root schema subscription always uses ID-based API
    let sse_url = format!("{}/sse/docs/{}", server, encode_node_id(&fs_root_id));

    loop {
        info!("Connecting to fs-root SSE: {}", sse_url);

        let request_builder = client.get(&sse_url);
        let mut es = match EventSource::new(request_builder) {
            Ok(es) => es,
            Err(e) => {
                error!("Failed to create fs-root EventSource: {}", e);
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        while let Some(event) = es.next().await {
            match event {
                Ok(SseEvent::Open) => {
                    info!("fs-root SSE connection opened");
                }
                Ok(SseEvent::Message(msg)) => {
                    debug!("fs-root SSE event: {} - {}", msg.event, msg.data);

                    match msg.event.as_str() {
                        "connected" => {
                            info!("fs-root SSE connected");
                        }
                        "edit" => {
                            // Schema changed on server, sync new files to local
                            // Spawn tasks for new files discovered during runtime
                            if let Err(e) = handle_schema_change(
                                &client,
                                &server,
                                &fs_root_id,
                                &directory,
                                &file_states,
                                true, // spawn_tasks: true for runtime schema changes
                                use_paths,
                            )
                            .await
                            {
                                warn!("Failed to handle schema change: {}", e);
                            }
                        }
                        "closed" => {
                            warn!("fs-root SSE: Target node shut down");
                            break;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!("fs-root SSE error: {}", e);
                    break;
                }
            }
        }

        warn!("fs-root SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Handle a schema change from the server - create new local files
async fn handle_schema_change(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    directory: &std::path::Path,
    file_states: &Arc<RwLock<HashMap<String, FileSyncState>>>,
    spawn_tasks: bool,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fetch current schema from server (fs-root schema always uses ID-based API)
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch fs-root HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;
    if head.content.is_empty() {
        return Ok(());
    }

    // Parse and validate schema BEFORE writing to disk
    // This prevents corrupting the local schema file with invalid/empty server content
    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse fs-root schema: {}", e);
            return Ok(());
        }
    };

    // Validate that schema has a populated root with entries - don't overwrite with empty/minimal schemas
    let has_valid_root = match &schema.root {
        Some(Entry::Dir(dir)) => dir.entries.is_some() || dir.node_id.is_some(),
        _ => false,
    };
    if !has_valid_root {
        warn!("Server returned schema without valid root (missing entries or node_id), not overwriting local schema");
        return Ok(());
    }

    // Only write valid schema to local .commonplace.json file
    if let Err(e) = write_schema_file(directory, &head.content).await {
        warn!("Failed to write schema file: {}", e);
    }

    // Collect all paths from schema (with explicit node_id if present)
    let mut schema_paths: Vec<(String, Option<String>)> = Vec::new();
    if let Some(ref root) = schema.root {
        collect_paths_from_entry(root, "", &mut schema_paths);
    }

    // Check for new paths not in our state
    let known_paths: Vec<String> = {
        let states = file_states.read().await;
        states.keys().cloned().collect()
    };

    for (path, explicit_node_id) in &schema_paths {
        if !known_paths.contains(path) {
            // New file from server - create local file and fetch content
            let file_path = directory.join(path);

            // Skip files with disallowed extensions
            if !is_allowed_extension(&file_path) {
                debug!("Ignoring server file with disallowed extension: {}", path);
                continue;
            }

            // When use_paths=true, use path for /files/* API
            // When use_paths=false, use explicit node_id or derive as fs_root:path for /docs/* API
            let identifier = if use_paths {
                path.clone()
            } else {
                explicit_node_id
                    .clone()
                    .unwrap_or_else(|| format!("{}:{}", fs_root_id, path))
            };

            info!(
                "Server created new file: {} -> {}",
                path,
                file_path.display()
            );

            // Create parent directories if needed
            if let Some(parent) = file_path.parent() {
                if !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }

            // Fetch content from server
            let file_head_url = build_head_url(server, &identifier, use_paths);
            if let Ok(resp) = client.get(&file_head_url).send().await {
                if resp.status().is_success() {
                    if let Ok(file_head) = resp.json::<HeadResponse>().await {
                        // Detect if file is binary and decode base64 if needed
                        // Use both extension-based detection AND try decoding as base64
                        // to handle files that were uploaded as binary via content sniffing
                        // Track actual bytes written for consistent hash computation
                        use base64::{engine::general_purpose::STANDARD, Engine};
                        let content_info = detect_from_path(&file_path);
                        let bytes_written: Vec<u8> = if content_info.is_binary {
                            // Extension indicates binary - decode base64
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) => decoded,
                                Err(e) => {
                                    warn!("Failed to decode binary content: {}", e);
                                    file_head.content.as_bytes().to_vec()
                                }
                            }
                        } else {
                            // Extension says text, but try decoding as base64 in case
                            // this was a binary file detected by content sniffing
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) if is_binary_content(&decoded) => {
                                    // Successfully decoded and content is binary
                                    decoded
                                }
                                _ => {
                                    // Not base64 or not binary - write as text
                                    file_head.content.as_bytes().to_vec()
                                }
                            }
                        };
                        tokio::fs::write(&file_path, &bytes_written).await?;

                        // Hash the actual bytes written to disk for consistent fork detection
                        let content_hash = compute_content_hash(&bytes_written);

                        // Add to file states
                        let state = Arc::new(RwLock::new(SyncState {
                            last_written_cid: file_head.cid.clone(),
                            last_written_content: file_head.content,
                            current_write_id: 0,
                            pending_write: None,
                            needs_head_refresh: false,
                            state_file: None, // Directory mode doesn't use state files yet
                            state_file_path: None,
                        }));

                        info!("Created local file: {}", file_path.display());

                        // Spawn sync tasks for the new file (only if requested)
                        let task_handles = if spawn_tasks {
                            spawn_file_sync_tasks(
                                client.clone(),
                                server.to_string(),
                                identifier.clone(),
                                file_path.clone(),
                                state.clone(),
                                use_paths,
                            )
                        } else {
                            Vec::new()
                        };
                        let mut states = file_states.write().await;
                        states.insert(
                            path.clone(),
                            FileSyncState {
                                relative_path: path.clone(),
                                identifier,
                                state,
                                task_handles,
                                use_paths,
                                content_hash: Some(content_hash),
                            },
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

/// Recursively collect file paths from an entry, including explicit node_id if present
/// Returns Vec<(path, explicit_node_id)> where explicit_node_id is Some if DocEntry has node_id
fn collect_paths_from_entry(
    entry: &Entry,
    prefix: &str,
    paths: &mut Vec<(String, Option<String>)>,
) {
    match entry {
        Entry::Dir(dir) => {
            if let Some(ref entries) = dir.entries {
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_paths_from_entry(child, &child_path, paths);
                }
            }
        }
        Entry::Doc(doc) => {
            paths.push((prefix.to_string(), doc.node_id.clone()));
        }
    }
}

/// Fetch the node_id (UUID) for a file path from the server's schema.
///
/// After pushing a schema update, the server's reconciler creates documents with UUIDs.
/// This function fetches the updated schema and looks up the UUID for the given path.
/// Returns None if the path is not found or if the node_id is not set.
async fn fetch_node_id_from_schema(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    relative_path: &str,
) -> Option<String> {
    // Build the full UUID map recursively (follows node-backed directories)
    let uuid_map = build_uuid_map_recursive(client, server, fs_root_id).await;
    uuid_map.get(relative_path).cloned()
}

/// Recursively build a map of relative paths to UUIDs by fetching all schemas.
///
/// This function follows node-backed directories and fetches their schemas
/// to build a complete map of all file paths to their UUIDs.
async fn build_uuid_map_recursive(
    client: &Client,
    server: &str,
    doc_id: &str,
) -> std::collections::HashMap<String, String> {
    let mut uuid_map = std::collections::HashMap::new();
    build_uuid_map_from_doc(client, server, doc_id, "", &mut uuid_map).await;
    uuid_map
}

/// Helper function to recursively build the UUID map from a document and its children.
#[async_recursion::async_recursion]
async fn build_uuid_map_from_doc(
    client: &Client,
    server: &str,
    doc_id: &str,
    path_prefix: &str,
    uuid_map: &mut std::collections::HashMap<String, String>,
) {
    // Fetch the schema from this document
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(doc_id));
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to fetch schema for {}: {}", doc_id, e);
            return;
        }
    };

    if !resp.status().is_success() {
        warn!(
            "Failed to fetch schema: {} (status {})",
            doc_id,
            resp.status()
        );
        return;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            warn!("Failed to parse schema response for {}: {}", doc_id, e);
            return;
        }
    };

    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            debug!("Document {} is not a schema ({}), skipping", doc_id, e);
            return;
        }
    };

    // Traverse the schema and collect UUIDs
    if let Some(ref root) = schema.root {
        collect_paths_with_node_backed_dirs(client, server, root, path_prefix, uuid_map).await;
    }
}

/// Recursively collect paths from an entry, following node-backed directories.
#[async_recursion::async_recursion]
async fn collect_paths_with_node_backed_dirs(
    client: &Client,
    server: &str,
    entry: &Entry,
    prefix: &str,
    uuid_map: &mut std::collections::HashMap<String, String>,
) {
    match entry {
        Entry::Dir(dir) => {
            // If this is a node-backed directory (entries: null, node_id: Some),
            // fetch its schema and continue recursively
            if dir.entries.is_none() {
                if let Some(ref node_id) = dir.node_id {
                    // This is a node-backed directory - fetch its schema
                    build_uuid_map_from_doc(client, server, node_id, prefix, uuid_map).await;
                }
            } else if let Some(ref entries) = dir.entries {
                // Inline directory - traverse its entries
                for (name, child) in entries {
                    let child_path = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collect_paths_with_node_backed_dirs(
                        client,
                        server,
                        child,
                        &child_path,
                        uuid_map,
                    )
                    .await;
                }
            }
        }
        Entry::Doc(doc) => {
            // This is a file - add it to the map if it has a node_id
            if let Some(ref node_id) = doc.node_id {
                debug!("Found UUID: {} -> {}", prefix, node_id);
                uuid_map.insert(prefix.to_string(), node_id.clone());
            }
        }
    }
}

/// Perform initial sync: fetch HEAD and write to local file
async fn initial_sync(
    client: &Client,
    server: &str,
    node_id: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(node_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to get HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;

    // Write content to file, handling binary content (base64-encoded on server)
    // Track the bytes we actually write to disk for proper hash computation
    use base64::{engine::general_purpose::STANDARD, Engine};
    let content_info = detect_from_path(file_path);
    let bytes_written: Vec<u8> = if content_info.is_binary {
        // Extension indicates binary - decode base64
        match STANDARD.decode(&head.content) {
            Ok(decoded) => {
                tokio::fs::write(file_path, &decoded).await?;
                decoded
            }
            Err(e) => {
                warn!("Failed to decode binary content: {}", e);
                let bytes = head.content.as_bytes().to_vec();
                tokio::fs::write(file_path, &bytes).await?;
                bytes
            }
        }
    } else {
        // Extension says text, but try decoding as base64 in case
        // this was a binary file detected by content sniffing
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                tokio::fs::write(file_path, &decoded).await?;
                decoded
            }
            _ => {
                let bytes = head.content.as_bytes().to_vec();
                tokio::fs::write(file_path, &bytes).await?;
                bytes
            }
        }
    };

    // Update state and persist to state file
    {
        let mut s = state.write().await;
        s.last_written_cid = head.cid.clone();
        s.last_written_content = head.content.clone();

        // Save to state file for offline change detection
        // Use the actual bytes written to disk, not the server response
        if let Some(ref cid) = head.cid {
            let content_hash = compute_content_hash(&bytes_written);
            let file_name = file_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            s.mark_synced(cid, &content_hash, &file_name).await;
        }
    }

    match &head.cid {
        Some(cid) => info!(
            "Initial sync complete: {} bytes at {}",
            head.content.len(),
            cid
        ),
        None => info!("Initial sync complete: empty document (no commits yet)"),
    }

    Ok(())
}
