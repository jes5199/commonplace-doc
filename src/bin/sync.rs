//! Commonplace Sync - Local file synchronization with a Commonplace server
//!
//! This binary syncs a local file or directory with a server-side document node.
//! It watches both directions: local changes push to server,
//! server changes update local files.

use clap::Parser;
use commonplace_doc::fs::{Entry, FsSchema};
use commonplace_doc::sync::state_file::{compute_content_hash, SyncStateFile};
use commonplace_doc::sync::{
    build_edit_url, build_fork_url, build_head_url, build_replace_url, build_sse_url,
    create_yjs_json_update, create_yjs_text_update, detect_from_path, encode_node_id,
    is_allowed_extension, is_binary_content, normalize_path, scan_directory,
    scan_directory_with_contents, schema_to_json, DirEvent, EditEventData, EditRequest,
    EditResponse, FileEvent, ForkResponse, HeadResponse, ReplaceResponse, ScanOptions,
};
use futures::StreamExt;
use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
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

    /// Node ID to sync with (also reads from COMMONPLACE_NODE env var; optional if --fork-from is provided)
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
}

/// Pending write from server - used for barrier-based echo detection
#[derive(Debug, Clone)]
struct PendingWrite {
    /// Unique token for this write operation
    write_id: u64,
    /// Content being written
    content: String,
    /// CID of the commit being written
    cid: Option<String>,
    /// When this write started (for timeout detection)
    started_at: std::time::Instant,
}

/// Shared state between file watcher and SSE tasks
#[derive(Debug)]
struct SyncState {
    /// CID of the commit we last wrote to the local file
    last_written_cid: Option<String>,
    /// Content we last wrote to the local file (for echo detection)
    last_written_content: String,
    /// Monotonic counter for write operations
    current_write_id: u64,
    /// Currently pending server write (barrier is "up" when Some)
    pending_write: Option<PendingWrite>,
    /// Flag indicating a server edit was skipped while barrier was up
    /// upload_task should refresh HEAD after clearing barrier if this is true
    needs_head_refresh: bool,
    /// Persistent state file for offline change detection (file mode only)
    state_file: Option<SyncStateFile>,
    /// Path to save the state file
    state_file_path: Option<PathBuf>,
}

impl SyncState {
    fn new() -> Self {
        Self {
            last_written_cid: None,
            last_written_content: String::new(),
            current_write_id: 0,
            pending_write: None,
            needs_head_refresh: false,
            state_file: None,
            state_file_path: None,
        }
    }

    fn with_state_file(state_file: SyncStateFile, state_file_path: PathBuf) -> Self {
        Self {
            last_written_cid: state_file.last_synced_cid.clone(),
            last_written_content: String::new(),
            current_write_id: 0,
            pending_write: None,
            needs_head_refresh: false,
            state_file: Some(state_file),
            state_file_path: Some(state_file_path),
        }
    }

    /// Update state file after successful sync and save to disk
    async fn mark_synced(&mut self, cid: &str, content_hash: &str, relative_path: &str) {
        if let Some(ref mut state_file) = self.state_file {
            state_file.mark_synced(cid.to_string());
            state_file.update_file(relative_path, content_hash.to_string());

            // Save to disk
            if let Some(ref path) = self.state_file_path {
                if let Err(e) = state_file.save(path).await {
                    warn!("Failed to save state file: {}", e);
                }
            }
        }
    }
}

/// Filename for the local schema JSON file written during directory sync
const SCHEMA_FILENAME: &str = ".commonplace.json";

/// Fork a source node and return the new node ID
async fn fork_node(
    client: &Client,
    server: &str,
    source_node: &str,
    at_commit: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let fork_url = build_fork_url(server, source_node, at_commit);

    let resp = client.post(&fork_url).send().await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Fork failed: {} - {}", status, body).into());
    }

    let fork_response: ForkResponse = resp.json().await?;
    info!(
        "Forked node {} -> {} (at commit {})",
        source_node,
        fork_response.id,
        &fork_response.head[..8.min(fork_response.head.len())]
    );

    Ok(fork_response.id)
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

    // Validate that either --file or --directory is provided
    if args.file.is_none() && args.directory.is_none() {
        error!("Either --file or --directory must be provided");
        return ExitCode::from(1);
    }

    // Exec mode requires --directory (doesn't make sense with single file)
    if args.exec.is_some() && args.directory.is_none() {
        error!("--exec requires --directory (exec mode doesn't support single file sync)");
        return ExitCode::from(1);
    }

    // Create HTTP client
    let client = Client::new();

    // Determine the node ID to sync with
    let node_id = match (&args.node, &args.fork_from) {
        (Some(node), None) => {
            // Direct sync to existing node
            node.clone()
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
        (Some(node), Some(source)) => {
            // Both provided - use --node but warn
            warn!(
                "--node and --fork-from both provided; using --node={} (ignoring --fork-from={})",
                node, source
            );
            node.clone()
        }
        (None, None) => {
            error!("Either --node or --fork-from must be provided");
            return ExitCode::from(1);
        }
    };

    // Route to appropriate mode
    let result = if let Some(directory) = args.directory {
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

    // Sync each file (file_states was created earlier for server-first pull)
    for file in &files {
        // When use_paths=true, use relative path directly for /files/* endpoints
        // When use_paths=false, derive node ID as fs_root_id:path for /docs/* endpoints
        let identifier = if use_paths {
            file.relative_path.clone()
        } else {
            format!("{}:{}", fs_root_id, file.relative_path)
        };
        info!("Syncing file: {} -> {}", file.relative_path, identifier);

        // Get or create the node (server's reconciler should have created it)
        // Wait a moment for reconciler
        sleep(Duration::from_millis(100)).await;

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
            let mut states = file_states.write().await;
            states.insert(
                file.relative_path.clone(),
                FileSyncState {
                    relative_path: file.relative_path.clone(),
                    identifier: identifier.clone(),
                    state,
                    task_handles: Vec::new(), // Will be populated after initial sync
                    use_paths,
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
                            // New file - push schema first so server creates the node
                            let identifier = if use_paths {
                                relative_path.clone()
                            } else {
                                format!("{}:{}", fs_root_id, relative_path)
                            };
                            let state = Arc::new(RwLock::new(SyncState::new()));

                            // Push updated schema FIRST so server reconciler creates the node
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

                            // Wait briefly for server to reconcile and create the node
                            sleep(Duration::from_millis(100)).await;

                            // Now push initial content (handle binary files)
                            if let Ok(raw_content) = tokio::fs::read(&path).await {
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

    for file in &files {
        let identifier = if use_paths {
            file.relative_path.clone()
        } else {
            format!("{}:{}", fs_root_id, file.relative_path)
        };
        info!("Syncing file: {} -> {}", file.relative_path, identifier);

        sleep(Duration::from_millis(100)).await;

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
            let mut states = file_states.write().await;
            states.insert(
                file.relative_path.clone(),
                FileSyncState {
                    relative_path: file.relative_path.clone(),
                    identifier: identifier.clone(),
                    state,
                    task_handles: Vec::new(),
                    use_paths,
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
                            // New file - push schema first so server creates the node
                            let identifier = if use_paths {
                                relative_path.clone()
                            } else {
                                format!("{}:{}", fs_root_id, relative_path)
                            };
                            let state = Arc::new(RwLock::new(SyncState::new()));

                            // Push updated schema FIRST so server reconciler creates the node
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

                            // Wait briefly for server to reconcile and create the node
                            sleep(Duration::from_millis(100)).await;

                            // Now push initial content (handle binary files)
                            if let Ok(raw_content) = tokio::fs::read(&path).await {
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

    // Spawn the child process in the synced directory
    let mut child = tokio::process::Command::new(&program)
        .args(&args)
        .current_dir(&directory)
        // Inherit stdin/stdout/stderr for interactive programs
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        // Pass through key environment variables
        .env("COMMONPLACE_SERVER", &server)
        .env("COMMONPLACE_NODE", &fs_root_id)
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

/// Push filesystem schema JSON to the fs-root node
///
/// Uses Y.Map updates for proper CRDT support. The edit endpoint is always used
/// because the replace endpoint only supports text content type.
///
/// This function properly handles deletions by:
/// 1. Fetching the current server content
/// 2. Comparing old keys vs new keys
/// 3. Creating an update that removes missing keys and inserts new ones
async fn push_schema_to_server(
    client: &Client,
    server: &str,
    fs_root_id: &str,
    schema_json: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // First fetch current server content and state to detect deletions
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(fs_root_id));
    let head_resp = client.get(&head_url).send().await?;
    let (_old_content, base_state) = if head_resp.status().is_success() {
        let head: HeadResponse = head_resp.json().await?;
        (Some(head.content), head.state)
    } else {
        (None, None)
    };

    // Create an update that properly handles deletions (using server state for CRDT consistency)
    let update = create_yjs_json_update(schema_json, base_state.as_deref())
        .map_err(|e| format!("Failed to create JSON update: {}", e))?;
    let edit_url = format!("{}/docs/{}/edit", server, encode_node_id(fs_root_id));
    let edit_req = EditRequest {
        update,
        author: Some("sync-client".to_string()),
        message: Some("Update filesystem schema".to_string()),
    };

    let resp = client.post(&edit_url).json(&edit_req).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to push schema: {} - {}", status, body).into());
    }

    Ok(())
}

/// Push JSON content to a node using Y.Map/Y.Array updates.
async fn push_json_content(
    client: &Client,
    server: &str,
    identifier: &str,
    content: &str,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let head_url = build_head_url(server, identifier, use_paths);
    let edit_url = build_edit_url(server, identifier, use_paths);
    let mut attempts = 0;
    let max_attempts = 30; // 3 seconds max wait

    loop {
        let head_resp = client.get(&head_url).send().await?;
        if head_resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let base_state = if head_resp.status().is_success() {
            let head: HeadResponse = head_resp.json().await?;
            head.state
        } else {
            None
        };

        let update = create_yjs_json_update(content, base_state.as_deref())?;
        let edit_req = EditRequest {
            update,
            author: Some("sync-client".to_string()),
            message: Some("Sync JSON content".to_string()),
        };

        let resp = client.post(&edit_url).json(&edit_req).send().await?;
        if resp.status().is_success() {
            let result: EditResponse = resp.json().await?;
            let mut s = state.write().await;
            s.last_written_cid = Some(result.cid);
            s.last_written_content = content.to_string();
            return Ok(());
        }

        if resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to push JSON content: {}", body).into());
    }
}

/// Push file content to a node
async fn push_file_content(
    client: &Client,
    server: &str,
    identifier: &str,
    content: &str,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // First check if there's existing content
    let head_url = build_head_url(server, identifier, use_paths);
    let head_resp = client.get(&head_url).send().await;

    match head_resp {
        Ok(resp) if resp.status().is_success() => {
            let head: HeadResponse = resp.json().await?;
            if let Some(parent_cid) = head.cid {
                // Use replace endpoint
                let replace_url = build_replace_url(server, identifier, &parent_cid, use_paths);
                let resp = client
                    .post(&replace_url)
                    .header("content-type", "text/plain")
                    .body(content.to_string())
                    .send()
                    .await?;
                if resp.status().is_success() {
                    let result: ReplaceResponse = resp.json().await?;
                    let mut s = state.write().await;
                    s.last_written_cid = Some(result.cid);
                    s.last_written_content = content.to_string();
                }
                return Ok(());
            }
        }
        _ => {}
    }

    // No existing content, use edit endpoint with retry for node creation
    debug!("Using edit endpoint for initial content: {}", identifier);
    let update = create_yjs_text_update(content);
    let edit_url = build_edit_url(server, identifier, use_paths);
    let edit_req = EditRequest {
        update,
        author: Some("sync-client".to_string()),
        message: Some("Initial file content".to_string()),
    };

    // Retry loop: wait for node to be created by reconciler
    // The reconciler processes schema changes asynchronously, so we need to wait
    let mut attempts = 0;
    let max_attempts = 30; // 3 seconds max wait
    loop {
        let resp = client.post(&edit_url).json(&edit_req).send().await?;
        if resp.status().is_success() {
            let result: EditResponse = resp.json().await?;
            let mut s = state.write().await;
            s.last_written_cid = Some(result.cid);
            s.last_written_content = content.to_string();
            info!("Successfully pushed content for: {}", identifier);
            return Ok(());
        } else if resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
            // Node not created yet by reconciler, wait and retry
            attempts += 1;
            info!(
                "Identifier {} not found, waiting for reconciler (attempt {}/{})",
                identifier, attempts, max_attempts
            );
            sleep(Duration::from_millis(100)).await;
        } else {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to push content for {}: {}", identifier, body);
            return Ok(());
        }
    }
}

/// Task that watches a directory for create/delete events
async fn directory_watcher_task(
    directory: PathBuf,
    tx: mpsc::Sender<DirEvent>,
    options: ScanOptions,
) {
    let (notify_tx, mut notify_rx) = mpsc::channel::<Result<Event, notify::Error>>(100);

    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default().with_poll_interval(Duration::from_millis(500)),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create directory watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&directory, RecursiveMode::Recursive) {
        error!("Failed to watch directory {}: {}", directory.display(), e);
        return;
    }

    info!("Watching directory: {}", directory.display());

    let debounce_duration = Duration::from_millis(500);
    let mut pending_events: HashMap<PathBuf, DirEvent> = HashMap::new();
    let mut debounce_timer: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        for path in event.paths {
                            // Skip hidden files if not configured
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy();
                                if !options.include_hidden && name_str.starts_with('.') {
                                    continue;
                                }
                            }

                            // Handle rename events specially - treat as delete+create
                            // so that sync tasks are properly stopped/started
                            let dir_event = if event.kind.is_create() {
                                Some(DirEvent::Created(path.clone()))
                            } else if let EventKind::Modify(ModifyKind::Name(rename_mode)) =
                                event.kind
                            {
                                // Rename events: treat as delete (old path) or create (new path)
                                match rename_mode {
                                    RenameMode::From => {
                                        // Source of rename - file moved away
                                        debug!("Rename from (treating as delete): {}", path.display());
                                        Some(DirEvent::Deleted(path.clone()))
                                    }
                                    RenameMode::To => {
                                        // Destination of rename - file moved here
                                        debug!("Rename to (treating as create): {}", path.display());
                                        Some(DirEvent::Created(path.clone()))
                                    }
                                    RenameMode::Both | RenameMode::Any | RenameMode::Other => {
                                        // Platform couldn't determine direction - check if path exists
                                        if path.exists() {
                                            debug!(
                                                "Rename (path exists, treating as create): {}",
                                                path.display()
                                            );
                                            Some(DirEvent::Created(path.clone()))
                                        } else {
                                            debug!(
                                                "Rename (path gone, treating as delete): {}",
                                                path.display()
                                            );
                                            Some(DirEvent::Deleted(path.clone()))
                                        }
                                    }
                                }
                            } else if event.kind.is_modify() {
                                Some(DirEvent::Modified(path.clone()))
                            } else if event.kind.is_remove() {
                                Some(DirEvent::Deleted(path.clone()))
                            } else {
                                None
                            };

                            if let Some(evt) = dir_event {
                                pending_events.insert(path, evt);
                                debounce_timer = Some(tokio::time::Instant::now() + debounce_duration);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Directory watcher error: {}", e);
                    }
                }
            }
            _ = async {
                if let Some(deadline) = debounce_timer {
                    tokio::time::sleep_until(deadline).await;
                    true
                } else {
                    std::future::pending::<bool>().await
                }
            } => {
                debounce_timer = None;
                for (_, event) in pending_events.drain() {
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
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

    // Write the schema to local .commonplace.json file
    if let Err(e) = write_schema_file(directory, &head.content).await {
        warn!("Failed to write schema file: {}", e);
    }

    // Parse schema
    let schema: FsSchema = match serde_json::from_str(&head.content) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse fs-root schema: {}", e);
            return Ok(());
        }
    };

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
                        use base64::{engine::general_purpose::STANDARD, Engine};
                        let content_info = detect_from_path(&file_path);
                        let write_result = if content_info.is_binary {
                            // Extension indicates binary - decode base64
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) => tokio::fs::write(&file_path, &decoded).await,
                                Err(e) => {
                                    warn!("Failed to decode binary content: {}", e);
                                    tokio::fs::write(&file_path, &file_head.content).await
                                }
                            }
                        } else {
                            // Extension says text, but try decoding as base64 in case
                            // this was a binary file detected by content sniffing
                            match STANDARD.decode(&file_head.content) {
                                Ok(decoded) if is_binary_content(&decoded) => {
                                    // Successfully decoded and content is binary
                                    tokio::fs::write(&file_path, &decoded).await
                                }
                                _ => {
                                    // Not base64 or not binary - write as text
                                    tokio::fs::write(&file_path, &file_head.content).await
                                }
                            }
                        };
                        write_result?;

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

/// Task that watches the local file for changes
async fn file_watcher_task(file_path: PathBuf, tx: mpsc::Sender<FileEvent>) {
    // Create a channel for notify events
    let (notify_tx, mut notify_rx) = mpsc::channel::<Result<Event, notify::Error>>(100);

    // Create watcher
    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default().with_poll_interval(Duration::from_millis(100)),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create file watcher: {}", e);
            return;
        }
    };

    // Watch the file
    if let Err(e) = watcher.watch(&file_path, RecursiveMode::NonRecursive) {
        error!("Failed to watch file {}: {}", file_path.display(), e);
        return;
    }

    info!("Watching file: {}", file_path.display());

    // Debounce timer
    let debounce_duration = Duration::from_millis(100);
    let mut debounce_timer: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        // Check if this is a modify event
                        if event.kind.is_modify() {
                            debug!("File modified event: {:?}", event);
                            // Reset debounce timer
                            debounce_timer = Some(tokio::time::Instant::now() + debounce_duration);
                        }
                    }
                    Err(e) => {
                        warn!("File watcher error: {}", e);
                    }
                }
            }
            _ = async {
                if let Some(deadline) = debounce_timer {
                    tokio::time::sleep_until(deadline).await;
                    true
                } else {
                    std::future::pending::<bool>().await
                }
            } => {
                // Debounce period elapsed, send event
                debounce_timer = None;
                if tx.send(FileEvent::Modified).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Number of retries when content differs during a pending write (handles partial writes)
const BARRIER_RETRY_COUNT: u32 = 5;
/// Delay between retries when checking for stable content
const BARRIER_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Task that handles file changes and uploads to server
async fn upload_task(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    mut rx: mpsc::Receiver<FileEvent>,
    use_paths: bool,
) {
    while let Some(_event) = rx.recv().await {
        // Read current file content as bytes
        let raw_content = match tokio::fs::read(&file_path).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read file: {}", e);
                continue;
            }
        };

        // Detect if file is binary and convert accordingly
        let content_info = detect_from_path(&file_path);
        let is_binary = content_info.is_binary || is_binary_content(&raw_content);
        let is_json = !is_binary && content_info.mime_type == "application/json";

        let mut content = if is_binary {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.encode(&raw_content)
        } else {
            String::from_utf8_lossy(&raw_content).to_string()
        };

        // Check for pending write barrier and handle echo detection
        // Track whether we detected an echo and need to refresh from HEAD
        let mut echo_detected = false;
        let mut should_refresh = false;

        {
            let mut s = state.write().await;

            // Check for pending write (barrier is up)
            if let Some(pending) = s.pending_write.take() {
                // Check for timeout
                if pending.started_at.elapsed() > PENDING_WRITE_TIMEOUT {
                    warn!(
                        "Pending write timed out (id={}), clearing barrier",
                        pending.write_id
                    );
                    // The pending write timed out - we don't know if the file contains
                    // the server content (write succeeded but watcher was slow) or
                    // stale content (write failed or was interrupted).
                    //
                    // If content matches the pending write, treat as delayed echo.
                    // Otherwise, upload with old parent_cid for CRDT merge.
                    if content == pending.content {
                        debug!(
                            "Timed-out pending matches current content, treating as delayed echo"
                        );
                        s.last_written_cid = pending.cid;
                        s.last_written_content = pending.content;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                        echo_detected = true;
                    } else {
                        // Content differs - this is a user edit that slipped through,
                        // or the write failed. Upload with old parent for merge.
                        debug!(
                            "Timed-out pending differs from current content, uploading as user edit"
                        );
                        // DON'T update last_written_* - use old parent for CRDT merge
                    }
                } else if content == pending.content {
                    // Content matches what we wrote - this is our echo
                    debug!(
                        "Echo detected: content matches pending write (id={})",
                        pending.write_id
                    );
                    s.last_written_cid = pending.cid;
                    s.last_written_content = pending.content;
                    // Barrier cleared (we took it with .take())
                    should_refresh = s.needs_head_refresh;
                    s.needs_head_refresh = false;
                    echo_detected = true;
                } else {
                    // Content differs from pending - could be:
                    // a) Partial write (we're mid-write or just finished)
                    // b) User edit during our write
                    //
                    // Retry a few times to handle partial writes
                    let pending_content = pending.content.clone();
                    let pending_cid = pending.cid.clone();
                    let pending_write_id = pending.write_id;

                    // Put the pending back while we retry
                    s.pending_write = Some(pending);
                    drop(s); // Release lock during retries

                    let mut is_echo = false;
                    for i in 0..BARRIER_RETRY_COUNT {
                        sleep(BARRIER_RETRY_DELAY).await;

                        // Re-read file
                        let raw = match tokio::fs::read(&file_path).await {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Failed to re-read file during retry: {}", e);
                                break;
                            }
                        };

                        let reread = if is_binary {
                            use base64::{engine::general_purpose::STANDARD, Engine};
                            STANDARD.encode(&raw)
                        } else {
                            String::from_utf8_lossy(&raw).to_string()
                        };

                        if reread == pending_content {
                            // Content now matches - was partial write, now complete
                            debug!(
                                "Retry {}: content now matches pending write (id={})",
                                i + 1,
                                pending_write_id
                            );
                            content = reread;
                            is_echo = true;
                            break;
                        }

                        if reread != content {
                            // Content still changing, update and keep retrying
                            debug!("Retry {}: content still changing", i + 1);
                            content = reread;
                        }
                        // If reread == content and != pending, content is stable but different (user edit)
                    }

                    // Re-acquire lock and finalize
                    let mut s = state.write().await;

                    if is_echo {
                        // Our write completed after retries
                        debug!("Echo confirmed after retries (id={})", pending_write_id);
                        s.last_written_cid = pending_cid;
                        s.last_written_content = content.clone();
                        s.pending_write = None;
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                        echo_detected = true;
                    } else {
                        // User edited during our write
                        info!(
                            "User edit detected during server write (id={})",
                            pending_write_id
                        );
                        s.pending_write = None; // Clear barrier
                                                // DON'T update last_written_* - use old parent for CRDT merge
                                                // Fall through to upload with old parent_cid

                        // IMPORTANT: Also check needs_head_refresh here!
                        // If server edits were skipped while barrier was up, we need to
                        // refresh after uploading to get the merged state.
                        should_refresh = s.needs_head_refresh;
                        s.needs_head_refresh = false;
                    }
                }
            } else {
                // No barrier - normal echo detection
                // Also check needs_head_refresh in case server edit was skipped
                // due to local changes being pending
                should_refresh = s.needs_head_refresh;
                s.needs_head_refresh = false;

                if content == s.last_written_content {
                    debug!("Ignoring echo: content matches last written");
                    echo_detected = true;
                }
            }
        }

        // If echo detected, optionally refresh from HEAD then skip upload
        if echo_detected {
            if should_refresh {
                let refresh_succeeded =
                    refresh_from_head(&client, &server, &identifier, &file_path, &state, use_paths)
                        .await;
                if !refresh_succeeded {
                    // Re-set the flag so we try again next time
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            }
            continue;
        }

        if is_json {
            let json_upload_succeeded =
                match push_json_content(&client, &server, &identifier, &content, &state, use_paths)
                    .await
                {
                    Ok(_) => true,
                    Err(e) => {
                        error!("JSON upload failed: {}", e);
                        false
                    }
                };
            // Refresh from HEAD if server edits were skipped AND upload succeeded
            // IMPORTANT: Don't refresh after failed upload to avoid overwriting local edits
            if should_refresh {
                if json_upload_succeeded {
                    let refresh_succeeded = refresh_from_head(
                        &client,
                        &server,
                        &identifier,
                        &file_path,
                        &state,
                        use_paths,
                    )
                    .await;
                    if !refresh_succeeded {
                        let mut s = state.write().await;
                        s.needs_head_refresh = true;
                    }
                } else {
                    // Upload failed - re-set the flag so we try again next time
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            }
            continue;
        }

        // Get parent CID to decide which endpoint to use
        let parent_cid = {
            let s = state.read().await;
            s.last_written_cid.clone()
        };

        // Track upload success - only refresh if upload succeeded
        let mut upload_succeeded = false;

        match parent_cid {
            Some(parent) => {
                // Normal case: use replace endpoint
                let replace_url = build_replace_url(&server, &identifier, &parent, use_paths);

                match client
                    .post(&replace_url)
                    .header("content-type", "text/plain")
                    .body(content.clone())
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<ReplaceResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Uploaded: {} chars inserted, {} deleted (cid: {})",
                                        result.summary.chars_inserted,
                                        result.summary.chars_deleted,
                                        &result.cid[..8.min(result.cid.len())]
                                    );

                                    // Update state and persist to state file
                                    // Hash the raw file bytes, not the (possibly base64) content
                                    let cid = result.cid.clone();
                                    let file_bytes = tokio::fs::read(&file_path).await.ok();
                                    let content_hash = file_bytes
                                        .as_ref()
                                        .map(|b| compute_content_hash(b))
                                        .unwrap_or_default();
                                    let file_name = file_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "file".to_string());

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name).await;
                                    upload_succeeded = true;
                                }
                                Err(e) => {
                                    error!("Failed to parse replace response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            error!("Upload failed: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        error!("Upload request failed: {}", e);
                    }
                }
            }
            None => {
                // First commit: use edit endpoint with generated Yjs update
                info!("Creating initial commit...");
                let update = create_yjs_text_update(&content);
                let edit_url = build_edit_url(&server, &identifier, use_paths);
                let edit_req = EditRequest {
                    update,
                    author: Some("sync-client".to_string()),
                    message: Some("Initial sync".to_string()),
                };

                match client.post(&edit_url).json(&edit_req).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<EditResponse>().await {
                                Ok(result) => {
                                    info!(
                                        "Created initial commit: {} bytes (cid: {})",
                                        content.len(),
                                        &result.cid[..8.min(result.cid.len())]
                                    );

                                    // Update state and persist to state file
                                    // Hash the raw file bytes, not the (possibly base64) content
                                    let cid = result.cid.clone();
                                    let file_bytes = tokio::fs::read(&file_path).await.ok();
                                    let content_hash = file_bytes
                                        .as_ref()
                                        .map(|b| compute_content_hash(b))
                                        .unwrap_or_default();
                                    let file_name = file_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "file".to_string());

                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
                                    s.mark_synced(&cid, &content_hash, &file_name).await;
                                    upload_succeeded = true;
                                }
                                Err(e) => {
                                    error!("Failed to parse edit response: {}", e);
                                }
                            }
                        } else {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            error!("Initial commit failed: {} - {}", status, body);
                        }
                    }
                    Err(e) => {
                        error!("Initial commit request failed: {}", e);
                    }
                }
            }
        }

        // After successful upload, refresh from HEAD if server edits were skipped
        // IMPORTANT: Only refresh after successful upload to avoid overwriting
        // local edits when upload fails
        if should_refresh {
            if upload_succeeded {
                let refresh_succeeded =
                    refresh_from_head(&client, &server, &identifier, &file_path, &state, use_paths)
                        .await;
                if !refresh_succeeded {
                    let mut s = state.write().await;
                    s.needs_head_refresh = true;
                }
            } else {
                // Upload failed - re-set the flag so we try again next time
                let mut s = state.write().await;
                s.needs_head_refresh = true;
            }
        }
    }
}

/// Task that subscribes to SSE and handles server changes
async fn sse_task(
    client: Client,
    server: String,
    identifier: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    use_paths: bool,
) {
    let sse_url = build_sse_url(&server, &identifier, use_paths);

    loop {
        info!("Connecting to SSE: {}", sse_url);

        let request_builder = client.get(&sse_url);

        let mut es = match EventSource::new(request_builder) {
            Ok(es) => es,
            Err(e) => {
                error!("Failed to create EventSource: {}", e);
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        while let Some(event) = es.next().await {
            match event {
                Ok(SseEvent::Open) => {
                    info!("SSE connection opened");
                }
                Ok(SseEvent::Message(msg)) => {
                    debug!("SSE event: {} - {}", msg.event, msg.data);

                    match msg.event.as_str() {
                        "connected" => {
                            info!("SSE connected to node");
                        }
                        "edit" => {
                            // Parse edit event
                            match serde_json::from_str::<EditEventData>(&msg.data) {
                                Ok(edit) => {
                                    handle_server_edit(
                                        &client,
                                        &server,
                                        &identifier,
                                        &file_path,
                                        &state,
                                        &edit,
                                        use_paths,
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    warn!("Failed to parse edit event: {}", e);
                                }
                            }
                        }
                        "closed" => {
                            warn!("SSE: Target node shut down");
                            break;
                        }
                        "warning" => {
                            warn!("SSE warning: {}", msg.data);
                        }
                        _ => {
                            debug!("Unknown SSE event type: {}", msg.event);
                        }
                    }
                }
                Err(e) => {
                    error!("SSE error: {}", e);
                    break;
                }
            }
        }

        warn!("SSE connection closed, reconnecting in 5s...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Timeout for pending write barrier (30 seconds)
const PENDING_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Refresh local file from server HEAD if needed
/// Called by upload_task after clearing barrier when needs_head_refresh was set
/// Returns true on success, false on failure (caller should re-set needs_head_refresh)
async fn refresh_from_head(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    use_paths: bool,
) -> bool {
    debug!("Refreshing from HEAD due to skipped server edit");

    // Fetch HEAD
    let head_url = build_head_url(server, identifier, use_paths);
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to fetch HEAD for refresh: {}", e);
            return false;
        }
    };

    if !resp.status().is_success() {
        error!("HEAD fetch failed for refresh: {}", resp.status());
        return false;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to parse HEAD response for refresh: {}", e);
            return false;
        }
    };

    // Check if content differs from what we have, and update state BEFORE writing
    // to prevent file watcher from re-uploading the refreshed content
    {
        let mut s = state.write().await;
        if head.content == s.last_written_content {
            // Content matches, but still update CID in case server advanced to new commit
            // with identical content (e.g., concurrent edits that merge to same text)
            debug!("HEAD matches last_written_content, updating CID only");
            s.last_written_cid = head.cid.clone();
            return true; // Success - content already matches
        }
        // Update state BEFORE writing file - this way if watcher fires after write,
        // echo detection will see matching content and skip upload
        s.last_written_cid = head.cid.clone();
        s.last_written_content = head.content.clone();
    }

    // Content differs - we need to write the new content
    // Use similar logic to handle_server_edit for binary detection
    let content_info = detect_from_path(file_path);
    use base64::{engine::general_purpose::STANDARD, Engine};
    let write_result = if content_info.is_binary {
        match STANDARD.decode(&head.content) {
            Ok(decoded) => tokio::fs::write(file_path, &decoded).await,
            Err(e) => {
                // Decode failed - fall back to writing as text (like initial sync does)
                warn!(
                    "Failed to decode base64 content for refresh, writing as text: {}",
                    e
                );
                tokio::fs::write(file_path, &head.content).await
            }
        }
    } else {
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                tokio::fs::write(file_path, &decoded).await
            }
            _ => tokio::fs::write(file_path, &head.content).await,
        }
    };

    if let Err(e) = write_result {
        error!("Failed to write file for refresh: {}", e);
        // Revert state on failure
        let mut s = state.write().await;
        s.last_written_cid = None;
        s.last_written_content = String::new();
        return false;
    }

    info!(
        "Refreshed local file from HEAD: {} bytes",
        head.content.len()
    );
    true
}

/// Handle a server edit event
async fn handle_server_edit(
    client: &Client,
    server: &str,
    identifier: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    _edit: &EditEventData,
    use_paths: bool,
) {
    // Detect if this file is binary (use both extension and content-based detection)
    let content_info = detect_from_path(file_path);

    // Fetch new content from server first
    let head_url = build_head_url(server, identifier, use_paths);
    let resp = match client.get(&head_url).send().await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to fetch HEAD: {}", e);
            return;
        }
    };

    if !resp.status().is_success() {
        error!("Failed to fetch HEAD: {}", resp.status());
        return;
    }

    let head: HeadResponse = match resp.json().await {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to parse HEAD response: {}", e);
            return;
        }
    };

    // Acquire write lock and set up barrier atomically
    let write_id = {
        let mut s = state.write().await;

        // Check if there's already a pending write (concurrent SSE events)
        if let Some(pending) = &s.pending_write {
            if pending.started_at.elapsed() < PENDING_WRITE_TIMEOUT {
                // Another write in progress, we can't process this SSE event now.
                // Set a flag so upload_task knows to refresh HEAD after clearing barrier.
                // This prevents data loss when multiple server edits arrive quickly.
                debug!(
                    "Skipping server edit - another write in progress (id={}), \
                     setting needs_head_refresh flag",
                    pending.write_id
                );
                s.needs_head_refresh = true;
                return;
            }
            // Timeout - clear stale pending and continue
            warn!("Clearing timed-out pending write (id={})", pending.write_id);
        }

        // Read local file content to check for pending local changes
        let raw_content = match std::fs::read(file_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read local file: {}", e);
                return;
            }
        };

        let is_binary = content_info.is_binary || is_binary_content(&raw_content);
        let local_content = if is_binary {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.encode(&raw_content)
        } else {
            String::from_utf8_lossy(&raw_content).to_string()
        };

        // Check if local content differs from what we last wrote
        if local_content != s.last_written_content {
            // Local changes pending - don't overwrite, let upload_task handle
            // Set needs_head_refresh so upload_task will fetch HEAD after uploading
            debug!("Skipping server update - local changes pending, setting needs_head_refresh");
            s.needs_head_refresh = true;
            return;
        }

        // Set barrier with new token BEFORE writing
        s.current_write_id += 1;
        let write_id = s.current_write_id;
        s.pending_write = Some(PendingWrite {
            write_id,
            content: head.content.clone(),
            cid: head.cid.clone(),
            started_at: std::time::Instant::now(),
        });

        write_id
    };
    // Lock released before I/O

    // Write directly to local file (not atomic)
    // We avoid temp+rename because it changes the inode, which breaks
    // inotify file watchers on Linux. Since the server is authoritative,
    // partial writes on crash are recoverable via re-sync.
    //
    // For binary detection, use both extension-based AND content-based detection:
    // - If extension suggests binary, decode base64
    // - If extension says text, still try decoding as base64 in case
    //   the file was detected as binary by content sniffing on upload
    use base64::{engine::general_purpose::STANDARD, Engine};
    let write_result = if content_info.is_binary {
        // Extension says binary - decode base64
        match STANDARD.decode(&head.content) {
            Ok(decoded) => tokio::fs::write(file_path, &decoded).await,
            Err(e) => {
                error!("Failed to decode base64 content: {}", e);
                // Clear barrier on failure
                let mut s = state.write().await;
                if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
                    s.pending_write = None;
                }
                return;
            }
        }
    } else {
        // Extension says text, but try decoding as base64 in case
        // this was a binary file detected by content sniffing on upload
        match STANDARD.decode(&head.content) {
            Ok(decoded) if is_binary_content(&decoded) => {
                // Successfully decoded and content is binary - write decoded bytes
                tokio::fs::write(file_path, &decoded).await
            }
            _ => {
                // Not base64 or not binary - write as text
                tokio::fs::write(file_path, &head.content).await
            }
        }
    };

    if let Err(e) = write_result {
        error!("Failed to write file: {}", e);
        // Clear barrier on failure
        let mut s = state.write().await;
        if s.pending_write.as_ref().map(|p| p.write_id) == Some(write_id) {
            s.pending_write = None;
        }
        return;
    }

    // DO NOT clear barrier or update last_written_* here!
    // upload_task will do it when it sees the matching content from file watcher.
    // This ensures proper echo detection even with stale watcher events.

    match &head.cid {
        Some(cid) => info!(
            "Wrote server content: {} bytes at {} (write_id={})",
            head.content.len(),
            &cid[..8.min(cid.len())],
            write_id
        ),
        None => info!(
            "Wrote server content: empty document (write_id={})",
            write_id
        ),
    }
}
