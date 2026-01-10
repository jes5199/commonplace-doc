//! Commonplace Sync - Local file synchronization with a Commonplace server
//!
//! This binary syncs a local file or directory with a server-side document node.
//! It watches both directions: local changes push to server,
//! server changes update local files.

use clap::Parser;
use commonplace_doc::fs::{DocEntry, Entry, FsSchema};
use commonplace_doc::mqtt::{MqttClient, MqttConfig};
use commonplace_doc::sync::state_file::{compute_content_hash, SyncStateFile};
use commonplace_doc::sync::{
    acquire_sync_lock, build_replace_url, build_uuid_map_recursive, check_server_has_content,
    detect_from_path, directory_mqtt_task, directory_sse_task, directory_watcher_task,
    encode_node_id, ensure_fs_root_exists, file_watcher_task, fork_node,
    get_all_node_backed_dir_ids, handle_file_created, handle_file_deleted, handle_file_modified,
    handle_schema_change, handle_schema_modified, initial_sync, is_binary_content,
    push_schema_to_server, scan_directory_with_contents, spawn_file_sync_tasks_with_flock,
    sse_task, subdir_mqtt_task, subdir_sse_task, sync_schema, sync_single_file, upload_task,
    DirEvent, FileEvent, FileSyncState, FlockSyncState, InodeKey, InodeTracker, ReplaceResponse,
    ScanOptions, SyncState, SCHEMA_FILENAME,
};
#[cfg(unix)]
use commonplace_doc::sync::{spawn_shadow_tasks, sse_task_with_tracker};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
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

    /// Node ID (UUID) to sync with (reads from COMMONPLACE_NODE env var; optional if --path or --fork-from is provided)
    #[arg(short, long, env = "COMMONPLACE_NODE")]
    node: Option<String>,

    /// Path relative to fs-root to sync with (reads from COMMONPLACE_PATH env var; resolved to UUID)
    /// Example: "bartleby" or "workspace/bartleby"
    #[arg(short, long, env = "COMMONPLACE_PATH", conflicts_with = "node")]
    path: Option<String>,

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
    #[arg(long, default_value = "skip", value_parser = ["local", "server", "skip"], env = "COMMONPLACE_INITIAL_SYNC")]
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

    /// Process name for log file naming in sandbox mode.
    /// Log files will be named __<name>.stdout.txt and __<name>.stderr.txt
    /// If not specified, defaults to extracting from the exec command.
    #[arg(long)]
    name: Option<String>,

    /// Push-only mode: watch local files and push changes to server.
    /// Ignores server-side updates (no SSE subscription).
    /// Use case: source-of-truth files like .beads/issues.jsonl
    #[arg(long, conflicts_with = "pull_only")]
    push_only: bool,

    /// Pull-only mode: subscribe to server updates and write to local files.
    /// Ignores local file changes (no file watcher).
    /// Use case: read-only mirrors, generated content
    #[arg(long, conflicts_with = "push_only")]
    pull_only: bool,

    /// Force-push mode: local file content replaces server entirely.
    /// On each local change, fetches current HEAD and replaces content.
    /// No CRDT merge - local always wins unconditionally.
    /// Use case: source-of-truth files, recovery scenarios
    #[arg(long)]
    force_push: bool,

    /// Shadow directory for inode tracking hardlinks.
    /// When syncing with atomic writes, old inodes are hardlinked here so
    /// slow writers to old inodes can be detected and merged.
    /// Must be on the same filesystem as the synced directory.
    /// Set to empty string to disable inode tracking.
    #[arg(
        long,
        default_value = "/tmp/commonplace-sync/hardlinks",
        env = "COMMONPLACE_SHADOW_DIR"
    )]
    shadow_dir: String,

    /// MQTT broker URL for pub/sub (also reads from COMMONPLACE_MQTT env var)
    /// When set, uses MQTT subscriptions instead of SSE for real-time updates.
    #[arg(long, env = "COMMONPLACE_MQTT")]
    mqtt_broker: Option<String>,

    /// MQTT workspace name for topic namespacing (also reads from COMMONPLACE_WORKSPACE env var)
    #[arg(long, default_value = "commonplace", env = "COMMONPLACE_WORKSPACE")]
    workspace: String,
}

/// Discover the fs-root document ID from the server.
///
/// Queries the GET /fs-root endpoint to get the fs-root ID.
/// This allows sync to work with --use-paths without requiring --node.
async fn discover_fs_root(
    client: &Client,
    server: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/fs-root", server);
    let resp = client.get(&url).send().await?;

    if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Err("Server was not started with --fs-root".into());
    }

    if !resp.status().is_success() {
        return Err(format!("Failed to discover fs-root: HTTP {}", resp.status()).into());
    }

    #[derive(serde::Deserialize)]
    struct FsRootResponse {
        id: String,
    }

    let response: FsRootResponse = resp.json().await?;
    Ok(response.id)
}

/// Resolve a path relative to fs-root to a UUID.
///
/// Discovers the fs-root first, then traverses the schema hierarchy.
/// For example, "bartleby" finds schema.root.entries["bartleby"].node_id
/// For nested paths like "foo/bar", follows intermediate node_ids.
async fn resolve_path_to_uuid(
    client: &Client,
    server: &str,
    path: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    use commonplace_doc::sync::resolve_path_to_uuid_http;

    let fs_root_id = discover_fs_root(client, server).await?;
    resolve_path_to_uuid_http(client, server, &fs_root_id, path).await
}

/// Resolve a path to UUID, or create the document if it doesn't exist.
///
/// This function first tries to resolve the path normally. If the final segment
/// doesn't exist (but the parent path does), it creates a new entry in the parent's
/// schema and waits for the reconciler to assign a UUID.
///
/// This is used for single-file sync when the server path doesn't exist yet.
async fn resolve_or_create_path(
    client: &Client,
    server: &str,
    path: &str,
    local_file: &std::path::Path,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // First try to resolve normally
    match resolve_path_to_uuid(client, server, path).await {
        Ok(id) => return Ok(id),
        Err(e) => {
            let err_msg = e.to_string();
            // Only proceed if it's a "not found" error for the final segment
            if !err_msg.contains("not found") && !err_msg.contains("no entry") {
                return Err(e);
            }
            info!("Path '{}' not found, will create it", path);
        }
    }

    // Split path into parent and filename
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Err("Cannot create document at root path".into());
    }

    let filename = segments.last().unwrap();
    let parent_segments = &segments[..segments.len() - 1];

    // Get the parent document ID
    let (parent_id, parent_path) = if parent_segments.is_empty() {
        // Parent is fs-root
        let fs_root_id = discover_fs_root(client, server).await?;
        (fs_root_id, "fs-root".to_string())
    } else {
        // Resolve parent path (must exist)
        let parent_path_str = parent_segments.join("/");
        let parent_id = resolve_path_to_uuid(client, server, &parent_path_str).await?;
        (parent_id, parent_path_str)
    };

    info!(
        "Creating '{}' in parent '{}' ({})",
        filename, parent_path, parent_id
    );

    // Fetch current parent schema
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(&parent_id));
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to fetch parent schema: HTTP {}", resp.status()).into());
    }

    #[derive(serde::Deserialize, serde::Serialize, Clone, Default)]
    struct Schema {
        #[serde(default)]
        version: u32,
        #[serde(default)]
        root: SchemaRoot,
    }

    #[derive(serde::Deserialize, serde::Serialize, Clone)]
    struct SchemaRoot {
        #[serde(rename = "type")]
        entry_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        entries: Option<std::collections::HashMap<String, SchemaEntry>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
    }

    impl Default for SchemaRoot {
        fn default() -> Self {
            Self {
                entry_type: "dir".to_string(),
                entries: None,
                node_id: None,
            }
        }
    }

    #[derive(serde::Deserialize, serde::Serialize, Clone)]
    struct SchemaEntry {
        #[serde(rename = "type")]
        entry_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
    }

    #[derive(serde::Deserialize)]
    struct HeadResp {
        content: String,
    }

    let head: HeadResp = resp.json().await?;
    // Parse schema, handling empty "{}" case
    let mut schema: Schema = if head.content.trim() == "{}" {
        Schema {
            version: 1,
            root: SchemaRoot::default(),
        }
    } else {
        serde_json::from_str(&head.content)?
    };

    // Determine content type from local file
    let content_info = detect_from_path(local_file);
    let content_type = content_info.mime_type;

    // Add entry for the new file with None node_id.
    // Server's reconciler will generate the UUID to ensure all clients get the same one.
    let entries = schema
        .root
        .entries
        .get_or_insert_with(std::collections::HashMap::new);
    entries.insert(
        filename.to_string(),
        SchemaEntry {
            entry_type: "doc".to_string(),
            node_id: None,
            content_type: Some(content_type.to_string()),
        },
    );

    // Push updated schema - this triggers the server-side reconciler
    // which creates the document and assigns a UUID
    let schema_json = serde_json::to_string_pretty(&schema)?;
    push_schema_to_server(client, server, &parent_id, &schema_json)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;

    // Wait for the reconciler to create the document
    info!("Waiting for server reconciler to create document...");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Fetch the updated schema to get the server-assigned UUID
    let head_url = format!("{}/docs/{}/head", server, encode_node_id(&parent_id));
    let resp = client.get(&head_url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("Failed to fetch updated schema: HTTP {}", resp.status()).into());
    }

    let head: HeadResp = resp.json().await?;
    let updated_schema: Schema = serde_json::from_str(&head.content)?;

    // Find the UUID assigned by the server
    let node_id = updated_schema
        .root
        .entries
        .as_ref()
        .and_then(|e| e.get(*filename))
        .and_then(|entry| entry.node_id.clone())
        .ok_or_else(|| {
            format!(
                "Server did not assign UUID for '{}'. Check server logs.",
                filename
            )
        })?;

    info!("Created document: {} -> {}", path, node_id);
    Ok(node_id)
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

    // Initialize MQTT client if broker URL is provided
    let mqtt_client = if let Some(ref broker_url) = args.mqtt_broker {
        info!("Initializing MQTT client for broker: {}", broker_url);
        let mqtt_config = MqttConfig {
            broker_url: broker_url.clone(),
            client_id: format!("sync-{}", uuid::Uuid::new_v4()),
            workspace: args.workspace.clone(),
            ..Default::default()
        };
        match MqttClient::connect(mqtt_config).await {
            Ok(mqtt) => {
                info!("Connected to MQTT broker, workspace: {}", args.workspace);
                Some(Arc::new(mqtt))
            }
            Err(e) => {
                error!("Failed to connect to MQTT broker: {}", e);
                return ExitCode::from(1);
            }
        }
    } else {
        None
    };

    // Determine the node ID to sync with
    // Priority: --node > --path > --fork-from > --use-paths discovery
    let node_id = if let Some(ref node) = args.node {
        // Direct UUID provided
        node.clone()
    } else if let Some(ref path) = args.path {
        // Path provided - resolve to UUID (or create if using single-file mode)
        info!("Resolving path '{}' to UUID...", path);
        if let Some(ref file) = args.file {
            // Single-file mode with --path: create document if it doesn't exist
            match resolve_or_create_path(&client, &args.server, path, file).await {
                Ok(id) => {
                    info!("Resolved '{}' -> {}", path, id);
                    id
                }
                Err(e) => {
                    error!("Failed to resolve/create path '{}': {}", path, e);
                    return ExitCode::from(1);
                }
            }
        } else {
            // Directory mode: path must already exist
            match resolve_path_to_uuid(&client, &args.server, path).await {
                Ok(id) => {
                    info!("Resolved '{}' -> {}", path, id);
                    id
                }
                Err(e) => {
                    error!("Failed to resolve path '{}': {}", path, e);
                    return ExitCode::from(1);
                }
            }
        }
    } else if let Some(ref source) = args.fork_from {
        // Fork from another node
        info!("Forking from node {}...", source);
        match fork_node(&client, &args.server, source, args.at_commit.as_deref()).await {
            Ok(id) => id,
            Err(e) => {
                error!("Fork failed: {}", e);
                return ExitCode::from(1);
            }
        }
    } else if args.use_paths {
        // No node specified - try to discover fs-root from server
        info!("Discovering fs-root from server...");
        match discover_fs_root(&client, &args.server).await {
            Ok(id) => {
                info!("Discovered fs-root: {}", id);
                id
            }
            Err(e) => {
                error!("Failed to discover fs-root: {}", e);
                error!(
                    "Either specify --node, --path, or ensure server was started with --fs-root"
                );
                return ExitCode::from(1);
            }
        }
    } else {
        error!("Either --node, --path, or --fork-from must be provided");
        return ExitCode::from(1);
    };

    // Route to appropriate mode
    let result = if args.sandbox {
        // Sandbox mode: create temp directory, sync there, run command, clean up
        // Clean up stale sandbox directories from previous runs (killed by SIGKILL)
        cleanup_stale_sandboxes();

        // Create sandbox with our prefix for easy identification during cleanup
        let sandbox_dir =
            std::env::temp_dir().join(format!("commonplace-sandbox-{}", uuid::Uuid::new_v4()));

        if let Err(e) = std::fs::create_dir_all(&sandbox_dir) {
            error!("Failed to create sandbox directory: {}", e);
            return ExitCode::from(1);
        }
        info!("Creating sandbox directory: {}", sandbox_dir.display());

        // Write PID file to mark this sandbox as active
        let pid_file = sandbox_dir.join(".pid");
        if let Err(e) = std::fs::write(&pid_file, std::process::id().to_string()) {
            warn!("Failed to write PID file: {}", e);
        }

        // Always ignore the schema file and PID file when scanning
        let mut ignore_patterns = args.ignore;
        ignore_patterns.push(SCHEMA_FILENAME.to_string());
        ignore_patterns.push(".pid".to_string());
        ignore_patterns.push(".commonplace-synced-dirs.json".to_string());

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
            args.push_only,
            args.pull_only,
            args.shadow_dir,
            args.name,
        )
        .await;

        // Clean up sandbox directory
        info!("Cleaning up sandbox directory: {}", sandbox_dir.display());
        if let Err(e) = std::fs::remove_dir_all(&sandbox_dir) {
            warn!("Failed to clean up sandbox directory: {}", e);
        }

        exec_result
    } else if let Some(directory) = args.directory {
        // Acquire sync lock for this directory (non-sandbox mode)
        let _sync_lock = match acquire_sync_lock(&directory) {
            Ok(lock) => lock,
            Err(e) => {
                error!("Failed to acquire sync lock: {}", e);
                return ExitCode::from(1);
            }
        };

        // Always ignore the schema file (.commonplace.json) when scanning
        let mut ignore_patterns = args.ignore;
        ignore_patterns.push(SCHEMA_FILENAME.to_string());
        ignore_patterns.push(".commonplace-sync.lock".to_string()); // Ignore lock file
        ignore_patterns.push(".commonplace-synced-dirs.json".to_string()); // Ignore synced dirs state

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
                args.push_only,
                args.pull_only,
                args.shadow_dir,
                args.name,
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
                args.push_only,
                args.pull_only,
                args.shadow_dir,
                mqtt_client,
                args.workspace,
            )
            .await
            .map(|_| 0u8)
        }
    } else if let Some(file) = args.file {
        run_file_mode(
            client,
            args.server,
            node_id,
            file,
            args.push_only,
            args.pull_only,
            args.force_push,
            args.shadow_dir,
        )
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
#[allow(clippy::too_many_arguments)]
async fn run_file_mode(
    client: Client,
    server: String,
    node_id: String,
    file: PathBuf,
    push_only: bool,
    pull_only: bool,
    force_push: bool,
    shadow_dir: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = if force_push {
        "force-push"
    } else if push_only {
        "push-only"
    } else if pull_only {
        "pull-only"
    } else {
        "bidirectional"
    };
    info!(
        "Starting commonplace-sync (file mode, {}): server={}, node={}, file={}",
        mode,
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

    // Set up inode tracking if shadow_dir is configured
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = if !shadow_dir.is_empty() {
        let shadow_path = PathBuf::from(&shadow_dir);

        // Create shadow directory
        if let Err(e) = tokio::fs::create_dir_all(&shadow_path).await {
            warn!(
                "Failed to create shadow directory {}: {} - disabling inode tracking",
                shadow_dir, e
            );
            None
        } else {
            // Create tracker and track the initial file's inode
            let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_path)));

            // Track the initial inode after initial_sync
            if file.exists() {
                if let Ok(inode_key) = InodeKey::from_path(&file) {
                    let cid = {
                        let s = state.read().await;
                        s.last_written_cid.clone().unwrap_or_default()
                    };
                    if !cid.is_empty() {
                        let mut t = tracker.write().await;
                        t.track(inode_key, cid, file.clone());
                        info!(
                            "Tracking initial inode {:x}-{:x} for {}",
                            inode_key.dev,
                            inode_key.ino,
                            file.display()
                        );
                    }
                }
            }

            info!("Inode tracking enabled with shadow dir: {}", shadow_dir);
            Some(tracker)
        }
    } else {
        debug!("Inode tracking disabled (no shadow_dir configured)");
        None
    };

    // Create channel for file events
    let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);

    // Start file watcher task (skip if pull-only)
    let watcher_handle = if !pull_only {
        Some(tokio::spawn(file_watcher_task(file.clone(), file_tx)))
    } else {
        info!("Pull-only mode: skipping file watcher");
        None
    };

    // Start file change handler task (skip if pull-only)
    // File mode always uses ID-based API (use_paths=false)
    let upload_handle = if !pull_only {
        Some(tokio::spawn(upload_task(
            client.clone(),
            server.clone(),
            node_id.clone(),
            file.clone(),
            state.clone(),
            file_rx,
            false, // use_paths: file mode uses ID-based API
            force_push,
        )))
    } else {
        None
    };

    // Start shadow watcher, handler, and GC if inode tracking is enabled (unix only)
    #[cfg(unix)]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle) =
        if let Some(ref tracker) = inode_tracker {
            let shadow_path = {
                let t = tracker.read().await;
                t.shadow_dir.clone()
            };

            let (watcher, handler, gc) = spawn_shadow_tasks(
                shadow_path,
                client.clone(),
                server.clone(),
                tracker.clone(),
                false, // use_paths: file mode uses ID-based API
            );

            (Some(watcher), Some(handler), Some(gc))
        } else {
            (None, None, None)
        };
    #[cfg(not(unix))]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle): (
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = (None, None, None);

    // Start SSE subscription task (skip if push-only)
    // Use tracker-aware version if inode tracking is enabled (unix only)
    let sse_handle = if !push_only {
        #[cfg(unix)]
        let handle = if let Some(ref tracker) = inode_tracker {
            Some(tokio::spawn(sse_task_with_tracker(
                client.clone(),
                server.clone(),
                node_id.clone(),
                file.clone(),
                state.clone(),
                false, // use_paths: file mode uses ID-based API
                tracker.clone(),
            )))
        } else {
            Some(tokio::spawn(sse_task(
                client.clone(),
                server.clone(),
                node_id.clone(),
                file.clone(),
                state.clone(),
                false, // use_paths: file mode uses ID-based API
            )))
        };
        #[cfg(not(unix))]
        let handle = Some(tokio::spawn(sse_task(
            client.clone(),
            server.clone(),
            node_id.clone(),
            file.clone(),
            state.clone(),
            false, // use_paths: file mode uses ID-based API
        )));
        handle
    } else {
        info!("Push-only mode: skipping SSE subscription");
        None
    };

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    // Cancel tasks
    if let Some(handle) = watcher_handle {
        handle.abort();
    }
    if let Some(handle) = upload_handle {
        handle.abort();
    }
    if let Some(handle) = sse_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_watcher_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_handler_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_gc_handle {
        handle.abort();
    }

    info!("Goodbye!");
    Ok(())
}

/// Run directory sync mode
#[allow(clippy::too_many_arguments)]
async fn run_directory_mode(
    client: Client,
    server: String,
    fs_root_id: String,
    directory: PathBuf,
    options: ScanOptions,
    initial_sync_strategy: String,
    use_paths: bool,
    push_only: bool,
    pull_only: bool,
    shadow_dir: String,
    mqtt_client: Option<Arc<MqttClient>>,
    workspace: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = if push_only {
        "push-only"
    } else if pull_only {
        "pull-only"
    } else {
        "bidirectional"
    };
    info!(
        "Starting commonplace-sync (directory mode, {}): server={}, fs-root={}, directory={}, use_paths={}",
        mode,
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

    // Set up inode tracking if shadow_dir is configured (unix only)
    #[cfg(unix)]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = if !shadow_dir.is_empty() {
        let shadow_path = PathBuf::from(&shadow_dir);

        // Create shadow directory
        if let Err(e) = tokio::fs::create_dir_all(&shadow_path).await {
            warn!(
                "Failed to create shadow directory {}: {} - disabling inode tracking",
                shadow_dir, e
            );
            None
        } else {
            let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_path)));
            info!("Inode tracking enabled with shadow dir: {}", shadow_dir);
            Some(tracker)
        }
    } else {
        debug!("Inode tracking disabled (no shadow_dir configured)");
        None
    };
    #[cfg(not(unix))]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = None;

    // Verify fs-root document exists (or create it)
    ensure_fs_root_exists(&client, &server, &fs_root_id).await?;

    // Check if server has existing schema
    let server_has_content = check_server_has_content(&client, &server, &fs_root_id).await;

    // If strategy is "server" and server has content, pull server files first
    // This creates the temporary file_states that handle_schema_change needs
    let file_states: Arc<RwLock<HashMap<String, FileSyncState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Create written_schemas tracker for user schema edit detection
    // This must be created before any handle_schema_change calls so we can track what we write
    let written_schemas: commonplace_doc::sync::WrittenSchemas =
        Arc::new(RwLock::new(std::collections::HashMap::new()));

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
            push_only,
            pull_only,
            #[cfg(unix)]
            inode_tracker.clone(),
            Some(&written_schemas),
        )
        .await?;
        info!("Server files pulled to local directory");
    }

    // Synchronize schema between local and server.
    // Capture the CID from initial sync to prevent subscription tasks from
    // pulling stale server content that predates our push.
    let (_schema_json, initial_schema_cid) = sync_schema(
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
            &directory,
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

    // Create local files for any schema entries that don't exist locally.
    // This handles commonplace-link entries where the linked target exists but the
    // link file needs to be created locally. We do this AFTER sync_single_file
    // so that file_states is populated and we correctly identify truly new files.
    handle_schema_change(
        &client,
        &server,
        &fs_root_id,
        &directory,
        &file_states,
        false, // Don't spawn tasks - main loop will do that
        use_paths,
        push_only,
        pull_only,
        #[cfg(unix)]
        inode_tracker.clone(),
        Some(&written_schemas),
    )
    .await?;

    // Start directory watcher (skip if pull-only)
    let (dir_tx, mut dir_rx) = mpsc::channel::<DirEvent>(100);
    let watcher_handle = if !pull_only {
        Some(tokio::spawn(directory_watcher_task(
            directory.clone(),
            dir_tx,
            options.clone(),
            Some(written_schemas.clone()),
        )))
    } else {
        info!("Pull-only mode: skipping directory watcher");
        None
    };

    // Track which subdirectories have tasks (shared between startup and dynamic discovery)
    let watched_subdirs: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

    // Create shared flock state for ancestry checking
    let flock_state = FlockSyncState::new();

    // Start subscription task for fs-root (skip if push-only)
    // Use MQTT if available, otherwise fall back to SSE
    let subscription_handle = if !push_only {
        if let Some(ref mqtt) = mqtt_client {
            // Spawn the MQTT event loop in a background task
            let mqtt_for_loop = mqtt.clone();
            tokio::spawn(async move {
                if let Err(e) = mqtt_for_loop.run_event_loop().await {
                    error!("MQTT event loop error: {}", e);
                }
            });

            info!("Using MQTT for directory sync subscriptions");
            Some(tokio::spawn(directory_mqtt_task(
                client.clone(),
                server.clone(),
                fs_root_id.clone(),
                directory.clone(),
                file_states.clone(),
                use_paths,
                push_only,
                pull_only,
                #[cfg(unix)]
                inode_tracker.clone(),
                watched_subdirs.clone(),
                mqtt.clone(),
                workspace.clone(),
                Some(written_schemas.clone()),
                initial_schema_cid.clone(),
            )))
        } else {
            info!("Using SSE for directory sync subscriptions");
            Some(tokio::spawn(directory_sse_task(
                client.clone(),
                server.clone(),
                fs_root_id.clone(),
                directory.clone(),
                file_states.clone(),
                use_paths,
                push_only,
                pull_only,
                #[cfg(unix)]
                inode_tracker.clone(),
                watched_subdirs.clone(),
                Some(written_schemas.clone()),
                initial_schema_cid.clone(),
            )))
        }
    } else {
        info!("Push-only mode: skipping subscription");
        None
    };

    // Start tasks for all node-backed subdirectories (skip if push-only)
    // This allows files created in subdirectories to propagate to other sync clients
    if !push_only {
        let node_backed_subdirs = get_all_node_backed_dir_ids(&client, &server, &fs_root_id).await;
        info!(
            "Found {} node-backed subdirectories to watch",
            node_backed_subdirs.len()
        );
        let mut watched = watched_subdirs.write().await;
        for (subdir_path, subdir_node_id) in node_backed_subdirs {
            if let Some(ref mqtt) = mqtt_client {
                info!(
                    "Spawning MQTT task for node-backed subdir: {} ({})",
                    subdir_path, subdir_node_id
                );
                watched.insert(subdir_node_id.clone());
                tokio::spawn(subdir_mqtt_task(
                    client.clone(),
                    server.clone(),
                    fs_root_id.clone(),
                    subdir_path,
                    subdir_node_id,
                    directory.clone(),
                    file_states.clone(),
                    use_paths,
                    push_only,
                    pull_only,
                    #[cfg(unix)]
                    inode_tracker.clone(),
                    mqtt.clone(),
                    workspace.clone(),
                    watched_subdirs.clone(),
                ));
            } else {
                info!(
                    "Spawning SSE task for node-backed subdir: {} ({})",
                    subdir_path, subdir_node_id
                );
                watched.insert(subdir_node_id.clone());
                tokio::spawn(subdir_sse_task(
                    client.clone(),
                    server.clone(),
                    fs_root_id.clone(),
                    subdir_path,
                    subdir_node_id,
                    directory.clone(),
                    file_states.clone(),
                    use_paths,
                    push_only,
                    pull_only,
                    #[cfg(unix)]
                    inode_tracker.clone(),
                    watched_subdirs.clone(),
                ));
            }
        }
        drop(watched); // Release lock before continuing
    }

    // Start file sync tasks for each file and store handles in FileSyncState
    {
        let mut states = file_states.write().await;
        for (relative_path, file_state) in states.iter_mut() {
            let file_path = directory.join(relative_path);

            // Parse doc_id from identifier if using UUID-based API
            let doc_id = if !file_state.use_paths {
                uuid::Uuid::parse_str(&file_state.identifier).ok()
            } else {
                None
            };

            // Spawn sync tasks and store handles in FileSyncState for cleanup on deletion
            file_state.task_handles = spawn_file_sync_tasks_with_flock(
                client.clone(),
                server.clone(),
                file_state.identifier.clone(),
                file_path,
                file_state.state.clone(),
                file_state.use_paths,
                push_only,
                pull_only,
                false, // force_push: directory mode doesn't support force-push
                flock_state.clone(),
                doc_id,
                #[cfg(unix)]
                inode_tracker.clone(),
            );
        }
    }

    // Start shadow watcher, handler, and GC if inode tracking is enabled (unix only)
    #[cfg(unix)]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle) =
        if let Some(ref tracker) = inode_tracker {
            let shadow_path = {
                let t = tracker.read().await;
                t.shadow_dir.clone()
            };

            let (watcher, handler, gc) = spawn_shadow_tasks(
                shadow_path,
                client.clone(),
                server.clone(),
                tracker.clone(),
                use_paths,
            );

            (Some(watcher), Some(handler), Some(gc))
        } else {
            (None, None, None)
        };
    #[cfg(not(unix))]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle): (
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = (None, None, None);

    // Handle directory-level events (file creation/deletion)
    let dir_event_handle = tokio::spawn({
        let client = client.clone();
        let server = server.clone();
        let fs_root_id = fs_root_id.clone();
        let directory = directory.clone();
        let options = options.clone();
        let file_states = file_states.clone();
        #[cfg(unix)]
        let inode_tracker = inode_tracker.clone();
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
                            push_only,
                            pull_only,
                            #[cfg(unix)]
                            inode_tracker.clone(),
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
                    DirEvent::SchemaModified(path, content) => {
                        info!("User edited schema file: {}", path.display());
                        // Find the owning document for this schema and push to server
                        if let Err(e) = handle_schema_modified(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &content,
                        )
                        .await
                        {
                            warn!("Failed to push schema change: {}", e);
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
    if let Some(handle) = watcher_handle {
        handle.abort();
    }
    if let Some(handle) = subscription_handle {
        handle.abort();
    }
    dir_event_handle.abort();

    // Abort shadow tasks
    if let Some(handle) = shadow_watcher_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_handler_handle {
        handle.abort();
    }
    if let Some(handle) = shadow_gc_handle {
        handle.abort();
    }

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
    push_only: bool,
    pull_only: bool,
    shadow_dir: String,
    process_name: Option<String>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let mode = if push_only {
        "push-only"
    } else if pull_only {
        "pull-only"
    } else {
        "bidirectional"
    };
    info!(
        "Starting commonplace-sync (exec mode, {}): server={}, fs-root={}, directory={}, exec={}",
        mode,
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

    // Set up inode tracking if shadow_dir is configured (unix only)
    #[cfg(unix)]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = if !shadow_dir.is_empty() {
        let shadow_path = PathBuf::from(&shadow_dir);

        // Create shadow directory
        if let Err(e) = tokio::fs::create_dir_all(&shadow_path).await {
            warn!(
                "Failed to create shadow directory {}: {} - disabling inode tracking",
                shadow_dir, e
            );
            None
        } else {
            let tracker = Arc::new(RwLock::new(InodeTracker::new(shadow_path)));
            info!("Inode tracking enabled with shadow dir: {}", shadow_dir);
            Some(tracker)
        }
    } else {
        debug!("Inode tracking disabled (no shadow_dir configured)");
        None
    };
    #[cfg(not(unix))]
    let inode_tracker: Option<Arc<RwLock<InodeTracker>>> = None;

    // Verify fs-root document exists (or create it)
    ensure_fs_root_exists(&client, &server, &fs_root_id).await?;

    // Check if server has existing schema
    let server_has_content = check_server_has_content(&client, &server, &fs_root_id).await;

    // If strategy is "server" and server has content, pull server files first
    let file_states: Arc<RwLock<HashMap<String, FileSyncState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Create written_schemas tracker for user schema edit detection
    // This must be created before any handle_schema_change calls so we can track what we write
    let written_schemas: commonplace_doc::sync::WrittenSchemas =
        Arc::new(RwLock::new(std::collections::HashMap::new()));

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
            push_only,
            pull_only,
            #[cfg(unix)]
            inode_tracker.clone(),
            Some(&written_schemas),
        )
        .await?;
        info!("Server files pulled to local directory");
    }

    // Synchronize schema between local and server.
    // Capture the CID from initial sync to prevent subscription tasks from
    // pulling stale server content that predates our push.
    let (_schema_json, initial_schema_cid) = sync_schema(
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
    // In sandbox mode, do this in a background task so exec can start sooner
    // The schema is already synced, so directory structure exists - file content
    // can sync while the exec is running
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

    // In sandbox mode, use a timeout to not block exec for too long
    // In non-sandbox mode, sync files synchronously before continuing
    let file_count = files.len();
    let sync_timeout = if sandbox {
        // Give sandbox mode 3 seconds for file sync, then proceed to exec
        // SSE subscription will handle any files that don't sync in time
        Some(Duration::from_secs(3))
    } else {
        None
    };

    let mut synced_count = 0usize;
    let sync_start = std::time::Instant::now();
    for file in files {
        // Check timeout in sandbox mode
        if let Some(timeout) = sync_timeout {
            if sync_start.elapsed() > timeout {
                info!(
                    "Sandbox file sync timeout after {} files ({}ms elapsed), proceeding to exec",
                    synced_count,
                    sync_start.elapsed().as_millis()
                );
                break;
            }
        }

        let file_path = directory.join(&file.relative_path);
        if let Err(e) = sync_single_file(
            &client,
            &server,
            &fs_root_id,
            &directory,
            &file,
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
        synced_count += 1;
    }
    info!(
        "Initial sync complete: {}/{} files synced ({}ms)",
        synced_count,
        file_count,
        sync_start.elapsed().as_millis()
    );

    // Start directory watcher (skip if pull-only)
    let (dir_tx, mut dir_rx) = mpsc::channel::<DirEvent>(100);
    let watcher_handle = if !pull_only {
        Some(tokio::spawn(directory_watcher_task(
            directory.clone(),
            dir_tx,
            options.clone(),
            Some(written_schemas.clone()),
        )))
    } else {
        info!("Pull-only mode: skipping directory watcher");
        None
    };

    // Track which subdirectories have SSE tasks (shared between startup and dynamic discovery)
    let watched_subdirs: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

    // Create shared flock state for ancestry checking
    let flock_state = FlockSyncState::new();

    // Start SSE task for fs-root (skip if push-only)
    let sse_handle = if !push_only {
        Some(tokio::spawn(directory_sse_task(
            client.clone(),
            server.clone(),
            fs_root_id.clone(),
            directory.clone(),
            file_states.clone(),
            use_paths,
            push_only,
            pull_only,
            #[cfg(unix)]
            inode_tracker.clone(),
            watched_subdirs.clone(),
            Some(written_schemas.clone()),
            initial_schema_cid.clone(),
        )))
    } else {
        info!("Push-only mode: skipping SSE subscription");
        None
    };

    // Start SSE tasks for all node-backed subdirectories (skip if push-only)
    if !push_only {
        let node_backed_subdirs = get_all_node_backed_dir_ids(&client, &server, &fs_root_id).await;
        info!(
            "Found {} node-backed subdirectories to watch",
            node_backed_subdirs.len()
        );
        let mut watched = watched_subdirs.write().await;
        for (subdir_path, subdir_node_id) in node_backed_subdirs {
            info!(
                "Spawning SSE task for node-backed subdir: {} ({})",
                subdir_path, subdir_node_id
            );
            watched.insert(subdir_node_id.clone());
            tokio::spawn(subdir_sse_task(
                client.clone(),
                server.clone(),
                fs_root_id.clone(),
                subdir_path,
                subdir_node_id,
                directory.clone(),
                file_states.clone(),
                use_paths,
                push_only,
                pull_only,
                #[cfg(unix)]
                inode_tracker.clone(),
                watched_subdirs.clone(),
            ));
        }
        drop(watched); // Release lock before continuing
    }

    // Start file sync tasks for each file
    {
        let mut states = file_states.write().await;
        for (relative_path, file_state) in states.iter_mut() {
            let file_path = directory.join(relative_path);

            // Parse doc_id from identifier if using UUID-based API
            let doc_id = if !file_state.use_paths {
                uuid::Uuid::parse_str(&file_state.identifier).ok()
            } else {
                None
            };

            file_state.task_handles = spawn_file_sync_tasks_with_flock(
                client.clone(),
                server.clone(),
                file_state.identifier.clone(),
                file_path,
                file_state.state.clone(),
                file_state.use_paths,
                push_only,
                pull_only,
                false, // force_push: sandbox mode doesn't support force-push
                flock_state.clone(),
                doc_id,
                #[cfg(unix)]
                inode_tracker.clone(),
            );
        }
    }

    // Start shadow watcher, handler, and GC if inode tracking is enabled (unix only)
    #[cfg(unix)]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle) =
        if let Some(ref tracker) = inode_tracker {
            let shadow_path = {
                let t = tracker.read().await;
                t.shadow_dir.clone()
            };

            let (watcher, handler, gc) = spawn_shadow_tasks(
                shadow_path,
                client.clone(),
                server.clone(),
                tracker.clone(),
                use_paths,
            );

            (Some(watcher), Some(handler), Some(gc))
        } else {
            (None, None, None)
        };
    #[cfg(not(unix))]
    let (shadow_watcher_handle, shadow_handler_handle, shadow_gc_handle): (
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = (None, None, None);

    // Start directory event handler (same logic as run_directory_mode)
    let dir_event_handle = tokio::spawn({
        let client = client.clone();
        let server = server.clone();
        let fs_root_id = fs_root_id.clone();
        let directory = directory.clone();
        let options = options.clone();
        let file_states = file_states.clone();
        #[cfg(unix)]
        let inode_tracker = inode_tracker.clone();
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
                            push_only,
                            pull_only,
                            #[cfg(unix)]
                            inode_tracker.clone(),
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
                    DirEvent::SchemaModified(path, content) => {
                        info!("User edited schema file: {}", path.display());
                        // Find the owning document for this schema and push to server
                        if let Err(e) = handle_schema_modified(
                            &client,
                            &server,
                            &fs_root_id,
                            &directory,
                            &path,
                            &content,
                        )
                        .await
                        {
                            warn!("Failed to push schema change: {}", e);
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

    // Calculate exec_name early - needed for log file schema entries
    // Use provided process_name if available, otherwise extract from program path
    let exec_name = process_name.unwrap_or_else(|| {
        std::path::Path::new(&program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("exec")
            .to_string()
    });

    // In sandbox mode, add log file entries to schema BEFORE starting exec
    // This prevents race condition where sync deletes newly created log files
    if sandbox {
        let stdout_name = format!("__{}.stdout.txt", exec_name);
        let stderr_name = format!("__{}.stderr.txt", exec_name);

        // Fetch current schema
        let head_url = format!("{}/docs/{}/head", server, fs_root_id);
        if let Ok(resp) = client.get(&head_url).send().await {
            if let Ok(head) = resp.json::<serde_json::Value>().await {
                if let Some(content) = head.get("content").and_then(|c| c.as_str()) {
                    if let Ok(mut schema) = serde_json::from_str::<FsSchema>(content) {
                        // Add log file entries if they don't exist
                        if let Some(Entry::Dir(ref mut dir)) = schema.root {
                            let entries = dir.entries.get_or_insert_with(HashMap::new);

                            let mut added = false;
                            if !entries.contains_key(&stdout_name) {
                                entries.insert(
                                    stdout_name.clone(),
                                    Entry::Doc(DocEntry {
                                        node_id: None,
                                        content_type: Some("text/plain".to_string()),
                                    }),
                                );
                                added = true;
                            }
                            if !entries.contains_key(&stderr_name) {
                                entries.insert(
                                    stderr_name.clone(),
                                    Entry::Doc(DocEntry {
                                        node_id: None,
                                        content_type: Some("text/plain".to_string()),
                                    }),
                                );
                                added = true;
                            }

                            if added {
                                // Push updated schema
                                let schema_json =
                                    serde_json::to_string_pretty(&schema).unwrap_or_default();
                                info!(
                                    "Adding log file entries to schema: {}, {}",
                                    stdout_name, stderr_name
                                );
                                if let Err(e) = push_schema_to_server(
                                    &client,
                                    &server,
                                    &fs_root_id,
                                    &schema_json,
                                )
                                .await
                                {
                                    warn!("Failed to add log file entries to schema: {}", e);
                                } else {
                                    // Wait for reconciler to create the documents
                                    sleep(Duration::from_millis(200)).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    info!("Launching: {} {:?}", program, args);

    // Build the command
    let mut cmd = tokio::process::Command::new(&program);
    cmd.args(&args)
        .current_dir(&directory)
        .stdin(std::process::Stdio::inherit())
        // Pass through key environment variables
        .env("COMMONPLACE_SERVER", &server)
        .env("COMMONPLACE_NODE", &fs_root_id);

    // For sandbox mode, capture stdout/stderr to write to log files
    // For non-sandbox mode, inherit stdout/stderr for interactive programs
    if sandbox {
        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Force unbuffered output for Python processes (and similar)
            .env("PYTHONUNBUFFERED", "1");
    } else {
        cmd.stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit());
    }

    // Make child a process group leader and set death signal on Linux
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            // Put child in its own process group so we can kill all descendants
            libc::setpgid(0, 0);
            // Request SIGTERM if parent dies (Linux-only, covers SIGKILL of parent)
            #[cfg(target_os = "linux")]
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }

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

    // In sandbox mode, spawn tasks to log stdout/stderr to files
    if sandbox {
        use tokio::fs::OpenOptions;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        // Stdout logging
        if let Some(stdout) = child.stdout.take() {
            let log_path = directory.join(format!("__{}.stdout.txt", exec_name));
            let exec_name_clone = exec_name.clone();
            tokio::spawn(async move {
                let mut log_file = match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                    .await
                {
                    Ok(f) => Some(f),
                    Err(e) => {
                        tracing::warn!("Failed to open stdout log file {:?}: {}", log_path, e);
                        None
                    }
                };

                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // Print to console (like non-sandbox mode would)
                    println!("[{}] {}", exec_name_clone, line);
                    // Write to log file
                    if let Some(ref mut file) = log_file {
                        let log_line = format!("{}\n", line);
                        if let Err(e) = file.write_all(log_line.as_bytes()).await {
                            tracing::warn!("Failed to write to stdout log: {}", e);
                        }
                    }
                }
            });
        }

        // Stderr logging
        if let Some(stderr) = child.stderr.take() {
            let log_path = directory.join(format!("__{}.stderr.txt", exec_name));
            tokio::spawn(async move {
                let mut log_file = match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                    .await
                {
                    Ok(f) => Some(f),
                    Err(e) => {
                        tracing::warn!("Failed to open stderr log file {:?}: {}", log_path, e);
                        None
                    }
                };

                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // Print to console (like non-sandbox mode would)
                    eprintln!("[{}] {}", exec_name, line);
                    // Write to log file
                    if let Some(ref mut file) = log_file {
                        let log_line = format!("{}\n", line);
                        if let Err(e) = file.write_all(log_line.as_bytes()).await {
                            tracing::warn!("Failed to write to stderr log: {}", e);
                        }
                    }
                }
            });
        }
    }

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
        _ = async {
            // Handle both SIGINT (Ctrl+C) and SIGTERM
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
                let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");
                tokio::select! {
                    _ = sigterm.recv() => info!("Received SIGTERM"),
                    _ = sigint.recv() => info!("Received SIGINT"),
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c().await.ok();
            }
        } => {
            info!("Terminating child process...");
            // Try to kill the child and its descendants gracefully
            #[cfg(unix)]
            {
                // Send SIGTERM to entire process group (negative PID)
                if let Some(pid) = child.id() {
                    info!("Sending SIGTERM to process group {}", pid);
                    unsafe {
                        // Negative PID kills the entire process group
                        libc::kill(-(pid as i32), libc::SIGTERM);
                    }
                }
                // Wait briefly for graceful shutdown
                tokio::select! {
                    _ = child.wait() => {
                        info!("Child process exited gracefully");
                    }
                    _ = sleep(Duration::from_secs(5)) => {
                        // Force kill the process group after timeout
                        if let Some(pid) = child.id() {
                            info!("Force killing process group {}", pid);
                            unsafe {
                                libc::kill(-(pid as i32), libc::SIGKILL);
                            }
                        }
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
    if let Some(handle) = watcher_handle {
        handle.abort();
    }
    if let Some(handle) = sse_handle {
        handle.abort();
    }
    dir_event_handle.abort();

    // Abort shadow tracking tasks
    #[cfg(unix)]
    {
        if let Some(handle) = shadow_watcher_handle {
            handle.abort();
        }
        if let Some(handle) = shadow_handler_handle {
            handle.abort();
        }
        if let Some(handle) = shadow_gc_handle {
            handle.abort();
        }
    }

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

/// Clean up stale sandbox directories from previous runs.
/// This handles directories left behind when a process was killed with SIGKILL
/// (which doesn't allow cleanup code to run).
fn cleanup_stale_sandboxes() {
    let temp_dir = std::env::temp_dir();

    // Look for directories matching our sandbox pattern
    let entries = match std::fs::read_dir(&temp_dir) {
        Ok(entries) => entries,
        Err(e) => {
            warn!(
                "Failed to read temp directory for stale sandbox cleanup: {}",
                e
            );
            return;
        }
    };

    let mut cleaned = 0;

    for entry in entries.flatten() {
        let path = entry.path();

        // Only process directories with our prefix
        if !path.is_dir() {
            continue;
        }

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        if !name.starts_with("commonplace-sandbox-") {
            continue;
        }

        // Check if this sandbox is still active by reading its PID file
        // Only clean up if we can confirm the owning process is dead
        let pid_file = path.join(".pid");
        let can_cleanup = if pid_file.exists() {
            // PID file exists - check if process is still running
            match std::fs::read_to_string(&pid_file) {
                Ok(pid_str) => match pid_str.trim().parse::<u32>() {
                    Ok(pid) => !is_process_running(pid), // Clean up only if process is dead
                    Err(_) => false,                     // Invalid PID, don't clean up to be safe
                },
                Err(_) => false, // Can't read PID file, don't clean up to be safe
            }
        } else {
            // No PID file - this could be a legacy sandbox or one where PID write failed
            // Be conservative: don't delete directories without PID files
            // They'll be cleaned up manually or when someone adds a PID file
            false
        };

        if !can_cleanup {
            continue;
        }

        // PID file exists and process is confirmed dead - safe to clean up
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {
                info!("Cleaned up stale sandbox: {}", path.display());
                cleaned += 1;
            }
            Err(e) => {
                // Directory might be in use by another process, that's fine
                debug!("Could not remove stale sandbox {}: {}", path.display(), e);
            }
        }
    }

    if cleaned > 0 {
        info!("Cleaned up {} stale sandbox directories", cleaned);
    }
}

/// Check if a process with the given PID is still running.
#[cfg(unix)]
fn is_process_running(pid: u32) -> bool {
    // On Unix, sending signal 0 checks if process exists without actually sending a signal
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
fn is_process_running(_pid: u32) -> bool {
    // On non-Unix platforms, assume process might be running to be safe
    true
}
