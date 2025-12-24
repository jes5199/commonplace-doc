//! Commonplace Sync - Local file synchronization with a Commonplace server
//!
//! This binary syncs a local file with a server-side document node.
//! It watches both directions: local file changes push to server,
//! server changes update the local file.

use clap::Parser;
use futures::StreamExt;
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use reqwest::Client;
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use yrs::{Doc, Text, Transact};

/// Commonplace Sync - Keep a local file in sync with a server document
#[derive(Parser, Debug)]
#[command(name = "commonplace-sync")]
#[command(about = "Sync a local file with a Commonplace document node")]
struct Args {
    /// Server URL
    #[arg(short, long, default_value = "http://localhost:3000")]
    server: String,

    /// Node ID to sync with (optional if --fork-from is provided)
    #[arg(short, long)]
    node: Option<String>,

    /// Local file path to sync
    #[arg(short, long)]
    file: PathBuf,

    /// Fork from this node before syncing (creates a new node)
    #[arg(long)]
    fork_from: Option<String>,

    /// When forking, use this commit instead of HEAD
    #[arg(long, requires = "fork_from")]
    at_commit: Option<String>,
}

/// Shared state between file watcher and SSE tasks
#[derive(Debug)]
struct SyncState {
    /// CID of the commit we last wrote to the local file
    last_written_cid: Option<String>,
    /// Content we last wrote to the local file (for echo detection)
    last_written_content: String,
}

impl SyncState {
    fn new() -> Self {
        Self {
            last_written_cid: None,
            last_written_content: String::new(),
        }
    }
}

/// Response from GET /nodes/:id/head
#[derive(Debug, Deserialize)]
struct HeadResponse {
    cid: Option<String>,
    content: String,
}

/// Response from POST /nodes/:id/replace
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ReplaceResponse {
    cid: String,
    edit_cid: String,
    summary: ReplaceSummary,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ReplaceSummary {
    chars_inserted: usize,
    chars_deleted: usize,
    operations: usize,
}

/// SSE edit event data
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct EditEventData {
    source: String,
    commit: CommitData,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CommitData {
    update: String,
    parents: Vec<String>,
    timestamp: u64,
    author: String,
    message: Option<String>,
}

/// Request for POST /nodes/:id/edit (initial commit)
#[derive(Debug, Serialize)]
struct EditRequest {
    update: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

/// Response from POST /nodes/:id/edit
#[derive(Debug, Deserialize)]
struct EditResponse {
    cid: String,
}

/// Response from POST /nodes/:id/fork
#[derive(Debug, Deserialize)]
struct ForkResponse {
    id: String,
    head: String,
}

/// File watcher events
#[derive(Debug)]
enum FileEvent {
    Modified,
}

/// Text root name used in Yrs documents (must match server)
const TEXT_ROOT_NAME: &str = "content";

/// Create a Yjs update that sets the full text content
fn create_yjs_text_update(content: &str) -> String {
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
    let update = {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, content);
        txn.encode_update_v1()
    };
    base64_encode(&update)
}

/// Simple base64 encoding (matching server's b64 module)
fn base64_encode(data: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(data)
}

/// Fork a source node and return the new node ID
async fn fork_node(
    client: &Client,
    server: &str,
    source_node: &str,
    at_commit: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut fork_url = format!("{}/nodes/{}/fork", server, source_node);
    if let Some(commit) = at_commit {
        fork_url = format!("{}?at_commit={}", fork_url, commit);
    }

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

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
            fork_node(&client, &args.server, source, args.at_commit.as_deref()).await?
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
            return Err("Either --node or --fork-from must be provided".into());
        }
    };

    info!(
        "Starting commonplace-sync: server={}, node={}, file={}",
        args.server,
        node_id,
        args.file.display()
    );

    // Verify node exists
    let node_url = format!("{}/nodes/{}", args.server, node_id);
    let resp = client.get(&node_url).send().await?;
    if !resp.status().is_success() {
        error!("Node {} not found on server", node_id);
        return Err(format!("Node {} not found", node_id).into());
    }
    info!("Connected to node {}", node_id);

    // Initialize shared state
    let state = Arc::new(RwLock::new(SyncState::new()));

    // Perform initial sync
    initial_sync(&client, &args.server, &node_id, &args.file, &state).await?;

    // Create channel for file events
    let (file_tx, file_rx) = mpsc::channel::<FileEvent>(100);

    // Start file watcher task
    let watcher_handle = tokio::spawn(file_watcher_task(args.file.clone(), file_tx));

    // Start file change handler task
    let upload_handle = tokio::spawn(upload_task(
        client.clone(),
        args.server.clone(),
        node_id.clone(),
        args.file.clone(),
        state.clone(),
        file_rx,
    ));

    // Start SSE subscription task
    let sse_handle = tokio::spawn(sse_task(
        client.clone(),
        args.server.clone(),
        node_id.clone(),
        args.file.clone(),
        state.clone(),
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

/// Perform initial sync: fetch HEAD and write to local file
async fn initial_sync(
    client: &Client,
    server: &str,
    node_id: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let head_url = format!("{}/nodes/{}/head", server, node_id);
    let resp = client.get(&head_url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("Failed to get HEAD: {}", resp.status()).into());
    }

    let head: HeadResponse = resp.json().await?;

    // Write content to file
    tokio::fs::write(file_path, &head.content).await?;

    // Update state
    {
        let mut s = state.write().await;
        s.last_written_cid = head.cid.clone();
        s.last_written_content = head.content.clone();
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

/// Task that handles file changes and uploads to server
async fn upload_task(
    client: Client,
    server: String,
    node_id: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
    mut rx: mpsc::Receiver<FileEvent>,
) {
    while let Some(_event) = rx.recv().await {
        // Read current file content
        let content = match tokio::fs::read_to_string(&file_path).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read file: {}", e);
                continue;
            }
        };

        // Check if this is an echo from our own write
        {
            let s = state.read().await;
            if content == s.last_written_content {
                debug!("Ignoring echo: content matches last written");
                continue;
            }
        }

        // Get parent CID to decide which endpoint to use
        let parent_cid = {
            let s = state.read().await;
            s.last_written_cid.clone()
        };

        match parent_cid {
            Some(parent) => {
                // Normal case: use replace endpoint
                let replace_url = format!(
                    "{}/nodes/{}/replace?parent_cid={}&author=sync-client",
                    server, node_id, parent
                );

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

                                    // Update state
                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
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
                let edit_url = format!("{}/nodes/{}/edit", server, node_id);
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

                                    // Update state
                                    let mut s = state.write().await;
                                    s.last_written_cid = Some(result.cid);
                                    s.last_written_content = content;
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
    }
}

/// Task that subscribes to SSE and handles server changes
async fn sse_task(
    client: Client,
    server: String,
    node_id: String,
    file_path: PathBuf,
    state: Arc<RwLock<SyncState>>,
) {
    let sse_url = format!("{}/sse/nodes/{}", server, node_id);

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
                                        &client, &server, &node_id, &file_path, &state, &edit,
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

/// Handle a server edit event
async fn handle_server_edit(
    client: &Client,
    server: &str,
    node_id: &str,
    file_path: &PathBuf,
    state: &Arc<RwLock<SyncState>>,
    _edit: &EditEventData,
) {
    // Read current local file content
    let local_content = match tokio::fs::read_to_string(file_path).await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to read local file: {}", e);
            return;
        }
    };

    // Check if there are pending local changes
    let has_local_changes = {
        let s = state.read().await;
        local_content != s.last_written_content
    };

    if has_local_changes {
        // Don't overwrite - local changes will be pushed and merged
        debug!("Skipping server update - local changes pending");
        return;
    }

    // Safe to overwrite - fetch new content from server
    let head_url = format!("{}/nodes/{}/head", server, node_id);
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

    // Update state BEFORE writing to file to prevent race condition.
    // If file watcher fires during the write, it will see the new content
    // and correctly detect it as an echo.
    {
        let mut s = state.write().await;
        s.last_written_cid = head.cid.clone();
        s.last_written_content = head.content.clone();
    }

    // Write to local file (atomic via temp file)
    let temp_path = file_path.with_extension("tmp");
    if let Err(e) = tokio::fs::write(&temp_path, &head.content).await {
        error!("Failed to write temp file: {}", e);
        // Note: state is already updated, which is fine - next server event
        // will retry and the echo detection will still work correctly
        return;
    }
    if let Err(e) = tokio::fs::rename(&temp_path, file_path).await {
        error!("Failed to rename temp file: {}", e);
        return;
    }

    match &head.cid {
        Some(cid) => info!(
            "Downloaded update: {} bytes at {}",
            head.content.len(),
            &cid[..8.min(cid.len())]
        ),
        None => info!("Downloaded update: empty document"),
    }
}
