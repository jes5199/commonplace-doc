# Cyan Schema Sync Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Initialize schema CRDT state via the MQTT cyan sync channel instead of HTTP, fixing sandbox file creation/deletion propagation.

**Architecture:** New `sync_schema_via_cyan()` function sends `Ancestors(HEAD)` on the cyan sync channel, receives commit history, builds a Y.Doc from the commits, and initializes schema CRDT state. This replaces all HTTP-based schema bootstrapping and resync paths.

**Tech Stack:** Rust, MQTT (rumqttc), Yrs CRDT, Tokio async

**Bug:** CP-uals

---

### Task 1: Implement `sync_schema_via_cyan()` function

**Files:**
- Modify: `src/sync/transport/subscriptions.rs` (add new function after `resync_subdir_schema_from_server`, around line 1600)

**Step 1: Write the function**

Add the following function. It implements the full cyan sync protocol for schema initialization:

```rust
use base64::Engine;

/// Initialize schema CRDT state via the cyan (sync) MQTT channel.
///
/// Sends an `Ancestors(HEAD)` request for the given document, receives
/// the commit history, builds a Y.Doc from commits, and initializes
/// the CRDT state. This replaces HTTP-based schema bootstrapping.
///
/// See design: docs/plans/2026-01-30-cyan-schema-sync-design.md
///
/// Returns `true` if initialization succeeded, `false` if timed out or failed.
async fn sync_schema_via_cyan(
    mqtt_client: &Arc<MqttClient>,
    workspace: &str,
    doc_path: &str,
    client_id: &str,
    schema_state: &mut CrdtPeerState,
) -> bool {
    use crate::mqtt::messages::SyncMessage;
    use crate::mqtt::topics::Topic;
    use yrs::{Doc, Transact, ReadTxn, updates::decoder::Decode, Update};

    // Generate unique request ID for correlation
    let req_id = uuid::Uuid::new_v4().to_string();

    // Build the sync topic: {workspace}/sync/{doc_path}/{client_id}
    let sync_topic = Topic::sync(workspace, doc_path, client_id);
    let sync_topic_str = sync_topic.to_topic_string();

    info!(
        "[CYAN-SYNC] Starting schema sync for doc={} on topic={}",
        doc_path, sync_topic_str
    );

    // Step 1: Create message receiver BEFORE subscribing (to catch retained messages)
    let mut message_rx = mqtt_client.subscribe_messages();

    // Step 2: Subscribe to the sync topic
    if let Err(e) = mqtt_client
        .subscribe(&sync_topic_str, rumqttc::QoS::AtLeastOnce)
        .await
    {
        warn!(
            "[CYAN-SYNC] Failed to subscribe to sync topic {}: {}",
            sync_topic_str, e
        );
        return false;
    }

    // Step 3: Send Ancestors request
    let ancestors_msg = SyncMessage::Ancestors {
        req: req_id.clone(),
        commit: "HEAD".to_string(),
        depth: None,
    };
    let payload = match serde_json::to_vec(&ancestors_msg) {
        Ok(p) => p,
        Err(e) => {
            warn!("[CYAN-SYNC] Failed to serialize Ancestors message: {}", e);
            let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
            return false;
        }
    };

    if let Err(e) = mqtt_client
        .publish(&sync_topic_str, &payload, rumqttc::QoS::AtLeastOnce)
        .await
    {
        warn!(
            "[CYAN-SYNC] Failed to publish Ancestors request: {}",
            e
        );
        let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
        return false;
    }

    debug!(
        "[CYAN-SYNC] Sent Ancestors(HEAD) request req={} to {}",
        req_id, sync_topic_str
    );

    // Step 4: Collect commit messages until Done or timeout
    let mut commits: Vec<(String, String)> = Vec::new(); // (id, base64_data)
    let mut head_cid: Option<String> = None;
    let timeout = tokio::time::Duration::from_secs(5);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            warn!(
                "[CYAN-SYNC] Timeout waiting for sync response for doc={} (req={})",
                doc_path, req_id
            );
            let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
            return false;
        }

        match tokio::time::timeout(remaining, message_rx.recv()).await {
            Ok(Ok(msg)) => {
                // Only process messages on our sync topic
                if msg.topic != sync_topic_str {
                    continue;
                }

                // Parse the sync message
                let sync_msg: SyncMessage = match serde_json::from_slice(&msg.payload) {
                    Ok(m) => m,
                    Err(e) => {
                        debug!(
                            "[CYAN-SYNC] Failed to parse sync message: {} (payload len={})",
                            e,
                            msg.payload.len()
                        );
                        continue;
                    }
                };

                match sync_msg {
                    SyncMessage::Commit {
                        req, id, data, ..
                    } if req == req_id => {
                        debug!(
                            "[CYAN-SYNC] Received commit {} (data len={})",
                            id, data.len()
                        );
                        // Track the last commit as HEAD
                        // Server sends oldest-first, so last commit is HEAD
                        head_cid = Some(id.clone());
                        commits.push((id, data));
                    }
                    SyncMessage::Done {
                        req, commits: commit_list, ..
                    } if req == req_id => {
                        info!(
                            "[CYAN-SYNC] Received Done for doc={}: {} commits",
                            doc_path,
                            commit_list.len()
                        );
                        // Done message commits list has HEAD last
                        if let Some(last) = commit_list.last() {
                            head_cid = Some(last.clone());
                        }
                        break;
                    }
                    SyncMessage::Error {
                        req, message, ..
                    } if req == req_id => {
                        warn!(
                            "[CYAN-SYNC] Server error for doc={}: {}",
                            doc_path, message
                        );
                        let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
                        return false;
                    }
                    _ => {
                        // Wrong req or unrelated message type — skip
                        continue;
                    }
                }
            }
            Ok(Err(e)) => {
                warn!("[CYAN-SYNC] Broadcast channel error: {}", e);
                let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
                return false;
            }
            Err(_) => {
                warn!(
                    "[CYAN-SYNC] Timeout waiting for sync response for doc={}",
                    doc_path
                );
                let _ = mqtt_client.unsubscribe(&sync_topic_str).await;
                return false;
            }
        }
    }

    // Step 5: Unsubscribe from sync topic
    if let Err(e) = mqtt_client.unsubscribe(&sync_topic_str).await {
        debug!("[CYAN-SYNC] Failed to unsubscribe from {}: {}", sync_topic_str, e);
    }

    // Step 6: Handle empty history (new document)
    if commits.is_empty() {
        info!(
            "[CYAN-SYNC] Empty history for doc={} — initializing empty schema",
            doc_path
        );
        schema_state.initialize_empty();
        return true;
    }

    // Step 7: Build Y.Doc from commits
    let doc = Doc::new();
    for (cid, data_b64) in &commits {
        let data_bytes = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "[CYAN-SYNC] Failed to decode base64 for commit {}: {}",
                    cid, e
                );
                continue;
            }
        };

        let update = match Update::decode_v1(&data_bytes) {
            Ok(u) => u,
            Err(e) => {
                warn!(
                    "[CYAN-SYNC] Failed to decode Yrs update for commit {}: {}",
                    cid, e
                );
                continue;
            }
        };

        let mut txn = doc.transact_mut();
        if let Err(e) = txn.apply_update(update) {
            warn!(
                "[CYAN-SYNC] Failed to apply Yrs update for commit {}: {}",
                cid, e
            );
        }
    }

    // Step 8: Encode final state and initialize CRDT
    let txn = doc.transact();
    let state_bytes = txn.encode_state_as_update_v1(&yrs::StateVector::default());
    drop(txn);

    let state_b64 = base64::engine::general_purpose::STANDARD.encode(&state_bytes);
    let cid = head_cid.as_deref().unwrap_or("unknown");

    schema_state.initialize_from_server(&state_b64, cid);

    info!(
        "[CYAN-SYNC] Schema CRDT initialized for doc={}: {} commits applied, head={}",
        doc_path,
        commits.len(),
        cid
    );

    true
}
```

**Step 2: Verify imports are available**

The function uses these imports that should already be available in the file:
- `uuid::Uuid` (for req_id generation)
- `rumqttc::QoS`
- `crate::mqtt::messages::SyncMessage`
- `crate::mqtt::topics::Topic`
- `base64::Engine` (may need to add)
- `yrs::{Doc, Transact, ReadTxn, updates::decoder::Decode, Update}` (may need to add)

Check existing imports at the top of `subscriptions.rs`. Add any missing imports.

**Step 3: Build and verify**

Run: `cargo build`
Expected: Compiles with a `dead_code` warning for `sync_schema_via_cyan` (not called yet).

**Step 4: Commit**

```bash
git add src/sync/transport/subscriptions.rs
git commit -m "Add sync_schema_via_cyan() function for MQTT-based schema init (CP-uals)"
```

---

### Task 2: Replace HTTP bootstrap in `directory_mqtt_task()`

**Files:**
- Modify: `src/sync/transport/subscriptions.rs:350-383`

**Step 1: Replace the HTTP bootstrap block**

In `directory_mqtt_task()`, replace lines 350-383 (the HTTP bootstrap block) with a cyan sync call. The current code is:

```rust
    // Bootstrap schema from HTTP before entering MQTT loop.
    if let Some(ref ctx) = crdt_context {
        if !push_only {
            info!(
                "Bootstrapping schema from HTTP for {} before MQTT loop",
                fs_root_id
            );
            if let Err(e) = handle_subdir_new_files(
                &http_client,
                // ... 12 args ...
                None, // No MQTT schema yet - fetch from HTTP
            )
            .await
            {
                warn!("Failed to bootstrap schema from HTTP: {}", e);
            }
        }
    }
```

Replace with:

```rust
    // Bootstrap schema CRDT state via cyan sync channel before entering MQTT loop.
    // This replaces HTTP bootstrap — see CP-uals, design doc: cyan-schema-sync-design.md
    if let Some(ref ctx) = crdt_context {
        if !push_only {
            let needs_init = {
                let state = ctx.crdt_state.read().await;
                state.schema.needs_server_init()
            };

            if needs_init {
                // Generate a client ID for the sync topic
                let sync_client_id = uuid::Uuid::new_v4().to_string();

                let initialized = {
                    let mut state = ctx.crdt_state.write().await;
                    sync_schema_via_cyan(
                        &mqtt_client,
                        &workspace,
                        &fs_root_id,
                        &sync_client_id,
                        &mut state.schema,
                    )
                    .await
                };

                if initialized {
                    info!(
                        "Schema CRDT initialized via cyan sync for {}",
                        fs_root_id
                    );
                    // Now bootstrap files using the initialized CRDT schema state
                    if let Err(e) = handle_subdir_new_files(
                        &http_client,
                        &server,
                        &fs_root_id,
                        "", // root directory = empty subdir_path
                        &directory,
                        &directory,
                        &file_states,
                        use_paths,
                        push_only,
                        pull_only,
                        shared_state_file.as_ref(),
                        &author,
                        #[cfg(unix)]
                        inode_tracker.clone(),
                        Some(ctx),
                        None, // Schema already initialized — HTTP fetch will populate files
                    )
                    .await
                    {
                        warn!("Failed to bootstrap files after cyan sync: {}", e);
                    }
                } else {
                    warn!(
                        "Cyan sync failed for {} — falling back to HTTP bootstrap",
                        fs_root_id
                    );
                    // Fall back to existing HTTP bootstrap
                    if let Err(e) = handle_subdir_new_files(
                        &http_client,
                        &server,
                        &fs_root_id,
                        "",
                        &directory,
                        &directory,
                        &file_states,
                        use_paths,
                        push_only,
                        pull_only,
                        shared_state_file.as_ref(),
                        &author,
                        #[cfg(unix)]
                        inode_tracker.clone(),
                        Some(ctx),
                        None,
                    )
                    .await
                    {
                        warn!("Failed to bootstrap schema from HTTP: {}", e);
                    }
                }
            }
        }
    }
```

**Step 2: Build and verify**

Run: `cargo build`
Expected: Compiles with no errors.

**Step 3: Commit**

```bash
git add src/sync/transport/subscriptions.rs
git commit -m "Replace HTTP bootstrap with cyan sync in directory_mqtt_task (CP-uals)"
```

---

### Task 3: Replace HTTP bootstrap in `subdir_mqtt_task()`

**Files:**
- Modify: `src/sync/transport/subscriptions.rs:1718-1729`

**Step 1: Add cyan sync before the UUID map build**

In `subdir_mqtt_task()`, before the existing UUID map build at line 1718, add schema CRDT initialization. The current code is:

```rust
    let subdir_full_path = directory.join(&subdir_path);
    let (initial_uuid_map, _) = crate::sync::uuid_map::build_uuid_map_and_write_schemas(
        &http_client,
        &server,
        &subdir_node_id,
        &subdir_full_path,
        None,
    )
    .await;
```

Insert cyan sync initialization before this block:

```rust
    // Initialize subdirectory schema CRDT state via cyan sync (CP-uals)
    if let Some(ref ctx) = crdt_context {
        let subdir_full_path_init = directory.join(&subdir_path);
        let subdir_uuid = match uuid::Uuid::parse_str(&subdir_node_id) {
            Ok(id) => Some(id),
            Err(e) => {
                warn!(
                    "[CYAN-SYNC] Failed to parse subdir node_id {} as UUID: {}",
                    subdir_node_id, e
                );
                None
            }
        };

        if let Some(subdir_uuid) = subdir_uuid {
            // Get or create the cached subdir state
            match ctx.subdir_cache.get_or_load(&subdir_full_path_init, subdir_uuid).await {
                Ok(subdir_state) => {
                    let needs_init = {
                        let state = subdir_state.read().await;
                        state.schema.needs_server_init()
                    };

                    if needs_init {
                        let sync_client_id = uuid::Uuid::new_v4().to_string();
                        let mut state = subdir_state.write().await;
                        let initialized = sync_schema_via_cyan(
                            &mqtt_client,
                            &workspace,
                            &subdir_node_id,
                            &sync_client_id,
                            &mut state.schema,
                        )
                        .await;

                        if initialized {
                            info!(
                                "[CYAN-SYNC] Subdir schema CRDT initialized for {} ({})",
                                subdir_path, subdir_node_id
                            );
                        } else {
                            warn!(
                                "[CYAN-SYNC] Failed to initialize subdir schema for {} — will rely on MQTT retained messages",
                                subdir_path
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "[CYAN-SYNC] Failed to load subdir state for {}: {}",
                        subdir_path, e
                    );
                }
            }
        }
    }

    // Build initial UUID map (still uses HTTP for now — file content init is out of scope)
    let subdir_full_path = directory.join(&subdir_path);
    let (initial_uuid_map, _) = crate::sync::uuid_map::build_uuid_map_and_write_schemas(
        // ... existing code unchanged ...
```

**Step 2: Build and verify**

Run: `cargo build`
Expected: Compiles with no errors.

**Step 3: Commit**

```bash
git add src/sync/transport/subscriptions.rs
git commit -m "Add cyan sync schema init to subdir_mqtt_task (CP-uals)"
```

---

### Task 4: Replace `resync_schema_from_server()` with cyan sync

**Files:**
- Modify: `src/sync/transport/subscriptions.rs:1388-1476`

**Step 1: Replace the function body**

Replace the body of `resync_schema_from_server` to use cyan sync instead of HTTP. Keep the same function signature for backward compatibility.

```rust
async fn resync_schema_from_server(
    http_client: &Client,
    server: &str,
    fs_root_id: &str,
    crdt_context: &CrdtFileSyncContext,
    directory: &Path,
    written_schemas: Option<&crate::sync::WrittenSchemas>,
) {
    warn!(
        "Resyncing schema CRDT state via cyan sync for {}",
        fs_root_id
    );

    // Step 1: Mark schema state as needing resync
    {
        let mut state_guard = crdt_context.crdt_state.write().await;
        state_guard.schema.mark_needs_resync();
        debug!(
            "Marked schema {} as needing resync - cleared CRDT state",
            fs_root_id
        );
    }

    // Step 2: Initialize via cyan sync
    let sync_client_id = uuid::Uuid::new_v4().to_string();
    let initialized = {
        let mut state_guard = crdt_context.crdt_state.write().await;
        sync_schema_via_cyan(
            &crdt_context.mqtt_client,
            &crdt_context.workspace,
            fs_root_id,
            &sync_client_id,
            &mut state_guard.schema,
        )
        .await
    };

    if initialized {
        // Step 3: Write schema content to disk from the CRDT state
        // The CRDT state now has the full schema — decode it to get the JSON content
        let schema_json = {
            let state_guard = crdt_context.crdt_state.read().await;
            if let Some(ref yjs_state_b64) = state_guard.schema.yjs_state {
                // Decode the Yrs state to extract schema JSON
                match base64::engine::general_purpose::STANDARD.decode(yjs_state_b64) {
                    Ok(state_bytes) => {
                        use yrs::{Doc, Transact, updates::decoder::Decode, Update, ReadTxn};
                        let doc = Doc::new();
                        if let Ok(update) = Update::decode_v1(&state_bytes) {
                            let mut txn = doc.transact_mut();
                            let _ = txn.apply_update(update);
                            drop(txn);
                            // Read the schema content from the Y.Doc
                            let txn = doc.transact();
                            crate::sync::crdt::ymap_schema::read_schema_json(&doc)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            } else {
                None
            }
        };

        if let Some(json) = schema_json {
            if let Err(e) =
                crate::sync::write_schema_file(directory, &json, written_schemas).await
            {
                warn!(
                    "Schema resync: Failed to write schema file for {}: {}",
                    fs_root_id, e
                );
            }
        }

        info!("Schema resync via cyan completed for {}", fs_root_id);
    } else {
        warn!(
            "Schema resync: Cyan sync failed for {} — falling back to HTTP",
            fs_root_id
        );

        // Fall back to HTTP fetch
        let head = match fetch_head(http_client, server, &encode_node_id(fs_root_id), false).await {
            Ok(Some(h)) => h,
            Ok(None) => {
                warn!("Schema resync: Document {} not found on server", fs_root_id);
                return;
            }
            Err(e) => {
                warn!("Schema resync: Failed to fetch HEAD for {}: {}", fs_root_id, e);
                return;
            }
        };

        if let Some(ref server_state) = head.state {
            let cid = head.cid.as_deref().unwrap_or("unknown");
            let mut state_guard = crdt_context.crdt_state.write().await;
            state_guard.schema.initialize_from_server(server_state, cid);
            info!("Schema resync: HTTP fallback initialized for {} at cid={}", fs_root_id, cid);
        } else {
            let mut state_guard = crdt_context.crdt_state.write().await;
            state_guard.schema.initialize_empty();
        }

        if !head.content.is_empty() {
            if let Err(e) = crate::sync::write_schema_file(directory, &head.content, written_schemas).await {
                warn!("Schema resync: Failed to write schema file: {}", e);
            }
        }
    }
}
```

**Step 2: Check if `read_schema_json` exists**

We need a way to extract JSON from the Y.Doc. Check if `ymap_schema::read_schema_json` exists. If not, we can skip the disk-write step in resync (the MQTT edit loop will handle it when the next retained message arrives). Simplify accordingly — for resync, we mainly need the CRDT state initialized; the disk state catches up via normal MQTT edit processing.

If `read_schema_json` doesn't exist, simplify Step 3 of the function: after cyan sync succeeds, just log success. The MQTT edit loop will process the next retained message and write the schema file.

**Step 3: Build and verify**

Run: `cargo build`
Expected: Compiles with no errors.

**Step 4: Commit**

```bash
git add src/sync/transport/subscriptions.rs
git commit -m "Replace resync_schema_from_server with cyan sync (CP-uals)"
```

---

### Task 5: Replace `resync_subdir_schema_from_server()` with cyan sync

**Files:**
- Modify: `src/sync/transport/subscriptions.rs:1490-1600`

**Step 1: Replace the function body**

Same pattern as Task 4 but for subdirectory schemas. Replace the body:

```rust
async fn resync_subdir_schema_from_server(
    http_client: &Client,
    server: &str,
    subdir_node_id: &str,
    subdir_path: &str,
    crdt_context: &CrdtFileSyncContext,
    directory: &Path,
) {
    warn!(
        "Resyncing subdirectory schema CRDT state via cyan sync for {} ({})",
        subdir_path, subdir_node_id
    );

    // Parse subdir_node_id as UUID for cache lookup
    let subdir_uuid = match Uuid::parse_str(subdir_node_id) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "Subdir schema resync: Failed to parse subdir_node_id {} as UUID: {}",
                subdir_node_id, e
            );
            return;
        }
    };

    let subdir_full_path = directory.join(subdir_path);

    // Step 1: Mark as needing resync
    {
        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                state.schema.mark_needs_resync();
                debug!(
                    "Marked subdir schema {} as needing resync",
                    subdir_node_id
                );
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to load cached state for {}: {}",
                    subdir_path, e
                );
                return;
            }
        }
    }

    // Step 2: Initialize via cyan sync
    let sync_client_id = uuid::Uuid::new_v4().to_string();
    let initialized = {
        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                sync_schema_via_cyan(
                    &crdt_context.mqtt_client,
                    &crdt_context.workspace,
                    subdir_node_id,
                    &sync_client_id,
                    &mut state.schema,
                )
                .await
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to reload state for {}: {}",
                    subdir_path, e
                );
                false
            }
        }
    };

    if initialized {
        info!(
            "Subdir schema resync via cyan completed for {} ({})",
            subdir_path, subdir_node_id
        );
    } else {
        warn!(
            "Subdir schema resync: Cyan sync failed for {} — falling back to HTTP",
            subdir_path
        );

        // Fall back to HTTP fetch
        let head = match fetch_head(http_client, server, &encode_node_id(subdir_node_id), false).await {
            Ok(Some(h)) => h,
            Ok(None) => {
                warn!(
                    "Subdir schema resync: Document {} not found on server",
                    subdir_node_id
                );
                return;
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to fetch HEAD for {}: {}",
                    subdir_node_id, e
                );
                return;
            }
        };

        match crdt_context
            .subdir_cache
            .get_or_load(&subdir_full_path, subdir_uuid)
            .await
        {
            Ok(subdir_state) => {
                let mut state = subdir_state.write().await;
                if let Some(ref server_state) = head.state {
                    let cid = head.cid.as_deref().unwrap_or("unknown");
                    state.schema.initialize_from_server(server_state, cid);
                    info!(
                        "Subdir schema resync: HTTP fallback initialized for {} at cid={}",
                        subdir_path, cid
                    );
                } else {
                    state.schema.initialize_empty();
                }
            }
            Err(e) => {
                warn!(
                    "Subdir schema resync: Failed to update state for {}: {}",
                    subdir_path, e
                );
            }
        }
    }
}
```

**Step 2: Build and verify**

Run: `cargo build`
Expected: Compiles with no errors.

**Step 3: Commit**

```bash
git add src/sync/transport/subscriptions.rs
git commit -m "Replace resync_subdir_schema_from_server with cyan sync (CP-uals)"
```

---

### Task 6: Run tests and clippy

**Step 1: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass (632+).

**Step 2: Run clippy**

Run: `cargo clippy`
Expected: No new warnings. Fix any issues.

**Step 3: Commit any clippy fixes**

```bash
git add -u
git commit -m "Fix clippy warnings from cyan sync implementation"
```

---

### Task 7: Integration test — orchestrator startup

Test that schema CRDT state is properly initialized on startup and files are created correctly.

**Step 1: Build release**

Run: `cargo build --release`

**Step 2: Stop existing orchestrator**

Run: `pkill -f commonplace-orchestrator` (if running)

**Step 3: Clean state**

```bash
git checkout -- workspace/
rm -f /tmp/commonplace-orchestrator-*.status.json
rm -rf /tmp/commonplace-sandbox-*
```

**Step 4: Start orchestrator**

```bash
RUST_LOG=info ./target/release/commonplace-orchestrator > /tmp/orchestrator-uals.log 2>&1 &
```

**Step 5: Wait and verify (30s)**

```bash
sleep 30
```

**Step 6: Check logs for cyan sync**

```bash
grep "CYAN-SYNC" /tmp/orchestrator-uals.log
```

Expected: `[CYAN-SYNC] Schema CRDT initialized for doc=...` messages for root and subdirectories. No timeout warnings.

**Step 7: Verify no spurious deletions**

```bash
ls workspace/bartleby/
```

Expected: All files present (test-note.txt, prompts.txt, etc.)

**Step 8: Verify processes running**

```bash
./target/release/commonplace-ps
```

Expected: All processes in Running state.

---

### Task 8: Integration test — file creation propagation (C2/C3)

These tests previously failed because sandbox schema CRDT was uninitialized.

**Step 1: Create a file in workspace**

```bash
echo "cyan test" > workspace/text-to-telegram/cyan-test.txt
```

**Step 2: Wait for sync**

```bash
sleep 15
```

**Step 3: Check sandbox**

Find the text-to-telegram sandbox directory:
```bash
ls /tmp/commonplace-sandbox-*/text-to-telegram/cyan-test.txt
```

Expected: File exists in sandbox with content "cyan test".

**Step 4: Check logs**

```bash
grep "cyan-test" /tmp/orchestrator-uals.log
```

Expected: Shows file sync activity, no errors.

**Step 5: Clean up test file**

```bash
rm workspace/text-to-telegram/cyan-test.txt
sleep 10
```

---

### Task 9: Integration test — file deletion propagation (D4)

**Step 1: Create and sync a test file**

```bash
echo "delete me" > workspace/text-to-telegram/delete-test.txt
sleep 15
```

**Step 2: Verify file exists in sandbox**

```bash
ls /tmp/commonplace-sandbox-*/text-to-telegram/delete-test.txt
```

Expected: File exists.

**Step 3: Delete the file**

```bash
rm workspace/text-to-telegram/delete-test.txt
sleep 15
```

**Step 4: Verify file deleted from sandbox**

```bash
ls /tmp/commonplace-sandbox-*/text-to-telegram/delete-test.txt 2>&1
```

Expected: "No such file or directory" — file has been deleted from sandbox.

**Step 5: Check CRDT-DELETE logs**

```bash
grep "CRDT-DELETE.*delete-test" /tmp/orchestrator-uals.log
```

Expected: Shows explicit deletion detected and applied.

**Step 6: Clean up**

```bash
pkill -f commonplace-orchestrator
```

---

### Task 10: Push and update beads

**Step 1: Push all commits**

```bash
git push
```

**Step 2: Close CP-uals**

```bash
bd close CP-uals --reason="Implemented cyan sync for schema CRDT initialization. HTTP bootstrap replaced with MQTT Ancestors protocol."
```

**Step 3: Sync beads**

```bash
bd sync
```

---

## Verification Checklist

After all tasks:

1. **Unit tests**: `cargo test --lib` — all pass
2. **Clippy**: `cargo clippy` — no warnings
3. **Startup test**: Orchestrator starts, `[CYAN-SYNC]` logs show successful init, no file deletions
4. **File creation (C2/C3)**: New file in workspace propagates to sandbox
5. **File deletion (D4)**: Deleted file in workspace propagates deletion to sandbox
6. **Log check**: `grep "CYAN-SYNC"` shows initialization; no timeout warnings in steady state
