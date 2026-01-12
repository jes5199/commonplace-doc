# CP-2wie: Initial Sync Callback Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add MQTT event notification when sync initial push completes, allowing orchestrator to react immediately instead of polling.

**Architecture:** Sync publishes `{workspace}/events/sync/initial-complete` after schema+files sync. Orchestrator subscribes if MQTT configured, falls back to existing polling otherwise.

**Tech Stack:** Rust, MQTT (rumqttc), tokio async

---

## Task 1: Add InitialSyncComplete Event Struct

**Files:**
- Modify: `src/sync/types.rs`

**Step 1: Add the event struct**

Add at end of file before closing (after `remove_file_state_and_abort`):

```rust
/// Event published when initial sync completes.
///
/// Published to `{fs_root_id}/events/sync/initial-complete` via MQTT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialSyncComplete {
    /// The filesystem root document ID (workspace name)
    pub fs_root_id: String,
    /// Number of files synced during initial sync
    pub files_synced: usize,
    /// Sync strategy used: "local", "server", or "skip"
    pub strategy: String,
    /// Time taken for initial sync in milliseconds
    pub duration_ms: u64,
}
```

**Step 2: Run cargo check**

Run: `cargo check --lib`
Expected: SUCCESS (no errors)

**Step 3: Commit**

```bash
git add src/sync/types.rs
git commit -m "feat(sync): add InitialSyncComplete event struct"
```

---

## Task 2: Add Event Publishing Helper

**Files:**
- Modify: `src/bin/sync.rs`

**Step 1: Add imports at top of file**

Near the existing imports (around line 9), ensure these are present:

```rust
use commonplace_doc::sync::types::InitialSyncComplete;
use rumqttc::QoS;
use std::time::Instant;
```

**Step 2: Add helper function**

Add after the `use` statements, before `fn main()`:

```rust
/// Publish initial sync complete event via MQTT if available.
async fn publish_initial_sync_complete(
    mqtt_client: &Option<Arc<MqttClient>>,
    fs_root_id: &str,
    files_synced: usize,
    strategy: &str,
    duration_ms: u64,
) {
    if let Some(mqtt) = mqtt_client {
        let event = InitialSyncComplete {
            fs_root_id: fs_root_id.to_string(),
            files_synced,
            strategy: strategy.to_string(),
            duration_ms,
        };
        let topic = format!("{}/events/sync/initial-complete", fs_root_id);
        match serde_json::to_string(&event) {
            Ok(payload) => {
                if let Err(e) = mqtt.publish(&topic, payload.as_bytes(), QoS::AtLeastOnce).await {
                    warn!("Failed to publish initial-sync-complete event: {}", e);
                } else {
                    info!("Published initial-sync-complete event to {}", topic);
                }
            }
            Err(e) => {
                warn!("Failed to serialize initial-sync-complete event: {}", e);
            }
        }
    }
}
```

**Step 3: Run cargo check**

Run: `cargo check --bin commonplace-sync`
Expected: SUCCESS (with warning about unused function - expected for now)

**Step 4: Commit**

```bash
git add src/bin/sync.rs
git commit -m "feat(sync): add publish_initial_sync_complete helper"
```

---

## Task 3: Publish Event in run_directory_mode

**Files:**
- Modify: `src/bin/sync.rs`

**Step 1: Add timing capture**

Find `async fn run_directory_mode` (around line 1072). Add at the start of the function, after the first few variable declarations:

```rust
let sync_start = Instant::now();
```

**Step 2: Publish event after state file save**

Find the "Initial sync complete" log line (around line 1248). After the state file save block (ending around line 1283), add:

```rust
    // Publish initial-sync-complete event via MQTT
    publish_initial_sync_complete(
        &mqtt_client,
        &fs_root_id,
        files.len(),
        initial_sync,
        sync_start.elapsed().as_millis() as u64,
    )
    .await;
```

**Step 3: Run cargo check**

Run: `cargo check --bin commonplace-sync`
Expected: SUCCESS

**Step 4: Commit**

```bash
git add src/bin/sync.rs
git commit -m "feat(sync): publish initial-sync-complete in directory mode"
```

---

## Task 4: Publish Event in run_exec_mode

**Files:**
- Modify: `src/bin/sync.rs`

**Step 1: Verify sync_start already exists**

The `run_exec_mode` function already has `let sync_start = Instant::now();` - verify it exists around line 1670.

**Step 2: Publish event after initial sync log**

Find the "Initial sync complete" log in run_exec_mode (around line 1755). After that log statement (before "Start directory watcher" comment), add:

```rust
    // Publish initial-sync-complete event via MQTT
    publish_initial_sync_complete(
        &mqtt_client,
        &fs_root_id,
        synced_count,
        &initial_sync_strategy,
        sync_start.elapsed().as_millis() as u64,
    )
    .await;
```

**Step 3: Run cargo check**

Run: `cargo check --bin commonplace-sync`
Expected: SUCCESS

**Step 4: Commit**

```bash
git add src/bin/sync.rs
git commit -m "feat(sync): publish initial-sync-complete in exec mode"
```

---

## Task 5: Add MQTT Wait Function to Orchestrator

**Files:**
- Modify: `src/bin/orchestrator.rs`

**Step 1: Add imports**

Near the top of the file, add:

```rust
use commonplace_doc::mqtt::{MqttClient, MqttConfig};
use commonplace_doc::sync::types::InitialSyncComplete;
use rumqttc::QoS;
```

**Step 2: Add wait_for_sync_via_mqtt function**

Add after `wait_for_sync_initial_push` function (around line 276):

```rust
/// Wait for sync initial-complete event via MQTT subscription.
/// Returns the event data on success, or None on timeout.
async fn wait_for_sync_via_mqtt(
    mqtt_client: Arc<MqttClient>,
    fs_root_id: &str,
    max_wait: Duration,
) -> Option<InitialSyncComplete> {
    let topic = format!("{}/events/sync/initial-complete", fs_root_id);

    // Subscribe to the topic
    if let Err(e) = mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await {
        tracing::warn!(
            "[orchestrator] Failed to subscribe to sync-complete event: {}",
            e
        );
        return None;
    }

    tracing::info!(
        "[orchestrator] Subscribed to {} (timeout: {:?})",
        topic,
        max_wait
    );

    // Get message receiver
    let mut receiver = mqtt_client.subscribe_messages();

    // Wait for the event with timeout
    let result = tokio::time::timeout(max_wait, async {
        loop {
            match receiver.recv().await {
                Ok(msg) if msg.topic == topic => {
                    // Parse the event - crash on malformed (indicates bug)
                    let event: InitialSyncComplete = serde_json::from_slice(&msg.payload)
                        .expect("Malformed initial-sync-complete event from sync");
                    return Some(event);
                }
                Ok(_) => {
                    // Message for different topic, continue waiting
                    continue;
                }
                Err(broadcast::RecvError::Lagged(_)) => {
                    // Missed some messages, continue
                    continue;
                }
                Err(broadcast::RecvError::Closed) => {
                    tracing::warn!("[orchestrator] MQTT message channel closed");
                    return None;
                }
            }
        }
    })
    .await;

    match result {
        Ok(Some(event)) => {
            tracing::info!(
                "[orchestrator] Received initial-sync-complete: {} files in {}ms",
                event.files_synced,
                event.duration_ms
            );
            Some(event)
        }
        Ok(None) => None,
        Err(_) => {
            tracing::warn!(
                "[orchestrator] Timed out waiting for sync-complete event after {:?}",
                max_wait
            );
            None
        }
    }
}
```

**Step 3: Add broadcast import**

At the top of the file with other imports, add:

```rust
use tokio::sync::broadcast;
```

**Step 4: Run cargo check**

Run: `cargo check --bin commonplace-orchestrator`
Expected: SUCCESS (with warning about unused function - expected)

**Step 5: Commit**

```bash
git add src/bin/orchestrator.rs
git commit -m "feat(orchestrator): add wait_for_sync_via_mqtt function"
```

---

## Task 6: Integrate MQTT Wait in Orchestrator Startup

**Files:**
- Modify: `src/bin/orchestrator.rs`

**Step 1: Add MQTT connection before sync start**

Find where sync is started (around line 463). Before the `if config.processes.contains_key("sync")` block, add MQTT connection logic:

```rust
    // Connect to MQTT if configured (for sync-complete event subscription)
    let mqtt_client: Option<Arc<MqttClient>> = if !broker_raw.is_empty() {
        let mqtt_config = MqttConfig {
            broker_url: broker_raw.to_string(),
            client_id: format!("orchestrator-{}", std::process::id()),
            keep_alive_secs: 30,
            clean_session: true,
        };
        match MqttClient::connect(mqtt_config).await {
            Ok(client) => {
                let client = Arc::new(client);
                // Start event loop in background
                let client_for_loop = client.clone();
                tokio::spawn(async move {
                    if let Err(e) = client_for_loop.run_event_loop().await {
                        tracing::error!("[orchestrator] MQTT event loop error: {}", e);
                    }
                });
                Some(client)
            }
            Err(e) => {
                tracing::warn!(
                    "[orchestrator] Failed to connect to MQTT ({}), will poll instead: {}",
                    broker_raw,
                    e
                );
                None
            }
        }
    } else {
        None
    };
```

**Step 2: Replace sync wait logic**

Find the existing `wait_for_sync_initial_push` call (around line 473). Replace the wait section with:

```rust
        // Wait for sync to complete initial push
        // Use MQTT subscription if available, otherwise fall back to polling
        let sync_ready = if let Some(ref mqtt) = mqtt_client {
            wait_for_sync_via_mqtt(mqtt.clone(), &fs_root_id, Duration::from_secs(120))
                .await
                .is_some()
        } else {
            wait_for_sync_initial_push(&client, &args.server, &fs_root_id, Duration::from_secs(120))
                .await
        };
```

**Step 3: Run cargo check**

Run: `cargo check --bin commonplace-orchestrator`
Expected: SUCCESS

**Step 4: Commit**

```bash
git add src/bin/orchestrator.rs
git commit -m "feat(orchestrator): use MQTT for sync-complete when available"
```

---

## Task 7: Add Unit Tests

**Files:**
- Modify: `src/sync/types.rs`

**Step 1: Add test for InitialSyncComplete serialization**

Add at end of file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_sync_complete_serialization() {
        let event = InitialSyncComplete {
            fs_root_id: "workspace".to_string(),
            files_synced: 42,
            strategy: "local".to_string(),
            duration_ms: 1523,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"fs_root_id\":\"workspace\""));
        assert!(json.contains("\"files_synced\":42"));
        assert!(json.contains("\"strategy\":\"local\""));
        assert!(json.contains("\"duration_ms\":1523"));

        let parsed: InitialSyncComplete = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.fs_root_id, "workspace");
        assert_eq!(parsed.files_synced, 42);
        assert_eq!(parsed.strategy, "local");
        assert_eq!(parsed.duration_ms, 1523);
    }
}
```

**Step 2: Run the test**

Run: `cargo test --lib test_initial_sync_complete`
Expected: PASS

**Step 3: Commit**

```bash
git add src/sync/types.rs
git commit -m "test(sync): add InitialSyncComplete serialization test"
```

---

## Task 8: Run Full Test Suite

**Step 1: Run library tests**

Run: `cargo test --lib`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy`
Expected: No errors (warnings about too_many_arguments are acceptable)

**Step 3: Run cargo fmt**

Run: `cargo fmt`

**Step 4: Final commit if any formatting changes**

```bash
git add -A
git commit -m "chore: formatting" || echo "No formatting changes"
```

---

## Summary

After completing all tasks:
1. Sync publishes `{workspace}/events/sync/initial-complete` after initial sync
2. Orchestrator subscribes to event if MQTT configured
3. Falls back to polling if no MQTT or connection fails
4. Event includes stats: `fs_root_id`, `files_synced`, `strategy`, `duration_ms`

**Testing:** Run the full orchestrator with MQTT broker and observe logs for:
- `[orchestrator] Subscribed to workspace/events/sync/initial-complete`
- `Published initial-sync-complete event to workspace/events/sync/initial-complete`
- `[orchestrator] Received initial-sync-complete: N files in Xms`
