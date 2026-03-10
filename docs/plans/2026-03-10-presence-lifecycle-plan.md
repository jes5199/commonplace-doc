# Presence Lifecycle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement dual-location identity (hot presence in working dirs + cold identity in `__identities/`), orchestrator identity service over MQTT, and heartbeat-gated reaping of stale presence files.

**Architecture:** The orchestrator ensures identity docs exist in `__identities/` before spawning processes, passing the UUID via env var. Sync agents create linked hot presence entries in working dirs. The orchestrator's health loop reaps stale hot presence files when heartbeat red events go silent. Cold identity survives reaping.

**Tech Stack:** Rust, MQTT (rumqttc), Yjs CRDTs, commonplace-types, commonplace-schema

**Design doc:** `docs/plans/2026-03-09-presence-lifecycle-design.md`

---

## Task 1: Add `heartbeat_timeout_seconds` to ActorIO schema

**Files:**
- Modify: `crates/commonplace-types/src/fs/actor.rs`

**Step 1: Write the failing test**

Add to the existing tests in `actor.rs`:

```rust
#[test]
fn test_actor_io_with_heartbeat_timeout() {
    let json = r#"{
        "name": "sync",
        "status": "Active",
        "heartbeat_timeout_seconds": 30
    }"#;
    let io: ActorIO = serde_json::from_str(json).unwrap();
    assert_eq!(io.heartbeat_timeout_seconds, Some(30));
}

#[test]
fn test_actor_io_default_heartbeat_timeout() {
    let json = r#"{"name": "sync", "status": "Active"}"#;
    let io: ActorIO = serde_json::from_str(json).unwrap();
    assert_eq!(io.heartbeat_timeout_seconds, None);
}
```

**Step 2: Run tests to verify failure**

Run: `cargo test -p commonplace-types test_actor_io_with_heartbeat`
Expected: FAIL — field doesn't exist

**Step 3: Add the field**

Add to `ActorIO` struct:

```rust
/// Heartbeat timeout in seconds. If the actor's red event stream
/// goes silent for longer than this, its hot presence file is eligible
/// for reaping. Defaults by extension: .exe=30, .usr=300, .bot=60, .who=60
#[serde(default, skip_serializing_if = "Option::is_none")]
pub heartbeat_timeout_seconds: Option<u64>,
```

Update all existing `ActorIO` struct literals in `src/sync/actor_io.rs` to include `heartbeat_timeout_seconds: None`.

**Step 4: Run tests**

Run: `cargo test -p commonplace-types test_actor_io_with_heartbeat && cargo test --lib`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/commonplace-types/src/fs/actor.rs src/sync/actor_io.rs
git commit -m "feat(schema): add heartbeat_timeout_seconds to ActorIO"
```

---

## Task 2: Add `identity_uuid` to CommonplaceConfig

**Files:**
- Modify: `crates/commonplace-types/src/config.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_config_with_identity_uuid() {
    let json = r#"{"identity_uuid": "abc-123-def"}"#;
    let config: CommonplaceConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.identity_uuid.as_deref(), Some("abc-123-def"));
}
```

**Step 2: Run test to verify failure**

Run: `cargo test -p commonplace-types test_config_with_identity_uuid`
Expected: FAIL

**Step 3: Add the field**

Add to `CommonplaceConfig`:

```rust
/// Identity UUID for presence lifecycle.
/// When set, the sync agent creates a linked hot presence entry
/// pointing to this UUID (which should exist in __identities/).
#[serde(default, skip_serializing_if = "Option::is_none")]
pub identity_uuid: Option<String>,
```

Add helper method:

```rust
pub fn identity_uuid_or(&self, default: &str) -> String {
    self.identity_uuid.clone().unwrap_or_else(|| default.to_string())
}
```

**Step 4: Run tests**

Run: `cargo test -p commonplace-types test_config_with_identity`
Expected: PASS

**Step 5: Add `--identity-uuid` CLI flag to sync binary**

In `src/bin/sync.rs`, add to the `Args` struct:

```rust
/// Identity UUID for linked presence (from __identities/ registry).
/// When set, creates a hot presence entry linked to this UUID.
/// When absent, creates an ephemeral presence with a fresh UUID.
#[arg(long, env = "COMMONPLACE_IDENTITY_UUID")]
identity_uuid: Option<String>,
```

After config resolution, resolve it:

```rust
let identity_uuid = args.identity_uuid.or(user_config.identity_uuid.clone());
```

**Step 6: Run build**

Run: `cargo build --bin commonplace-sync`
Expected: Clean build

**Step 7: Commit**

```bash
git add crates/commonplace-types/src/config.rs src/bin/sync.rs
git commit -m "feat(config): add identity_uuid field for presence lifecycle"
```

---

## Task 3: Orchestrator identity ensure — MQTT API

**Files:**
- Create: `src/orchestrator/identity.rs`
- Modify: `src/orchestrator/mod.rs`
- Modify: `src/mqtt/request.rs` (add identity commands to MqttRequestClient)

**Step 1: Define the request/response types**

Create `src/orchestrator/identity.rs`:

```rust
//! Identity management for the orchestrator.
//!
//! Provides MQTT API for creating and querying permanent identity records
//! in the __identities/ directory.

use serde::{Deserialize, Serialize};

/// Request to ensure an identity exists.
#[derive(Debug, Serialize, Deserialize)]
pub struct EnsureIdentityRequest {
    pub req: String,
    pub name: String,
    pub extension: String,
    /// Path to the repo root (e.g., "/myapp")
    pub repo_path: String,
}

/// Response from identity ensure.
#[derive(Debug, Serialize, Deserialize)]
pub struct EnsureIdentityResponse {
    pub req: String,
    pub uuid: String,
    pub identity_path: String,
}

/// Request to list identities.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIdentitiesRequest {
    pub req: String,
    pub repo_path: String,
}

/// A single identity entry.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityEntry {
    pub name: String,
    pub extension: String,
    pub uuid: String,
    pub status: Option<String>,
}

/// Response from identity list.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIdentitiesResponse {
    pub req: String,
    pub identities: Vec<IdentityEntry>,
}

/// Default heartbeat timeout in seconds by extension.
pub fn default_heartbeat_timeout(extension: &str) -> u64 {
    match extension {
        "exe" => 30,
        "usr" => 300,
        "bot" => 60,
        _ => 60,
    }
}
```

**Step 2: Wire into orchestrator mod.rs**

Add `pub mod identity;` to `src/orchestrator/mod.rs`.

**Step 3: Run build**

Run: `cargo build`
Expected: Clean build (types only, no handler yet)

**Step 4: Commit**

```bash
git add src/orchestrator/identity.rs src/orchestrator/mod.rs
git commit -m "feat(identity): define MQTT request/response types for identity service"
```

---

## Task 4: Implement identity ensure handler in orchestrator

**Files:**
- Modify: `src/orchestrator/identity.rs`
- Modify: `src/orchestrator/manager.rs` or `src/bin/orchestrator.rs`

This task implements the server-side handler for `{workspace}/commands/__system/ensure-identity`. When received, it:

1. Checks if `__identities/{name}.{ext}.json` exists in the repo's schema
2. If yes: reads the doc UUID and returns it
3. If no: creates a new doc with ActorIO content, adds schema entry in `__identities/`, returns new UUID

**Step 1: Add the ensure handler function**

In `src/orchestrator/identity.rs`, add:

```rust
use crate::mqtt::MqttRequestClient;
use commonplace_types::fs::actor::{ActorIO, ActorStatus};
use uuid::Uuid;

/// Ensure an identity exists in __identities/ for the given actor.
///
/// Returns the UUID of the identity document (existing or newly created).
pub async fn ensure_identity(
    mqtt_client: &MqttRequestClient,
    workspace: &str,
    repo_path: &str,
    name: &str,
    extension: &str,
) -> Result<(String, String), String> {
    let identity_filename = format!("{}.{}.json", name, extension);
    let identity_path = format!("{}/__identities/{}", repo_path.trim_start_matches('/'), identity_filename);

    // Try to get existing identity doc
    match mqtt_client.get_info_by_path(&identity_path).await {
        Ok(info) => {
            // Identity already exists
            Ok((info.id, identity_path))
        }
        Err(_) => {
            // Create __identities/ directory if needed, then create the identity doc
            let identities_dir = format!("{}/__identities", repo_path.trim_start_matches('/'));

            // Ensure __identities/ directory exists
            // (This may fail if it already exists — that's fine)
            let _ = mqtt_client.create_directory(&identities_dir).await;

            // Create the identity document with initial ActorIO content
            let timeout = default_heartbeat_timeout(extension);
            let io = ActorIO {
                name: name.to_string(),
                status: ActorStatus::Stopped,
                started_at: None,
                last_heartbeat: None,
                pid: None,
                capabilities: vec![],
                metadata: None,
                docref: None,
                heartbeat_timeout_seconds: Some(timeout),
            };

            let content = serde_json::to_string_pretty(&io)
                .map_err(|e| format!("Failed to serialize identity: {}", e))?;

            let uuid = mqtt_client
                .create_document_at_path(&identity_path, &content, "application/json")
                .await
                .map_err(|e| format!("Failed to create identity doc: {}", e))?;

            Ok((uuid, identity_path))
        }
    }
}
```

**Note:** The exact MqttRequestClient methods (`get_info_by_path`, `create_directory`, `create_document_at_path`) may need to be added or adapted from existing methods. Check `src/mqtt/request.rs` for available methods and add thin wrappers if needed.

**Step 2: Add MQTT command handler to orchestrator**

Subscribe to `{workspace}/commands/__system/ensure-identity` in the orchestrator's MQTT setup. When a message arrives:

1. Parse `EnsureIdentityRequest`
2. Call `ensure_identity()`
3. Publish `EnsureIdentityResponse` to `{workspace}/responses`

Also subscribe to `{workspace}/commands/__system/list-identities` and implement the list handler by reading `__identities/` schema entries.

**Step 3: Wire the handler into the orchestrator's MQTT subscription loop**

The orchestrator already subscribes to MQTT topics. Add the identity commands to its subscription list and dispatch in its message handler.

**Step 4: Run build and tests**

Run: `cargo build && cargo test --lib`
Expected: Clean build, tests pass

**Step 5: Commit**

```bash
git add src/orchestrator/identity.rs src/orchestrator/manager.rs src/mqtt/request.rs
git commit -m "feat(identity): implement ensure-identity MQTT handler in orchestrator"
```

---

## Task 5: Orchestrator passes identity UUID to sync agents

**Files:**
- Modify: `src/orchestrator/manager.rs`
- Modify: `src/orchestrator/discovered_manager.rs`

**Step 1: Call ensure_identity before spawning sync processes**

In the process spawn flow, before creating the child process:

```rust
// For sync processes, ensure identity exists and pass UUID
if process_name == "sync" || process.command.contains("commonplace-sync") {
    match ensure_identity(&mqtt_client, &workspace, &repo_path, &process_name, "exe").await {
        Ok((uuid, _path)) => {
            cmd.env("COMMONPLACE_IDENTITY_UUID", &uuid);
        }
        Err(e) => {
            warn!("Failed to ensure identity for {}: {}", process_name, e);
            // Continue without identity — sync will use ephemeral mode
        }
    }
}
```

**Step 2: Also handle evaluate processes**

In `discovered_manager.rs`, add the same pattern for discovered processes. The name comes from the process key, extension from the process type (evaluate → "exe", sandbox-exec → "exe").

**Step 3: Run build**

Run: `cargo build`
Expected: Clean build

**Step 4: Commit**

```bash
git add src/orchestrator/manager.rs src/orchestrator/discovered_manager.rs
git commit -m "feat(orchestrator): pass COMMONPLACE_IDENTITY_UUID to spawned sync agents"
```

---

## Task 6: Sync agent creates linked hot presence

**Files:**
- Modify: `src/sync/actor_io.rs`
- Modify: `src/bin/sync.rs`

**Step 1: Add `new_linked` constructor to ActorIOWriter**

```rust
/// Create an IO writer that links to an existing identity UUID.
///
/// The hot presence file in the working directory will reference the same
/// document UUID as the cold identity in __identities/.
pub fn new_linked(directory: &Path, name: &str, extension: &str, _identity_uuid: &str) -> Self {
    // The actual UUID linking happens at the schema level, not the file level.
    // This constructor just records the identity_uuid for later use when
    // writing the presence file (sets docref field).
    let filename = format!("{}.{}", name, extension);
    Self {
        path: directory.join(filename),
        name: name.to_string(),
        pid: std::process::id(),
    }
}
```

**Step 2: Update sync binary to use identity UUID**

In `src/bin/sync.rs`, where `ActorIOWriter::with_collision_check` is called (around line 3081):

```rust
let actor_io = if let Some(ref uuid) = identity_uuid {
    // Linked mode: hot presence shares UUID with __identities/ entry
    // TODO: create linked schema entry pointing to this UUID
    ActorIOWriter::with_collision_check(&directory, &author, "exe").await
} else {
    // Ephemeral mode: standalone presence, no cold identity
    ActorIOWriter::with_collision_check(&directory, &author, "exe").await
};
```

The actual schema linking (making the working dir entry point to the identity UUID) requires adding the file to the schema with a specific `node_id` rather than generating a new one. This uses the same mechanism as `commonplace-link`: set the schema entry's `node_id` to the identity UUID.

**Step 3: Update clean shutdown**

Currently `shutdown()` sets status to Stopped. With linked identity, clean shutdown should:
1. Update cold identity status to Stopped (via MQTT)
2. Delete the hot working dir entry (remove file + schema entry)

```rust
// In the shutdown path of sync.rs:
if identity_uuid.is_some() {
    // Delete hot presence file (identity persists in __identities/)
    if let Err(e) = actor_io.remove().await {
        warn!("Failed to remove hot presence file: {}", e);
    }
} else {
    // Ephemeral: just mark as stopped
    if let Err(e) = actor_io.shutdown().await {
        warn!("Failed to update presence file to stopped: {}", e);
    }
}
```

**Step 4: Run build and tests**

Run: `cargo build && cargo test --lib`
Expected: Clean build, tests pass

**Step 5: Commit**

```bash
git add src/sync/actor_io.rs src/bin/sync.rs
git commit -m "feat(sync): linked hot presence with identity UUID support"
```

---

## Task 7: Orchestrator heartbeat reaper

**Files:**
- Modify: `src/orchestrator/identity.rs`
- Modify: `src/orchestrator/manager.rs`

**Step 1: Implement reaper logic**

Add to `src/orchestrator/identity.rs`:

```rust
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::DateTime;

/// Check if a presence file's heartbeat has expired.
pub fn is_heartbeat_expired(io: &ActorIO) -> bool {
    let timeout = io.heartbeat_timeout_seconds.unwrap_or(60);

    let last_heartbeat = match &io.last_heartbeat {
        Some(hb) => hb,
        None => return true, // No heartbeat ever recorded
    };

    let hb_time = match DateTime::parse_from_rfc3339(last_heartbeat) {
        Ok(t) => t.timestamp() as u64,
        Err(_) => return true, // Can't parse — treat as expired
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    now - hb_time > timeout
}
```

**Step 2: Write tests for the reaper**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expired_heartbeat() {
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: Some("2020-01-01T00:00:00+00:00".to_string()),
            pid: Some(1234),
            capabilities: vec![],
            metadata: None,
            docref: None,
            heartbeat_timeout_seconds: Some(30),
        };
        assert!(is_heartbeat_expired(&io));
    }

    #[test]
    fn test_fresh_heartbeat() {
        let now = chrono::Utc::now().to_rfc3339();
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: Some(now),
            pid: Some(1234),
            capabilities: vec![],
            metadata: None,
            docref: None,
            heartbeat_timeout_seconds: Some(30),
        };
        assert!(!is_heartbeat_expired(&io));
    }

    #[test]
    fn test_no_heartbeat_is_expired() {
        let io = ActorIO {
            name: "sync".to_string(),
            status: ActorStatus::Active,
            started_at: None,
            last_heartbeat: None,
            pid: None,
            capabilities: vec![],
            metadata: None,
            docref: None,
            heartbeat_timeout_seconds: Some(30),
        };
        assert!(is_heartbeat_expired(&io));
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(default_heartbeat_timeout("exe"), 30);
        assert_eq!(default_heartbeat_timeout("usr"), 300);
        assert_eq!(default_heartbeat_timeout("bot"), 60);
        assert_eq!(default_heartbeat_timeout("unknown"), 60);
    }
}
```

**Step 3: Add reaper to orchestrator health loop**

In the orchestrator's 500ms `check_and_restart()` loop, add a periodic reaper check (e.g., every 10 seconds to avoid too much overhead):

```rust
// In check_and_restart or a sibling method:
if self.last_reap_check.elapsed() > Duration::from_secs(10) {
    self.last_reap_check = Instant::now();
    self.reap_stale_presence_files().await;
}
```

The `reap_stale_presence_files()` method:
1. Walks synced directories for presence files (glob `*.exe`, `*.usr`, `*.bot`, etc.)
2. Reads each file, parses ActorIO
3. If `is_heartbeat_expired()` and status is not `Stopped`: delete the file
4. Log the reaping event

**Step 4: Run build and tests**

Run: `cargo build && cargo test --lib`
Expected: Clean build, all tests pass

**Step 5: Commit**

```bash
git add src/orchestrator/identity.rs src/orchestrator/manager.rs
git commit -m "feat(orchestrator): heartbeat reaper for stale presence files"
```

---

## Task 8: Allow `__identities/` through command path validation

**Files:**
- Modify: `src/commands.rs` (around line 105-109)

Currently, paths starting with `__` are rejected:

```rust
if doc_path.starts_with("__") {
    return Err(CommandError::ReservedPath(...));
}
```

This needs to allow `__identities/` specifically (or be relaxed for the orchestrator's internal use). Options:

**A) Whitelist `__identities/`:**
```rust
if doc_path.starts_with("__") && !doc_path.starts_with("__identities/") {
    return Err(CommandError::ReservedPath(...));
}
```

**B) Make it a per-request flag (internal vs external):**
Only validate for external HTTP requests, not internal MQTT commands.

Go with **A** for simplicity.

**Step 1: Update the validation**

**Step 2: Add a test**

```rust
#[test]
fn test_identities_path_allowed() {
    // __identities/ should not be rejected as reserved
    assert!(validate_path("__identities/sync.exe.json").is_ok());
    // Other __ paths should still be rejected
    assert!(validate_path("__private/secret.txt").is_err());
}
```

**Step 3: Run tests**

Run: `cargo test --lib`
Expected: PASS

**Step 4: Commit**

```bash
git add src/commands.rs
git commit -m "feat: allow __identities/ path through command validation"
```

---

## Task 9: Integration test — full lifecycle

**Files:**
- Create: `tests/presence_lifecycle_tests.rs`

Write an integration test that exercises the full lifecycle:

1. Start orchestrator with a repo that has a sync process
2. Verify `__identities/sync.exe.json` was created
3. Verify `main/sync.exe` hot presence file exists and shares the UUID
4. Stop the sync process cleanly
5. Verify hot presence file was deleted
6. Verify cold identity still exists with status Stopped
7. Restart sync process
8. Verify hot presence file re-created with same UUID

This test can use the existing orchestrator test harness pattern from `tests/orchestrator_integration_tests.rs`.

**Step 1: Write the test**

**Step 2: Run test**

Run: `cargo test presence_lifecycle`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/presence_lifecycle_tests.rs
git commit -m "test: integration test for presence lifecycle"
```

---

## Task 10: Final cleanup + close

**Step 1: Run clippy**

Run: `cargo clippy --all-targets`
Expected: No new warnings

**Step 2: Run full test suite**

Run: `cargo test`
Expected: All pass (except pre-existing infra-dependent failures)

**Step 3: Commit any cleanup**

**Step 4: Close beads and push**

```bash
bd close <issue-id>
git push
```

**Step 5: Announce on IRC**

Share the completed work in #loom.
