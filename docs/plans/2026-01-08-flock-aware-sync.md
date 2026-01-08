# Flock-Aware Sync Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent sync from overwriting local edits by using flock coordination and commit ancestry checking.

**Architecture:** Add per-path state tracking (pending_outbound, pending_inbound) to the sync client. Before writing inbound updates, verify all local pending commits are ancestors of the incoming commit. Use flock to detect when agents are actively editing files.

**Tech Stack:** Rust, libc (flock), tokio (async), redb (commit storage)

---

## Task 1: Add is_ancestor function to replay.rs

**Files:**
- Modify: `src/replay.rs`
- Modify: `src/store.rs` (if needed for iteration)

**Step 1: Write the failing test**

Add to `src/replay.rs`:

```rust
#[cfg(test)]
mod ancestry_tests {
    use super::*;
    use crate::store::CommitStore;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn test_is_ancestor_same_commit() {
        let dir = tempdir().unwrap();
        let store = CommitStore::new(dir.path().join("test.redb")).unwrap();
        let doc_id = Uuid::new_v4();

        // Create initial commit
        let cid = store.store_commit(&doc_id, &[], b"content1").unwrap();

        assert!(is_ancestor(&store, &doc_id, &cid, &cid).unwrap());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test ancestry_tests::test_is_ancestor_same_commit -- --nocapture`
Expected: FAIL with "cannot find function `is_ancestor`"

**Step 3: Write minimal implementation**

Add to `src/replay.rs`:

```rust
use std::collections::{HashSet, VecDeque};

/// Check if `ancestor` is an ancestor of `descendant` in the commit DAG.
/// Returns true if ancestor == descendant or if there's a path from descendant back to ancestor.
pub fn is_ancestor(
    store: &CommitStore,
    doc_id: &Uuid,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, ReplayError> {
    if ancestor == descendant {
        return Ok(true);
    }

    // BFS backwards from descendant
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(descendant.to_string());

    while let Some(cid) = queue.pop_front() {
        if !visited.insert(cid.clone()) {
            continue;
        }

        if let Some(commit) = store.get_commit(doc_id, &cid)? {
            for parent in &commit.parents {
                if parent == ancestor {
                    return Ok(true);
                }
                queue.push_back(parent.clone());
            }
        }
    }

    Ok(false)
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test ancestry_tests::test_is_ancestor_same_commit -- --nocapture`
Expected: PASS

**Step 5: Add test for linear chain**

```rust
#[test]
fn test_is_ancestor_linear_chain() {
    let dir = tempdir().unwrap();
    let store = CommitStore::new(dir.path().join("test.redb")).unwrap();
    let doc_id = Uuid::new_v4();

    // Create chain: c1 <- c2 <- c3
    let c1 = store.store_commit(&doc_id, &[], b"v1").unwrap();
    let c2 = store.store_commit(&doc_id, &[c1.clone()], b"v2").unwrap();
    let c3 = store.store_commit(&doc_id, &[c2.clone()], b"v3").unwrap();

    // c1 is ancestor of c3
    assert!(is_ancestor(&store, &doc_id, &c1, &c3).unwrap());
    // c2 is ancestor of c3
    assert!(is_ancestor(&store, &doc_id, &c2, &c3).unwrap());
    // c3 is NOT ancestor of c1
    assert!(!is_ancestor(&store, &doc_id, &c3, &c1).unwrap());
}
```

**Step 6: Run tests**

Run: `cargo test ancestry_tests -- --nocapture`
Expected: PASS

**Step 7: Add test for branching/merge**

```rust
#[test]
fn test_is_ancestor_with_merge() {
    let dir = tempdir().unwrap();
    let store = CommitStore::new(dir.path().join("test.redb")).unwrap();
    let doc_id = Uuid::new_v4();

    // Create DAG:
    //   c1 <- c2
    //    \     \
    //     <- c3 <- c4 (merge)
    let c1 = store.store_commit(&doc_id, &[], b"v1").unwrap();
    let c2 = store.store_commit(&doc_id, &[c1.clone()], b"v2").unwrap();
    let c3 = store.store_commit(&doc_id, &[c1.clone()], b"v3").unwrap();
    let c4 = store.store_commit(&doc_id, &[c2.clone(), c3.clone()], b"v4").unwrap();

    // c1, c2, c3 are all ancestors of c4
    assert!(is_ancestor(&store, &doc_id, &c1, &c4).unwrap());
    assert!(is_ancestor(&store, &doc_id, &c2, &c4).unwrap());
    assert!(is_ancestor(&store, &doc_id, &c3, &c4).unwrap());
    // c2 is NOT ancestor of c3 (siblings)
    assert!(!is_ancestor(&store, &doc_id, &c2, &c3).unwrap());
}
```

**Step 8: Run all ancestry tests**

Run: `cargo test ancestry_tests -- --nocapture`
Expected: PASS

**Step 9: Commit**

```bash
git add src/replay.rs
git commit -m "feat(replay): add is_ancestor function for commit DAG queries

Implements BFS traversal from descendant to ancestor to check if one
commit is reachable from another in the commit history."
```

---

## Task 2: Add /docs/{id}/is-ancestor API endpoint

**Files:**
- Modify: `src/api.rs`
- Modify: `src/lib.rs` (router)

**Step 1: Write the failing integration test**

Add to `tests/api_tests.rs`:

```rust
#[tokio::test]
async fn test_is_ancestor_endpoint() {
    let app = create_test_app_with_database().await;

    // Create a document
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/docs")
                .header("Content-Type", "text/plain")
                .body(Body::from("initial"))
                .unwrap(),
        )
        .await
        .unwrap();

    let doc_id = get_doc_id_from_response(response).await;

    // Make a commit
    let commit1_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .header("Content-Type", "text/plain")
                .body(Body::from("version 2"))
                .unwrap(),
        )
        .await
        .unwrap();

    let commit1: serde_json::Value = get_json_body(commit1_response).await;
    let cid1 = commit1["cid"].as_str().unwrap();

    // Make another commit
    let commit2_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/docs/{}/replace", doc_id))
                .header("Content-Type", "text/plain")
                .body(Body::from("version 3"))
                .unwrap(),
        )
        .await
        .unwrap();

    let commit2: serde_json::Value = get_json_body(commit2_response).await;
    let cid2 = commit2["cid"].as_str().unwrap();

    // Check ancestry: cid1 should be ancestor of cid2
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/docs/{}/is-ancestor?ancestor={}&descendant={}",
                    doc_id, cid1, cid2
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = get_json_body(response).await;
    assert_eq!(body["is_ancestor"], true);

    // Check non-ancestry: cid2 should NOT be ancestor of cid1
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/docs/{}/is-ancestor?ancestor={}&descendant={}",
                    doc_id, cid2, cid1
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = get_json_body(response).await;
    assert_eq!(body["is_ancestor"], false);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_is_ancestor_endpoint -- --nocapture`
Expected: FAIL with 404 (route not found)

**Step 3: Add query and response types to api.rs**

```rust
#[derive(Debug, Deserialize)]
pub struct AncestorQuery {
    ancestor: String,
    descendant: String,
}

#[derive(Debug, Serialize)]
pub struct AncestorResponse {
    is_ancestor: bool,
}
```

**Step 4: Add the handler to api.rs**

```rust
pub async fn is_ancestor_handler(
    Path(doc_id): Path<Uuid>,
    Query(params): Query<AncestorQuery>,
    State(state): State<AppState>,
) -> Result<Json<AncestorResponse>, (StatusCode, String)> {
    let store = state.commit_store.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Database not configured".to_string(),
    ))?;

    let is_ancestor = crate::replay::is_ancestor(store, &doc_id, &params.ancestor, &params.descendant)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(AncestorResponse { is_ancestor }))
}
```

**Step 5: Wire up the route in lib.rs**

Add to the router:

```rust
.route("/docs/:id/is-ancestor", get(api::is_ancestor_handler))
```

**Step 6: Run test to verify it passes**

Run: `cargo test test_is_ancestor_endpoint -- --nocapture`
Expected: PASS

**Step 7: Commit**

```bash
git add src/api.rs src/lib.rs tests/api_tests.rs
git commit -m "feat(api): add /docs/{id}/is-ancestor endpoint

Returns whether one commit is an ancestor of another in the DAG.
Used by sync client to verify local edits are included in server updates."
```

---

## Task 3: Create flock helper module

**Files:**
- Create: `src/sync/flock.rs`
- Modify: `src/sync/mod.rs`

**Step 1: Create the flock module with types**

Create `src/sync/flock.rs`:

```rust
//! File locking utilities for sync coordination with agents.
//!
//! Uses flock(2) to coordinate access to files between the sync client
//! and agents like Bartleby that hold files open while editing.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

/// Default timeout for acquiring flock before proceeding anyway
pub const FLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between flock retry attempts
pub const FLOCK_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Result of attempting to acquire a file lock
pub enum FlockResult {
    /// Lock was acquired successfully
    Acquired(FlockGuard),
    /// Timed out waiting for lock - proceeding anyway
    Timeout,
}

/// RAII guard that releases the flock when dropped
pub struct FlockGuard {
    file: File,
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        // SAFETY: flock with LOCK_UN is safe on a valid file descriptor
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

impl FlockGuard {
    /// Get a reference to the underlying file
    pub fn file(&self) -> &File {
        &self.file
    }
}
```

**Step 2: Add to mod.rs**

Add to `src/sync/mod.rs`:

```rust
pub mod flock;
```

**Step 3: Run clippy to check syntax**

Run: `cargo clippy --lib -p commonplace-doc -- -D warnings`
Expected: PASS (or warnings to fix)

**Step 4: Add the try_flock_exclusive function**

Add to `src/sync/flock.rs`:

```rust
/// Attempt to acquire an exclusive lock on a file, retrying until timeout.
///
/// This is used before writing to a file to ensure no agent is actively editing it.
/// If the lock cannot be acquired within FLOCK_TIMEOUT, proceeds anyway with a warning.
///
/// # Arguments
/// * `path` - Path to the file to lock
/// * `timeout` - Maximum time to wait for lock (defaults to FLOCK_TIMEOUT)
///
/// # Returns
/// * `FlockResult::Acquired` - Lock acquired, write can proceed safely
/// * `FlockResult::Timeout` - Timed out, proceeding anyway (agent's problem)
pub async fn try_flock_exclusive(path: &Path, timeout: Option<Duration>) -> io::Result<FlockResult> {
    let timeout = timeout.unwrap_or(FLOCK_TIMEOUT);
    let start = Instant::now();

    loop {
        // Try to open the file
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // File doesn't exist, no lock needed
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        let fd = file.as_raw_fd();

        // Try non-blocking exclusive lock
        // SAFETY: flock is safe on a valid file descriptor
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };

        if result == 0 {
            return Ok(FlockResult::Acquired(FlockGuard { file }));
        }

        // Check if we should give up
        if start.elapsed() >= timeout {
            tracing::warn!(
                ?path,
                elapsed = ?start.elapsed(),
                "flock timeout after {:?}, proceeding anyway",
                timeout
            );
            return Ok(FlockResult::Timeout);
        }

        // Wait and retry
        tokio::time::sleep(FLOCK_RETRY_INTERVAL).await;
    }
}
```

**Step 5: Add a unit test**

Add to `src/sync/flock.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_flock_acquires_on_unlocked_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();

        let result = try_flock_exclusive(file.path(), Some(Duration::from_secs(1)))
            .await
            .unwrap();

        assert!(matches!(result, FlockResult::Acquired(_)));
    }

    #[tokio::test]
    async fn test_flock_returns_not_found_for_missing_file() {
        let result = try_flock_exclusive(Path::new("/nonexistent/file"), Some(Duration::from_secs(1))).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
}
```

**Step 6: Run tests**

Run: `cargo test sync::flock::tests -- --nocapture`
Expected: PASS

**Step 7: Add test for timeout behavior**

```rust
#[tokio::test]
async fn test_flock_times_out_when_locked() {
    use std::os::unix::io::AsRawFd;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(b"test content").unwrap();

    // Hold an exclusive lock
    let lock_file = File::open(file.path()).unwrap();
    unsafe {
        libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX);
    }

    // Try to acquire with short timeout
    let start = Instant::now();
    let result = try_flock_exclusive(file.path(), Some(Duration::from_millis(200)))
        .await
        .unwrap();

    assert!(matches!(result, FlockResult::Timeout));
    assert!(start.elapsed() >= Duration::from_millis(200));

    // Release the lock
    unsafe {
        libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN);
    }
}
```

**Step 8: Run all flock tests**

Run: `cargo test sync::flock::tests -- --nocapture`
Expected: PASS

**Step 9: Commit**

```bash
git add src/sync/flock.rs src/sync/mod.rs
git commit -m "feat(sync): add flock helper module for agent coordination

Provides try_flock_exclusive() that attempts to acquire LOCK_EX with
retry and timeout. Returns Acquired or Timeout so caller can decide
whether to proceed."
```

---

## Task 4: Create PathState and InboundWrite types

**Files:**
- Create: `src/sync/flock_state.rs`
- Modify: `src/sync/mod.rs`

**Step 1: Create the state types**

Create `src/sync/flock_state.rs`:

```rust
//! Per-path state for flock-aware sync.
//!
//! Tracks pending outbound commits and queued inbound writes to ensure
//! we never overwrite local edits with stale server state.

use bytes::Bytes;
use std::collections::HashSet;
use std::time::Instant;

/// State tracked per synced file path
#[derive(Debug)]
pub struct PathState {
    /// Current inode for this path (for shadow tracking)
    pub current_inode: u64,

    /// Commits uploaded but not yet confirmed in server response.
    /// We must verify all of these are ancestors of incoming commits
    /// before writing inbound updates.
    pub pending_outbound: HashSet<String>,

    /// Server update waiting to be written.
    /// Blocked by pending_outbound or flock contention.
    pub pending_inbound: Option<InboundWrite>,
}

/// A queued inbound write from the server
#[derive(Debug, Clone)]
pub struct InboundWrite {
    /// Content to write
    pub content: Bytes,

    /// Commit ID of this update
    pub commit_id: String,

    /// When we received this update
    pub received_at: Instant,
}

impl PathState {
    /// Create new state for a path
    pub fn new(inode: u64) -> Self {
        Self {
            current_inode: inode,
            pending_outbound: HashSet::new(),
            pending_inbound: None,
        }
    }

    /// Check if we have any pending outbound commits
    pub fn has_pending_outbound(&self) -> bool {
        !self.pending_outbound.is_empty()
    }

    /// Record that we uploaded a commit
    pub fn add_pending_outbound(&mut self, commit_id: String) {
        self.pending_outbound.insert(commit_id);
    }

    /// Remove confirmed commits from pending_outbound
    pub fn confirm_outbound(&mut self, commit_ids: &[String]) {
        for cid in commit_ids {
            self.pending_outbound.remove(cid);
        }
    }

    /// Queue an inbound write (replaces any existing queued write)
    pub fn queue_inbound(&mut self, content: Bytes, commit_id: String) {
        self.pending_inbound = Some(InboundWrite {
            content,
            commit_id,
            received_at: Instant::now(),
        });
    }

    /// Take the pending inbound write if any
    pub fn take_pending_inbound(&mut self) -> Option<InboundWrite> {
        self.pending_inbound.take()
    }
}
```

**Step 2: Add to mod.rs**

Add to `src/sync/mod.rs`:

```rust
pub mod flock_state;
```

**Step 3: Add unit tests**

Add to `src/sync/flock_state.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state_has_no_pending() {
        let state = PathState::new(12345);
        assert!(!state.has_pending_outbound());
        assert!(state.pending_inbound.is_none());
    }

    #[test]
    fn test_add_and_confirm_outbound() {
        let mut state = PathState::new(12345);

        state.add_pending_outbound("cid1".to_string());
        state.add_pending_outbound("cid2".to_string());

        assert!(state.has_pending_outbound());
        assert_eq!(state.pending_outbound.len(), 2);

        state.confirm_outbound(&["cid1".to_string()]);
        assert!(state.has_pending_outbound());
        assert_eq!(state.pending_outbound.len(), 1);

        state.confirm_outbound(&["cid2".to_string()]);
        assert!(!state.has_pending_outbound());
    }

    #[test]
    fn test_queue_inbound_replaces_existing() {
        let mut state = PathState::new(12345);

        state.queue_inbound(Bytes::from("content1"), "cid1".to_string());
        assert_eq!(state.pending_inbound.as_ref().unwrap().commit_id, "cid1");

        state.queue_inbound(Bytes::from("content2"), "cid2".to_string());
        assert_eq!(state.pending_inbound.as_ref().unwrap().commit_id, "cid2");
    }

    #[test]
    fn test_take_pending_inbound() {
        let mut state = PathState::new(12345);

        state.queue_inbound(Bytes::from("content"), "cid".to_string());

        let taken = state.take_pending_inbound();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().commit_id, "cid");

        // Second take returns None
        assert!(state.take_pending_inbound().is_none());
    }
}
```

**Step 4: Run tests**

Run: `cargo test sync::flock_state::tests -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/sync/flock_state.rs src/sync/mod.rs
git commit -m "feat(sync): add PathState and InboundWrite types

Tracks pending_outbound commits (HashSet) and pending_inbound writes
per path. Used to coordinate between upload and SSE write paths."
```

---

## Task 5: Add ancestry checking client helper

**Files:**
- Modify: `src/sync/urls.rs` (add URL builder)
- Create: `src/sync/ancestry.rs` (client helper)
- Modify: `src/sync/mod.rs`

**Step 1: Add URL builder for is-ancestor endpoint**

Add to `src/sync/urls.rs` in the appropriate section:

```rust
/// Build URL for ancestry check endpoint
pub fn build_is_ancestor_url(base: &str, doc_id: &Uuid, ancestor: &str, descendant: &str) -> String {
    format!(
        "{}/docs/{}/is-ancestor?ancestor={}&descendant={}",
        base.trim_end_matches('/'),
        doc_id,
        urlencoding::encode(ancestor),
        urlencoding::encode(descendant)
    )
}
```

**Step 2: Add URL builder test**

Add to `src/sync/urls.rs` tests:

```rust
#[test]
fn test_build_is_ancestor_url() {
    let url = build_is_ancestor_url(
        "http://localhost:3000",
        &Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        "abc123",
        "def456",
    );
    assert_eq!(
        url,
        "http://localhost:3000/docs/550e8400-e29b-41d4-a716-446655440000/is-ancestor?ancestor=abc123&descendant=def456"
    );
}
```

**Step 3: Run URL test**

Run: `cargo test test_build_is_ancestor_url -- --nocapture`
Expected: PASS

**Step 4: Create ancestry client module**

Create `src/sync/ancestry.rs`:

```rust
//! Client helper for checking commit ancestry.

use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;
use uuid::Uuid;

use super::urls::build_is_ancestor_url;

#[derive(Debug, Deserialize)]
struct AncestorResponse {
    is_ancestor: bool,
}

/// Check if all commits in `pending` are ancestors of `descendant`.
///
/// Returns true only if every pending commit is an ancestor.
/// Used to verify local edits are included in server's merged state.
pub async fn all_are_ancestors(
    client: &Client,
    server_url: &str,
    doc_id: &Uuid,
    pending: &HashSet<String>,
    descendant: &str,
) -> Result<bool, reqwest::Error> {
    for ancestor in pending {
        let url = build_is_ancestor_url(server_url, doc_id, ancestor, descendant);
        let response: AncestorResponse = client.get(&url).send().await?.json().await?;

        if !response.is_ancestor {
            tracing::debug!(
                ancestor,
                descendant,
                "pending commit is not ancestor of incoming"
            );
            return Ok(false);
        }
    }

    Ok(true)
}
```

**Step 5: Add to mod.rs**

Add to `src/sync/mod.rs`:

```rust
pub mod ancestry;
```

**Step 6: Run clippy**

Run: `cargo clippy --lib -p commonplace-doc -- -D warnings`
Expected: PASS

**Step 7: Commit**

```bash
git add src/sync/urls.rs src/sync/ancestry.rs src/sync/mod.rs
git commit -m "feat(sync): add ancestry checking client helper

Provides all_are_ancestors() that checks each pending_outbound commit
against the incoming commit via the server's is-ancestor endpoint."
```

---

## Task 6: Integrate flock + ancestry into SSE write path

**Files:**
- Modify: `src/sync/sse.rs` or `src/sync/file_sync.rs` (wherever SSE writes happen)

**Step 1: Read the current SSE write code**

Run: `grep -n "write" src/sync/sse.rs | head -30`

Identify where inbound writes happen. Look for `tokio::fs::write` or `atomic_write_with_shadow`.

**Step 2: Create a new write_with_flock_check function**

This step depends on the exact structure found. The general pattern:

```rust
use crate::sync::flock::{try_flock_exclusive, FlockResult};
use crate::sync::flock_state::PathState;
use crate::sync::ancestry::all_are_ancestors;

/// Write an inbound update with flock and ancestry checking.
///
/// Returns Ok(true) if written, Ok(false) if queued for later.
pub async fn write_inbound_with_checks(
    path: &Path,
    content: &Bytes,
    commit_id: &str,
    doc_id: &Uuid,
    path_state: &mut PathState,
    client: &Client,
    server_url: &str,
) -> Result<bool, SyncError> {
    // If file doesn't exist, just create it
    if !path.exists() {
        tokio::fs::write(path, content).await?;
        return Ok(true);
    }

    // Check ancestry if we have pending outbound commits
    if path_state.has_pending_outbound() {
        let all_included = all_are_ancestors(
            client,
            server_url,
            doc_id,
            &path_state.pending_outbound,
            commit_id,
        )
        .await?;

        if !all_included {
            // Queue for later - our edits aren't in this update yet
            path_state.queue_inbound(content.clone(), commit_id.to_string());
            tracing::debug!(
                ?path,
                commit_id,
                pending = ?path_state.pending_outbound,
                "queuing inbound - pending outbound not yet merged"
            );
            return Ok(false);
        }
    }

    // Try to acquire flock
    match try_flock_exclusive(path, None).await {
        Ok(FlockResult::Acquired(_guard)) => {
            atomic_write_with_shadow(path, content).await?;
            tracing::debug!(?path, commit_id, "wrote inbound update with flock");
        }
        Ok(FlockResult::Timeout) => {
            // Proceed anyway after timeout
            atomic_write_with_shadow(path, content).await?;
            tracing::warn!(?path, commit_id, "wrote inbound update after flock timeout");
        }
        Err(e) => {
            return Err(SyncError::Io(e));
        }
    }

    Ok(true)
}
```

**Step 3: Update SSE handler to use new function**

Find the SSE event handler and replace direct writes with `write_inbound_with_checks`.

**Step 4: Add PathState storage**

The sync task needs to maintain a `HashMap<PathBuf, PathState>` that persists across SSE events.

**Step 5: Process pending inbound after outbound confirmed**

After clearing confirmed outbound commits, check if pending_inbound can now be written:

```rust
// After confirming outbound commits
if !path_state.has_pending_outbound() {
    if let Some(queued) = path_state.take_pending_inbound() {
        write_inbound_with_checks(
            path,
            &queued.content,
            &queued.commit_id,
            doc_id,
            path_state,
            client,
            server_url,
        )
        .await?;
    }
}
```

**Step 6: Write integration test**

Add to `tests/` a new test file or existing sync tests:

```rust
#[tokio::test]
async fn test_flock_aware_sync_queues_when_pending() {
    // 1. Set up sync with a file
    // 2. Trigger local edit -> adds to pending_outbound
    // 3. Send SSE update that doesn't include local edit
    // 4. Assert: file NOT written, pending_inbound has the update
    // 5. Send SSE update that includes local edit (ancestor check passes)
    // 6. Assert: file written with latest content
}
```

**Step 7: Run tests**

Run: `cargo test test_flock_aware_sync -- --nocapture`
Expected: PASS

**Step 8: Commit**

```bash
git add src/sync/sse.rs src/sync/file_sync.rs
git commit -m "feat(sync): integrate flock + ancestry checking into SSE writes

Inbound updates now:
1. Check if all pending_outbound are ancestors of incoming commit
2. Queue as pending_inbound if not (local edits not yet merged)
3. Acquire flock before writing (timeout after 30s)
4. Process pending_inbound when pending_outbound clears"
```

---

## Task 7: Track pending_outbound in upload path

**Files:**
- Modify: `src/sync/file_sync.rs` or upload handling code

**Step 1: Find the upload code**

Run: `grep -n "upload\|commit_edit\|POST.*replace" src/sync/*.rs | head -20`

**Step 2: Add pending_outbound tracking**

After successfully uploading a local edit, add the returned commit_id to pending_outbound:

```rust
// After successful upload
let response: CommitResponse = client.post(&url).body(content).send().await?.json().await?;
path_state.add_pending_outbound(response.cid.clone());
tracing::debug!(?path, cid = %response.cid, "added to pending_outbound");
```

**Step 3: Add test**

```rust
#[tokio::test]
async fn test_upload_adds_to_pending_outbound() {
    // Setup sync with PathState tracking
    // Make local edit
    // Verify pending_outbound contains the commit_id
}
```

**Step 4: Run test**

Run: `cargo test test_upload_adds_to_pending_outbound -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/sync/file_sync.rs
git commit -m "feat(sync): track pending_outbound on local uploads

After uploading a local edit, adds the commit_id to pending_outbound.
Cleared when SSE confirms the edit is included in server state."
```

---

## Task 8: Handle SSE commit confirmations

**Files:**
- Modify: `src/sync/sse.rs`

**Step 1: Update SSE event handler to clear confirmed commits**

When an SSE commit event arrives, check each pending_outbound against the incoming commit:

```rust
async fn handle_sse_commit_event(
    event: &CommitEvent,
    path_state: &mut PathState,
    client: &Client,
    server_url: &str,
    doc_id: &Uuid,
) -> Result<(), SyncError> {
    let incoming_cid = &event.cid;

    // Find which pending_outbound are now confirmed (ancestors of incoming)
    let mut confirmed = Vec::new();
    for pending in &path_state.pending_outbound {
        if all_are_ancestors(client, server_url, doc_id, &HashSet::from([pending.clone()]), incoming_cid).await? {
            confirmed.push(pending.clone());
        }
    }

    // Clear confirmed
    path_state.confirm_outbound(&confirmed);
    if !confirmed.is_empty() {
        tracing::debug!(?confirmed, remaining = ?path_state.pending_outbound, "confirmed outbound commits");
    }

    // Now try to write the inbound update
    // ... (call write_inbound_with_checks)

    Ok(())
}
```

**Step 2: Run full test suite**

Run: `cargo test -- --nocapture`
Expected: PASS

**Step 3: Commit**

```bash
git add src/sync/sse.rs
git commit -m "feat(sync): clear confirmed commits on SSE events

When SSE commit arrives, checks which pending_outbound are now
ancestors of the incoming commit and clears them. Processes
pending_inbound if all outbound confirmed."
```

---

## Task 9: Final integration test and cleanup

**Files:**
- Create: `tests/flock_sync_tests.rs`

**Step 1: Write comprehensive integration test**

```rust
//! Integration tests for flock-aware sync

use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;

#[tokio::test]
async fn test_inbound_waits_for_pending_outbound() {
    // Full end-to-end test:
    // 1. Start server with database
    // 2. Create document and file
    // 3. Make local edit, upload (pending_outbound)
    // 4. Before confirmation, send SSE update without local edit
    // 5. Verify file NOT updated (pending_inbound queued)
    // 6. Send SSE with merged state
    // 7. Verify file updated
}

#[tokio::test]
async fn test_flock_blocks_concurrent_write() {
    // 1. Create file
    // 2. Hold flock on it (simulating agent)
    // 3. Trigger SSE update
    // 4. Verify update blocked until lock released
}

#[tokio::test]
async fn test_deletion_clears_pending() {
    // 1. Make local edit (pending_outbound)
    // 2. Delete file locally
    // 3. Verify pending_outbound cleared
    // 4. SSE update should not resurrect file
}
```

**Step 2: Run all tests**

Run: `cargo test -- --nocapture`
Expected: PASS

**Step 3: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: PASS

**Step 4: Final commit**

```bash
git add tests/flock_sync_tests.rs
git commit -m "test(sync): add integration tests for flock-aware sync

Covers:
- Inbound waits for pending outbound ancestry
- Flock blocks concurrent writes
- Deletion clears pending state"
```

---

## Summary

| Task | Description | Key Files |
|------|-------------|-----------|
| 1 | is_ancestor in replay.rs | src/replay.rs |
| 2 | /docs/{id}/is-ancestor API | src/api.rs, src/lib.rs |
| 3 | flock helper module | src/sync/flock.rs |
| 4 | PathState types | src/sync/flock_state.rs |
| 5 | ancestry client helper | src/sync/ancestry.rs |
| 6 | SSE write integration | src/sync/sse.rs |
| 7 | Upload pending_outbound | src/sync/file_sync.rs |
| 8 | SSE confirmation handling | src/sync/sse.rs |
| 9 | Integration tests | tests/flock_sync_tests.rs |

After completing all tasks:
```bash
git push origin CP-qt6z/flock-aware-sync
```

Then create PR for review.
