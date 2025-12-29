# Fix Sync Endpoints Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restore /nodes HTTP endpoints required by commonplace-sync binary

**Architecture:** Add /nodes routes as thin wrappers over /docs + CommitStore. The sync binary expects specific response formats that map cleanly to existing functionality.

**Tech Stack:** Axum routes, existing DocumentStore and CommitStore

---

## Background

The `commonplace-sync` binary uses HTTP `/nodes/*` endpoints that were removed when the node abstraction was deprecated. The sync binary is 1300+ lines and works correctly - we just need to restore the HTTP API it expects.

## Required Endpoints

From `src/bin/sync.rs` analysis:

| Endpoint | Purpose | Maps To |
|----------|---------|---------|
| `POST /nodes` | Create node | `POST /docs` |
| `GET /nodes/:id` | Get node info | `GET /docs/:id` + metadata |
| `GET /nodes/:id/head` | Get HEAD + content + state | CommitStore + DocumentStore |
| `POST /nodes/:id/edit` | Apply Yjs update | `POST /docs/:id/commit` |
| `POST /nodes/:id/replace` | Replace via diff | Diff + commit |
| `POST /nodes/:id/fork` | Fork node | Create + replay commits |
| `GET /sse/nodes/:id` | SSE stream | Existing SSE infra |

---

## Task 1: Add /nodes/:id/head endpoint

The sync binary's most critical endpoint. Returns HEAD commit ID, content, and Yjs state.

**Files:**
- Modify: `src/api.rs`

**Step 1: Add HeadResponse struct**

```rust
#[derive(Serialize)]
struct HeadResponse {
    cid: Option<String>,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
}
```

**Step 2: Add get_node_head handler**

```rust
async fn get_node_head(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<HeadResponse>, StatusCode> {
    let doc = state.doc_store.get_document(&id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    let cid = if let Some(store) = &state.commit_store {
        store.get_document_head(&id).await.ok().flatten()
    } else {
        None
    };

    // Get Yjs state bytes if document has a ydoc
    let state_bytes = state.doc_store.get_yjs_state(&id).await;
    let state_b64 = state_bytes.map(|b| crate::b64::encode(&b));

    Ok(Json(HeadResponse {
        cid,
        content: doc.content,
        state: state_b64,
    }))
}
```

**Step 3: Add get_yjs_state method to DocumentStore**

In `src/document.rs`:

```rust
pub async fn get_yjs_state(&self, id: &str) -> Option<Vec<u8>> {
    let docs = self.documents.read().await;
    let doc = docs.get(id)?;
    let ydoc = doc.ydoc.as_ref()?;
    let txn = ydoc.transact();
    Some(txn.encode_state_as_update_v1(&yrs::StateVector::default()))
}
```

**Step 4: Add route**

```rust
.route("/nodes/:id/head", get(get_node_head))
```

**Step 5: Run tests**

```bash
cargo test && cargo clippy
```

**Step 6: Commit**

```bash
git add -A && git commit -m "CP-li3: Add /nodes/:id/head endpoint for sync"
```

---

## Task 2: Add POST /nodes/:id/edit endpoint

Applies a Yjs update and creates a commit. Sync binary uses this for pushing changes.

**Files:**
- Modify: `src/api.rs`

**Step 1: Add request/response structs**

```rust
#[derive(Deserialize)]
struct NodeEditRequest {
    update: String,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Serialize)]
struct NodeEditResponse {
    cid: String,
}
```

**Step 2: Add edit_node handler**

```rust
async fn edit_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(req): Json<NodeEditRequest>,
) -> Result<Json<NodeEditResponse>, StatusCode> {
    let commit_store = state.commit_store.as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    // Verify document exists
    let _doc = state.doc_store.get_document(&id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    let update_bytes = b64::decode(&req.update)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get current head
    let current_head = commit_store.get_document_head(&id).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let parents = current_head.into_iter().collect();
    let author = req.author.unwrap_or_else(|| "anonymous".to_string());

    let commit = Commit::new(parents, req.update, author, req.message);
    let timestamp = commit.timestamp;

    let cid = commit_store.store_commit(&commit).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply update to document
    state.doc_store.apply_yjs_update(&id, &update_bytes).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Update head
    commit_store.set_document_head(&id, &cid).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    broadcast_commit(&state, &id, &cid, timestamp);

    Ok(Json(NodeEditResponse { cid }))
}
```

**Step 3: Add route**

```rust
.route("/nodes/:id/edit", post(edit_node))
```

**Step 4: Run tests**

```bash
cargo test && cargo clippy
```

**Step 5: Commit**

```bash
git add -A && git commit -m "CP-li3: Add /nodes/:id/edit endpoint for sync"
```

---

## Task 3: Add POST /nodes and GET /nodes/:id endpoints

Create nodes and get node info.

**Files:**
- Modify: `src/api.rs`

**Step 1: Add structs**

```rust
#[derive(Deserialize)]
struct CreateNodeRequest {
    #[serde(rename = "type")]
    node_type: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
}

#[derive(Serialize)]
struct CreateNodeResponse {
    id: String,
}

#[derive(Serialize)]
struct NodeInfoResponse {
    id: String,
    #[serde(rename = "type")]
    node_type: String,
}
```

**Step 2: Add create_node handler**

```rust
async fn create_node(
    State(state): State<ApiState>,
    Json(req): Json<CreateNodeRequest>,
) -> Result<Json<CreateNodeResponse>, StatusCode> {
    let content_type = req.content_type
        .as_deref()
        .and_then(ContentType::from_mime)
        .unwrap_or(ContentType::Text);

    let id = state.doc_store.create_document(content_type).await;

    Ok(Json(CreateNodeResponse { id }))
}
```

**Step 3: Add get_node handler**

```rust
async fn get_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<NodeInfoResponse>, StatusCode> {
    let _doc = state.doc_store.get_document(&id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(NodeInfoResponse {
        id: id.clone(),
        node_type: "document".to_string(),
    }))
}
```

**Step 4: Add routes**

```rust
.route("/nodes", post(create_node))
.route("/nodes/:id", get(get_node))
```

**Step 5: Run tests**

```bash
cargo test && cargo clippy
```

**Step 6: Commit**

```bash
git add -A && git commit -m "CP-li3: Add POST /nodes and GET /nodes/:id endpoints"
```

---

## Task 4: Add POST /nodes/:id/replace endpoint

Replace content using character-level diff. Uses existing diff module.

**Files:**
- Modify: `src/api.rs`

**Step 1: Add structs**

```rust
#[derive(Serialize)]
struct ReplaceResponse {
    cid: String,
    edit_cid: String,
    summary: ReplaceSummary,
}

#[derive(Serialize)]
struct ReplaceSummary {
    chars_inserted: usize,
    chars_deleted: usize,
    operations: usize,
}
```

**Step 2: Add replace_node handler**

```rust
async fn replace_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(params): Query<ReplaceParams>,
    body: String,
) -> Result<Json<ReplaceResponse>, StatusCode> {
    let commit_store = state.commit_store.as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    let doc = state.doc_store.get_document(&id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Generate diff
    let (update, summary) = crate::diff::diff_to_yjs_update(&doc.content, &body);
    let update_b64 = b64::encode(&update);

    // Create commit
    let parent = params.parent_cid.or_else(|| {
        // Use current head if no parent specified
        commit_store.get_document_head(&id).await.ok().flatten()
    });

    let parents = parent.into_iter().collect();
    let author = params.author.unwrap_or_else(|| "anonymous".to_string());

    let commit = Commit::new(parents, update_b64.clone(), author, None);
    let cid = commit_store.store_commit(&commit).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply update
    state.doc_store.apply_yjs_update(&id, &update).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    commit_store.set_document_head(&id, &cid).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ReplaceResponse {
        cid: cid.clone(),
        edit_cid: cid,
        summary: ReplaceSummary {
            chars_inserted: summary.inserted,
            chars_deleted: summary.deleted,
            operations: summary.operations,
        },
    }))
}

#[derive(Deserialize)]
struct ReplaceParams {
    parent_cid: Option<String>,
    author: Option<String>,
}
```

**Step 3: Add route**

```rust
.route("/nodes/:id/replace", post(replace_node))
```

**Step 4: Verify diff module exists and has correct interface**

Check `src/diff.rs` for the diff_to_yjs_update function signature.

**Step 5: Run tests**

```bash
cargo test && cargo clippy
```

**Step 6: Commit**

```bash
git add -A && git commit -m "CP-li3: Add /nodes/:id/replace endpoint for sync"
```

---

## Task 5: Add POST /nodes/:id/fork endpoint

Fork a node, optionally at a specific commit.

**Files:**
- Modify: `src/api.rs`

**Step 1: Add structs**

```rust
#[derive(Deserialize)]
struct ForkParams {
    at_commit: Option<String>,
}

#[derive(Serialize)]
struct ForkResponse {
    id: String,
    head: String,
}
```

**Step 2: Add fork_node handler**

```rust
async fn fork_node(
    State(state): State<ApiState>,
    Path(source_id): Path<String>,
    Query(params): Query<ForkParams>,
) -> Result<Json<ForkResponse>, StatusCode> {
    let commit_store = state.commit_store.as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    // Get source document
    let source_doc = state.doc_store.get_document(&source_id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Create new document
    let new_id = state.doc_store.create_document(source_doc.content_type).await;

    // Get target commit (specified or HEAD)
    let target_cid = if let Some(cid) = params.at_commit {
        cid
    } else {
        commit_store.get_document_head(&source_id).await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?
    };

    // Replay commits from source to new document
    let commits = crate::replay::collect_ancestors(commit_store, &target_cid).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for cid in commits {
        let commit = commit_store.get_commit(&cid).await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if !commit.update.is_empty() {
            let update_bytes = b64::decode(&commit.update)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            state.doc_store.apply_yjs_update(&new_id, &update_bytes).await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }

        // Store commit for new document
        commit_store.store_commit(&commit).await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    // Set head for new document
    commit_store.set_document_head(&new_id, &target_cid).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ForkResponse {
        id: new_id,
        head: target_cid,
    }))
}
```

**Step 3: Add route**

```rust
.route("/nodes/:id/fork", post(fork_node))
```

**Step 4: Run tests**

```bash
cargo test && cargo clippy
```

**Step 5: Commit**

```bash
git add -A && git commit -m "CP-li3: Add /nodes/:id/fork endpoint for sync"
```

---

## Task 6: Add GET /sse/nodes/:id endpoint

SSE stream for node changes. Wraps existing SSE infrastructure.

**Files:**
- Modify: `src/sse.rs`

**Step 1: Add node SSE handler**

The SSE module already has document streaming. Add a route that maps node ID to document ID:

```rust
.route("/sse/nodes/:id", get(stream_node))
```

Where `stream_node` is an alias to the existing document stream handler, since nodes ARE documents now.

**Step 2: Run tests**

```bash
cargo test && cargo clippy
```

**Step 3: Commit**

```bash
git add -A && git commit -m "CP-li3: Add /sse/nodes/:id endpoint for sync"
```

---

## Task 7: Manual Integration Test

Test sync between two directories as specified in original plan.

**Step 1: Build release binaries**

```bash
cargo build --release
```

**Step 2: Start server with database**

```bash
./target/release/commonplace-server --database ./test.redb --port 3000
```

**Step 3: Create test directories**

```bash
mkdir -p /tmp/sync-test/dir-a /tmp/sync-test/dir-b
echo "initial content" > /tmp/sync-test/dir-a/test.txt
```

**Step 4: Start sync for directory A**

```bash
./target/release/commonplace-sync --server http://localhost:3000 --directory /tmp/sync-test/dir-a
```

**Step 5: In another terminal, start sync for directory B**

```bash
./target/release/commonplace-sync --server http://localhost:3000 --directory /tmp/sync-test/dir-b
```

**Step 6: Edit file in A, verify it appears in B**

```bash
echo "updated content" >> /tmp/sync-test/dir-a/test.txt
sleep 2
cat /tmp/sync-test/dir-b/test.txt
# Should show "initial content\nupdated content"
```

**Step 7: Edit file in B, verify it appears in A**

```bash
echo "from B" >> /tmp/sync-test/dir-b/test.txt
sleep 2
cat /tmp/sync-test/dir-a/test.txt
# Should show all content including "from B"
```

**Step 8: Document results**

Save test output to `docs/test-results/sync-integration-test.txt`

**Step 9: Commit**

```bash
git add -A && git commit -m "CP-li3: Integration test passing - sync works"
```

---

## Task 8: Run Codex Review

**Step 1: Run codex review on branch changes**

```bash
codex review --base main --title "CP-li3: Restore /nodes endpoints for sync"
```

**Step 2: Address all P1 issues**

Fix any blocking issues found.

**Step 3: Re-run codex review until clear**

Repeat until no P1 issues remain.

---

## Completion Criteria

1. All /nodes endpoints implemented
2. Manual sync test passes (bidirectional sync between directories)
3. Codex review finds no P1 issues
4. All existing tests still pass
