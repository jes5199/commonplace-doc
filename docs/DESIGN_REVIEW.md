# Commonplace Design Review

This document provides a comprehensive review of the Commonplace system's design, examining how well each module adheres to the core design principles.

## Table of Contents

1. [Core Design Principles](#core-design-principles)
2. [System Components Overview](#system-components-overview)
3. [Module-by-Module Analysis](#module-by-module-analysis)
4. [Alignment Ratings](#alignment-ratings)
5. [Recommendations](#recommendations)

---

## Core Design Principles

Based on analysis of the codebase and documentation, these are the foundational design principles of Commonplace:

### 1. CRDT-First: All State is Collaborative

Everything is a CRDT document. Text, JSON, XML—all flow through Yjs (Yrs). This enables:
- Automatic conflict resolution
- Concurrent editing by design
- No special sync logic needed

**The ideal**: Any piece of state can be edited by multiple parties simultaneously without coordination.

### 2. Content-Addressed, Merkle-Tree History

Commits are identified by SHA-256 CIDs. History forms a DAG like git:
- Enables distributed trust
- Supports offline editing with later merges
- Verifiable history

**The ideal**: History is immutable, verifiable, and supports arbitrary branching/merging.

### 3. Blue and Red Edges: State vs Events

Two fundamentally different communication channels:

| Aspect | Blue (Edits) | Red (Events) |
|--------|--------------|--------------|
| Persistence | Stored in commit history | Fire-and-forget |
| Direction | Collaborative (anyone can edit) | Authoritative (owner broadcasts) |
| Content | Yjs deltas | Arbitrary JSON |
| Requirement | Must subscribe before editing | Can fire without subscription |

**The ideal**: Clear separation between "what is" (blue) and "what happened" (red).

### 4. MQTT as Universal Wiring

Topics ARE the wiring. No separate routing configuration:
- `{path}/edits` - collaborative CRDT updates
- `{path}/sync/{client}` - history catch-up
- `{path}/events/{name}` - node broadcasts
- `{path}/commands/{verb}` - node commands

**The ideal**: The doc store is "just another subscriber." MQTT handles all fanout.

### 5. Transport Agnosticism

The same CRDT operations work over:
- HTTP REST API
- Server-Sent Events (SSE)
- MQTT
- WebSocket

**The ideal**: Protocol choice doesn't affect semantics; it's just a transport decision.

### 6. Filesystem-as-JSON

Directories are JSON schema documents. Files are CRDT documents with UUIDs:
- Inline directories (no UUID, just nested entries)
- Node-backed directories (have UUID, contain own schema)
- Explicit vs derived UUIDs

**The ideal**: The filesystem is just another CRDT document organizing other documents.

### 7. No Special Cases

Design favors uniformity over special-casing:
- Documents and processes are both "nodes"
- The doc store is just another MQTT subscriber
- Processes are just documents with events/commands ports

**The ideal**: Everything is a node with blue and red ports.

### 8. Sandbox Isolation with Explicit Sharing

Processes see isolated directory trees:
- No filesystem symlinks (break sandbox, don't sync)
- Share content via UUID linking
- Location of `__processes.json` determines scope

**The ideal**: Isolation by default, explicit sharing when needed.

---

## System Components Overview

### Server (`commonplace-server`)

The HTTP/SSE server providing REST API for document operations:
- Document CRUD via `/docs`
- CRDT operations (`/edit`, `/replace`, `/fork`)
- Commit persistence (`/commit`)
- SSE subscriptions (`/sse/docs/{id}`)

### Sync Client (`commonplace-sync`)

Bidirectional CRDT sync between filesystem and server:
- Filesystem watching via `notify`
- CRDT state tracking per file
- Ancestry-based conflict resolution
- Shadow file tracking (Unix)

### Orchestrator (`commonplace-orchestrator`)

Process lifecycle management:
- Recursive `__processes.json` discovery
- Multiple execution modes (evaluate, sandbox-exec, command)
- Restart policies and dependencies
- Status tracking for `commonplace-ps`

### Node System (`src/node/`)

Reactive abstraction for document processing:
- `Node` trait with receive/subscribe/emit
- `DocumentNode` wrapping Yjs documents
- `ConnectionNode` for transient SSE clients
- `NodeRegistry` with cycle detection

### Storage Layer

- `DocumentStore`: In-memory document state
- `CommitStore`: Optional redb persistence for commits
- Content-addressed CIDs with monotonic descent validation

---

## Module-by-Module Analysis

### `src/api.rs` - REST API

**Alignment: HIGH (8/10)**

Strengths:
- Clean separation of endpoints
- Proper CRDT operation support (`/edit`, `/replace`)
- Transport-agnostic (just HTTP adapter)

Concerns:
- Some JSON body parsing feels ad-hoc
- Mix of Content-Type header vs JSON body for content type specification
- The `/docs/{id}` endpoint returns raw content, but `/docs/{id}/head` returns structured JSON—inconsistent response models

### `src/document.rs` - DocumentStore

**Alignment: HIGH (8/10)**

Strengths:
- Clean `Arc<RwLock<HashMap>>` pattern
- Content type abstraction with defaults
- Yrs document per document

Concerns:
- In-memory only—not a concern per design, but creates impedance mismatch with CommitStore
- Default content by type is embedded in code rather than being configurable

### `src/commit.rs` - Commit Model

**Alignment: VERY HIGH (9/10)**

Strengths:
- Clean merkle-tree structure
- SHA-256 CID calculation
- Support for multi-parent merges
- Snapshot optimization via extensions

Concerns:
- CID calculation uses `serde_json::to_string` which may have non-deterministic key ordering for HashMap (extensions field). Could cause issues if extensions are used.

### `src/store.rs` - CommitStore

**Alignment: HIGH (8/10)**

Strengths:
- Clean redb wrapper
- Monotonic descent validation
- Per-document head tracking
- Proper atomic operations

Concerns:
- Tightly coupled to redb (no trait abstraction for storage backend)
- Error handling returns strings in some places rather than typed errors

### `src/node/mod.rs` - Node Trait

**Alignment: VERY HIGH (9/10)**

Strengths:
- Clean trait definition
- Async-first design
- Clear separation of concerns (receive vs emit)
- Health checking built in

Concerns:
- `receive_edit` and `receive_event` are separate methods—could be a single `receive(NodeMessage)` for uniformity

### `src/node/document_node.rs` - DocumentNode

**Alignment: HIGH (8/10)**

Strengths:
- Proper Yrs integration
- Broadcast channel for fanout
- Content type awareness

Concerns:
- Some duplication with `DocumentStore` concepts
- The `apply_edit` logic has complexity around content type serialization

### `src/node/registry.rs` - NodeRegistry

**Alignment: HIGH (8/10)**

Strengths:
- Cycle detection via DFS
- Clean wire/unwire semantics
- Task-based forwarding

Concerns:
- Manual task abort tracking (could leak on edge cases)
- Wire operations don't validate node existence before wiring

### `src/sse.rs` - SSE Endpoint

**Alignment: MEDIUM-HIGH (7/10)**

Strengths:
- Clean async stream implementation
- Proper SSE event formatting
- Warning on subscription lag

Concerns:
- Creates transient connection nodes (good for the model, but adds complexity)
- Some coupling to specific event types

### `src/sync/` - Sync Client

**Alignment: MEDIUM (6/10)**

Strengths:
- Comprehensive CRDT sync implementation
- Proper ancestry tracking
- Shadow file handling for atomic writes

Concerns:
- **Complex state machine**: The reconciliation logic is intricate
- **Multiple concerns in one module**: File watching, CRDT sync, schema management, MQTT communication
- **Sync strategies**: `local`/`server`/`merge` adds complexity
- **Platform-specific code**: Shadow file tracking is Unix-only
- Some functions are quite long (`sync_file_to_server`, `handle_server_edit`)

### `src/bin/orchestrator.rs` - Orchestrator

**Alignment: MEDIUM (6/10)**

Strengths:
- Clean process discovery pattern
- Multiple execution modes
- Dependency management

Concerns:
- **Large binary with many responsibilities**: Process discovery, lifecycle, monitoring, signaling
- **Config-driven complexity**: Three execution modes with different semantics
- **Status file management**: Shared JSON file for `commonplace-ps` feels like a workaround
- Some duplication with sync client for MQTT connection setup

### `src/diff.rs` - Character-level Diffing

**Alignment: HIGH (8/10)**

Strengths:
- Clean diff algorithm implementation
- Proper conversion to Yrs operations
- Handles edge cases well

Concerns:
- Performance could be a concern for large documents (no streaming)

### `src/replay.rs` - Commit Replay

**Alignment: HIGH (8/10)**

Strengths:
- Topological traversal
- Snapshot optimization
- Clean state reconstruction

Concerns:
- Could benefit from caching intermediate states
- Error handling could be more granular

---

## Alignment Ratings

| Module | Rating | Notes |
|--------|--------|-------|
| `commit.rs` | 9/10 | Near-ideal merkle-tree implementation |
| `node/mod.rs` | 9/10 | Clean trait design |
| `api.rs` | 8/10 | Good transport adapter |
| `document.rs` | 8/10 | Clean in-memory store |
| `store.rs` | 8/10 | Solid persistence layer |
| `node/document_node.rs` | 8/10 | Good CRDT integration |
| `node/registry.rs` | 8/10 | Proper graph management |
| `diff.rs` | 8/10 | Clean diffing |
| `replay.rs` | 8/10 | Good history reconstruction |
| `sse.rs` | 7/10 | Works but adds complexity |
| `sync/` | 6/10 | Too many concerns, complex state |
| `orchestrator.rs` | 6/10 | Large scope, config complexity |

### Overall Assessment

**Core (commit, node, document, store): EXCELLENT**
- The fundamental abstractions are well-designed
- Clear adherence to CRDT-first and content-addressed principles

**API Layer (api, sse): GOOD**
- Proper transport abstraction
- Some inconsistencies in response models

**Infrastructure (sync, orchestrator): NEEDS ATTENTION**
- Complexity accumulation
- Multiple responsibilities per module
- Platform-specific concerns leak through

---

## Recommendations

### High Priority

1. **Refactor sync client**: Split into smaller modules:
   - File watcher
   - CRDT state manager
   - Schema reconciler
   - MQTT client

2. **Standardize error handling**: Create typed errors throughout, avoid stringly-typed errors

3. **Fix CID determinism**: Use `BTreeMap` instead of `HashMap` for extensions, or sort keys before hashing

### Medium Priority

4. **Extract orchestrator concerns**: Separate process discovery from lifecycle management

5. **Unify response models**: Make API responses consistently structured (always JSON with status/data)

6. **Add storage trait**: Abstract over redb to enable testing and alternative backends

### Low Priority

7. **Document node consolidation**: Consider whether `DocumentStore` and `DocumentNode` should be unified

8. **Streaming diff**: For large documents, consider streaming diff algorithm

9. **Cross-platform sync**: Abstract shadow file handling for Windows compatibility

---

## Specific Exceptions to Design Principles

This section documents specific code patterns that deviate from the core design principles.

### Principle Violations

#### 1. CID Determinism Issue (Commit Model)

**Location**: `src/commit.rs:51`
**Principle Violated**: Content-Addressed Everything

```rust
pub extensions: HashMap<String, serde_json::Value>,
// ...
let json = serde_json::to_string(self).unwrap();
```

The `extensions` field uses `HashMap` which has non-deterministic iteration order. If extensions are ever populated with multiple keys, CID calculation could produce different results for semantically identical commits on different runs.

**Severity**: Medium (currently extensions are rarely used)
**Fix**: Use `BTreeMap` or sort keys before serialization

#### 2. Content Type Detection Logic Complexity (DocumentStore)

**Location**: `src/document.rs:239-335`
**Principle Violated**: No Special Cases

The `apply_yjs_update` method has complex match logic that detects and migrates content types at runtime:

```rust
match content_type {
    ContentType::Text => {
        match root {
            Some(Value::YMap(map)) => {
                // This is actually a schema document, update content_type
                doc.content_type = ContentType::Json;
                // ...
            }
            // ...
        }
    }
    ContentType::Json => {
        match root {
            Some(Value::YText(text)) => {
                // Handle YText in case document was created as JSON but contains text
                doc.content_type = ContentType::Text;
                // ...
            }
            // ...
        }
    }
}
```

This creates implicit type migration that can be confusing. Documents can change type based on what CRDT updates arrive.

**Severity**: Medium
**Impact**: Makes debugging harder, violates principle of explicit types

#### 3. Sync Module Sprawl

**Location**: `src/sync/` (26 files, ~166 public exports)
**Principle Violated**: Separation of Concerns

The sync `mod.rs` exports 166 items across categories that should be separate modules:
- File operations (flock, shadow writes)
- HTTP client (fetch_head, fork_node, push_*)
- CRDT operations (create_yjs_*, publish_*, merge_*)
- Schema management (build_uuid_map, write_schema_*)
- State management (SyncState, CrdtPeerState)

**Severity**: High
**Impact**: Difficult to understand, maintain, and test independently

#### 4. Platform-Specific Code Leakage

**Location**: Multiple `#[cfg(unix)]` guards in `src/sync/mod.rs`
**Principle Violated**: Transport Agnosticism (generalized to platform agnosticism)

```rust
#[cfg(unix)]
pub mod flock;
// ...
#[cfg(unix)]
pub use sse::{
    atomic_write_with_shadow, handle_server_edit_with_tracker, // ...
};
```

Unix-specific features (shadow files, flock) are conditionally compiled, but the sync client doesn't gracefully degrade on Windows—it simply lacks functionality.

**Severity**: Medium
**Impact**: Windows users get degraded experience

#### 5. Retry Loops with Magic Numbers

**Location**: `src/sync/client.rs:262-315`
**Principle Violated**: No Special Cases

```rust
let mut attempts = 0;
let max_attempts = 30; // 3 seconds max wait

loop {
    // ...
    if head_resp.status() == reqwest::StatusCode::NOT_FOUND && attempts < max_attempts {
        attempts += 1;
        info!("Identifier {} not found, waiting for reconciler (attempt {}/{})", ...);
        sleep(Duration::from_millis(100)).await;
        continue;
    }
    // ...
}
```

This pattern appears in multiple push functions (`push_json_content`, `push_jsonl_content`, `push_file_content`). It's a workaround for the async reconciler rather than a proper synchronization mechanism.

**Severity**: Medium
**Impact**: Race condition handling via polling rather than proper event-driven coordination

#### 6. Error Type Inconsistency

**Location**: Various
**Principle Violated**: Uniform Error Handling

The codebase mixes:
- Custom error enums (`StoreError`, `ServiceError`, `FetchHeadError`)
- `Box<dyn std::error::Error + Send + Sync>` (in sync client)
- String errors (`ApplyError::InvalidUpdate(String)`)

```rust
// Good: Typed error
pub enum StoreError {
    DatabaseError(String),
    CommitNotFound(String),
    // ...
}

// Less good: Boxing
pub async fn fork_node(...) -> Result<String, Box<dyn std::error::Error + Send + Sync>>

// Problematic: String wrapping
ApplyError::InvalidUpdate(e.to_string())
```

**Severity**: Low
**Impact**: Makes error handling harder, loses type information

#### 7. DocumentStore vs DocumentNode Duplication

**Location**: `src/document.rs` and `src/node/document_node.rs`
**Principle Violated**: No Special Cases

Both structures wrap a Yjs document with content and content_type. They have similar responsibilities but different APIs:
- `DocumentStore` is the REST API backing store
- `DocumentNode` is the node graph participant

This creates two ways to interact with documents, which doesn't align with "everything is a node."

**Severity**: Medium
**Impact**: Conceptual confusion, maintenance burden

### Patterns That Exemplify Principles

#### Good: Commit Structure (commit.rs)

The commit model cleanly implements the merkle-tree vision:
- Parents vector for DAG
- Content-addressed via CID
- Extensions for future features
- Snapshot support via extensions

#### Good: StoreError Enum (store.rs)

Clean, typed errors that cover the domain:
```rust
pub enum StoreError {
    DatabaseError(String),
    CommitNotFound(String),
    ParentNotFound(String),
    NotMonotonicDescendent(String),
    InvalidUpdate(String),
}
```

#### Good: Monotonic Descent Validation (store.rs)

Proper enforcement of the commit DAG invariant:
```rust
pub async fn validate_monotonic_descent(
    &self,
    doc_id: &str,
    new_commit_cid: &str,
) -> Result<(), StoreError>
```

---

## Conclusion

### Design Vision Assessment

Commonplace has a **clear and compelling architectural vision**:

1. **CRDT-native document system** - Everything is a collaborative document
2. **Content-addressed history** - Git-like merkle DAG for commits
3. **Blue/Red edge model** - Clean separation of state and events
4. **MQTT as universal wiring** - No central routing, just topic subscriptions
5. **Filesystem-as-JSON** - Directories are just schema documents

The vision is well-articulated in the MQTT.md and ARCHITECTURE.md docs. The core implementation (commit, store, node system) adheres closely to this vision.

### Where the Vision Breaks Down

The infrastructure layer (sync, orchestrator) has accumulated **accidental complexity** that deviates from the principles:

1. **Sync client is a monolith** - 26 files with 166 exports, mixing concerns that should be separate
2. **Platform coupling** - Unix-specific shadow file handling limits portability
3. **Polling instead of events** - Retry loops with magic numbers work around async coordination issues
4. **Two document abstractions** - `DocumentStore` and `DocumentNode` create conceptual confusion

### Root Causes

1. **Organic growth** - The sync client evolved to handle edge cases without refactoring
2. **Missing middleware** - No abstraction layer between filesystem events and CRDT operations
3. **Test coverage gaps** - Complex state machines are hard to test, so they grow without constraints

### Recommended Refactoring Priority

| Priority | Module | Action | Impact |
|----------|--------|--------|--------|
| **P0** | `sync/` | Split into 4-5 focused modules | High complexity reduction |
| **P1** | `commit.rs` | Use `BTreeMap` for extensions | Fixes CID determinism |
| **P1** | Error handling | Standardize on typed errors | Better debugging |
| **P2** | `orchestrator` | Extract process discovery | Cleaner separation |
| **P2** | Node consolidation | Merge `DocumentStore`/`DocumentNode` | Conceptual clarity |
| **P3** | Cross-platform | Abstract shadow files | Windows support |

### Final Ratings

| Layer | Rating | Rationale |
|-------|--------|-----------|
| **Core Model** (commit, node) | 9/10 | Excellent adherence to principles |
| **Storage** (document, store) | 8/10 | Clean but could unify abstractions |
| **API** (api, sse) | 7.5/10 | Functional but inconsistent models |
| **Sync** | 5.5/10 | Works but violates separation of concerns |
| **Orchestrator** | 6/10 | Too many responsibilities |

**Overall Design Alignment: 7.3/10**

The vision is 9/10. The core implementation is 8.5/10. The infrastructure drags the average down to 7.3/10.

### Path Forward

The system would benefit most from **applying the same rigor found in the core to the infrastructure**:

1. Define clear module boundaries for sync (file → CRDT → transport)
2. Use event-driven coordination instead of polling
3. Abstract platform-specific code behind traits
4. Unify document abstractions around the node model

The architecture is sound. The infrastructure needs a refactoring pass to match the elegance of the core.
