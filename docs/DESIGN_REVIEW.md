# Commonplace Design Review

This document reviews the Commonplace design against the current repository state.
Last updated: 2026-01-23.

## Table of Contents

1. Core Design Principles
2. System Components Overview
3. Module-by-Module Analysis
4. Alignment Ratings
5. Recommendations
6. Specific Exceptions to Design Principles
7. Conclusion

---

Scope note: This review compares docs/DESIGN_REVIEW.md against src/*, src/bin/*,
sdk/, static/, and docs/*. Tests, examples, and scripts are only noted for
existence and are not analyzed for design alignment.

## Core Design Principles

The principles below reflect what is implemented today, not just aspirational goals.

### 1. CRDT-First Documents (with explicit sync logic)

Document state is stored in Yjs (yrs) structures and updated via Yjs deltas.
However, the system still relies on explicit sync logic, diffing, and state
tracking in the filesystem sync layer and in HTTP replace operations.

### 2. Content-Addressed Commit History

Commits are identified by SHA-256 CIDs and form a DAG with merge commits.
Commit history is persisted in redb when enabled, and replay utilities rebuild
state from commit chains.

### 3. Blue and Red Channels (Edits vs Events/Commands)

Collaborative edits (blue) are Yjs updates. Events and commands (red) are
ephemeral JSON messages. This split is implemented in MQTT ports and mirrored
in WebSocket and HTTP command endpoints.

### 4. MQTT as Wiring (Workspace Namespacing)

MQTT topics are the primary wiring for edits, sync, events, and commands.
Topics follow the format:
- {workspace}/{port}/{path}
- {workspace}/{port}/{path}/{qualifier}

Ports are: edits, sync, events, commands. System commands live under
{workspace}/commands/__system/..., and command responses use {workspace}/responses.

### 5. Multiple Transports, Not Fully Agnostic

The system supports HTTP REST, SSE, WebSocket, and MQTT. The semantics differ
by transport: HTTP has CRUD and replace endpoints, SSE streams commits, WebSocket
speaks y-websocket or a custom protocol, and MQTT handles edits/sync/events/commands.
An HTTP gateway exists to translate HTTP to MQTT.

### 6. Filesystem-as-JSON (Schema v1)

A filesystem root document stores a versioned JSON schema describing directories
and documents. Node-backed directories and document nodes use explicit node_id
fields. Local sync uses a .commonplace.json schema file on disk and reconciles
changes against the server schema.

### 7. Process Orchestration as a First-Class Concern

Process management is handled by the orchestrator with explicit configuration,
dependency resolution, and status tracking. This is not a pure "node graph" model,
but it is built around filesystem discovery and MQTT wiring.

### 8. Sandbox Isolation with Explicit Sharing

Sandboxed execution is supported by commonplace-sync --sandbox, and explicit
sharing is handled by commonplace-link. Symlinks are avoided for sync safety.

---

## System Components Overview

### Document Server (commonplace-server)

- Binary: src/bin/server.rs
- Uses the unified router from src/lib.rs
- Exposes REST API (src/api.rs), path-based API (src/files.rs), SSE (src/sse.rs),
  WebSocket (src/ws), viewer routes (src/viewer.rs), SDK routes (src/sdk.rs)
- Optionally connects to MQTT for publishing and for commands endpoints

### Document Store (commonplace-store)

- Binary: src/bin/store.rs
- MQTT-only storage node with DocumentStore and CommitStore
- Subscribes to MQTT edits/sync/events/commands and serves store-level commands

### HTTP Gateway (commonplace-http)

- Binary: src/bin/http.rs
- Stateless bridge from HTTP/SSE to MQTT (src/http_gateway)
- Uses MQTT request/response topics for data fetch

### Sync Client (commonplace-sync)

- Binary: src/bin/sync.rs
- Watches files and directories, maintains CRDT state per file
- Uses MQTT for edits and HTTP/SSE for commit history and metadata
- Supports push-only, pull-only, and force-push modes plus sandbox exec

### Orchestrator (commonplace-orchestrator)

- Binary: src/bin/orchestrator.rs
- Modules in src/orchestrator
- Manages process lifecycles, dependencies, and status files for commonplace-ps

### CLI Tools and Utilities

- Dispatcher: src/bin/commonplace.rs
- CLI args: src/cli.rs
- Tools: commonplace-cmd, commonplace-link, commonplace-uuid, commonplace-log,
  commonplace-show, commonplace-replay, commonplace-ps, commonplace-signal,
  commonplace-mcp, cbd (issue CLI)

### SDK and Viewer

- SDK served from sdk/ via src/sdk.rs
- Viewer served from static/ via src/viewer.rs

---

## Module-by-Module Analysis

### src/document.rs - DocumentStore

Alignment: HIGH (8/10)

Strengths:
- Simple Arc<RwLock<HashMap>> store
- Y.Doc per document with content-type support

Concerns:
- Content type inference can migrate types at runtime
- In-memory store only; persistence is separate

### src/commit.rs - Commit Model

Alignment: HIGH (8/10)

Strengths:
- Clear commit DAG with parents and updates
- Snapshot extensions supported

Concerns:
- extensions uses HashMap, which can affect CID determinism

### src/store.rs - CommitStore

Alignment: HIGH (8/10)

Strengths:
- Redb-backed persistence
- Monotonic descent checks

Concerns:
- Storage backend is not abstracted

### src/services/document.rs - DocumentService

Alignment: HIGH (8/10)

Strengths:
- Encapsulates document CRUD, commits, and broadcasting
- Integrates filesystem reconciliation when enabled

Concerns:
- Service error model is not shared across all transports

### src/content_type.rs, src/b64.rs, src/events.rs, src/cli.rs - Core Utilities

Alignment: HIGH (8/10)

Strengths:
- Clear content-type mapping and base64 helpers
- Simple commit broadcaster and CLI argument definitions

Concerns:
- Utilities are spread across modules without a single shared error model

### src/api.rs and src/files.rs - HTTP REST and Path API

Alignment: MEDIUM-HIGH (7/10)

Strengths:
- CRUD, edit, replace, fork, head endpoints
- Path-based access via fs-root schema

Concerns:
- Response models are inconsistent across endpoints
- Path resolution assumptions diverge from schema flexibility

### src/sse.rs - SSE Endpoints

Alignment: MEDIUM (7/10)

Strengths:
- Streams document changes and commits
- Supports doc and path SSE

Concerns:
- Semantics differ from MQTT and WebSocket sync paths

### src/ws/* - WebSocket Sync

Alignment: HIGH (8/10)

Strengths:
- Supports y-websocket and commonplace subprotocols
- Room manager and commit notifications

Concerns:
- Transport semantics are distinct from HTTP and MQTT

### src/mqtt/* - MQTT Transport

Alignment: HIGH (8/10)

Strengths:
- Clear port separation (edits, sync, events, commands)
- Workspace namespacing and topic utilities

Concerns:
- Requires careful alignment with HTTP gateway and server endpoints

### src/http_gateway/* - HTTP to MQTT Bridge

Alignment: MEDIUM-HIGH (7/10)

Strengths:
- Stateless HTTP interface backed by MQTT
- SSE bridge for edits

Concerns:
- Timeouts and response-topic coupling add complexity

### src/commands.rs - HTTP Command Bridge

Alignment: MEDIUM (7/10)

Strengths:
- Simple path parsing and MQTT publish

Concerns:
- Parallel command handling exists in MQTT handlers

### src/fs/*, src/path.rs, src/workspace.rs - Filesystem Schema and Resolution

Alignment: MEDIUM-HIGH (7/10)

Strengths:
- Schema v1 with node-backed directories
- Reconciler updates schema based on document content
 - Workspace helpers for local path to UUID resolution

Concerns:
- Inline vs node-backed directory assumptions differ across modules

### src/sync/* and src/bin/sync.rs - Sync Client

Alignment: MEDIUM (6/10)

Strengths:
- CRDT merge logic and Yjs update generation
- File watcher, SSE subscriber, MQTT edits, and atomic writes

Concerns:
- Large state machine with many modes and edge cases
- Echo suppression and state tracking add complexity

### src/orchestrator/* - Orchestrator

Alignment: MEDIUM (6/10)

Strengths:
- Process discovery and dependency handling
- Status file for commonplace-ps

Concerns:
- Multiple responsibilities in a single subsystem

### src/sdk.rs and sdk/* - SDK Serving

Alignment: HIGH (8/10)

Strengths:
- Simple static serving with path traversal checks

Concerns:
- None significant

### src/viewer.rs and static/* - Viewer

Alignment: MEDIUM-HIGH (7/10)

Strengths:
- Simple static serving and routing

Concerns:
- None significant

### src/cbd.rs - Issue CLI (Commonplace Bug Database)

Alignment: MEDIUM-HIGH (7/10)

Strengths:
- Straightforward REST client for issues JSONL

Concerns:
- Separate CLI path from beads tooling

---

## Alignment Ratings

| Module Group | Rating | Notes |
|-------------|--------|-------|
| Core Model (document, commit, store) | 8/10 | Strong, cohesive core | 
| Service Layer (services) | 8/10 | Clear orchestration, limited reuse across transports |
| HTTP API (api, files, sse) | 7/10 | Functional, some inconsistencies |
| WebSocket (ws) | 8/10 | Solid Yjs sync transport |
| MQTT (mqtt, commands) | 8/10 | Clear port model, requires careful topic alignment |
| HTTP Gateway (http_gateway) | 7/10 | Useful bridge, extra complexity |
| Filesystem (fs, path, workspace) | 7/10 | Good schema, mixed assumptions |
| Sync Client (sync) | 6/10 | Feature-rich, complex state machine |
| Orchestrator | 6/10 | Useful but broad scope |
| SDK/Viewer | 7.5/10 | Simple and stable |

---

## Recommendations

1. Fix CID determinism by replacing HashMap with BTreeMap or sorted keys in commit.rs.
2. Unify path resolution and schema assumptions (inline vs node-backed directories).
3. Reduce sync complexity by splitting watcher, merge, transport, and schema logic.
4. Standardize error types across HTTP, MQTT, and sync layers.
5. Align transport semantics and response models (HTTP API, gateway, WebSocket, MQTT).

---

## Specific Exceptions to Design Principles

### 1. CRDT-First, but heavy sync logic

The sync layer relies on explicit watchers, diffing, echo suppression, and
state files. CRDTs are central, but "no special sync logic" is not accurate.

### 2. MQTT wiring is workspace-scoped

Topics are workspace-prefixed and port-scoped, not path-first. This is critical
for accurate subscriptions and tooling.

### 3. Transport semantics diverge

HTTP, SSE, WebSocket, and MQTT share concepts but do not have identical
contracts. The HTTP gateway and server APIs are similar but not identical.

### 4. Filesystem schema interpretation differs

fs/schema.rs supports inline and node-backed directories, while path resolution
in path.rs and files.rs assumes node-backed directories. This mismatch makes
schema handling less predictable.

### 5. Error type inconsistency

Mixed error styles exist across modules (custom enums, boxed errors, strings),
which complicates cross-layer error handling.

### 6. Retry loops with magic numbers

sync/client.rs uses fixed retry loops for eventual consistency, which could
be replaced with more explicit coordination or backoff policies.

---

## Conclusion

Commonplace has a strong core model: CRDT documents with a content-addressed
commit history. The transport and sync layers are powerful but divergent, and
the filesystem schema handling has inconsistent assumptions across modules.

The most valuable improvements are to reduce sync complexity, unify schema and
path resolution, and standardize transport error models. The system already has
useful building blocks (MQTT ports, WebSocket rooms, DocumentService) that can
be aligned for a cleaner, more predictable architecture.
