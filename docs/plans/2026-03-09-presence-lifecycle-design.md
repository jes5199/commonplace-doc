# Presence Lifecycle Design

> Based on commonplace-plan's `docs/presence-lifecycle.md` and `docs/observer-presence.md`.

**Goal:** Implement dual-location identity (hot presence + cold identity registry), heartbeat-gated reaping, and orchestrator identity service.

**Architecture:** Every actor has a permanent identity doc in `__identities/` at the repo level and a transient hot presence file in working directories. The orchestrator manages identity creation and reaping. Sync agents receive their identity UUID via env var/config/CLI and create linked hot presence entries.

## Design Decisions

### Identity UUID injection
- Three-level config: CLI (`--identity-uuid`) > env (`COMMONPLACE_IDENTITY_UUID`) > config file > None (ephemeral)
- Orchestrator creates identity docs and passes UUID via env var when spawning processes
- Standalone sync agents can use config file or CLI flag
- Ephemeral mode (no UUID): fresh UUID, no cold identity, no reaping

### `__identities/` directory
- Repo-level directory, peer to branches (e.g., `/myapp/__identities/`)
- Skipped by branch discovery (existing `__` prefix convention)
- Contains `name.ext.json` files — CRDT docs with ActorIO schema
- Created lazily by orchestrator on first identity ensure

### Orchestrator identity service (MQTT API)
- `{workspace}/identity/ensure` — idempotent: create doc + schema entry if missing, return UUID
  - Request: `{ "name": "sync", "extension": "exe", "repo_path": "/myapp" }`
  - Response: `{ "uuid": "abc-123", "identity_path": "__identities/sync.exe.json" }`
- `{workspace}/identity/list` — list known identities for a repo
  - Request: `{ "repo_path": "/myapp" }`
  - Response: `{ "identities": [...] }`

### Hot presence linking
- Sync agent creates working dir entry linked to identity UUID (same doc, two schema entries)
- Uses existing `commonplace-link` mechanism (two schema entries → one UUID)
- On clean shutdown: delete hot entry, update cold identity to Stopped
- On crash: hot entry goes stale, orchestrator reaps after heartbeat timeout

### Heartbeat + reaping
- `heartbeat_timeout_seconds` added to ActorIO schema
- Defaults: .exe=30s, .usr=300s, .bot=60s, .who=60s
- Liveness: `now - last_red_event_timestamp > timeout`
- Orchestrator reaper task: periodically walks hot presence files, checks timestamps, deletes stale entries
- Cold identity untouched by reaping

## Data Flow

```
Orchestrator                     Server/Store          Sync Agent
    |                                |
    |-- ensure identity ------------>|                     (not started)
    |   (create doc + __identities/  |
    |    schema entry if missing)    |
    |<-- uuid ----------------------|
    |                                |
    |-- spawn sync with UUID env ----|----------------->|
    |                                |                  |-- create hot presence
    |                                |                  |   (linked to same UUID)
    |                                |                  |-- emit red events
    |                                |
    |-- reaper checks heartbeats --->|
    |   (deletes stale hot entries)  |
```

## Lifecycle Summary

```
Orchestrator ensures identity  -->  __identities/sync.exe.json created (permanent)
Sync agent starts              -->  main/sync.exe created (hot, same UUID)
Agent emits red events         -->  heartbeat stays fresh
Agent moves (checkout)         -->  main/sync.exe deleted, experiment/sync.exe created
Clean shutdown                 -->  hot entry deleted, cold identity -> Stopped
Crash                          -->  heartbeat expires, orchestrator reaps hot entry
Agent comes back               -->  main/sync.exe re-created (same UUID from __identities/)
```
