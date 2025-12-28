# Commonplace MQTT Protocol Specification

## Overview

Commonplace uses MQTT as its message fabric. All nodes—documents, processes, devices, gateways—communicate via MQTT topics following consistent naming conventions.

The core insight: **topics are the wiring**. There is no separate wiring document. Nodes publish and subscribe to topics. If topics match, nodes are connected.

## Design Principles

- Documents are like spreadsheet value cells (anyone can edit)
- Processes are like spreadsheet formula cells (computed, read-only output)
- MQTT handles all fanout; no central routing logic
- The doc store is just another subscriber (the one that remembers)
- Permissions are enforced via macaroons at the MQTT auth layer

## Topic Structure

All topics follow this pattern:

```
{path.ext}/{port}
{path.ext}/{port}/{qualifier}
```

### Path Rules

- Paths use `/` as separator and can have arbitrary depth
- Files **must** have extensions
- The path ends at the segment containing a dot
- `edits`, `sync`, `events`, `commands` are reserved (never valid extensions)

#### Allowed Extensions

| Extension | Content Type | Yjs Type | Notes |
|-----------|--------------|----------|-------|
| `.txt` | text/plain | Y.Text | General text |
| `.json` | application/json | Y.Map | Structured data |
| `.xml` | application/xml | Y.XmlFragment | XML documents |
| `.xhtml` | application/xhtml+xml | Y.XmlFragment | XHTML documents |
| `.bin` | application/octet-stream | Y.Text (base64) | Discouraged; no meaningful merge |

Examples:
```
terminal/screen.txt          → path is "terminal/screen.txt"
gateways/telegram/main.js    → NOT ALLOWED (no .js extension)
astrolabe/clock.json         → path is "astrolabe/clock.json"
deep/nested/path/doc.txt     → path is "deep/nested/path/doc.txt"
```

## Ports

Each path exposes up to four ports:

| Port | Topic Pattern | Direction | Purpose |
|------|---------------|-----------|---------|
| **edits** | `{path}/edits` | collaborative | Live document changes (Yjs deltas) |
| **sync** | `{path}/sync/{client-id}` | private | History catch-up, not persisted |
| **events** | `{path}/events/{event-name}` | node → world | Broadcasts from the node |
| **commands** | `{path}/commands/{verb}` | world → node | Commands to the node |

### Color Names (informal)

For mental modeling:

| Port | Color | Mnemonic |
|------|-------|----------|
| edits | blue | cool, collaborative |
| sync | cyan | lighter blue, ephemeral |
| events | red | warm, authoritative |
| commands | magenta | lighter red, imperative |

## Port Details

### edits (blue)

The primary document channel. Yjs deltas flow here.

```
{path}/edits
```

- **Publishers**: Anyone with write permission
- **Subscribers**: Anyone interested in the document (including doc store)
- **Payload**: JSON envelope with base64-encoded Yjs delta
- **Persistence**: Doc store subscribes and persists all edits

The doc store does not re-emit. MQTT broker handles fanout. All subscribers hear the original publish.

### sync (cyan)

Private catch-up channel for new subscribers. Implements git-like merkle tree synchronization.

```
{path}/sync/{client-id}
```

- **Purpose**: Request and receive historical commits
- **Not persisted**: These messages are ephemeral
- **Flow**:
  1. Client subscribes to `{path}/sync/{my-id}`
  2. Client publishes sync request to same topic
  3. Doc store responds with commits
  4. Client unsubscribes when caught up

All messages include a `req` field for correlation.

#### Sync Message Types

**head** — Get current HEAD commit:
```json
{ "type": "head", "req": "r-001" }
```
```json
{ "type": "head", "req": "r-001", "commit": "abc123" }
```

**get** — Fetch specific commits by ID:
```json
{ "type": "get", "req": "r-002", "commits": ["abc123", "def456"] }
```

**pull** — Incremental sync (like git pull):
```json
{ "type": "pull", "req": "r-003", "have": ["abc123"], "want": "HEAD" }
```
```json
{ "type": "pull", "req": "r-004", "have": ["abc123"], "want": "def456" }
```

**ancestors** — Full history (like git clone):
```json
{ "type": "ancestors", "req": "r-005", "commit": "HEAD", "depth": null }
```

**commit** — Response containing commit data:
```json
{ "type": "commit", "req": "r-003", "id": "def456", "parent": "abc123", "data": "<base64>" }
```

**done** — All requested commits sent:
```json
{ "type": "done", "req": "r-003", "commits": ["def456", "ghi789"] }
```

**error** — Request failed:
```json
{ "type": "error", "req": "r-003", "message": "commit not found" }
```

### events (red)

Broadcasts from a node. The node is the authority.

```
{path}/events/{event-name}
```

- **Publishers**: The node that owns this path
- **Subscribers**: Anyone interested
- **Payload**: JSON

Examples:
```
terminal/screen.txt/events/complete     # "I finished"
terminal/screen.txt/events/error        # "Something broke"
astrolabe/clock.json/events/planetary-hour  # "Venus hour began"
```

### commands (magenta)

Commands to a node. The node listens and reacts.

```
{path}/commands/{verb}
```

- **Publishers**: Anyone with permission
- **Subscribers**: The node that owns this path
- **Payload**: JSON

Examples:
```
terminal/screen.txt/commands/clear      # "Clear the screen"
gateways/telegram/script.txt/commands/restart  # "Restart the process"
astrolabe/clock.json/commands/sync      # "Sync to GPS"
```

## Wildcard Patterns

MQTT wildcards:
- `+` matches exactly one segment
- `#` matches zero or more segments (must be last)

Paths can have arbitrary depth (`a/b/c/d/file.txt`). Wildcards are most useful **after** the path:

```
# Everything for one path
terminal/screen.txt/#

# All events from a specific file
terminal/screen.txt/events/#

# Specific event type
terminal/screen.txt/events/+

# All commands to a file
gateways/telegram/script.txt/commands/#
```

**Note**: MQTT cannot express "all topics ending in `/edits`" because `#` must be last. The doc store subscribes to specific known paths (from filesystem JSON) rather than wildcards.

## Permissions (Macaroons)

Macaroons encode topic permissions with caveats:

```yaml
caveats:
  - topic: "{pattern}"
    op: subscribe | publish | both | none
  - expires: {timestamp}
```

### Example Macaroons

**Observer** (read-only):
```yaml
caveats:
  - topic: "terminal/screen.txt/edits", op: subscribe
  - topic: "terminal/screen.txt/sync/observer-1", op: both
  - topic: "terminal/screen.txt/events/#", op: subscribe
  - expires: 2025-02-01
```

**Editor** (can edit docs):
```yaml
caveats:
  - topic: "terminal/+/edits", op: both
  - topic: "terminal/+/sync/editor-1", op: both
  - topic: "terminal/+/events/#", op: subscribe
  - expires: 2025-02-01
```

**Controller** (can command, can't edit):
```yaml
caveats:
  - topic: "terminal/screen.txt/events/#", op: subscribe
  - topic: "terminal/screen.txt/commands/restart", op: publish
  - expires: 2025-02-01
```

**The node itself** (process):
```yaml
caveats:
  - topic: "terminal/screen.txt/edits", op: both
  - topic: "terminal/screen.txt/events/#", op: publish
  - topic: "terminal/screen.txt/commands/#", op: subscribe
  - topic: "terminal/prompt.txt/edits", op: subscribe
  - expires: 2025-06-01
```

**ESP32 device**:
```yaml
caveats:
  - topic: "astrolabe/#", op: both
  - topic: "time/events/sync", op: subscribe
  - expires: 2026-01-01
```

## Node Types

### Plain Document

A passive document. Accepts all valid Yjs edits.

**Uses:**
- `edits` (read/write)
- `sync` (for catch-up)

**Optionally uses:**
- `events` (if something watches and broadcasts changes)

### Process / Formula Document

A computed document. Output is read-only; process is sole author.

**Structure:**
```
gateways/telegram/
  script.txt        # source code (plain doc)
  config.json       # configuration (plain doc)
  output.txt        # computed output (read-only doc)
```

**Uses:**
- `output.txt/edits` — process publishes, others subscribe only
- `output.txt/events/#` — process broadcasts status
- `output.txt/commands/#` — process receives commands
- `config.json/edits` — process subscribes to config changes
- `script.txt/edits` — process subscribes to code changes (hot reload)

### External Process

A process that connects from outside (not sandboxed by kernel).

Connects to MQTT, subscribes/publishes per its config. No special registration required if path already exists in filesystem JSON.

### Device (ESP32, etc.)

Lightweight node. Speaks red only (no Yjs).

**Uses:**
- `events/{name}` — broadcasts state
- `commands/{verb}` — receives commands

A bridge process can persist device events to a document if history is needed.

## Doc Store Responsibilities

The doc store is a process that:

1. **Subscribes to edits for known paths**: Subscribes to `{path}/edits` for each path in filesystem JSON
2. **Persists Yjs deltas**: Maintains merkle tree history per path
3. **Responds to sync requests**: Subscribes to `{path}/sync/+` for each known path
4. **Maintains path→UUID mapping**: Internal only, not exposed via MQTT
5. **Manages filesystem JSON**: Source of truth for what paths exist

The doc store does **not**:
- Re-emit edits (MQTT handles fanout)
- Route between nodes (MQTT handles routing)
- Enforce permissions (MQTT auth plugin handles this)
- Use topic wildcards (subscribes to enumerated paths)

## Examples

### Terminal Session

```
terminal/
  prompt.txt        # input doc (user/bot writes)
  screen.txt        # output doc (tmux connector writes)
```

Tmux connector:
- Subscribes: `terminal/prompt.txt/edits`
- Publishes: `terminal/screen.txt/edits`
- Publishes: `terminal/screen.txt/events/complete`

Telegram bot:
- Subscribes: `terminal/screen.txt/edits`
- Subscribes: `terminal/screen.txt/events/#`
- Publishes: `terminal/prompt.txt/edits`

### Planetary Clock

```
astrolabe/
  clock.json        # device state
  history.json      # persisted event log (optional)
```

ESP32:
- Publishes: `astrolabe/clock.json/events/planetary-hour`
- Subscribes: `astrolabe/clock.json/commands/sync`

Bridge process (optional):
- Subscribes: `astrolabe/clock.json/events/#`
- Publishes: `astrolabe/history.json/edits` (appends events)

## Summary

| Concept | Implementation |
|---------|----------------|
| Document | Path with edits/sync ports |
| Process | Path with events/commands ports + exclusive edits |
| Wiring | Implicit in topic subscriptions |
| Permissions | Macaroons with topic/op caveats |
| Fanout | MQTT broker |
| Persistence | Doc store (just another subscriber) |
| History | Sync port (private, ephemeral) |
| Devices | Red ports only (events/commands) |
