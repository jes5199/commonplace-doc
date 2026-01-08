# Named Workspaces with MQTT Topic Namespacing

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:writing-plans to create the implementation plan for this design.

**Issue:** CP-jr39

**Goal:** Prefix all MQTT topics with a configurable workspace name, enabling multiple independent commonplace instances to share an MQTT broker.

**Architecture:** Add workspace name to MqttConfig, validate it, and prepend to all topic strings. Default workspace is "commonplace".

---

## Configuration

### commonplace.json (orchestrator level)

```json
{
  "workspace": "commonplace",
  "mqtt_broker": "mqtt://localhost:1883",
  "processes": { ... }
}
```

- If `workspace` omitted, defaults to `"commonplace"`
- Validated on load: alphanumeric, hyphens, underscores only
- Error on invalid characters

### MqttConfig (process level)

```rust
pub struct MqttConfig {
    pub broker_url: String,
    pub client_id: String,
    pub workspace: String,  // NEW - defaults to "commonplace"
    pub keep_alive_secs: u64,
    pub clean_session: bool,
}
```

Orchestrator passes workspace to child processes. Individual processes can override if needed (e.g., bridging between namespaces).

---

## Topic Structure

All topics prefixed with workspace name:

| Current | New (workspace="commonplace") |
|---------|-------------------------------|
| `terminal/screen.txt/edits` | `commonplace/terminal/screen.txt/edits` |
| `terminal/screen.txt/sync/client-123` | `commonplace/terminal/screen.txt/sync/client-123` |
| `terminal/screen.txt/events/complete` | `commonplace/terminal/screen.txt/events/complete` |
| `terminal/screen.txt/commands/clear` | `commonplace/terminal/screen.txt/commands/clear` |
| `$store/commands/create-document` | `commonplace/commands/create-document` |
| `$store/responses` | `commonplace/responses` |

### Topic struct

```rust
pub struct Topic {
    pub workspace: String,  // NEW
    pub path: String,
    pub port: Port,
    pub qualifier: Option<String>,
}
```

- `Topic::to_topic_string()` prepends workspace
- `Topic::parse(topic, workspace)` strips/validates prefix
- Wildcard subscriptions also prefixed: `commonplace/+/+.txt/edits`

---

## Implementation Changes

### 1. src/mqtt/topics.rs
- Add `workspace` field to `Topic` struct
- Update `Topic::parse(topic: &str, workspace: &str)` to expect and strip prefix
- Update `Topic::to_topic_string()` to prepend workspace
- Update constructors (`edits()`, `sync()`, etc.) to take workspace
- Add `validate_workspace_name()` function

### 2. src/mqtt/mod.rs
- Add `workspace: String` to `MqttConfig` with default "commonplace"
- Pass workspace through to all topic operations

### 3. src/mqtt/commands.rs
- Replace `$store/commands/create-document` with `{workspace}/commands/create-document`
- Replace `$store/responses` with `{workspace}/responses`

### 4. src/orchestrator/
- Parse `workspace` from commonplace.json
- Pass to child processes (env var or CLI arg)

### 5. src/cli.rs
- Add `--workspace` flag for standalone server usage

---

## Validation Rules

Workspace name must:
- Be at least one character
- Contain only: `[a-zA-Z0-9_-]`
- NOT contain: `/`, `+`, `#`, spaces, or other special characters

Rejected at config load time with clear error message.

---

## Testing

- Unit tests in `topics.rs` for workspace prefix parsing/generation
- Unit tests for `validate_workspace_name()` (valid chars, rejects `/+#` and spaces)
- Integration tests verifying cross-process communication with workspace prefix
- Test that default "commonplace" works when config omits workspace

---

## Migration

**For existing deployments:**
1. Clear MQTT retained messages (topic names have changed)
2. No config changes needed if "commonplace" default is acceptable

**Breaking change:** No backwards-compatible mode. All topics get prefixed.
