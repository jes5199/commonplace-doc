# Named Workspaces Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Prefix all MQTT topics with a configurable workspace name to enable multi-tenant deployments.

**Architecture:** Add `workspace` field to `Topic` struct and `MqttConfig`, validate workspace names, prepend to all topic strings. Default workspace is "commonplace".

**Tech Stack:** Rust, MQTT (rumqttc), clap (CLI), serde (config)

---

## Task 1: Workspace Name Validation

**Files:**
- Modify: `src/mqtt/topics.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_validate_workspace_valid() {
    assert!(validate_workspace_name("commonplace").is_ok());
    assert!(validate_workspace_name("my-workspace").is_ok());
    assert!(validate_workspace_name("workspace_1").is_ok());
    assert!(validate_workspace_name("A").is_ok());
}

#[test]
fn test_validate_workspace_invalid() {
    assert!(validate_workspace_name("").is_err());
    assert!(validate_workspace_name("has/slash").is_err());
    assert!(validate_workspace_name("has+plus").is_err());
    assert!(validate_workspace_name("has#hash").is_err());
    assert!(validate_workspace_name("has space").is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib test_validate_workspace`
Expected: FAIL with "cannot find function"

**Step 3: Write minimal implementation**

Add to `src/mqtt/topics.rs`:

```rust
/// Validate a workspace name.
/// Valid characters: alphanumeric, hyphens, underscores.
/// Must be at least one character.
pub fn validate_workspace_name(name: &str) -> Result<(), MqttError> {
    if name.is_empty() {
        return Err(MqttError::InvalidTopic("Workspace name cannot be empty".to_string()));
    }

    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '-' && c != '_' {
            return Err(MqttError::InvalidTopic(format!(
                "Invalid character '{}' in workspace name. Allowed: alphanumeric, hyphen, underscore",
                c
            )));
        }
    }

    Ok(())
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --lib test_validate_workspace`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mqtt/topics.rs
git commit -m "feat(mqtt): add workspace name validation"
```

---

## Task 2: Add Workspace to Topic Struct

**Files:**
- Modify: `src/mqtt/topics.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_topic_with_workspace() {
    let topic = Topic {
        workspace: "commonplace".to_string(),
        path: "terminal/screen.txt".to_string(),
        port: Port::Edits,
        qualifier: None,
    };
    assert_eq!(topic.workspace, "commonplace");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib test_topic_with_workspace`
Expected: FAIL with "no field `workspace`"

**Step 3: Write minimal implementation**

Update `Topic` struct in `src/mqtt/topics.rs`:

```rust
/// A parsed MQTT topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    /// The workspace namespace
    pub workspace: String,
    /// The document path (e.g., "terminal/screen.txt")
    pub path: String,
    /// The port type
    pub port: Port,
    /// Optional qualifier (e.g., client-id for sync, event-name for events)
    pub qualifier: Option<String>,
}
```

**Step 4: Fix compilation errors**

Update all `Topic` instantiations to include `workspace: String::new()` temporarily to compile.

**Step 5: Run test to verify it passes**

Run: `cargo test --lib test_topic_with_workspace`
Expected: PASS

**Step 6: Commit**

```bash
git add src/mqtt/topics.rs
git commit -m "feat(mqtt): add workspace field to Topic struct"
```

---

## Task 3: Update Topic Constructors

**Files:**
- Modify: `src/mqtt/topics.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_construct_edits_with_workspace() {
    let topic = Topic::edits("commonplace", "terminal/screen.txt");
    assert_eq!(topic.workspace, "commonplace");
    assert_eq!(topic.path, "terminal/screen.txt");
    assert_eq!(topic.port, Port::Edits);
}

#[test]
fn test_construct_sync_with_workspace() {
    let topic = Topic::sync("myspace", "doc.txt", "client-123");
    assert_eq!(topic.workspace, "myspace");
    assert_eq!(topic.qualifier, Some("client-123".to_string()));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib test_construct_edits_with_workspace`
Expected: FAIL with argument mismatch

**Step 3: Update all constructors**

```rust
/// Construct an edits topic for a path.
pub fn edits(workspace: &str, path: &str) -> Self {
    Topic {
        workspace: workspace.to_string(),
        path: path.to_string(),
        port: Port::Edits,
        qualifier: None,
    }
}

/// Construct a sync topic for a path and client ID.
pub fn sync(workspace: &str, path: &str, client_id: &str) -> Self {
    Topic {
        workspace: workspace.to_string(),
        path: path.to_string(),
        port: Port::Sync,
        qualifier: Some(client_id.to_string()),
    }
}

/// Construct an events topic for a path and event name.
pub fn events(workspace: &str, path: &str, event_name: &str) -> Self {
    Topic {
        workspace: workspace.to_string(),
        path: path.to_string(),
        port: Port::Events,
        qualifier: Some(event_name.to_string()),
    }
}

/// Construct a commands topic for a path and verb.
pub fn commands(workspace: &str, path: &str, verb: &str) -> Self {
    Topic {
        workspace: workspace.to_string(),
        path: path.to_string(),
        port: Port::Commands,
        qualifier: Some(verb.to_string()),
    }
}
```

**Step 4: Update wildcard helpers**

```rust
/// Get the wildcard pattern for subscribing to sync requests.
pub fn sync_wildcard(workspace: &str, path: &str) -> String {
    format!("{}/{}/sync/+", workspace, path)
}

/// Get the wildcard pattern for subscribing to all events.
pub fn events_wildcard(workspace: &str, path: &str) -> String {
    format!("{}/{}/events/#", workspace, path)
}

/// Get the wildcard pattern for subscribing to all commands.
pub fn commands_wildcard(workspace: &str, path: &str) -> String {
    format!("{}/{}/commands/#", workspace, path)
}
```

**Step 5: Run tests**

Run: `cargo test --lib`
Expected: Compilation errors in callers (will fix in later tasks)

**Step 6: Commit**

```bash
git add src/mqtt/topics.rs
git commit -m "feat(mqtt): update Topic constructors to take workspace parameter"
```

---

## Task 4: Update to_topic_string

**Files:**
- Modify: `src/mqtt/topics.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_to_topic_string_with_workspace() {
    let topic = Topic::edits("commonplace", "terminal/screen.txt");
    assert_eq!(topic.to_topic_string(), "commonplace/terminal/screen.txt/edits");
}

#[test]
fn test_to_topic_string_with_qualifier() {
    let topic = Topic::sync("myspace", "doc.txt", "client-123");
    assert_eq!(topic.to_topic_string(), "myspace/doc.txt/sync/client-123");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib test_to_topic_string_with_workspace`
Expected: FAIL (wrong output)

**Step 3: Update to_topic_string**

```rust
/// Convert the topic to its string representation.
pub fn to_topic_string(&self) -> String {
    match &self.qualifier {
        Some(q) => format!("{}/{}/{}/{}", self.workspace, self.path, self.port.as_str(), q),
        None => format!("{}/{}/{}", self.workspace, self.path, self.port.as_str()),
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --lib test_to_topic_string_with`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mqtt/topics.rs
git commit -m "feat(mqtt): prepend workspace to topic strings"
```

---

## Task 5: Update Topic::parse

**Files:**
- Modify: `src/mqtt/topics.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_parse_with_workspace() {
    let topic = Topic::parse("commonplace/terminal/screen.txt/edits", "commonplace").unwrap();
    assert_eq!(topic.workspace, "commonplace");
    assert_eq!(topic.path, "terminal/screen.txt");
    assert_eq!(topic.port, Port::Edits);
}

#[test]
fn test_parse_wrong_workspace() {
    let result = Topic::parse("other/terminal/screen.txt/edits", "commonplace");
    assert!(result.is_err());
}

#[test]
fn test_parse_with_qualifier() {
    let topic = Topic::parse("myspace/doc.txt/sync/client-123", "myspace").unwrap();
    assert_eq!(topic.workspace, "myspace");
    assert_eq!(topic.qualifier, Some("client-123".to_string()));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib test_parse_with_workspace`
Expected: FAIL (wrong signature)

**Step 3: Update parse method**

```rust
/// Parse a topic string into its components.
///
/// Topic format: `{workspace}/{path}/{port}` or `{workspace}/{path}/{port}/{qualifier}`
///
/// The path ends at the segment containing a dot (the extension).
pub fn parse(topic_str: &str, expected_workspace: &str) -> Result<Self, MqttError> {
    let segments: Vec<&str> = topic_str.split('/').collect();

    if segments.len() < 3 {
        return Err(MqttError::InvalidTopic(format!(
            "Topic must have at least workspace, path, and port: {}",
            topic_str
        )));
    }

    // First segment is workspace
    let workspace = segments[0];
    if workspace != expected_workspace {
        return Err(MqttError::InvalidTopic(format!(
            "Topic workspace '{}' does not match expected '{}'",
            workspace, expected_workspace
        )));
    }

    // Find where the path ends (segment with a dot = has extension)
    let mut path_end_idx = None;
    for (i, segment) in segments.iter().enumerate().skip(1) {
        if segment.contains('.') {
            path_end_idx = Some(i);
            break;
        }
    }

    let path_end_idx = path_end_idx.ok_or_else(|| {
        MqttError::InvalidTopic(format!("Path must have an extension: {}", topic_str))
    })?;

    // Path is from index 1 to path_end_idx (skip workspace)
    let path = segments[1..=path_end_idx].join("/");

    // Validate the extension
    validate_extension(&path)?;

    // Next segment should be the port
    if path_end_idx + 1 >= segments.len() {
        return Err(MqttError::InvalidTopic(format!(
            "Missing port in topic: {}",
            topic_str
        )));
    }

    let port_str = segments[path_end_idx + 1];
    let port = Port::parse(port_str).ok_or_else(|| {
        MqttError::InvalidTopic(format!(
            "Invalid port '{}' in topic: {}",
            port_str, topic_str
        ))
    })?;

    // Remaining segments are the qualifier
    let qualifier = if path_end_idx + 2 < segments.len() {
        Some(segments[path_end_idx + 2..].join("/"))
    } else {
        None
    };

    Ok(Topic {
        workspace: workspace.to_string(),
        path,
        port,
        qualifier,
    })
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --lib test_parse_with`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mqtt/topics.rs
git commit -m "feat(mqtt): update Topic::parse to expect workspace prefix"
```

---

## Task 6: Update Existing Tests

**Files:**
- Modify: `src/mqtt/topics.rs`

**Step 1: Update all existing tests to use workspace**

Update the test module to use the new signatures:

```rust
#[test]
fn test_parse_edits_topic() {
    let topic = Topic::parse("commonplace/terminal/screen.txt/edits", "commonplace").unwrap();
    assert_eq!(topic.path, "terminal/screen.txt");
    assert_eq!(topic.port, Port::Edits);
    assert_eq!(topic.qualifier, None);
}

#[test]
fn test_parse_sync_topic() {
    let topic = Topic::parse("commonplace/terminal/screen.txt/sync/client-123", "commonplace").unwrap();
    assert_eq!(topic.path, "terminal/screen.txt");
    assert_eq!(topic.port, Port::Sync);
    assert_eq!(topic.qualifier, Some("client-123".to_string()));
}

// ... update remaining tests similarly
```

**Step 2: Run all tests**

Run: `cargo test --lib`
Expected: Some failures in other modules calling Topic methods

**Step 3: Commit**

```bash
git add src/mqtt/topics.rs
git commit -m "test(mqtt): update topic tests for workspace support"
```

---

## Task 7: Add Workspace to MqttConfig

**Files:**
- Modify: `src/mqtt/mod.rs`

**Step 1: Update MqttConfig struct**

```rust
/// Configuration for MQTT connection.
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// MQTT broker URL (e.g., "mqtt://localhost:1883")
    pub broker_url: String,
    /// Client ID for this doc store instance
    pub client_id: String,
    /// Workspace name for topic namespacing
    pub workspace: String,
    /// Keep-alive interval in seconds
    pub keep_alive_secs: u64,
    /// Whether to use clean session
    pub clean_session: bool,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker_url: "mqtt://localhost:1883".to_string(),
            client_id: uuid::Uuid::new_v4().to_string(),
            workspace: "commonplace".to_string(),
            keep_alive_secs: 60,
            clean_session: true,
        }
    }
}
```

**Step 2: Run tests**

Run: `cargo test --lib`
Expected: PASS (default handles existing callers)

**Step 3: Commit**

```bash
git add src/mqtt/mod.rs
git commit -m "feat(mqtt): add workspace field to MqttConfig with default"
```

---

## Task 8: Update MqttService Store Topics

**Files:**
- Modify: `src/mqtt/mod.rs`

**Step 1: Replace hardcoded $store topics**

Update `MqttService`:

```rust
impl MqttService {
    /// Create a new MQTT service with the given configuration.
    pub async fn new(
        config: MqttConfig,
        document_store: Arc<DocumentStore>,
        commit_store: Option<Arc<CommitStore>>,
    ) -> Result<Self, MqttError> {
        // Validate workspace name
        topics::validate_workspace_name(&config.workspace)?;

        let client = Arc::new(MqttClient::connect(config.clone()).await?);

        let edits_handler =
            edits::EditsHandler::new(client.clone(), document_store.clone(), commit_store.clone(), config.workspace.clone());

        let sync_handler = sync::SyncHandler::new(client.clone(), commit_store.clone(), config.workspace.clone());

        let events_handler = events::EventsHandler::new(client.clone(), config.workspace.clone());

        let commands_handler =
            commands::CommandsHandler::new(client.clone(), document_store.clone(), config.workspace.clone());

        Ok(Self {
            client,
            workspace: config.workspace,
            document_store,
            commit_store,
            edits_handler,
            sync_handler,
            events_handler,
            commands_handler,
        })
    }

    /// Get the store commands topic for this workspace.
    fn store_commands_topic(&self) -> String {
        format!("{}/commands/create-document", self.workspace)
    }

    /// Get the store responses topic for this workspace.
    fn store_responses_topic(&self) -> String {
        format!("{}/responses", self.workspace)
    }
}
```

**Step 2: Update subscribe_store_commands**

```rust
pub async fn subscribe_store_commands(&self) -> Result<(), MqttError> {
    let topic = self.store_commands_topic();
    self.client
        .subscribe(&topic, QoS::AtLeastOnce)
        .await?;
    tracing::debug!("Subscribed to store commands: {}", topic);
    Ok(())
}
```

**Step 3: Update dispatch_message**

```rust
async fn dispatch_message(&self, topic_str: &str, payload: &[u8]) -> Result<(), MqttError> {
    // Check for store-level commands first
    if topic_str == self.store_commands_topic() {
        return self.commands_handler.handle_create_document(payload).await;
    }

    // Parse the topic with workspace
    let topic = match topics::Topic::parse(topic_str, &self.workspace) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!("Ignoring unparseable topic {}: {}", topic_str, e);
            return Ok(());
        }
    };
    // ... rest unchanged
}
```

**Step 4: Commit**

```bash
git add src/mqtt/mod.rs
git commit -m "feat(mqtt): use workspace for store topics in MqttService"
```

---

## Task 9: Update Handlers with Workspace

**Files:**
- Modify: `src/mqtt/edits.rs`
- Modify: `src/mqtt/sync.rs`
- Modify: `src/mqtt/events.rs`
- Modify: `src/mqtt/commands.rs`

**Step 1: Update EditsHandler**

Add `workspace: String` field, update constructor, update all `Topic::` calls.

**Step 2: Update SyncHandler**

Add `workspace: String` field, update constructor, update all `Topic::` calls.

**Step 3: Update EventsHandler**

Add `workspace: String` field, update constructor, update all `Topic::` calls.

**Step 4: Update CommandsHandler**

Add `workspace: String` field, update constructor, update `$store/responses` to use workspace.

**Step 5: Run tests**

Run: `cargo test --lib`
Expected: PASS

**Step 6: Commit**

```bash
git add src/mqtt/edits.rs src/mqtt/sync.rs src/mqtt/events.rs src/mqtt/commands.rs
git commit -m "feat(mqtt): pass workspace through all handlers"
```

---

## Task 10: Add Workspace to OrchestratorConfig

**Files:**
- Modify: `src/orchestrator/config.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_workspace_field() {
    let json = r#"{
        "workspace": "myspace",
        "processes": {}
    }"#;
    let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.workspace, "myspace");
}

#[test]
fn test_workspace_default() {
    let json = r#"{ "processes": {} }"#;
    let config: OrchestratorConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.workspace, "commonplace");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib test_workspace_field`
Expected: FAIL

**Step 3: Update OrchestratorConfig**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    #[serde(default = "default_workspace")]
    pub workspace: String,
    #[serde(default = "default_mqtt_broker")]
    pub mqtt_broker: String,
    // ... rest unchanged
}

fn default_workspace() -> String {
    "commonplace".to_string()
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --lib test_workspace`
Expected: PASS

**Step 5: Commit**

```bash
git add src/orchestrator/config.rs
git commit -m "feat(orchestrator): add workspace field to config"
```

---

## Task 11: Add --workspace CLI Flag

**Files:**
- Modify: `src/cli.rs`

**Step 1: Add workspace to relevant Args structs**

Add to `StoreArgs`, `HttpArgs`, `CmdArgs`:

```rust
/// Workspace name for MQTT topic namespacing
#[clap(long, default_value = "commonplace")]
pub workspace: String,
```

**Step 2: Run build**

Run: `cargo build`
Expected: PASS

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): add --workspace flag to MQTT-using binaries"
```

---

## Task 12: Update Binaries to Use Workspace

**Files:**
- Modify: `src/bin/store.rs`
- Modify: `src/bin/cmd.rs`
- Modify: `src/bin/orchestrator.rs`

**Step 1: Update store binary**

Pass `workspace` from args to `MqttConfig`.

**Step 2: Update cmd binary**

Use workspace when constructing topics.

**Step 3: Update orchestrator**

Pass workspace from config to child processes via environment variable or args.

**Step 4: Run build**

Run: `cargo build`
Expected: PASS

**Step 5: Commit**

```bash
git add src/bin/store.rs src/bin/cmd.rs src/bin/orchestrator.rs
git commit -m "feat(bin): wire workspace through all binaries"
```

---

## Task 13: Integration Test

**Files:**
- Create: `tests/mqtt_workspace_test.rs` (or add to existing integration tests)

**Step 1: Write integration test**

Test that topics are correctly namespaced when using the MQTT service.

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Final commit**

```bash
git add .
git commit -m "test: add integration tests for workspace namespacing"
```

---

## Summary

After completing all tasks:
- All MQTT topics will be prefixed with workspace name
- Default workspace is "commonplace"
- Workspace name is validated (alphanumeric, hyphens, underscores)
- Configuration via commonplace.json or CLI flags
- Store topics (`$store/...`) replaced with `{workspace}/...`
