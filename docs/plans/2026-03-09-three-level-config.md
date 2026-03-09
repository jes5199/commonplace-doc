# Three-Level Config Integration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire `CommonplaceConfig` (from `commonplace-types::config`) into all CLI binaries so settings resolve as: config file < env var < CLI flag.

**Architecture:** Each clap struct gets a `resolve(config: &CommonplaceConfig) -> Self` method that fills in `None` fields from config, falling back to hardcoded defaults. The `default_value` annotations on clap fields for `server`, `mqtt_broker`, and `workspace` are removed, making those fields `Option<String>`. After `Args::parse()`, call `args.resolve(&config)` to get final values. Env vars still work via clap's `env = "..."` attribute (they override config file values because clap sets the field to `Some(...)` when the env var is present).

**Tech Stack:** Rust, clap, serde_json, commonplace-types

---

## Design Decisions

- **Config file location:** `~/.commonplace/config.json` (or `$COMMONPLACE_CONFIG`)
- **Precedence:** CLI flag > env var > config file > hardcoded default
- **Mechanism:** Remove `default_value` from clap fields that should be config-aware, making them `Option<String>`. After parse, resolve against `CommonplaceConfig`. Clap's `env` still works because it sets the field to `Some(value)`.
- **Which fields are config-aware:** `server`, `mqtt_broker`, `workspace`, `name`, `extension`
- **Which binaries need changes:** All binaries in `src/bin/` that reference `DEFAULT_SERVER_URL`, `DEFAULT_MQTT_BROKER_URL`, or `DEFAULT_WORKSPACE`.

## Affected Structs (in `src/cli.rs`)

| Struct | server | mqtt_broker | workspace |
|--------|--------|-------------|-----------|
| `Args` (server) | via `mqtt_broker` | `Option` | `default_value` |
| `StoreArgs` | - | required | `default_value` |
| `HttpArgs` | - | required | `default_value` |
| `OrchestratorArgs` | `default_value` | `Option` | - |
| `CmdArgs` | - | `default_value` | `default_value` |
| `LinkArgs` | `default_value` | - | - |
| `ReplayArgs` | `default_value` | - | - |
| `LogArgs` | `default_value` | - | - |
| `ShowArgs` | `default_value` | - | - |
| `EventArgs` | `default_value` | - | - |
| `StatusArgs` | - | `default_value` | `default_value` |
| `BranchArgs` | `default_value` | `default_value` | `default_value` |
| `CheckoutArgs` | `default_value` | `default_value` | `default_value` |
| `InitArgs` | `default_value` | - | - |
| `WorktreeArgs` | - | `default_value` | `default_value` |

And in `src/bin/sync.rs`:

| Struct | server | mqtt_broker | workspace |
|--------|--------|-------------|-----------|
| `Args` (sync) | `default_value` | required | `default_value` |

---

### Task 1: Add `resolve_config` helper to `commonplace-types::config`

**Files:**
- Modify: `crates/commonplace-types/src/config.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_resolve_field_precedence() {
    let config = CommonplaceConfig {
        server: Some("http://config-server:5199".to_string()),
        ..Default::default()
    };
    // CLI/env wins over config
    assert_eq!(
        resolve_field(Some("http://cli-server:5199".to_string()), config.server.as_deref(), "http://default:5199"),
        "http://cli-server:5199"
    );
    // Config wins over hardcoded default
    assert_eq!(
        resolve_field(None, config.server.as_deref(), "http://default:5199"),
        "http://config-server:5199"
    );
    // Hardcoded default when both are None
    let empty_config = CommonplaceConfig::default();
    assert_eq!(
        resolve_field(None, empty_config.server.as_deref(), "http://default:5199"),
        "http://default:5199"
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p commonplace-types test_resolve_field_precedence`
Expected: FAIL — `resolve_field` not defined

**Step 3: Implement `resolve_field`**

Add to `config.rs`:

```rust
/// Resolve a single config field with three-level precedence:
/// CLI/env (Some) > config file > hardcoded default.
pub fn resolve_field(cli_value: Option<String>, config_value: Option<&str>, default: &str) -> String {
    cli_value
        .or_else(|| config_value.map(|s| s.to_string()))
        .unwrap_or_else(|| default.to_string())
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p commonplace-types test_resolve_field_precedence`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/commonplace-types/src/config.rs
git commit -m "feat(config): add resolve_field for three-level precedence"
```

---

### Task 2: Convert `src/cli.rs` structs — remove `default_value` for config-aware fields

**Files:**
- Modify: `src/cli.rs`

This is a large mechanical change. For each struct that has `server`, `mqtt_broker`, or `workspace` with a `default_value = DEFAULT_*`:

1. Change the field type from `String` to `Option<String>`
2. Remove the `default_value = DEFAULT_*` attribute
3. Keep the `env = "..."` attribute (clap will set `Some(value)` if env var is present)

**Important exceptions:**
- `StoreArgs.mqtt_broker` and `HttpArgs.mqtt_broker` are **required** fields (no default) — leave as `String`
- `Args.port`, `Args.host` — not config-aware, leave as-is
- Sync's `Args.mqtt_broker` is also required — leave as `String`

**Step 1: Make the changes to cli.rs**

For each affected field, the pattern is:
```rust
// Before:
#[clap(long, default_value = DEFAULT_SERVER_URL)]
pub server: String,

// After:
#[clap(long)]
pub server: Option<String>,
```

And for fields with env:
```rust
// Before:
#[clap(long, env = "COMMONPLACE_BROKER", default_value = DEFAULT_MQTT_BROKER_URL)]
pub mqtt_broker: String,

// After:
#[clap(long, env = "COMMONPLACE_BROKER")]
pub mqtt_broker: Option<String>,
```

Apply to all structs listed in the table above. Leave required fields as `String`.

**Step 2: Verify it compiles (expect errors in binaries)**

Run: `cargo check --lib 2>&1 | head -30`
Expected: Library compiles (binaries will fail since they access fields as `String`)

**Step 3: Commit the cli.rs changes**

```bash
git add src/cli.rs
git commit -m "refactor(cli): make server/mqtt_broker/workspace fields Optional for config resolution"
```

---

### Task 3: Add resolution calls in each binary

**Files:**
- Modify: `src/bin/server.rs`
- Modify: `src/bin/sync.rs`
- Modify: `src/bin/orchestrator.rs`
- Modify: `src/bin/cmd.rs`
- Modify: `src/bin/link.rs`
- Modify: `src/bin/replay.rs`
- Modify: `src/bin/log.rs`
- Modify: `src/bin/show.rs`
- Modify: `src/bin/event.rs`
- Modify: `src/bin/status.rs`
- Modify: `src/bin/branch.rs`
- Modify: `src/bin/checkout.rs`
- Modify: `src/bin/init.rs`
- Modify: `src/bin/worktree.rs`
- Modify: `src/bin/store.rs`
- Modify: `src/bin/http.rs`
- Modify: `src/bin/beads_bridge.rs`

Each binary follows the same pattern. After `Args::parse()`, load config and resolve:

```rust
use commonplace_types::config::{CommonplaceConfig, resolve_field};
use commonplace_doc::{DEFAULT_SERVER_URL, DEFAULT_MQTT_BROKER_URL, DEFAULT_WORKSPACE};

let args = Args::parse();
let config = CommonplaceConfig::load().unwrap_or_default();

// Resolve fields — example for a binary with server + workspace:
let server = resolve_field(args.server, config.server.as_deref(), DEFAULT_SERVER_URL);
let workspace = resolve_field(args.workspace, config.workspace.as_deref(), DEFAULT_WORKSPACE);
```

Then use `server` and `workspace` locals instead of `args.server` and `args.workspace` throughout.

**Step 1: Update each binary**

For each binary, add the config load + resolve pattern after `Args::parse()`. The specific fields depend on which struct each binary uses (see table in Design Decisions). Each binary should also log the config source at debug level:

```rust
tracing::debug!("Config: server={server}, workspace={workspace}");
```

**Step 2: Verify everything compiles**

Run: `cargo build 2>&1 | tail -20`
Expected: Clean build

**Step 3: Run tests**

Run: `cargo test`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/bin/
git commit -m "feat(config): wire three-level config into all CLI binaries"
```

---

### Task 4: Update sync binary's local Args struct

**Files:**
- Modify: `src/bin/sync.rs`

The sync binary has its own `Args` struct (not from cli.rs). Apply the same pattern:

1. Change `server: String` → `server: Option<String>` (remove `default_value`)
2. Change `workspace: String` → `workspace: Option<String>` (remove `default_value`)
3. `mqtt_broker` is required — leave as `String` (but resolve from config if we want it optional)
4. Add config resolution after parse

**Step 1: Modify sync's Args struct**

```rust
// server field:
#[arg(short, long, env = "COMMONPLACE_SERVER")]
server: Option<String>,

// workspace field:
#[arg(long, env = "COMMONPLACE_WORKSPACE")]
workspace: Option<String>,
```

**Step 2: Add config resolution in main()**

After `let args = Args::parse();`:
```rust
let config = CommonplaceConfig::load().unwrap_or_default();
let server = resolve_field(args.server, config.server.as_deref(), DEFAULT_SERVER_URL);
let workspace = resolve_field(args.workspace, config.workspace.as_deref(), DEFAULT_WORKSPACE);
```

Then replace all `args.server` → `server` and `args.workspace` → `workspace` references.

**Step 3: Verify it compiles and tests pass**

Run: `cargo build --bin commonplace-sync && cargo test`
Expected: Clean build, all tests pass

**Step 4: Commit**

```bash
git add src/bin/sync.rs
git commit -m "feat(config): wire three-level config into commonplace-sync"
```

---

### Task 5: Integration test — config file overrides defaults

**Files:**
- Create: `tests/config_integration.rs`

**Step 1: Write the test**

```rust
use commonplace_types::config::{CommonplaceConfig, resolve_field};
use std::path::Path;

#[test]
fn config_file_provides_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    std::fs::write(&path, r#"{"server": "http://myserver:9999", "workspace": "myws"}"#).unwrap();

    let config = CommonplaceConfig::load_from(&path).unwrap();

    // Config overrides hardcoded default
    assert_eq!(resolve_field(None, config.server.as_deref(), "http://localhost:5199"), "http://myserver:9999");
    assert_eq!(resolve_field(None, config.workspace.as_deref(), "commonplace"), "myws");

    // CLI value overrides config
    assert_eq!(resolve_field(Some("http://cli:1234".into()), config.server.as_deref(), "http://localhost:5199"), "http://cli:1234");
}

#[test]
fn missing_config_falls_back_to_default() {
    let config = CommonplaceConfig::load_from(Path::new("/nonexistent")).unwrap();
    assert_eq!(resolve_field(None, config.server.as_deref(), "http://localhost:5199"), "http://localhost:5199");
}
```

**Step 2: Run test**

Run: `cargo test -p commonplace-types config_integration`
Expected: PASS (these tests are in commonplace-types, so put them in config.rs tests module)

**Step 3: Commit**

```bash
git add crates/commonplace-types/src/config.rs
git commit -m "test(config): integration tests for three-level precedence"
```

---

### Task 6: Verify clippy + final cleanup

**Step 1: Run clippy**

Run: `cargo clippy --all-targets 2>&1 | head -30`
Expected: No warnings (fix any unused import warnings from removing `DEFAULT_*` usage)

**Step 2: Clean up unused imports**

Remove `DEFAULT_SERVER_URL`, `DEFAULT_MQTT_BROKER_URL`, `DEFAULT_WORKSPACE` imports from files that no longer use them directly. Note: `src/cli.rs` may still need them for the `fetch_head` helper and other non-config code. Check each file.

**Step 3: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 4: Commit and close bead**

```bash
git add -A
git commit -m "chore: clippy cleanup for three-level config"
bd close CP-ri6h
git push
```
