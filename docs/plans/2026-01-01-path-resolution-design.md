# CP-365: Path Resolution for Sandbox Sync

**Date:** 2026-01-01

## Problem

Currently, `processes.json` requires hardcoded UUIDs and full command specifications:

```json
{
  "processes": {
    "bartleby": {
      "command": [
        "commonplace-sync", "--server", "http://localhost:3000",
        "--node", "c721fded-1341-4394-b9ba-5493f19021dd",
        "--sandbox", "--initial-sync", "server",
        "--exec", "python bartleby.py"
      ]
    }
  }
}
```

UUIDs are fragile, hard to discover, and break when the server restarts with a fresh database.

## Solution

Two complementary features:

### 1. Path Resolution in Sync

The sync binary accepts paths instead of UUIDs:

```bash
commonplace-sync --path bartleby --sandbox --exec "python bartleby.py"
```

Resolution:
1. Query `GET /fs-root` → get fs-root document ID
2. Query `GET /docs/{fs-root}/head` → get schema
3. Traverse `schema.root.entries["bartleby"].node_id` → UUID
4. For nested paths like `foo/bar`, follow intermediate node_ids

### 2. Environment Variables

| Env Var | CLI Equivalent | Purpose |
|---------|----------------|---------|
| `COMMONPLACE_SERVER` | `--server` | Server URL (already exists) |
| `COMMONPLACE_NODE` | `--node` | Direct UUID (already exists) |
| `COMMONPLACE_PATH` | `--path` | Path to resolve (new) |
| `COMMONPLACE_INITIAL_SYNC` | `--initial-sync` | Sync strategy (new) |

### 3. Simplified Orchestrator Config

New `sandbox-exec` field for common case:

```json
{
  "processes": {
    "bartleby": {
      "sandbox-exec": "uv run --project /home/jes/bartleby python bartleby.py"
    },
    "text-to-telegram": {
      "sandbox-exec": "uv run --project /home/jes/text-to-telegram python -m text_to_telegram"
    }
  }
}
```

When orchestrator spawns a `sandbox-exec` process:
1. Constructs: `commonplace-sync --sandbox --exec "<value>"`
2. Sets env vars:
   - `COMMONPLACE_SERVER` = orchestrator's server URL
   - `COMMONPLACE_PATH` = process name
   - `COMMONPLACE_INITIAL_SYNC` = `server`

The `command` field remains supported for custom processes; env vars are still set.

## Implementation Plan

### Step 1: Add --path to sync

File: `src/bin/sync.rs`

- Add `--path` CLI arg and `COMMONPLACE_PATH` env var
- Add `resolve_path_to_uuid()` function:
  - GET `/fs-root` to get root doc ID
  - GET `/docs/{id}/head` to get schema
  - Traverse path segments through entries
- Use resolved UUID for syncing

### Step 2: Add COMMONPLACE_INITIAL_SYNC env var

File: `src/bin/sync.rs`

- Add env var support for `--initial-sync`

### Step 3: Add sandbox-exec to orchestrator

File: `src/orchestrator/discovery.rs`

- Add `sandbox_exec` field to `DiscoveredProcess`
- In spawn logic, construct sync command and set env vars

## Error Handling

- Path not found: Clear error message with available entries
- Server unreachable: Same as current behavior
- Both `--node` and `--path` provided: Error, pick one

## Testing

1. Unit test: `resolve_path_to_uuid()` with mock schema
2. Integration test: Start server with fs-root, resolve paths
3. E2E test: Orchestrator with `sandbox-exec` spawns working sandbox
