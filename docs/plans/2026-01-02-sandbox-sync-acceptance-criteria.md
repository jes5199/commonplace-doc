# Sandbox Sync Acceptance Criteria

**Date:** 2026-01-02
**Updated:** 2026-01-10
**Status:** Active

## Overview

This document describes the acceptance criteria for the commonplace sync system with sandboxed processes. An agent should be able to verify each criterion in order.

## Directory Topology

Three directories are synced to the same commonplace server:

1. **Workspace** (`./workspace`) - The main synced directory on the host
2. **Bartleby Sandbox** (`/tmp/commonplace-sandbox-*/`) - Isolated directory for bartleby process
3. **Text-to-Telegram Sandbox** (`/tmp/commonplace-sandbox-*/`) - Isolated directory for text-to-telegram process

Each sandbox syncs a portion of the server's fs-root:
- **Bartleby sandbox** syncs the entire `workspace/` tree, with `bartleby/` as a subdirectory within it. This gives bartleby access to the full workspace, making the agent more useful since it can see and interact with files outside its own directory.
- **Text-to-telegram sandbox** syncs only the `workspace/text-to-telegram/` subdirectory. It only sees its own files.

The workspace directory on the host contains the authoritative structure that the sandboxes derive from.

## Process Configuration Topology

Discovered processes are defined in `__processes.json` files (note the double underscore prefix). Each sandbox's sync scope is determined by where its `__processes.json` is located. A process syncs at the directory containing its `__processes.json`:

```
workspace/
├── __processes.json        ← bartleby defined here (syncs entire workspace/)
├── bartleby/
│   └── (no __processes.json - bartleby is defined at root level)
├── text-to-telegram/
│   └── __processes.json    ← text-to-telegram defined here (syncs only this subdirectory)
└── tmux/
    └── __processes.json    ← file-tmux-file defined here (syncs only this subdirectory)
```

**Key principle:** If a process needs access to the entire workspace tree, define it in `workspace/__processes.json`. If a process only needs its own subdirectory, define it in `workspace/<subdirectory>/__processes.json`.

### Process Definition Format

Each `__processes.json` contains a `processes` object with named process definitions:

```json
{
  "processes": {
    "bartleby": {
      "sandbox-exec": "/home/jes/.local/bin/uv run --project /home/jes/bartleby python /home/jes/bartleby/bartleby.py"
    }
  }
}
```

The `sandbox-exec` field specifies the command to run inside a sandbox. The orchestrator automatically:
1. Creates a temporary sandbox directory
2. Starts a sync process for that sandbox
3. Sets environment variables (COMMONPLACE_SERVER, COMMONPLACE_PATH)
4. Runs the specified command

## UUID Link Topology

Files can share the same commonplace UUID, causing them to automatically share content via CRDT sync:

```
workspace/
├── bartleby/
│   ├── prompts.txt ─────── UUID-A ◄──┐
│   └── output.txt ──────── UUID-B ───┼──┐
│                                     │  │
└── text-to-telegram/                 │  │
    ├── content.txt ───── UUID-A ─────┘  │  (incoming to bartleby)
    └── input.txt ─────── UUID-B ────────┘  (outgoing from bartleby)
```

- `text-to-telegram/content.txt` and `bartleby/prompts.txt` share UUID-A
- `bartleby/output.txt` and `text-to-telegram/input.txt` share UUID-B

## Orchestrator Configuration

The orchestrator is configured via `commonplace.json` in the project root:

```json
{
  "database": "./data/commonplace.redb",
  "mqtt_broker": "localhost:1883",
  "managed_paths": ["/"],
  "processes": {
    "server": {
      "command": "./target/release/commonplace-server",
      "args": ["--fs-root", "workspace"],
      "restart": { "policy": "always", "backoff_ms": 500, "max_backoff_ms": 10000 }
    },
    "sync": {
      "command": "./target/release/commonplace-sync",
      "args": ["--server", "http://localhost:3000", "--node", "workspace", "--directory", "./workspace", "--initial-sync", "local"],
      "restart": { "policy": "always", "backoff_ms": 500, "max_backoff_ms": 10000 },
      "depends_on": ["server"]
    }
  }
}
```

**Key fields:**
- `database`: Path to the redb database file. The orchestrator automatically passes `--database` to the server.
- `managed_paths`: Paths to monitor for `__processes.json` discovery (use `["/"]` for recursive)
- `processes`: Base processes (server, sync) that the orchestrator manages directly

## Acceptance Criteria Checklist

### Prerequisites

Start the orchestrator (it manages everything else):

```bash
# Build release binaries
cargo build --release

# Start orchestrator (manages server, sync, and discovered processes)
./target/release/commonplace-orchestrator
```

The orchestrator:
1. Starts the server with `--database` from config
2. Waits for server health check
3. Starts the workspace sync
4. Discovers `__processes.json` files via SSE subscription
5. Starts sandbox processes for each discovered process

Check status with `commonplace-ps`:

```bash
$ commonplace-ps
Orchestrator PID: 12345 (started at 2026-01-10 19:36:56)

NAME                           PID STATE      SOURCE               CWD
----------------------------------------------------------------------------------------------------
bartleby                   12350 Running    /                    /tmp/commonplace-sandbox-abc123
beads-sync                 12348 Running    (commonplace.json)   /home/jes/commonplace
file-tmux-file             12351 Running    /tmux                /tmp/commonplace-sandbox-def456
server                     12346 Running    (commonplace.json)   /home/jes/commonplace
sync                       12347 Running    (commonplace.json)   /home/jes/commonplace
text-to-telegram           12352 Running    /text-to-telegram    /tmp/commonplace-sandbox-ghi789
```

- [ ] **P1**: Orchestrator is running: `commonplace-ps` shows orchestrator PID
- [ ] **P2**: Server is running with database persistence
- [ ] **P3**: Workspace sync is running
- [ ] **P4**: Bartleby is discovered and running (source: `/`)
- [ ] **P5**: Text-to-telegram is discovered and running (source: `/text-to-telegram`)
- [ ] **P6**: File-tmux-file is discovered and running (source: `/tmux`)

### File Creation Propagation

- [ ] **C1**: Create a new file `workspace/text-to-telegram/test-file.txt` with content "hello"
- [ ] **C2**: Within 5 seconds, verify file appears in text-to-telegram sandbox at `test-file.txt`
- [ ] **C3**: Verify the content matches: "hello"
- [ ] **C4**: Create a new file `workspace/bartleby/test-note.txt` with content "note"
- [ ] **C5**: Within 5 seconds, verify file appears in bartleby sandbox at `bartleby/test-note.txt`
- [ ] **C6**: Verify the content matches: "note"

### Edit Propagation

- [ ] **E1**: Edit `workspace/text-to-telegram/test-file.txt` to append " world"
- [ ] **E2**: Within 5 seconds, verify text-to-telegram sandbox `test-file.txt` shows "hello world"
- [ ] **E3**: Edit the file in text-to-telegram sandbox to append "!"
- [ ] **E4**: Within 5 seconds, verify `workspace/text-to-telegram/test-file.txt` shows "hello world!"
- [ ] **E5**: Edit `workspace/bartleby/test-note.txt` to change content to "updated note"
- [ ] **E6**: Within 5 seconds, verify bartleby sandbox `bartleby/test-note.txt` shows "updated note"

### JSONL File Handling

JSONL (JSON Lines) files use append-style updates where each line is a valid JSON object.

- [ ] **J1**: Create a new JSONL file: `echo '{"event": "start"}' > workspace/bartleby/test.jsonl`
- [ ] **J2**: Within 5 seconds, verify file appears in bartleby sandbox at `bartleby/test.jsonl`
- [ ] **J3**: Verify the content matches: `{"event": "start"}`
- [ ] **J4**: Append a line: `echo '{"event": "middle"}' >> workspace/bartleby/test.jsonl`
- [ ] **J5**: Within 5 seconds, verify bartleby sandbox file has both lines
- [ ] **J6**: Append from the sandbox: `echo '{"event": "end"}' >> /tmp/commonplace-sandbox-*/bartleby/test.jsonl`
- [ ] **J7**: Within 5 seconds, verify `workspace/bartleby/test.jsonl` has all three lines
- [ ] **J8**: Delete the test file: `rm workspace/bartleby/test.jsonl`

### File Deletion Propagation

- [ ] **D1**: Delete `workspace/text-to-telegram/test-file.txt`
- [ ] **D2**: Within 5 seconds, verify file is removed from text-to-telegram sandbox
- [ ] **D3**: Delete `workspace/bartleby/test-note.txt`
- [ ] **D4**: Within 5 seconds, verify file is removed from bartleby sandbox at `bartleby/test-note.txt`

### commonplace-link UUID Sharing

- [ ] **L1**: Create two files in different directories:
  - `workspace/shared-a.txt` with content "original"
  - `workspace/bartleby/shared-b.txt` (empty)
- [ ] **L2**: Run `commonplace-link workspace/shared-a.txt workspace/bartleby/shared-b.txt`
- [ ] **L3**: Verify both files now have the same UUID in `.commonplace.json`
- [ ] **L4**: Verify `workspace/bartleby/shared-b.txt` now contains "original"
- [ ] **L5**: Edit `workspace/bartleby/shared-b.txt` to "modified"
- [ ] **L6**: Within 5 seconds, verify `workspace/shared-a.txt` shows "modified"
- [ ] **L7**: Verify the linked file also appears in bartleby sandbox with "modified"

### Telegram-to-Bartleby Message Flow

This tests the full message round-trip using UUID-linked files.

#### Incoming Message (Telegram → Bartleby)

- [ ] **M1**: Append a line to `workspace/text-to-telegram/content.txt`: "Hello bartleby\n"
- [ ] **M2**: Verify the message reached bartleby's prompts.txt (check edit history if consumed quickly: `GET /documents/{uuid}/changes`)
- [ ] **M3**: Bartleby consumes the prompt (deletes the line from his `bartleby/prompts.txt`)
- [ ] **M4**: Within 5 seconds, verify `workspace/bartleby/prompts.txt` is empty or has line removed
- [ ] **M5**: Verify `workspace/text-to-telegram/content.txt` also has the line removed (UUID-linked)

#### Outgoing Response (Bartleby → Telegram)

- [ ] **M6**: Bartleby appends a response to his `bartleby/output.txt` (response is non-deterministic)
- [ ] **M7**: Verify the response reached text-to-telegram's input.txt (check edit history if consumed quickly: `GET /documents/{uuid}/changes`)
- [ ] **M8**: Text-to-telegram consumes the message (sends to Telegram, clears file)
- [ ] **M9**: Within 5 seconds, verify `workspace/text-to-telegram/input.txt` is empty
- [ ] **M10**: Verify `workspace/bartleby/output.txt` is also empty (UUID-linked)

> **Tip:** For files that are consumed immediately, use the server's edit history API (`GET /documents/{id}/changes`) to verify the content was present, rather than racing to read the file before consumption.

### Offline Editing

- [ ] **O1**: Stop the workspace sync process: `commonplace-signal -n sync -s TERM`
- [ ] **O2**: Edit `workspace/bartleby/system_prompt.txt` to append "offline edit test"
- [ ] **O3**: Verify the server content has NOT changed (edit was local only)
- [ ] **O4**: Wait for orchestrator to restart sync (automatic restart policy)
- [ ] **O5**: Within 5 seconds of restart, verify server content now includes "offline edit test"
- [ ] **O6**: Verify bartleby sandbox `bartleby/system_prompt.txt` also received the edit

### End-to-End Integration

- [ ] **I1**: Send a message via Telegram to the bot (e.g., "Hello!")
- [ ] **I2**: Verify message appears in `workspace/text-to-telegram/content.txt` (NOT `workspace/content.txt`)
- [ ] **I3**: Verify message syncs to `workspace/bartleby/prompts.txt` via UUID link
- [ ] **I4**: Wait for bartleby to respond (may take 30-60 seconds, response is non-deterministic)
- [ ] **I5**: Verify response was generated in `workspace/bartleby/output.txt`
- [ ] **I6**: Verify response syncs to `workspace/text-to-telegram/input.txt` via UUID link
- [ ] **I7**: Verify response is sent back via Telegram (content will vary)
- [ ] **I8**: Verify both prompt and response files are empty after consumption

> **IMPORTANT:** Text-to-telegram MUST write to `text-to-telegram/content.txt` (the UUID-linked file),
> not `content.txt` at workspace root. If messages appear in `workspace/content.txt`, the sandbox
> configuration is wrong - text-to-telegram needs its own subdirectory `__processes.json` so it only
> sees its own files.

### Process Termination Cascade

When the orchestrator is killed, all managed processes and their children should terminate.

- [ ] **T1**: Record the PIDs: `commonplace-ps`
- [ ] **T2**: Kill only the orchestrator: `kill $(commonplace-ps --json | jq -r '.orchestrator_pid')`
- [ ] **T3**: Within 5 seconds, verify all processes have terminated: `ps aux | grep commonplace`
- [ ] **T4**: Verify no orphaned python/node processes remain from managed sandboxes
- [ ] **T5**: Verify sandbox directories still exist (cleanup happens on next orchestrator start)

> **Note:** This tests graceful shutdown cascade. Use `kill` (SIGTERM), not `kill -9` (SIGKILL), to allow proper cleanup signal propagation.

### Process Hot-Reload

The orchestrator watches `__processes.json` files and restarts processes when configuration changes.

- [ ] **H1**: Edit `workspace/__processes.json` to change bartleby's command slightly
- [ ] **H2**: Within 10 seconds, verify bartleby process restarted with new PID: `commonplace-ps`
- [ ] **H3**: Add a new process to `workspace/__processes.json`
- [ ] **H4**: Within 10 seconds, verify new process appears in `commonplace-ps`
- [ ] **H5**: Remove a process from `workspace/__processes.json`
- [ ] **H6**: Within 10 seconds, verify process is stopped and removed from `commonplace-ps`

## Cleanup

- [ ] **X1**: Remove any test files created during verification
- [ ] **X2**: Verify sandbox directories still exist and processes are healthy: `commonplace-ps`

## Environment Reset

To fully reset the environment after testing:

```bash
# 1. Stop orchestrator (stops all managed processes)
pkill -f commonplace-orchestrator

# 2. Wait for graceful shutdown
sleep 5

# 3. Clean up sandbox directories
rm -rf /tmp/commonplace-sandbox-*

# 4. Remove test files from workspace
rm -f workspace/text-to-telegram/test-file.txt
rm -f workspace/bartleby/test-note.txt
rm -f workspace/shared-a.txt
rm -f workspace/bartleby/shared-b.txt

# 5. (Optional) Reset database for fresh start
rm -f ./data/commonplace.redb

# 6. (Optional) Remove sync state files
find workspace -name ".commonplace-sync-state.json" -delete

# 7. Restart orchestrator
./target/release/commonplace-orchestrator
```

> **Note:** Removing the database (step 5) will lose all document history. Only do this if you want a completely fresh start.

## CLI Tools Reference

| Command | Description |
|---------|-------------|
| `commonplace-ps` | List all managed processes and their status |
| `commonplace-signal -n <name> -s <signal>` | Send signal to a named process |
| `commonplace-uuid <path>` | Get UUID for a synced file |
| `commonplace-replay <path> --list` | List commit history for a file |
| `commonplace-link <source> <target>` | Link two files to share same UUID |

## Notes

- All sync propagation should complete within 5 seconds under normal conditions
- Use `commonplace-ps` to check process status and find sandbox paths
- The Yjs CRDT ensures that concurrent edits merge correctly without conflicts
- Files with shared UUIDs are logically the same document - changes anywhere appear everywhere
- Bartleby is a non-deterministic AI agent - his responses will vary between runs
- Bartleby's sandbox has the full workspace tree, allowing it to see files outside its primary directory
- Text-to-telegram's sandbox only contains its own subdirectory files
- Database persistence is critical - without `--database`, server loses all data on restart

## Sync Protection

The sync system includes three-layer protection against data loss during concurrent edits:

1. **Shadow Hardlinks**: Detect writes to old inodes after atomic rename
2. **Flock Protection**: Coordinate with agents via advisory file locks
3. **Ancestry Checking**: Ensure causal ordering of CRDT commits

See `docs/SYNC_PROTECTION.md` for detailed documentation.

## Offline Editing Behavior

When sync is not running and files are edited locally, the sync process should detect and push those changes on startup:

1. Sync maintains a local state file (`.commonplace-sync-state.json`) tracking:
   - Last synced commit IDs for each document
   - Content hashes of synced files
2. On startup, sync compares local file hashes against the stored state
3. Any files that changed locally while sync was down are automatically pushed to the server
4. This ensures no edits are lost when sync restarts

## Troubleshooting

### Server shows "Failed" state
Check if the database file is locked by another process. The server requires exclusive access to the database file.

### Processes not discovered
1. Verify `__processes.json` exists and has valid JSON
2. Check that the file is synced to server: `curl http://localhost:3000/docs/<uuid>/head`
3. Check orchestrator logs for discovery errors

### Sandbox files not syncing
1. Verify sandbox sync is running: `commonplace-ps`
2. Check sandbox directory exists: look at CWD column in `commonplace-ps`
3. Verify the sandbox has correct files: `ls /tmp/commonplace-sandbox-*/`

### Empty `__processes.json` after restart
If the server restarts without database persistence, it loses all data. When sync reconnects, it may pull empty content from the server and overwrite local files. Always ensure the `database` field is set in `commonplace.json`.
