# Sandbox Sync Acceptance Criteria

**Date:** 2026-01-02
**Status:** Draft

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

## Acceptance Criteria Checklist

### Prerequisites

Start these processes in order:

```bash
# 1. Server (with persistence)
./target/release/commonplace-server --database ./data.redb --fs-root workspace

# 2. Workspace sync (syncs local ./workspace to server)
./target/release/commonplace-sync --server http://localhost:3000 --directory ./workspace --path ""

# 3. Orchestrator (discovers processes.json files from server, starts sandbox syncs)
./target/release/commonplace-orchestrator --server http://localhost:3000
```

The orchestrator uses recursive discovery mode by default. It reads `processes.json` files from the server's fs-root and automatically starts sandbox sync processes for each one.

> **Note:** An older `commonplace.json` config may reference `sandbox-workspace` - this is outdated. The standard is `workspace`.

- [ ] **P1**: Server is running with `--database` and `--fs-root workspace`
- [ ] **P2**: Workspace sync is running: `commonplace-sync --directory ./workspace --path ""`
- [ ] **P3**: Orchestrator is running and has started both sandbox processes
- [ ] **P4**: Bartleby sandbox exists at `/tmp/commonplace-sandbox-*/` with full workspace tree (including `bartleby/` subdirectory)
- [ ] **P5**: Text-to-telegram sandbox exists at `/tmp/commonplace-sandbox-*/` with `text-to-telegram/` files only

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

- [ ] **O1**: Stop the workspace sync process (Ctrl-C or kill)
- [ ] **O2**: Edit `workspace/bartleby/system_prompt.txt` to append "offline edit test"
- [ ] **O3**: Verify the server content has NOT changed (edit was local only)
- [ ] **O4**: Restart the workspace sync process
- [ ] **O5**: Within 5 seconds of startup, verify server content now includes "offline edit test"
- [ ] **O6**: Verify bartleby sandbox `bartleby/system_prompt.txt` also received the edit

### End-to-End Integration

- [ ] **I1**: Send a message via Telegram to the bot (e.g., "Hello!")
- [ ] **I2**: Verify message was received (check edit history for the content.txt/prompts.txt UUID)
- [ ] **I3**: Wait for bartleby to respond (may take 30-60 seconds, response is non-deterministic)
- [ ] **I4**: Verify a response was generated (check edit history for the output.txt/input.txt UUID)
- [ ] **I5**: Verify response is sent back via Telegram (content will vary)
- [ ] **I6**: Verify both prompt and response files are empty after consumption

### Process Termination Cascade

When the orchestrator is killed, all managed processes and their children should terminate.

- [ ] **T1**: Record the PIDs of all running processes:
  - Orchestrator PID
  - Bartleby sandbox sync PID
  - Bartleby process PID (python/uv)
  - Text-to-telegram sandbox sync PID
  - Text-to-telegram process PID
- [ ] **T2**: Kill only the orchestrator: `kill <orchestrator-pid>`
- [ ] **T3**: Within 5 seconds, verify all sandbox sync processes have terminated
- [ ] **T4**: Verify all subprocess PIDs (bartleby, text-to-telegram) have terminated
- [ ] **T5**: Verify no orphaned python/node processes remain from managed sandboxes
- [ ] **T6**: Verify sandbox directories are cleaned up (or marked for cleanup on restart)

> **Note:** This tests graceful shutdown cascade. Use `kill` (SIGTERM), not `kill -9` (SIGKILL), to allow proper cleanup signal propagation.

## Cleanup

- [ ] **X1**: Remove any test files created during verification
- [ ] **X2**: Verify sandbox directories still exist and processes are healthy

## Environment Reset

To fully reset the environment after testing:

```bash
# 1. Stop all processes
pkill -f commonplace-orchestrator
pkill -f commonplace-sync
pkill -f commonplace-server

# 2. Clean up sandbox directories
rm -rf /tmp/commonplace-sandbox-*

# 3. Remove test files from workspace
rm -f workspace/text-to-telegram/test-file.txt
rm -f workspace/bartleby/test-note.txt
rm -f workspace/shared-a.txt
rm -f workspace/bartleby/shared-b.txt

# 4. (Optional) Reset database for fresh start
rm -f ./data.redb

# 5. (Optional) Remove sync state files
find workspace -name ".commonplace-sync-state.json" -delete

# 6. Restart services
./target/release/commonplace-server --database ./data.redb --fs-root workspace &
sleep 2
./target/release/commonplace-sync --server http://localhost:3000 --directory ./workspace --path "" &
sleep 3
./target/release/commonplace-orchestrator --server http://localhost:3000 &
```

> **Note:** Removing the database (step 4) will lose all document history. Only do this if you want a completely fresh start.

## Notes

- All sync propagation should complete within 5 seconds under normal conditions
- If a step fails, check the sync logs at `/tmp/sync.log` and `/tmp/orchestrator.log`
- The Yjs CRDT ensures that concurrent edits merge correctly without conflicts
- Files with shared UUIDs are logically the same document - changes anywhere appear everywhere
- Bartleby is a non-deterministic AI agent - his responses will vary between runs, so only verify that *some* response was generated, not the exact content
- Bartleby's sandbox has the full workspace tree, allowing it to see files outside its primary directory
- Text-to-telegram's sandbox only contains its own subdirectory files

## Offline Editing Behavior

When sync is not running and files are edited locally, the sync process should detect and push those changes on startup:

1. Sync maintains a local state file (`.commonplace-sync-state.json`) tracking:
   - Last synced commit IDs for each document
   - Content hashes of synced files
2. On startup, sync compares local file hashes against the stored state
3. Any files that changed locally while sync was down are automatically pushed to the server
4. This ensures no edits are lost when sync restarts
