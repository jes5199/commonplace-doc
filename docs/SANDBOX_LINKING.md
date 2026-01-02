# Sandbox Linking Architecture

## Overview

Multiple sandboxed processes (bartleby, text-to-telegram, etc.) need to share files through commonplace sync without breaking sandbox isolation. Each sandbox is a subdirectory of a single synced workspace.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Commonplace Server                        │
│                                                              │
│                    ┌──────────────────┐                      │
│                    │    workspace     │                      │
│                    │    (fs-root)     │                      │
│                    └──────────────────┘                      │
│                             │                                │
│              ┌──────────────┴──────────────┐                 │
│              │                             │                 │
│       UUID-A (shared)              UUID-B (shared)           │
│              │                             │                 │
└──────────────┼─────────────────────────────┼─────────────────┘
               │                             │
               ▼                             ▼
┌─────────────────────────────────────────────────────────────┐
│  workspace/                   ← ONE fs-root                  │
│  ├── text-to-telegram/        ← Sandbox 1                    │
│  │   ├── content.txt ─────────── UUID-A ───┐                │
│  │   └── input.txt ───────────── UUID-B ◄──┼──┐             │
│  │                                         │  │             │
│  └── bartleby/                ← Sandbox 2  │  │             │
│      ├── prompts.txt ─────────── UUID-A ◄──┘  │             │
│      ├── output.txt ──────────── UUID-B ──────┘             │
│      └── system_prompt.txt                                   │
└─────────────────────────────────────────────────────────────┘

Links (same UUID = same content):
  text-to-telegram/content.txt ←→ bartleby/prompts.txt   (UUID-A)
  text-to-telegram/input.txt   ←→ bartleby/output.txt    (UUID-B)
```

## Setup Process

> **IMPORTANT:** The server MUST be started with `--database` for schema push to work.
> The linking workflow requires persistence to push local schemas to the server.

### 1. Start the server with persistence

```bash
commonplace-server --fs-root workspace --database /path/to/data.redb
```

### 2. Set up the directory structure

```bash
mkdir -p /path/to/workspace/text-to-telegram
mkdir -p /path/to/workspace/bartleby

# Create initial files
touch /path/to/workspace/text-to-telegram/content.txt
touch /path/to/workspace/text-to-telegram/input.txt
touch /path/to/workspace/bartleby/prompts.txt
touch /path/to/workspace/bartleby/output.txt
touch /path/to/workspace/bartleby/system_prompt.txt
```

### 3. Run initial sync with local strategy

This creates the `.commonplace.json` schema from the local directory structure:

```bash
commonplace-sync --directory /path/to/workspace \
  --node workspace --server http://localhost:3000 \
  --initial-sync local
```

After running, stop the sync (Ctrl-C) before creating links.

### 4. Create links between sandbox files

```bash
cd /path/to/workspace
commonplace-link text-to-telegram/content.txt bartleby/prompts.txt
commonplace-link bartleby/output.txt text-to-telegram/input.txt
```

This modifies `.commonplace.json` to give linked files the same UUID.

### 5. Push linked schema and start sync

Run sync again with `--initial-sync local` to push the linked schema:

```bash
commonplace-sync --directory /path/to/workspace \
  --node workspace --server http://localhost:3000 \
  --initial-sync local
```

> **CRITICAL:** You MUST use `--initial-sync local` after creating links to push
> the updated schema to the server. Otherwise, the server won't use your linked UUIDs.

### 6. How linking works

When `commonplace-link A B` is run:
1. Gets or creates a UUID for file A
2. Assigns the same UUID to file B in the schema
3. Both files now have the same content (synced via the shared UUID)
4. Changes to either file propagate to the other via sync

## File Flow Example

```
User sends Telegram message
         │
         ▼
text-to-telegram/content.txt  ──sync──►  Server (UUID-A)
                                              │
                                              │ (same UUID)
                                              ▼
bartleby/prompts.txt  ◄──sync──  Server (UUID-A)
         │
         ▼
Bartleby reads prompt and responds
         │
         ▼
bartleby/output.txt  ──sync──►  Server (UUID-B)
                                    │
                                    │ (same UUID)
                                    ▼
text-to-telegram/input.txt  ◄──sync──  Server (UUID-B)
         │
         ▼
text-to-telegram sends to Telegram
```

## Running Sandboxed Processes

Each process runs with its subdirectory as the working directory:

```bash
# text-to-telegram runs in its sandbox
cd /path/to/workspace/text-to-telegram
python text_to_telegram.py

# bartleby runs in its sandbox
cd /path/to/workspace/bartleby
python bartleby.py
```

Processes only see their own subdirectory but share content via linked files.

## Process Configuration with Orchestrator

The orchestrator discovers `processes.json` files recursively. Where you place a process
definition determines what gets synced to its sandbox:

- **Root processes.json**: Sandbox gets the full workspace tree
- **Subdirectory processes.json**: Sandbox gets only that subdirectory's content

Choose based on what the process expects:

### Pattern 1: Process expects its own subdirectory (text-to-telegram)

text-to-telegram reads/writes files relative to its own directory (content.txt, input.txt).
Define it in its subdirectory's processes.json:

**workspace/text-to-telegram/processes.json:**
```json
{
  "processes": {
    "text-to-telegram": {
      "sandbox-exec": "/path/to/python -m text_to_telegram"
    }
  }
}
```

Sandbox structure: `content.txt`, `input.txt` at root (what text-to-telegram expects).

### Pattern 2: Process expects workspace tree (bartleby)

bartleby looks for `bartleby/prompts.txt` relative to cwd, expecting the workspace structure.
Define it in the ROOT processes.json:

**workspace/processes.json:**
```json
{
  "processes": {
    "bartleby": {
      "sandbox-exec": "/path/to/python /path/to/bartleby.py"
    }
  }
}
```

Sandbox structure: `bartleby/prompts.txt`, `bartleby/output.txt` (what bartleby expects).

### Common Mistake

Putting bartleby in `workspace/bartleby/processes.json` syncs only the bartleby node,
giving a sandbox with `prompts.txt` at root instead of in `bartleby/` subdirectory.

## Related Issues

- CP-2pr: Sync refactoring (completed)
- CP-g0c: Schema corruption fix (completed)
- CP-8vr: Sync race condition with shared UUIDs (completed)
- CP-jrc: Preserve local schema during sync (completed)
