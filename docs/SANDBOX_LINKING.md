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

### 1. Create the workspace fs-root

```bash
curl -X POST http://localhost:3000/docs -H "Content-Type: application/json" \
  -d '{"id": "workspace", "content_type": "application/json"}'
```

### 2. Set up the directory structure

```bash
mkdir -p /path/to/workspace/text-to-telegram
mkdir -p /path/to/workspace/bartleby
```

### 3. Start sync for the whole workspace

```bash
commonplace-sync --directory /path/to/workspace \
  --node workspace --server http://localhost:3000
```

### 4. Create links between sandbox files

```bash
cd /path/to/workspace
commonplace-link text-to-telegram/content.txt bartleby/prompts.txt
commonplace-link bartleby/output.txt text-to-telegram/input.txt
```

### 5. How linking works

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

## Related Issues

- CP-2pr: Sync refactoring (completed)
- CP-g0c: Schema corruption fix (completed)
- CP-8vr: Sync race condition with shared UUIDs (completed)
