# Bartleby + Commonplace Integration

**Date:** 2025-12-31

## Goal

Run bartleby (autonomous Claude agent) with its working directory synced to commonplace, enabling:
- Prompts added via commonplace appear for bartleby to process
- Files bartleby creates are synced back to commonplace
- Real-time bidirectional sync between commonplace documents and bartleby's workspace

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   mosquitto     │◄────│ commonplace-     │────►│ redb database   │
│   (systemd)     │     │ server           │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        ▲                       │
        │                       │ HTTP/SSE
        │               ┌───────▼────────┐
        │               │ commonplace-   │
        │               │ sync           │
        │               │ --directory    │
        │               │ ./workspace    │
        │               └───────┬────────┘
        │                       │
        │                       ▼
        │               ┌─────────────────┐
        │               │ ./workspace/    │
        │               │  └─ bartleby/   │
        │               │      ├─ prompts.txt
        │               │      ├─ output.txt
        │               │      └─ system_prompt.txt
        │               └────────┬────────┘
        │                        │ symlink from ~/bartleby/working
        │               ┌────────▼────────┐
        └───────────────│ bartleby.py     │
          (future MQTT) │ (Claude agent)  │
                        └─────────────────┘
```

## Prerequisites

- mosquitto running (systemd)
- commonplace built (`cargo build --release`)
- bartleby installed at ~/bartleby/

## Setup Steps

### 1. Start commonplace-server

```bash
# From commonplace repo
cargo run --release --bin commonplace-server -- \
  --database ./bartleby-test.redb \
  --fs-root bartleby-workspace
```

This creates:
- A redb database at ./bartleby-test.redb
- An fs-root node called "bartleby-workspace"

### 2. Create workspace directory

```bash
mkdir -p ./workspace
```

### 3. Symlink bartleby's working directory

```bash
# Back up existing working dir if needed
mv ~/bartleby/working ~/bartleby/working.bak

# Symlink to commonplace workspace
ln -sf $(pwd)/workspace ~/bartleby/working
```

### 4. Initialize bartleby structure in workspace

```bash
mkdir -p ./workspace/bartleby
cp ~/bartleby/system_prompt.txt.default ./workspace/bartleby/system_prompt.txt
cp ~/bartleby/mcp_servers.json.default ./workspace/bartleby/mcp_servers.json
touch ./workspace/bartleby/prompts.txt
touch ./workspace/bartleby/output.txt
```

### 5. Start commonplace-sync

```bash
# Sync the workspace to the fs-root node
cargo run --release --bin commonplace-sync -- \
  --server http://localhost:3000 \
  --node bartleby-workspace \
  --directory ./workspace
```

### 6. Start bartleby

```bash
cd ~/bartleby
./run.sh
```

## Interaction Patterns

### Add a prompt via commonplace

```bash
# Option 1: Edit the synced file directly
echo "Hello bartleby, what time is it?" >> ./workspace/bartleby/prompts.txt

# Option 2: Use commonplace API (once path-based editing works)
curl -X POST http://localhost:3000/files/bartleby/prompts.txt/append \
  -d "Hello bartleby, what time is it?"
```

### Watch bartleby's output

```bash
tail -f ./workspace/bartleby/output.txt
```

### Graceful shutdown

```bash
touch ./workspace/bartleby/shutdown
```

## Backgrounding for Testing

To run everything in background for interactive testing:

```bash
# Terminal 1: Server
cargo run --release --bin commonplace-server -- \
  --database ./bartleby-test.redb \
  --fs-root bartleby-workspace &

# Terminal 2: Sync
cargo run --release --bin commonplace-sync -- \
  --server http://localhost:3000 \
  --node bartleby-workspace \
  --directory ./workspace &

# Terminal 3: Bartleby
cd ~/bartleby && ./run.sh &
```

Or use the Task tool with `run_in_background: true` for each.

## Blockers

~~1. **CP-9u1 (P1)**: Sync client uses derived IDs instead of UUIDs from schema.~~ **FIXED** - Sync client now recursively fetches schemas from node-backed directories to resolve UUIDs.

## Known Limitations

1. **~~Bartleby hardcodes WORKING_DIR~~** - FIXED: Now supports `BARTLEBY_WORKING_DIR` env var (commit 8abec04)
2. **Sync latency** - File changes take a moment to propagate through SSE
3. **Binary files** - Commonplace sync may not handle binary files well
4. **Conflict resolution** - If both bartleby and commonplace edit same file simultaneously, conflicts possible

## Future Enhancements

1. **MQTT integration** - Bartleby could subscribe to commonplace MQTT topics directly
2. **Directory-attached process** - Use new CP-4xd feature to have conductor manage bartleby
3. **Event-driven prompts** - Bartleby reacts to commonplace document events instead of polling prompts.txt
