# Testing Observer Presence Files

Manual test guide for verifying that multiple sync agents create
visible presence files and can see each other.

## Prerequisites

```bash
# Build
cargo build --release

# Start MQTT broker (if not already running)
mosquitto &

# Start the server with a fresh database
cargo run --release --bin commonplace-server -- --database /tmp/presence-test.redb --fs-root workspace
```

## Setup: Two Sync Agents in the Same Directory

Open three terminals. Terminal 1 runs the server (above).

### Terminal 2: First sync agent

```bash
mkdir -p /tmp/presence-a
cargo run --release --bin commonplace-sync -- \
  --server http://localhost:5199 \
  --node workspace \
  --directory /tmp/presence-a \
  --workspace test \
  --name sync-alpha
```

### Terminal 3: Second sync agent

```bash
mkdir -p /tmp/presence-b
cargo run --release --bin commonplace-sync -- \
  --server http://localhost:5199 \
  --node workspace \
  --directory /tmp/presence-b \
  --workspace test \
  --name sync-beta
```

## What to Observe

### 1. Each agent creates its own presence file

```bash
# In terminal 2's directory:
ls /tmp/presence-a/*.exe
# Expected: sync-alpha.exe

# In terminal 3's directory:
ls /tmp/presence-b/*.exe
# Expected: sync-beta.exe
```

### 2. Presence files contain status and PID

```bash
cat /tmp/presence-a/sync-alpha.exe
# Expected: JSON with status "active", pid, started_at, last_heartbeat
```

### 3. Each agent can see the other's presence file (via sync)

Since both agents sync to the same server node (`workspace`), files
created by one agent should appear in the other's directory:

```bash
# Agent alpha should see agent beta's presence file:
ls /tmp/presence-a/sync-beta.exe

# Agent beta should see agent alpha's presence file:
ls /tmp/presence-b/sync-alpha.exe

# Both should show status "active"
cat /tmp/presence-a/sync-beta.exe
cat /tmp/presence-b/sync-alpha.exe
```

### 4. Shutdown sets status to Stopped

Stop terminal 3 (Ctrl+C). Then check:

```bash
# Agent alpha's copy of beta's presence file should show "stopped"
cat /tmp/presence-a/sync-beta.exe
# Expected: "status": "stopped"
```

The file persists — it's not deleted on shutdown.

### 5. Restart reclaims the same name

Restart the second agent (same command as terminal 3). Check:

```bash
cat /tmp/presence-b/sync-beta.exe
# Expected: "status": "active" again, same filename (no hash suffix)
```

## Collision Test: Same Name, Same Directory

This tests the hash suffix disambiguation. Both agents must point
at the same local directory (unusual but possible):

### Terminal 2:

```bash
mkdir -p /tmp/presence-collision
cargo run --release --bin commonplace-sync -- \
  --server http://localhost:5199 \
  --node workspace \
  --directory /tmp/presence-collision \
  --workspace test \
  --name sync
```

### Terminal 3 (while terminal 2 is still running):

```bash
cargo run --release --bin commonplace-sync -- \
  --server http://localhost:5199 \
  --node workspace \
  --directory /tmp/presence-collision \
  --workspace test \
  --name sync
```

### What to observe:

```bash
ls /tmp/presence-collision/sync*.exe
# Expected: sync.exe (first agent) and sync-{hash}.exe (second agent)

# Both should show status "active" with different PIDs
cat /tmp/presence-collision/sync.exe
cat /tmp/presence-collision/sync-*.exe
```

## Glob-Based Discovery

The extension convention enables glob-based presence queries:

```bash
# Who's here?
ls /tmp/presence-a/*.*

# What processes are running?
ls /tmp/presence-a/*.exe

# Are any users connected? (none expected in this test)
ls /tmp/presence-a/*.usr 2>/dev/null
```

## Cleanup

```bash
rm -rf /tmp/presence-a /tmp/presence-b /tmp/presence-collision /tmp/presence-test.redb
```
