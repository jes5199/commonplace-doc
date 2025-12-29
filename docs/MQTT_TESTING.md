# Testing the MQTT Flow

This guide walks through testing the complete reactive document flow:
1. Send a command via `commonplace-cmd`
2. Python counter process receives command and updates document
3. Document changes are persisted in the store
4. `commonplace-sync` syncs changes to local files

## Prerequisites

### 1. Install Mosquitto (MQTT Broker)

```bash
# Ubuntu/Debian
sudo apt install mosquitto mosquitto-clients

# macOS
brew install mosquitto

# Start the broker
mosquitto
```

### 2. Build Commonplace Binaries

```bash
cargo build --release
```

### 3. Install Python Dependencies

```bash
cd examples/python-client
uv sync
```

## Architecture

```
┌─────────────────┐     MQTT      ┌──────────────────┐
│ commonplace-cmd │──────────────▶│   Mosquitto      │
│ (send command)  │               │   (broker)       │
└─────────────────┘               └────────┬─────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
          ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
          │ Python Counter  │    │ commonplace-    │    │ commonplace-    │
          │ (file process)  │    │ store           │    │ sync            │
          │                 │    │ (persistence)   │    │ (file sync)     │
          └────────┬────────┘    └────────┬────────┘    └────────┬────────┘
                   │                      │                      │
                   │    publishes edit    │                      │
                   └─────────────────────▶│                      │
                                          │    SSE updates       │
                                          └─────────────────────▶│
                                                                 │
                                                          writes to disk
                                                                 │
                                                                 ▼
                                                    ┌─────────────────────┐
                                                    │  ~/sync-test/       │
                                                    │  examples/          │
                                                    │    counter.json     │
                                                    └─────────────────────┘
```

## Step-by-Step Test

### Terminal 1: Start Mosquitto

```bash
mosquitto -v
```

You should see: `mosquitto version X.X.X starting`

### Terminal 2: Start the Document Store

```bash
./target/release/commonplace-store \
  --database ./test-data.redb \
  --mqtt-broker mqtt://localhost:1883 \
  --fs-root test-fs
```

### Terminal 3: Start the Sync Client

Create a test directory and start syncing:

```bash
mkdir -p ~/sync-test
./target/release/commonplace-sync \
  --directory ~/sync-test \
  --server http://localhost:3000 \
  --fs-root test-fs
```

### Terminal 4: Start the Python Counter

```bash
cd examples/python-client
uv run python counter_example.py --path examples/counter.json
```

You should see:
```
============================================================
Counter Example - External Process as First-Class Citizen
============================================================
...
[examples/counter.json] Starting file process...
[examples/counter.json] Connected to MQTT broker
[examples/counter.json] File process ready. Listening for commands...
```

### Terminal 5: Send Commands

```bash
# Increment by 1
./target/release/commonplace-cmd examples/counter.json increment

# Increment by 5
./target/release/commonplace-cmd examples/counter.json increment --payload '{"amount": 5}'

# Check the value
./target/release/commonplace-cmd examples/counter.json get

# Reset
./target/release/commonplace-cmd examples/counter.json reset
```

### Verify the Results

Check the synced file:
```bash
cat ~/sync-test/examples/counter.json
```

You should see something like:
```json
{"counter": 6}
```

## Watching Events

To see events broadcast by the counter:

```bash
mosquitto_sub -t "examples/counter.json/events/#" -v
```

Then send a command and watch the events appear.

## Troubleshooting

### "Connection refused" on MQTT

Make sure Mosquitto is running:
```bash
pgrep mosquitto || mosquitto -d
```

### Python counter doesn't receive commands

Check that the path includes the `.json` extension:
```bash
# Correct
commonplace-cmd examples/counter.json increment

# Wrong - missing extension
commonplace-cmd examples/counter increment
```

### Sync client doesn't see changes

1. Verify the store is running with the same `--fs-root`
2. Check that the HTTP server is accessible: `curl http://localhost:3000/health`
3. Look for SSE connection in sync client logs

## Message Flow Details

### Command Message Format

Topic: `examples/counter.json/commands/increment`
```json
{
  "payload": {"amount": 5},
  "source": "commonplace-cmd"
}
```

### Edit Message Format

Topic: `examples/counter.json/edits`
```json
{
  "update": "base64-encoded-yjs-update",
  "parents": ["previous-commit-cid"],
  "author": "file-process-uuid",
  "message": "Increment by 5",
  "timestamp": 1704067200000
}
```

### Event Message Format

Topic: `examples/counter.json/events/value-changed`
```json
{
  "payload": {"old": 1, "new": 6, "delta": 5},
  "source": "file-process-uuid"
}
```
