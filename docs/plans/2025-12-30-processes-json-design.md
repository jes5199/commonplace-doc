# Process Discovery via .processes.json

**Date:** 2025-12-30

## Overview

The conductor discovers `.processes.json` files in the document tree and automatically launches/manages declared processes. This enables user-defined processes to attach to file paths without central configuration.

## File Format

**File location:** `.processes.json` in any directory within the document tree

**Format:**
```json
{
  "processes": {
    "counter": {
      "command": "uv run python counter_example.py",
      "owns": "counter.json",
      "cwd": "/home/jes/commonplace/examples/python-client"
    }
  }
}
```

**Required fields:**
- `command` - String or array of command and arguments
- `owns` - Relative path (file or directory) this process claims, within the same directory
- `cwd` - Absolute path on host filesystem where process runs

**Environment variables set by conductor:**
- `COMMONPLACE_PATH` - Full document path (e.g., `examples/counter.json`)
- `COMMONPLACE_MQTT` - Broker address (e.g., `localhost:1883`)

## Discovery and Lifecycle

**Discovery:**
- Conductor watches the document tree for `.processes.json` files
- On startup, scans existing tree and launches declared processes
- When a `.processes.json` is created/modified, starts new processes (or restarts changed ones)
- When a `.processes.json` is deleted, stops its processes

**Process lifecycle:**
- Conductor starts the process with the configured `cwd` and environment variables
- On process exit, restarts with exponential backoff (default: 500ms initial, 10s max)
- Backoff resets after process runs successfully for 30 seconds
- On conductor shutdown, sends SIGTERM to all managed processes

**Conflict handling:**
- If two `.processes.json` files declare processes that own the same path, the conductor logs a warning and only starts the first one discovered
- Modifying either file triggers re-evaluation

## Example

`examples/.processes.json` in the document tree:
```json
{
  "processes": {
    "counter": {
      "command": "uv run python counter_example.py",
      "owns": "counter.json",
      "cwd": "/home/jes/commonplace/examples/python-client"
    }
  }
}
```

When conductor sees this file, it:
1. Launches `uv run python counter_example.py`
2. Sets `COMMONPLACE_PATH=examples/counter.json`
3. Sets `COMMONPLACE_MQTT=localhost:1883`
4. Runs from `/home/jes/commonplace/examples/python-client`

## Relationship to Orchestrator

- Orchestrator (`commonplace.json`) manages core infrastructure: store, http, conductor
- Conductor manages user-defined processes via `.processes.json` discovery
- They complement each other - orchestrator for infra, conductor for document-attached processes
