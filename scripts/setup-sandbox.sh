#!/bin/bash
# Setup sandbox workspace with linked files for bartleby and text-to-telegram
#
# This script:
# 1. Creates the sandbox directory structure
# 2. Runs initial sync to generate .commonplace.json
# 3. Creates links between sandbox files
# 4. Pushes the linked schema to the server
#
# Prerequisites:
# - Server must be running with: commonplace-server --fs-root sandbox-workspace --database /path/to/data.redb

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SANDBOX_DIR="${SANDBOX_DIR:-$HOME/sandbox-workspace}"
SERVER_URL="${SERVER_URL:-http://localhost:3000}"
FS_ROOT="${FS_ROOT:-sandbox-workspace}"

echo "=== Sandbox Setup ==="
echo "Sandbox directory: $SANDBOX_DIR"
echo "Server URL: $SERVER_URL"
echo "FS root: $FS_ROOT"
echo ""

# Check server is running
if ! curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
    echo "ERROR: Server not responding at $SERVER_URL"
    echo "Start server with: commonplace-server --fs-root $FS_ROOT --database /path/to/data.redb"
    exit 1
fi

# Create directory structure
echo "Creating directory structure..."
mkdir -p "$SANDBOX_DIR/bartleby"
mkdir -p "$SANDBOX_DIR/text-to-telegram"

# Create initial files if they don't exist
touch "$SANDBOX_DIR/bartleby/prompts.txt"
touch "$SANDBOX_DIR/bartleby/output.txt"
touch "$SANDBOX_DIR/bartleby/system_prompt.txt"
touch "$SANDBOX_DIR/text-to-telegram/content.txt"
touch "$SANDBOX_DIR/text-to-telegram/input.txt"

# Copy system prompt if available and local one is empty
if [ -f "$PROJECT_DIR/workspace/bartleby/system_prompt.txt" ] && [ ! -s "$SANDBOX_DIR/bartleby/system_prompt.txt" ]; then
    echo "Copying system prompt..."
    cp "$PROJECT_DIR/workspace/bartleby/system_prompt.txt" "$SANDBOX_DIR/bartleby/system_prompt.txt"
fi

# Check server fs-root is empty (avoid CRDT merge corruption)
SERVER_CONTENT=$(curl -s "$SERVER_URL/docs/$FS_ROOT")
if [ "$SERVER_CONTENT" != "{}" ]; then
    echo "WARNING: Server fs-root is not empty. Schema may get corrupted."
    echo "Current content: $SERVER_CONTENT"
    echo ""
    echo "To reset, restart server with fresh database or delete the fs-root document."
    read -p "Continue anyway? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create initial schema locally using commonplace-link --init
echo "Creating local schema..."
cd "$SANDBOX_DIR"

# Initialize schema from directory structure
"$PROJECT_DIR/target/release/commonplace-link" --init . 2>/dev/null || true

# If no schema exists yet, we need to create it manually
if [ ! -f "$SANDBOX_DIR/.commonplace.json" ]; then
    cat > "$SANDBOX_DIR/.commonplace.json" << 'SCHEMA'
{
  "version": 1,
  "root": {
    "type": "dir",
    "entries": {
      "bartleby": {
        "type": "dir",
        "entries": {
          "prompts.txt": {"type": "doc", "node_id": null, "content_type": "text/plain"},
          "output.txt": {"type": "doc", "node_id": null, "content_type": "text/plain"},
          "system_prompt.txt": {"type": "doc", "node_id": null, "content_type": "text/plain"}
        },
        "node_id": null,
        "content_type": null
      },
      "text-to-telegram": {
        "type": "dir",
        "entries": {
          "content.txt": {"type": "doc", "node_id": null, "content_type": "text/plain"},
          "input.txt": {"type": "doc", "node_id": null, "content_type": "text/plain"}
        },
        "node_id": null,
        "content_type": null
      }
    },
    "node_id": null,
    "content_type": null
  }
}
SCHEMA
fi

echo "Schema created."

# Create links BEFORE running sync
echo "Creating file links..."

# Link: text-to-telegram/content.txt <-> bartleby/prompts.txt
# (telegram sends messages -> bartleby reads as prompts)
"$PROJECT_DIR/target/release/commonplace-link" \
    text-to-telegram/content.txt bartleby/prompts.txt

# Link: bartleby/output.txt <-> text-to-telegram/input.txt
# (bartleby responds -> telegram sends to user)
"$PROJECT_DIR/target/release/commonplace-link" \
    bartleby/output.txt text-to-telegram/input.txt

echo "Links created."

# Push linked schema to server (fresh push to empty server)
echo "Pushing linked schema to server..."
"$PROJECT_DIR/target/release/commonplace-sync" \
    --server "$SERVER_URL" \
    --node "$FS_ROOT" \
    --directory "$SANDBOX_DIR" \
    --initial-sync local &
SYNC_PID=$!
sleep 5
kill $SYNC_PID 2>/dev/null || true
wait $SYNC_PID 2>/dev/null || true

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Linked files:"
echo "  text-to-telegram/content.txt <-> bartleby/prompts.txt"
echo "  bartleby/output.txt <-> text-to-telegram/input.txt"
echo ""
echo "To start sync:"
echo "  $PROJECT_DIR/target/release/commonplace-sync \\"
echo "    --server $SERVER_URL \\"
echo "    --node $FS_ROOT \\"
echo "    --directory $SANDBOX_DIR"
echo ""
echo "To run bartleby with sync:"
echo "  $PROJECT_DIR/scripts/run-bartleby.sh"
