#!/bin/bash
# Run bartleby with commonplace sync
#
# This script starts sync and runs bartleby in the synced sandbox.
# When bartleby exits, sync also stops.
#
# Prerequisites:
# - Server running: commonplace-server --fs-root sandbox-workspace --database /path/to/data.redb
# - Sandbox set up: scripts/setup-sandbox.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SANDBOX_DIR="${SANDBOX_DIR:-$HOME/sandbox-workspace}"
SERVER_URL="${SERVER_URL:-http://localhost:3000}"
FS_ROOT="${FS_ROOT:-sandbox-workspace}"

# Check server is running
if ! curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
    echo "ERROR: Server not responding at $SERVER_URL"
    echo "Start server with: commonplace-server --fs-root $FS_ROOT --database /path/to/data.redb"
    exit 1
fi

# Check sandbox is set up
if [ ! -f "$SANDBOX_DIR/.commonplace.json" ]; then
    echo "ERROR: Sandbox not set up. Run scripts/setup-sandbox.sh first."
    exit 1
fi

echo "Starting bartleby with sync..."
echo "Sandbox: $SANDBOX_DIR"
echo "Server: $SERVER_URL"
echo ""

# Run sync with bartleby as exec command
# Sync continues while bartleby runs, stops when bartleby exits
exec "$SCRIPT_DIR/target/release/commonplace-sync" \
    --server "$SERVER_URL" \
    --node "$FS_ROOT" \
    --directory "$SANDBOX_DIR" \
    --exec "~/.local/bin/uv run --project /home/jes/bartleby python /home/jes/bartleby/bartleby.py"
