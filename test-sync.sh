#!/bin/bash
# Test script for directory sync

# Kill existing processes
pkill -f commonplace-server 2>/dev/null
pkill -f commonplace-sync 2>/dev/null
sleep 1

# Clean up
rm -f /tmp/commonplace-test.redb
rm -rf /tmp/commonplace-test
mkdir -p /tmp/commonplace-test/notes
echo "Test content from local" > /tmp/commonplace-test/notes/test.txt

# Build
echo "=== Building ==="
cargo build --quiet

# Start server
echo "=== Starting server ==="
RUST_LOG=info cargo run --bin commonplace-server -- --fs-root test-fs --database /tmp/commonplace-test.redb > /tmp/server.log 2>&1 &
sleep 2
curl -s http://localhost:3000/health || { echo "Server failed to start"; exit 1; }

# Start sync
echo "=== Starting sync ==="
RUST_LOG=info cargo run --bin commonplace-sync -- --directory /tmp/commonplace-test/notes --node test-fs --initial-sync=local > /tmp/sync.log 2>&1 &
sleep 5

# Check results
echo ""
echo "=== Sync log (key lines) ==="
grep -E "(Push|Node|Successfully|WARN|ERROR|content empty)" /tmp/sync.log

echo ""
echo "=== Server content ==="
curl -s "http://localhost:3000/nodes/test-fs%3Atest.txt/head"

echo ""
echo ""
echo "=== To view full logs ==="
echo "cat /tmp/sync.log"
echo "cat /tmp/server.log"
