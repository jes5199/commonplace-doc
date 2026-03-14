# Testing Strategy: MQTT Retained Message Flood Fix (CP-4t5a)

## Problem Statement

On startup, the sync client received 86,480+ stale MQTT messages, overwhelming the 1024-slot broadcast channel. Sync became non-functional — CYAN-SYNC timed out, new files never synced, and the process consumed 93% CPU retrying failed resyncs.

## Root Cause Analysis

Before writing any fix, I mapped the full message flow:

1. **Exploration phase**: Used an Explore agent to trace the path from MQTT broker → rumqttc event loop → broadcast channel → receivers (root_task, subdir_task, request client). This revealed the single broadcast channel (capacity 1024) fanning out to all receivers.

2. **Retained message audit**: Searched all `publish_retained` call sites. Found three sources:
   - `src/services/document.rs` — server published every edit as retained
   - `src/sync/transport/missing_parent.rs` — rebroadcasts were retained
   - `crates/commonplace-crdt/src/` — schema commits, merges, and general publishes all used retained via the `MqttPublisher` trait

3. **Key insight**: The sync client bootstraps state via CYAN-SYNC, not from retained edit messages. Retained edits were completely redundant — they existed for a "new subscriber gets latest state" pattern that CYAN-SYNC already handles better.

## Fix Strategy (Incremental)

### Phase 1: Core fix (commit d64742b)

**Changes:**
- Added `retain: bool` field to `IncomingMessage` (captured from `publish.retain`)
- Changed `publish_retained` → `publish` in `document.rs` and `missing_parent.rs`
- Added retained message skipping in `root_task.rs` and `subdir_task.rs`
- Added retained message clearing on startup (publish empty retained payloads)

**Testing:**
- `cargo clippy` — clean
- `cargo test --lib` — 367 passed
- Started orchestrator, checked sync logs for "lagged by" warnings
- Created a test file in `workspace/`, verified it synced to server via UUID

**Result:** Lag dropped from 86,480 to 5,755 messages. File sync worked.

### Phase 2: Complete elimination (commit 5018ef5)

**Why phase 2 was needed:** Testing phase 1 revealed that `crdt_new_file.rs` still published retained schema commits via the `MqttPublisher` trait, accounting for the remaining 5,755 lag.

**Changes:**
- Renamed `MqttPublisher::publish_retained` → `publish` across the trait, impl, and all call sites
- Increased broadcast channel capacity from 1024 to 65536
- Added retained-clearing to `subdir_task.rs` (was only in `root_task.rs`)
- Fixed CP-awto: orphaned subdirs now warn instead of crashing sync

**Testing:**
- `cargo clippy` — clean
- `cargo test --lib` — 367 passed
- Started orchestrator, checked sync logs

**Result:** 0 lag messages on startup.

## Testing Methodology

### Unit tests (automated)

```bash
cargo test --lib    # 367 unit tests
cargo clippy        # lint check
```

These verify compilation and basic correctness but can't test the MQTT integration.

### Integration tests (require infrastructure)

```bash
cargo test          # includes integration tests that need MQTT broker
```

Two integration tests (`crdt_peer_sync_tests`) require a running MQTT broker and orchestrator. These were pre-existing failures unrelated to this fix.

### Exploratory testing (manual, iterative)

This was the most valuable testing approach for this fix:

1. **Start the orchestrator**: `cargo run --release --bin commonplace-orchestrator`
2. **Monitor sync logs**: `grep "lagged\|retained\|Cleared\|ERROR" /tmp/commonplace-logs/sync.log`
3. **Create test files**: `echo "test" > workspace/test-file.txt`
4. **Verify sync**: `curl http://localhost:5199/docs/{uuid}` or check sync logs for file detection
5. **Check presence**: `cat workspace/sync-client.exe | python3 -m json.tool`
6. **Kill and restart**: Verify clean startup on subsequent runs

### Key metrics tracked

| Metric | Before | After Phase 1 | After Phase 2 |
|--------|--------|---------------|---------------|
| Broadcast channel lag | 86,480 messages | 5,755 messages | 0 messages |
| CYAN-SYNC timeouts on startup | Many | Few | None from lag |
| File sync (create → server) | Never completes | ~3 seconds | ~3 seconds |
| Broadcast channel capacity | 1,024 | 1,024 | 65,536 |
| Retained message sources | 3 code paths | 1 code path | 0 code paths |

### Issues discovered during testing

1. **Old sync process lingering** (PID from previous session at 93% CPU) — had to `kill -9` before new sync could function. The orchestrator doesn't detect/kill orphaned sync processes from previous runs.

2. **CP-awto: Orphaned subdirectory crash** — `workspace/test-reload/` had a `.commonplace.json` but wasn't in the parent schema. Sync crashed with "Directory test-reload not found in parent schema". Fixed by changing the fatal `?` to a `warn!` and continue.

3. **Bartleby divergence loop** — After the MQTT flood was fixed, a separate issue emerged: bartleby files are stuck in a divergence-resync loop where CYAN-SYNC responses time out repeatedly, consuming 90%+ CPU. This is not caused by the MQTT flood but was masked by it. Filed as a separate concern.

### Lessons learned

- **Incremental testing reveals hidden issues**: Phase 1 testing revealed the `crdt_new_file.rs` retained publishes that weren't found in initial analysis.
- **Kill stale processes**: Always check for orphaned processes from previous runs before testing.
- **Broadcast channel capacity matters**: 1024 slots is too small when subscribing to 100+ topics that may each have a retained message. 65536 provides headroom for burst scenarios.
- **Retained messages are persistent**: Unlike normal MQTT messages, retained messages survive broker restarts and must be explicitly cleared (publish empty payload with retain=true). Simply stopping retained publishes doesn't clear existing ones.
