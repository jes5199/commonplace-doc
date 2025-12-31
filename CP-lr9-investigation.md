# CP-lr9: Sync Race Condition Investigation

## Symptom Summary
When bartleby writes responses to output.txt, the content is sometimes
overwritten/lost. The `<RESPONSE>` marker appears but response content
disappears.

## System Components
1. **commonplace-server** - Document storage with Yjs CRDT
2. **commonplace-sync** - Bidirectional file sync (file watcher + SSE)
3. **bartleby** - Writes to output.txt via append
4. **filesystem** - Local files in workspace/

## Data Flow
```
bartleby writes → output.txt → sync detects → pushes to server
server updates → SSE event → sync receives → writes to output.txt
```

## Phase 1: Evidence Gathering

### Observation 1: Timeline of Events
- 20:03:16 - Bartleby writes prompt header + `<RESPONSE>` marker
- Response content written by bartleby
- Content disappears, file reverts to just `<RESPONSE>` marker

### Observation 2: Multiple Writers
- Bartleby appends to output.txt
- Sync client may be writing to output.txt (from server state)
- Potential race: sync overwrites bartleby's writes

### Observation 3: Schema File Issues
Sync log shows repeated warnings:
```
WARN: Failed to parse fs-root schema: missing field `version`
```
This suggests schema document is being corrupted/overwritten.

---

## Evidence to Gather

### Test 1: Trace file modification times
- [ ] Monitor output.txt mtime during bartleby write

### Test 2: Check sync log for overwrites
- [ ] Look for "Refreshed local file from HEAD" messages around response time

### Test 3: Compare local vs server content
- [ ] Capture both at same moment during/after bartleby write

### Test 4: Check if sync pulls OLD server state
- [ ] Server may have stale content that overwrites fresh local writes

---

## Hypotheses (DO NOT FIX YET)

1. **Sync pulls stale server content**: Server has old version, sync overwrites local
2. **Race between push and pull**: Bartleby writes, sync pushes, then pulls older state
3. **Yjs merge conflict**: CRDT merge loses content somehow
4. **File watcher double-fires**: Detects bartleby write, pushes, then detects again

---

## KEY FINDING - Evidence from Sync Logs

The sync log shows a repeating pattern:
```
Uploaded: 168 chars inserted, 0 deleted (cid: 6a671a56)
Refreshed local file from HEAD: 283 bytes   <-- IMMEDIATELY AFTER UPLOAD
```

**This happens EVERY time!**

### Analysis
1. Sync detects local file change (bartleby write)
2. Sync pushes diff to server ("Uploaded: X chars inserted")
3. Server broadcasts SSE event for the update
4. Sync receives SSE event (for its OWN update!)
5. Sync refreshes local file from HEAD → OVERWRITES current local content

### The Bug
**The sync client is receiving its OWN update events and re-applying them!**

This creates a race:
- Bartleby writes content A
- Sync pushes A to server
- Bartleby writes content B (append)
- Sync receives SSE for A, refreshes local file → LOSES content B
- Sync detects local file changed, pushes... but now it's a diff from the wrong base

### Evidence Pattern
The log shows this cycle repeating constantly:
- Upload → Refresh → Upload → Refresh

The "Refreshed local file from HEAD: 283 bytes" overwrites with server state,
which may not have the most recent local writes.

## Next Steps
1. Verify: Check if sync has logic to ignore its own updates
2. Look at SSE handler code for self-update detection
3. Check if there's a write_id or similar to track self-originated updates

---

## ROOT CAUSE IDENTIFIED

### Finding: `source` field exists but is UNUSED

In `src/sync/types.rs:38`:
```rust
pub struct EditEventData {
    pub source: String,      // <-- EXISTS!
    pub commit: CommitData,
}
```

In `src/sync/sse.rs:214`:
```rust
pub async fn handle_server_edit(
    ...
    _edit: &EditEventData,   // <-- UNDERSCORE = INTENTIONALLY UNUSED!
    ...
) {
```

The `_edit` parameter contains `source` which could identify where the event came from,
but the handler completely ignores it!

### The Bug Cycle

1. Bartleby writes content A to output.txt
2. File watcher detects change
3. Upload task reads content A, pushes to server
4. Server broadcasts SSE "edit" event (with source="sync-client" presumably)
5. SSE handler receives event
6. SSE handler IGNORES the source field
7. SSE handler compares local content with `last_written_content`
8. If local differs (e.g., bartleby wrote more while uploading), sets `needs_head_refresh=true`
9. Upload task completes, sees `needs_head_refresh=true`
10. Calls `refresh_from_head()` which OVERWRITES local file with server state
11. File watcher detects this write
12. Upload task pushes the refreshed content (which may be OLDER than what bartleby wrote)
13. Cycle repeats

### Why This Causes Data Loss

The critical race:
- Bartleby appends to file: "Hello\nWorld\n"
- Sync detects "Hello\n", uploads
- SSE event arrives with source="sync-client" (or similar)
- Meanwhile bartleby wrote "World\n"
- SSE handler ignores source, sees local differs from last_written
- Sets needs_head_refresh
- Upload completes, refresh_from_head() writes server state ("Hello\n") to file
- "World\n" is LOST

### Fix Approach (DO NOT IMPLEMENT YET)

1. Track our client ID (generate UUID on startup)
2. Include client ID in upload requests (as author or separate field)
3. Server includes source client ID in SSE events
4. SSE handler checks if event.source matches our client ID
5. If self-originated event, skip processing entirely

---

## Questions for Codex Review

1. Is this analysis correct?
2. What does the server put in the `source` field of SSE events?
3. Is there a simpler fix using existing mechanisms?

---

## Additional Investigation (Phase 2)

### Key Finding: Server Merge is Working Correctly

The `/replace` endpoint at `src/services/document.rs:528` correctly:
1. Computes Yjs diff from parent to new content
2. Creates merge commit if parent differs from HEAD (lines 553-583)
3. Applies Yjs update which MERGES concurrent edits (line 607)
4. Returns the new CID

The `apply_yjs_update` at `src/document.rs:136` correctly:
1. Applies Yjs update
2. Recomputes `doc.content` from merged Yjs state (line 148)

### The Puzzle: Why is refresh_from_head writing?

In `src/sync/sse.rs:112`, `refresh_from_head` only writes if:
```rust
if head.content != s.last_written_content
```

After successful upload (sync.rs:2875-2877):
```rust
s.last_written_content = content;  // content = what was uploaded
```

So for refresh to write, `head.content` must differ from what was uploaded.
This happens if:
1. **Server merged concurrent edits** - HEAD contains merged content
2. **Encoding difference** - Content differs due to trailing newlines, encoding, etc.

### Hypothesis: Encoding/Normalization Mismatch

The log shows:
```
Uploaded: 168 chars inserted, 0 deleted (cid: 6a671a56)
Refreshed local file from HEAD: 283 bytes
```

The "283 bytes" might differ from uploaded content due to:
- Trailing newline handling
- Base64 encoding/decoding
- UTF-8 normalization

### Test to Verify
Add debug logging to compare:
1. Exact bytes of `content` before upload
2. Exact bytes of `head.content` after fetch
3. Check if they differ and how

---

## CODEX REVIEW CONFIRMATION

Codex review (gpt-5.2-codex) confirms the analysis:

> **[P1] Ignore source of self-originated SSE edits** — src/sync/sse.rs:208-217
>
> The SSE handler takes an `EditEventData` parameter but ignores it (`_edit`),
> so there is no check to drop events that originated from this client.
> When the server broadcasts the SSE for our own upload while bartleby continues
> appending, `handle_server_edit` still runs and sets `needs_head_refresh`
> because local content differs. The subsequent upload task sees this flag
> and calls `refresh_from_head`, overwriting the newer local text with the
> server's previous commit, which matches the observed loss of output.txt content.
> Filtering out self-originated events via the `source` field is needed to avoid
> this self-induced refresh.

### Confirmed Root Cause

The sync client processes its OWN SSE events without filtering them out.

**Race condition sequence:**
1. Bartleby writes "Hello" to output.txt
2. Sync detects change, uploads "Hello" to server
3. Bartleby appends "World" - file is now "HelloWorld"
4. Server broadcasts SSE edit event (for "Hello" commit)
5. SSE handler sees local="HelloWorld" != last_written (old state)
6. SSE handler sets `needs_head_refresh = true` and returns
7. Upload completes, sees `needs_head_refresh`, calls `refresh_from_head`
8. `refresh_from_head` fetches HEAD (content="Hello")
9. `refresh_from_head` writes "Hello" to local file
10. "World" is LOST

### Fix Required

In `handle_server_edit` (src/sync/sse.rs:208):
1. Generate a unique client ID on sync startup
2. Include client ID in upload requests (as author or source)
3. Server includes source in SSE events
4. SSE handler checks if `edit.source` matches our client ID
5. If self-originated, skip processing entirely

Alternatively, a simpler fix might be possible if the server already puts
a client identifier in the `source` field that we can match against.

---

## SIMPLER FIX FOUND

The SSE event structure (src/sse.rs:292-305) includes:
```rust
struct EditEventData {
    source: String,      // hardcoded to "server" - NOT useful
    commit: CommitEventData {
        author: String,  // THIS IS THE KEY!
        ...
    },
}
```

The sync client uses `author: "sync-client"` for ALL uploads (src/sync/urls.rs:59, src/bin/sync.rs:1964, etc.)

**Simple Fix:**
In `handle_server_edit` (src/sync/sse.rs:208):
```rust
pub async fn handle_server_edit(
    ...
    edit: &EditEventData,  // Remove underscore!
    ...
) {
    // Skip self-originated events
    if edit.commit.author == "sync-client" {
        debug!("Skipping self-originated edit (author=sync-client)");
        return;
    }
    ...
}
```

This is a minimal fix that requires:
1. Remove underscore from `_edit` parameter
2. Add author check at the start of the function

**Caveat:** This assumes only one sync client per document. If multiple sync clients
could edit the same document, they'd all use "sync-client" as author and would ignore
each other's edits. For multi-client scenarios, a unique client ID would be needed.
