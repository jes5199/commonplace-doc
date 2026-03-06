# CLI QA Plan

Comprehensive QA for commonplace CLI tools. Requires a running system (server + MQTT broker + sync agent). The orchestrator handles this — just run `cargo run --bin commonplace-orchestrator`.

## Prerequisites

```bash
cargo build
# Start the system (in another terminal or background)
cargo run --bin commonplace-orchestrator &
# Wait for startup (~3 seconds)
sleep 3
```

Verify the system is up:
```bash
commonplace ps          # Should show running processes
commonplace status      # Should show "connected"
```

---

## 1. Help & Discovery

Every subcommand should have working `--help`. Verify each one prints usage without crashing.

```bash
commonplace --help
commonplace help
commonplace help sync
commonplace init --help
commonplace branch --help
commonplace checkout --help
commonplace worktree --help
commonplace status --help
commonplace log --help
commonplace show --help
commonplace ps --help
commonplace signal --help
commonplace link --help
commonplace uuid --help
commonplace cmd --help
```

**Expected**: Each prints usage text and exits 0. No panics, no "unknown command."

**Also test**: Unknown command gives a helpful error:
```bash
commonplace nonexistent   # Should suggest similar commands
commonplace brunch        # Should suggest "branch"
```

---

## 2. Init — Repo Creation

### 2.1 Create a new repo

```bash
commonplace init test-repo
```

**Expected**:
- Prints root UUID, repo doc UUID, main branch UUID
- `workspace/test-repo/` directory appears (after sync agent picks it up)
- `workspace/test-repo/.commonplace.json` exists with `main` entry
- `workspace/test-repo/main/` directory appears

### 2.2 Duplicate repo name rejected

```bash
commonplace init test-repo
```

**Expected**: Error "Repo 'test-repo' already exists in the root schema." Exit 1.

### 2.3 Invalid repo names rejected

```bash
commonplace init ""
commonplace init "."
commonplace init ".."
commonplace init "has/slash"
commonplace init "_underscore"
```

**Expected**: Each prints an error and exits 1.

---

## 3. Branch — List, Create, Activate, Deactivate

### 3.1 List branches

```bash
cd workspace/test-repo
commonplace branch
```

**Expected**: Shows `  main` (at minimum).

### 3.2 Create a branch

```bash
commonplace branch create dev --from main
```

**Expected**:
- Prints "Creating branch 'dev' from 'main'"
- Prints "Branch 'dev' created (root: ...)"
- Prints "Branch 'dev' added to repo"
- After sync reconciles: `workspace/test-repo/dev/` directory appears

### 3.3 List shows new branch

```bash
commonplace branch
```

**Expected**: Shows both `  main` and `  dev`.

### 3.4 Duplicate branch rejected

```bash
commonplace branch create dev --from main
```

**Expected**: Error "Branch 'dev' already exists." Exit 1.

### 3.5 Invalid source branch

```bash
commonplace branch create feature --from nonexistent
```

**Expected**: Error "Source branch 'nonexistent' not found." Exit 1.

### 3.6 Deactivate a branch

```bash
commonplace branch deactivate dev
```

**Expected**:
- Prints "Branch 'dev' deactivated (sync: false)."
- After sync reconciles: sync agent stops watching `dev/`
- Local files in `workspace/test-repo/dev/` are NOT deleted

### 3.7 List shows inactive

```bash
commonplace branch
```

**Expected**: Shows `  main` and `  dev (inactive)`.

### 3.8 Activate a branch

```bash
commonplace branch activate dev
```

**Expected**:
- Prints "Branch 'dev' activated (sync: true)."
- After sync reconciles: sync agent starts watching `dev/` again

### 3.9 List shows active again

```bash
commonplace branch
```

**Expected**: Shows `  main` and `  dev` (no "inactive" marker).

---

## 4. Status

### 4.1 Basic status

```bash
cd workspace
commonplace status
```

**Expected**: Prints workspace path, broker URL, server connection status, entry count.

### 4.2 JSON output

```bash
commonplace status --json
```

**Expected**: Valid JSON with `workspace`, `broker`, `server_connected`, `entry_count` fields.

### 4.3 Not a workspace

```bash
cd /tmp
commonplace status
```

**Expected**: Error "Not a commonplace workspace" Exit 1.

---

## 5. File Operations — UUID, Log, Show, Link

### 5.1 Create a test file and wait for sync

```bash
echo "hello world" > workspace/test-repo/main/test.txt
sleep 2  # Wait for sync to pick it up
```

### 5.2 Resolve UUID

```bash
commonplace uuid test-repo/main/test.txt
```

**Expected**: Prints a UUID. Exit 0.

### 5.3 View commit log

```bash
commonplace log test-repo/main/test.txt
```

**Expected**: Shows at least one commit (the initial sync).

### 5.4 Show content at HEAD

```bash
commonplace show test-repo/main/test.txt
```

**Expected**: Shows "hello world" (or the synced content).

### 5.5 Edit and verify new commit

```bash
echo "updated content" > workspace/test-repo/main/test.txt
sleep 2
commonplace log test-repo/main/test.txt
```

**Expected**: Shows at least two commits now.

### 5.6 Link two files

```bash
echo "shared" > workspace/test-repo/main/source.txt
sleep 2
commonplace link test-repo/main/source.txt test-repo/main/linked.txt
sleep 2
cat workspace/test-repo/main/linked.txt
```

**Expected**: `linked.txt` contains "shared". Both files share the same UUID.

---

## 6. Process Management — PS, Signal

### 6.1 List processes

```bash
commonplace ps
```

**Expected**: Table with process name, PID, state, cwd.

### 6.2 JSON output

```bash
commonplace ps --json
```

**Expected**: Valid JSON array of process objects.

### 6.3 Signal a process

```bash
commonplace signal -n sync
```

**Expected**: Sends SIGTERM to sync process. Orchestrator restarts it.

---

## 7. Checkout (Flat-Branch Model)

Note: With the repo/branch layout, checkout is typically unnecessary since all branches are materialized as subdirectories. This tests the flat-branch model where a single sync agent switches roots.

### 7.1 Checkout requires MQTT

```bash
commonplace checkout dev --sync-name sync
```

**Expected**: Sends re-root command via MQTT. Prints success message.

---

## 8. Worktree (Flat-Branch Model)

Similarly vestigial with repo/branch layout, but should still work.

### 8.1 List worktrees

```bash
commonplace worktree list
```

**Expected**: "No worktrees configured." or a list.

### 8.2 Add a worktree

```bash
commonplace worktree add dev /tmp/test-worktree
```

**Expected**: Creates directory, registers sync agent in `__processes.json`.

### 8.3 Remove a worktree

```bash
commonplace worktree remove /tmp/test-worktree
```

**Expected**: Removes sync agent from `__processes.json`.

---

## 9. Edge Cases & Error Handling

### 9.1 Server not running

Stop the server, then:

```bash
commonplace init should-fail
commonplace status
commonplace branch create fail --from main
```

**Expected**: Each gives a clear connection error, not a panic.

### 9.2 Wrong directory

```bash
cd /tmp
commonplace branch
commonplace branch create x
```

**Expected**: "Not a commonplace repo" error.

### 9.3 Sparse sync — file persistence

```bash
# Create file in dev branch
echo "dev data" > workspace/test-repo/dev/devfile.txt
sleep 2
# Deactivate branch
cd workspace/test-repo
commonplace branch deactivate dev
sleep 2
# File should still exist locally
cat workspace/test-repo/dev/devfile.txt
```

**Expected**: File still contains "dev data" — deactivation doesn't delete files.

### 9.4 Sparse sync — re-activation picks up changes

```bash
# While deactivated, edit the file
echo "edited while inactive" > workspace/test-repo/dev/devfile.txt
# Reactivate
commonplace branch activate dev
sleep 3
# Check if change synced
cat workspace/test-repo/dev/devfile.txt
```

**Expected**: Content syncs correctly after reactivation. The local edit should propagate to server (or server state overwrites, depending on sync direction — document which happens).

---

## 10. Cleanup

```bash
# Remove test repo (manually for now — branch delete not yet implemented)
# Stop orchestrator
kill %1
```

---

## Reporting

For each section, report:
- **PASS**: All expected behaviors confirmed
- **FAIL**: What happened vs. what was expected
- **SKIP**: Why it couldn't be tested (e.g., server not available)
- **NOTE**: Any unexpected behavior worth investigating, even if not a failure

File bugs as beads for any failures:
```bash
bd create --title="CLI QA: <description>" --type=bug --priority=2
```
