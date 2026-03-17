# Doltlite

A SQLite fork with Git-like version control built into the storage engine.

Doltlite replaces SQLite's B-tree storage with a content-addressed
[prolly tree](https://docs.dolthub.com/architecture/storage-engine/prolly-tree),
enabling branch, merge, diff, and time-travel on any SQL database. Everything
above SQLite's `btree.h` interface (VDBE, query planner, parser) is untouched.
Everything below it — the pager, WAL, and on-disk format — is replaced with a
prolly tree engine backed by an append-only chunk store.

15,500 lines of new C code. 87,000+ SQLite test cases pass. 798 Dolt feature tests.

## Building

```
cd build
../configure
make
./doltlite :memory:
```

Verify the prolly tree engine is active:

```sql
SELECT doltlite_engine();
-- prolly
```

Build stock SQLite for comparison:

```
make DOLTLITE_PROLLY=0 sqlite3
```

## Version Control Operations

All version control is done through SQL — no external tools needed.

### Commit

```sql
-- Stage and commit in one step
SELECT dolt_commit('-A', '-m', 'Initial schema');

-- Or stage specific tables first
SELECT dolt_add('users');
SELECT dolt_commit('-m', 'Add users table');

-- Commit with author
SELECT dolt_commit('-m', 'Fix data', '--author', 'Alice <alice@example.com>');
```

### Log and Status

```sql
SELECT * FROM dolt_log;
-- commit_hash | committer | email | date | message

SELECT * FROM dolt_status;
-- table_name | staged | status
```

### Branch

Each connection tracks its own branch. Two connections can be on different
branches simultaneously, seeing different data.

```sql
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
SELECT active_branch();
SELECT * FROM dolt_branches;
SELECT dolt_branch('-d', 'feature');
```

### Merge

Three-way merge at the row level. Non-conflicting changes to different rows
auto-merge. Conflicts are detected and stored for resolution.

```sql
SELECT dolt_merge('feature');
-- Returns commit hash (clean) or "Merge completed with N conflict(s)"
```

### Conflicts

```sql
-- Summary
SELECT * FROM dolt_conflicts;

-- Individual conflict rows
SELECT * FROM dolt_conflicts_users;
-- base_rowid | base_value | our_rowid | our_value | their_rowid | their_value

-- Resolve one row at a time (keeps working value)
DELETE FROM dolt_conflicts_users WHERE base_rowid = 5;

-- Or resolve all at once
SELECT dolt_conflicts_resolve('--ours', 'users');
SELECT dolt_conflicts_resolve('--theirs', 'users');
```

### Cherry-Pick

Apply changes from a specific commit onto the current branch:

```sql
SELECT dolt_cherry_pick('abc123...');
```

### Revert

Undo a specific commit by creating a new inverse commit:

```sql
SELECT dolt_revert('abc123...');
```

### Diff

```sql
-- Working changes vs HEAD
SELECT * FROM dolt_diff('users');

-- Between two commits
SELECT * FROM dolt_diff('users', 'abc123...', 'def456...');
-- diff_type | rowid_val | from_value | to_value
```

### Audit Log (dolt_diff_&lt;table&gt;)

Every change ever committed, with commit context:

```sql
SELECT * FROM dolt_diff_users;
-- diff_type | rowid_val | from_value | to_value
--   | from_commit | to_commit | from_commit_date | to_commit_date

-- What happened to row 42?
SELECT diff_type, to_commit FROM dolt_diff_users WHERE rowid_val = 42;
```

### History (dolt_history_&lt;table&gt;)

Time-travel: every version of every row at every commit:

```sql
SELECT * FROM dolt_history_users;
-- rowid_val | value | commit_hash | committer | commit_date

-- What did this row look like 3 commits ago?
SELECT * FROM dolt_history_users
  WHERE rowid_val = 42
  ORDER BY commit_date DESC LIMIT 1 OFFSET 3;
```

### Tags

```sql
SELECT dolt_tag('v1.0');
SELECT dolt_tag('v1.0', 'abc123...');
SELECT dolt_tag('-d', 'v1.0');
SELECT * FROM dolt_tags;
```

### Reset

```sql
SELECT dolt_reset('--soft');   -- unstage, keep changes
SELECT dolt_reset('--hard');   -- discard all uncommitted changes
```

### Merge Base

```sql
SELECT dolt_merge_base('abc123...', 'def456...');
-- Returns the common ancestor commit hash
```

### Garbage Collection

```sql
SELECT dolt_gc();
-- "12 chunks removed, 45 chunks kept"
```

Stop-the-world mark-and-sweep. Walks all branches, tags, and commit history
to find reachable chunks, then rewrites the file with only live data.

## Running Tests

### Dolt Feature Tests (798 tests)

```bash
cd build
bash ../test/doltlite_commit.sh         # 24 tests
bash ../test/doltlite_staging.sh        # 26 tests
bash ../test/doltlite_diff.sh           # 13 tests
bash ../test/doltlite_reset.sh          # 14 tests
bash ../test/doltlite_branch.sh         # 17 tests
bash ../test/doltlite_merge.sh          # 32 tests
bash ../test/doltlite_tag.sh            # 13 tests
bash ../test/doltlite_conflicts.sh      # 15 tests
bash ../test/doltlite_conflict_rows.sh  # 30 tests
bash ../test/doltlite_cherry_pick.sh    # 76 tests
bash ../test/doltlite_gc.sh             # 48 tests
bash ../test/doltlite_diff_table.sh     # 36 tests
bash ../test/doltlite_history.sh        # 26 tests
bash ../test/doltlite_e2e.sh            # 44 tests
bash ../test/doltlite_edge_cases.sh     # 177 tests
bash ../test/doltlite_advanced.sh       # 140 tests
bash ../test/doltlite_feature_deep.sh   # 67 tests
```

### SQLite Test Suite (87,000+ tests)

```bash
cd build
../configure --with-tcl=$(brew --prefix tcl-tk)/lib
make testfixture OPTS="-L$(brew --prefix)/lib"
./testfixture ../test/select1.test
```

### Concurrent Branch Test

```bash
cd build
make libsqlite3.a USE_AMALGAMATION=0
cc -g -O0 -I. -I../src -DDOLTLITE_PROLLY=1 -D_HAVE_SQLITE_CONFIG_H \
  -o concurrent_branch_test ../test/concurrent_branch_test.c \
  libsqlite3.a -lz -lpthread -lm
./concurrent_branch_test
```

## Architecture

```
 SQL (unchanged)
  |
 VDBE (unchanged)
  |
 btree.h API (same 60 function signatures, new implementation)
  |
 Prolly Tree Engine (content-addressed, structural sharing)
  |
 Chunk Store (append-only, single-file, atomic commits)
```

### Prolly Tree Engine

| File | Purpose |
|------|---------|
| `prolly_hash.c/h` | xxHash32 content addressing |
| `prolly_node.c/h` | Binary node format (serialization, field access) |
| `prolly_cache.c/h` | LRU node cache |
| `prolly_cursor.c/h` | Tree cursor (seek, next, prev) |
| `prolly_mutmap.c/h` | Skip list write buffer for pending edits |
| `prolly_chunker.c/h` | Rolling hash tree builder |
| `prolly_mutate.c/h` | Merge-flush edits into tree |
| `prolly_diff.c/h` | Two-tree diff |
| `prolly_three_way_diff.c/h` | Three-way diff for merge |
| `prolly_arena.c/h` | Arena allocator |
| `prolly_btree.c` | `btree.h` implementation (~3000 lines) |
| `chunk_store.c/h` | File-backed, append-only chunk storage |
| `pager_shim.c` | Pager facade (no page-based I/O) |

### Dolt Feature Files

| File | Purpose |
|------|---------|
| `doltlite.c` | `dolt_add`, `dolt_commit`, `dolt_reset`, `dolt_merge`, `dolt_cherry_pick`, `dolt_revert` |
| `doltlite_status.c` | `dolt_status` virtual table |
| `doltlite_log.c` | `dolt_log` virtual table |
| `doltlite_diff.c` | `dolt_diff()` table-valued function |
| `doltlite_diff_table.c` | `dolt_diff_<table>` audit log virtual tables |
| `doltlite_history.c` | `dolt_history_<table>` time-travel virtual tables |
| `doltlite_branch.c` | `dolt_branch`, `dolt_checkout`, `active_branch`, `dolt_branches` |
| `doltlite_tag.c` | `dolt_tag`, `dolt_tags` |
| `doltlite_merge.c` | Three-way catalog and row-level merge |
| `doltlite_conflicts.c` | `dolt_conflicts`, `dolt_conflicts_<table>`, `dolt_conflicts_resolve` |
| `doltlite_ancestor.c` | Common ancestor search, `dolt_merge_base` |
| `doltlite_gc.c` | Stop-the-world garbage collection |
| `doltlite_commit.h/c` | Commit object serialization |
| `doltlite_ancestor.h` | Ancestor-finding API |
