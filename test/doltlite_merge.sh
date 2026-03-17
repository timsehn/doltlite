#!/bin/bash
# Tests for doltliteMergeCatalogs (three-way catalog merge).
#
# Since doltliteMergeCatalogs is a C function (not yet exposed as SQL),
# we test it indirectly by verifying the build compiles and the existing
# branch/commit infrastructure works correctly. The function will be wired
# up to dolt_merge() by the next bead (do-tig).
#
# For now, verify the prerequisite operations that the merge function depends on:
# - Creating branches with divergent data
# - Commits produce distinct catalog hashes
# - Branch switching preserves catalog isolation

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Merge Infrastructure Tests ==="
echo ""

# Setup: create a db with a common ancestor commit, then diverge on two branches
DB=/tmp/test_merge_$$.db; rm -f "$DB"

# Create initial table and commit (ancestor)
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'ancestor'); SELECT dolt_commit('-A','-m','ancestor commit');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create feature branch
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Modify on main (ours)
echo "INSERT INTO t VALUES(2,'main-only'); SELECT dolt_commit('-A','-m','main change');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Switch to feature and modify (theirs)
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'feature-only'); SELECT dolt_commit('-A','-m','feature change');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify branches diverged
run_test "feature_has_3_not_2" "SELECT count(*) FROM t WHERE id=3;" "1" "$DB"
run_test "feature_no_main_row" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "main_has_2_not_3" "SELECT count(*) FROM t WHERE id=2;" "1" "$DB"
run_test "main_no_feature_row" "SELECT count(*) FROM t WHERE id=3;" "0" "$DB"

# Verify commit history has diverged
run_test "main_two_commits" "SELECT count(*) FROM dolt_log;" "2" "$DB"

echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "feature_two_commits" "SELECT count(*) FROM dolt_log;" "2" "$DB"

# Test: add a new table on feature only
echo "CREATE TABLE t2(x INTEGER); INSERT INTO t2 VALUES(42); SELECT dolt_commit('-A','-m','add t2 on feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "feature_has_t2" "SELECT x FROM t2;" "42" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "main_no_t2" "SELECT x FROM t2;" "no such table" "$DB"

rm -f "$DB"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
