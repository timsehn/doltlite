#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() {
  local name="$1" sql="$2" expected="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if [ "$result" = "$expected" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"; fi
}
run_test_match() {
  local name="$1" sql="$2" pattern="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if echo "$result" | grep -qE "$pattern"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"; fi
}

echo "=== Doltlite Branch Tests ==="
echo ""
DB=/tmp/test_branch_$$.db; rm -f "$DB"

# Setup
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# active_branch defaults to main
run_test "default_branch" "SELECT active_branch();" "main" "$DB"

# Create branch
run_test "create_branch" "SELECT dolt_branch('feature');" "0" "$DB"

# List branches
run_test "list_branches" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test "main_is_current" "SELECT is_current FROM dolt_branches WHERE name='main';" "1" "$DB"
run_test "feature_not_current" "SELECT is_current FROM dolt_branches WHERE name='feature';" "0" "$DB"

# Checkout feature
run_test "checkout_feature" "SELECT dolt_checkout('feature');" "0" "$DB"
run_test "active_is_feature" "SELECT active_branch();" "feature" "$DB"

# Changes on feature
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','on feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "data_on_feature" "SELECT count(*) FROM t;" "2" "$DB"

# Checkout back to main
run_test "checkout_main" "SELECT dolt_checkout('main');" "0" "$DB"
run_test "active_is_main" "SELECT active_branch();" "main" "$DB"
run_test "main_has_one_row" "SELECT count(*) FROM t;" "1" "$DB"

# Feature still has 2
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "feature_still_has_two" "SELECT count(*) FROM t;" "2" "$DB"

# Duplicate branch errors
run_test_match "dup_branch_error" "SELECT dolt_branch('feature');" "already exists" "$DB"

# Delete branch (must not be current)
run_test_match "delete_current_error" "SELECT dolt_branch('-d','feature');" "cannot delete the current" "$DB"
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "delete_feature" "SELECT dolt_branch('-d','feature');" "0" "$DB"
run_test "one_branch_left" "SELECT count(*) FROM dolt_branches;" "1" "$DB"

# Checkout nonexistent errors
run_test_match "checkout_nonexistent" "SELECT dolt_checkout('nope');" "not found" "$DB"

# Branch before first commit errors
DB2=/tmp/test_branch2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(x);" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test_match "branch_no_commits" "SELECT dolt_branch('foo');" "no commits" "$DB2"

# Dirty checkout errors
DB3=/tmp/test_branch3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('b2');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test_match "dirty_checkout" "SELECT dolt_checkout('b2');" "uncommitted" "$DB3"

rm -f "$DB" "$DB2" "$DB3"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
