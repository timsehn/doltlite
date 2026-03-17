#!/bin/bash
#
# Tests for DoltLite branding: engine function, version, prompt.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== DoltLite Branding Tests ==="
echo ""

# doltlite_engine() returns "prolly"
run_test "engine_func" "SELECT doltlite_engine();" "prolly" ":memory:"

# Old misspelling does NOT work
run_test_match "engine_old_gone" "SELECT doltite_engine();" "no such function" ":memory:"

# -version flag says DoltLite
VER=$($DOLTLITE -version 2>&1)
if echo "$VER" | grep -q "DoltLite"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: version_flag\n  expected: DoltLite\n  got:      $VER"; fi

# -version includes SQLite version
if echo "$VER" | grep -q "SQLite"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: version_sqlite\n  expected: SQLite\n  got:      $VER"; fi

# Interactive banner says DoltLite (use script to fake a tty)
BANNER=$(script -q /dev/null $DOLTLITE :memory: <<'EOF' 2>&1 | head -3
.quit
EOF
)
if echo "$BANNER" | grep -q "DoltLite"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: banner\n  expected: DoltLite\n  got:      $BANNER"; fi

# Prompt says doltlite> (may not appear in all script captures, so check with a query)
PROMPT=$(script -q /dev/null $DOLTLITE :memory: <<'PEOF' 2>&1
SELECT 1;
.quit
PEOF
)
if echo "$PROMPT" | grep -q "doltlite>"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: prompt\n  expected: doltlite>\n  got:      $PROMPT"; fi

# Banner does NOT say "SQLite version" (the old format)
if echo "$BANNER" | grep -q "SQLite version"; then FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: no_sqlite_version\n  should not contain: SQLite version\n  got:      $BANNER"; else PASS=$((PASS+1)); fi

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
