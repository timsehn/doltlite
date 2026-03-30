#!/bin/bash
#
# run_sqllogictest.sh — Run sqllogictest .test files against doltlite and sqlite3.
#
# Usage: bash run_sqllogictest.sh <doltlite> <sqlite3> <test-dir>
#
# Parses the sqllogictest format (statement ok/error, query with expected results)
# and runs each test file against both engines, comparing pass rates.
# Exit code 0 always (informational — does not fail CI).

set -euo pipefail

DOLTLITE="$1"
SQLITE3="$2"
TESTDIR="$3"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Evidence tests (small, fast) + select tests (large, comprehensive)
TEST_FILES=()
for f in "$TESTDIR"/evidence/*.test; do
  [ -f "$f" ] && TEST_FILES+=("$f")
done
for f in "$TESTDIR"/select1.test "$TESTDIR"/select2.test "$TESTDIR"/select3.test; do
  [ -f "$f" ] && TEST_FILES+=("$f")
done

if [ ${#TEST_FILES[@]} -eq 0 ]; then
  echo "ERROR: No test files found in $TESTDIR"
  echo "Expected: $TESTDIR/evidence/*.test and/or $TESTDIR/select*.test"
  exit 0
fi

# Run a single SQL statement against an engine.
run_sql() {
  local engine="$1" db="$2" sql="$3"
  timeout 5 "$engine" "$db" "$sql" 2>/dev/null || true
}

# Parse and run a single .test file against one engine.
# Outputs: pass fail skip total
run_test_file() {
  local engine="$1" engine_name="$2" testfile="$3"
  local db="$TMPDIR/${engine_name}_$RANDOM.db"
  local pass=0 fail=0 skip=0 total=0
  local mode="" sql="" expected_status="" sort_mode=""
  local in_results=0 results=""

  rm -f "$db"

  while IFS= read -r line || [ -n "$line" ]; do
    # Blank line = end of record
    if [ -z "$line" ]; then
      if [ "$mode" = "statement" ] && [ -n "$sql" ]; then
        total=$((total + 1))
        local output
        output=$(run_sql "$engine" "$db" "$sql" 2>&1)
        if [ "$expected_status" = "ok" ]; then
          if echo "$output" | grep -qi "error\|Parse error"; then
            fail=$((fail + 1))
          else
            pass=$((pass + 1))
          fi
        elif [ "$expected_status" = "error" ]; then
          if echo "$output" | grep -qi "error\|Parse error"; then
            pass=$((pass + 1))
          else
            fail=$((fail + 1))
          fi
        fi
        mode="" sql="" expected_status=""

      elif [ "$mode" = "query" ] && [ -n "$sql" ]; then
        total=$((total + 1))
        local output
        output=$(run_sql "$engine" "$db" "$sql" 2>&1)

        if [ -n "$results" ]; then
          local expected_clean actual_clean
          expected_clean=$(echo "$results" | sed '/^$/d')
          actual_clean=$(echo "$output" | sed '/^$/d')

          if [ "$sort_mode" = "rowsort" ]; then
            expected_clean=$(echo "$expected_clean" | sort)
            actual_clean=$(echo "$actual_clean" | sort)
          elif [ "$sort_mode" = "valuesort" ]; then
            expected_clean=$(echo "$expected_clean" | tr '|' '\n' | sort)
            actual_clean=$(echo "$actual_clean" | tr '|' '\n' | sort)
          fi

          # Hash-based results: "N values hashing to HASH"
          if echo "$expected_clean" | grep -q "values hashing to"; then
            local expected_hash
            expected_hash=$(echo "$expected_clean" | grep -oE '[0-9a-f]{32}')
            # Compute MD5 of actual results (one value per line, matching sqllogictest format)
            local actual_hash
            actual_hash=$(echo "$actual_clean" | tr '|' '\n' | sed '/^$/d' | md5sum | awk '{print $1}')
            if [ "$actual_hash" = "$expected_hash" ]; then
              pass=$((pass + 1))
            else
              fail=$((fail + 1))
            fi
          elif [ "$expected_clean" = "$actual_clean" ]; then
            pass=$((pass + 1))
          else
            fail=$((fail + 1))
          fi
        else
          # No expected results — just check it didn't error
          if echo "$output" | grep -qi "error\|Parse error"; then
            fail=$((fail + 1))
          else
            pass=$((pass + 1))
          fi
        fi
        mode="" sql="" results="" in_results=0 sort_mode=""
      fi
      continue
    fi

    # Comment lines
    [[ "$line" == \#* ]] && continue

    # skipif/onlyif
    if [[ "$line" == skipif\ sqlite ]]; then
      mode="skip"; continue
    fi
    if [[ "$line" == onlyif\ * ]]; then
      local only="${line#onlyif }"
      if [ "$only" != "sqlite" ]; then
        mode="skip"
      fi
      continue
    fi
    if [[ "$line" == skipif\ * ]]; then
      # skipif for non-sqlite engines — ignore (we are sqlite-compatible)
      continue
    fi

    [ "$line" = "halt" ] && break
    [[ "$line" == hash-threshold\ * ]] && continue

    # Record start
    if [[ "$line" == statement\ * ]]; then
      [ "$mode" = "skip" ] && { mode=""; skip=$((skip + 1)); continue; }
      mode="statement"; expected_status="${line#statement }"; sql=""; continue
    fi
    if [[ "$line" == query\ * ]]; then
      [ "$mode" = "skip" ] && { mode=""; skip=$((skip + 1)); continue; }
      mode="query"
      local parts=($line)
      sort_mode="${parts[2]:-nosort}"
      sql="" results="" in_results=0; continue
    fi

    [ "$mode" = "skip" ] && continue

    # Result separator
    if [ "$line" = "----" ]; then in_results=1; continue; fi

    # Accumulate SQL or results
    if [ "$in_results" = "1" ]; then
      results="${results:+$results
}$line"
    elif [ "$mode" = "statement" ] || [ "$mode" = "query" ]; then
      sql="${sql:+$sql
}$line"
    fi
  done < "$testfile"

  echo "$pass $fail $skip $total"
  rm -f "$db"
}

# Main
echo "============================================"
echo "SQL Logic Test: doltlite vs sqlite3"
echo "============================================"
echo ""

total_dl_pass=0 total_dl_fail=0
total_sq_pass=0 total_sq_fail=0
n_files=0

for f in "${TEST_FILES[@]}"; do
  fname=$(basename "$f")
  n_files=$((n_files + 1))

  sq_result=$(run_test_file "$SQLITE3" "sqlite" "$f")
  sq_pass=$(echo "$sq_result" | awk '{print $1}')
  sq_fail=$(echo "$sq_result" | awk '{print $2}')
  sq_total=$(echo "$sq_result" | awk '{print $4}')

  dl_result=$(run_test_file "$DOLTLITE" "doltlite" "$f")
  dl_pass=$(echo "$dl_result" | awk '{print $1}')
  dl_fail=$(echo "$dl_result" | awk '{print $2}')
  dl_total=$(echo "$dl_result" | awk '{print $4}')

  printf "%-45s  sqlite3: %5d/%5d  doltlite: %5d/%5d\n" \
    "$fname" "$sq_pass" "$sq_total" "$dl_pass" "$dl_total"

  total_sq_pass=$((total_sq_pass + sq_pass))
  total_sq_fail=$((total_sq_fail + sq_fail))
  total_dl_pass=$((total_dl_pass + dl_pass))
  total_dl_fail=$((total_dl_fail + dl_fail))
done

echo ""
echo "============================================"
echo "Totals ($n_files files)"
echo "  sqlite3:  $total_sq_pass passed, $total_sq_fail failed"
echo "  doltlite: $total_dl_pass passed, $total_dl_fail failed"
if [ "$total_sq_pass" -gt 0 ]; then
  pct=$((total_dl_pass * 100 / total_sq_pass))
  echo "  doltlite pass rate: ${pct}% of sqlite3"
fi
echo "============================================"

# Informational — don't fail CI
exit 0
