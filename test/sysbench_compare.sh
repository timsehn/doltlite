#!/bin/bash
#
# Sysbench-style OLTP benchmark: doltlite vs stock SQLite
#
# Mimics sysbench workloads: bulk insert, point select, range select,
# update index, update non-index, delete/insert, read-only, read-write.
#
# Output format: markdown table (for CI PR comments)
# Set BENCH_FORMAT=terminal for human-readable terminal output.
#
set -e

DOLTLITE=${DOLTLITE:-./doltlite}
SQLITE3=${SQLITE3:-./sqlite3}
ROWS=${BENCH_ROWS:-10000}
SEED=42

DB_DL=/tmp/bench_doltlite_$$.db
DB_SQ=/tmp/bench_sqlite3_$$.db

cleanup() { rm -f "$DB_DL" "$DB_SQ"; }
trap cleanup EXIT

# Timing helper: returns milliseconds
time_ms() {
  local start=$(python3 -c "import time; print(int(time.time()*1000))")
  eval "$@" > /dev/null 2>&1
  local end=$(python3 -c "import time; print(int(time.time()*1000))")
  echo $((end - start))
}

# Output helpers
if [ "${BENCH_FORMAT}" = "terminal" ]; then
  header() { printf "%-40s %10s %10s %10s\n" "Test" "SQLite" "Doltlite" "Ratio"; printf "%-40s %10s %10s %10s\n" "----" "------" "--------" "-----"; }
  row() { printf "%-40s %8dms %8dms %10s\n" "$1" "$2" "$3" "$4"; }
  footer() { echo ""; echo "Rows: $ROWS | Ratio = doltlite/sqlite (1.00x = same speed)"; }
else
  header() {
    echo "## Sysbench-Style Benchmark: doltlite vs stock SQLite"
    echo ""
    echo "| Test | SQLite (ms) | Doltlite (ms) | Ratio |"
    echo "|------|-------------|---------------|-------|"
  }
  row() { echo "| $1 | $2 | $3 | $4 |"; }
  footer() {
    echo ""
    echo "_${ROWS} rows per table. Ratio = doltlite/sqlite (1.00x = parity)._"
  }
fi

bench() {
  local name="$1" sql_file="$2"
  local t_sq t_dl ratio
  t_sq=$(time_ms "cat '$sql_file' | $SQLITE3 '$DB_SQ'")
  t_dl=$(time_ms "cat '$sql_file' | $DOLTLITE '$DB_DL'")
  ratio=$(python3 -c "print(f'{$t_dl/$t_sq:.2f}x' if $t_sq>0 else 'N/A')")
  row "$name" "$t_sq" "$t_dl" "$ratio"
}

# ============================================================
# Generate all SQL workloads upfront (deterministic via seed)
# ============================================================
TMPDIR=$(mktemp -d)

python3 -c "
import random, string, os
random.seed($SEED)
R=$ROWS
d='$TMPDIR'

def rstr(n):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

# Schema + bulk insert
with open(f'{d}/prepare.sql','w') as f:
    f.write('''CREATE TABLE sbtest1(
  id INTEGER PRIMARY KEY,
  k INTEGER NOT NULL DEFAULT 0,
  c TEXT NOT NULL DEFAULT '',
  pad TEXT NOT NULL DEFAULT ''
);
CREATE INDEX k_idx ON sbtest1(k);
BEGIN;
''')
    for i in range(1, R+1):
        f.write(f\"INSERT INTO sbtest1 VALUES({i},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# Point selects
with open(f'{d}/point_select.sql','w') as f:
    for _ in range(1000):
        f.write(f'SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n')

# Range selects
with open(f'{d}/range_select.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')

# Sum range
with open(f'{d}/sum_range.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')

# Order range
with open(f'{d}/order_range.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')

# Distinct range
with open(f'{d}/distinct_range.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')

# Index scan
with open(f'{d}/index_scan.sql','w') as f:
    for _ in range(100):
        f.write(f'SELECT id, c FROM sbtest1 WHERE k={random.randint(1,R)};\n')

# Update index
with open(f'{d}/update_index.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(1000):
        f.write(f'UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n')
    f.write('COMMIT;\n')

# Update non-index
with open(f'{d}/update_nonindex.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(1000):
        f.write(f\"UPDATE sbtest1 SET c='{rstr(120)}' WHERE id={random.randint(1,R)};\n\")
    f.write('COMMIT;\n')

# Delete + insert
with open(f'{d}/delete_insert.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(500):
        id=random.randint(1,R)
        f.write(f'DELETE FROM sbtest1 WHERE id={id};\n')
        f.write(f\"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# Read-only mixed
with open(f'{d}/read_only.sql','w') as f:
    for _ in range(100):
        for _ in range(10):
            f.write(f'SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')

# Read-write mixed
with open(f'{d}/read_write.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(100):
        for _ in range(10):
            f.write(f'SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        f.write(f'UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n')
        f.write(f\"UPDATE sbtest1 SET c='{rstr(120)}' WHERE id={random.randint(1,R)};\n\")
        id=random.randint(1,R)
        f.write(f'DELETE FROM sbtest1 WHERE id={id};\n')
        f.write(f\"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# Table scan
with open(f'{d}/table_scan.sql','w') as f:
    f.write(\"SELECT COUNT(*) FROM sbtest1 WHERE c LIKE '%abc%';\n\")
"

# ============================================================
# Run benchmarks
# ============================================================
header

# Prepare both databases
t_sq=$(time_ms "cat '$TMPDIR/prepare.sql' | $SQLITE3 '$DB_SQ'")
t_dl=$(time_ms "cat '$TMPDIR/prepare.sql' | $DOLTLITE '$DB_DL'")
ratio=$(python3 -c "print(f'{$t_dl/$t_sq:.2f}x' if $t_sq>0 else 'N/A')")
row "oltp_bulk_insert ($ROWS rows)" "$t_sq" "$t_dl" "$ratio"

bench "oltp_point_select (1000 queries)" "$TMPDIR/point_select.sql"
bench "oltp_range_select (100 x 100 rows)" "$TMPDIR/range_select.sql"
bench "oltp_sum_range (100 queries)" "$TMPDIR/sum_range.sql"
bench "oltp_order_range (100 queries)" "$TMPDIR/order_range.sql"
bench "oltp_distinct_range (100 queries)" "$TMPDIR/distinct_range.sql"
bench "oltp_index_scan (100 queries)" "$TMPDIR/index_scan.sql"
bench "oltp_update_index (1000 updates)" "$TMPDIR/update_index.sql"
bench "oltp_update_non_index (1000 updates)" "$TMPDIR/update_nonindex.sql"
bench "oltp_delete_insert (500 pairs)" "$TMPDIR/delete_insert.sql"
bench "oltp_read_only (100 txns)" "$TMPDIR/read_only.sql"
bench "oltp_read_write (100 txns)" "$TMPDIR/read_write.sql"
bench "table_scan (full scan LIKE)" "$TMPDIR/table_scan.sql"

footer

rm -rf "$TMPDIR"
