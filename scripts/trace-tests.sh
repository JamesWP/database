#!/usr/bin/env bash
# Attach bpftrace USDT probes to a cargo test run.
#
# Usage:
#   sudo is NOT needed to run this script — it calls sudo internally for bpftrace.
#
#   ./scripts/trace-tests.sh                   # trace all tests
#   ./scripts/trace-tests.sh test_sql_insert   # trace a single test
#
# Output: perf-test-stats.txt

set -euo pipefail

# 1. Build all test binaries without running them; capture all paths.
echo "==> Building test binaries..."
mapfile -t TEST_BINS < <(
    cargo test --no-run --message-format=json 2>/dev/null \
    | grep -o '"executable":"[^"]*"' \
    | sed 's/"executable":"//;s/"//'
)

if [[ ${#TEST_BINS[@]} -eq 0 ]]; then
    echo "Error: could not locate any test binaries." >&2
    exit 1
fi

echo "==> Found ${#TEST_BINS[@]} test binary/binaries:"
for b in "${TEST_BINS[@]}"; do echo "    $b"; done

# 2. Generate a temporary bpftrace script.
#    Start with the first binary substituted into the template, then append
#    extra usdt probe stanzas for each additional binary so all binaries are
#    traced into the same counters.
TMPSCRIPT=$(mktemp /tmp/bpftrace-test-XXXXXX.bt)
trap 'rm -f "$TMPSCRIPT"' EXIT

FIRST="${TEST_BINS[0]}"
sed "s|./target/release/database|$FIRST|g" scripts/trace-sakila.bt > "$TMPSCRIPT"

for BIN in "${TEST_BINS[@]:1}"; do
    # Append one probe stanza per usdt line for this binary.
    grep '^usdt:' scripts/trace-sakila.bt \
        | sed "s|./target/release/database|$BIN|g" >> "$TMPSCRIPT"
done

# 3. Start bpftrace in the background.
echo "==> Starting bpftrace (output -> perf-test-stats.txt)..."
sudo bpftrace "$TMPSCRIPT" > perf-test-stats.txt 2>&1 &
BPFTRACE_PID=$!

# Give bpftrace time to attach before the test binaries start.
sleep 1

# 4. Run the tests, forwarding any extra args (e.g. a test filter).
echo "==> Running: cargo test $*"
cargo test "$@" || true

# Let bpftrace flush its final maps.
sleep 1

sudo kill "$BPFTRACE_PID" 2>/dev/null || true

echo "==> Done. Stats written to perf-test-stats.txt"
