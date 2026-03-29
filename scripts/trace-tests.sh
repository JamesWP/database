#!/usr/bin/env bash
# Attach bpftrace USDT probes to a cargo test run or the sakila load.
#
# Usage:
#   sudo is NOT needed to run this script — it calls sudo internally for bpftrace.
#
#   ./scripts/trace-tests.sh                        # trace all tests
#   ./scripts/trace-tests.sh test_sql_insert        # trace a single test (filter)
#   ./scripts/trace-tests.sh --test sql_runner      # trace only the sql_runner suite
#   ./scripts/trace-tests.sh --sakila               # trace the sakila load (release binary)
#
# The --test SUITE flag is passed to both `cargo test --no-run` (so only that
# suite's binary is built and attached to bpftrace) and to `cargo test` when
# running.  Any remaining arguments after --test SUITE are forwarded to
# `cargo test` as additional filters.
#
# Output:
#   --sakila mode  → perf-stats.txt
#   test mode      → perf-test-stats.txt

set -euo pipefail

# Parse optional --sakila flag.
SAKILA_MODE=0
if [[ "${1:-}" == "--sakila" ]]; then
    SAKILA_MODE=1
    shift
fi

if [[ $SAKILA_MODE -eq 1 ]]; then
    # ── Sakila mode ───────────────────────────────────────────────────────────
    RELEASE_BIN="target/release/database"

    if [[ ! -x "$RELEASE_BIN" ]]; then
        echo "==> Building release binary..."
        cargo build --release
    fi

    TMPSCRIPT=$(mktemp /tmp/bpftrace-sakila-XXXXXX.bt)
    trap 'rm -f "$TMPSCRIPT"' EXIT

    sed "s|./target/release/database|$RELEASE_BIN|g" scripts/trace-sakila.bt > "$TMPSCRIPT"

    echo "==> Starting bpftrace (output -> perf-stats.txt)..."
    rm -f sakila.db perf-stats.txt
    sudo bpftrace "$TMPSCRIPT" > perf-stats.txt 2>&1 &
    BPFTRACE_PID=$!

    sleep 1

    echo "==> Running: make test-sakila (5s timeout)"
    timeout 5 make test-sakila || true

    sleep 1

    sudo kill -SIGINT "$BPFTRACE_PID" 2>/dev/null || true
    wait $BPFTRACE_PID

    echo "==> Done. Stats written to perf-stats.txt"
    exit 0
fi

# ── Test mode ─────────────────────────────────────────────────────────────────

# Parse optional --test SUITE argument.
CARGO_TEST_SUITE=()   # passed to `cargo test --no-run` and `cargo test`
if [[ "${1:-}" == "--test" && -n "${2:-}" ]]; then
    CARGO_TEST_SUITE=(--test "$2")
    shift 2
fi

# 1. Build the relevant test binaries without running them; capture all paths.
echo "==> Building test binaries..."
mapfile -t TEST_BINS < <(
    cargo test "${CARGO_TEST_SUITE[@]}" --no-run --message-format=json 2>/dev/null \
    | grep -o '"executable":"[^"]*deps[^"]*"' \
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
echo "==> Running: cargo test ${CARGO_TEST_SUITE[*]} $*"
cargo test "${CARGO_TEST_SUITE[@]}" "$@" || true

# Let bpftrace flush its final maps.
sleep 1

sudo kill -SIGINT "$BPFTRACE_PID" 2>/dev/null || true

# let bpftrace print the summary
wait $BPFTRACE_PID

echo "==> Done. Stats written to perf-test-stats.txt"
