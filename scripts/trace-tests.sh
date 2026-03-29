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

SAKILA_MODE=0
if [[ "${1:-}" == "--sakila" ]]; then
    SAKILA_MODE=1
    shift
fi

if [[ $SAKILA_MODE -eq 1 ]]; then
    echo "==> Building release binary..."
    cargo build --release
    BINS=("target/release/database")
    OUTPUT="perf-stats.txt"
else
    # Parse optional --test SUITE argument.
    CARGO_TEST_SUITE=()
    if [[ "${1:-}" == "--test" && -n "${2:-}" ]]; then
        CARGO_TEST_SUITE=(--test "$2")
        shift 2
    fi

    echo "==> Building test binaries..."
    mapfile -t BINS < <(
        cargo test "${CARGO_TEST_SUITE[@]}" --no-run --message-format=json 2>/dev/null \
        | grep -o '"executable":"[^"]*deps[^"]*"' \
        | sed 's/"executable":"//;s/"//'
    )

    if [[ ${#BINS[@]} -eq 0 ]]; then
        echo "Error: could not locate any test binaries." >&2
        exit 1
    fi

    echo "==> Found ${#BINS[@]} test binary/binaries:"
    for b in "${BINS[@]}"; do echo "    $b"; done

    OUTPUT="perf-test-stats.txt"
fi

# Generate a temporary bpftrace script with binary path(s) substituted in.
TMPSCRIPT=$(mktemp /tmp/bpftrace-XXXXXX.bt)
trap 'rm -f "$TMPSCRIPT"' EXIT

sed "s|./target/release/database|${BINS[0]}|g" scripts/trace-sakila.bt > "$TMPSCRIPT"

for BIN in "${BINS[@]:1}"; do
    grep '^usdt:' scripts/trace-sakila.bt \
        | sed "s|./target/release/database|$BIN|g" >> "$TMPSCRIPT"
done

echo "==> Starting bpftrace (output -> $OUTPUT)..."
rm -f "$OUTPUT"
sudo bpftrace "$TMPSCRIPT" > "$OUTPUT" 2>&1 &
BPFTRACE_PID=$!

sleep 1

if [[ $SAKILA_MODE -eq 1 ]]; then
    echo "==> Running: make test-sakila (5s timeout)"
    timeout 5 make test-sakila || true
else
    echo "==> Running: cargo test ${CARGO_TEST_SUITE[*]:-} $*"
    cargo test "${CARGO_TEST_SUITE[@]}" "$@" || true
fi

sleep 1

sudo kill -SIGINT "$BPFTRACE_PID" 2>/dev/null || true
wait $BPFTRACE_PID

echo "==> Done. Stats written to $OUTPUT"
