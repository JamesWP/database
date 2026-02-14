#!/bin/bash
# Helper script for running SQL integration tests with environment variables
#
# Usage:
#   ./test-sql.sh [test_file]           # Run specific test file
#   ./test-sql.sh [test_file] --update  # Update expected output for test file
#   ./test-sql.sh --update              # Update all expected outputs
#   ./test-sql.sh                       # Run all SQL tests

set -e

TEST_FILE=""
UPDATE_MODE=""

# Parse arguments
for arg in "$@"; do
    case $arg in
        --update|-u)
            UPDATE_MODE="1"
            ;;
        *)
            TEST_FILE="$arg"
            ;;
    esac
done

# Build the command
CMD="cargo test test_sql_scripts"

# Set environment variables
if [ -n "$TEST_FILE" ]; then
    export SQL_TEST_FILE="$TEST_FILE"
fi

if [ -n "$UPDATE_MODE" ]; then
    export UPDATE_EXPECTED="1"
fi

# Run the test
echo "Running SQL tests..."
if [ -n "$TEST_FILE" ]; then
    echo "  Test file: $TEST_FILE"
fi
if [ -n "$UPDATE_MODE" ]; then
    echo "  Mode: UPDATE (will update .expected files)"
fi
echo ""

$CMD
