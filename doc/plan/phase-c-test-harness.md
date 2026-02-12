# Phase C — Test Harness Enhancements

Phase C enhances the SQL test runner with better error testing, developer productivity features, and test management capabilities.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 14 | 7.3 | Error case testing support | — |
| 15 | 7.3 | Single test execution | — |
| 16 | 7.3 | Test update mode | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

## 14. Error Case Testing Support (Track 7.3)

### What Changes

Currently, sql_runner panics when SQL execution fails. Add support for testing expected errors via special syntax in `.expected` files.

### Key Files

- `tests/sql_runner.rs` — modify error handling to check against expected errors

### Implementation Approach

1. **Expected file syntax**: Use `ERROR: <pattern>` to indicate an expected error
   ```
   Table 'users' created
   1
   ERROR: Table 'users' already exists
   ERROR: TableNotFound: nonexistent
   ```

2. **Error matching**: When execute() returns Err, check if next expected line starts with `ERROR:`
   - If yes, verify error message contains the pattern (case-insensitive substring match)
   - If no, panic with current behavior

3. **Error format**: Convert ExecuteError to string and match against pattern

### Tests

- `error_cases.sql` / `.expected` — the deferred test from Phase B
  - CREATE duplicate table → "ERROR: already exists"
  - SELECT from nonexistent table → "ERROR: TableNotFound"
  - INSERT with wrong column count → "ERROR: ColumnCountMismatch"
  - Malformed SQL → "ERROR: Parse error"

### Benefits

- Enables testing error handling paths
- Documents expected error messages
- Catches error message regressions

---

## 15. Single Test Execution (Track 7.3)

### What Changes

Add ability to run a single SQL test file instead of all tests, speeding up development iteration.

### Implementation Approach

1. **Environment variable**: Check `SQL_TEST_FILE` environment variable
   ```bash
   SQL_TEST_FILE=where_clauses cargo test test_sql_scripts
   ```

2. **Filter logic**: In `test_sql_scripts()`, filter test_files to only include matching name
   - Match on filename stem (without .sql extension)
   - If env var not set, run all tests (current behavior)

### Tests

- Manually verify with existing tests:
  - `SQL_TEST_FILE=basic_crud cargo test test_sql_scripts` runs only basic_crud
  - `cargo test test_sql_scripts` still runs all tests

### Benefits

- Faster iteration when developing new SQL tests
- Easier debugging of specific test failures
- Reduces noise when working on single test

---

## 16. Test Update Mode (Track 7.3)

### What Changes

Add `--update` mode to automatically update `.expected` files from actual output, similar to Jest's `--updateSnapshot`.

### Implementation Approach

1. **Environment variable**: Check `UPDATE_EXPECTED=1` environment variable

2. **Update logic**: When mismatch detected and UPDATE_EXPECTED=1:
   - Write actual_output to .expected file
   - Print "Updated: <filename>.expected"
   - Continue to next test (don't panic)

3. **Safety**: Print summary at end:
   ```
   Updated 3 .expected files:
   - where_clauses.expected
   - expressions.expected
   - multi_table.expected

   Review changes with: git diff tests/sql/
   ```

### Tests

- Create a test with intentionally wrong .expected file
- Run with UPDATE_EXPECTED=1
- Verify .expected file gets updated
- Verify git diff shows the change

### Benefits

- Eliminates manual .expected file creation/updates
- Makes test creation workflow much faster
- Reduces transcription errors in expected output

---

## Verification

For each item, before considering it done:
- [ ] Tests written first (TDD where applicable)
- [ ] All tests pass: `cargo test`
- [ ] Manual verification of new feature
- [ ] Code formatted: `cargo fmt`
- [ ] No compiler warnings: `cargo build 2>&1 | grep -i warning`
- [ ] Documentation updated (this plan, CLAUDE.md if workflow changes)
