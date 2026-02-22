# Phase K — Inline SQL Test Expected Output

Merge `.expected` files into their corresponding `.sql` files so that input and expected output live together, making tests easier to read and review.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 50 | 7 | New `.sql` file format with inline expected output | — |
| 51 | 7 | Migrate existing tests to new format | 50 |
| 52 | 7 | Update `update-sql-tests` tool to write inline output | 51 |
| 53 | 7 | Remove obsolete `.expected` files | 52 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Currently each SQL test consists of two files:

```
tests/sql/where_clauses.sql      ← input
tests/sql/where_clauses.expected ← expected output (separate file)
```

The separation makes code review hard — the reviewer must switch between two files to understand what each statement does and what it's supposed to produce. The goal of this phase is to merge them into a single self-contained file:

```sql
-- tests/sql/where_clauses.sql
CREATE TABLE numbers (id INTEGER, value INTEGER, name TEXT)
-- > Table 'numbers' created
INSERT INTO numbers VALUES (1, 100, 'one')
-- > 1
SELECT id, value FROM numbers WHERE value=100
-- > 1	100
-- > 3	100
```

The `update-sql-tests` tool is updated to rewrite the inline expected output in place, so the workflow for regenerating expected output is unchanged.

---

## 50. New `.sql` Format with Inline Expected Output (Track 7)

### Format Specification

Each SQL statement is followed immediately by its expected output, expressed as `-- >` comment lines. Multiple output lines are each prefixed with `-- >`. The `-- >` marker is chosen because:
- It is a valid SQL comment, so the file remains syntactically valid SQL
- `>` visually suggests "output"
- It is unambiguous — regular comments use `--` without `>`

```sql
-- Regular comments (ignored by runner, no output)
CREATE TABLE numbers (id INTEGER, value INTEGER, name TEXT)
-- > Table 'numbers' created

INSERT INTO numbers VALUES (1, 100, 'one')
-- > 1

SELECT id, value FROM numbers WHERE value = 100
-- > 1	100
-- > 3	100

-- Error assertions use the same ERROR: prefix convention
CREATE TABLE numbers (id INTEGER)
-- > ERROR: already exists
```

Rules:
- A `-- >` line immediately following a SQL statement is an expected output line.
- Multiple consecutive `-- >` lines represent multiple output rows.
- Regular `--` comments (without `>`) are ignored.
- Blank lines are ignored.
- `ERROR:` prefix in expected output retains the existing case-insensitive substring match behaviour.

### Parser Changes (`src/testing/sql_runner.rs`)

Replace the two-file `execute_sql_script` + file read with a single-file parser:

```rust
struct SqlTestFile {
    statements: Vec<SqlStatement>,
}

struct SqlStatement {
    sql: String,
    expected: Vec<String>,  // lines after "-- >"
}
```

Parsing algorithm:
1. Iterate lines.
2. Non-empty, non-comment lines → new `SqlStatement`.
3. `-- > <text>` lines immediately after a statement → push to that statement's `expected`.
4. Plain `--` comments and blank lines → skip.

The runner then executes each statement and compares its output against `statement.expected` rather than reading a separate file.

### Key Files

- `src/testing/sql_runner.rs` — parse new format, update `execute_sql_script`, `compare_output`, `update_expected_file`
- `build.rs` — no change needed (still scans for `.sql` files)
- `src/bin/update-sql-tests.rs` — no change needed (calls `run_sql_test`)

### Tests

Unit test the new parser directly:
```rust
#[test]
fn test_parse_inline_expected() {
    let input = "SELECT 1\n-- > 1\nSELECT 2\n-- > 2\n-- > extra\n";
    let parsed = parse_sql_test_file(input);
    assert_eq!(parsed[0].sql, "SELECT 1");
    assert_eq!(parsed[0].expected, vec!["1"]);
    assert_eq!(parsed[1].sql, "SELECT 2");
    assert_eq!(parsed[1].expected, vec!["2", "extra"]);
}
```

### Implementation Steps (2 commits)

#### Step 50.1 — Parser: read inline expected output

Add `parse_sql_test_file()` that returns `Vec<SqlStatement>`.

Update `execute_sql_script` and `compare_output` to use the new structure.

Keep backward compatibility: if a `.expected` file exists alongside the `.sql` file, fall back to the old behaviour (enables incremental migration in item 51).

**Commit:** Add inline expected output format to SQL test runner

#### Step 50.2 — Runner: per-statement error reporting

Update error messages to report the SQL statement alongside the line number so failures are immediately actionable without cross-referencing two files.

**Commit:** Improve per-statement error messages in SQL test runner

---

## 51. Migrate Existing Tests to New Format (Track 7)

### Approach

Write a one-off migration script (or update `update-sql-tests` with a `--migrate` flag) that:
1. Reads `<name>.sql` and `<name>.expected`
2. Re-executes the SQL to pair each statement with its output
3. Rewrites `<name>.sql` with `-- >` lines inserted after each statement

Because the runner already runs the SQL to verify, this is equivalent to running `update-sql-tests` with the new writer.

Migration is done test-by-test so each file can be reviewed in its own commit.

### Implementation Steps (2 commits)

#### Step 51.1 — Add `--migrate` flag to `update-sql-tests`

When `--migrate` is passed, rewrite the `.sql` file with inline expected output instead of writing/updating a `.expected` file.

**Commit:** Add --migrate flag to update-sql-tests

#### Step 51.2 — Migrate all existing `.sql` files

Run: `cargo run --bin update-sql-tests --migrate`

Review the diff for each file. Commit the batch migration.

**Commit:** Migrate all SQL tests to inline expected format

---

## 52. Update `update-sql-tests` to Write Inline Output (Track 7)

### What Changes

After migration, the `update-sql-tests` tool must write inline expected output (overwriting the `-- >` lines in the `.sql` file) rather than writing a separate `.expected` file.

The tool should:
1. Parse the `.sql` file using the new `parse_sql_test_file()`.
2. Execute each statement.
3. Replace each statement's `-- >` block with fresh output.
4. Write the updated `.sql` file in place.

Blank lines and `--` comments between statements are preserved unchanged.

### Key Files

- `src/testing/sql_runner.rs` — `update_expected_file` → `update_sql_file_inline`
- `src/bin/update-sql-tests.rs` — no change to CLI interface

### Implementation Steps (1 commit)

#### Step 52.1 — Inline writer

Implement `update_sql_file_inline()`:

```rust
fn update_sql_file_inline(sql_path: &PathBuf, actual_per_statement: &[Vec<String>]) {
    // Rebuild the file, replacing -- > blocks with fresh output
}
```

Update `run_sql_test(update_mode=true)` to call this instead of writing a separate file.

**Commit:** update-sql-tests writes inline expected output

---

## 53. Remove Obsolete `.expected` Files (Track 7)

Once all tests have been migrated and the runner no longer reads `.expected` files:

1. Delete all `tests/sql/*.expected` files.
2. Remove the backward-compatibility fallback from the runner.
3. Update `CLAUDE.md` and `README.md` references.

### Implementation Steps (1 commit)

#### Step 53.1 — Delete `.expected` files and remove fallback

```bash
rm tests/sql/*.expected
```

Remove the `.expected` fallback path from `sql_runner.rs`.

Update `CLAUDE.md`:
- Change `cargo run --bin update-sql-tests` description to reflect inline format.
- Remove references to `.expected` files.

**Commit:** Remove .expected files, runner reads inline format only

---

## Verification

For each item:
- [ ] `cargo test test_sql_` — all SQL tests pass
- [ ] `cargo run --bin update-sql-tests` — rewrites inline output correctly; `git diff` shows only whitespace/value changes, no structural diff
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`

**End-to-end check:**
```bash
# Mutate a test file to produce wrong output, then regenerate
cargo run --bin update-sql-tests where_clauses
git diff tests/sql/where_clauses.sql   # should show only the corrected -- > lines
cargo test test_sql_where_clauses      # should pass
```

---

## Example: Before and After

**Before (two files):**

`tests/sql/where_clauses.sql`:
```sql
CREATE TABLE numbers (id INTEGER, value INTEGER, name TEXT)
INSERT INTO numbers VALUES (1, 100, 'one')
INSERT INTO numbers VALUES (2, 200, 'two')
SELECT id, value FROM numbers WHERE value=100
SELECT id, value FROM numbers WHERE value!=100
```

`tests/sql/where_clauses.expected`:
```
Table 'numbers' created
1
1
1	100
3	100
2	200
4	300
5	150
```

**After (single file):**

`tests/sql/where_clauses.sql`:
```sql
CREATE TABLE numbers (id INTEGER, value INTEGER, name TEXT)
-- > Table 'numbers' created

INSERT INTO numbers VALUES (1, 100, 'one')
-- > 1
INSERT INTO numbers VALUES (2, 200, 'two')
-- > 1

SELECT id, value FROM numbers WHERE value=100
-- > 1	100
-- > 3	100

SELECT id, value FROM numbers WHERE value!=100
-- > 2	200
-- > 4	300
-- > 5	150
```
