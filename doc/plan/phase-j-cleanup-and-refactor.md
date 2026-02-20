# Phase J — Cleanup and Refactoring

This phase focuses on paying down technical debt accumulated during the rapid development of the core features. We will consolidate duplicated logic, refactor direct storage access to use the proper architectural layers, and remove temporary "V1" restrictions.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 45 | 8.1 | Consolidate Value Printing | — |
| 46 | 8.2 | Refactor Index Creation (Use Engine) | — |
| 47 | 8.3 | Unwind "V1" Cleanup | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Completed Items

| # | Track | Item | Completed |
|---|-------|------|-----------|
| | | | |

---

## Overview

The codebase has accumulated several "V1" shortcuts and duplicated logic patterns during the initial push for features. This phase addresses three high-impact areas:
1.  **Value Printing:** `ScalarValue` formatting is duplicated in the REPL and Engine.
2.  **Index Creation:** `CREATE INDEX` bypasses the query engine, using brittle manual cursor operations.
3.  **V1 Artifacts:** Numerous `TODO`s and `panic!`s refer to "V1" limitations that are either no longer true (thanks to variable-length keys) or need proper error handling.

---

## 45. Consolidate Value Printing (Track 8.1)

### What Changes

Centralize the logic for formatting `ScalarValue` as text into a single `Display` implementation.

### Background

Currently, logic for formatting values is scattered:
- `src/engine/scalarvalue.rs`: `impl Display` (used by some debug paths).
- `src/repl/modes/sql.rs`: `plain_value` function (used for table output).
- `src/repl/modes/btree.rs`: Manual formatting (used for debug output).

### Implementation Approach

1.  **Enhance ScalarValue Display**: Update `impl Display for ScalarValue` in `src/engine/scalarvalue.rs` to handle all types cleanly (Strings with quotes? Or just content? typically for SQL output we want content, but for debug we want quotes. We might need `Display` for user output and `Debug` for internal).
    *   Decision: `Display` should be the user-facing string representation (e.g. `123`, `hello`, `NULL`).
    *   Update `plain_value` in `sql.rs` to use this trait.
2.  **Refactor REPL**: Update `src/repl/modes/sql.rs` to use the unified `Display` implementation.
3.  **Refactor BTree Debug**: Update `src/repl/modes/btree.rs` to use the unified implementation.

### Key Files

- `src/engine/scalarvalue.rs`
- `src/repl/modes/sql.rs`
- `src/repl/modes/btree.rs`

### Tests

- Manual verification of REPL output for `SELECT *`.
- Verify `NULL`, `Blob`, and `String` formatting.

### Implementation Steps (2 commits)

#### Step 45.1 — Engine: robust Display for ScalarValue

Update `impl Display` in `scalarvalue.rs` to cover all types, matching the desired output format of the SQL REPL.

**Commit:** Implement robust Display for ScalarValue

#### Step 45.2 — REPL: use ScalarValue Display

Remove `plain_value` from `sql.rs` and manual formatting from `btree.rs`, replacing them with `val.to_string()`.

**Commit:** Refactor REPL to use ScalarValue::Display

---

## 46. Refactor Index Creation (Track 8.2)

### What Changes

Update `CREATE INDEX` to execute via the Planner and Engine, rather than manual B-Tree cursor operations.

### Background

The current `CREATE INDEX` implementation in `src/db.rs` manually:
1.  Opens a cursor on the table.
2.  Iterates every row.
3.  Manually decodes columns.
4.  Manually inserts into the index B-Tree.

This duplicates scanning logic and bypasses the engine's safeguards.

### Implementation Approach

**Planner Changes:**
- No new logical nodes are strictly required if we can compose existing ones, but a `LogicalPlan::CopyTableToIndex` might be cleanest.
- Alternatively, construct a plan equivalent to: `INSERT INTO index_table SELECT col, pk FROM source_table`.

**Execution Logic (`src/db.rs`):**
1.  **Allocate**: Create the index tree (keep this in `db.rs`).
2.  **Plan**: Construct a `LogicalPlan` that scans the source table and writes to the new index rootpage.
    *   Node 1: `Scan { table_name, ... }`
    *   Node 2: `Project { exprs: [col, pk] }`
    *   Node 3: `WriteIndex { index_rootpage, ... }`
3.  **Execute**: Compile and run this plan using the `Engine`.

### Key Files

- `src/db.rs` — Remove manual loop, build Plan.
- `src/planner.rs` — Ensure `WriteIndex` can be planned/targeted (or just build the `LogicalPlan` enum manually in `db.rs` if it's a special utility operation).

### Tests

- `test_execute_create_index`: Verify index is still populated correctly.
- `test_execute_create_index_on_populated_table`.

### Implementation Steps (2 commits)

#### Step 46.1 — DB: use Engine for Create Index

Rewrite the `Statement::CreateIndex` handler in `src/db.rs` to construct a `LogicalPlan` (Scan -> Project -> WriteIndex) and execute it.

**Commit:** Refactor CREATE INDEX to use Engine execution

#### Step 46.2 — Cleanup: remove manual cursor logic

Remove any helper functions in `db.rs` that were only used for the manual scan.

**Commit:** Remove deprecated manual index population logic

---

## 47. Unwind "V1" Cleanup (Track 8.3)

### What Changes

Remove "V1" terminology, resolve `TODO`s, and replace panics with proper errors.

### Background

The codebase contains numerous comments like `// V1: only INTEGER supported` or `panic!("V1 limitation")`. With the introduction of variable-length keys in Phase G4 (already in codebase), many of these are obsolete.

### Implementation Approach

1.  **Search & Destroy**: Grep for "V1", "temporary", "limit".
2.  **Error Handling**: Replace `panic!` with `Err(ExecuteError::FeatureNotSupported)`.
3.  **API Migration**:
    *   In `src/engine.rs`, `WriteIndex` and `WriteCursor` might be using `insert_u64`.
    *   Switch them to use `insert` (taking `&[u8]`) where possible, or clearly mark *why* they remain if `u64` is still required by the opcode signature.
    *   Update `src/db.rs` validation: if we still don't support TEXT indexes (until Phase G4 complete), return a proper error, don't panic or say "V1".

### Key Files

- `src/db.rs`
- `src/engine.rs`
- `src/storage/btree.rs`

### Tests

- `test_error_cases.sql`: Ensure unsupported features return errors, not crashes.

### Implementation Steps (3 commits)

#### Step 47.1 — DB/Planner: standardize error handling

Replace "V1" panics in `db.rs` (like column type checks) with proper `ExecuteError` variants.

**Commit:** Replace V1 panics with ExecuteErrors

#### Step 47.2 — Engine: cleanup V1 comments

Remove obsolete "V1" comments in `engine.rs` and `compiler`.

**Commit:** Remove obsolete V1 comments and TODOs

#### Step 47.3 — Storage: consolidate insert API usage

Review usage of `insert_u64` vs `insert`. If `insert_u64` is just a wrapper, keep it but ensure comments reflect that it's for convenience, not a V1 limitation.

**Commit:** Cleanup storage API usage comments

---

## Verification

For each item:
- [ ] Tests written/verified first.
- [ ] All tests pass: `cargo test --bin database`.
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`.
