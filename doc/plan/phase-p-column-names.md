# Phase P — Column Names in Query Output

Surface column names alongside query results so that callers and the REPL can display a labelled header row.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 70 | 1 | Add `column_names` to `CompiledProgram`; compiler populates it from projection | — |
| 71 | 1 | Expose `column_names` on `QueryExecution` in the public DB API | 70 |
| 72 | 7 | REPL SQL mode displays a header row; SQL tests updated | 71 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

SELECT currently returns rows as `Vec<Vec<ScalarValue>>` with no metadata. The column names that appear in the query (e.g. `SELECT id, name FROM users`) are computed by the planner and used to order the projection, but they are discarded after compilation. The REPL formats values with `|` separators but shows no header.

After this phase:

```
db> SELECT id, name FROM users WHERE id = 1
id  | name
----|------
1   | alice
```

And the public `QueryExecution` struct exposes `column_names: Vec<String>` alongside the row iterator.

The column names come from the projection computed in `plan_select`. Each projected expression already has a name (the column name for simple column refs, or the function name / expression text for computed expressions). The compiler just needs to carry them through to `CompiledProgram`.

---

## 70. `column_names` in `CompiledProgram` (Track 1)

### What Changes

- `CompiledProgram` (in `src/compiler/mod.rs`) gains a `column_names: Vec<String>` field.
- The compiler populates it from the output column names of the top-level plan node.
- Non-query statements (INSERT, DELETE, UPDATE, CREATE TABLE) get an empty `column_names`.

### Background

`CompiledProgram` currently:

```rust
pub struct CompiledProgram {
    pub operations: Vec<Operation>,
    pub num_registers: usize,
}
```

The planner's `Project` node carries `columns: Vec<(String, PlanExpr)>` — the first element of each tuple is the output column name. `Aggregate` carries `aggregates: Vec<(String, AggregateExpr)>` with the same layout. `Count` implies a single column named `"COUNT(*)"`.

The compiler's entry point (`compile` in `src/compiler/mod.rs`) builds a `CodegenContext` and dispatches to `codegen_node`. After codegen, collect names from the top-level node:

```rust
let column_names = extract_output_column_names(&plan);
```

`extract_output_column_names` walks the plan to the first output-producing node:

```rust
fn extract_output_column_names(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Project { columns, .. } =>
            columns.iter().map(|(name, _)| name.clone()).collect(),
        LogicalPlan::Aggregate { group_keys, aggregates, .. } => {
            // group key names + aggregate output names
            let mut names: Vec<String> = group_keys.iter()
                .map(|e| expr_name(e))
                .collect();
            names.extend(aggregates.iter().map(|(name, _)| name.clone()));
            names
        }
        LogicalPlan::Count { .. } => vec!["COUNT(*)".into()],
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. } =>
            extract_output_column_names(input),
        _ => vec![],
    }
}
```

`expr_name` returns a display string for a `PlanExpr` (e.g. `"id"` for `ColumnRef`, `"COUNT(*)"` for aggregates).

### Key Files

- `src/compiler/mod.rs` — `CompiledProgram`; `compile` function
- `src/planner.rs` — reference for plan node shapes

### Tests

```rust
#[test]
fn test_compiled_program_has_column_names() {
    let program = compile_sql("SELECT id, name FROM users");
    assert_eq!(program.column_names, vec!["id", "name"]);
}

#[test]
fn test_insert_program_has_no_column_names() {
    let program = compile_sql("INSERT INTO users VALUES (1, 'alice')");
    assert!(program.column_names.is_empty());
}
```

### Implementation Steps (1 commit)

#### Step 70.1 — Add `column_names` to `CompiledProgram` and populate in compiler

Add field, implement `extract_output_column_names`, populate in `compile`. All existing tests pass (field is additive).

**Commit:** Compiler: add column_names to CompiledProgram

---

## 71. Expose `column_names` on `QueryExecution` (Track 1)

### What Changes

- `QueryExecution` (in `src/db.rs`) gains `pub column_names: Vec<String>`.
- `Database::execute` (or `execute` free function) threads `column_names` from `CompiledProgram` through to `QueryExecution`.
- All construction sites of `QueryExecution` are updated.

### Background

`QueryExecution` currently:

```rust
pub struct QueryExecution {
    rows: Vec<Vec<ScalarValue>>,
    pos: usize,
}
```

After this item:

```rust
pub struct QueryExecution {
    pub column_names: Vec<String>,
    rows: Vec<Vec<ScalarValue>>,
    pos: usize,
}
```

The `execute` function already compiles the SQL into a `CompiledProgram` and runs the engine. After running, `column_names` is available from the program and can be moved into `QueryExecution`.

### Key Files

- `src/db.rs` — `QueryExecution` struct; `execute` function

### Tests

```rust
#[test]
fn test_query_execution_exposes_column_names() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
    let result = db.query("SELECT id, val FROM t").unwrap();
    assert_eq!(result.column_names, vec!["id", "val"]);
}
```

### Implementation Steps (1 commit)

#### Step 71.1 — Expose column_names on QueryExecution

Add field to struct, thread from `CompiledProgram` in `execute`. Add test.

**Commit:** DB API: expose column_names on QueryExecution

---

## 72. REPL Displays Header Row (Track 7)

### What Changes

- `src/repl/modes/sql.rs` reads `query.column_names` and prints a header row with `---` separator before the data rows, matching the style of `sqlite3`'s `.headers on` mode.
- A SQL test verifies the header is present (via EXPLAIN or a dedicated REPL test).

### Background

The SQL mode currently formats rows via `plain_string()` and `|`-joins, with no header. The column names are now available from `query.column_names`. The header format:

```
id  | name
----|------
1   | alice
```

Implementation in `src/repl/modes/sql.rs`:

```rust
// After collecting all rows and computing column widths:
if !column_names.is_empty() {
    let header: Vec<String> = column_names.iter()
        .enumerate()
        .map(|(i, name)| format!("{:<width$}", name, width = col_widths[i]))
        .collect();
    output.push(header.join(" | "));
    let sep: Vec<String> = col_widths.iter()
        .map(|w| "-".repeat(*w))
        .collect();
    output.push(sep.join("-|-"));
}
```

When the result set is empty but there are column names, show just the header (no data rows). When there are no column names (DDL statements), nothing changes.

### Key Files

- `src/repl/modes/sql.rs` — result formatting
- `tests/sql/` — SQL tests may need header rows added to expected output if the REPL output is tested end-to-end

### Tests

The SQL test runner exercises the engine output, not the REPL formatting layer, so existing SQL tests are unaffected. Manual verification via `cargo run -- test.db sql "SELECT ..."`.

Add a REPL integration test or manual test entry in `manual_tests/`.

### Implementation Steps (1 commit)

#### Step 72.1 — REPL SQL mode: print header row before results

Update `src/repl/modes/sql.rs` to print header + separator using `query.column_names`. Verify manually with `cargo run`.

**Commit:** REPL: display column name header row for SELECT results

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `SELECT id, name FROM t` — REPL shows `id | name` header then `---|---` separator
- [ ] `INSERT INTO t …` — no header printed (non-query)
- [ ] `SELECT COUNT(*) FROM t` — header shows `COUNT(*)`
- [ ] `SELECT dept, SUM(amount) FROM t GROUP BY dept` — header shows `dept | SUM(amount)`
- [ ] `query.column_names` is accessible from the public `QueryExecution` API
