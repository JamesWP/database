# Phase T — INSERT INTO … SELECT

Allow the source of an INSERT to be a SELECT query rather than a literal VALUES list.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 85 | 1 | Parser: recognise `INSERT INTO t SELECT …` syntax | — |
| 86 | 1 | Planner: `plan_insert` accepts a SELECT as input; validate column count | 85 |
| 87 | 7 | SQL regression tests for INSERT … SELECT | 86 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

INSERT currently only accepts `VALUES (…)` as its row source:

```sql
INSERT INTO users VALUES (1, 'alice')
```

After this phase, the source can be any SELECT:

```sql
INSERT INTO archive SELECT id, name FROM users WHERE active = 0
INSERT INTO summary SELECT dept, COUNT(*) FROM employees GROUP BY dept
INSERT INTO t2 SELECT * FROM t1
```

The compiler already models INSERT generically: `LogicalPlan::Insert { input: Box<LogicalPlan>, … }` where `input` is whatever plan produces the rows. Today `input` is always `LogicalPlan::Values`. This phase wires a full `LogicalPlan::Select` (or any plan) into that slot. The codegen and engine need no changes — they iterate over whatever `input` yields.

The work is entirely in the **parser** (recognise SELECT after `INSERT INTO t`) and the **planner** (call `plan_select` instead of `plan_values`, validate column count).

---

## 85. Parser: `INSERT INTO t SELECT …` (Track 1)

### What Changes

`InsertStatement` in `src/frontend/ast.rs` currently holds a `Values` source. It gains an enum for the source:

```rust
#[derive(Debug)]
pub enum InsertSource {
    Values(Vec<Vec<Expression>>),      // INSERT INTO t VALUES (…), (…)
    Query(Box<SelectStatement>),        // INSERT INTO t SELECT …
}

#[derive(Debug)]
pub struct InsertStatement {
    pub table_name: String,
    pub column_names: Option<Vec<String>>,
    pub source: InsertSource,           // replaces old `values` field
}
```

The parser's INSERT handler (`parse_insert_statement`) currently expects `VALUES` after the table name / optional column list. It now branches:

```rust
fn parse_insert_statement(&mut self) -> ParseResult<ast::InsertStatement> {
    // … parse table_name, optional column_names …
    let source = match self.input.peek() {
        Token::Values => {
            self.input.advance();
            let rows = self.parse_values_rows()?;
            ast::InsertSource::Values(rows)
        }
        Token::Select => {
            let select = self.parse_select_statement()?;
            ast::InsertSource::Query(Box::new(select))
        }
        _ => return Err(ParseError::Expected("VALUES or SELECT")),
    };
    Ok(ast::InsertStatement { table_name, column_names, source })
}
```

All existing match arms on `InsertStatement` (in `src/planner.rs`) update to destructure `source` instead of the old `values` field.

### Key Files

- `src/frontend/ast.rs` — `InsertSource` enum; `InsertStatement.source`
- `src/frontend/parser.rs` — `parse_insert_statement`: branch on `VALUES` vs `SELECT`

### Tests

```rust
#[test]
fn test_parse_insert_select() {
    let stmt = parse("INSERT INTO t2 SELECT id, name FROM t1").unwrap();
    let ins = stmt.as_insert().unwrap();
    assert!(matches!(ins.source, ast::InsertSource::Query(_)));
}

#[test]
fn test_parse_insert_values_still_works() {
    let stmt = parse("INSERT INTO t VALUES (1, 'x')").unwrap();
    let ins = stmt.as_insert().unwrap();
    assert!(matches!(ins.source, ast::InsertSource::Values(_)));
}
```

### Implementation Steps (1 commit)

#### Step 85.1 — Add InsertSource enum; parse SELECT as INSERT source

Add `InsertSource` to AST, extend parser, update all destructuring sites. All existing tests pass.

**Commit:** Parser: support INSERT INTO … SELECT syntax

---

## 86. Planner: Accept SELECT as INSERT Source (Track 1)

### What Changes

`plan_insert` (in `src/planner.rs`) currently calls `plan_values` to build the `input` plan node. It now dispatches on `InsertSource`:

```rust
let input = match insert.source {
    InsertSource::Values(rows) => plan_values(rows)?,
    InsertSource::Query(select) => plan_select(*select, btree)?,
};
```

After building `input`, validate that the number of output columns matches the target table's column count (or the explicit column list if provided):

```rust
let expected_cols = target_columns.len();
let produced_cols = output_width(&input);   // helper: count columns yielded by plan
if produced_cols != expected_cols {
    return Err(PlanError::ColumnCountMismatch {
        expected: expected_cols,
        got: produced_cols,
    });
}
```

`output_width` walks the plan to the first output node (same logic as `extract_output_column_names` from Phase P):

```rust
fn output_width(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Project { columns, .. } => columns.len(),
        LogicalPlan::Aggregate { group_keys, aggregates, .. } => group_keys.len() + aggregates.len(),
        LogicalPlan::Count { .. } => 1,
        LogicalPlan::Values { rows, .. } => rows.first().map(|r| r.len()).unwrap_or(0),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. } => output_width(input),
        LogicalPlan::Scan { columns, .. } => columns.len(),
        _ => 0,
    }
}
```

Add `PlanError::ColumnCountMismatch { expected: usize, got: usize }` and update its `Display`.

`LogicalPlan::Insert` already holds `input: Box<LogicalPlan>` — no struct change needed. The rest of the pipeline (compiler, engine) is unchanged.

### Key Files

- `src/planner.rs` — `plan_insert`: dispatch on `InsertSource`; `output_width` helper; `PlanError::ColumnCountMismatch`

### Tests

```rust
#[test]
fn test_plan_insert_select() {
    let plan = plan_sql("INSERT INTO t2 SELECT id FROM t1");
    if let LogicalPlan::Insert { input, .. } = &plan {
        // input should be a Select/Project plan, not Values
        assert!(!matches!(input.as_ref(), LogicalPlan::Values { .. }));
    } else {
        panic!("expected Insert plan");
    }
}

#[test]
fn test_plan_insert_select_column_count_mismatch() {
    let err = plan_sql_err("INSERT INTO t2 SELECT id, name FROM t1");
    // t2 has 1 column, SELECT produces 2
    assert!(matches!(err, PlanError::ColumnCountMismatch { .. }));
}
```

### Implementation Steps (1 commit)

#### Step 86.1 — plan_insert dispatches on InsertSource; validate column count

Dispatch on `InsertSource::Values` / `InsertSource::Query`, add `output_width`, add `ColumnCountMismatch` error, add planner tests. Full test suite passes.

**Commit:** Planner: INSERT INTO … SELECT uses query plan as input

---

## 87. SQL Regression Tests (Track 7)

### What Changes

`tests/sql/insert_select.sql` — covers the main INSERT … SELECT patterns.

### Test File

```sql
-- tests/sql/insert_select.sql

CREATE TABLE src (id INTEGER, name TEXT)
-- > Table 'src' created

INSERT INTO src VALUES (1, 'alice')
-- > 1 row inserted
INSERT INTO src VALUES (2, 'bob')
-- > 1 row inserted
INSERT INTO src VALUES (3, 'carol')
-- > 1 row inserted

CREATE TABLE dst (id INTEGER, name TEXT)
-- > Table 'dst' created

-- Copy all rows
INSERT INTO dst SELECT id, name FROM src
-- > 3 rows inserted

SELECT id FROM dst ORDER BY id
-- > 1
-- > 2
-- > 3

-- Copy filtered rows
CREATE TABLE seniors (id INTEGER, name TEXT)
-- > Table 'seniors' created

INSERT INTO seniors SELECT id, name FROM src WHERE id >= 2
-- > 2 rows inserted

SELECT id FROM seniors ORDER BY id
-- > 2
-- > 3

-- Insert aggregate results
CREATE TABLE counts (dept TEXT, n INTEGER)
-- > Table 'counts' created

CREATE TABLE employees (dept TEXT, name TEXT)
-- > Table 'employees' created

INSERT INTO employees VALUES ('eng', 'alice')
-- > 1 row inserted
INSERT INTO employees VALUES ('eng', 'bob')
-- > 1 row inserted
INSERT INTO employees VALUES ('hr', 'carol')
-- > 1 row inserted

INSERT INTO counts SELECT dept, COUNT(*) FROM employees GROUP BY dept
-- > 2 rows inserted

SELECT dept, n FROM counts ORDER BY dept
-- > eng|2
-- > hr|1

-- Column count mismatch — must error
INSERT INTO dst SELECT id FROM src
-- > ERROR: column count
```

> Run `cargo run --bin update-sql-tests insert_select` to pin expected output after implementation.

### Key Files

- `tests/sql/insert_select.sql` — new test file

### Tests

`cargo test test_sql_insert_select`

### Implementation Steps (1 commit)

#### Step 87.1 — Add insert_select.sql SQL regression tests

**Commit:** Tests: SQL regression tests for INSERT INTO … SELECT

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `INSERT INTO dst SELECT … FROM src` — rows are copied correctly
- [ ] `INSERT INTO t SELECT … WHERE …` — WHERE filter applied before insert
- [ ] `INSERT INTO summary SELECT dept, COUNT(*) FROM employees GROUP BY dept` — aggregate results inserted
- [ ] Column count mismatch → clear `ColumnCountMismatch` error
- [ ] `cargo test test_sql_insert` — existing INSERT tests unaffected
