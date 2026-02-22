# Phase O — HAVING Clause

Add `HAVING` support to GROUP BY queries, allowing aggregate results to be filtered after grouping.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 66 | 1 | Parse HAVING: add `having` field to AST, extend parser | — |
| 67 | 1 | Plan HAVING: convert to `PlanExpr`, validate, store in `LogicalPlan::Aggregate` | 66 |
| 68 | 1 | Compile HAVING: emit filter after `YieldFromGroupTable` | 67 |
| 69 | 7 | SQL regression tests for HAVING | 68 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

GROUP BY and aggregate functions (COUNT, SUM, AVG, MIN, MAX) are fully implemented. The one missing piece is the `HAVING` clause, which filters the groups produced by `GROUP BY` in the same way that `WHERE` filters individual rows before grouping.

Example:

```sql
SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 3;
```

The `HAVING` keyword is already recognised by the lexer but the parser discards it — no AST node, planner support, or codegen exists.

The implementation follows the existing aggregate pipeline:

```
Scan → [Filter (WHERE)] → Aggregate → [HAVING filter] → [Sort] → [Limit] → Yield
```

HAVING is cleanly modelled by extending `LogicalPlan::Aggregate` with an optional `having: Option<PlanExpr>` field. The compiler emits the condition check inline between `YieldFromGroupTable` and `Yield`, avoiding a new plan node.

---

## 66. Parse HAVING Clause (Track 1)

### What Changes

- `SelectStatement` gains `having: Option<Expression>`.
- The parser, after consuming the `GROUP BY` clause, checks for `HAVING` and if present parses the following expression into the new field.
- All construction sites that build `SelectStatement` are updated.

### Background

`SelectStatement` is defined in `src/frontend/ast.rs`:

```rust
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<ColumnExpression>,
    pub from: NamedTupleSource,
    pub joins: Vec<JoinClause>,
    pub filter: Option<Expression>,   // WHERE
    pub limit: Option<Expression>,
    pub order_by: Option<Vec<OrderByClause>>,
    pub group_by: Option<Vec<Expression>>,
    // having: missing
}
```

The lexer already defines a `Having` token. The parser skips it when encountered because there is no corresponding grammar rule.

### Implementation Approach

**`src/frontend/ast.rs`** — add field:

```rust
pub struct SelectStatement {
    // ... existing fields ...
    pub group_by: Option<Vec<Expression>>,
    pub having: Option<Expression>,  // NEW
}
```

**`src/frontend/parser.rs`** — in the SELECT parsing function, after the GROUP BY block:

```rust
// After parsing GROUP BY:
let having = if self.peek_token_is(Token::Having) {
    self.consume(); // eat HAVING
    Some(self.parse_expression()?)
} else {
    None
};
```

Update the `SelectStatement { ... }` construction to include `having`.

All `SelectStatement { .. }` wildcard patterns compile unchanged; any exhaustive constructions (tests, planner) need `having: None` added. Search with:

```bash
cargo build 2>&1 | grep "missing field"
```

### Key Files

- `src/frontend/ast.rs` — `SelectStatement` definition
- `src/frontend/parser.rs` — SELECT parsing; HAVING token consumption

### Tests

**Unit test in parser:**

```rust
#[test]
fn test_parse_having_clause() {
    let stmt = parse_select("SELECT dept, COUNT(*) FROM t GROUP BY dept HAVING COUNT(*) > 3");
    let sel = stmt.as_select().unwrap();
    assert!(sel.group_by.is_some());
    assert!(sel.having.is_some());
}

#[test]
fn test_parse_select_without_having_is_none() {
    let stmt = parse_select("SELECT dept FROM t GROUP BY dept");
    let sel = stmt.as_select().unwrap();
    assert!(sel.having.is_none());
}
```

**Error test — HAVING without GROUP BY** (deferred to planner; parser accepts it):

The parser does not enforce HAVING requires GROUP BY; that check is the planner's job (item 67).

### Implementation Steps (1 commit)

#### Step 66.1 — Add `having` field to AST and parse it

Add `having: Option<Expression>` to `SelectStatement`, extend the parser to consume `HAVING <expr>`, update all construction sites. Run `cargo test` — all tests pass.

**Commit:** Parser: add HAVING clause to SelectStatement

---

## 67. Plan HAVING (Track 1)

### What Changes

- `LogicalPlan::Aggregate` gains `having: Option<PlanExpr>`.
- `plan_select` converts `select.having` to a `PlanExpr` and stores it.
- Validation: HAVING is only allowed when there is a GROUP BY or at least one aggregate in the SELECT list; report a `PlanError` otherwise.
- All destructuring sites of `LogicalPlan::Aggregate` are updated.

### Background

`LogicalPlan::Aggregate` currently (in `src/planner.rs`):

```rust
Aggregate {
    group_keys: Vec<PlanExpr>,
    aggregates: Vec<(String, AggregateExpr)>,  // (output_name, expr)
    input: Box<LogicalPlan>,
}
```

The HAVING predicate is evaluated after groups are fully accumulated, so it can reference:

1. **Group key columns** — available as columns in the Aggregate output row.
2. **Aggregate results** — e.g. `COUNT(*) > 3`.

The HAVING expression must be converted using the same `convert_expression` path as SELECT columns, but the column resolution context is the *output* of the Aggregate node (group keys + aggregates), not the raw input table.

### Implementation Approach

**`src/planner.rs`** — extend `LogicalPlan::Aggregate`:

```rust
Aggregate {
    group_keys: Vec<PlanExpr>,
    aggregates: Vec<(String, AggregateExpr)>,
    having: Option<PlanExpr>,          // NEW
    input: Box<LogicalPlan>,
}
```

**`plan_select`** — after building the `Aggregate` node (existing logic around line 454-532), convert `select.having`:

```rust
let having = if let Some(having_expr) = select.having {
    // Validate: HAVING only valid with GROUP BY or aggregates
    if !use_aggregation {
        return Err(PlanError::InvalidHaving(
            "HAVING requires GROUP BY or aggregate functions".into()
        ));
    }
    // Convert using same column context as aggregate output
    Some(convert_having_expression(having_expr, &group_key_exprs, &aggregates)?)
} else {
    None
};
```

`convert_having_expression` resolves column references against the aggregate output columns (group keys first, then aggregate result names), and detects aggregate sub-expressions within the HAVING predicate by calling `convert_aggregate` for any `FunctionCall` node that `is_aggregate_function` returns true for.

**Simple approach for V1**: Re-use `convert_expression` with the same `columns_needed` context used for the SELECT list. The HAVING expression is treated identically to a SELECT expression — aggregate calls in HAVING produce new `AggregateExpr` entries appended to `aggregates` (deduplicated by name). The output registers for those aggregates are then referenced in the `having` `PlanExpr`.

**`PlanError`** — add variant:

```rust
InvalidHaving(String),
```

Update `Display` for `PlanError`.

All match arms on `LogicalPlan::Aggregate` add `having: _` or destructure the new field.

### Key Files

- `src/planner.rs` — `LogicalPlan::Aggregate` definition; `plan_select` HAVING conversion; `PlanError`
- `src/compiler/nodes.rs` — destructuring of `LogicalPlan::Aggregate` (add `having`)
- `src/explain.rs` — `Aggregate` arm (add `having` display if non-None)

### Tests

**Planner unit tests:**

```rust
#[test]
fn test_plan_having_count_star() {
    let plan = plan_with_schema("SELECT dept, COUNT(*) FROM t GROUP BY dept HAVING COUNT(*) > 3");
    if let LogicalPlan::Aggregate { having, .. } = &plan {
        assert!(having.is_some(), "expected HAVING predicate");
    } else {
        panic!("expected Aggregate node");
    }
}

#[test]
fn test_plan_having_without_group_by_errors() {
    let err = plan_with_schema_err("SELECT id FROM t HAVING COUNT(*) > 1");
    assert!(matches!(err, PlanError::InvalidHaving(_)));
}
```

### Implementation Steps (1 commit)

#### Step 67.1 — Add `having` to Aggregate plan node; populate in plan_select

Add field to struct, convert `select.having` in `plan_select`, add `InvalidHaving` error variant, update all destructuring sites. Add planner unit tests. Run `cargo test`.

**Commit:** Planner: add HAVING predicate to LogicalPlan::Aggregate

---

## 68. Compile HAVING (Track 1)

### What Changes

`codegen_aggregate` (in `src/compiler/nodes.rs`) emits a conditional skip after `YieldFromGroupTable` populates the output registers: if `having` is `Some(predicate)`, evaluate the predicate and jump past `Yield` when it is false.

### Background

The current aggregate codegen loop (`codegen_aggregate`, around line 971) looks like:

```
INIT:
  InitGroupTable(table_reg)
  [child init]

BODY:
  [child body — for each row:]
  UpdateGroup(table_reg, key_regs, agg_specs)

YIELD:
yield_loop:
  YieldFromGroupTable(out_regs, table_reg, done)   ← populates out_regs; jumps to done when empty
  Yield(out_regs)
  GoTo(yield_loop)

done:
  Halt
```

With HAVING the yield loop becomes:

```
yield_loop:
  YieldFromGroupTable(out_regs, table_reg, done)
  [evaluate having predicate into cond_reg]
  GoToIfFalse(yield_loop, cond_reg)               ← skip Yield, fetch next group
  Yield(out_regs)
  GoTo(yield_loop)

done:
  Halt
```

The HAVING predicate references aggregate output registers and group key registers — both of which are populated by `YieldFromGroupTable` before the condition is checked. No additional state is needed.

### Implementation Approach

**`src/compiler/nodes.rs`** — `codegen_aggregate` signature:

```rust
fn codegen_aggregate(
    ctx: &mut CompileCtx,
    group_keys: &[PlanExpr],
    aggregates: &[(String, AggregateExpr)],
    having: Option<&PlanExpr>,           // NEW
    input: &LogicalPlan,
) { ... }
```

Update the caller to pass `having.as_ref()`.

In the yield loop, after `YieldFromGroupTable`:

```rust
if let Some(pred) = having {
    let cond_reg = ctx.registers.alloc();
    compile_expr(ctx, pred, cond_reg, &out_reg_map);  // evaluate predicate
    ctx.body_emitter.emit(Operation::GoToIfFalse(yield_loop_target, cond_reg));
}
ctx.body_emitter.emit(Operation::Yield(out_regs.clone()));
```

`out_reg_map` maps column indices (group keys + aggregate results) to the registers populated by `YieldFromGroupTable`, so `compile_expr` resolves `ColumnRef` nodes to the correct registers.

`GoToIfFalse` takes a `JumpTarget` — use the same back-edge target used for `GoTo(yield_loop)`.

### Key Files

- `src/compiler/nodes.rs` — `codegen_aggregate` body; pass `having` from `LogicalPlan::Aggregate` match arm

### Tests

**Engine integration test:**

```rust
#[test]
fn test_having_filters_groups() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE sales (dept TEXT, amount INTEGER)").unwrap();
    db.execute("INSERT INTO sales VALUES ('eng', 100)").unwrap();
    db.execute("INSERT INTO sales VALUES ('eng', 200)").unwrap();
    db.execute("INSERT INTO sales VALUES ('hr', 50)").unwrap();

    // Only eng has SUM > 100
    let rows = db.execute_rows(
        "SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 100"
    ).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ScalarValue::String("eng".into()));
    assert_eq!(rows[0][1], ScalarValue::Integer(300));
}

#[test]
fn test_having_count_star() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE t (cat TEXT)").unwrap();
    for _ in 0..3 { db.execute("INSERT INTO t VALUES ('a')").unwrap(); }
    db.execute("INSERT INTO t VALUES ('b')").unwrap();

    let rows = db.execute_rows(
        "SELECT cat, COUNT(*) FROM t GROUP BY cat HAVING COUNT(*) >= 3"
    ).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ScalarValue::String("a".into()));
}
```

### Implementation Steps (2 commits)

#### Step 68.1 — Thread `having` into codegen_aggregate

Destructure `having` in the `LogicalPlan::Aggregate` match arm in `codegen_aggregate`'s caller; pass it to `codegen_aggregate`. No codegen change yet — `having` is ignored. Tests pass unchanged.

**Commit:** Compiler: thread having predicate into codegen_aggregate

#### Step 68.2 — Emit HAVING filter after YieldFromGroupTable

Add the conditional skip after `YieldFromGroupTable` using `GoToIfFalse`. Add `test_having_filters_groups` and `test_having_count_star`. Run `cargo test`.

**Commit:** Compiler: emit HAVING filter in aggregate codegen

---

## 69. SQL Regression Tests for HAVING (Track 7)

### What Changes

Add `tests/sql/having.sql` with inline expected output covering the main HAVING patterns. Run `update-sql-tests` to pin expected output.

### Test File

```sql
-- tests/sql/having.sql

CREATE TABLE orders (customer TEXT, amount INTEGER)
-- > Table 'orders' created

INSERT INTO orders VALUES ('alice', 100)
-- > 1 row inserted
INSERT INTO orders VALUES ('alice', 200)
-- > 1 row inserted
INSERT INTO orders VALUES ('bob', 50)
-- > 1 row inserted
INSERT INTO orders VALUES ('carol', 300)
-- > 1 row inserted
INSERT INTO orders VALUES ('carol', 400)
-- > 1 row inserted

-- HAVING COUNT(*) >= 2: alice and carol
SELECT customer, COUNT(*) FROM orders GROUP BY customer HAVING COUNT(*) >= 2
-- > alice|2
-- > carol|2

-- HAVING SUM > threshold: only carol
SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 500
-- > carol|700

-- HAVING with WHERE (WHERE filters rows first, then HAVING filters groups)
SELECT customer, COUNT(*) FROM orders WHERE amount >= 100 GROUP BY customer HAVING COUNT(*) = 1
-- > alice|1
-- > carol|1

-- HAVING MIN
SELECT customer, MIN(amount) FROM orders GROUP BY customer HAVING MIN(amount) < 100
-- > bob|50

-- No groups pass HAVING — empty result
SELECT customer FROM orders GROUP BY customer HAVING COUNT(*) > 100
-- > (no output)
```

> Note: update the `-- >` expected lines using `cargo run --bin update-sql-tests having` after implementation.

### Key Files

- `tests/sql/having.sql` — new test file

### Tests

`cargo test test_sql_having`

### Implementation Steps (1 commit)

#### Step 69.1 — Add having.sql SQL regression test

Create `tests/sql/having.sql`, run `update-sql-tests having` to pin output, run `cargo test test_sql_having`.

**Commit:** Tests: SQL regression tests for HAVING clause

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `cargo test test_sql_having` — all HAVING tests pass
- [ ] `cargo test test_sql_group_by` — existing GROUP BY tests unaffected
- [ ] `test_having_filters_groups` — only groups meeting condition are returned
- [ ] `test_having_count_star` — COUNT(*) in HAVING works
- [ ] `test_plan_having_without_group_by_errors` — clear error when HAVING used without GROUP BY
- [ ] `EXPLAIN SELECT … GROUP BY … HAVING …` — HAVING predicate visible in plan output
