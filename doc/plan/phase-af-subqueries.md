# Phase AF — Non-Correlated Subqueries

Add non-correlated subquery support: FROM-clause derived tables, IN (literal list), IN (subquery), and scalar subqueries in SELECT/WHERE. All subqueries compile into a single flat bytecode program using RowBuffer materialization — no nested program execution.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 103 | 1 | AST + parser: `Expression::In`, `Expression::ScalarSubquery` | — |
| 104 | 4 | FROM subqueries (derived tables): planner + compiler | 103 |
| 105 | 4 | IN operator: literal list (OR desugar) + IN (subquery) via RowBuffer | 103 |
| 106 | 4 | Scalar subqueries in SELECT list and WHERE | 103 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Why non-correlated first?

Non-correlated subqueries — those that do not reference outer-query columns — can be fully evaluated once as a prelude before the outer query begins. This maps cleanly onto the existing flat-bytecode model: the compiler emits an inner loop that fills a `RowBuffer`, followed by the outer query that reads from it. No new control-flow primitives or register types are needed.

Correlated subqueries (referencing outer row values, e.g. `EXISTS`) require re-executing the inner loop per outer row; that is left for a later phase.

### Single flat program model

Every subquery compiles into the **same** `CompiledProgram` as the outer query. The inner query becomes a **prelude section** — a loop that runs to completion before the outer query's cursor is even opened. Results are held in a `RowBuffer` register and consumed by the outer query.

```
[inner prelude loop → fills RowBuffer r_buf]
RewindRowBuffer r_buf         ; reset read cursor for outer
[outer query loop → reads from r_buf or checks membership in r_buf]
Halt
```

This approach:
- Reuses all existing VM operations
- Adds exactly one new VM operation (`RowBufferContains`) with a lazy HashSet cache
- Adds one field to `RowBuffer` (the cache)
- Requires no new `RegisterValue` variants

### RowBufferContains and lazy HashSet cache

For `IN (subquery)`, the outer loop calls `RowBufferContains` once per outer row. To avoid O(n) linear scan, `RowBuffer` gains an `Option<HashSet<ScalarValue>>` cache field:

```rust
pub struct RowBuffer {
    pub rows: Vec<Vec<ScalarValue>>,
    pub cursor: usize,
    /// Built lazily on first RowBufferContains call. Indexes rows[i][0].
    contains_cache: Option<HashSet<ScalarValue>>,
}
```

The first `RowBufferContains` execution builds the set from `rows[*][0]`; subsequent calls are O(1). The cache is never built if the buffer is only used for `NextFromRowBuffer` (FROM subqueries, ORDER BY) — zero overhead for those cases.

`ScalarValue` already implements `Hash + Eq`, so `HashSet<ScalarValue>` works directly.

---

## 103. AST + parser: `Expression::In` and `Expression::ScalarSubquery` (Track 1)

### What Changes

Two new `Expression` variants are added to `src/frontend/ast.rs`. The parser is extended to recognize `IN (...)`, `NOT IN (...)`, and `(SELECT ...)` in expression position.

### New AST nodes

```rust
// src/frontend/ast.rs

pub enum InSource {
    /// IN (1, 2, 3)
    Values(Vec<Expression>),
    /// IN (SELECT ...)
    Subquery(Box<SelectStatement>),
}

pub enum Expression {
    // ... existing variants ...

    /// expr IN (...) / expr NOT IN (...)
    In {
        expr: Box<Expression>,
        source: InSource,
        negated: bool,
    },

    /// (SELECT expr FROM ...)  — returns a single scalar value
    ScalarSubquery(Box<SelectStatement>),
}
```

`get_column_references` on `Expression` gains arms for the new variants (returning `vec![]` for `ScalarSubquery`; delegating to `expr` for `In`).

### Parser changes

**IN / NOT IN** (`src/frontend/parser.rs`):

`IN` is a postfix operator parsed after the primary expression in `parse_expression` (similar to how `IS NULL` / `IS NOT NULL` are handled as `UnaryOp` variants applied after the left-hand side). After parsing a left-hand expression, peek for `IN` or `NOT IN`:

```rust
// After parsing lhs expression:
if self.input.peek() == lexer::Type::In
    || (self.input.peek() == lexer::Type::Not && next_is_in) {
    let negated = /* consume NOT if present */;
    self.input.expect(Expect::Token(lexer::Type::In))?;
    self.input.expect(Expect::LeftParen)?;

    if self.input.peek() == lexer::Type::Select {
        // IN (SELECT ...)
        let subquery = self.parse_select_statement()?;
        self.input.expect(Expect::RightParen)?;
        return Ok(Expression::In {
            expr: Box::new(lhs),
            source: InSource::Subquery(Box::new(subquery)),
            negated,
        });
    } else {
        // IN (expr, expr, ...)
        let values = self.parse_comma_separated(|p| p.parse_expression())?;
        self.input.expect(Expect::RightParen)?;
        return Ok(Expression::In {
            expr: Box::new(lhs),
            source: InSource::Values(values),
            negated,
        });
    }
}
```

**Scalar subquery** — in the primary expression parser, when a `(` is seen, peek ahead: if `SELECT` follows, parse as a subquery rather than a grouped expression:

```rust
lexer::Type::LeftParen => {
    self.input.advance();
    if self.input.peek() == lexer::Type::Select {
        let stmt = self.parse_select_statement()?;
        self.input.expect(Expect::RightParen)?;
        Ok(Expression::ScalarSubquery(Box::new(stmt)))
    } else {
        // existing: grouped expression
        let expr = self.parse_expression()?;
        self.input.expect(Expect::RightParen)?;
        Ok(expr)
    }
}
```

**Lexer**: add `In` and `Not` token types if not already present. (`Not` may already exist for `IS NOT NULL`; verify.)

### Key Files

- `src/frontend/ast.rs` — new enum variants
- `src/frontend/parser.rs` — `IN`/`NOT IN`/scalar subquery parsing
- `src/frontend/lexer.rs` — `In` keyword token (if missing)

### Tests

Parser unit tests (in `src/frontend/parser.rs` `#[cfg(test)]`):

```rust
#[test]
fn parse_in_literal_list() {
    // SELECT id FROM users WHERE id IN (1, 2, 3)
    // → filter = Expression::In { negated: false, source: Values([1,2,3]) }
}

#[test]
fn parse_not_in_literal_list() {
    // WHERE id NOT IN (10, 20)
    // → Expression::In { negated: true, source: Values([10, 20]) }
}

#[test]
fn parse_in_subquery() {
    // WHERE id IN (SELECT user_id FROM admins)
    // → Expression::In { source: InSource::Subquery(...) }
}

#[test]
fn parse_scalar_subquery_in_select() {
    // SELECT (SELECT COUNT(*) FROM orders), name FROM users
    // → columns[0] = ScalarSubquery(...)
}
```

### Implementation Steps (2 commits)

#### Step 103.1 — AST: add `InSource`, `Expression::In`, `Expression::ScalarSubquery`

Add the enum variants. Update `get_column_references`. No parser changes yet — the new variants are simply unreachable. All tests pass.

**Commit:** `AST: add Expression::In and Expression::ScalarSubquery variants`

#### Step 103.2 — Parser: IN / NOT IN / scalar subquery

Add `In` lexer token. Extend the expression parser. Add parser unit tests.

**Commit:** `Parser: add IN, NOT IN, and scalar subquery expression parsing`

---

## 104. FROM subqueries (derived tables) (Track 4)

### What Changes

`FROM (SELECT ...) AS alias` is already parsed correctly into `TupleSource::Subquery` (the parser code exists and is functional). The planner currently returns `UnsupportedStatement` for this case. This item removes that error and compiles the inner query as a RowBuffer prelude; the outer query iterates the buffer using `NextFromRowBuffer`.

### Background

`NamedTupleSource` already holds the alias:

```rust
pub enum NamedTupleSource {
    Named { alias: String, source: TupleSource },
    Anonymous(TupleSource),
}
```

The planner's `extract_table_name` in `resolver.rs` currently panics on `TupleSource::Subquery`. The new path:

1. Detect `TupleSource::Subquery` in `single_table_context` (or the unified `plan_select` after Phase Z).
2. Plan the inner `SelectStatement` recursively → `inner_plan: LogicalPlan`.
3. Wrap in a new `LogicalPlan::DerivedTable` node that carries the alias and the inner plan's output column names (for the resolver to use in column lookups).
4. The compiler emits the inner plan as a RowBuffer prelude, then the outer query iterates with `NextFromRowBuffer`.

### New plan node

```rust
// src/planner/mod.rs
pub enum LogicalPlan {
    // ... existing variants ...

    /// FROM (SELECT ...) AS alias — inner plan materialized into a RowBuffer.
    DerivedTable {
        source: Box<LogicalPlan>,
        alias: String,
        /// Output column names from the inner SELECT (for column resolution in outer query).
        columns: Vec<String>,
    },
}
```

### Column resolution

A new `DerivedTableResolver` implements `ColumnResolver`, mapping `alias.col_name` and bare `col_name` to positional indices in the buffer row:

```rust
struct DerivedTableResolver<'a> {
    alias: &'a str,
    columns: &'a [String],   // names in order
}
```

`resolve_column("col_name")` → index of `col_name` in `columns`.
`resolve_column("alias.col_name")` → same, with alias prefix stripped.

### Compiler output

`codegen_derived_table` in `src/compiler/nodes.rs`:

```
// Prelude: compile inner plan inline, routing Yield into AppendToRowBuffer
InitRowBuffer          r_buf

[inner plan bytecode, with Yield replaced by AppendToRowBuffer(r_buf, yielded_regs)]

// After inner plan completes (Halt → jump to here):
RewindRowBuffer        r_buf

// Outer query continuation reads from buffer:
OuterLoopStart:
  NextFromRowBuffer    [r0, r1, ...], r_buf → Halt
  [rest of outer query using r0, r1 ...]
  GoTo OuterLoopStart
Halt
```

The inner plan's `Yield` instructions are intercepted at codegen time and replaced with `AppendToRowBuffer` + `GoTo` back to the inner loop — the same technique already used to compile `INSERT INTO ... SELECT`.

### EXPLAIN output

```
DerivedTable AS alias
  [inner plan tree indented]
```

### Key Files

- `src/planner/mod.rs` — `LogicalPlan::DerivedTable`
- `src/planner/select.rs` (or `planner.rs`) — detect `TupleSource::Subquery`, build `DerivedTable` node, derive column names from inner plan
- `src/compiler/nodes.rs` — `codegen_derived_table`
- `src/explain.rs` — render `DerivedTable` in EXPLAIN

### Tests

```rust
// planner test
#[test]
fn plan_from_subquery_produces_derived_table_node() {
    // SELECT name FROM (SELECT id, name FROM users) AS u
    // → Project(DerivedTable(Scan(users)))
}
```

```sql
-- tests/sql/subquery_from.sql
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)
-- > 3 rows inserted

SELECT name FROM (SELECT id, name FROM users WHERE age > 28) AS young ORDER BY name
-- > alice
-- > carol

EXPLAIN SELECT name FROM (SELECT id, name FROM users) AS u
-- > 0, "Project [name:1]"
-- > 1, "  DerivedTable AS u"
-- > 2, "    Scan users [cols: id, name]"
```

### Implementation Steps (3 commits)

#### Step 104.1 — Planner: `LogicalPlan::DerivedTable`; update EXPLAIN

Add the node variant, update `explain.rs`, add a stub compiler arm returning `Err(UnsupportedStatement)`. All tests pass.

**Commit:** `Planner: add DerivedTable plan node; update EXPLAIN`

#### Step 104.2 — Planner: detect `TupleSource::Subquery`, build `DerivedTable`

Remove the `UnsupportedStatement` error from `extract_table_name`. Build the `DerivedTable` node and `DerivedTableResolver`. Add planner unit test.

**Commit:** `Planner: resolve FROM subqueries into DerivedTable nodes`

#### Step 104.3 — Compiler: `codegen_derived_table`; add SQL integration tests

Implement the codegen: inner plan prelude fills RowBuffer, outer iterates via `NextFromRowBuffer`. Add `tests/sql/subquery_from.sql`.

**Commit:** `Compiler: emit RowBuffer prelude for FROM subqueries (derived tables)`

---

## 105. IN operator: literal list + IN (subquery) (Track 4)

### What Changes

`Expression::In` (from item 103) is handled in the planner and compiler.

- **Literal list** (`IN (1, 2, 3)`): the planner desugars to an OR chain of equality expressions. No new VM operations needed.
- **Subquery** (`IN (SELECT ...)`): the compiler emits a RowBuffer prelude (inner query materializes results), then uses a new `RowBufferContains` VM operation with a **lazy HashSet cache** for O(1) membership testing.

### Literal list desugar (planner)

In `convert_expr` in `resolver.rs`:

```rust
Expression::In { expr, source: InSource::Values(values), negated } => {
    // Desugar: expr IN (a, b, c) → (expr = a OR expr = b OR expr = c)
    // negated: expr NOT IN (a, b, c) → (expr != a AND expr != b AND expr != c)
    let equalities: Vec<PlanExpr> = values.iter().map(|v| {
        PlanExpr::BinaryOp {
            op: if negated { BinaryOp::NotEquals } else { BinaryOp::Equals },
            lhs: convert_expr(expr, resolver)?,
            rhs: convert_expr(v, resolver)?,
        }
    }).collect()?;
    // fold with OR (non-negated) or AND (negated)
    Ok(equalities.into_iter().reduce(|acc, e| PlanExpr::BinaryOp {
        op: if negated { BinaryOp::And } else { BinaryOp::Or },
        lhs: Box::new(acc),
        rhs: Box::new(e),
    }).unwrap())
}
```

### Subquery IN: new plan node

```rust
// src/planner/mod.rs
pub enum LogicalPlan {
    // ...
    /// Semi-join filter: retain outer rows whose key column appears in the subquery result set.
    InSubquery {
        input: Box<LogicalPlan>,
        /// The expression to test (usually a column reference from the outer query).
        key_expr: PlanExpr,
        /// The subquery producing the candidate set (single-column output expected).
        subquery: Box<LogicalPlan>,
        negated: bool,
    },
}
```

### New VM operation

```rust
// src/engine/program.rs
/// RowBufferContains(dest, buffer, val):
/// Sets dest = Boolean(true) if any row in buffer has row[0] == val.
/// Builds and caches a HashSet<ScalarValue> on the first call.
RowBufferContains(Reg, Reg, Reg),
```

### RowBuffer lazy cache

```rust
// src/engine/registers.rs
pub struct RowBuffer {
    pub rows: Vec<Vec<ScalarValue>>,
    pub cursor: usize,
    /// Lazily built on first RowBufferContains. Indexes rows[i][0].
    contains_cache: Option<std::collections::HashSet<ScalarValue>>,
}
```

`InitRowBuffer` initialises `contains_cache: None`. `AppendToRowBuffer` sets `contains_cache = None` (invalidate cache if rows are added after it was built — though in practice the prelude always completes before `RowBufferContains` is called).

VM execution of `RowBufferContains(dest, buf_reg, val_reg)`:

```rust
let val = registers.get(val_reg).scalar().cloned();
let buf = registers.get_mut(buf_reg).row_buffer_mut().unwrap();
if buf.contains_cache.is_none() {
    buf.contains_cache = Some(
        buf.rows.iter().filter_map(|r| r.first().cloned()).collect()
    );
}
let found = match &val {
    Some(v) => buf.contains_cache.as_ref().unwrap().contains(v),
    None => false,  // NULL IN (...) → false
};
*registers.get_mut(dest) = RegisterValue::ScalarValue(ScalarValue::Boolean(found));
```

### Compiler output for IN (subquery)

`codegen_in_subquery` in `nodes.rs`:

```
// Prelude: materialize inner plan into r_set
InitRowBuffer         r_set
[inner plan, Yield → AppendToRowBuffer(r_set, ...)]

// Outer loop (from outer plan codegen):
[outer scan / NextFromRowBuffer loop]

  // For each outer row, evaluate key_expr → r_key
  [key_expr codegen → r_key]
  RowBufferContains     r_match, r_set, r_key
  // If negated: invert r_match with NotValue
  GoToIfFalse           r_match → NextOuterRow

  Yield [...]
```

### EXPLAIN output

```
InSubquery [NOT] key_expr
  [outer plan]
  [subquery plan]
```

### Key Files

- `src/planner/mod.rs` — `LogicalPlan::InSubquery`
- `src/planner/resolver.rs` — desugar `InSource::Values`; convert `InSource::Subquery` to `InSubquery` node
- `src/engine/registers.rs` — `contains_cache` field on `RowBuffer`; update `InitRowBuffer`/`AppendToRowBuffer` execution
- `src/engine/program.rs` — `RowBufferContains` operation + Display
- `src/engine/mod.rs` — execute `RowBufferContains`
- `src/compiler/nodes.rs` — `codegen_in_subquery`
- `src/explain.rs` — render `InSubquery`

### Tests

```rust
#[test]
fn plan_in_literal_desugars_to_or_chain() {
    // WHERE id IN (1, 2, 3) → Filter(OR(id=1, OR(id=2, id=3)))
    // No InSubquery node in plan
}

#[test]
fn plan_in_subquery_produces_in_subquery_node() {
    // WHERE id IN (SELECT user_id FROM admins) → InSubquery(Scan(users), Scan(admins))
}
```

```sql
-- tests/sql/subquery_in.sql
CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE admins (user_id INTEGER)
-- > Table 'admins' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')
-- > 3 rows inserted
INSERT INTO admins VALUES (1), (3)
-- > 2 rows inserted

SELECT name FROM users WHERE id IN (1, 3) ORDER BY name
-- > alice
-- > carol

SELECT name FROM users WHERE id NOT IN (1, 3) ORDER BY name
-- > bob

SELECT name FROM users WHERE id IN (SELECT user_id FROM admins) ORDER BY name
-- > alice
-- > carol

SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins) ORDER BY name
-- > bob

-- NULL IN: should return no rows (NULL comparisons are false)
SELECT name FROM users WHERE id IN (SELECT user_id FROM admins WHERE user_id > 100)
-- > (no rows)
```

### Implementation Steps (4 commits)

#### Step 105.1 — Planner: desugar `IN (literals)` to OR chain; add SQL tests for literal IN

No new VM ops. Add `tests/sql/subquery_in.sql` with literal IN cases only.

**Commit:** `Planner: desugar IN (literal list) to OR equality chain`

#### Step 105.2 — Planner: `LogicalPlan::InSubquery`; update EXPLAIN; stub compiler arm

**Commit:** `Planner: add InSubquery plan node; update EXPLAIN`

#### Step 105.3 — VM: add `RowBufferContains` + lazy HashSet cache in `RowBuffer`

Add the cache field, update `InitRowBuffer`/`AppendToRowBuffer` execution, implement `RowBufferContains`.

**Commit:** `VM: add RowBufferContains with lazy HashSet cache`

#### Step 105.4 — Compiler: `codegen_in_subquery`; complete SQL integration tests

Add the subquery IN cases to `tests/sql/subquery_in.sql`.

**Commit:** `Compiler: emit RowBuffer prelude + RowBufferContains for IN (subquery)`

---

## 106. Scalar subqueries in SELECT list and WHERE (Track 4)

### What Changes

`Expression::ScalarSubquery` (from item 103) is handled in the planner and compiler. A scalar subquery compiles as a RowBuffer prelude (same as the others) that captures exactly one value. The outer query reads it once with `NextFromRowBuffer` and holds the result in a scalar register for the lifetime of the outer query.

### Background

A scalar subquery must return at most one row and one column. At runtime:
1. A prelude loop runs the inner query, appending at most one row to a RowBuffer.
2. After the prelude, `NextFromRowBuffer` reads the single value into a register.
3. That register is used as a constant throughout the outer query.

If the inner query returns zero rows, the register holds `NULL`. If it returns more than one row, a runtime error is raised (`ScalarSubqueryReturnedMultipleRows`).

This is clean because the scalar value is computed once and stored in a register — the outer query treats it exactly like a `StoreValue` constant.

### New plan node

```rust
// src/planner/mod.rs
pub enum PlanExpr {
    // ... existing variants ...

    /// (SELECT expr FROM ...) — evaluated once; used as a scalar constant.
    ScalarSubquery {
        plan: Box<LogicalPlan>,
    },
}
```

The planner converts `Expression::ScalarSubquery(stmt)` in `convert_expr`:

```rust
Expression::ScalarSubquery(stmt) => {
    let inner_plan = plan_select(*stmt, btree)?;
    // Validate: inner plan must produce exactly 1 output column
    if output_width(&inner_plan) != 1 {
        return Err(PlanError::ScalarSubqueryMustReturnOneColumn);
    }
    Ok(PlanExpr::ScalarSubquery { plan: Box::new(inner_plan) })
}
```

### Compiler output

In `codegen_expr`, when a `PlanExpr::ScalarSubquery` is encountered, the compiler emits a **prelude** into the init section (before the outer loop) and returns the register holding the captured value:

```
// In program init section:
InitRowBuffer            r_buf_scalar

[inner plan, Yield → AppendToRowBuffer(r_buf_scalar, [yielded_col])]

// After inner plan:
NextFromRowBuffer         [r_scalar], r_buf_scalar → ScalarNull
GoTo                      AfterScalarPrelude
ScalarNull:
  StoreValue              r_scalar, Null
AfterScalarPrelude:
```

`r_scalar` is then used wherever the scalar subquery expression appears in the outer query — in `Project` expressions, `Filter` expressions, etc.

> **Multiple rows check**: if the inner plan could yield more than one row, the compiler inserts a guard:
> after the first `AppendToRowBuffer`, emit `MoveCursor inner_cursor Next; GoToIf EOF → done; Halt(Err(ScalarSubqueryMultipleRows))`.
> In practice this is a code-size concern; for the initial implementation a simpler approach is to just take the first row and ignore the rest (document as "undefined for multi-row scalar subqueries", matching SQLite's behaviour of returning the first row with a warning).

### EXPLAIN output

Scalar subqueries appear inline in the expression display:

```
0, "Project [(ScalarSubquery):0, name:1]"
1, "  Scan users [cols: id, name]"
```

And EXPLAIN for the scalar subquery's inner plan is shown as a sub-tree.

### New error variant

```rust
// src/planner/mod.rs or error module
PlanError::ScalarSubqueryMustReturnOneColumn,
```

### Key Files

- `src/planner/mod.rs` — `PlanExpr::ScalarSubquery`; `PlanError::ScalarSubqueryMustReturnOneColumn`
- `src/planner/resolver.rs` — `convert_expr` arm for `Expression::ScalarSubquery`
- `src/compiler/nodes.rs` — emit scalar prelude from `codegen_expr`
- `src/explain.rs` — render `PlanExpr::ScalarSubquery`

### Tests

```rust
#[test]
fn plan_scalar_subquery_in_select() {
    // SELECT (SELECT COUNT(*) FROM orders), name FROM users
    // → Project([ScalarSubquery(Count(Scan(orders))), col:1], Scan(users))
}

#[test]
fn plan_scalar_subquery_multi_column_error() {
    // SELECT (SELECT id, name FROM users LIMIT 1) → PlanError
}
```

```sql
-- tests/sql/subquery_scalar.sql
CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE orders (id INTEGER, user_id INTEGER)
-- > Table 'orders' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob')
-- > 2 rows inserted
INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)
-- > 3 rows inserted

-- Scalar subquery in SELECT list
SELECT name, (SELECT COUNT(*) FROM orders) AS total_orders FROM users ORDER BY name
-- > alice | 3
-- > bob | 3

-- Scalar subquery in WHERE
SELECT name FROM users WHERE id = (SELECT MAX(user_id) FROM orders)
-- > bob

-- Empty subquery yields NULL
SELECT (SELECT id FROM users WHERE id = 999)
-- > NULL

-- Error: scalar subquery returns multiple columns
SELECT (SELECT id, name FROM users LIMIT 1)
-- > ERROR: scalar subquery must return exactly one column
```

### Implementation Steps (3 commits)

#### Step 106.1 — Planner: `PlanExpr::ScalarSubquery`; update EXPLAIN; add error variant

**Commit:** `Planner: add PlanExpr::ScalarSubquery; validate single-column output`

#### Step 106.2 — Compiler: emit scalar prelude into init section

Implement `codegen_expr` arm for `PlanExpr::ScalarSubquery`. The scalar prelude is emitted into the init section; `r_scalar` is returned as the register for the expression.

**Commit:** `Compiler: emit scalar subquery prelude; capture result in scalar register`

#### Step 106.3 — SQL integration tests: `tests/sql/subquery_scalar.sql`

**Commit:** `Tests: add scalar subquery SQL integration tests`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `SELECT name FROM users WHERE id IN (1, 3)` returns correct rows
- [ ] `SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)` returns correct rows
- [ ] `SELECT name FROM (SELECT name FROM users WHERE age > 28) AS young` returns correct rows
- [ ] `SELECT (SELECT COUNT(*) FROM orders) FROM users` returns the count for every user row
- [ ] `EXPLAIN` shows `DerivedTable`, `InSubquery`, `ScalarSubquery` nodes
- [ ] `RowBufferContains` with 0-row buffer returns false (not a crash)
- [ ] `RowBufferContains` lazy cache: correct results on first and subsequent calls
- [ ] Scalar subquery returning 0 rows yields NULL (not a crash)
- [ ] Each commit is independently testable
