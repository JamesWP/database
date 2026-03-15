# Phase AB — Non-Correlated Subqueries

Add non-correlated subquery support using a minimal set of reusable primitives: a single
`Materialize` plan node replaces the proposed `DerivedTable` node, `JoinStrategy::Semi`
replaces the proposed `InSubquery` node, and scalar subqueries compile as a prelude using
the same `Materialize` infrastructure.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 103 | 1 | AST + parser: `Expression::In`, `Expression::ScalarSubquery` | — |
| 104 | 4 | `LogicalPlan::Materialize` node + FROM subquery planner + compiler | 103 |
| 105 | 4 | `PlanExpr::In` + `JoinStrategy::Semi` — literal IN and IN (subquery) | 103, 104 |
| 106 | 4 | `PlanExpr::ScalarSubquery` — scalar subqueries in SELECT list and WHERE | 103, 104 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Why non-correlated first?

Non-correlated subqueries — those that do not reference outer-query columns — can be fully
evaluated once as a prelude before the outer query begins. This maps cleanly onto the
existing flat-bytecode model: the compiler emits an inner loop that fills a `RowBuffer`,
followed by the outer query that reads from it. No new control-flow primitives or register
types are needed.

Correlated subqueries (referencing outer row values, e.g. `EXISTS`) require re-executing
the inner loop per outer row; that is left for a later phase.

### Design principle: one new node, not three

The original plan for this phase proposed three new plan-level constructs: `DerivedTable`,
`InSubquery`, and `PlanExpr::ScalarSubquery`. Two of those can be eliminated by observing:

1. **`DerivedTable` → `Materialize`**: The alias (`AS u`) is a *resolver concern* during
   planning — it maps column names to indices. By the time the plan is built, all column
   references are resolved to `ColumnRef(i)` indices. The alias is consumed and discarded;
   it never needs to appear in the plan tree. `Materialize` is a pure buffering primitive
   with no alias field.

2. **`InSubquery` → `JoinStrategy::Semi`**: An IN (subquery) filter is structurally a
   semi-join: for each outer row, check whether the row's key value appears in the inner
   result set. `codegen_join` (Hash strategy) already materializes the right side into a
   `RowBuffer` and iterates it per left row. `Semi` reuses this infrastructure, short-
   circuits after the first match, and yields only left columns. Only ~10 lines of
   `codegen_join` change.

`PlanExpr::ScalarSubquery` is unavoidable — it is an expression that produces a value, not
a row stream — but it compiles using the same `Materialize` buffering primitive.

### Single flat program model

Every subquery compiles into the **same** `CompiledProgram` as the outer query. The inner
query becomes a **prelude section** — a loop that runs to completion before the outer
query's cursor is opened. Results are held in a `RowBuffer` register and consumed by the
outer query.

```
[inner prelude loop → fills RowBuffer r_buf]
RewindRowBuffer r_buf         ; reset read cursor for outer
[outer query loop → reads from r_buf or checks membership]
Halt
```

---

## 103. AST + parser: `Expression::In` and `Expression::ScalarSubquery` (Track 1)

### What Changes

Two new `Expression` variants are added to `src/frontend/ast.rs`. The parser is extended to
recognise `IN (...)`, `NOT IN (...)`, and `(SELECT ...)` in expression position.

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

`get_column_references` on `Expression` gains arms for the new variants (returning `vec![]`
for `ScalarSubquery`; delegating to `expr` for `In`).

### Parser changes

**IN / NOT IN** (`src/frontend/parser.rs`):

`IN` is a postfix operator parsed after the primary expression in `parse_expression`
(similar to how `IS NULL` / `IS NOT NULL` are handled). After parsing a left-hand
expression, peek for `IN` or `NOT IN`:

```rust
if self.input.peek() == lexer::Type::In
    || (self.input.peek() == lexer::Type::Not && next_is_in) {
    let negated = /* consume NOT if present */;
    self.input.expect(Expect::Token(lexer::Type::In))?;
    self.input.expect(Expect::LeftParen)?;

    if self.input.peek() == lexer::Type::Select {
        let subquery = self.parse_select_statement()?;
        self.input.expect(Expect::RightParen)?;
        return Ok(Expression::In {
            expr: Box::new(lhs),
            source: InSource::Subquery(Box::new(subquery)),
            negated,
        });
    } else {
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

**Scalar subquery** — in the primary expression parser, when `(` is seen, peek ahead: if
`SELECT` follows, parse as a subquery rather than a grouped expression:

```rust
lexer::Type::LeftParen => {
    self.input.advance();
    if self.input.peek() == lexer::Type::Select {
        let stmt = self.parse_select_statement()?;
        self.input.expect(Expect::RightParen)?;
        Ok(Expression::ScalarSubquery(Box::new(stmt)))
    } else {
        let expr = self.parse_expression()?;
        self.input.expect(Expect::RightParen)?;
        Ok(expr)
    }
}
```

**Lexer**: add `In` token type if not already present. (`Not` exists for `IS NOT NULL`.)

### Key Files

- `src/frontend/ast.rs` — new enum variants
- `src/frontend/parser.rs` — `IN`/`NOT IN`/scalar subquery parsing
- `src/frontend/lexer.rs` — `In` keyword token (if missing)

### Tests

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

Add the enum variants. Update `get_column_references`. No parser changes yet — the new
variants are unreachable. All tests pass.

**Commit:** `AST: add Expression::In and Expression::ScalarSubquery variants`

#### Step 103.2 — Parser: IN / NOT IN / scalar subquery

Add `In` lexer token. Extend the expression parser. Add parser unit tests.

**Commit:** `Parser: add IN, NOT IN, and scalar subquery expression parsing`

---

## 104. `LogicalPlan::Materialize` + FROM subquery support (Track 4)

### What Changes

A new `Materialize` plan node buffers all rows from its child into a `RowBuffer`, then
yields them. This is the single primitive underlying FROM subqueries, the right side of
semi-joins, and scalar subquery preludes.

`FROM (SELECT ...) AS alias` is already parsed correctly into `TupleSource::Subquery`.
The planner currently returns `UnsupportedStatement` for this case. This item removes
that error: the inner query is planned recursively, wrapped in `Materialize`, and the outer
query iterates the buffer via `NextFromRowBuffer`.

### Why no alias field on `Materialize`

The alias (`AS u`) exists only to resolve column names during planning. When the planner
processes `SELECT u.name FROM (SELECT id, name FROM users) AS u`, it builds a
`MaterializeResolver` that maps `u.name` → `ColumnRef(1)`. By the time the `Materialize`
node is emitted, all column references in the outer query's expressions are already
resolved to integer indices. The alias is consumed and discarded — it is never needed again
at codegen or execution time.

Contrast with `DerivedTable { alias, columns }` from the original proposal: those fields
would be plan-tree weight carrying information the compiler never uses.

### New plan node

```rust
// src/planner/mod.rs
pub enum LogicalPlan {
    // ... existing variants ...

    /// Buffer all rows from `input` into a RowBuffer, then yield them.
    /// Used for: FROM subqueries, right side of semi-joins, scalar subquery preludes.
    /// No alias field — alias is a resolver concern consumed during planning.
    Materialize {
        input: Box<LogicalPlan>,
    },
}
```

### Column resolution

A new `MaterializeResolver` implements `ColumnResolver`, mapping column names and
`alias.col_name` references to positional indices:

```rust
struct MaterializeResolver<'a> {
    alias: &'a str,       // consumed during planning, not stored in the node
    columns: &'a [String], // output column names of the inner plan, in order
}
```

`resolve_identifier("name")` → index of `"name"` in `columns`.
`resolve_qualified("u", "name")` → same, with alias prefix checked and stripped.

This resolver is used only while planning the outer query's expressions. Once column
indices are resolved, the resolver is dropped.

### Compiler output

`codegen_materialize` in `src/compiler/nodes.rs` emits the same prelude pattern already
used by Sort and Distinct — fill a buffer, rewind, yield from it:

```
// INIT section:
InitRowBuffer          r_buf

// BODY — prelude: run inner plan, collect rows
[inner plan bytecode, Yield → AppendToRowBuffer(r_buf, yielded_regs)]

// After inner plan completes:
RewindRowBuffer        r_buf

// Outer iteration (driven by parent node):
OuterLoop:
  NextFromRowBuffer    [r0, r1, ...], r_buf → on_done
  GoTo                 on_tuple
```

The inner plan's `Yield` instructions are intercepted at codegen time and redirected to
`AppendToRowBuffer` — the same technique used for `INSERT INTO ... SELECT` and the Hash
join's right-side materialization.

### EXPLAIN output

```
0, "Materialize"
1, "  Scan users [cols: id, name]"
```

### Key Files

- `src/planner/mod.rs` — `LogicalPlan::Materialize`
- `src/planner/select.rs` — detect `TupleSource::Subquery`, build `Materialize` node,
  construct `MaterializeResolver` for outer column resolution
- `src/compiler/nodes.rs` — `codegen_materialize`
- `src/explain.rs` — render `Materialize`

### Tests

```rust
#[test]
fn plan_from_subquery_produces_materialize_node() {
    // SELECT name FROM (SELECT id, name FROM users) AS u
    // → Project(Materialize(Scan(users)))
    // Note: no alias field in Materialize
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

SELECT name FROM (SELECT id, name FROM users) AS u WHERE u.id > 1 ORDER BY name
-- > bob
-- > carol

EXPLAIN SELECT name FROM (SELECT id, name FROM users) AS u
-- > 0, "Project [name:1]"
-- > 1, "  Materialize"
-- > 2, "    Scan users [cols: id, name]"
```

### Implementation Steps (3 commits)

#### Step 104.1 — Planner: `LogicalPlan::Materialize`; update EXPLAIN; stub compiler arm

Add the node variant, update `explain.rs`, add a stub compiler arm returning
`Err(UnsupportedStatement)`. All tests pass.

**Commit:** `Planner: add Materialize plan node; update EXPLAIN`

#### Step 104.2 — Planner: detect `TupleSource::Subquery`, build `Materialize`

Remove the `UnsupportedStatement` error from `extract_table_name`. Plan the inner query
recursively, wrap in `Materialize`. Build `MaterializeResolver` for outer column
resolution. Add planner unit test.

**Commit:** `Planner: resolve FROM subqueries into Materialize nodes`

#### Step 104.3 — Compiler: `codegen_materialize`; add SQL integration tests

Implement the codegen. Add `tests/sql/subquery_from.sql`.

**Commit:** `Compiler: codegen_materialize — buffer inner plan into RowBuffer`

---

## 105. `PlanExpr::In` + `JoinStrategy::Semi` (Track 4)

### What Changes

Two additions handle the `IN` operator:

- **Literal list** (`IN (1, 2, 3)`): a new `PlanExpr::In` variant keeps the plan clean
  (avoids polluting EXPLAIN with deep OR chains). The compiler desugars it to an equality
  OR chain at codegen time; a later optimization can emit a HashSet probe.
- **Subquery** (`IN (SELECT ...)`): a new `JoinStrategy::Semi` variant extends the
  existing `Join` node. No new plan node is needed — the infrastructure from `codegen_join`
  is reused almost verbatim.

### Why `JoinStrategy::Semi` instead of a new `InSubquery` node

`IN (subquery)` is semantically a semi-join: retain outer rows whose key value appears in
the inner result set. Structurally it is almost identical to `JoinStrategy::Hash`:

| Step | Hash join | Semi join |
|------|-----------|-----------|
| Prelude | Materialize right → RowBuffer | Materialize right → RowBuffer |
| Per left row | Rewind + iterate buffer, eval `on_condition` | `RowBufferContains` with lazy HashSet |
| Yield | Left + right columns | Left columns only (on match) |

The delta from `codegen_join` is about 10 lines. Expressing this as a new `JoinStrategy`
variant is both the most economical and the most semantically accurate choice.

`NOT IN` maps to `Semi { negated: true }` — the codegen inverts the `RowBufferContains`
result before the conditional jump.

### New `PlanExpr` variant

```rust
// src/planner/mod.rs
pub enum PlanExpr {
    // ... existing variants ...

    /// expr IN (val, val, ...) — kept as a plan node for clean EXPLAIN output.
    /// The compiler desugars to an OR equality chain at codegen time.
    In {
        expr: Box<PlanExpr>,
        values: Vec<PlanExpr>,
        negated: bool,
    },
}
```

The planner converts `Expression::In { source: InSource::Values(vals), negated }` by
calling `convert_expr` on each value and collecting into `PlanExpr::In`.

### New `JoinStrategy` variant

```rust
// src/planner/mod.rs
pub enum JoinStrategy {
    Hash,
    NestedLoop,
    /// Semi-join: retain left rows whose key appears in the (materialised) right set.
    /// `negated: true` → anti-semi-join (NOT IN).
    Semi { negated: bool },
}
```

The `Join` node's existing `on_condition` field carries the semi-join key expression:
for `WHERE id IN (SELECT user_id FROM admins)`, the planner sets
`on_condition = PlanExpr::ColumnRef(left_id_idx)`. The codegen reads this as the value
to probe via `RowBufferContains`.

The planner converts `Expression::In { source: InSource::Subquery(stmt), negated }` as:

```rust
let inner_plan = plan_select(*stmt, btree)?;
let key_expr = convert_expr(expr, resolver)?;
LogicalPlan::Join {
    left: outer_plan,
    right: Box::new(inner_plan),
    on_condition: key_expr,    // left-side key, probed in right buffer
    strategy: JoinStrategy::Semi { negated },
    left_column_count: outer_column_count,
}
```

### New VM operation

```rust
// src/engine/program.rs
/// RowBufferContains(dest, buffer, val):
/// Sets dest = Boolean(true) if any row in buffer has row[0] == val.
/// Builds and caches a HashSet<ScalarValue> on the first call (lazy).
RowBufferContains(Reg, Reg, Reg),
```

### RowBuffer lazy HashSet cache

```rust
// src/engine/registers.rs
pub struct RowBuffer {
    pub rows: Vec<Vec<ScalarValue>>,
    pub cursor: usize,
    /// Lazily built on first RowBufferContains call. Indexes rows[i][0].
    contains_cache: Option<std::collections::HashSet<ScalarValue>>,
}
```

`InitRowBuffer` sets `contains_cache: None`. `AppendToRowBuffer` clears the cache
(invalidate on write — in practice the prelude always completes before any
`RowBufferContains` call, so this is a safety guard, not a hot path).

### Compiler output for `Semi` join

`codegen_join_semi` in `nodes.rs` (called from the `Join` match arm):

```
// INIT: materialize right side (identical to Hash join prelude)
InitRowBuffer           r_set
[right plan, Yield → AppendToRowBuffer(r_set, right_regs)]

// Outer loop:
OuterLoop:
  [left child drives outer iteration]

  LEFT_ON_TUPLE:
    [eval on_condition (key_expr from left row) → r_key]
    RowBufferContains   r_match, r_set, r_key
    // if negated: NotValue r_match
    GoToIfFalse         r_match → OuterLoop
    Yield               [left_regs...]
    GoTo                OuterLoop
Halt
```

### EXPLAIN output

```
0, "Project [name:1]"
1, "  Join [Semi] on id"
2, "    Scan users [cols: id, name]"
3, "    Scan admins [cols: user_id]"
```

### Key Files

- `src/planner/mod.rs` — `PlanExpr::In`; `JoinStrategy::Semi { negated }`
- `src/planner/resolver.rs` — `convert_expr` arms for `InSource::Values` and
  `InSource::Subquery`
- `src/engine/registers.rs` — `contains_cache` on `RowBuffer`; update
  `InitRowBuffer`/`AppendToRowBuffer` execution
- `src/engine/program.rs` — `RowBufferContains` operation + Display
- `src/engine/mod.rs` — execute `RowBufferContains`
- `src/compiler/nodes.rs` — `codegen_join_semi`; desugar `PlanExpr::In` in `codegen_expr`
- `src/explain.rs` — render `PlanExpr::In`; render `Join [Semi]`

### Tests

```rust
#[test]
fn plan_in_literal_produces_plan_expr_in() {
    // WHERE id IN (1, 2, 3) → Filter(PlanExpr::In { values: [1,2,3], negated: false })
    // No Semi join in plan
}

#[test]
fn plan_in_subquery_produces_semi_join() {
    // WHERE id IN (SELECT user_id FROM admins)
    // → Join { strategy: Semi { negated: false }, on_condition: ColumnRef(0), ... }
}

#[test]
fn plan_not_in_subquery_produces_anti_semi_join() {
    // WHERE id NOT IN (SELECT user_id FROM admins)
    // → Join { strategy: Semi { negated: true }, ... }
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

-- Empty right side: no rows match (NULL-safe: empty set → false)
SELECT name FROM users WHERE id IN (SELECT user_id FROM admins WHERE user_id > 100)
-- > (no rows)

EXPLAIN SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)
-- > 0, "Project [name:1]"
-- > 1, "  Join [Semi] on id"
-- > 2, "    Scan users [cols: id, name]"
-- > 3, "    Scan admins [cols: user_id]"
```

### Implementation Steps (4 commits)

#### Step 105.1 — Planner: `PlanExpr::In`; desugar literal IN in resolver; literal IN SQL tests

No new VM ops. Update `explain.rs` for `PlanExpr::In`. Add `tests/sql/subquery_in.sql`
with literal IN cases only.

**Commit:** `Planner: add PlanExpr::In; compile literal IN list`

#### Step 105.2 — Planner: `JoinStrategy::Semi { negated }`; update EXPLAIN; stub codegen

Add the variant, extend the `Join` match arm in the compiler with
`Err(UnsupportedStatement)`, update `explain.rs` to show `[Semi]` in join display. Add
planner unit tests.

**Commit:** `Planner: add JoinStrategy::Semi for IN (subquery)`

#### Step 105.3 — VM: `RowBufferContains` + lazy HashSet cache

Add `contains_cache` to `RowBuffer`. Update `InitRowBuffer`/`AppendToRowBuffer` execution.
Implement and test `RowBufferContains`.

**Commit:** `VM: add RowBufferContains with lazy HashSet cache`

#### Step 105.4 — Compiler: `codegen_join_semi`; complete SQL integration tests

Implement `codegen_join_semi`. Add the subquery IN cases to `tests/sql/subquery_in.sql`.

**Commit:** `Compiler: codegen_join_semi — semi-join using RowBufferContains`

---

## 106. Scalar subqueries in SELECT list and WHERE (Track 4)

### What Changes

`Expression::ScalarSubquery` (from item 103) is handled in the planner and compiler. A
scalar subquery compiles as a `Materialize` prelude that captures exactly one value. The
outer query reads it once with `NextFromRowBuffer` and holds the result in a scalar
register for the lifetime of the outer query.

### Background

A scalar subquery must return at most one row and one column. At runtime:
1. A prelude loop runs the inner query, appending at most one row to a RowBuffer.
2. After the prelude, `NextFromRowBuffer` reads the single value into a register.
3. That register is used as a constant throughout the outer query.

If the inner query returns zero rows, the register holds `NULL`. If it returns more than
one row, the first row is used (matching SQLite behaviour — document as undefined for
multi-row scalar subqueries; strict checking can be added later).

The scalar prelude is the same `Materialize` buffering infrastructure from item 104,
specialised to extract a single value after the prelude loop completes.

### New `PlanExpr` variant

```rust
// src/planner/mod.rs
pub enum PlanExpr {
    // ... existing variants ...

    /// (SELECT expr FROM ...) — evaluated once before the outer query begins.
    /// The inner plan must produce exactly one output column.
    ScalarSubquery {
        plan: Box<LogicalPlan>,
    },
}
```

The planner converts `Expression::ScalarSubquery(stmt)` in `convert_expr`:

```rust
Expression::ScalarSubquery(stmt) => {
    let inner_plan = plan_select(*stmt, btree)?;
    if output_width(&inner_plan) != 1 {
        return Err(PlanError::ScalarSubqueryMustReturnOneColumn);
    }
    Ok(PlanExpr::ScalarSubquery { plan: Box::new(inner_plan) })
}
```

`output_width` is a helper that returns the number of output columns of a `LogicalPlan`
(already needed elsewhere — add to `mod.rs` if not present).

### Compiler output

In `codegen_expr`, when a `PlanExpr::ScalarSubquery` is encountered, the compiler emits a
prelude into the init section and returns the scalar register:

```
// In program init section:
InitRowBuffer            r_buf_scalar
[inner plan, Yield → AppendToRowBuffer(r_buf_scalar, [yielded_col])]

// After inner plan (single-value extraction):
NextFromRowBuffer         [r_scalar], r_buf_scalar → ScalarNull
GoTo                      AfterScalarPrelude
ScalarNull:
  StoreValue              r_scalar, Null
AfterScalarPrelude:
```

`r_scalar` is then used wherever the scalar subquery expression appears in the outer query
— in `Project` expressions, `Filter` expressions, etc. — as though it were a
`StoreValue` constant.

### New error variant

```rust
// src/planner/mod.rs
PlanError::ScalarSubqueryMustReturnOneColumn,
```

### EXPLAIN output

```
0, "Project [(ScalarSubquery), name:1]"
1, "  Scan users [cols: id, name]"
```

The subquery inner plan appears as a sub-tree indented below.

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
    // → Project([ScalarSubquery(...), ColumnRef(1)], Scan(users))
}

#[test]
fn plan_scalar_subquery_multi_column_error() {
    // SELECT (SELECT id, name FROM users LIMIT 1) → PlanError::ScalarSubqueryMustReturnOneColumn
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

-- Scalar subquery in SELECT list — same value for every row
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

#### Step 106.1 — Planner: `PlanExpr::ScalarSubquery`; error variant; update EXPLAIN

Add the variant. Add `PlanError::ScalarSubqueryMustReturnOneColumn`. Implement
`convert_expr` arm. Update `explain.rs`. Add planner unit tests.

**Commit:** `Planner: add PlanExpr::ScalarSubquery; validate single-column output`

#### Step 106.2 — Compiler: emit scalar prelude into init section

Implement `codegen_expr` arm for `PlanExpr::ScalarSubquery`. The prelude is emitted into
the init section; `r_scalar` is returned as the expression's register.

**Commit:** `Compiler: emit scalar subquery prelude; capture result in scalar register`

#### Step 106.3 — SQL integration tests: `tests/sql/subquery_scalar.sql`

**Commit:** `Tests: add scalar subquery SQL integration tests`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `SELECT name FROM users WHERE id IN (1, 3)` — correct rows, EXPLAIN shows `PlanExpr::In`
- [ ] `SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)` — correct rows,
      EXPLAIN shows `Join [Semi]`
- [ ] `SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins)` — correct rows
- [ ] `SELECT name FROM (SELECT name FROM users WHERE age > 28) AS young` — correct rows,
      EXPLAIN shows `Materialize` (no `DerivedTable`)
- [ ] `SELECT (SELECT COUNT(*) FROM orders) FROM users` — count for every user row
- [ ] `RowBufferContains` with 0-row buffer → false (not a crash)
- [ ] `RowBufferContains` lazy cache — correct on first and subsequent calls
- [ ] Scalar subquery returning 0 rows → NULL (not a crash)
- [ ] Each commit is independently testable
