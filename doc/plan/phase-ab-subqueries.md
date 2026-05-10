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
existing flat-bytecode model: the compiler emits an inner loop that fills a `RowBuffer` in
the **init section** (which runs exactly once before the body begins), and the outer query
reads from it in the body. No new control-flow primitives or register types are needed.

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
   result set. The planner wraps the inner plan in `Materialize` and emits
   `Join { strategy: Semi, right: Materialize { inner } }`. The codegen reuses
   `Materialize`'s buffer infrastructure and iterates the right side with early exit on
   match, yielding only left columns.

`PlanExpr::ScalarSubquery` is unavoidable — it is an expression that produces a value, not
a row stream — but it compiles using the same fill-in-init pattern as `Materialize`.

### Materialize fill goes in the INIT section

`CodegenContext::finalize()` produces: `[init_ops] GoTo(body_start) [body_ops]`. The init
section runs exactly once before the body loop begins. The `BytecodeEmitter` used for init
supports arbitrary operations including loops and conditional jumps — there is no
restriction to simple sequential setup code.

`codegen_materialize` exploits this: the inner plan's scan and `AppendToRowBuffer` loop are
emitted into the **init section**, so the buffer is fully populated before the body starts.
The body section contains only the yield loop. This has two consequences:

- **`NodeOutput.reset` is meaningful**: Materialize sets `reset = Some(reset_label)` where
  `reset_label` emits `RewindRowBuffer` then falls into the yield loop. Any parent node can
  rewind and re-iterate the buffer without re-running the fill.

- **`Join { right: Materialize { ... } }` works without extra interface machinery**: the
  semi-join codegen compiles the left child first (body starts at the left scan), then
  compiles the right child (Materialize's yield loop appears later in body). The buffer is
  already full when the body starts. Per left row, the join calls `right_output.reset` to
  rewind and iterate — the inner loop exits on first match (semi) or on exhaustion without
  match (anti-semi). No `RowBufferContains` instruction, no `buffer_reg` in `NodeOutput`,
  no `on_fill_done` continuation field is needed.

### Single flat program model

Every subquery compiles into the **same** `CompiledProgram` as the outer query.

```
INIT:
  [inner plan cursor setup + fill loop → fills RowBuffer r_buf]
  RewindRowBuffer r_buf
  GoTo body_start

BODY:
  reset_label:
    RewindRowBuffer r_buf       ; re-entry point for per-row rewind
  yield_next:
    NextFromRowBuffer [r0, r1, ...], r_buf → on_done
    GoTo on_tuple
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
yields them. This is the single buffering primitive underlying FROM subqueries, the right
side of semi-joins, and scalar subquery preludes.

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
    alias: &'a str,        // consumed during planning, not stored in the node
    columns: &'a [String], // output column names of the inner plan, in order
}
```

`resolve_identifier("name")` → index of `"name"` in `columns`.
`resolve_qualified("u", "name")` → same, with alias prefix checked and stripped.

This resolver is used only while planning the outer query's expressions. Once column
indices are resolved, the resolver is dropped.

### Compiler output

`codegen_materialize` splits across the two emitters:

**Fill phase — INIT section** (runs exactly once before the body):

```
InitRowBuffer          r_buf

[inner plan INIT: Open(cursor), MoveCursor(First)]

FILL_CHECK:
  CanReadCursor        flag, cursor
  GoToIfFalse          FILL_DONE, flag
  ReadCursor           [row_regs], cursor
  MoveCursor           cursor, Next
  AppendToRowBuffer    r_buf, [row_regs]
  GoTo                 FILL_CHECK
FILL_DONE:
  RewindRowBuffer      r_buf
  ; falls through to GoTo(body_start) added by finalize()
```

**Yield loop — BODY section** (iterated by parent):

```
reset_label:             ← NodeOutput.reset
  RewindRowBuffer      r_buf
yield_next:
  NextFromRowBuffer    [output_regs], r_buf → cont.on_done
  GoTo                 cont.on_tuple
mat_next:                ← NodeOutput.next
  GoTo                 yield_next
```

`NodeOutput` returned by `codegen_materialize`:

```rust
NodeOutput {
    next: mat_next,          // call to get the next buffered row
    reset: Some(reset_label), // rewinds buffer and restarts from first row
    output_regs,
}
```

`reset` is what enables a parent join to call `right_output.reset` per left row to rewind
and re-iterate the right buffer without re-running the fill.

**Important — labels are local to their emitter.** The init emitter and body emitter are
separate `BytecodeEmitter` instances. Labels in the fill loop (`FILL_CHECK`, `FILL_DONE`)
are init-local. Labels in the yield loop (`reset_label`, `yield_next`, `mat_next`) are
body-local. `finalize()` concatenates them with the correct offset applied to body labels.
No cross-section label references are needed: the fill loop falls through to the
`GoTo(body_start)` that `finalize()` inserts automatically.

**Note on inner plan body code.** Calling `codegen(inner_plan, child_cont, ctx)` emits the
inner plan's body (e.g., Scan check/read/advance loop) into `ctx.body_emitter`. This body
code drives the fill loop: child_cont directs each yielded row to `AppendToRowBuffer` and
signals fill completion to `FILL_DONE`. The inner plan body code is only reachable during
the fill phase (via jumps from the fill loop in init); it is not re-entered afterwards.

### EXPLAIN output

```
0, "Materialize"
1, "  Scan users [cols: id, name]"
```

### Key Files

- `src/planner/mod.rs` — `LogicalPlan::Materialize`
- `src/planner/select.rs` — detect `TupleSource::Subquery`, build `Materialize` node,
  construct `MaterializeResolver` for outer column resolution
- `src/compiler/nodes.rs` — `codegen_materialize`; fill loop to `init_emitter`,
  yield loop to `body_emitter`; set `reset` in `NodeOutput`
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

Implement `codegen_materialize` with fill in the init section and yield loop in the body
section. Set `reset: Some(reset_label)` in `NodeOutput`. Add `tests/sql/subquery_from.sql`.

**Commit:** `Compiler: codegen_materialize — fill in init section, yield loop in body`

---

## 105. `PlanExpr::In` + `JoinStrategy::Semi` (Track 4)

### What Changes

Two additions handle the `IN` operator:

- **Literal list** (`IN (1, 2, 3)`): a new `PlanExpr::In` variant keeps the plan clean
  (avoids polluting EXPLAIN with deep OR chains). The compiler desugars it to an equality
  OR chain at codegen time; a later optimization can emit a HashSet probe.
- **Subquery** (`IN (SELECT ...)`): a new `JoinStrategy::Semi` variant extends the
  existing `Join` node. The planner wraps the inner plan in `Materialize`. The codegen
  iterates the right buffer with early exit on first match, yielding only left columns.
  No new VM instructions are needed.

### Why `JoinStrategy::Semi` instead of a new `InSubquery` node

`IN (subquery)` is semantically a semi-join: retain outer rows whose key value appears in
the inner result set. With Materialize handling buffering, the semi-join codegen is a small
variation on `codegen_join` (Hash):

| Step | Hash join | Semi join |
|------|-----------|-----------|
| Right side | `Materialize { inner }` in plan | `Materialize { inner }` in plan |
| Buffer filled | INIT section via `codegen_materialize` | INIT section via `codegen_materialize` |
| Compilation order | left first, right second | left first, right second |
| Per left row | `right_output.reset` → iterate all, eval `on_condition`, yield left+right | `right_output.reset` → iterate, exit on first match, yield left only |
| Yield columns | left + right | left only |

The delta between `codegen_join` and `codegen_join_semi` is ~15 lines. `NOT IN` maps to
`Semi { negated: true }` — the match/exhaust outcomes are swapped.

**No `RowBufferContains` instruction.** An earlier design proposed a `RowBufferContains` VM
instruction backed by a lazy `HashSet` cache on `RowBuffer`. This is not needed: iteration
with early exit (O(n) per left row) is correct and sufficient. The `RowBuffer` struct has
no `contains_cache` field.

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

### Planner: how IN (subquery) becomes a semi-join

`Expression::In { source: InSource::Subquery(stmt), negated }` is converted in the
resolver as:

```rust
let inner_plan = plan_select(*stmt, btree)?;
let mat_plan = LogicalPlan::Materialize { input: Box::new(inner_plan) };
let key_expr = convert_expr(expr, resolver)?;       // left-side key, e.g. ColumnRef(0)

// on_condition compares left key with the first column of each right row.
// Column indices in on_condition are over combined (left ++ right) output registers.
// left_column_count is the width of the left (outer) plan.
let right_col_idx = left_column_count;              // first right column in combined regs
let on_condition = PlanExpr::Equals(
    Box::new(key_expr),
    Box::new(PlanExpr::ColumnRef(right_col_idx)),
);

LogicalPlan::Join {
    left: outer_plan,
    right: Box::new(mat_plan),
    on_condition,
    strategy: JoinStrategy::Semi { negated },
    left_column_count,
}
```

`on_condition` follows the same convention as `JoinStrategy::Hash`: it is a boolean
expression evaluated over the concatenation of left and right output registers.

### Compiler output for `Semi` join

`codegen_join_semi` in `nodes.rs`. The key structural difference from `codegen_join`:

1. **Compile left first, right second.** The body section starts at the left scan's check.
   The Materialize yield loop appears later in the body and is only reached via explicit
   jumps.

2. **Per left row: call `right_output.reset`**, which rewinds the buffer and begins
   yielding from the first row.

3. **Exit the inner loop on first match** (semi) or on exhaustion without match
   (anti-semi), then immediately request the next left row.

```
INIT: (from left scan and right Materialize — buffer fully populated before body)

BODY:
  LEFT_CHECK:
    [left scan: CanReadCursor / ReadCursor / MoveCursor Next]
    GoTo LEFT_ON_TUPLE   on tuple
    GoTo cont.on_done    on exhausted

  [right Materialize yield loop — compiled second, not the body entry point]
  reset_label:
    RewindRowBuffer      r_buf
  yield_next:
    NextFromRowBuffer    [right_regs], r_buf → RIGHT_DONE
    GoTo                 RIGHT_ON_TUPLE
  mat_next:
    GoTo                 yield_next

  LEFT_ON_TUPLE:
    GoTo                 reset_label          ; rewind right buffer, begin iteration

  RIGHT_ON_TUPLE:
    [eval on_condition over left_regs ++ right_regs → match_reg]
    if not negated:
      GoToIfFalse        mat_next, match_reg  ; no match → next right row
      GoTo               cont.on_tuple        ; match → yield left row
    if negated:
      GoToIfFalse        mat_next, match_reg  ; no match → next right row
      GoTo               left_output.next     ; match found → skip this left row

  RIGHT_DONE:            ; right buffer exhausted for this left row
    if not negated:
      GoTo               left_output.next     ; no match found → skip left row
    if negated:
      GoTo               cont.on_tuple        ; no match found → yield left row

  semi_next:             ; NodeOutput.next — parent calls here after consuming a left row
    GoTo                 left_output.next
```

`NodeOutput` for the semi-join:
```rust
NodeOutput {
    next: semi_next,
    reset: None,          // semi-join does not support reset (materialising node)
    output_regs: left_output.output_regs,  // only left columns yielded
}
```

### EXPLAIN output

The `Materialize` node appears in the plan tree as the right child:

```
0, "Project [name:1]"
1, "  Join [Semi] on id"
2, "    Scan users [cols: id, name]"
3, "    Materialize"
4, "      Scan admins [cols: user_id]"
```

### Key Files

- `src/planner/mod.rs` — `PlanExpr::In`; `JoinStrategy::Semi { negated }`
- `src/planner/resolver.rs` — `convert_expr` arms for `InSource::Values` and
  `InSource::Subquery`; wraps inner plan in `Materialize`; builds `on_condition` as an
  `Equals` expression over combined column indices
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
    // → Join {
    //     strategy: Semi { negated: false },
    //     right: Materialize { Scan(admins) },
    //     on_condition: Equals(ColumnRef(0), ColumnRef(left_count)),
    //   }
}

#[test]
fn plan_not_in_subquery_produces_anti_semi_join() {
    // WHERE id NOT IN (SELECT user_id FROM admins)
    // → Join { strategy: Semi { negated: true }, right: Materialize { ... }, ... }
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

-- Empty right side: no rows match (IN returns nothing; NOT IN returns all)
SELECT name FROM users WHERE id IN (SELECT user_id FROM admins WHERE user_id > 100)
-- > (no rows)

SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins WHERE user_id > 100) ORDER BY name
-- > alice
-- > bob
-- > carol

EXPLAIN SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)
-- > 0, "Project [name:1]"
-- > 1, "  Join [Semi] on id"
-- > 2, "    Scan users [cols: id, name]"
-- > 3, "    Materialize"
-- > 4, "      Scan admins [cols: user_id]"
```

### Implementation Steps (3 commits)

#### Step 105.1 — Planner: `PlanExpr::In`; desugar literal IN in resolver; literal IN SQL tests

No new VM ops. Update `explain.rs` for `PlanExpr::In`. Add `tests/sql/subquery_in.sql`
with literal IN cases only.

**Commit:** `Planner: add PlanExpr::In; compile literal IN list`

#### Step 105.2 — Planner: `JoinStrategy::Semi { negated }`; update EXPLAIN; stub codegen

Add the variant. The planner wraps the inner plan in `Materialize` and builds `on_condition`
as an `Equals` expression over combined column indices. Extend the `Join` match arm in the
compiler with `Err(UnsupportedStatement)`. Update `explain.rs` to render `[Semi]` and show
the `Materialize` child. Add planner unit tests verifying the plan shape.

**Commit:** `Planner: add JoinStrategy::Semi for IN (subquery)`

#### Step 105.3 — Compiler: `codegen_join_semi`; complete SQL integration tests

Implement `codegen_join_semi`: compile left first, right second; use `right_output.reset`
per left row; iterate inner loop with early match exit; yield only left columns; swap
match/exhaust outcomes for `negated`. Add the subquery IN cases to
`tests/sql/subquery_in.sql`.

**Commit:** `Compiler: codegen_join_semi — semi-join via Materialize reset and iteration`

---

## 106. Scalar subqueries in SELECT list and WHERE (Track 4)

### What Changes

`Expression::ScalarSubquery` (from item 103) is handled in the planner and compiler. A
scalar subquery evaluates to a single value, captured in a register during the **init
section** before the body begins. That register is used as a constant throughout the outer
query.

### Background

A scalar subquery must return at most one row and one column. At runtime:
1. A fill loop runs in the init section, appending at most one row to a RowBuffer.
2. Also in init, `NextFromRowBuffer` reads the single value into `r_scalar`.
3. If the inner query returned zero rows, `r_scalar` holds `NULL`.
4. The body uses `r_scalar` as a constant — it is never re-evaluated.

If the inner query returns more than one row, only the first row is used (matching SQLite
behaviour — document as undefined for multi-row scalar subqueries; strict checking can be
added later).

This follows the same fill-in-init principle as `Materialize` (item 104). Unlike FROM
subquery `Materialize`, no yield loop is emitted — the scalar is extracted immediately
after fill, all within the init section.

### New `PlanExpr` variant

```rust
// src/planner/mod.rs
pub enum PlanExpr {
    // ... existing variants ...

    /// (SELECT expr FROM ...) — evaluated once in the init section.
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

In `codegen_expr`, when `PlanExpr::ScalarSubquery` is encountered, the fill loop and
scalar extraction are emitted entirely into `ctx.init_emitter`:

```
// All of the following is in the INIT section:

InitRowBuffer            r_buf_scalar

// Inner plan fill loop (same pattern as codegen_materialize's fill phase):
[inner plan INIT code: Open(cursor), MoveCursor(First)]
SCALAR_FILL_CHECK:
  CanReadCursor          flag, cursor
  GoToIfFalse            SCALAR_FILL_DONE, flag
  ReadCursor             [col_reg], cursor
  MoveCursor             cursor, Next
  AppendToRowBuffer      r_buf_scalar, [col_reg]
  GoTo                   SCALAR_FILL_CHECK
SCALAR_FILL_DONE:

// Extract first (and only) value:
NextFromRowBuffer         [r_scalar], r_buf_scalar → SCALAR_NULL
GoTo                      SCALAR_AFTER
SCALAR_NULL:
  StoreValue              r_scalar, Null
SCALAR_AFTER:
```

`r_scalar` is returned by `codegen_expr` and used wherever the scalar subquery expression
appears in the outer query — in `Project` expressions, `Filter` expressions, etc. — as
though it were a `StoreValue` constant.

**Implementation note.** The inner plan's body code (scan check/read/advance loop) is also
emitted into `ctx.body_emitter` as a side effect of calling `codegen(inner_plan, ...)`.
This body code is only reachable via jumps from the fill loop in init; it is never
re-entered once the fill is complete, and plays the same role as in `codegen_materialize`.

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
- `src/compiler/nodes.rs` — `codegen_expr` arm for `PlanExpr::ScalarSubquery`; emits
  fill loop and extraction to `ctx.init_emitter`
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

Implement `codegen_expr` arm for `PlanExpr::ScalarSubquery`. Emit the fill loop and
`NextFromRowBuffer` extraction to `ctx.init_emitter`. Return `r_scalar`.

**Commit:** `Compiler: emit scalar subquery prelude; capture result in scalar register`

#### Step 106.3 — SQL integration tests: `tests/sql/subquery_scalar.sql`

**Commit:** `Tests: add scalar subquery SQL integration tests`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `SELECT name FROM users WHERE id IN (1, 3)` — correct rows, EXPLAIN shows `PlanExpr::In`
- [ ] `SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)` — correct rows,
      EXPLAIN shows `Join [Semi]` with `Materialize` child
- [ ] `SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins)` — correct rows
- [ ] `SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins WHERE user_id > 100)`
      — all rows returned (empty right side, NOT IN → all pass)
- [ ] `SELECT name FROM (SELECT name FROM users WHERE age > 28) AS young` — correct rows,
      EXPLAIN shows `Materialize` (no `DerivedTable`)
- [ ] `SELECT (SELECT COUNT(*) FROM orders) FROM users` — count for every user row
- [ ] `RowBuffer` has no `contains_cache` field — no lazy HashSet machinery
- [ ] `NodeOutput.reset` is `Some(...)` for `Materialize`, `None` for `Join` (any strategy)
- [ ] Materialize with 0-row inner scan → empty buffer → `NextFromRowBuffer` fires `on_done`
      immediately (not a crash)
- [ ] Scalar subquery returning 0 rows → NULL (not a crash)
- [ ] Semi-join with empty right side → no rows returned for IN, all rows for NOT IN
- [ ] Each commit is independently testable
