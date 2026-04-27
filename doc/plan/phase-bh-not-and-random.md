# Phase BH — Boolean NOT and RANDOM()

Two small expression-layer additions needed by the "compatible pair" application query:

```sql
SELECT a.id, b.id FROM names a JOIN names b
ON NOT (a.gender='m' AND b.gender='f') AND NOT (a.gender='f' AND b.gender='m') AND a.id != b.id
WHERE a.id = (SELECT id FROM names ORDER BY RANDOM() LIMIT 1) LIMIT 1
```

Everything else in that query already works (self-joins, aliases, `!=`, `LIMIT`, complex AND
in ON). The scalar subquery in WHERE is covered by Phase AB item 106. This phase adds the
two remaining missing pieces: boolean `NOT` as a unary prefix operator and `RANDOM()` as a
zero-argument scalar function.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| BH-1 | 1 | Parser + AST: `UnaryOp::Not`; `NOT expr` prefix | — |
| BH-2 | 1 | Planner + compiler: emit `NotValue` for `UnaryOp::Not` | BH-1 |
| BH-3 | 1 | `RANDOM()` zero-argument function: lexer keyword, planner, VM, compiler | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Boolean NOT

The lexer already emits a `Not` token for the `NOT` keyword. The parser uses it only for
`IS NOT NULL` and column constraints. `NOT (expr)` as a standalone prefix operator is
unsupported: `parse_unary` matches only `+` and `!` (numeric) via `as_unary()`. Adding
`UnaryOp::Not` and a `parse_unary` branch for `lexer::Type::Not` is the only change needed;
the planner and compiler already handle `UnaryOp` via `codegen_expr`.

The existing `as_unary()` helper maps token → `UnaryOp`. Adding `lexer::Type::Not →
Some(UnaryOp::Not)` makes the parse just work — no other parser changes required.

At the planner level, `UnaryOp::Not` maps to `PlanExpr::Not(inner)` (or the existing
`PlanExpr::Negate` if it's already generic — check before adding a new variant). At the
compiler level, emit `NotValue(dest, src)`, which the engine evaluates as:
`Boolean(false) → Boolean(true)`, `Boolean(true) → Boolean(false)`, `NULL → NULL`,
other types → error.

### RANDOM()

`RANDOM()` takes zero arguments and returns a random 64-bit integer (matching SQLite
semantics). The current function infrastructure in `src/compiler/expr.rs` handles
single-argument functions; a zero-argument path is needed.

The simplest approach: in `codegen_expr`, after matching a `FunctionCall` with
`fn_name = "RANDOM"` and an empty arg list, emit `RandomValue(dest)` directly, bypassing
the single-arg helper. No intermediate register is needed.

`ORDER BY RANDOM()` works without further changes once `RANDOM()` is a valid expression —
the sort infrastructure already handles arbitrary sort keys.

---

## BH-1. Parser + AST: `UnaryOp::Not`

### What Changes

**AST** (`src/frontend/ast.rs`):

```rust
pub enum UnaryOp {
    Plus,
    Negate,
    IsNull,
    IsNotNull,
    Not,   // NEW: boolean NOT
}
```

**Parser** (`src/frontend/parser.rs`):

In `as_unary()` (the `lexer::Type → Option<UnaryOp>` helper):

```rust
lexer::Type::Not => Some(ast::UnaryOp::Not),
```

That's the only change. `parse_unary` already calls `as_unary()` and wraps the result in
`UnaryOp { op, expression }`, so `NOT expr` now parses correctly.

### Key Files

- `src/frontend/ast.rs` — `UnaryOp::Not` variant
- `src/frontend/parser.rs` — `as_unary()`: map `Not` token

### Tests

```rust
#[test]
fn parse_not_boolean() {
    // NOT (a = 1) → UnaryOp { op: Not, expression: BinaryOp(Equals, ...) }
}

#[test]
fn parse_not_compound() {
    // NOT (a = 1 AND b = 2) → UnaryOp { op: Not, expression: And(...) }
}
```

### Implementation Steps (1 commit)

#### Step BH-1.1 — AST: add UnaryOp::Not; parser: map Not token in as_unary

**Commit:** `Parser: add boolean NOT as unary prefix operator`

---

## BH-2. Planner + Compiler: `NotValue` VM operation

### What Changes

**Planner** (`src/planner/resolver.rs`): add arm for `UnaryOp::Not`:

```rust
UnaryOp::Not => PlanExpr::Not(Box::new(convert_expr(*expression, resolver)?)),
```

Check whether a `PlanExpr::Not` variant already exists or needs adding. If the planner
currently uses a generic `PlanExpr::UnaryOp { op, expr }`, extend that enum instead.

**VM** (`src/engine/program.rs`):

```rust
/// dest = !src: true→false, false→true, NULL→NULL
NotValue(Reg, Reg),
```

**Engine** (`src/engine.rs`): execute `NotValue`:

```rust
Operation::NotValue(dest, src) => {
    let val = match registers.get(src) {
        ScalarValue::Boolean(b) => ScalarValue::Boolean(!b),
        ScalarValue::Null => ScalarValue::Null,
        _ => return Err(StepError::TypeMismatch("NOT requires boolean")),
    };
    registers.set(dest, val);
}
```

**Compiler** (`src/compiler/expr.rs`): emit `NotValue` for `PlanExpr::Not`:

```rust
PlanExpr::Not(inner) => {
    let src = codegen_expr(inner, ctx)?;
    let dest = ctx.alloc_reg();
    ctx.emit(Operation::NotValue(dest, src));
    dest
}
```

### Key Files

- `src/planner/resolver.rs` — arm for `UnaryOp::Not`
- `src/planner/mod.rs` — `PlanExpr::Not` variant (if needed)
- `src/engine/program.rs` — `NotValue` + Display
- `src/engine.rs` — execute `NotValue`
- `src/compiler/expr.rs` — emit `NotValue`
- `src/explain.rs` — render `PlanExpr::Not`

### Tests

```sql
-- tests/sql/boolean_not.sql
CREATE TABLE items (id INTEGER, active INTEGER)
-- > Table 'items' created
INSERT INTO items VALUES (1, 1), (2, 0), (3, 1)
-- > 3 rows inserted

SELECT id FROM items WHERE NOT (active = 1) ORDER BY id
-- > 2

SELECT id FROM items WHERE NOT (id = 1 OR id = 3) ORDER BY id
-- > 2

-- NOT with NULL propagation
SELECT id FROM items WHERE NOT (id = 99)
-- > 1
-- > 2
-- > 3
```

### Implementation Steps (1 commit)

#### Step BH-2.1 — Planner, VM, compiler: evaluate boolean NOT

**Commit:** `Compiler: add NotValue VM operation and boolean NOT expression support`

---

## BH-3. `RANDOM()` Zero-Argument Function

### What Changes

**Lexer** (`src/frontend/lexer.rs`): check whether `random` is already scanned as a plain
identifier. If so, no lexer change is needed — function calls are parsed by name. If a
`Random` keyword token would help disambiguation, add it; otherwise rely on identifier
matching.

**Planner** (`src/planner/resolver.rs`): in the supported-functions list, add `"RANDOM"`.
Handle zero-argument case: the current path requires exactly one argument and calls the
arg through `convert_expr`. Add a zero-argument branch:

```rust
if fn_name == "RANDOM" {
    if !args.is_empty() {
        return Err(PlanError::WrongNumberOfArguments { fn_name, expected: 0, got: args.len() });
    }
    return Ok(PlanExpr::FunctionCall { fn_name: "RANDOM".into(), args: vec![] });
}
```

**VM** (`src/engine/program.rs`):

```rust
/// dest = random i64 (uniform, full range, matching SQLite's RANDOM() semantics)
RandomValue(Reg),
```

**Engine** (`src/engine.rs`):

```rust
Operation::RandomValue(dest) => {
    use std::collections::hash_map::DefaultHasher;
    // Use rand crate (already a dev-dependency or add it) or thread_rng
    let val = rand::random::<i64>();
    registers.set(dest, ScalarValue::Integer(val));
}
```

Check whether `rand` is already a dependency (`Cargo.toml`). If not, add
`rand = "0.8"` to `[dependencies]` in the library crate.

**Compiler** (`src/compiler/expr.rs`): in the `FunctionCall` match arm, before the
single-argument helper, detect zero-arg `RANDOM`:

```rust
if fn_name == "RANDOM" && args.is_empty() {
    let dest = ctx.alloc_reg();
    ctx.emit(Operation::RandomValue(dest));
    return Ok(dest);
}
```

### Key Files

- `src/planner/resolver.rs` — RANDOM in supported list; zero-arg branch
- `src/engine/program.rs` — `RandomValue` + Display
- `src/engine.rs` — execute `RandomValue`
- `src/compiler/expr.rs` — emit `RandomValue` for zero-arg RANDOM
- `crates/database/Cargo.toml` — `rand` dependency if not present

### Tests

```sql
-- tests/sql/random_function.sql
CREATE TABLE numbers (id INTEGER, val INTEGER)
-- > Table 'numbers' created
INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)
-- > 3 rows inserted

-- RANDOM() returns an integer (row count confirms query executes)
SELECT COUNT(*) FROM numbers WHERE RANDOM() IS NOT NULL
-- > 3

-- ORDER BY RANDOM() — result is non-deterministic, just verify it runs without error
-- Use a fixed seed via a subquery workaround is not possible; instead verify count
SELECT COUNT(*) FROM (SELECT id FROM numbers ORDER BY RANDOM() LIMIT 2)
-- > 2
```

Note: because `RANDOM()` is non-deterministic, SQL integration tests only verify that it
executes and returns plausible results. Engine unit tests can use seeded RNG if needed.

### Implementation Steps (1 commit)

#### Step BH-3.1 — RANDOM(): planner allowlist, RandomValue VM op, compiler emit

**Commit:** `Compiler: add RANDOM() zero-argument function`

---

## Verification

- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo fmt && cargo build --workspace 2>&1 | grep warning` — zero warnings
- [ ] `SELECT id FROM t WHERE NOT (active = 1)` — returns correct rows
- [ ] `NOT (a AND b)` in ON clause of self-join — filters correctly
- [ ] `ORDER BY RANDOM()` — query runs, returns all rows in some order
- [ ] `SELECT id FROM t ORDER BY RANDOM() LIMIT 1` — returns exactly one row
- [ ] `RANDOM()` in WHERE — query runs, no crash
- [ ] `RANDOM(1)` — `WrongNumberOfArguments` error (or graceful parse error)
- [ ] Each commit is independently testable
