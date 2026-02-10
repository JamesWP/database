# Phase E — Query Power

Phase E adds expressive query capabilities: sorting, aggregation, grouping, and pattern matching.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 24 | 1.6 | ORDER BY | — |
| 25 | 1.7 | COUNT(*) | — |
| 26 | 1.8 | GROUP BY + aggregates | 25 |
| 27 | 4.2 | LIKE operator | — |

---

## 24. ORDER BY (Track 1.6)

### What Changes

Parser learns `ORDER BY col [ASC|DESC]`. New `LogicalPlan::Sort` node that collects all rows, sorts in memory, then yields.

### Key Files

- `src/frontend/parser.rs` — parse ORDER BY clause
- `src/frontend/ast.rs` — add OrderByClause to SelectStatement
- `src/planner.rs` — wrap plan with Sort node
- `src/compiler/nodes.rs` — new `compile_sort()` that materializes, sorts, yields

### Implementation Approach

1. Parser consumes optional `ORDER BY column [ASC|DESC]` after existing SELECT parsing. Support multiple sort keys.
2. Compiler: run child to completion collecting all rows into Vec, sort with `sort_by`, yield each row.
3. Multi-column sort: compare first key, break ties with second, etc.

### Tests

- ORDER BY ASC / DESC / default / multiple columns / with WHERE / with LIMIT

---

## 25. COUNT(*) (Track 1.7)

### What Changes

Wire existing `LogicalPlan::Count` node to SQL syntax.

### Key Files

- `src/frontend/parser.rs` — recognize `COUNT(*)` in select list
- `src/frontend/ast.rs` — aggregate function representation
- `src/planner.rs` — detect COUNT(*), wrap with Count node
- `src/compiler/nodes.rs` — existing Count node (verify end-to-end)

### Implementation Approach

1. Parser: check for `COUNT` `(` `*` `)` when parsing select columns.
2. Planner: if select list is only COUNT(*) and no GROUP BY, wrap scan in Count node.
3. The existing compiler Count node handles counting and yielding a single row.

### Tests

- COUNT(*) all rows / with WHERE / empty table returns 0

---

## 26. GROUP BY + Aggregates (Track 1.8)

### What Changes

New `LogicalPlan::Aggregate` with grouping keys and accumulator functions (SUM, AVG, MIN, MAX, COUNT).

### Key Files

- `src/frontend/parser.rs` — parse GROUP BY, recognize aggregate functions
- `src/planner.rs` — build Aggregate node
- `src/compiler/nodes.rs` — hash-based grouping with accumulators
- `src/engine/scalarvalue.rs` — accumulator arithmetic

### Implementation Approach

1. Parser handles `GROUP BY col1, col2` and `SUM(col)`, `AVG(col)`, etc. in SELECT.
2. Compiler: scan all rows, group by key columns using `HashMap<Vec<ScalarValue>, Vec<Accumulator>>`, each accumulator tracks count/sum/min/max, yield one row per group.
3. AVG = sum/count at yield time.

### Tests

- GROUP BY with COUNT / SUM / AVG / MIN / MAX / multiple keys / with WHERE / aggregate without GROUP BY

---

## 27. LIKE Operator (Track 4.2)

### What Changes

Parser recognizes `expr LIKE pattern`. Pattern matching with `%` (any sequence) and `_` (single char).

### Key Files

- `src/frontend/parser.rs` — parse LIKE as binary operator
- `src/frontend/ast.rs` — add Like variant
- `src/engine/scalarvalue.rs` — LIKE evaluation

### Implementation Approach

1. After parsing a comparison, check for LIKE keyword. Parse RHS as pattern.
2. Implement `sql_like_match(value, pattern) -> bool` — convert `%` to `.*`, `_` to `.`, or use simple state machine.
3. Emit a comparison instruction in the compiler.

### Tests

- LIKE with `%` / `_` / exact match / no match / `%` matches all

---

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`
