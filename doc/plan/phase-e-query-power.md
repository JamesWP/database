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
Important: Each item should be committed seperately, follow 'Git Workflow' in CLAUDE.md

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

New `LogicalPlan::Aggregate` node with hash-based grouping and five aggregate functions (COUNT, SUM, AVG, MIN, MAX).

### Key Files

- `src/frontend/lexer.rs` — add GROUP keyword
- `src/frontend/ast.rs` — add group_by field to SelectStatement
- `src/frontend/parser.rs` — parse GROUP BY, recognize aggregate functions
- `src/planner.rs` — add Aggregate node, AggregateExpr, AggregateFunction types
- `src/engine/program.rs` — new operations: InitGroupTable, UpdateGroup, YieldFromGroupTable
- `src/engine/registers.rs` — add GroupTable and Accumulator register types
- `src/compiler/nodes.rs` — codegen_aggregate() with hash-based grouping
- `src/engine.rs` — implement group table operations

### Logical Plan Structure

```rust
LogicalPlan::Aggregate {
    input: Box<LogicalPlan>,
    group_keys: Vec<PlanExpr>,      // Expressions to group by
    aggregates: Vec<AggregateExpr>,  // Aggregate functions
}

AggregateExpr {
    function: AggregateFunction,     // COUNT, SUM, AVG, MIN, MAX
    argument: Option<PlanExpr>,      // None for COUNT(*)
}
```

**Example:** `SELECT dept, COUNT(*), AVG(salary) FROM emp GROUP BY dept`
- Input: `[dept, salary, ...]`
- Output: `[dept, count_result, avg_result]`
- group_keys: `[ColumnRef(dept)]`
- aggregates: `[Count(None), Avg(Some(ColumnRef(salary)))]`

### VM Operations

Three new operations for hash-based grouping:

1. **InitGroupTable(Reg)** - Initialize empty `HashMap<Vec<ScalarValue>, Vec<Accumulator>>`
2. **UpdateGroup** - Evaluate group keys, lookup/create group, update accumulators
3. **YieldFromGroupTable** - Pop groups and yield rows (group keys + aggregates)

### Accumulators

Each aggregate function maintains state:
- COUNT: `{ count: i64 }`
- SUM: `{ sum: ScalarValue, count: i64 }` (track NULLs)
- AVG: `{ sum: ScalarValue, count: i64 }` (finalize as sum/count)
- MIN: `{ value: Option<ScalarValue> }`
- MAX: `{ value: Option<ScalarValue> }`

### Implementation Approach

1. **Lexer/Parser:**
   - Add GROUP keyword
   - Parse `GROUP BY expr1, expr2, ...` after WHERE, before ORDER BY
   - Support full expressions (not just column names)
   - Recognize aggregate function names: COUNT, SUM, AVG, MIN, MAX

2. **Planner:**
   - Detect GROUP BY clause OR standalone aggregates in SELECT
   - Build Aggregate node with group_keys and aggregates
   - Extract aggregate functions from SELECT columns
   - Handle special case: aggregates without GROUP BY (empty group_keys = one big group)

3. **Compiler:**
   - Emit InitGroupTable in init phase
   - Scan loop: ReadCursor → UpdateGroup → MoveCursor
   - After scan: YieldFromGroupTable loop
   - Each YieldFromGroupTable outputs: group_keys + finalized aggregates

4. **Engine:**
   - UpdateGroup: hash group keys, lookup/insert, update accumulator state
   - YieldFromGroupTable: pop from hash table, finalize accumulators, store in dest_regs
   - NULL handling: NULLs group together, aggregates ignore NULLs (except COUNT(*))

### Bytecode Pattern

Example: `SELECT dept, COUNT(*) FROM emp GROUP BY dept`

Aggregate node wraps a child (e.g., Scan). Child produces rows, Aggregate collects them.

```
INIT:
  0  InitGroupTable R0

BODY:
  // Child node (Scan) emits: Open, MoveCursor, CanRead, ReadCursor, etc.
  // Child's on_tuple continuation points to update_group label below

  // update_group: called for each row from child
  X  UpdateGroup    R0, keys:[R3], aggregates:[Count(None)]
  X+1  GoTo         child.next   // Get next row from child

  // yield_from_groups: called after child exhausted (on_done)
  Y  YieldFromGroupTable [R4, R5], R0, @halt
  Y+1  GoTo         @on_tuple
  Y+2  GoTo         @yield_from_groups   // Loop back

  // on_tuple: emit each group
  Z  Yield          [R4, R5]
  Z+1  GoTo         @(Y+2)   // Back to yield next group

  // halt:
  H  Halt
```

**Continuation wiring:**
- Child's `on_tuple` → update_group (X)
- Child's `on_done` → yield_from_groups (Y)
- Root's `on_tuple` → Yield (Z)
- Root's `on_done` → Halt (H)

**IMPORTANT:** YieldFromGroupTable has a JumpTarget parameter.
When adding this operation, MUST update:
- `src/compiler/nodes.rs::adjust_jump_targets()` - adjust by offset
- `src/compiler/emitter.rs::finalize()` - resolve unresolved targets
(See Phase H - Compiler Safety for details on making this impossible to forget)

### Expression Support

**Implemented (Option B):**
- Full expression support in GROUP BY: `GROUP BY salary + bonus`
- Function calls: `GROUP BY UPPER(name)`
- Complex expressions: `GROUP BY CASE WHEN ... END`
- Multiple group keys with expressions

**Deferred:**
- Column position references: `GROUP BY 1, 2`
- Alias references: `GROUP BY total_alias`
- HAVING clause: `HAVING COUNT(*) > 10`

### NULL Handling

- NULL is a valid group key (all NULLs group together)
- COUNT(*): counts all rows including NULLs
- COUNT(expr): counts only non-NULL values
- SUM/AVG/MIN/MAX: ignore NULL values
- If all values are NULL: COUNT(*)=count, COUNT(expr)=0, others=NULL

### Special Cases

**Aggregates without GROUP BY:**
```sql
SELECT COUNT(*), AVG(salary) FROM employees;
```
Treated as GROUP BY with empty group_keys (one big group, returns one row).

**Mixed aggregate/non-aggregate columns:**
```sql
SELECT dept, name, COUNT(*) FROM emp GROUP BY dept;
```
`name` returns arbitrary row from each dept group (like COUNT(*) without GROUP BY).

### Tests

- Single group key: `SELECT dept, COUNT(*) FROM t GROUP BY dept`
- Multiple group keys: `SELECT dept, location, COUNT(*) FROM t GROUP BY dept, location`
- All five aggregates: COUNT(*), COUNT(expr), SUM, AVG, MIN, MAX
- Expression-based grouping: `GROUP BY salary + bonus`
- Aggregates without GROUP BY: `SELECT COUNT(*) FROM t`
- NULL handling: NULL group keys, NULL in aggregate columns
- Combinations: GROUP BY + WHERE, GROUP BY + ORDER BY, GROUP BY + LIMIT
- Empty result set, single row tables

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
