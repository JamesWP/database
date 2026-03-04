# Phase AC — Join Improvements

Rename, extend, and accelerate the join execution model: add an explicit join strategy field,
add a nested-loop execution path, extend to multi-column ON conditions, and introduce
index-probe acceleration for equi-joins.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 107 | 4 | Refactor: add `JoinStrategy` to `Join` node; default `Hash` | — |
| 108 | 4 | Planner emits `NestedLoop`; optimizer promotes to `Hash` when no index | 107 |
| 109 | 4 | Compiler: `NestedLoop` codegen path + unit tests | 108 |
| 110 | 4 | `IndexProbe` plan node + compiler codegen | 109 |
| 111 | 4 | Optimizer: rewrite `NestedLoop(Scan right)` to `NestedLoop(IndexProbe)` for equi-joins | 110 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The existing `Join` node implements a single strategy: materialize the right side into a
`RowBuffer`, then for each left row scan the buffer checking the ON condition. This is a
hash/buffer join. The plan has no way to express an alternative execution strategy.

This phase introduces `JoinStrategy` to make the strategy explicit, adds a nested-loop path
(right child re-driven per left row), extends join planning to multi-column ON conditions,
and introduces an `IndexProbe` node that the optimizer can substitute for a right-side
`Scan` when an index exists on the join key — enabling index-accelerated nested-loop joins.

### Canonical plan shape

The planner always emits `NestedLoop`. The optimizer then applies one of two rules:

1. No index on right join column → promote to `Hash`
2. Equi-join condition + matching index on right table → keep `NestedLoop`, replace right
   `Scan` with `RowidLookup(IndexProbe(...))`

`NestedLoop(Scan)` is never the final plan.

---

## Node structures

```rust
pub enum JoinStrategy {
    /// Materialise the entire right side into a RowBuffer once, then
    /// for each left row scan the buffer checking on_condition.
    Hash,
    /// Re-drive the right child once per left row via its reset entry point.
    /// Right child must be resettable (see "Reset contract" below).
    /// The optimizer either promotes to Hash (no index) or leaves NestedLoop
    /// with RowidLookup(IndexProbe(...)) as right child (index available).
    NestedLoop,
}

LogicalPlan::Join {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
    on_condition: PlanExpr,
    strategy: JoinStrategy,
    left_column_count: usize,
}

/// Dynamic-key counterpart to IndexScan: yields rowids only.
/// Column fetching is delegated to a RowidLookup node above it, exactly as
/// with IndexScan. Typical right-child subtree in a NestedLoop join:
///
///   RowidLookup(IndexProbe { index_rootpage, key_expr, index_col_idx },
///               table_rootpage, columns)
LogicalPlan::IndexProbe {
    index_rootpage: u32,
    /// Expression evaluated against the *left* row's registers to produce the probe key.
    /// ColumnRef(i) refers to left output column i — resolved by NestedLoop codegen
    /// via CodegenContext.outer_regs (see "Codegen context and coupling" below).
    /// This is the ONLY place in a right-child subtree where ColumnRef refers to left
    /// columns rather than right columns (see "Column reference spaces" below).
    key_expr: PlanExpr,
    index_col_idx: usize,
}
```

---

## Codegen context and coupling

### Normal context flow

The codegen framework has three clean channels for passing information:

| Channel | Direction | Mechanism |
|---------|-----------|-----------|
| Where to jump on row/done | down | `NodeContinuation { on_tuple, on_done }` |
| Which registers hold output | up | `NodeOutput { next, reset, output_regs }` |
| Plan-time constants | in node | rootpage, columns, key_expr, etc. |

Most nodes only need these. Adding a new node should not require anything beyond them.

### NestedLoop-specific couplings

`NestedLoop` introduces two couplings that sit outside the normal channels. Both are
contracts between the join and its right subtree that intermediate pass-through nodes
(`RowidLookup`, `Filter`, `Project`) carry without using.

**1. `reset: Option<Label>` on `NodeOutput`**

The nested-loop join requires the right child to emit a reset entry point. Resettable nodes
populate `NodeOutput.reset`; others leave it `None`. The nested-loop join panics if `reset`
is `None`. Pass-through nodes delegate reset to their child and propagate the label upward.

**2. `outer_regs: Option<Vec<Reg>>` on `CodegenContext`**

`IndexProbe.key_expr` contains `ColumnRef(i)` references that index into the *left* row's
registers, not the right subtree's registers. These register numbers are only known at the
time the nested-loop join compiles its left child — after which they are injected into
`CodegenContext.outer_regs`. `codegen_index_probe` reads them from there to call
`compile_expr(key_expr, outer_regs)`.

`codegen_join_nested_loop` sets `ctx.outer_regs` before descending into the right subtree
and clears it immediately after. Pass-through nodes in the right subtree (`RowidLookup`,
`Filter`) do not read or modify `outer_regs` — they simply call `codegen` on their child
as normal and the field propagates automatically.

This is the only place in the codebase where `CodegenContext` carries transient
parent-injected state. The field must be `None` everywhere except inside a nested-loop
join right subtree compilation. If a future node also needs outer-row context, it should
extend this mechanism rather than introduce a second field.

---

## Column reference spaces

`ColumnRef(i)` means different things depending on where it appears. This must be respected
by both the planner (when building expressions) and the compiler (when calling
`compile_expr`).

| Expression location | `ColumnRef(i)` refers to | `compile_expr` called with |
|--------------------|--------------------------|---------------------------|
| Join `on_condition` | combined output — left cols first, then right | `left_regs + right_regs` |
| Right-child `Filter` predicate | right child's output cols only | `right_output_regs` |
| Right-child `Project` expressions | right child's input cols only | `right_output_regs` |
| `IndexProbe.key_expr` | left output cols (0-based) | `ctx.outer_regs` (left regs) |

**The right-child subtree is a left-column-free zone** — no expression inside a right-child
node (Filter predicate, Project expression, etc.) may reference left columns, except for
`IndexProbe.key_expr` which is explicitly designed to do so. Cross-column conditions belong
in the join's `on_condition`, not pushed into the right subtree.

This boundary keeps `compile_expr` calls unambiguous: the caller always knows which register
slice to pass, and no node needs to be aware of outer context registers except through
`ctx.outer_regs`.

### Why this works at runtime

VM registers are persistent mutable state. The left loop writes into left registers each
iteration. When the nested-loop join jumps to the right child's reset label, those registers
already hold the current left row. `IndexProbe.key_expr` compiled against `outer_regs` reads
the current values — no special runtime passing is needed.

---

## Reset contract

A **resettable** node emits a `reset` label in its body. When jumped to, `reset`
re-evaluates the node from scratch using whatever is currently in the VM registers —
including left registers, which the enclosing left loop has just updated.

`NodeOutput` gains a `reset: Option<Label>` field. The nested-loop join requires `reset`
to be `Some` on the right child's output and panics if it is `None`.

### Nodes resettable in this phase

| Node | `reset` behaviour |
|------|-----------------|
| `Scan` | emit `RESET` label in body: `MoveCursor(First)` then fall through to `CHECK` |
| `Filter` | propagate child's `reset` label unchanged |
| `Project` | propagate child's `reset` label unchanged |
| `RowidLookup` | propagate child's `reset` label unchanged |
| `IndexProbe` | emit `RESET` label: evaluate `key_expr` via `outer_regs` + `MoveCursor(Find)` |

### Materializing nodes — deferred

`Sort`, `Distinct`, `Aggregate`, `Count`, and `Join(Hash)` buffer their output during
their first pass. A simple buffer rewind is only safe if no expression in their subtree
references left columns — an invariant that is hard to guarantee in general. Rather than
implement reset for these nodes speculatively, correct handling is deferred until a
concrete use case arises with examples to verify against. The compiler panics if any of
these appear as a nested-loop join right child.

---

## 107. Refactor: add `JoinStrategy` to `Join` node (Track 4)

### What Changes

Add `JoinStrategy` enum and a `strategy: JoinStrategy` field to `LogicalPlan::Join`.
Set `Hash` everywhere — planner, tests, EXPLAIN, optimizer, compiler. No behaviour change.

### Background

The existing `Join` node has no strategy field; it always compiles as hash/buffer join.
This item introduces the field with `Hash` as the only value, threading it through every
match arm so subsequent items can add the `NestedLoop` arm cleanly.

### Implementation Approach

Add to `src/planner/mod.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy { Hash, NestedLoop }
```

Add `strategy: JoinStrategy` to `LogicalPlan::Join`. Update every match arm that
destructures `Join`:

- `src/planner/select.rs` — `plan_select_joined`: set `strategy: JoinStrategy::Hash`
- `src/planner/optimizer.rs` — `optimize` and `fuse_projects`: pass `strategy` through
- `src/compiler/nodes.rs` — `codegen` dispatch and `codegen_join`: accept `strategy`,
  currently only has the `Hash` path
- `src/explain.rs` — render strategy in label: `Join [Hash]`

### Key Files

- `src/planner/mod.rs`
- `src/planner/select.rs`
- `src/planner/optimizer.rs`
- `src/compiler/nodes.rs`
- `src/explain.rs`

### Tests

Existing join tests pass unchanged. EXPLAIN output changes from `Join [...]` to
`Join [Hash | ...]` — update inline expected output with
`cargo run --bin update-sql-tests`.

### Implementation Steps (1 commit)

#### Step 107.1 — Add `JoinStrategy`; thread through all match arms

**Commit:** `Refactor: add JoinStrategy field to Join node; default Hash`

---

## 108. Planner emits `NestedLoop`; optimizer promotes to `Hash` (Track 4)

### What Changes

- `plan_select_joined` changes to `strategy: JoinStrategy::NestedLoop`
- Optimizer adds a new rule: `Join { strategy: NestedLoop, right: Scan }` with no index
  on the right table's join column → flip to `strategy: Hash`

All existing SQL integration tests continue to produce the same results (the optimizer
immediately promotes every join to Hash since no IndexProbe exists yet).

### Background

After this item the planner + optimizer pipeline looks like:

```
planner:   Join { NestedLoop, left: Scan(L), right: Scan(R) }
optimizer: Join { Hash,       left: Scan(L), right: Scan(R) }   ← no index
```

The optimizer rule fires inside the existing `optimize` match on `LogicalPlan::Join`.
It checks whether `strategy == NestedLoop` and the right child is a `Scan` whose table
has no index on the join column. If so, it returns the same node with `strategy: Hash`.
The existing hash codegen is unchanged.

### Key Files

- `src/planner/select.rs` — emit `NestedLoop`
- `src/planner/optimizer.rs` — new rule in `optimize`

### Tests

```rust
#[test]
fn optimizer_promotes_nested_loop_to_hash_when_no_index() {
    // Build Join { NestedLoop, Scan(users), Scan(orders) } with no index.
    // After optimize(), strategy must be Hash.
}
```

### Implementation Steps (1 commit)

#### Step 108.1 — Planner: emit NestedLoop; optimizer: promote to Hash

**Commit:** `Planner: emit NestedLoop by default; optimizer promotes to Hash when no index`

---

## 109. Compiler: `NestedLoop` codegen path (Track 4)

### What Changes

`NodeOutput` gains `reset: Option<Label>`. `codegen_join` dispatches on `strategy`; the
`Hash` path is unchanged. A new `codegen_join_nested_loop` handles `NestedLoop`.

Resettable nodes (`Scan`, `Filter`, `Project`, `RowidLookup`) emit a `reset` label and
populate `NodeOutput.reset`. Pass-through nodes propagate the child's label.

See "Reset contract" and "Column reference spaces" sections — both apply here.

### Implementation Approach

**`NodeOutput`:**

```rust
pub struct NodeOutput {
    pub next: Label,
    /// Entry point to restart this node from the beginning for the current
    /// outer (left) row. Required by NestedLoop join; None for nodes that
    /// do not support reset (materializing nodes). See phase-ac plan.
    pub reset: Option<Label>,
    pub output_regs: Vec<Reg>,
}
```

All existing callers set `reset: None` initially; they are updated in this item.

**`codegen_scan` reset:**

The current `codegen_scan` emits `MoveCursor(First)` in **init** (runs once). To support
reset, `MoveCursor(First)` moves to the **body**, emitted as a `RESET` label that falls
through to the existing `CHECK` label:

```text
INIT:
  Open(cursor, rootpage)        ← cursor open only; no positioning

BODY:
  RESET:                        ← NodeOutput.reset = this label
    MoveCursor(cursor, First)
    [fall through to CHECK]
  CHECK:
    CanReadCursor(flag, cursor); GoToIfFalse(on_done, flag)
    ReadCursor(output_regs, cursor)
    MoveCursor(cursor, Next)
    GoTo(on_tuple)
```

This changes `codegen_scan` slightly: `MoveCursor(First)` moves from init to body.
Existing callers are unaffected because the body is entered via `next` (= `CHECK` label)
on first call, which falls through from `RESET` only when explicitly jumped to.

Wait — the body entry on first call must still position the cursor. The fix: the
nested-loop join always jumps to `reset` (not `next`) for each left row, including the
first. For non-nested-loop callers, the `RESET` label is never jumped to, so the
`MoveCursor(First)` in init must remain for those callers.

To avoid changing semantics for existing callers, emit `MoveCursor(First)` in **both**
init (for standalone use) and as the `RESET` label in body (for nested-loop use):

```text
INIT:
  Open(cursor, rootpage)
  MoveCursor(cursor, First)     ← still here for standalone callers

BODY:
  RESET:                        ← NodeOutput.reset; jumped to by NLJ per left row
    MoveCursor(cursor, First)
    [fall through to CHECK]
  CHECK:   ...
```

**`codegen_filter`, `codegen_project`, `codegen_rowid_lookup` reset:**

These pass-through nodes propagate the child's `reset`:

```rust
NodeOutput {
    next: child_output.next,
    reset: child_output.reset,  // propagate unchanged
    output_regs: child_output.output_regs,
}
```

**`codegen_join_nested_loop`:**

```text
INIT:
  <left child init>
  <right child init>
  ctx.outer_regs = left_output.output_regs  ← set before right child compiled
  <right child init>
  ctx.outer_regs = None                     ← clear after

BODY:
  GoTo(LEFT_NEXT)

  RIGHT_RESET:             ← right_output.reset (= IndexProbe or Scan reset)
    [right child restarts from here]
  RIGHT_NEXT:              ← right_output.next
    [right child advances]
    → on_tuple: RIGHT_CHECK
    → on_done:  LEFT_NEXT

  RIGHT_CHECK:
    [right child yielded; evaluate on_condition over combined regs]
    GoToIfFalse(right_output.next, pred)
    GoTo(cont.on_tuple)

  JOIN_NEXT:               ← NodeOutput.next (parent calls for next row)
    GoTo(right_output.next)

  LEFT_NEXT:
    [left child advances]
    → on_tuple: LEFT_GOT_ROW
    → on_done:  cont.on_done

  LEFT_GOT_ROW:
    GoTo(RIGHT_RESET)      ← restart right child for new left row
```

The right child is compiled with `ctx.outer_regs = Some(left_output.output_regs)` set
before the `codegen(right, ...)` call so that `codegen_index_probe` (item 110) can read
it. Cleared immediately after.

### Key Files

- `src/compiler/nodes.rs` — `NodeOutput.reset`; `codegen_join` dispatch; new
  `codegen_join_nested_loop`; reset emission in `codegen_scan`, `codegen_filter`,
  `codegen_rowid_lookup`; `outer_regs` field on `CodegenContext`

### Code comments required

- **`CodegenContext.outer_regs`**: doc comment — set only by `codegen_join_nested_loop`
  before compiling right subtree, consumed only by `codegen_index_probe`, must be `None`
  everywhere else; references phase-ac plan "Codegen context and coupling"
- **`NodeOutput.reset`**: doc comment — the reset contract, which nodes implement it,
  that pass-through nodes propagate from child
- **`codegen_join_nested_loop`**: block comment — both couplings (`reset` + `outer_regs`),
  left registers populated before reset is called, left-column-free zone invariant,
  materializing right children unsupported (panic)
- **`codegen_scan` reset label**: comment — jumped to by nested-loop join per left row;
  standalone callers use init MoveCursor(First) instead

### Tests

**Setup:** two tables in a `TestDb` — `users(id, name)` and `orders(user_id, amount)`.

```rust
#[test]
fn nested_loop_join_scan_right() {
    // Plan: Join { NestedLoop, Scan(users), Scan(orders), on: users.id = orders.user_id }
    // Insert: users[(1,"alice"),(2,"bob")], orders[(1,100),(1,200),(2,300)]
    // Expect combined rows: (1,alice,1,100), (1,alice,1,200), (2,bob,2,300)
}

#[test]
fn nested_loop_join_filter_scan_right() {
    // Plan: Join { NestedLoop, Scan(users), Filter(Scan(orders), amount > 150),
    //              on: users.id = orders.user_id }
    // Expect: (1,alice,1,200), (2,bob,2,300)  [order 100 filtered out]
}

#[test]
fn nested_loop_join_empty_right() {
    // Right side returns no rows for any left row → no output rows
}

#[test]
fn nested_loop_join_empty_left() {
    // Left side returns no rows → no output rows, right side never reset
}
```

### Implementation Steps (1 commit)

#### Step 109.1 — `NodeOutput.reset`; `codegen_join` dispatch; resettable nodes; `codegen_join_nested_loop`; unit tests

**Commit:** `Compiler: add NestedLoop execution path to codegen_join`

---

## 110. `IndexProbe` plan node + compiler codegen (Track 4)

### What Changes

Add `LogicalPlan::IndexProbe`. Wire into EXPLAIN. Implement `codegen_index_probe`.

`IndexProbe` is the dynamic-key counterpart to `IndexScan` — it yields rowids only.
Column fetching is delegated to `RowidLookup` above it, reusing the existing node.
The right-child subtree the optimizer will produce (item 111) is:

```
RowidLookup(IndexProbe { index_rootpage, key_expr, index_col_idx },
            table_rootpage, columns)
```

### Implementation Approach

**`codegen_index_probe`** receives `ctx.outer_regs` to evaluate `key_expr`. Its structure
mirrors `codegen_index_scan` but with a dynamic key instead of a stored literal:

```text
INIT:
  Open(index_cursor, index_rootpage)
  // No MoveCursor here — reset does the positioning each time

BODY:
  RESET:                              ← NodeOutput.reset
    // Evaluate key_expr against outer_regs (left row registers)
    compile_expr(key_expr, outer_regs) → key_reg
    EncodeIndexKey(key_reg, key_reg)
    MoveCursor(index_cursor, Find(key_reg))
    [fall through to CHECK]

  CHECK:                              ← NodeOutput.next
    CanReadCursor(flag, index_cursor); GoToIfFalse(cont.on_done, flag)
    ReadCurrentKey(key_blob, index_cursor)
    BlobStartsWith(matches, key_blob, key_reg); GoToIfFalse(cont.on_done, matches)
    BlobSliceLast(pk_blob, key_blob, 8)
    DecodeU64Key(pk_reg, pk_blob)
    GoTo(cont.on_tuple)

  INDEX_NEXT:
    MoveCursor(index_cursor, Next)
    GoTo(CHECK)
```

Note: `key_reg` is allocated in the reset label body and must remain live through CHECK
and INDEX_NEXT. Allocate it once before RESET and reuse.

The `outer_regs` are read from `ctx.outer_regs.as_ref().expect(...)` at the start of
`codegen_index_probe`. The `RowidLookup` node above calls `codegen` on `IndexProbe`
normally; `ctx.outer_regs` is already set by `codegen_join_nested_loop` at that point.

**EXPLAIN:**

```
IndexProbe [index_col=<col_name>, key=<expr>]
```

### Key Files

- `src/planner/mod.rs` — `IndexProbe` variant
- `src/explain.rs` — render `IndexProbe`
- `src/compiler/nodes.rs` — `codegen_index_probe`; `codegen` dispatch arm

### Code comments required

- **`IndexProbe` in `mod.rs`**: doc comment on `key_expr` — `ColumnRef(i)` is left output
  col `i`, sole exception to left-column-free zone, resolved via `ctx.outer_regs`
- **`codegen_index_probe`**: block comment — `ctx.outer_regs` holds left registers already
  populated by enclosing left loop; reset re-evaluates `key_expr` and re-probes index;
  only rowids yielded, column fetching is `RowidLookup`'s responsibility

### Tests

```rust
#[test]
fn nested_loop_join_index_probe_right() {
    // Setup: users(id, name), orders(user_id, amount), index on orders.user_id
    // Plan (bypass optimizer): Join { NestedLoop,
    //   left: Scan(users),
    //   right: RowidLookup(IndexProbe { key_expr: ColumnRef(0) }, orders, [0,1]),
    //   on_condition: Literal(true),
    // }
    // Insert: users[(1,"alice"),(2,"bob")], orders[(1,100),(1,200),(2,300)]
    // Expect: (1,alice,1,100), (1,alice,1,200), (2,bob,2,300)
}
```

### Implementation Steps (2 commits)

#### Step 110.1 — Planner: `IndexProbe` node; EXPLAIN; stub compiler arm returning `UnsupportedStatement`

**Commit:** `Planner: add IndexProbe node; EXPLAIN rendering`

#### Step 110.2 — Compiler: `IndexProbe` codegen; unit test

**Commit:** `Compiler: IndexProbe codegen — dynamic key probe via outer_regs`

---

## 111. Optimizer: rewrite `NestedLoop(Scan)` to `NestedLoop(IndexProbe)` (Track 4)

### What Changes

New optimizer rule: when a `Join { strategy: NestedLoop }` has an equi-join condition
`left.col = right.col` and a matching index exists on the right table's join column:

1. Replace the right `Scan` with `RowidLookup(IndexProbe { key_expr: ColumnRef(left_col_idx), ... })`
2. Remove the matched equality from `on_condition` (set to `Literal(true)` if it was the
   only condition; leave remaining AND conditions if compound)

The matched equality has the shape `BinaryOp(Equals, ColumnRef(left_col), ColumnRef(right_col_in_combined_space))`. The right column index in combined space is `left_column_count + right_table_col_idx`. The `key_expr` uses the left column index directly: `ColumnRef(left_col_idx)`.

### Implementation Approach

New function `try_index_probe_plan` in `optimizer.rs`, called inside the `Join` match arm:

```rust
LogicalPlan::Join { left, right, on_condition, strategy: JoinStrategy::NestedLoop, left_column_count } => {
    let opt_left = optimize(*left, btree);
    let opt_right = optimize(*right, btree);

    if let Some((new_right, residual_cond)) =
        try_index_probe_plan(&on_condition, &opt_right, *left_column_count, btree)
    {
        return LogicalPlan::Join {
            left: Box::new(opt_left),
            right: Box::new(new_right),
            on_condition: residual_cond,
            strategy: JoinStrategy::NestedLoop,
            left_column_count: *left_column_count,
        };
    }

    // No index: promote to Hash
    LogicalPlan::Join {
        left: Box::new(opt_left),
        right: Box::new(opt_right),
        on_condition,
        strategy: JoinStrategy::Hash,
        left_column_count: *left_column_count,
    }
}
```

`try_index_probe_plan` extracts equi-join pairs from `on_condition` (same approach as
`extract_index_bounds` but matching `ColumnRef = ColumnRef` pairs straddling the
`left_column_count` boundary), looks up indexes on the right table, and if a match is
found returns the rewritten right subtree and residual condition.

### Key Files

- `src/planner/optimizer.rs` — new rule in `optimize`; `try_index_probe_plan` helper

### Tests

```rust
#[test]
fn optimizer_rewrites_nested_loop_to_index_probe_when_index_exists() {
    // Join { NestedLoop, Scan(users), Scan(orders), on: users.id = orders.user_id }
    // with index on orders.user_id
    // → Join { NestedLoop, Scan(users), RowidLookup(IndexProbe(key=ColumnRef(0))), on: true }
}

#[test]
fn optimizer_promotes_nested_loop_to_hash_when_no_index() {
    // Same join but no index → strategy: Hash
}
```

```sql
-- tests/sql/join_index_probe.sql
CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE orders (user_id INTEGER, amount INTEGER)
-- > Table 'orders' created
CREATE INDEX idx_orders_user ON orders (user_id)
-- > Index 'idx_orders_user' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')
-- > 3 rows inserted
INSERT INTO orders VALUES (1, 100), (1, 200), (2, 300)
-- > 3 rows inserted

SELECT users.name, orders.amount FROM users JOIN orders ON orders.user_id = users.id ORDER BY users.name, orders.amount
-- > alice | 100
-- > alice | 200
-- > bob | 300

EXPLAIN SELECT users.name, orders.amount FROM users JOIN orders ON orders.user_id = users.id
-- > 0, "Project [name:1, amount:3]"
-- > 1, "  Join [NestedLoop | true]"
-- > 2, "    Scan users [cols: id, name]"
-- > 3, "    RowidLookup orders [cols: user_id, amount]"
-- > 4, "      IndexProbe [index_col=user_id, key=id:0]"
```

### Implementation Steps (1 commit)

#### Step 111.1 — Optimizer: equi-join + index → `RowidLookup(IndexProbe)` on right side

**Commit:** `Optimizer: rewrite NestedLoop right Scan to IndexProbe for equi-joins with index`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] Existing join SQL tests produce identical results after item 107 (only EXPLAIN label changes)
- [ ] `EXPLAIN` shows `Join [Hash | ...]` or `Join [NestedLoop | ...]`
- [ ] `EXPLAIN` shows `RowidLookup → IndexProbe` as right child when index is used
- [ ] Multi-column ON condition: `JOIN ON a.x = b.x AND a.y = b.y` returns correct rows
- [ ] Three-table join returns correct rows
- [ ] Index-accelerated join: correct rows returned; EXPLAIN confirms `IndexProbe`
- [ ] `nested_loop_join_index_probe_right` unit test passes
- [ ] Empty left / empty right edge cases handled without panic
- [ ] Each commit is independently testable
