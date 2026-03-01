# Phase AC — IndexJoin Refactor + Multi-Key Join Support

Refactor `IndexJoin` to use a clean enum for the right-side descriptor, remove dead fields, and extend the optimizer and codegen to support multi-column index probes on join conditions.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 103 | 4.1 | Structural refactor: `IndexJoinRight` enum, `left_key_cols: Vec<usize>`, remove `left_col_count` | — |
| 104 | 4.2 | Optimizer: detect multi-equality AND conditions and match against multi-column indexes | 103 |
| 105 | 4.3 | Codegen: encode composite index key prefix for multi-column index joins | 104 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`IndexJoin` currently has six fields, two of which (`right_covered`, `right_index_positions`) are being added by Phase AD. Without refactoring first, Phase AD would produce a node with 8 fields and a boolean that creates invalid-state combinations.

More importantly, the current `IndexJoin` only supports single-column join conditions. The optimizer's `extract_join_equality` extracts exactly one `Eq(col, col)` pair, and codegen encodes exactly one key value. Multi-column indexes on the right side are therefore never used for joins, even when they would apply.

This phase fixes both issues before Phase AD adds covering-index logic on top of a clean foundation.

### Current `IndexJoin` shape

```rust
IndexJoin {
    left: Box<LogicalPlan>,
    index_rootpage: u32,
    right_table_rootpage: u32,
    right_columns: Vec<usize>,
    left_key_col_idx: usize,    // single key only
    left_col_count: usize,      // unused in codegen (prefixed _)
}
```

### Target shape after this phase

```rust
pub enum IndexJoinRight {
    /// Fetch right columns from the table B-tree via rowid lookup.
    TableLookup {
        table_rootpage: u32,
        columns: Vec<usize>,
    },
    // Covering variant added in Phase AD.
}

IndexJoin {
    left: Box<LogicalPlan>,
    index_rootpage: u32,
    left_key_cols: Vec<usize>,  // one entry per probed index column
    right: IndexJoinRight,
}
```

`left_col_count` is dropped entirely — it was never used after the optimizer emitted the node.

---

## 103. Structural Refactor (Track 4.1)

### What Changes

- Add `IndexJoinRight` enum to `src/planner/mod.rs` with a single `TableLookup` variant (the `Covering` variant comes in Phase AD).
- Replace `right_table_rootpage: u32` + `right_columns: Vec<usize>` with `right: IndexJoinRight`.
- Replace `left_key_col_idx: usize` with `left_key_cols: Vec<usize>`.
- Remove `left_col_count: usize`.
- Update all constructor sites in `optimizer.rs` and tests.
- Update `codegen_index_join` in `nodes.rs`: unpack `right`, use `left_key_cols[0]` for now (behaviour unchanged — single-key only).
- Update `explain.rs`: unpack `right` to get `table_rootpage` and `columns`.
- Update `fuse_projects` and `optimize` in `optimizer.rs`.

### Key Files

- `src/planner/mod.rs` — enum + updated `IndexJoin` variant
- `src/planner/optimizer.rs` — constructor sites, `try_index_join`, `fuse_projects`, `optimize`
- `src/compiler/nodes.rs` — `codegen_index_join`
- `src/explain.rs` — `node_output_cols`, `format_plan`, `plan_children`

### Tests

All existing tests pass unchanged. No behaviour change — this is a pure structural refactor.

### Implementation Steps (1 commit)

#### Step 103.1 — Refactor IndexJoin: IndexJoinRight enum, Vec key cols, drop left_col_count

**Commit:** `Planner: refactor IndexJoin — IndexJoinRight enum, left_key_cols: Vec, drop left_col_count`

---

## 104. Optimizer: Multi-Equality Join Detection (Track 4.2)

### What Changes

- Replace `extract_join_equality` (returns one pair) with `extract_join_equalities` (returns `Vec<(usize, usize)>` for all `AND`-chained equality pairs).
- Replace `find_index_for_column` with `find_index_for_columns`: given a list of right physical column indices, returns an index whose leading columns match them in order.
- Update `try_index_join` to use the new helpers and populate `left_key_cols` with multiple entries when a multi-column index matches.

### Implementation Approach

`extract_join_equalities` walks `AND(AND(Eq(...), Eq(...)), Eq(...))` trees recursively:

```rust
fn extract_join_equalities(
    on_condition: &PlanExpr,
    left_col_count: usize,
) -> Vec<(usize, usize)> { // (left_col_idx, right_col_idx)
    match on_condition {
        PlanExpr::BinaryOp { op: BinaryOp::And, left, right } => {
            let mut pairs = extract_join_equalities(left, left_col_count);
            pairs.extend(extract_join_equalities(right, left_col_count));
            pairs
        }
        PlanExpr::BinaryOp { op: BinaryOp::Equals, left, right } => {
            // extract single Eq(ColumnRef(L), ColumnRef(R)) pair
            ...
        }
        _ => vec![],
    }
}
```

`find_index_for_columns` checks that a candidate index's leading columns match the required physical column indices in order:

```rust
fn find_index_for_columns(
    right_rootpage: u32,
    right_physical_cols: &[usize], // must match index prefix in this order
    btree: &BTree,
) -> Option<u32> { // index rootpage
    ...
}
```

`try_index_join` updated:
1. Collect all equality pairs from the condition.
2. Extract right physical columns in order.
3. Call `find_index_for_columns` — succeeds only if an index has these columns as a prefix.
4. Emit `IndexJoin { left_key_cols: left_cols, right: TableLookup { .. } }`.

Single-column joins continue to work — `left_key_cols` will just have one entry.

### Key Files

- `src/planner/optimizer.rs` — `extract_join_equalities`, `find_index_for_columns`, `try_index_join`

### Tests

- Optimizer unit test: two-column AND condition + matching two-column index → `IndexJoin` with `left_key_cols.len() == 2`.
- Optimizer unit test: two-column AND condition but only a single-column index → falls back to plain `Join`.
- Optimizer unit test: single equality condition → `left_key_cols.len() == 1` (existing behaviour preserved).

### Implementation Steps (1 commit)

#### Step 104.1 — Optimizer: detect multi-equality AND conditions and match multi-column indexes

**Commit:** `Optimizer: detect multi-column equality join conditions and match against index prefixes`

---

## 105. Codegen: Composite Index Key Encoding (Track 4.3)

### What Changes

- `codegen_index_join` encodes all `left_key_cols` into a single composite prefix blob and uses that as the probe key.
- Currently: `EncodeIndexKey(key_reg, left_regs[col])` — encodes one value.
- New: encode each key column value in order and concatenate into a single blob register used for `MoveCursor(Find)` and `BlobStartsWith`.

### Implementation Approach

A new VM operation `EncodeIndexKeyMulti(dest: Reg, src_regs: Vec<Reg>)` encodes multiple values into a single composite key prefix blob, using the same tagged encoding as `EncodeIndexKey` but for an arbitrary number of columns.

Alternatively — and more simply — emit multiple `AppendIndexKey(dest, src)` operations that build up the prefix incrementally:

```
StoreBlob(key_reg, [])         // start with empty blob
AppendIndexKey(key_reg, left_regs[col0])   // append col0 encoding
AppendIndexKey(key_reg, left_regs[col1])   // append col1 encoding
MoveCursor(index_cursor, Find(key_reg))
```

`AppendIndexKey(dest, src)` encodes `src` as a tagged index value and appends it to the blob in `dest`. This is simpler than a variadic op and composes naturally.

For the single-column case, the existing `EncodeIndexKey` can stay as a convenience, or be replaced with `StoreBlob([]) + AppendIndexKey`.

The `BlobStartsWith` check after cursor positioning is unchanged — it already checks whether the current key starts with the probe prefix, which works for both single and multi-column prefixes.

### Key Files

- `src/engine/program.rs` — new `AppendIndexKey(Reg, Reg)` instruction
- `src/engine.rs` — instruction execution
- `src/compiler/nodes.rs` — `codegen_index_join`: loop over `left_key_cols`, emit `AppendIndexKey` for each

### Tests

- New SQL integration test: two-table join where the ON condition matches a two-column index.
- Verify EXPLAIN shows `IndexJoin` (not `Join`) with the two-column index.
- Verify query results match a plain nested-loop `Join`.

### Implementation Steps (1 commit)

#### Step 105.1 — Codegen: AppendIndexKey op and composite probe key in codegen_index_join

**Commit:** `Compiler+VM: AppendIndexKey op and composite index key encoding in codegen_index_join`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] Existing `join_index.sql` passes unchanged (single-column path)
- [ ] New multi-column join SQL test passes
- [ ] EXPLAIN shows `IndexJoin` for a two-column equality join with matching index
- [ ] Plain `Join` still emitted when no matching index prefix exists
