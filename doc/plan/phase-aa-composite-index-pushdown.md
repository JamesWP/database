# Phase AA — Composite Index Predicate Pushdown & Sort Elision

Teach the optimizer to use all leading columns of a composite index when WHERE predicates reference them, encoding a composite B-tree key rather than falling back to a full table scan; and to elide a Sort node when ORDER BY matches the trailing columns of the index already used for filtering.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 97 | 4 | Composite index predicate pushdown via composite key encoding | — |
| 98 | 4 | Sort elision for ORDER BY matching trailing composite index columns | 97 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Current limitation

When a query has predicates on multiple columns of a composite index the planner falls back to a full table scan with a post-scan `Filter` node:

```
EXPLAIN SELECT id FROM pairs WHERE a = 10 AND b = 200
-- (index idx_ab exists on (a, b))
0, "Project [id:0]"
1, "  Filter [a:1 = 10 AND b:2 = 200]"
2, "    Scan pairs [cols: id, a, b]"
```

Similarly, when a first-column equality scan is followed by ORDER BY on the second index column, the planner emits an unnecessary Sort node even though the index already delivers rows in that order:

```
EXPLAIN SELECT id FROM pairs WHERE a = 10 ORDER BY b
0, "Project [id:0:0]"
1, "  Sort [b:2:1 ASC]"
2, "    Project [id:0, b:2]"
3, "      RowidLookup pairs [cols: id, a, b]"
4, "        IndexScan via idx_ab [= 10]"
```

### What this phase adds

**Item 97** rewrites `IndexScan` to carry per-column bounds for all constrained leading index columns. The compiler concatenates these into a composite B-tree key. For `WHERE a = 10 AND b = 200` the scan positions with a 16-byte key `[encode(10)][encode(200)]` and terminates at the same prefix — no post-scan filter, O(log N) access to the exact partition. For `WHERE a = 10 AND b > 100` the composite lower key `[encode(10)][encode(100)]` is used for positioning and the scan terminates at the `a = 10` partition boundary.

**Item 98** teaches `can_elide_sort` that when an `IndexScan` has a first-column equality constraint on a composite index, ORDER BY on the second (and subsequent) index columns ASC can be dropped: the index already delivers rows in that order within the fixed first-column partition.

---

## 97. Composite index predicate pushdown via composite key encoding (Track 4)

### What Changes

The `IndexScan` plan node gains a `column_bounds: Vec<IndexColumnBound>` field replacing the existing flat `index_col_idx` / `lower_bound` / `upper_bound` fields. Each entry carries the physical table column index and per-column lower/upper bounds. For a single-column scan this is a one-element Vec and behaviour is unchanged.

The optimizer detects AND predicates that span multiple leading columns of a composite index and populates the new Vec. The compiler concatenates the encoded column values into a composite B-tree key.

### Background

**Composite key format** (already used by the storage layer):

```
[encode(col0_value)] [encode(col1_value)] ... [encode(colN_value)] [8 bytes: rowid]
```

Each `encode()` produces a fixed-width, order-preserving byte sequence. Crucially, the existing `BlobPrefixLe`, `BlobPrefixLt`, and `BlobStartsWith` VM operations compare only the prefix whose length equals the encoded comparison key — so passing a 16-byte key `[encode(10)][encode(200)]` as the upper bound naturally stops the scan at exactly the right place.

The existing `EncodeIndexKey(reg, reg)` operation encodes a single `ScalarValue` from a register. A new `EncodeIndexKeyComposite(dst, srcs)` operation (or equivalent multi-step encoding) is needed to concatenate multiple encoded values into a single blob register.

### New plan node structure

```rust
/// Per-column bounds for one index column.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumnBound {
    /// Physical table column index for this index column.
    pub table_col_idx: usize,
    /// Lower bound: (value, inclusive). None = unbounded below.
    pub lower: Option<(Literal, bool)>,
    /// Upper bound: (value, inclusive). None = unbounded above.
    pub upper: Option<(Literal, bool)>,
}

// In LogicalPlan:
IndexScan {
    index_rootpage: u32,
    /// Ordered bounds for each constrained leading index column.
    /// Invariant: the first N columns of the index are represented in index key order.
    /// For single-column scans: one element (backward-compatible).
    column_bounds: Vec<IndexColumnBound>,
},
```

The existing `index_col_idx` field is removed; the first element's `table_col_idx` gives the same information. All callers (compiler, EXPLAIN, optimizer, tests) are updated.

### Optimizer changes

**`extract_index_bounds`** currently handles single-column predicates and returns `Option<(scan_out_idx, lower, upper)>`. It is extended to also handle AND predicates whose sub-predicates target different leading columns of the same index:

```rust
// New return type:
Option<Vec<IndexColumnBound>>   // one entry per constrained leading column
```

For existing single-column predicates the Vec has one entry (unchanged semantics). For `col1_pred AND col2_pred` where both reference leading columns of the same composite index:

```rust
PlanExpr::BinaryOp { op: And, left, right } => {
    // Try to build multi-column bounds.
    // Extract single-column bound from each side:
    let left_bound  = extract_single_column_bound(left,  scan_columns)?;
    let right_bound = extract_single_column_bound(right, scan_columns)?;

    // Look up an index whose leading columns match both bound columns (in order).
    // Return Vec<IndexColumnBound> ordered by index column position.
    combine_for_composite_index([left_bound, right_bound], indexes)
}
```

`try_index_scan_plan` is updated to pass the full `Vec<IndexColumnBound>` to `LogicalPlan::IndexScan`.

**EXPLAIN format** is updated to show all column bounds:

```
IndexScan via idx_ab [= 10, = 200]      -- two-column equality
IndexScan via idx_ab [= 10, > 100]      -- equality first col, range second col
IndexScan via idx_ab [= 10]             -- unchanged: single-column (first col only)
```

### Compiler changes

`codegen_index_scan` in `src/compiler/nodes.rs` receives `&[IndexColumnBound]` instead of separate `lower_bound` / `upper_bound`. Key changes:

**Composite lower key construction:**
Concatenate the encoded lower bounds of all `column_bounds` entries that have a lower bound, stopping at the first column with no lower bound:

```
lower_key = encode(col0.lower) ++ encode(col1.lower) ++ ...
```

This blob is used for `MoveCursor(Find, lower_key_reg)` — positioning at or after the composite lower key.

For exclusive lower bounds on the last specified column, the existing skip-advance loop (`BlobStartsWith` + `MoveCursor(Next)`) applies to the full composite key.

**Composite upper key construction:**
Concatenate the encoded upper bounds of all columns that have an upper bound:

```
upper_key = encode(col0.upper) ++ encode(col1.upper) ++ ...
            (stop at first column with no upper bound)
```

If the last column with a bound is inclusive, use `BlobPrefixLe(key_blob, upper_key)` to stop.
If exclusive, use `BlobPrefixLt(key_blob, upper_key)`.

For `WHERE a = 10 AND b > 100` the upper key is just `[encode(10)]` (col0 equality, col1 has no upper bound). `BlobPrefixLe(key, [encode(10)])` naturally terminates the scan when `a` changes.

**New VM operation: `AppendEncodedIndexKey(dst, src)`** appends the index-encoded value of `src` to the blob in `dst`. This allows iterative composite key construction:

```
StoreValue(r0, 10)
EncodeIndexKey(r0, r0)         -- r0 = [encode(10)]
StoreValue(r1, 200)
AppendEncodedIndexKey(r0, r1)  -- r0 = [encode(10)][encode(200)]
```

Alternatively, if the number of columns is small and known at compile time, two `EncodeIndexKey` + a `BlobConcat` operation could be used.

### Key Files

- `src/planner/mod.rs` — new `IndexColumnBound` struct; update `LogicalPlan::IndexScan`
- `src/planner/optimizer.rs` — `extract_index_bounds` multi-column AND arm; `try_index_scan_plan` update
- `src/compiler/nodes.rs` — `codegen_index_scan` composite key construction
- `src/engine/program.rs` — new `AppendEncodedIndexKey` operation (or `BlobConcat`)
- `src/engine/mod.rs` — execute new VM operation
- `src/explain.rs` — render composite bounds in EXPLAIN output

### Tests

**Planner unit tests** (extend `optimizer.rs` `#[cfg(test)]`):

```rust
#[test]
fn composite_equality_builds_two_column_index_scan() {
    // WHERE a = 10 AND b = 200 on index(a, b)
    // → IndexScan { column_bounds: [(col0, =10), (col1, =200)] }
    // No Filter node in plan
}

#[test]
fn composite_range_second_col_builds_two_column_index_scan() {
    // WHERE a = 10 AND b > 100 on index(a, b)
    // → IndexScan { column_bounds: [(col0, =10), (col1, >100)] }
    // No Filter node in plan
}

#[test]
fn single_column_predicate_unchanged() {
    // WHERE a = 10 on index(a, b) → single-element column_bounds
}
```

**SQL integration tests** (`tests/sql/index_multi_column.sql` additions):

```sql
-- Composite AND equality: composite key scan, no Filter node
EXPLAIN SELECT id FROM pairs WHERE a = 10 AND b = 200
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup pairs [cols: id, a, b]"
-- > 2, "    IndexScan via idx_ab [= 10, = 200]"

SELECT id FROM pairs WHERE a = 10 AND b = 200
-- > 2

-- Composite AND range: composite lower bound, first-column partition boundary
EXPLAIN SELECT id FROM pairs WHERE a = 10 AND b > 100
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup pairs [cols: id, a, b]"
-- > 2, "    IndexScan via idx_ab [= 10, > 100]"

SELECT id FROM pairs WHERE a = 10 AND b > 100 ORDER BY id
-- > 2
-- > 5

-- Bounded range on second col
SELECT id FROM pairs WHERE a = 10 AND b >= 100 AND b <= 200 ORDER BY id
-- > 1
-- > 2
```

### Implementation Steps (3 commits)

#### Step 97.1 — Add `IndexColumnBound`, update `IndexScan` node and all call sites

Introduce `IndexColumnBound`. Change `LogicalPlan::IndexScan` to use `column_bounds: Vec<IndexColumnBound>`. Update optimizer (single-column path builds a one-element Vec), compiler, EXPLAIN, and all tests. Single-column behaviour is unchanged. All tests pass.

**Commit:** Planner: replace IndexScan flat fields with IndexColumnBound Vec (refactor, no behaviour change)

#### Step 97.2 — Add `AppendEncodedIndexKey` VM operation; composite key construction in compiler

Add the new VM op. Update `codegen_index_scan` to iterate `column_bounds`, build composite lower/upper keys via `AppendEncodedIndexKey`, and use the appropriate `BlobPrefixLe`/`BlobPrefixLt` termination. Single-column path produces identical bytecode (one-element Vec, no `AppendEncodedIndexKey` call). All existing tests still pass.

**Commit:** Compiler: build composite index keys from IndexColumnBound Vec

#### Step 97.3 — Optimizer: detect multi-column AND predicates; build composite IndexScan

Extend `extract_index_bounds` to handle AND predicates targeting different leading columns. Update `try_index_scan_plan` to pass a multi-element Vec when a composite index match is found. Add planner unit tests and SQL integration tests.

**Commit:** Optimizer: composite index predicate pushdown — encode all leading column bounds

---

## 98. Sort elision for ORDER BY matching trailing composite index columns (Track 4)

### What Changes

`can_elide_sort` in `src/planner/optimizer.rs` is extended to recognise that when an `IndexScan` has a first-column equality constraint on a composite index, ORDER BY on the second (and subsequent) index columns ASC can be dropped.

### Background

An index on `(a, b)` stores keys in order `[encode(a)][encode(b)][rowid]`. A scan constrained to `a = 10` (equality on the first column) yields rows in `b ASC` order within that partition. Given:

```sql
SELECT id FROM pairs WHERE a = 10 ORDER BY b
```

the IndexScan already delivers results ordered by `b`, so the Sort is unnecessary. The current `can_elide_sort` (Phase N) checks only whether the single ORDER BY key matches the sole indexed column; it needs to be extended for the composite case.

### Implementation Approach

After item 97, `IndexScan.column_bounds` records all constrained columns. For sort elision the check is:

1. The plan below the Sort must unwrap to a `RowidLookup(IndexScan(...))`.
2. The `IndexScan` must have an equality constraint on its first column (i.e., `column_bounds[0].lower == column_bounds[0].upper` and both are `Some`).
3. Look up the full ordered column list for the index via `BTree::lookup_index_by_rootpage` (new helper).
4. The ORDER BY keys must be a prefix of the *remaining* index columns (i.e., index columns after the equality-constrained prefix), all ASC.

```rust
fn can_elide_sort(plan_below_sort: &LogicalPlan, sort_keys: &[SortKey], btree: &BTree) -> bool {
    let index_scan = unwrap_to_index_scan(plan_below_sort)?;

    // First column must be equality-constrained
    let first = index_scan.column_bounds.first()?;
    if first.lower != first.upper || first.lower.is_none() {
        return false;
    }

    // Count equality prefix length
    let eq_prefix_len = index_scan.column_bounds.iter()
        .take_while(|b| b.lower == b.upper && b.lower.is_some())
        .count();

    // Retrieve full index column list in order
    let index_physical_cols = lookup_index_physical_columns(index_scan.index_rootpage, btree)?;
    let trailing = &index_physical_cols[eq_prefix_len..];

    // ORDER BY must be a prefix of trailing, all ASC
    if sort_keys.len() > trailing.len() { return false; }
    sort_keys.iter().zip(trailing.iter()).all(|(sk, &col)| {
        sk.col_idx == col && sk.direction == SortDirection::Asc
    })
}
```

Only ASC elision is implemented. DESC elision would require a reverse cursor scan, which the VM does not yet support.

**`BTree::lookup_index_by_rootpage`** — new one-liner added to `src/storage/btree.rs`:

```rust
pub fn lookup_index_by_rootpage(&self, rootpage: u32) -> Option<IndexInfo> {
    self.index_cache.values().flatten()
        .find(|i| i.rootpage == rootpage)
        .cloned()
}
```

### Key Files

- `src/planner/optimizer.rs` — extend `can_elide_sort`; add `lookup_index_physical_columns` helper
- `src/storage/btree.rs` — `BTree::lookup_index_by_rootpage`

### Tests

```rust
#[test]
fn sort_elided_for_trailing_index_column() {
    // WHERE a = 10 ORDER BY b ASC with index(a, b) → no Sort node in plan
}

#[test]
fn sort_not_elided_for_range_first_column() {
    // WHERE a > 5 ORDER BY b with index(a, b) → Sort retained (a not fixed)
}

#[test]
fn sort_not_elided_for_desc() {
    // WHERE a = 10 ORDER BY b DESC → Sort retained
}
```

**SQL integration tests** (`tests/sql/index_multi_column.sql` additions):

```sql
-- Sort elision: ORDER BY b matches trailing index column after a = 10 equality
EXPLAIN SELECT id FROM pairs WHERE a = 10 ORDER BY b
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup pairs [cols: id, b]"
-- > 2, "    IndexScan via idx_ab [= 10]"

SELECT id FROM pairs WHERE a = 10 ORDER BY b
-- > 1
-- > 2
-- > 5

-- No elision: first column is a range, b ordering not guaranteed across partitions
EXPLAIN SELECT id FROM pairs WHERE a > 5 ORDER BY b
-- > 0, "Project [id:0:0]"
-- > 1, "  Sort [b:2:1 ASC]"
-- > 2, "    Project [id:0, b:2]"
-- > 3, "      RowidLookup pairs [cols: id, a, b]"
-- > 4, "        IndexScan via idx_ab [> 5]"
```

### Implementation Steps (2 commits)

#### Step 98.1 — Add `BTree::lookup_index_by_rootpage` and `lookup_index_physical_columns`

No behaviour change; infrastructure only.

**Commit:** Storage: add BTree::lookup_index_by_rootpage helper

#### Step 98.2 — Extend `can_elide_sort` for trailing composite index columns

Update `can_elide_sort` to check the equality-prefix length and match ORDER BY against trailing index columns. Add planner unit tests and SQL integration test additions.

**Commit:** Optimizer: elide Sort when ORDER BY matches trailing columns of composite index

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `EXPLAIN … WHERE a = 10 AND b = 200` shows `IndexScan [= 10, = 200]` with no `Filter` node
- [ ] `EXPLAIN … WHERE a = 10 AND b > 100` shows `IndexScan [= 10, > 100]` with no `Filter` node
- [ ] `EXPLAIN … WHERE a = 10 ORDER BY b` shows no `Sort` node
- [ ] `EXPLAIN … WHERE a > 5 ORDER BY b` still shows a `Sort` node
- [ ] `EXPLAIN … WHERE a = 10 ORDER BY b DESC` still shows a `Sort` node
- [ ] Single-column index scans produce identical plans and bytecode as before item 97
- [ ] Query results correct for all new plan shapes
