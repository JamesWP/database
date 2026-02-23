# Phase N — Index Maintenance & Sort Elision

Fix indexes going stale on DELETE and teach the planner to skip a redundant ORDER BY sort when an index scan already guarantees the required row order.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 61 | 4 | Add `indexes` field to `LogicalPlan::Delete`; `plan_delete` gathers indexes | — |
| 62 | 4 | Add `DeleteIndex` operation + engine handler | — |
| 63 | 4 | `compile_delete` emits `DeleteIndex` before `DeleteCursor` | 61, 62 |
| 64 | 7 | Index verify command and SQL regression test | 63 |
| 65 | 4 | Sort elision: skip `Sort` when `IndexScan` guarantees required order | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Problem 1 — Indexes go stale on DELETE

When a row is deleted the engine calls `DeleteCursor`, which removes the row from the primary B-tree. None of the secondary indexes are touched. The dangling index entries are never cleaned up and will silently produce wrong results: a subsequent `SELECT … WHERE indexed_col = X` via `IndexScan` finds the stale key, looks up a now-absent rowid, and either panics or returns garbage.

INSERT already does the right thing: `plan_insert` calls `btree.lookup_indexes_for_table()`, passes `Vec<IndexMaintenanceInfo>` through the `LogicalPlan::Insert` node, and `compile_insert` emits `WriteIndex` for every index after the primary write.

The fix mirrors that pattern for DELETE:
1. `plan_delete` gathers `IndexMaintenanceInfo` (item 61).
2. A new `DeleteIndex` operation is added (item 62).
3. `compile_delete` opens index cursors and emits `DeleteIndex` before `DeleteCursor` (item 63).
4. A SQL regression test validates round-trip correctness (item 64).

UPDATE has the same latent bug (index entries not updated) but is left for a follow-up phase; the pattern established here will make it straightforward.

### Problem 2 — Redundant materialise-and-sort for index-ordered queries

`SELECT … WHERE col = X ORDER BY col` compiles to an `IndexScan` wrapping a `Sort` node. The index scan already produces rows in ascending B-tree order (by indexed column), so the sort is wasteful — it materialises all matching rows into a `RowBuffer` and re-sorts them unnecessarily.

The planner can detect this case statically: if `Sort` directly wraps `IndexScan` (or wraps `Project`/`Filter` nodes on top of `IndexScan`), and all sort keys are covered by the index scan's ordering, the `Sort` node is dropped entirely.

---

## 61. Add `indexes` to `LogicalPlan::Delete` (Track 4)

### What Changes

- `LogicalPlan::Delete` gains an `indexes: Vec<IndexMaintenanceInfo>` field.
- `plan_delete` calls `btree.lookup_indexes_for_table()` and populates the field, mirroring `plan_insert`.

### Background

`LogicalPlan::Delete` currently:

```rust
Delete {
    rootpage: u32,
    table_columns: Vec<usize>,
    filter: Option<PlanExpr>,
}
```

`LogicalPlan::Insert` carries:

```rust
Insert {
    rootpage: u32,
    table_columns: Vec<usize>,
    input: Box<LogicalPlan>,
    indexes: Vec<IndexMaintenanceInfo>,
}
```

The `IndexMaintenanceInfo` struct in `src/planner.rs`:

```rust
pub struct IndexMaintenanceInfo {
    pub rootpage: u32,
    pub column_idxs: Vec<usize>,  // which table columns to index
}
```

### Implementation Approach

**`src/planner.rs`** — `LogicalPlan::Delete` struct:

```rust
Delete {
    rootpage: u32,
    table_columns: Vec<usize>,
    filter: Option<PlanExpr>,
    indexes: Vec<IndexMaintenanceInfo>,   // NEW
}
```

**`plan_delete`** (currently near line 1111) — add index gathering after table lookup:

```rust
// after resolving rootpage and table_columns...
let index_infos = btree.lookup_indexes_for_table(&delete.table_name);
let indexes = index_infos.iter().map(|info| {
    let column_idxs = info.column_names.iter()
        .map(|name| table.columns.iter().position(|c| &c.name == name).unwrap())
        .collect();
    IndexMaintenanceInfo { rootpage: info.rootpage, column_idxs }
}).collect();

Ok(LogicalPlan::Delete {
    rootpage: table.rootpage,
    table_columns,
    filter,
    indexes,
})
```

All existing match arms on `LogicalPlan::Delete` (in `src/compiler/nodes.rs`, `src/explain.rs`, etc.) need updating to include the new field (use `..` or add `indexes: _` where unused).

### Key Files

- `src/planner.rs` — `LogicalPlan::Delete` definition; `plan_delete` body
- `src/compiler/nodes.rs` — destructuring of `LogicalPlan::Delete` (add `indexes: _` for now)
- `src/explain.rs` — `Delete` arm in `collect_rows` (add `indexes: _`)

### Tests

The planner unit tests for DELETE already exist. Add:

```rust
#[test]
fn test_plan_delete_gathers_indexes() {
    let btree = make_btree_with_index();  // table + index on col
    let plan = plan_delete_from(&btree, "DELETE FROM users WHERE id = 1");
    if let LogicalPlan::Delete { indexes, .. } = plan {
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].column_idxs, vec![0]);
    } else {
        panic!("expected Delete plan");
    }
}
```

### Implementation Steps (1 commit)

#### Step 61.1 — Add `indexes` field to `LogicalPlan::Delete`

Add field to struct, populate in `plan_delete`, update all destructuring sites with `indexes: _`. Run `cargo test` — all existing tests must still pass.

**Commit:** Planner: add indexes field to LogicalPlan::Delete

---

## 62. Add `DeleteIndex` Operation (Track 4)

### What Changes

A new `DeleteIndex(cursor_reg, value_regs, pk_reg)` operation is added. The engine handler encodes the composite index key (same layout as `WriteIndex`) and deletes it from the index B-tree.

### Background

The index key format (from CLAUDE.md):

> **Key Encoding**: Composite key: `[encoded_column_value][encoded_rowid]`.
> - Column Value: Big-endian `i64` with sign bit flipped (preserves sort order).
> - Rowid: Big-endian `u64`.

`WriteIndex` builds this key and calls `c.insert(key, [])`. `DeleteIndex` builds the identical key and calls `c.delete(&key)` (or the equivalent cursor API).

### Implementation Approach

**`src/engine/program.rs`** — add variant:

```rust
/// Delete a composite index entry.
/// cursor_reg: open cursor on the index B-tree
/// value_regs: registers holding the indexed column values (one per indexed column)
/// pk_reg: register holding the primary key (INTEGER rowid)
DeleteIndex(Reg, Vec<Reg>, Reg),
```

Add `Display` arm:
```rust
DeleteIndex(c, vs, pk) => write!(f, "DeleteIndex  r{c}  [{regs}]  r{pk}",
    regs = vs.iter().map(|r| format!("r{r}")).collect::<Vec<_>>().join(", ")),
```

**`src/engine.rs`** — add match arm (alongside `WriteIndex`):

```rust
DeleteIndex(cursor_reg, value_regs, pk_reg) => {
    // Build the same composite key as WriteIndex
    let mut index_key = Vec::new();
    for &vr in value_regs {
        let v = self.registers.get(vr).scalar().unwrap();
        index_key.extend_from_slice(&storage::encode_index_value(v));
    }
    let pk = match self.registers.get(*pk_reg).scalar().unwrap() {
        ScalarValue::Integer(i) => *i as u64,
        _ => panic!("DeleteIndex: primary key must be INTEGER"),
    };
    index_key.extend_from_slice(&storage::encode_u64_key(pk));

    let cursor = self.registers.get_mut(*cursor_reg).cursor_mut().unwrap();
    let mut c = cursor.open_readwrite();
    // Position at the exact key and delete
    if c.find(&index_key).is_ok() {
        c.delete_current();
    }
}
```

**`src/compiler/emitter.rs`** — add `DeleteIndex` to the exhaustive no-jump arm.

### Key Files

- `src/engine/program.rs` — new `DeleteIndex` variant + Display
- `src/engine.rs` — match arm
- `src/compiler/emitter.rs` — exhaustive arm

### Tests

```rust
#[test]
fn test_delete_index_removes_entry() {
    // Build a minimal program:
    //   Open index cursor (reg 0)
    //   StoreValue(1, INTEGER 42)    -- indexed col
    //   StoreValue(2, INTEGER 1)     -- pk
    //   WriteIndex(0, [1], 2)
    //   -- verify entry exists (via find) --
    //   DeleteIndex(0, [1], 2)
    //   -- verify entry gone --
    //   Halt
    // (use TestDb or construct BTree directly)
}
```

### Implementation Steps (1 commit)

#### Step 62.1 — Add DeleteIndex operation and engine handler

**Commit:** Engine: add DeleteIndex operation

---

## 63. `compile_delete` Emits `DeleteIndex` (Track 4)

### What Changes

`codegen_delete` (in `src/compiler/nodes.rs`) receives the `indexes` field, opens a cursor for each index, reads the indexed column values from the current row in phase 2, and emits `DeleteIndex` for each index before `DeleteCursor`.

### Background

The current two-phase DELETE pattern:

```
Phase 1: scan table → collect rowids into RowBuffer
Phase 2: for each rowid → MoveCursor(Find) → DeleteCursor
```

Phase 2 already positions the main cursor at the exact row via `MoveCursor(Find)`. Before calling `DeleteCursor`, we can `ReadCursor` to load the indexed column values, then emit `DeleteIndex` for each index.

### Implementation Approach

**`src/compiler/nodes.rs`** — update `codegen_delete` signature and body:

```rust
fn codegen_delete(
    ctx: &mut CompileCtx,
    rootpage: u32,
    table_columns: &[usize],
    filter: Option<&PlanExpr>,
    indexes: &[IndexMaintenanceInfo],       // NEW
) { ... }
```

**Step 1: Open index cursors in init phase** (same as `codegen_insert`):

```rust
let mut index_cursor_regs = Vec::new();
for index in indexes {
    let reg = ctx.registers.alloc();
    ctx.init_emitter.emit(Operation::Open(reg, index.rootpage));
    index_cursor_regs.push(reg);
}
```

**Step 2: In phase 2 (deletion loop), after `MoveCursor(Find)`, before `DeleteCursor`**:

```rust
// Allocate registers for indexed column values (reuse across iterations)
let mut index_col_reg_groups: Vec<Vec<Reg>> = indexes.iter()
    .map(|idx| idx.column_idxs.iter().map(|_| ctx.registers.alloc()).collect())
    .collect();

// In the deletion loop body:
// Read the full row into scratch registers
let row_regs: Vec<Reg> = table_columns.iter().map(|_| ctx.registers.alloc()).collect();
ctx.body_emitter.emit(Operation::ReadCursor(cursor_reg, row_regs.clone()));

// Extract indexed column values
for (i, index) in indexes.iter().enumerate() {
    for (j, &col_idx) in index.column_idxs.iter().enumerate() {
        let src = row_regs[col_idx];
        let dst = index_col_reg_groups[i][j];
        ctx.body_emitter.emit(Operation::Move(dst, src));
    }
    ctx.body_emitter.emit(Operation::DeleteIndex(
        index_cursor_regs[i],
        index_col_reg_groups[i].clone(),
        key_reg,
    ));
}

// Then delete from primary table
ctx.body_emitter.emit(Operation::DeleteCursor(cursor_reg));
```

> Note: `ReadCursor` already exists for SELECT; it reads the current cursor row into a set of registers. If there is no `Move` operation, directly use `row_regs[col_idx]` in the `DeleteIndex` call — the `index_col_reg_groups` indirection is not needed.

### Key Files

- `src/compiler/nodes.rs` — `codegen_delete`; pass `indexes` from caller

### Tests

Regression: SQL delete tests (`cargo test test_sql_delete`) must pass.

New integration test:

```rust
#[test]
fn test_delete_keeps_index_in_sync() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE users (id INTEGER, age INTEGER)").unwrap();
    db.execute("CREATE INDEX idx_age ON users(age)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 25)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 30)").unwrap();
    db.execute("INSERT INTO users VALUES (3, 25)").unwrap();

    // Delete one of the age=25 rows
    db.execute("DELETE FROM users WHERE id = 1").unwrap();

    // Query via index — must not see deleted row
    let rows = db.execute_rows("SELECT id FROM users WHERE age = 25").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ScalarValue::Integer(3));

    // Full scan must also agree
    let all = db.execute_rows("SELECT id FROM users").unwrap();
    assert_eq!(all.len(), 2);
}
```

### Implementation Steps (2 commits)

#### Step 63.1 — Thread `indexes` from planner through codegen_delete

Wire `indexes` from the `LogicalPlan::Delete` destructuring down to `codegen_delete`. Add index cursor opens in init phase. No `DeleteIndex` emitted yet; tests pass as before.

**Commit:** Compiler: thread indexes into codegen_delete

#### Step 63.2 — Emit DeleteIndex before DeleteCursor

In the deletion loop: `ReadCursor` → extract indexed columns → `DeleteIndex` for each index → `DeleteCursor`. Add `test_delete_keeps_index_in_sync`.

**Commit:** Compiler: emit DeleteIndex before DeleteCursor to maintain indexes

---

## 64. Index Verify Command and SQL Regression (Track 7)

### What Changes

1. **REPL**: `verify indexes` (or `verify all` extended) verifies all index B-trees registered in `db_schema`, reporting any structural problems.
2. **SQL test file**: `tests/sql/index_maintenance.sql` inserts rows, creates an index, deletes some rows, then queries via the index and via full scan and asserts the results match.

### Background

After item 63, indexes stay in sync during DELETE. But there is no automated test that explicitly validates the index B-tree structure after mutations. Adding one here closes the gap.

The REPL's `verify all` command currently iterates over table entries in `db_schema`. Extending it to also open and verify index B-trees gives an easy sanity check.

### Implementation Approach

**`src/repl/modes/btree.rs`** — update `verify all` handler:

```rust
["verify", "all"] => {
    let entries = self.btree.catalog_entries()?;
    for entry in entries {
        if entry.type_ == "table" || entry.type_ == "index" {
            let handle = self.btree.open(entry.rootpage);
            match handle.open_readonly().verify() {
                Ok(msg) => output.push(format!("{} ({}): {}", entry.name, entry.type_, msg)),
                Err(e) => output.push(format!("{} ({}): FAILED — {}", entry.name, entry.type_, e)),
            }
        }
    }
}
```

**`tests/sql/index_maintenance.sql`** — round-trip test:

```sql
CREATE TABLE products (id INTEGER, price INTEGER)
-- > Table 'products' created

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

INSERT INTO products VALUES (1, 100)
-- > 1 row inserted
INSERT INTO products VALUES (2, 200)
-- > 1 row inserted
INSERT INTO products VALUES (3, 100)
-- > 1 row inserted

DELETE FROM products WHERE id = 1
-- > 1 row deleted

-- Query via index — should not see deleted row
SELECT id FROM products WHERE price = 100
-- > 3

-- Full scan — must agree
SELECT id FROM products ORDER BY id
-- > 2
-- > 3
```

Run `cargo run --bin update-sql-tests index_maintenance` to capture expected output after implementation.

### Key Files

- `src/repl/modes/btree.rs` — extend `verify all`
- `tests/sql/index_maintenance.sql` — new test file

### Tests

`cargo test test_sql_index_maintenance`

### Implementation Steps (2 commits)

#### Step 64.1 — Extend `verify all` to include index B-trees

**Commit:** REPL: verify all now includes index B-trees

#### Step 64.2 — Add index_maintenance.sql regression test

**Commit:** Tests: SQL regression test for index maintenance on DELETE

---

## 65. Sort Elision for Index-Ordered Scans (Track 4)

### What Changes

When the planner would wrap an `IndexScan` with a `Sort` node, and the `Sort` key matches the indexed column in the same direction (ASC), the `Sort` node is dropped entirely.

### Background

Consider:

```sql
CREATE INDEX idx_age ON users(age);
SELECT id FROM users WHERE age > 20 ORDER BY age;
```

Current plan:

```
Sort [age ASC]
  Project [id]
    Filter [age > 20]          ← (generated by full scan path; with index:)
      IndexScan via idx_age [> 20]
```

Because the `IndexScan` traverses the B-tree in ascending key order, it already produces rows sorted by `age ASC`. The `Sort` is redundant.

After this item:

```
Project [id]
  IndexScan via idx_age [> 20, cols: id, age]
```

### V1 Scope

Only eliminate `Sort` when **all** of the following hold:
1. There is exactly **one** sort key.
2. The sort direction is **ASC** (the index always stores ascending).
3. The sort key expression is a `ColumnRef` that resolves to the **single** indexed column.
4. The plan tree between `Sort` and `IndexScan` contains only pass-through nodes: `Project`, `Filter`, `Limit` — no `Join`, `Aggregate`, `Distinct`.

This keeps the implementation simple and correct. Multi-column and DESC cases are left for a future phase.

### Implementation Approach

Add a helper `can_elide_sort` in `src/planner.rs`:

```rust
/// Returns true if `plan` is rooted at an IndexScan on `index_col_idx`
/// with only pass-through nodes (Project/Filter/Limit) between Sort and IndexScan.
fn can_elide_sort(plan: &LogicalPlan, sort_col_idx: usize) -> bool {
    match plan {
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. } => can_elide_sort(input, sort_col_idx),

        LogicalPlan::IndexScan { index_col_idx, .. } => *index_col_idx == sort_col_idx,

        _ => false,
    }
}
```

> `IndexScan` needs to carry `index_col_idx: usize` — the table column index of the indexed column — so that `can_elide_sort` can match it against the sort key. Add this field to `LogicalPlan::IndexScan` (currently it only has `index_rootpage`, `lower_bound`, `upper_bound`). `plan_select` already knows which column is indexed (via `IndexMaintenanceInfo`) when it builds the `IndexScan`.

In `plan_select`, where `LogicalPlan::Sort` is built (currently after applying filter/project/limit):

```rust
// Before wrapping with Sort:
if order_by.len() == 1 && !order_by[0].descending {
    if let PlanExpr::ColumnRef(col_idx) = &sort_keys[0].expr {
        if can_elide_sort(&plan, *col_idx) {
            // skip Sort — index already provides correct order
            return Ok(plan);
        }
    }
}
plan = LogicalPlan::Sort { input: Box::new(plan), sort_keys };
```

### Extending `LogicalPlan::IndexScan`

Add `index_col_idx: usize` (the table column index of the indexed column):

```rust
IndexScan {
    index_rootpage: u32,
    index_col_idx: usize,               // NEW: table column index
    lower_bound: Option<(Literal, bool)>,
    upper_bound: Option<(Literal, bool)>,
    table_rootpage: u32,
    columns: Vec<usize>,
}
```

Update all construction sites (there is one in `plan_select`) and all destructuring sites (compiler's `codegen_index_scan`, `explain.rs`).

### Key Files

- `src/planner.rs` — `LogicalPlan::IndexScan` (add `index_col_idx`); `plan_select` sort elision; `can_elide_sort` helper
- `src/compiler/nodes.rs` — update `IndexScan` destructuring (add `index_col_idx: _`)
- `src/explain.rs` — update `IndexScan` destructuring

### Tests

**Unit test:**

```rust
#[test]
fn test_sort_elided_for_index_scan() {
    let btree = make_btree_with_index_on_age();
    let plan = plan_sql(&btree, "SELECT id FROM users WHERE age > 20 ORDER BY age");
    // Sort node must NOT appear in the plan
    assert!(!plan_contains_sort(&plan), "expected sort to be elided, got:\n{:#?}", plan);
}

#[test]
fn test_sort_not_elided_for_desc() {
    let btree = make_btree_with_index_on_age();
    let plan = plan_sql(&btree, "SELECT id FROM users ORDER BY age DESC");
    assert!(plan_contains_sort(&plan), "DESC should not be elided");
}

#[test]
fn test_sort_not_elided_for_different_column() {
    let btree = make_btree_with_index_on_age();
    let plan = plan_sql(&btree, "SELECT id FROM users ORDER BY id");
    assert!(plan_contains_sort(&plan), "non-indexed column should not be elided");
}
```

**SQL test file `tests/sql/sort_elision.sql`:**

```sql
CREATE TABLE events (id INTEGER, ts INTEGER)
-- > Table 'events' created

CREATE INDEX idx_ts ON events(ts)
-- > Index 'idx_ts' created

INSERT INTO events VALUES (1, 300)
-- > 1 row inserted
INSERT INTO events VALUES (2, 100)
-- > 1 row inserted
INSERT INTO events VALUES (3, 200)
-- > 1 row inserted

-- Should come back in ts order without explicit sort
SELECT id FROM events ORDER BY ts
-- > 2
-- > 3
-- > 1

-- EXPLAIN must show no Sort node
EXPLAIN SELECT id FROM events ORDER BY ts
-- > (update-sql-tests: must not contain "Sort")
```

**EXPLAIN test**: After running `update-sql-tests sort_elision`, verify the EXPLAIN output contains no `Sort` row.

### Implementation Steps (3 commits)

#### Step 65.1 — Add `index_col_idx` to `LogicalPlan::IndexScan`

Add field, update construction in `plan_select`, update all destructuring sites with `index_col_idx: _`. Run `cargo test` — all tests pass.

**Commit:** Planner: add index_col_idx to LogicalPlan::IndexScan

#### Step 65.2 — Implement sort elision in plan_select

Add `can_elide_sort` helper. In `plan_select`, check condition before wrapping with `Sort`. Add unit tests for elision and non-elision cases.

**Commit:** Planner: elide Sort when IndexScan provides correct row order

#### Step 65.3 — SQL tests for sort elision and EXPLAIN verification

Add `tests/sql/sort_elision.sql`. Run `update-sql-tests` to pin expected output. Verify `EXPLAIN` output shows no `Sort` for the indexed ORDER BY case.

**Commit:** Tests: SQL tests for sort elision via index scan

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `cargo test test_sql_delete` — all delete tests pass
- [ ] `cargo test test_sql_index` — all index tests pass
- [ ] `test_delete_keeps_index_in_sync` — passes; stale index entries are gone
- [ ] `test_sort_elided_for_index_scan` — Sort node absent from plan
- [ ] `test_sort_not_elided_for_desc` — Sort node present for DESC
- [ ] `EXPLAIN SELECT … ORDER BY indexed_col` — output contains no `Sort` row
- [ ] `REPL: verify all` includes index B-trees in output
