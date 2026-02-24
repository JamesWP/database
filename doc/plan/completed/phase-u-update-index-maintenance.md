# Phase U — UPDATE Index Maintenance

Fix indexes going stale on UPDATE by deleting the old index entry and writing a new one whenever an indexed column is modified.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 88 | 4 | Add `indexes` field to `LogicalPlan::Update`; `plan_update` gathers indexes | Phase N |
| 89 | 4 | `codegen_update` emits `DeleteIndex` + `WriteIndex` around `WriteCursor` | 88 |
| 90 | 7 | SQL regression tests for UPDATE index maintenance | 89 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Phase N fixed indexes going stale on DELETE. UPDATE has the same latent bug. When a row is updated, only the primary B-tree is modified; secondary indexes still point to the old column values. A subsequent `SELECT … WHERE indexed_col = new_value` via `IndexScan` will miss the row entirely, and `WHERE indexed_col = old_value` will return a row that no longer matches.

The fix follows the exact pattern Phase N established for DELETE. `plan_update` gathers `IndexMaintenanceInfo` (item 88). `codegen_update` uses those indexes to emit `DeleteIndex` with old values and `WriteIndex` with new values around the primary `WriteCursor` (item 89).

### The UPDATE codegen structure (current)

`codegen_update` runs in two phases (see `src/compiler/nodes.rs` ~line 1244):

```
Phase 1: scan → collect matching rowids into RowBuffer
Phase 2: for each rowid:
    MoveCursor(Find, key_reg)
    ReadCursor(read_regs, cursor_reg)        ← old values are here
    [apply assignments into read_regs]        ← new values are here
    WriteCursor(cursor_reg, key_reg, new_values)
    GoTo(update_loop)
```

For index maintenance, the emission order in Phase 2 becomes:

```
    MoveCursor(Find, key_reg)
    ReadCursor(read_regs, cursor_reg)         ← (A) old indexed values available
    DeleteIndex(idx_cursor, old_val_regs, key_reg)    ← remove stale entry
    [apply assignments into read_regs]
    WriteIndex(idx_cursor, new_val_regs, key_reg)     ← insert updated entry
    WriteCursor(cursor_reg, key_reg, new_values)
```

`DeleteIndex` and `WriteIndex` were both added in Phase N, so no new operations are needed.

---

## 88. Add `indexes` to `LogicalPlan::Update` (Track 4)

### What Changes

- `LogicalPlan::Update` gains `indexes: Vec<IndexMaintenanceInfo>`.
- `plan_update` calls `btree.lookup_indexes_for_table()` and populates the field, mirroring `plan_delete` (Phase N item 61).

### Background

`LogicalPlan::Update` currently (in `src/planner.rs`):

```rust
Update {
    rootpage: u32,
    table_columns: Vec<usize>,
    assignments: Vec<(usize, PlanExpr)>,
    filter: Option<PlanExpr>,
}
```

`LogicalPlan::Delete` after Phase N:

```rust
Delete {
    rootpage: u32,
    table_columns: Vec<usize>,
    filter: Option<PlanExpr>,
    indexes: Vec<IndexMaintenanceInfo>,
}
```

`IndexMaintenanceInfo` (already in `src/planner.rs`):

```rust
pub struct IndexMaintenanceInfo {
    pub rootpage: u32,
    pub column_idxs: Vec<usize>,
}
```

### Implementation Approach

**`src/planner.rs`** — `LogicalPlan::Update`:

```rust
Update {
    rootpage: u32,
    table_columns: Vec<usize>,
    assignments: Vec<(usize, PlanExpr)>,
    filter: Option<PlanExpr>,
    indexes: Vec<IndexMaintenanceInfo>,     // NEW
}
```

**`plan_update`** — add index gathering after `table_columns` is built (mirrors `plan_delete`):

```rust
let index_infos = btree.lookup_indexes_for_table(&update.table_name);
let indexes = index_infos.iter().map(|info| {
    let column_idxs = info.column_names.iter()
        .map(|name| table.columns.iter().position(|c| &c.name == name).unwrap())
        .collect();
    IndexMaintenanceInfo { rootpage: info.rootpage, column_idxs }
}).collect();

Ok(LogicalPlan::Update {
    rootpage: table.rootpage,
    table_columns,
    assignments,
    filter,
    indexes,
})
```

All match arms on `LogicalPlan::Update` (in `src/compiler/nodes.rs`, `src/explain.rs`) need `indexes` added — use `indexes: _` at sites that don't use it yet.

### Key Files

- `src/planner.rs` — `LogicalPlan::Update` definition; `plan_update` body
- `src/compiler/nodes.rs` — `LogicalPlan::Update` destructuring (add `indexes: _` for now)
- `src/explain.rs` — `Update` arm in `collect_rows` / `collect_plan_rows` (add `indexes: _`)

### Tests

```rust
#[test]
fn test_plan_update_gathers_indexes() {
    let btree = make_btree_with_index(); // table + index on one column
    let plan = plan_update_from(&btree, "UPDATE users SET age = 30 WHERE id = 1");
    if let LogicalPlan::Update { indexes, .. } = plan {
        assert_eq!(indexes.len(), 1);
    } else {
        panic!("expected Update plan");
    }
}
```

### Implementation Steps (1 commit)

#### Step 88.1 — Add `indexes` field to `LogicalPlan::Update`

Add field to struct, populate in `plan_update`, update all destructuring sites with `indexes: _`. All existing tests pass.

**Commit:** Planner: add indexes field to LogicalPlan::Update

---

## 89. `codegen_update` Emits Index Maintenance (Track 4)

### What Changes

`codegen_update` (in `src/compiler/nodes.rs`) receives the `indexes` slice, opens an index cursor for each index in the init phase, and in Phase 2 emits:

1. `DeleteIndex` with the **old** indexed column values (from `read_regs` immediately after `ReadCursor`).
2. `WriteIndex` with the **new** indexed column values (from `new_values` after assignments are applied).

Both operations use the same `key_reg` (the rowid) already allocated in `codegen_update`.

### Implementation Approach

**`src/compiler/nodes.rs`** — update `codegen_update` signature:

```rust
pub fn codegen_update(
    rootpage: u32,
    table_columns: &[usize],
    assignments: &[(usize, PlanExpr)],
    filter: &Option<PlanExpr>,
    indexes: &[IndexMaintenanceInfo],     // NEW
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput { ... }
```

**Init phase** — open index cursors (same pattern as `codegen_insert` and the Phase N `codegen_delete`):

```rust
let mut index_cursor_regs = Vec::new();
for index in indexes {
    let reg = ctx.registers.alloc();
    ctx.init_emitter.emit(Operation::Open(reg, index.rootpage));
    index_cursor_regs.push(reg);
}
```

**Phase 2 — after `ReadCursor`, before applying assignments** — emit `DeleteIndex` with old values:

```rust
// read_regs holds old column values at this point
for (i, index) in indexes.iter().enumerate() {
    let old_val_regs: Vec<Reg> = index.column_idxs.iter()
        .map(|&ci| read_regs[ci])
        .collect();
    ctx.body_emitter.emit(Operation::DeleteIndex(
        index_cursor_regs[i],
        old_val_regs,
        key_reg,
    ));
}
```

**Phase 2 — after applying assignments** — emit `WriteIndex` with new values:

```rust
// new_values (= read_regs modified by assignments) holds updated column values
for (i, index) in indexes.iter().enumerate() {
    let new_val_regs: Vec<Reg> = index.column_idxs.iter()
        .map(|&ci| new_values[ci])   // new_values[ci] == read_regs[ci] after assignment
        .collect();
    ctx.body_emitter.emit(Operation::WriteIndex(
        index_cursor_regs[i],
        new_val_regs,
        key_reg,
    ));
}
// Then: WriteCursor (primary row)
```

Note: In `codegen_update`, `new_values` is a clone of `read_regs` at line 1344 (`let new_values = read_regs.clone()`), and assignments modify `new_values[col_idx]` in place via `CopyValue`. After the assignments loop, `new_values[ci]` correctly holds the new value for column `ci`, including unchanged columns (still the original `read_regs[ci]` value).

> **Optimisation (optional)**: Only emit `DeleteIndex` + `WriteIndex` for indexes whose indexed column appears in `assignments`. If the indexed column is not being changed, the old and new index entries are identical — deleting and re-inserting wastes work. A simple check: `index.column_idxs.iter().any(|&ci| assignments.iter().any(|(ac, _)| *ac == ci))`. Include this if it is straightforward; skip it and add a TODO comment if it complicates the implementation.

### Key Files

- `src/compiler/nodes.rs` — `codegen_update`; caller at `LogicalPlan::Update` match arm (pass `indexes`)

### Tests

```rust
#[test]
fn test_update_keeps_index_in_sync() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE users (id INTEGER, age INTEGER)").unwrap();
    db.execute("CREATE INDEX idx_age ON users(age)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 25)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 30)").unwrap();

    // Change age of row 1 from 25 to 40
    db.execute("UPDATE users SET age = 40 WHERE id = 1").unwrap();

    // Old value must not be findable via index
    let old = db.execute_rows("SELECT id FROM users WHERE age = 25").unwrap();
    assert!(old.is_empty(), "stale index entry for age=25 must be gone");

    // New value must be findable via index
    let new = db.execute_rows("SELECT id FROM users WHERE age = 40").unwrap();
    assert_eq!(new.len(), 1);
    assert_eq!(new[0][0], ScalarValue::Integer(1));

    // Full scan agrees
    let all = db.execute_rows("SELECT id FROM users ORDER BY id").unwrap();
    assert_eq!(all.len(), 2);
}
```

### Implementation Steps (2 commits)

#### Step 89.1 — Thread `indexes` from planner into codegen_update; open index cursors

Destructure `indexes` from `LogicalPlan::Update`, pass to `codegen_update`, open index cursors in init phase. No `DeleteIndex`/`WriteIndex` emitted yet; all tests pass.

**Commit:** Compiler: thread indexes into codegen_update

#### Step 89.2 — Emit DeleteIndex + WriteIndex in codegen_update

After `ReadCursor` emit `DeleteIndex` (old values); after assignments emit `WriteIndex` (new values). Add `test_update_keeps_index_in_sync`. Run full test suite.

**Commit:** Compiler: emit DeleteIndex + WriteIndex in codegen_update to maintain indexes

---

## 90. SQL Regression Tests (Track 7)

### What Changes

`tests/sql/update_index_maintenance.sql` — round-trip correctness for UPDATE with indexed columns.

### Test File

```sql
-- tests/sql/update_index_maintenance.sql

CREATE TABLE products (id INTEGER, price INTEGER, name TEXT)
-- > Table 'products' created

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

INSERT INTO products VALUES (1, 100, 'apple')
-- > 1 row inserted
INSERT INTO products VALUES (2, 200, 'banana')
-- > 1 row inserted
INSERT INTO products VALUES (3, 100, 'cherry')
-- > 1 row inserted

-- Update price of product 1: index must reflect new value
UPDATE products SET price = 150 WHERE id = 1
-- > 1 row updated

-- Old price must yield only the non-updated rows
SELECT id FROM products WHERE price = 100 ORDER BY id
-- > 3

-- New price must be findable
SELECT id FROM products WHERE price = 150
-- > 1

-- Unaffected row still findable at original price
SELECT id FROM products WHERE price = 200
-- > 2

-- Full scan agrees with index results
SELECT id FROM products ORDER BY id
-- > 1
-- > 2
-- > 3

-- Update non-indexed column: index must be unchanged
UPDATE products SET name = 'avocado' WHERE id = 1
-- > 1 row updated

SELECT id FROM products WHERE price = 150
-- > 1
```

### Key Files

- `tests/sql/update_index_maintenance.sql` — new test file

### Tests

`cargo test test_sql_update_index_maintenance`

### Implementation Steps (1 commit)

#### Step 90.1 — Add SQL regression tests for UPDATE index maintenance

**Commit:** Tests: SQL regression tests for UPDATE index maintenance

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `test_update_keeps_index_in_sync` — old index entry gone, new entry present
- [ ] `cargo test test_sql_update` — all existing UPDATE tests unaffected
- [ ] `cargo test test_sql_update_index_maintenance` — all new tests pass
- [ ] `cargo test test_sql_index` — all existing index tests unaffected
- [ ] Updating a non-indexed column leaves indexes unchanged (no spurious DeleteIndex/WriteIndex)
