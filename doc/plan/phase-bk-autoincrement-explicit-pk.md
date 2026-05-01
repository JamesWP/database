# Phase BK — AUTOINCREMENT Explicit PK Insert

Fix the stub introduced in Phase BI: when a user supplies an explicit value for an `INTEGER PRIMARY KEY AUTOINCREMENT` column, use that value as the B-tree key and advance the rowid cache past it.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 1 | 2.1 | Replace `fill_autoincrement_at: Option<usize>` with `AutoincrementMode` enum in planner and compiler | Phase BI |
| 2 | 2.2 | Compiler: emit `CopyValue(key_reg, pk_value_reg)` for `Explicit` mode before `WriteCursor` | BK-1 |
| 3 | 7.1 | SQL integration tests: explicit PK insert + verify next auto-assign continues from max+1 | BK-2 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Phase BI introduced `INTEGER PRIMARY KEY AUTOINCREMENT` support, but only for the case where the user *omits* the PK column from the INSERT column list. In that mode the compiler emits `InitRowid` to pick the next auto-rowid, assigns that rowid as the B-tree key, and copies it into the data row's PK slot — correct.

The stub left unimplemented: when the user *provides* an explicit PK value (e.g. `INSERT INTO votes (id, ...) VALUES (100, ...)`), the column is present in `table_columns` and `fill_autoincrement_at` is `None`. The planner emits a normal `Values` row containing the literal 100. But `codegen_insert` still uses `key_reg` (the result of `InitRowid`) as the B-tree key — the user's 100 only lives in the data registers, not in the key slot. The row is reachable only by scanning, not by key lookup. Worse, the rowid cache is not advanced past 100, so a subsequent auto-insert may collide or produce a key lower than 100.

The fix requires two cooperating changes:

1. **Planner**: replace `Option<usize>` with an `AutoincrementMode` enum that distinguishes three cases:
   - `None` — no autoincrement column (existing INSERT behavior unchanged).
   - `Fill { pk_col }` — PK omitted; compiler fills from auto-rowid (existing Phase BI behavior).
   - `Explicit { pk_col }` — PK was supplied by the user; compiler must use that value as the B-tree key.

2. **Compiler**: for `Explicit { pk_col }`, after reordering child registers, emit `CopyValue(key_reg, reordered_regs[pk_col])` to load the user-supplied value into `key_reg` before index writes and `WriteCursor`. No new VM operations are needed: `WriteCursor` already advances the rowid cache via `if next > current { set_cached_next_rowid(next + 1) }`, so a user key of 100 will naturally advance the cache to 101.

---

## Stubs

None.

---

## 1. AutoincrementMode Enum (Track 2.1)

### What Changes

Replace the `fill_autoincrement_at: Option<usize>` field on `LogicalPlan::Insert` with an `AutoincrementMode` field.

### Background

`Option<usize>` encodes two meanings with the same type — "no autoincrement" (`None`) and "PK omitted, fill from rowid" (`Some(i)`). Phase BK adds a third state ("PK supplied by user"), which cannot be represented without introducing an invalid-state combination (e.g. `fill_autoincrement_at: None` with a secret side-band flag). An enum is cleaner and self-documenting.

### Implementation Approach

**New type** in `src/planner/mod.rs` (alongside `LogicalPlan`):

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum AutoincrementMode {
    /// No AUTOINCREMENT column; use auto-rowid as usual.
    None,
    /// PK column was omitted from INSERT; fill slot pk_col from auto-rowid.
    Fill { pk_col: usize },
    /// PK column was explicitly provided; use its value as the B-tree key.
    Explicit { pk_col: usize },
}
```

**`LogicalPlan::Insert`** change:

```rust
Insert {
    rootpage: u32,
    table_columns: Vec<usize>,
    input: Box<LogicalPlan>,
    indexes: Vec<IndexMaintenanceInfo>,
    autoincrement: AutoincrementMode,  // replaces fill_autoincrement_at: Option<usize>
},
```

**`src/planner/dml.rs`** (`plan_insert`) — change the `fill_autoincrement_at` computation:

```rust
let autoincrement = match (autoincrement_col_idx, &insert.columns) {
    (None, _) => AutoincrementMode::None,
    (Some(pk_idx), Some(col_names)) => {
        let cols_contain_pk = col_names.iter().any(|n| {
            table.get_column_index(n) == Some(pk_idx)
        });
        if cols_contain_pk {
            AutoincrementMode::Explicit { pk_col: pk_idx }
        } else {
            AutoincrementMode::Fill { pk_col: pk_idx }
        }
    }
    (Some(pk_idx), None) => {
        // positional: same "one short" logic as Phase BI
        if first_row_len == Some(num_table_columns - 1) {
            AutoincrementMode::Fill { pk_col: pk_idx }
        } else {
            AutoincrementMode::Explicit { pk_col: pk_idx }
        }
    }
};
```

**`src/compiler/nodes.rs`** — update `codegen_insert` signature:

```rust
pub fn codegen_insert(
    rootpage: u32,
    table_columns: &[usize],
    input: &LogicalPlan,
    indexes: &[IndexMaintenanceInfo],
    autoincrement: &AutoincrementMode,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput { ... }
```

All call-sites and test `LogicalPlan::Insert` constructors must be updated (mechanical `fill_autoincrement_at` → `autoincrement` rename).

### Key Files

- `src/planner/mod.rs` — add `AutoincrementMode` enum; update `LogicalPlan::Insert`
- `src/planner/dml.rs` — update `plan_insert` to produce `AutoincrementMode`
- `src/compiler/nodes.rs` — update `codegen_insert` signature; update all `LogicalPlan::Insert` match arms

### Tests

Existing Phase BI tests continue to cover `Fill` mode. No new test needed for this item alone — covered in BK-3.

### Implementation Steps (2 commits)

#### Step 1.1 — Add AutoincrementMode enum and update LogicalPlan::Insert
**Commit:** `planner: replace fill_autoincrement_at with AutoincrementMode enum`

1. Add `AutoincrementMode` to `src/planner/mod.rs`.
2. Change `fill_autoincrement_at: Option<usize>` → `autoincrement: AutoincrementMode` on `LogicalPlan::Insert`.
3. Update `plan_insert` in `src/planner/dml.rs` to produce `AutoincrementMode` (including the `Explicit` variant when the user provides the PK column).
4. Update all `LogicalPlan::Insert { ... }` constructors in tests (use `autoincrement: AutoincrementMode::None`).
5. `cargo test --workspace` — all existing tests must pass.

#### Step 1.2 — Update compiler to consume AutoincrementMode
**Commit:** `compiler: consume AutoincrementMode in codegen_insert`

1. Update `codegen_insert` signature to accept `&AutoincrementMode`.
2. Replace the `fill_autoincrement_at` branch with a match on `AutoincrementMode`:
   - `None` → no change to `reordered_regs`.
   - `Fill { pk_col }` → `reordered_regs[pk_col] = key_reg` (same as before).
   - `Explicit { pk_col }` → emit `CopyValue(key_reg, reordered_regs[pk_col])` (see BK-2 below — can land in the same commit).
3. `cargo test --workspace`.

---

## 2. Compiler: CopyValue for Explicit Mode (Track 2.2)

### What Changes

For `AutoincrementMode::Explicit { pk_col }`, before emitting index writes and `WriteCursor`, copy the user-supplied PK value into `key_reg` so it becomes the B-tree key.

### Background

`InitRowid` sets `key_reg` to the next auto-rowid at the start of each INSERT batch. For `Explicit` mode the user has already placed the desired PK value in `reordered_regs[pk_col]`. The fix is a single `CopyValue(key_reg, reordered_regs[pk_col])` before any writes. After `WriteCursor` runs, its existing logic advances the rowid cache: `if next_rowid > current_cache { set_cached_next_rowid(next_rowid + 1) }`, so the cache is automatically advanced past the user-supplied value.

### Implementation Approach

In `codegen_insert`, after the `reordered_regs` block (reordering + Fill handling), add:

```rust
match autoincrement {
    AutoincrementMode::Fill { pk_col } => {
        reordered_regs[*pk_col] = key_reg;
    }
    AutoincrementMode::Explicit { pk_col } => {
        body!(ctx; CopyValue(key_reg, reordered_regs[*pk_col]));
    }
    AutoincrementMode::None => {}
}
```

`CopyValue` already exists in the VM; no new operations are needed.

### Key Files

- `src/compiler/nodes.rs` — `codegen_insert`: add `Explicit` arm with `CopyValue`

### Tests

Covered by BK-3 SQL integration tests.

### Implementation Steps (1 commit)

This change lands in the same commit as Step 1.2 above — they are tightly coupled and not independently testable.

---

## 3. SQL Integration Tests (Track 7.1)

### What Changes

Add explicit-PK test cases to `tests/sql/autoincrement.sql`.

### Tests

```sql
-- Explicit PK then auto: cache advances past explicit value
CREATE TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT)
INSERT INTO seq (id, label) VALUES (100, 'explicit')
INSERT INTO seq (label) VALUES ('auto-a')
INSERT INTO seq (label) VALUES ('auto-b')
SELECT id, label FROM seq ORDER BY id
-- > 100, "explicit"
-- > 101, "auto-a"
-- > 102, "auto-b"

-- Explicit PK respects uniqueness (duplicate explicit key)
INSERT INTO seq (id, label) VALUES (100, 'dupe')
-- > ERROR: constraint violation

-- Mixed: auto then explicit higher than cache; next auto continues from there
CREATE TABLE seq2 (id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)
INSERT INTO seq2 (v) VALUES (1)
INSERT INTO seq2 (v) VALUES (2)
INSERT INTO seq2 (id, v) VALUES (50, 99)
INSERT INTO seq2 (v) VALUES (3)
SELECT id, v FROM seq2 ORDER BY id
-- > 1, 1
-- > 2, 2
-- > 50, 99
-- > 51, 3
```

### Implementation Steps (1 commit)

#### Step 3.1 — Add explicit-PK SQL tests
**Commit:** `tests: add explicit PK autoincrement integration tests`

1. Add the test cases above to `tests/sql/autoincrement.sql`.
2. Run `cargo run --bin update-sql-tests autoincrement` to generate expected output.
3. Run `cargo test test_sql_autoincrement` to confirm.

---

## Verification

- [ ] Tests pass: `cargo test --workspace`
- [ ] Zero warnings: `cargo fmt --all && cargo build --workspace 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] Explicit PK rows are retrievable by key lookup (not just scan)
- [ ] Rowid cache advances past explicit PK value so next auto-insert gets `max(pk)+1`
