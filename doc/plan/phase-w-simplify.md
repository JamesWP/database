# Phase W — Simplify: Targeted Refactors to Reduce Duplication

Four focused refactors that each remove a clear pattern of duplication without touching unrelated code.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 91 | 4 | Planner: extract `gather_indexes` helper shared by `plan_insert`, `plan_delete`, `plan_update` | Phase U |
| 92 | 4 | Planner: move filter-extraction helpers into a single `filter_analysis` module section | — |
| 93 | 4 | Planner: add `column_resolver` method to `schema::Table`; replace duplicated HashMap setup in `plan_update` / `plan_delete` | — |
| 94 | 4 | Compiler: extract `open_index_cursors` / `emit_write_indexes` / `emit_delete_indexes` helpers | Phase U |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

As the codebase has grown, several small patterns have been copy-pasted across multiple sites.
None of these are bugs — the code is correct — but each duplicate makes future changes and reviews harder.
Phase W surgically removes four such patterns. No new features, no behaviour changes; every test must pass before and after each commit.

Items 92 and 93 are independent. Items 91 and 94 both require Phase U to be merged first (so that `plan_update` / `codegen_update` exist with their index-maintenance code).

---

## 91. Planner: extract `gather_indexes` helper (Track 4)

### What Changes

`plan_insert`, `plan_delete`, and `plan_update` all contain the same ~10-line block:

```rust
let index_infos = btree.lookup_indexes_for_table(&table_name);
let mut indexes = Vec::new();
for index_info in index_infos {
    let column_idxs = index_info.column_names.iter()
        .map(|name| table.columns.iter().position(|c| &c.name == name).unwrap())
        .collect();
    indexes.push(IndexMaintenanceInfo { rootpage: index_info.rootpage, column_idxs });
}
```

Extract it into a single private helper so the three callers each become a one-liner.

### Background

- `plan_insert` (line ~1031) — first site
- `plan_delete` (line ~1314) — second site
- `plan_update` (after Phase U, ~line 1280) — third site

All three blocks are functionally identical; only the table-name variable name differs.

### Implementation Approach

Add a private helper immediately before `plan_insert` in `src/planner.rs`:

```rust
/// Collect all secondary-index maintenance records for `table_name`.
fn gather_indexes(table_name: &str, table: &schema::TableInfo, btree: &BTree) -> Vec<IndexMaintenanceInfo> {
    btree.lookup_indexes_for_table(table_name)
        .into_iter()
        .map(|info| {
            let column_idxs = info.column_names.iter()
                .map(|name| {
                    table.columns.iter().position(|c| &c.name == name)
                        .expect("index column not found in table")
                })
                .collect();
            IndexMaintenanceInfo { rootpage: info.rootpage, column_idxs }
        })
        .collect()
}
```

Then replace each existing block:

```rust
// plan_insert
let indexes = gather_indexes(&insert.table_name, &table, btree);

// plan_delete
let indexes = gather_indexes(&delete.table_name, &table, btree);

// plan_update (Phase U)
let indexes = gather_indexes(&update.table_name, &table, btree);
```

### Key Files

- `src/planner.rs` — new `gather_indexes` helper; three call sites changed

### Tests

All existing planner tests cover this path; no new tests needed.
Run `cargo test` and verify zero failures and zero warnings.

### Implementation Steps (1 commit)

#### Step 91.1 — Extract `gather_indexes` helper in planner

**Commit:** Planner: extract gather_indexes helper; remove duplication across plan_insert/delete/update

---

## 92. Planner: consolidate filter-extraction helpers (Track 4)

### What Changes

`src/planner.rs` currently has six small private functions scattered ~lines 1129–1228:

| Function | Returns | Used by |
|----------|---------|---------|
| `extract_range_filter` | `Option<(String, Option<(Literal, bool)>, Option<(Literal, bool)>)>` | `try_plan_index_scan` |
| `extract_single_range` | same tuple | `extract_range_filter` |
| `extract_is_null_filter` | `Option<String>` | `try_plan_index_scan` |
| `extract_equality_filter` | `Option<(String, Literal)>` | `try_plan_index_scan` |
| `extract_column_name` | `Option<String>` | `extract_range_filter`, `extract_equality_filter`, `extract_is_null_filter` |
| `extract_literal` | `Option<Literal>` | `extract_range_filter`, `extract_equality_filter` |

These six free functions are logically a single group. Moving them to a clearly-demarcated section (a `// ── Filter Analysis ──` block or a `mod filter_analysis`) makes the structure of the planner easier to scan.

### Background

This is a readability / grouping refactor. No logic changes.
`try_plan_index_scan` (line ~1061) is the only external caller — it will keep working unchanged.

### Implementation Approach

**Option A — Section header (minimal change):**
Add a clearly-labelled comment block above the six functions:

```rust
// ============================================================================
// Filter Analysis — extract index-scannable predicates from AST expressions
// ============================================================================
```

Move `extract_column_name` and `extract_literal` immediately before the functions that use them, so the reading order is top-to-bottom without forward references:

```
extract_column_name
extract_literal
extract_is_null_filter
extract_equality_filter
extract_single_range
extract_range_filter
```

**Option B — Private inline module (stronger boundary):**

```rust
mod filter_analysis {
    use super::*;

    pub(super) fn extract_equality(expr: &ast::Expression) -> Option<(String, Literal)> { … }
    pub(super) fn extract_is_null(expr: &ast::Expression) -> Option<String> { … }
    pub(super) fn extract_range(expr: &ast::Expression) -> … { … }

    fn extract_column_name(expr: &ast::Expression) -> Option<String> { … }
    fn extract_literal(expr: &ast::Expression) -> Option<Literal> { … }
    fn extract_single_range(expr: &ast::Expression) -> … { … }
}
```

Callers become `filter_analysis::extract_equality(filter)` etc.

Prefer **Option B** — the `mod` creates a hard visibility boundary that prevents accidental new callers.
If Option B causes significant friction (e.g., type imports), fall back to Option A.

### Key Files

- `src/planner.rs` — rearrange ~lines 1061–1228; update call sites inside `try_plan_index_scan`

### Tests

All existing planner tests. No new tests needed.

### Implementation Steps (1 commit)

#### Step 92.1 — Group filter-extraction helpers into a `filter_analysis` section

**Commit:** Planner: group filter-extraction helpers into filter_analysis module

---

## 93. Planner: add `column_resolver` to `schema::Table` (Track 4)

### What Changes

`plan_update` and `plan_delete` each contain the same 6-line block that builds a `HashMap<String, usize>` and a `SingleTableResolver` from it:

```rust
// plan_update (lines ~1243–1253)  and  plan_delete (lines ~1293–1302)
let mut column_map = HashMap::new();
for (i, col) in table.columns.iter().enumerate() {
    column_map.insert(col.name.clone(), i);
}
let resolver = SingleTableResolver {
    table_ref: &table_name,
    columns: &column_map,
};
```

Add a method to `schema::Table` that produces the map in one call, then remove both duplicates.

### Background

`schema::Table` already has `get_column_index(&self, name) -> Option<usize>`, which does a linear scan.
`plan_select` avoids this pattern via `build_column_mapping()` + `SingleTableResolver`, but that helper also handles column-subset selection (not all columns are always needed for a SELECT). For UPDATE and DELETE, every column is always needed, so the full sequential map is correct and simpler.

`plan_insert` does not have this block — it only calls `table.get_column_index(name)` per column when the INSERT specifies an explicit column list, which is already fine.

### Implementation Approach

Add one method to `schema::Table` in `src/planner.rs` (inside the `schema` module):

```rust
impl Table {
    // existing:
    pub fn get_column_index(&self, name: &str) -> Option<usize> { … }

    // new:
    /// Build a name→index map for all columns in schema order.
    pub fn column_name_map(&self) -> HashMap<String, usize> {
        self.columns.iter().enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect()
    }
}
```

Then replace the duplicated blocks:

```rust
// plan_update and plan_delete — replace the 6-line block with:
let column_map = table.column_name_map();
let resolver = SingleTableResolver {
    table_ref: &table_name,
    columns: &column_map,
};
```

`HashMap` is already imported in scope at both call sites.

Note: `column_name_map` lives on `schema::Table` rather than as a free function because it only uses `self.columns` — no `BTree` or statement context needed — and it gives the method a stable, discoverable home if a third call site appears (e.g. a future `plan_replace`).

### Key Files

- `src/planner.rs` — new `column_name_map` method on `schema::Table`; two call sites in `plan_update` and `plan_delete`

### Tests

All existing planner tests cover both paths. No new tests needed.

### Implementation Steps (1 commit)

#### Step 93.1 — Add `column_name_map` to `schema::Table`; use in `plan_update` and `plan_delete`

**Commit:** Planner: add column_name_map to schema::Table; remove duplicated HashMap setup

---

## 94. Compiler: extract index cursor codegen helpers (Track 4)

### What Changes

After Phase U, three codegen functions open index cursors and emit `WriteIndex` / `DeleteIndex` with nearly identical code:

**`codegen_insert`** (lines ~1131–1135, ~1210–1222):
```rust
let mut index_cursor_regs = Vec::new();
for index in indexes {
    let reg = ctx.registers.alloc();
    ctx.init_emitter.emit(Operation::Open(reg, index.rootpage));
    index_cursor_regs.push(reg);
}
// … later …
for (i, index) in indexes.iter().enumerate() {
    let col_regs: Vec<_> = index.column_idxs.iter().map(|&c| reordered_regs[c]).collect();
    ctx.body_emitter.emit(Operation::WriteIndex(index_cursor_regs[i], col_regs, key_reg));
}
```

**`codegen_delete`** (lines ~1563–1570, ~1658–1669): same open pattern; `DeleteIndex` instead.

**`codegen_update`** (Phase U): open + `DeleteIndex` (old values) + `WriteIndex` (new values).

Extract three helpers that can be called from all three codegen functions.

### Background

The `WriteIndex` / `DeleteIndex` operations were introduced in Phase G4 and are emitted by the same init/body emitter API. The opening pattern is always identical; the emit pattern differs only in which registers hold the column values.

### Implementation Approach

Add three free functions in `src/compiler/nodes.rs`, near the top of the file (before `codegen_insert`):

```rust
/// Open one index cursor register per entry in `indexes` during the init phase.
/// Returns the allocated cursor registers in the same order as `indexes`.
fn open_index_cursors(
    indexes: &[IndexMaintenanceInfo],
    ctx: &mut CodegenContext,
) -> Vec<Reg> {
    indexes.iter().map(|index| {
        let reg = ctx.registers.alloc();
        ctx.init_emitter.emit(Operation::Open(reg, index.rootpage));
        reg
    }).collect()
}

/// Emit a `WriteIndex` for each index using the given column-value registers.
/// `row_regs[col_idx]` must hold the current value for column `col_idx`.
fn emit_write_indexes(
    indexes: &[IndexMaintenanceInfo],
    index_cursor_regs: &[Reg],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for (index, &cursor_reg) in indexes.iter().zip(index_cursor_regs) {
        let col_regs: Vec<Reg> = index.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter.emit(Operation::WriteIndex(cursor_reg, col_regs, key_reg));
    }
}

/// Emit a `DeleteIndex` for each index using the given column-value registers.
fn emit_delete_indexes(
    indexes: &[IndexMaintenanceInfo],
    index_cursor_regs: &[Reg],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for (index, &cursor_reg) in indexes.iter().zip(index_cursor_regs) {
        let col_regs: Vec<Reg> = index.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter.emit(Operation::DeleteIndex(cursor_reg, col_regs, key_reg));
    }
}
```

Then replace each inline block in `codegen_insert`, `codegen_delete`, and `codegen_update`:

```rust
// codegen_insert
let index_cursor_regs = open_index_cursors(indexes, ctx);
// … existing write cursor …
emit_write_indexes(indexes, &index_cursor_regs, &reordered_regs, key_reg, ctx);

// codegen_delete
let index_cursor_regs = open_index_cursors(indexes, ctx);
// … read cursor …
emit_delete_indexes(indexes, &index_cursor_regs, &phase2_read_regs, key_reg, ctx);

// codegen_update
let index_cursor_regs = open_index_cursors(indexes, ctx);
// … read cursor (old values) …
emit_delete_indexes(indexes, &index_cursor_regs, &read_regs, key_reg, ctx);
// … apply assignments …
emit_write_indexes(indexes, &index_cursor_regs, &new_values, key_reg, ctx);
```

### Key Files

- `src/compiler/nodes.rs` — three new helpers; `codegen_insert`, `codegen_delete`, `codegen_update` simplified

### Tests

All existing compiler and SQL integration tests cover this path; no new tests needed.
Specifically verify: `cargo test test_sql_index`, `cargo test test_sql_delete`, `cargo test test_sql_update`.

### Implementation Steps (1 commit)

#### Step 94.1 — Extract open_index_cursors / emit_write_indexes / emit_delete_indexes helpers

**Commit:** Compiler: extract index cursor codegen helpers; remove duplication in insert/delete/update

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable and leaves all tests green
- [ ] Item 91: `gather_indexes` called in all three plan_* functions; no inline index-gathering remains
- [ ] Item 92: filter_analysis module (or section) groups all six helper functions; only `try_plan_index_scan` calls them
- [ ] Item 93: `schema::Table::column_name_map` exists; `plan_update` and `plan_delete` each reduced to a 3-line resolver setup
- [ ] Item 94: `open_index_cursors`, `emit_write_indexes`, `emit_delete_indexes` exist; no inline equivalents remain in the three codegen functions
- [ ] No behaviour changes: all SQL integration tests produce identical output before and after
