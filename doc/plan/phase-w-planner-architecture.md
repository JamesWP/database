# Phase W — Planner Architecture

Restructure `src/planner.rs` (3584 lines, one file) into a module with clear DDL/DML/Query boundaries, and introduce a two-phase SELECT planner that separates naive plan construction from optimisation.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 91 | 4 | Extract `src/planner/schema.rs` — schema types, `resolve_table`, `column_name_map` | — |
| 92 | 4 | Split planner into modules: `mod.rs`, `resolver.rs`, `ddl.rs`, `dml.rs`, `select.rs` | 91 |
| 93 | 4 | Two-phase SELECT: `select.rs` produces naive plan; new `optimizer.rs` applies index-scan and sort-elision rules | 92 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`src/planner.rs` has grown to 3584 lines by accumulating four unrelated concerns in a single file:
- **Schema resolution** — looking up tables, mapping names to column indices
- **DDL planning** — CREATE TABLE, CREATE INDEX
- **DML planning** — INSERT, UPDATE, DELETE (including index-maintenance info)
- **Query planning** — SELECT, including name resolution, naive plan construction, and query optimisation

The two optimisation rules (index scan selection, sort elision) are interleaved in the middle of `plan_select`, making both the optimisation logic and the translation logic harder to read and test independently.

This phase splits the file into a `src/planner/` module and introduces a clean two-phase architecture for SELECT planning: a naive phase that only translates AST → `LogicalPlan`, and an optimiser phase that rewrites the plan tree.

No public API changes. No behaviour changes. Every test must pass before and after each commit.

---

## 91. Extract `src/planner/schema.rs` (Track 4)

### What Changes

The `schema` module currently lives as a nested `pub mod schema { … }` block inside `planner.rs` (~lines 270–302). Move it to its own file and add the `column_name_map` helper that is currently duplicated in `plan_update` and `plan_delete`.

### Background

`schema::Table` is the load-bearing type passed through all of planner: `resolve_table` returns it, every `plan_*` function receives it. It has no reason to be buried inside a 3584-line file. Moving it to a dedicated file makes it immediately findable and gives it a stable home for future additions (column types, constraints, etc.).

`column_name_map` removes a 5-line `HashMap` construction that is duplicated in `plan_update` and `plan_delete`. The method belongs on `schema::Table` because it only depends on `self.columns`.

`resolve_table` also moves here — it is pure schema logic (BTree lookup + DDL parse → `schema::Table`) with no dependency on the planner's own types.

### Implementation Approach

Create `src/planner/schema.rs`:

```rust
use std::collections::HashMap;
use crate::frontend::{ast::Statement, parser::parse};
use crate::storage::BTree;
use super::PlanError;

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub rootpage: u32,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

impl Table {
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Build a name→index map for all columns in schema order.
    pub fn column_name_map(&self) -> HashMap<String, usize> {
        self.columns.iter().enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect()
    }
}

pub fn resolve_table(table_name: &str, btree: &BTree) -> Result<Table, PlanError> {
    let (rootpage, sql) = btree
        .lookup_table(table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;
    let stmt = parse(&sql).map_err(|_| PlanError::UnsupportedStatement)?;
    let create = match stmt {
        Statement::CreateTable(c) => c,
        _ => return Err(PlanError::UnsupportedStatement),
    };
    let columns = create.columns.into_iter()
        .map(|col| Column { name: col.name })
        .collect();
    Ok(Table { name: table_name.to_string(), rootpage, columns })
}
```

In `src/planner/mod.rs`, replace the inline `pub mod schema { … }` block with:

```rust
pub mod schema;
use schema::{resolve_table, Table};
```

Update `plan_update` and `plan_delete` to use `table.column_name_map()`:

```rust
// Before (both functions):
let mut column_map = HashMap::new();
for (i, col) in table.columns.iter().enumerate() {
    column_map.insert(col.name.clone(), i);
}

// After:
let column_map = table.column_name_map();
```

### Key Files

- `src/planner/schema.rs` — new file
- `src/planner/mod.rs` — `pub mod schema;` replaces inline block; two call sites updated

### Tests

All existing planner tests. No new tests needed for this mechanical move.

### Implementation Steps (2 commits)

#### Step 91.1 — Convert `planner.rs` to `planner/mod.rs`

Create `src/planner/` directory, move `src/planner.rs` to `src/planner/mod.rs`. Verify `cargo test` passes unchanged.

**Commit:** Planner: convert planner.rs to planner/mod.rs

#### Step 91.2 — Extract schema module; add `column_name_map`

Move `pub mod schema { … }` to `src/planner/schema.rs`. Add `column_name_map`. Update `plan_update` and `plan_delete`.

**Commit:** Planner: extract schema module to planner/schema.rs; add column_name_map

---

## 92. Split planner into DDL, DML, resolver, and select modules (Track 4)

### What Changes

After item 91, `src/planner/mod.rs` is still ~3500 lines. Move each concern to its own file:

| File | Contents | ~Lines |
|------|----------|--------|
| `src/planner/mod.rs` | Public types, module declarations, `plan()` entry point | ~200 |
| `src/planner/schema.rs` | (from item 91) | ~60 |
| `src/planner/resolver.rs` | `ColumnResolver` trait and impls, `convert_expr`, `build_column_mapping`, `collect_columns*`, `remap_column_indices` | ~400 |
| `src/planner/ddl.rs` | `plan_create_table`, `plan_create_index` | ~100 |
| `src/planner/dml.rs` | `plan_insert`, `plan_update`, `plan_delete`, `gather_indexes` | ~400 |
| `src/planner/select.rs` | `plan_select`, `plan_select_with_joins`, optimisation helpers (temporary) | ~700 |

### Background

The split follows natural dependency boundaries:

```
schema.rs   ← no planner deps
resolver.rs ← schema.rs, AST
ddl.rs      ← schema.rs, resolver.rs
dml.rs      ← schema.rs, resolver.rs   (gather_indexes lives here)
select.rs   ← schema.rs, resolver.rs   (optimisation helpers live here until item 93)
mod.rs      ← all of the above
```

`gather_indexes` (previously a standalone item in an earlier draft of this phase) is written as a private helper inside `dml.rs`. It is only ever called by the three DML planners, so `dml.rs` is its natural home — no separate phase needed.

The filter-extraction helpers (`extract_equality_filter`, `extract_is_null_filter`, `extract_range_filter`, `extract_column_name`, `extract_literal`, `extract_single_range`) stay with `select.rs` for now and move to `optimizer.rs` in item 93.

This is a pure file-reorganisation step. The rule is: move code, change `use` paths, adjust visibility (`pub(super)` / `pub(crate)`) as needed, verify `cargo build` after each sub-step.

### Key Files

- `src/planner/resolver.rs` — new
- `src/planner/ddl.rs` — new
- `src/planner/dml.rs` — new (includes private `gather_indexes`)
- `src/planner/select.rs` — new
- `src/planner/mod.rs` — slimmed to public API

### Tests

All existing planner and SQL integration tests. No new tests needed.

### Implementation Steps (1 commit per module)

#### Step 92.1 — Extract `resolver.rs`
Move `ColumnResolver` trait and all implementations, `convert_expr`, `build_column_mapping`, `collect_columns*`, `remap_column_indices`.

**Commit:** Planner: extract resolver module

#### Step 92.2 — Extract `ddl.rs`
Move `plan_create_table`, `plan_create_index`.

**Commit:** Planner: extract ddl module

#### Step 92.3 — Extract `dml.rs`; add `gather_indexes`
Move `plan_insert`, `plan_update`, `plan_delete`. Write `gather_indexes` as a private helper, replacing the three duplicated inline blocks.

**Commit:** Planner: extract dml module; add gather_indexes helper

#### Step 92.4 — Extract `select.rs`; slim `mod.rs`
Move `plan_select`, `plan_select_with_joins`, and all helpers called only from them. Slim `mod.rs` to types + entry point.

**Commit:** Planner: extract select module; slim mod.rs to public API

---

## 93. Two-phase SELECT: naive planner + optimizer pass (Track 4)

### What Changes

After item 92, `select.rs` contains both naive plan construction and the two optimisation rules (`try_plan_index_scan`, `can_elide_sort`). This item separates them:

- `select.rs` becomes a pure translation layer: AST → `LogicalPlan` with `Scan + Filter`, never `IndexScan` or `RowidLookup`. No BTree access for optimisation decisions.
- New `src/planner/optimizer.rs` contains `optimize(plan, btree)`, a recursive tree-rewriting function.
- `plan()` in `mod.rs` calls both in sequence.

### Background

Currently the two optimisation rules fire from *inside* `plan_select`, at specific points mid-function:

- Rule 1 (index scan) fires at line ~487, during base-plan construction, before Sort is considered.
- Rule 2 (sort elision) fires at line ~704, during ORDER BY handling, after the IndexScan may have been inserted.

Rule 2 depends on rule 1 having already run. In the two-phase model, this dependency is natural: Phase 2 applies rule 1 first (promoting `Scan+Filter` → `IndexScan+RowidLookup`) then rule 2 (detecting the `IndexScan` just inserted).

The split makes the two phases independently testable and makes adding a third optimisation rule straightforward: add one match arm to `optimize()` in `optimizer.rs`, with no changes to `select.rs`.

### Implementation Approach

**`select.rs`** — remove `try_plan_index_scan`, `can_elide_sort`, and filter-extraction helpers (they move to `optimizer.rs`). Simplify `plan_select` so the base-plan step is always naive:

```rust
// Before: tries index scan first, falls back to Scan+Filter
let mut plan = if let Some(ref filter) = select.filter {
    if let Some(index_plan) = try_plan_index_scan(...) {
        index_plan
    } else {
        make_scan_filter(...)
    }
} else { make_scan(...) };

// After: always naive
let mut plan = if let Some(ref filter) = select.filter {
    LogicalPlan::Filter {
        predicate: convert_expr(filter, &resolver)?,
        input: Box::new(LogicalPlan::Scan {
            rootpage: table.rootpage,
            columns: mapping.scan_columns,
            with_key: false,
        }),
    }
} else {
    LogicalPlan::Scan { rootpage: table.rootpage, columns: mapping.scan_columns, with_key: false }
};
// Sort node always added when ORDER BY is present; no elision here.
```

**`optimizer.rs`** — recursive bottom-up rewriter with two rules:

```rust
pub fn optimize(plan: LogicalPlan, btree: &BTree) -> LogicalPlan {
    match plan {
        // Rule 1: Scan+Filter → IndexScan+RowidLookup when a matching index exists
        LogicalPlan::Filter { predicate, input } => {
            let opt_input = optimize(*input, btree);
            if let LogicalPlan::Scan { rootpage, ref columns, .. } = opt_input {
                if let Some(index_plan) = try_index_scan(&predicate, rootpage, columns, btree) {
                    return index_plan;
                }
            }
            LogicalPlan::Filter { predicate, input: Box::new(opt_input) }
        }

        // Rule 2: Sort → elided when IndexScan below already provides the ordering
        LogicalPlan::Sort { sort_keys, input } => {
            let opt_input = optimize(*input, btree);
            if can_elide_sort(&opt_input, &sort_keys) {
                opt_input
            } else {
                LogicalPlan::Sort { sort_keys, input: Box::new(opt_input) }
            }
        }

        // All other nodes: recurse into children
        LogicalPlan::Project { columns, input } =>
            LogicalPlan::Project { columns, input: Box::new(optimize(*input, btree)) },
        LogicalPlan::RowidLookup { input, table_rootpage, columns } =>
            LogicalPlan::RowidLookup { input: Box::new(optimize(*input, btree)), table_rootpage, columns },
        // ... remaining single-child variants ...

        // Leaf nodes: no children
        leaf @ (LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. }
              | LogicalPlan::Values { .. } | LogicalPlan::Sequence { .. }) => leaf,

        // Multi-child nodes (Join): recurse into each child
        LogicalPlan::Join { left, right, on_condition, left_column_count } =>
            LogicalPlan::Join {
                left: Box::new(optimize(*left, btree)),
                right: Box::new(optimize(*right, btree)),
                on_condition, left_column_count,
            },

        // DML nodes: pass through (optimize is a no-op for non-query plans)
        other => other,
    }
}
```

`try_plan_index_scan` is renamed `try_index_scan` (private to `optimizer.rs`). The filter-extraction helpers move with it.

**`mod.rs`** — updated entry point:

```rust
pub fn plan(stmt: &Statement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let naive = match stmt {
        Statement::Select(s)      => select::plan_select(s, btree)?,
        Statement::Insert(s)      => dml::plan_insert(s, btree)?,
        Statement::Update(s)      => dml::plan_update(s, btree)?,
        Statement::Delete(s)      => dml::plan_delete(s, btree)?,
        Statement::CreateTable(s) => ddl::plan_create_table(s, btree)?,
        Statement::CreateIndex(s) => ddl::plan_create_index(s, btree)?,
        _ => return Err(PlanError::UnsupportedStatement),
    };
    Ok(optimizer::optimize(naive, btree))
}
```

Applying `optimize` to DML/DDL plans is a no-op — they contain no `Filter`/`Sort` nodes and the match falls through to `other => other`. This is intentional: the entry point stays uniform and future DML optimisations slot in naturally.

### Key Files

- `src/planner/select.rs` — `try_plan_index_scan` and `can_elide_sort` removed; `plan_select` simplified
- `src/planner/optimizer.rs` — new file: `optimize`, `try_index_scan`, `can_elide_sort`, filter-extraction helpers
- `src/planner/mod.rs` — `plan()` calls `select::plan_select` then `optimizer::optimize`

### Tests

Add targeted tests for the two phases in isolation:

```rust
// In optimizer.rs #[cfg(test)]

#[test]
fn naive_plan_never_contains_index_scan() {
    // Verify that plan_select alone (without optimize) never produces IndexScan
    // even when an index is available.
}

#[test]
fn optimizer_promotes_scan_filter_to_index_scan() {
    // Build a Filter(age=30, Scan) manually, call optimize(), assert IndexScan+RowidLookup.
}

#[test]
fn optimizer_elides_sort_over_index_scan() {
    // Build Sort(age ASC, RowidLookup(IndexScan(idx_age))), call optimize(), assert Sort absent.
}

#[test]
fn optimizer_keeps_sort_when_no_index() {
    // Build Sort(age ASC, Filter(age=30, Scan)) with no index, assert Sort present.
}
```

### Implementation Steps (2 commits)

#### Step 93.1 — Create `optimizer.rs`; move helpers; `optimize` is initially a pass-through

Move `try_plan_index_scan` (rename to `try_index_scan`), `can_elide_sort`, and the filter-extraction helpers from `select.rs` to `optimizer.rs`. Implement `optimize` as a pass-through that returns `plan` unchanged. All tests still pass.

**Commit:** Planner: extract optimizer module; optimize() is pass-through

#### Step 93.2 — Implement optimizer rules; simplify `plan_select` to naive-only

Implement rule 1 (index scan promotion) and rule 2 (sort elision) in `optimize()`. Remove the inline optimisation logic from `plan_select`. Update `plan()` to call `optimize`. Add the four new tests.

**Commit:** Planner: two-phase planning — naive plan + optimizer pass

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit leaves all tests green
- [ ] After item 91: `src/planner/schema.rs` exists; `column_name_map` used in `plan_update` and `plan_delete`
- [ ] After item 92: `src/planner/mod.rs` ≤250 lines; `gather_indexes` is private to `dml.rs`; each module file ≤700 lines
- [ ] After item 93: `plan_select` contains no call to `try_index_scan` or `can_elide_sort`; `optimizer.rs` contains both; all four optimizer tests pass
- [ ] No public API changes: all callers in `src/db.rs`, `src/explain.rs`, `src/repl/` compile unchanged
- [ ] `cargo test test_sql_` — all SQL integration tests produce identical output before and after
