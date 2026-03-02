# Phase Z — Unified SELECT Planner

Merge `plan_select` and `plan_select_with_joins` into a single function and extend the optimizer to accelerate equality joins with index nested-loop lookups.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 95 | 4 | Merge `plan_select` and `plan_select_with_joins` into a unified function | Phase W (item 92) |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Why merge the two SELECT planners?

`plan()` dispatches to one of two entirely separate paths depending on whether `select.joins` is empty:

```rust
if select.joins.is_empty() {
    plan_select(select, btree)           // ~345 lines
} else {
    plan_select_with_joins(select, btree) // ~188 lines
}
```

The two functions share identical structure for five phases (filter, project, ORDER BY extra-column handling, DISTINCT, LIMIT) but duplicate the code because they use different concrete resolver types. The join path has quietly diverged from the single-table path: it gets none of the single-table improvements (sort elision, aggregation, `SELECT DISTINCT`, index-scan optimization).

After Phase W splits the planner into modules (`select.rs` + `optimizer.rs`), the resolver abstraction is already in place (`&dyn ColumnResolver`). The remaining blocker — different concrete types for `SingleTableResolver` vs `JoinResolver` — disappears once we use trait objects.

The merge:
1. Builds one of two contexts (single-table or join) in an early branch.
2. Shares all post-base logic: filter → aggregate/project → ORDER BY → DISTINCT → LIMIT.
3. Naturally extends the join path to support aggregation, `DISTINCT`, and sort elision that the single-table path already has.

### Why join + index optimization?

Currently joins produce:

```
Join(
  Scan(left_table),
  Scan(right_table),
  on_condition = left.col = right.col
)
```

This is an O(M × N) nested-loop join. When the right table has an index on its join column, the VM can instead do O(M × log N): for each left row, probe the right index. The optimizer can detect the equality join pattern and rewrite to a new `IndexJoin` plan node.

---

## 95. Merge `plan_select` and `plan_select_with_joins` (Track 4)

### What Changes

`plan_select_with_joins` is deleted. `plan_select` is rewritten to handle both cases through a unified body, with an early branch building a `SelectContext` (base plan + resolver + column count metadata).

### Background

The two functions share the following phases verbatim or near-verbatim:

| Phase | `plan_select` | `plan_select_with_joins` | Notes |
|-------|--------------|--------------------------|-------|
| WHERE filter | lines ~487–515 | lines ~839–845 | identical logic |
| Project columns | lines ~607–672 | lines ~848–871 | structurally identical |
| ORDER BY (extra cols) | lines ~631–744 | lines ~884–942 | duplicated, minor diffs |
| DISTINCT | lines ~748–753 | missing | join path missing feature |
| LIMIT | lines ~755–762 | lines ~945–952 | identical |

ORDER BY is the largest duplicated section (~80 lines each). It is extracted as a private helper `apply_order_by` so the merge step only needs to wire one call site.

### Implementation Approach

**Step 1: Extract `apply_order_by` helper (in `select.rs` after Phase W item 92)**

Signature:

```rust
/// Wrap `plan` with Sort (and optional final-project to strip extra ORDER BY columns).
/// `project_exprs` is the slice of exprs already in the Project node above `plan`.
/// Returns the updated plan. Uses `resolver` only to convert ORDER BY expressions.
fn apply_order_by(
    plan: LogicalPlan,
    order_by: &[ast::OrderByClause],
    project_exprs: &[PlanExpr],
    select_col_count: usize,
    resolver: &dyn ColumnResolver,
) -> Result<LogicalPlan, PlanError>
```

Body: consolidate the two near-identical ORDER BY blocks from `plan_select` and `plan_select_with_joins`. The single-table version includes sort elision via `can_elide_sort`; the helper should accept a `allow_sort_elision: bool` parameter so the join path passes `false` (no sort elision for joins until item 96 enables it).

After extraction, both call sites in `plan_select` and `plan_select_with_joins` become:

```rust
plan = apply_order_by(plan, order_by, &project_exprs, select_col_count, &resolver, elide_sort)?;
```

**Step 2: Merge the two functions**

Introduce a `SelectContext` struct that carries the per-path data:

```rust
struct SelectContext<'a> {
    base_plan: LogicalPlan,
    resolver: Box<dyn ColumnResolver + 'a>,
    /// Column count exposed by base_plan (used by wildcard expansion).
    output_col_count: usize,
    /// For single-table: the schema::Table; for joins: None.
    table: Option<&'a schema::Table>,
    /// Total number of physical columns in the base plan output (left+right for joins).
    total_col_count: usize,
    /// Whether sort elision is allowed (true for single-table, false for joins).
    allow_sort_elision: bool,
}
```

Two private builders:

```rust
fn single_table_context<'a>(
    select: &'a ast::SelectStatement,
    btree: &BTree,
) -> Result<SelectContext<'a>, PlanError>

fn join_context<'a>(
    select: &'a ast::SelectStatement,
    btree: &BTree,
) -> Result<SelectContext<'a>, PlanError>
```

Unified `plan_select`:

```rust
fn plan_select(select: ast::SelectStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let ctx = if select.joins.is_empty() {
        single_table_context(&select, btree)?
    } else {
        join_context(&select, btree)?
    };

    let SelectContext { mut plan, resolver, table, allow_sort_elision, .. } = ctx;

    // --- Column collection is already done inside context builders ---

    // Detect aggregation intent
    let is_distinct = select.distinct;
    let has_group_by = select.group_by.is_some();
    let has_aggregates = select.columns.iter().any(|col| has_aggregate(col));
    let use_aggregation = has_group_by || has_aggregates;

    // Count(*) fast path (single-table only; guard for joins)
    let is_count_star = ctx.table.is_some()
        && !has_group_by
        && /* existing count(*) detection */ ...;

    // WHERE filter
    if let Some(ref filter) = select.filter {
        plan = apply_filter(plan, filter, resolver.as_ref())?;
    }

    // HAVING guard
    if select.having.is_some() && !use_aggregation && !is_count_star {
        return Err(PlanError::InvalidHaving(...));
    }

    // Aggregate / Count / Project
    if is_count_star {
        plan = LogicalPlan::Count { input: Box::new(plan) };
    } else if use_aggregation {
        plan = apply_aggregate(plan, &select, resolver.as_ref())?;
    } else {
        let (projected_plan, project_exprs, select_col_count) =
            apply_project(plan, &select, resolver.as_ref(), ctx.table)?;
        plan = projected_plan;

        // ORDER BY
        if let Some(ref order_by) = select.order_by {
            plan = apply_order_by(plan, order_by, &project_exprs, select_col_count, resolver.as_ref(), allow_sort_elision)?;
        }
    }

    // DISTINCT
    if is_distinct {
        plan = LogicalPlan::Distinct { input: Box::new(plan) };
    }

    // LIMIT
    if let Some(ref limit_expr) = select.limit {
        plan = LogicalPlan::Limit { input: Box::new(plan), count: extract_limit_value(limit_expr)? };
    }

    Ok(plan)
}
```

The aggregation path (`apply_aggregate`) and filter path (`apply_filter`) are also small extracted helpers; this incidentally removes the last duplication between the two paths.

**Note on Phase W dependency**: If Phase W is not yet done when this item is implemented, the merge happens directly in `src/planner.rs` (still a single file). The approach is identical — the `SelectContext` struct and helper functions live in the same file. Phase W's module split would then move them to `select.rs`.

### Key Files

- `src/planner.rs` (or `src/planner/select.rs` after Phase W item 92)
- `plan_select_with_joins` — deleted
- New private helpers: `single_table_context`, `join_context`, `apply_filter`, `apply_order_by`, `apply_project`, `apply_aggregate`

### Tests

All existing planner and SQL integration tests exercise both paths. Add join-specific tests that now also cover:

```rust
// Previously untested for joins:
#[test]
fn join_with_limit() { /* SELECT ... FROM a JOIN b ON ... LIMIT 5 */ }

#[test]
fn join_with_distinct() { /* SELECT DISTINCT a.id FROM a JOIN b ON ... */ }

#[test]
fn join_with_order_by() { /* SELECT ... FROM a JOIN b ON ... ORDER BY a.name */ }
```

### Implementation Steps (3 commits)

#### Step 95.1 — Extract `apply_order_by` helper

Move the ORDER BY + extra-column logic from `plan_select` into `apply_order_by`. Call it from `plan_select`; `plan_select_with_joins` still has its own inline version (removed in 95.2). All tests pass.

**Commit:** Planner: extract apply_order_by helper from plan_select

#### Step 95.2 — Merge `plan_select_with_joins` into unified `plan_select`

Introduce `single_table_context` and `join_context` builders. Rewrite `plan_select` with the shared body. Delete `plan_select_with_joins`. Add join tests for LIMIT, DISTINCT, ORDER BY.

**Commit:** Planner: merge plan_select_with_joins into unified plan_select

#### Step 95.3 — Extract remaining helpers (`apply_filter`, `apply_project`, `apply_aggregate`)

Tidy up by extracting the remaining phases as small named helpers. No behaviour change.

**Commit:** Planner: extract apply_filter / apply_project / apply_aggregate helpers

---

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] `plan_select_with_joins` no longer exists in the codebase
- [ ] `plan_select` handles both join and non-join cases
- [ ] Joins now support DISTINCT, ORDER BY, and LIMIT (previously silently missing)
- [ ] `SELECT * FROM a JOIN b ON a.x = b.id` with an index on `b.id` shows `IndexJoin` in EXPLAIN output
- [ ] `SELECT * FROM a JOIN b ON a.x = b.id` without an index still shows `Join` in EXPLAIN output
- [ ] All SQL integration tests produce identical output before and after item 95: `cargo test test_sql_`
