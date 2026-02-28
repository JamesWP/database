# Phase Y — Project Node Fusion

Eliminate redundant consecutive `Project` nodes from the logical plan by fixing the wildcard ORDER BY column detection and adding a general Project-fusion optimization pass.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 95 | 1 | Fix wildcard ORDER BY column detection in planner | — |
| 96 | 1 | Add Project-fusion optimization pass | 95 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`EXPLAIN SELECT * FROM users WHERE age = 30 ORDER BY age` (with an index on `age`) currently produces:

```
Project [id:0:0, name:1:1, age:2:2]
  Project [id:0, name:1, age:2, age:2]
    RowidLookup users [cols: id, name, age]
      IndexScan via idx_age [= 30]
```

The inner `Project` carries `age` twice because the `already_in_select` check in `plan_select` (planner.rs ~line 639) always returns `false` for `Wildcard` entries — it doesn't expand the wildcard to check whether the ORDER BY column is already covered. This causes the extra column to be appended, and an outer trimming `Project` to be emitted even though the sort is elided.

The correct output should be:

```
Project [id:0, name:1, age:2]
  RowidLookup users [cols: id, name, age]
    IndexScan via idx_age [= 30]
```

This phase fixes the root cause (item 95) and adds a general defence (item 96) so that any future `Project(Project(...))` pairs produced by the planner are collapsed before the plan reaches the compiler.

---

## 95. Fix wildcard ORDER BY column detection (Track 1)

### What Changes

In `plan_select` (`src/planner.rs`), the loop that builds `extra_order_columns` checks whether each ORDER BY column is already present in the SELECT list. The `Wildcard` arm currently returns `false`, meaning any ORDER BY column is considered "not in select" and gets appended as an extra column even when it's already covered by `SELECT *`.

The fix: when any entry in `select.columns` is `Wildcard`, treat *all* table columns as present in the select list and skip adding any extra ORDER BY columns that are covered.

### Background

The `already_in_select` check exists to support:
```sql
SELECT name FROM users ORDER BY age   -- age not in select, must be added temporarily
```
But for:
```sql
SELECT * FROM users ORDER BY age      -- age IS in select via wildcard
```
the wildcard arm of the match returns `false`, causing `age` to be treated as extra. This produces `Project [name, age, age]` → (Sort elided) → `Project [name, age]`.

### Implementation Approach

Replace the per-column `already_in_select` check with a two-step approach:

```rust
// Check if SELECT * is present (wildcard expands to all table columns)
let has_wildcard = select.columns.iter().any(|c| matches!(c, ast::ColumnExpression::Wildcard));

for clause in order_by {
    if let ast::Expression::Value(ast::ScalarValue::Identifier(col_name)) = &clause.expression {
        let already_in_select = has_wildcard || select.columns.iter().any(|col_expr| match col_expr {
            ast::ColumnExpression::Anonyomous(expr) => {
                matches!(expr.as_ref(),
                    ast::Expression::Value(ast::ScalarValue::Identifier(name)) if name == col_name)
            }
            ast::ColumnExpression::Named { expression, .. } => {
                matches!(expression.as_ref(),
                    ast::Expression::Value(ast::ScalarValue::Identifier(name)) if name == col_name)
            }
            ast::ColumnExpression::Wildcard => false, // handled by has_wildcard above
        });

        if !already_in_select {
            let order_col_expr = convert_expr(&clause.expression, &resolver)?;
            extra_order_columns.push(order_col_expr);
        }
    }
}
```

### Key Files

- `src/planner.rs` — `plan_select`, the `already_in_select` loop (~line 632)

### Tests

Add a test in `tests/sql/` or as a planner unit test:

```sql
-- tests/sql/explain_wildcard_order.sql
CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)
INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25)
CREATE INDEX idx_age ON t (age)
EXPLAIN SELECT * FROM t WHERE age = 30 ORDER BY age
-- > Project [id:0, name:1, age:2]
-- > IndexScan via idx_age
```

Verify that the plan contains exactly one `Project` node and no duplicate column indices.

### Implementation Steps (1 commit)

#### Step 95.1 — Fix wildcard ORDER BY column detection

Update the `already_in_select` logic to short-circuit to `true` when a wildcard is present in the SELECT list.

**Commit:** Planner: fix wildcard SELECT treating ORDER BY columns as extra; eliminates spurious inner Project

---

## 96. Add Project-fusion optimization pass (Track 1)

### What Changes

Add a `fuse_projects` function to `src/planner.rs` that walks a `LogicalPlan` tree and collapses any `Project { input: Project { input: inner, columns: inner_cols }, columns: outer_cols }` into a single `Project { input: inner, columns: fused_cols }`, where `fused_cols` is produced by substituting `outer_cols` through `inner_cols`.

Apply `fuse_projects` as the final step of `plan_statement` before returning the plan.

### Background

Item 95 fixes the specific wildcard case. This item adds a general rule so that any future double-Project the planner might produce (e.g., from ORDER BY trimming combined with DISTINCT, or in edge cases not yet identified) is collapsed automatically.

The substitution rule for a `ColumnRef(i)` in `outer_cols` is: replace it with `inner_cols[i]`. Non-`ColumnRef` expressions in `outer_cols` (e.g. literals, arithmetic) pass through unchanged.

### Implementation Approach

```rust
/// Collapse Project(Project(inner, inner_cols), outer_cols) → Project(inner, fused_cols).
/// Applied recursively bottom-up.
fn fuse_projects(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, columns } => {
            let input = fuse_projects(*input);
            if let LogicalPlan::Project { input: inner_input, columns: inner_cols } = input {
                // Substitute: outer ColumnRef(i) → inner_cols[i]
                let fused: Vec<PlanExpr> = columns.into_iter().map(|expr| {
                    substitute_column_refs(expr, &inner_cols)
                }).collect();
                LogicalPlan::Project { input: inner_input, columns: fused }
            } else {
                LogicalPlan::Project { input: Box::new(input), columns }
            }
        }
        // Recurse into all other node types
        LogicalPlan::Filter { input, predicate } =>
            LogicalPlan::Filter { input: Box::new(fuse_projects(*input)), predicate },
        LogicalPlan::Sort { input, sort_keys } =>
            LogicalPlan::Sort { input: Box::new(fuse_projects(*input)), sort_keys },
        LogicalPlan::Limit { input, count } =>
            LogicalPlan::Limit { input: Box::new(fuse_projects(*input)), count },
        // Leaf nodes (Scan, IndexScan, RowidLookup, etc.) pass through unchanged
        other => other,
    }
}

fn substitute_column_refs(expr: PlanExpr, inner_cols: &[PlanExpr]) -> PlanExpr {
    match expr {
        PlanExpr::ColumnRef(i) => inner_cols.get(i).cloned().unwrap_or(PlanExpr::ColumnRef(i)),
        other => other, // literals, aggregates, etc. unchanged
    }
}
```

Call site in `plan_statement`:
```rust
let plan = self.plan_select(select)?;
Ok(fuse_projects(plan))
```

### Key Files

- `src/planner.rs` — new `fuse_projects` and `substitute_column_refs` functions; call site in `plan_statement`

### Tests

Unit test covering the fusion:

```rust
// Project(Project(Scan, [col0, col1, col0]), [col0, col1]) → Project(Scan, [col0, col1])
let inner = LogicalPlan::Project {
    input: Box::new(scan),
    columns: vec![ColumnRef(0), ColumnRef(1), ColumnRef(0)],
};
let outer = LogicalPlan::Project {
    input: Box::new(inner),
    columns: vec![ColumnRef(0), ColumnRef(1)],
};
let fused = fuse_projects(outer);
assert_matches!(fused, LogicalPlan::Project { columns, .. } if columns == vec![ColumnRef(0), ColumnRef(1)]);
```

Integration: re-run the `EXPLAIN SELECT * FROM t WHERE age = 30 ORDER BY age` test from item 95 and assert the plan has exactly one `Project` line.

### Implementation Steps (1 commit)

#### Step 96.1 — Add fuse_projects optimization pass

**Commit:** Planner: add Project-fusion pass; collapse consecutive Project nodes

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `EXPLAIN SELECT * FROM users WHERE age = 30 ORDER BY age` shows exactly one `Project` node
- [ ] `EXPLAIN SELECT * FROM users ORDER BY name` (no index) shows one `Project` above `Sort`
- [ ] `EXPLAIN SELECT name FROM users ORDER BY age` (extra column case) still works correctly — `age` added, trimmed back to `name`
- [ ] Planner unit tests for `fuse_projects` pass
