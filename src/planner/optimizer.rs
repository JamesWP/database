//! Query optimizer: rewrites a naive LogicalPlan tree to use indexes and elide redundant sorts.

use crate::catalog::Catalog;
use crate::frontend::ast::Statement;
use crate::frontend::parse;

use super::{BinaryOp, JoinStrategy, Literal, LogicalPlan, PlanExpr, UnaryOp};

/// Collapse consecutive Project(Project(inner, inner_cols), outer_cols) into a single Project.
///
/// Applied bottom-up so that chains of three or more Projects are fully reduced.
pub(super) fn fuse_projects(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, columns } => {
            let input = fuse_projects(*input);
            if let LogicalPlan::Project {
                input: inner_input,
                columns: inner_cols,
            } = input
            {
                let fused = columns
                    .into_iter()
                    .map(|expr| substitute_column_refs(expr, &inner_cols))
                    .collect();
                LogicalPlan::Project {
                    input: inner_input,
                    columns: fused,
                }
            } else {
                LogicalPlan::Project {
                    input: Box::new(input),
                    columns,
                }
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(fuse_projects(*input)),
            predicate,
        },
        LogicalPlan::Sort { input, sort_keys } => LogicalPlan::Sort {
            input: Box::new(fuse_projects(*input)),
            sort_keys,
        },
        LogicalPlan::Limit { input, count } => LogicalPlan::Limit {
            input: Box::new(fuse_projects(*input)),
            count,
        },
        LogicalPlan::Count { input } => LogicalPlan::Count {
            input: Box::new(fuse_projects(*input)),
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => LogicalPlan::Aggregate {
            input: Box::new(fuse_projects(*input)),
            group_keys,
            aggregates,
            having,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(fuse_projects(*input)),
        },
        LogicalPlan::RowidLookup {
            input,
            table_rootpage,
            columns,
        } => LogicalPlan::RowidLookup {
            input: Box::new(fuse_projects(*input)),
            table_rootpage,
            columns,
        },
        LogicalPlan::PopulateIndex {
            input,
            index_rootpage,
            column_idxs,
        } => LogicalPlan::PopulateIndex {
            input: Box::new(fuse_projects(*input)),
            index_rootpage,
            column_idxs,
        },
        LogicalPlan::Join {
            left,
            right,
            on_condition,
            strategy,
            left_column_count,
        } => LogicalPlan::Join {
            left: Box::new(fuse_projects(*left)),
            right: Box::new(fuse_projects(*right)),
            on_condition,
            strategy,
            left_column_count,
        },
        // Leaf and DML nodes: no change.
        other => other,
    }
}

/// Substitute ColumnRef(i) → inner_cols[i] recursively within an expression.
fn substitute_column_refs(expr: PlanExpr, inner_cols: &[PlanExpr]) -> PlanExpr {
    match expr {
        PlanExpr::ColumnRef(i) => inner_cols.get(i).cloned().unwrap_or(PlanExpr::ColumnRef(i)),
        PlanExpr::BinaryOp { op, left, right } => PlanExpr::BinaryOp {
            op,
            left: Box::new(substitute_column_refs(*left, inner_cols)),
            right: Box::new(substitute_column_refs(*right, inner_cols)),
        },
        PlanExpr::UnaryOp { op, operand } => PlanExpr::UnaryOp {
            op,
            operand: Box::new(substitute_column_refs(*operand, inner_cols)),
        },
        PlanExpr::FunctionCall { name, args } => PlanExpr::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|a| substitute_column_refs(a, inner_cols))
                .collect(),
        },
        other => other,
    }
}

/// Apply optimization rules to a naive LogicalPlan.
///
/// Rule 1: Filter(predicate, Scan) → IndexScan+RowidLookup when a matching index exists.
/// Rule 2: Sort(keys, plan) → plan when an IndexScan below already provides the ordering.
pub(super) fn optimize(plan: LogicalPlan, catalog: &Catalog) -> LogicalPlan {
    match plan {
        // Rule 1: Scan+Filter → IndexScan+RowidLookup when a matching index exists.
        LogicalPlan::Filter { predicate, input } => {
            let opt_input = optimize(*input, catalog);
            if let LogicalPlan::Scan {
                rootpage,
                ref columns,
                ..
            } = opt_input
            {
                if let Some(index_plan) =
                    try_index_scan_plan(&predicate, rootpage, columns, catalog)
                {
                    return index_plan;
                }
            }
            LogicalPlan::Filter {
                predicate,
                input: Box::new(opt_input),
            }
        }

        // Rule 2: Sort → elided when IndexScan below already provides the ordering.
        LogicalPlan::Sort { sort_keys, input } => {
            let opt_input = optimize(*input, catalog);
            // Elide sort if there is exactly one ASC key and an IndexScan provides the order.
            let elide = if sort_keys.len() == 1 && !sort_keys[0].descending {
                if let PlanExpr::ColumnRef(proj_idx) = sort_keys[0].expr {
                    can_elide_sort_by_proj(&opt_input, proj_idx)
                } else {
                    false
                }
            } else {
                false
            };
            if elide {
                opt_input
            } else {
                LogicalPlan::Sort {
                    sort_keys,
                    input: Box::new(opt_input),
                }
            }
        }

        // Single-child nodes: recurse.
        LogicalPlan::Project { columns, input } => LogicalPlan::Project {
            columns,
            input: Box::new(optimize(*input, catalog)),
        },
        LogicalPlan::Limit { count, input } => LogicalPlan::Limit {
            count,
            input: Box::new(optimize(*input, catalog)),
        },
        LogicalPlan::Count { input } => LogicalPlan::Count {
            input: Box::new(optimize(*input, catalog)),
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => LogicalPlan::Aggregate {
            input: Box::new(optimize(*input, catalog)),
            group_keys,
            aggregates,
            having,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(optimize(*input, catalog)),
        },
        // Rule 3: RowidLookup(IndexScan) → covering IndexScan when all requested
        // columns are present in the index key. The primary B-tree lookup is elided.
        LogicalPlan::RowidLookup {
            input,
            table_rootpage,
            columns,
        } => {
            let opt_input = optimize(*input, catalog);
            if let Some(covering) =
                try_covering_index_scan(&opt_input, table_rootpage, &columns, catalog)
            {
                return covering;
            }
            LogicalPlan::RowidLookup {
                input: Box::new(opt_input),
                table_rootpage,
                columns,
            }
        }
        LogicalPlan::PopulateIndex {
            input,
            index_rootpage,
            column_idxs,
        } => LogicalPlan::PopulateIndex {
            input: Box::new(optimize(*input, catalog)),
            index_rootpage,
            column_idxs,
        },

        // Two-child node.
        LogicalPlan::Join {
            left,
            right,
            on_condition,
            strategy,
            left_column_count,
        } => {
            let opt_left = optimize(*left, catalog);
            let opt_right = optimize(*right, catalog);

            // For NestedLoop: try to substitute right Scan with IndexProbe when an equi-join
            // condition matches an index on the right table. If no index found, promote to Hash.
            if strategy == JoinStrategy::NestedLoop {
                if let Some((new_right, residual)) =
                    try_index_probe_plan(&on_condition, &opt_right, left_column_count, catalog)
                {
                    return LogicalPlan::Join {
                        left: Box::new(opt_left),
                        right: Box::new(new_right),
                        on_condition: residual,
                        strategy: JoinStrategy::NestedLoop,
                        left_column_count,
                    };
                }
            }

            // No index available (or strategy is already Hash): promote to Hash.
            LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                on_condition,
                strategy: JoinStrategy::Hash,
                left_column_count,
            }
        }

        // Leaf and DML nodes: no optimization.
        other => other,
    }
}

/// Try to replace Filter(predicate, Scan{rootpage}) with IndexScan+RowidLookup.
///
/// Works on the already-resolved PlanExpr predicate. Looks up the table name from
/// the rootpage, then finds a matching index.
fn try_index_scan_plan(
    predicate: &PlanExpr,
    table_rootpage: u32,
    scan_columns: &[usize],
    catalog: &Catalog,
) -> Option<LogicalPlan> {
    let table_name = catalog.lookup_table_by_rootpage(table_rootpage)?;
    let indexes = catalog.lookup_indexes_for_table(&table_name);
    if indexes.is_empty() {
        return None;
    }

    // extract_index_bounds returns a scan-output column index (ColumnRef position).
    // We remap it to the table column index via scan_columns.
    let (scan_out_idx, lower_bound, upper_bound) = extract_index_bounds(predicate)?;
    let table_col_idx = *scan_columns.get(scan_out_idx)?;

    // Find an index whose first column (by TABLE column index) matches table_col_idx.
    let index = indexes.iter().find(|idx| {
        idx.column_names
            .first()
            .and_then(|col_name| {
                // Resolve the column name to a table column index.
                catalog.lookup_table(&table_name).and_then(|(_, sql)| {
                    parse(&sql).ok().and_then(|stmt| match stmt {
                        Statement::CreateTable(ct) => {
                            ct.columns.iter().position(|c| c.name == *col_name)
                        }
                        _ => None,
                    })
                })
            })
            .unwrap_or(usize::MAX)
            == table_col_idx
    })?;

    let index_scan = LogicalPlan::IndexScan {
        index_rootpage: index.rootpage,
        index_col_idx: table_col_idx,
        lower_bound,
        upper_bound,
        output_columns: None,
    };

    // Try the covering index optimisation: if all requested scan columns are
    // present in the index key, skip the primary B-tree lookup entirely.
    if let Some(covering) =
        try_covering_index_scan(&index_scan, table_rootpage, scan_columns, catalog)
    {
        return Some(covering);
    }

    Some(LogicalPlan::RowidLookup {
        input: Box::new(index_scan),
        table_rootpage,
        columns: scan_columns.to_vec(),
    })
}

/// Try to rewrite a NestedLoop join's right subtree to use an IndexProbe.
///
/// Inspects `on_condition` for equi-join conditions of the form
/// `ColumnRef(left_col) = ColumnRef(right_col_combined)` where
/// `right_col_combined >= left_column_count` (the right column in combined space).
/// If the right subtree is a Scan whose table has an index on the right join column,
/// returns `(new_right, residual_condition)` where:
/// - `new_right` = `RowidLookup(IndexProbe { key_expr: ColumnRef(left_col_idx), ... })`
/// - `residual_condition` = remaining AND conditions, or `Literal(Bool(true))` if the
///   matched equality was the only condition.
///
/// Returns `None` if no applicable index probe is possible.
fn try_index_probe_plan(
    on_condition: &PlanExpr,
    right: &LogicalPlan,
    left_column_count: usize,
    catalog: &Catalog,
) -> Option<(LogicalPlan, PlanExpr)> {
    // Right side must be a plain Scan (or possibly RowidLookup wrapping a scan that
    // the sub-optimizer already rewrote — we only handle the Scan case here).
    let (right_rootpage, right_columns) = match right {
        LogicalPlan::Scan {
            rootpage, columns, ..
        } => (*rootpage, columns.clone()),
        _ => return None,
    };

    let table_name = catalog.lookup_table_by_rootpage(right_rootpage)?;
    let indexes = catalog.lookup_indexes_for_table(&table_name);
    if indexes.is_empty() {
        return None;
    }

    // Extract equi-join pairs: (left_col_idx, right_table_col_idx) from the ON condition.
    // right_col_in_combined = left_column_count + right_table_col_position_in_scan
    let equi_pairs = extract_equi_join_pairs(on_condition, left_column_count, &right_columns);

    for (left_col_idx, right_table_col_idx) in &equi_pairs {
        // Find an index whose first column matches right_table_col_idx
        let index = indexes.iter().find(|idx| {
            idx.column_names
                .first()
                .and_then(|col_name| {
                    catalog.lookup_table(&table_name).and_then(|(_, sql)| {
                        parse(&sql).ok().and_then(|stmt| match stmt {
                            Statement::CreateTable(ct) => {
                                ct.columns.iter().position(|c| c.name == *col_name)
                            }
                            _ => None,
                        })
                    })
                })
                .unwrap_or(usize::MAX)
                == *right_table_col_idx
        });

        if let Some(index) = index {
            let new_right = LogicalPlan::RowidLookup {
                input: Box::new(LogicalPlan::IndexProbe {
                    index_rootpage: index.rootpage,
                    key_expr: PlanExpr::ColumnRef(*left_col_idx),
                    index_col_idx: *right_table_col_idx,
                }),
                table_rootpage: right_rootpage,
                columns: right_columns,
            };

            // Remove the matched equality from on_condition; leave residual as Literal(true)
            // if it was the only condition.
            let residual = remove_equi_condition(
                on_condition,
                *left_col_idx,
                left_column_count + *right_table_col_idx,
            );

            return Some((new_right, residual));
        }
    }

    None
}

/// Extract equi-join pairs (left_col_idx, right_table_col_idx) from on_condition.
/// right_table_col_idx = combined_col_idx - left_column_count, after mapping
/// through right_scan_columns (which maps scan output position → table column index).
fn extract_equi_join_pairs(
    condition: &PlanExpr,
    left_column_count: usize,
    right_columns: &[usize],
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    collect_equi_pairs(condition, left_column_count, right_columns, &mut pairs);
    pairs
}

fn collect_equi_pairs(
    condition: &PlanExpr,
    left_column_count: usize,
    right_columns: &[usize],
    pairs: &mut Vec<(usize, usize)>,
) {
    match condition {
        PlanExpr::BinaryOp {
            op: BinaryOp::Equals,
            left,
            right,
        } => {
            if let (PlanExpr::ColumnRef(l), PlanExpr::ColumnRef(r)) =
                (left.as_ref(), right.as_ref())
            {
                // Identify which is left and which is right column reference
                let (left_col, right_combined) =
                    if *l < left_column_count && *r >= left_column_count {
                        (*l, *r)
                    } else if *r < left_column_count && *l >= left_column_count {
                        (*r, *l)
                    } else {
                        return;
                    };
                // right_combined is the index in combined output; subtract left_column_count
                // to get the right-side scan output position, then map to table column index
                let right_scan_pos = right_combined - left_column_count;
                if let Some(&right_table_col_idx) = right_columns.get(right_scan_pos) {
                    pairs.push((left_col, right_table_col_idx));
                }
            }
        }
        PlanExpr::BinaryOp {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_equi_pairs(left, left_column_count, right_columns, pairs);
            collect_equi_pairs(right, left_column_count, right_columns, pairs);
        }
        _ => {}
    }
}

/// Remove the matched equi-join condition from `condition`.
/// If `condition` is the exact equality, return `Literal(Bool(true))`.
/// If `condition` is AND, strip the matching arm and return the other.
fn remove_equi_condition(condition: &PlanExpr, left_col: usize, right_combined: usize) -> PlanExpr {
    let is_match = |expr: &PlanExpr| -> bool {
        if let PlanExpr::BinaryOp {
            op: BinaryOp::Equals,
            left,
            right,
        } = expr
        {
            matches!(
                (left.as_ref(), right.as_ref()),
                (PlanExpr::ColumnRef(l), PlanExpr::ColumnRef(r))
                    if (*l == left_col && *r == right_combined)
                       || (*r == left_col && *l == right_combined)
            )
        } else {
            false
        }
    };

    if is_match(condition) {
        return PlanExpr::Literal(Literal::Bool(true));
    }

    if let PlanExpr::BinaryOp {
        op: BinaryOp::And,
        left,
        right,
    } = condition
    {
        if is_match(left) {
            return *right.clone();
        }
        if is_match(right) {
            return *left.clone();
        }
        // Recurse for deeper AND trees
        let new_left = remove_equi_condition(left, left_col, right_combined);
        let new_right = remove_equi_condition(right, left_col, right_combined);
        return PlanExpr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(new_left),
            right: Box::new(new_right),
        };
    }

    condition.clone()
}

/// Extract (column_index, lower_bound, upper_bound) from a PlanExpr filter predicate.
/// Returns None if the predicate is not a supported index filter shape.
fn extract_index_bounds(
    predicate: &PlanExpr,
) -> Option<(usize, Option<(Literal, bool)>, Option<(Literal, bool)>)> {
    match predicate {
        // Equality: col = lit or lit = col
        PlanExpr::BinaryOp {
            op: BinaryOp::Equals,
            left,
            right,
        } => {
            if let (PlanExpr::ColumnRef(col), PlanExpr::Literal(lit)) =
                (left.as_ref(), right.as_ref())
            {
                return Some((*col, Some((lit.clone(), true)), Some((lit.clone(), true))));
            }
            if let (PlanExpr::Literal(lit), PlanExpr::ColumnRef(col)) =
                (left.as_ref(), right.as_ref())
            {
                return Some((*col, Some((lit.clone(), true)), Some((lit.clone(), true))));
            }
            None
        }

        // IS NULL: col IS NULL
        PlanExpr::UnaryOp {
            op: UnaryOp::IsNull,
            operand,
        } => {
            if let PlanExpr::ColumnRef(col) = operand.as_ref() {
                return Some((
                    *col,
                    Some((Literal::Null, true)),
                    Some((Literal::Null, true)),
                ));
            }
            None
        }

        // Range comparisons and AND combinations
        PlanExpr::BinaryOp { op, left, right } => {
            // Single range: col op lit
            if let (PlanExpr::ColumnRef(col), PlanExpr::Literal(lit)) =
                (left.as_ref(), right.as_ref())
            {
                match op {
                    BinaryOp::GreaterThan => {
                        return Some((*col, Some((lit.clone(), false)), None));
                    }
                    BinaryOp::GreaterThanOrEqual => {
                        return Some((*col, Some((lit.clone(), true)), None));
                    }
                    BinaryOp::LessThan => {
                        return Some((*col, None, Some((lit.clone(), false))));
                    }
                    BinaryOp::LessThanOrEqual => {
                        return Some((*col, None, Some((lit.clone(), true))));
                    }
                    _ => {}
                }
            }
            // AND combination: (col > L) AND (col < U)
            if let BinaryOp::And = op {
                let left_bounds = extract_index_bounds(left)?;
                let right_bounds = extract_index_bounds(right)?;
                if left_bounds.0 != right_bounds.0 {
                    return None; // Different columns
                }
                let col = left_bounds.0;
                let lower = left_bounds.1.or(right_bounds.1);
                let upper = left_bounds.2.or(right_bounds.2);
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                return Some((col, lower, upper));
            }
            None
        }

        _ => None,
    }
}

/// Returns true if the plan tree rooted at `plan` contains an IndexScan on
/// `sort_col_idx` with only pass-through nodes in between — meaning the scan
/// already produces rows in ascending order by that column.
pub(super) fn can_elide_sort(plan: &LogicalPlan, sort_col_idx: usize) -> bool {
    match plan {
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. } => can_elide_sort(input, sort_col_idx),

        LogicalPlan::RowidLookup { input, columns, .. } => {
            // sort_col_idx is a scan-output position; translate to table-column
            // space before descending into IndexScan, which uses table column indices.
            if let Some(&table_col) = columns.get(sort_col_idx) {
                can_elide_sort(input, table_col)
            } else {
                false
            }
        }

        LogicalPlan::IndexScan { index_col_idx, .. } => *index_col_idx == sort_col_idx,

        _ => false,
    }
}

/// Like can_elide_sort but takes a projection-space index and looks through
/// Project nodes to map back to scan-space.
fn can_elide_sort_by_proj(plan: &LogicalPlan, proj_idx: usize) -> bool {
    match plan {
        LogicalPlan::Project { columns, input } => {
            // Map proj_idx → scan column index via the project expressions.
            if let Some(PlanExpr::ColumnRef(scan_idx)) = columns.get(proj_idx) {
                can_elide_sort(input, *scan_idx)
            } else {
                false
            }
        }
        other => can_elide_sort(other, proj_idx),
    }
}

/// Try to rewrite `RowidLookup { input: IndexScan, .. }` into a covering
/// `IndexScan { output_columns: Some(...) }` when every column requested by the
/// RowidLookup is present in the index key.
///
/// `scan_columns` are the table-column indices the RowidLookup needs to fetch.
/// Returns `Some(IndexScan { output_columns: Some(mapped) })` if all columns are
/// covered; `None` otherwise (caller keeps the RowidLookup unchanged).
fn try_covering_index_scan(
    input: &LogicalPlan,
    table_rootpage: u32,
    scan_columns: &[usize],
    catalog: &Catalog,
) -> Option<LogicalPlan> {
    // Only applies when the direct child is a plain IndexScan.
    let (index_rootpage, index_col_idx, lower_bound, upper_bound) = match input {
        LogicalPlan::IndexScan {
            index_rootpage,
            index_col_idx,
            lower_bound,
            upper_bound,
            output_columns: None,
        } => (*index_rootpage, *index_col_idx, lower_bound, upper_bound),
        _ => return None,
    };

    // Look up the table's column names to map table-col-idx → name.
    let table_name = catalog.lookup_table_by_rootpage(table_rootpage)?;
    let (_, table_sql) = catalog.lookup_table(&table_name)?;
    let table_cols: Vec<String> = match parse(&table_sql).ok()? {
        Statement::CreateTable(ct) => ct.columns.into_iter().map(|c| c.name).collect(),
        _ => return None,
    };

    // Look up the index's column names.
    let indexes = catalog.lookup_indexes_for_table(&table_name);
    let index = indexes.iter().find(|idx| idx.rootpage == index_rootpage)?;
    let index_cols = &index.column_names;

    // For each requested scan column, find its position in the index key.
    let mut output_columns: Vec<usize> = Vec::with_capacity(scan_columns.len());
    for &table_col in scan_columns {
        let col_name = table_cols.get(table_col)?;
        let idx_pos = index_cols.iter().position(|n| n == col_name)?;
        output_columns.push(idx_pos);
    }

    // All columns are covered — return a covering IndexScan.
    Some(LogicalPlan::IndexScan {
        index_rootpage,
        index_col_idx,
        lower_bound: lower_bound.clone(),
        upper_bound: upper_bound.clone(),
        output_columns: Some(output_columns),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::SortKey;
    use crate::test::TestDb;

    /// Create a test database with users(id, name, age) table.
    fn make_users_db() -> (TestDb, u32) {
        let mut db = TestDb::default();
        let root = db.catalog.btree_mut().create_tree();
        db.catalog.insert_entry(
            "table",
            "users",
            "users",
            root,
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
        );
        (db, root)
    }

    /// Create a test database with users(id, name, age) and an index on age.
    fn make_users_db_with_age_index() -> (TestDb, u32) {
        let (mut db, root) = make_users_db();
        let idx_root = db.catalog.btree_mut().create_tree();
        db.catalog.insert_entry(
            "index",
            "idx_age",
            "users",
            idx_root,
            "CREATE INDEX idx_age ON users (age)",
        );
        (db, root)
    }

    fn make_index_scan(
        table_rootpage: u32,
        index_rootpage: u32,
        index_col_idx: usize,
    ) -> LogicalPlan {
        LogicalPlan::RowidLookup {
            input: Box::new(LogicalPlan::IndexScan {
                index_rootpage,
                index_col_idx,
                lower_bound: Some((Literal::Integer(30), true)),
                upper_bound: Some((Literal::Integer(30), true)),
                output_columns: None,
            }),
            table_rootpage,
            columns: vec![0, 1, 2],
        }
    }

    fn plan_contains_index_scan(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::IndexScan { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Count { input }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::RowidLookup { input, .. }
            | LogicalPlan::PopulateIndex { input, .. } => plan_contains_index_scan(input),
            LogicalPlan::Aggregate { input, .. } => plan_contains_index_scan(input),
            LogicalPlan::Join { left, right, .. } => {
                plan_contains_index_scan(left) || plan_contains_index_scan(right)
            }
            _ => false,
        }
    }

    #[test]
    fn naive_plan_never_contains_index_scan() {
        // Verify that plan_select alone (without optimize) never produces IndexScan
        // even when an index is available.
        let (db, _root) = make_users_db_with_age_index();

        let naive = super::super::select::plan_select(
            match crate::frontend::parse("SELECT * FROM users WHERE age = 30").unwrap() {
                crate::frontend::ast::Statement::Select(s) => s,
                _ => panic!("expected select"),
            },
            &db.catalog,
        )
        .expect("plan_select failed");

        assert!(
            !plan_contains_index_scan(&naive),
            "naive plan should not contain IndexScan, got: {:?}",
            naive
        );
    }

    #[test]
    fn optimizer_promotes_scan_filter_to_index_scan() {
        // Build Filter(age=30, Scan) manually, call optimize(), assert IndexScan+RowidLookup.
        let (db, root) = make_users_db_with_age_index();

        // col index 2 = age (users: id=0, name=1, age=2)
        let naive = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::Equals,
                left: Box::new(PlanExpr::ColumnRef(2)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(30))),
            },
            input: Box::new(LogicalPlan::Scan {
                rootpage: root,
                columns: vec![0, 1, 2],
                with_key: false,
            }),
        };

        let optimized = optimize(naive, &db.catalog);
        assert!(
            matches!(&optimized, LogicalPlan::RowidLookup { input, .. }
                if matches!(input.as_ref(), LogicalPlan::IndexScan { .. })),
            "expected RowidLookup(IndexScan), got: {:?}",
            optimized
        );
    }

    #[test]
    fn optimizer_elides_sort_over_index_scan() {
        // Build Sort(age ASC, Project(RowidLookup(IndexScan(age)))) and assert Sort is absent.
        let (db, root) = make_users_db_with_age_index();
        let idx_root = db.catalog.lookup_indexes_for_table("users")[0].rootpage;

        let index_plan = make_index_scan(root, idx_root, 2); // age is col 2

        // Wrap in Project: pass all 3 columns through
        let project = LogicalPlan::Project {
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
            ],
            input: Box::new(index_plan),
        };

        let sort_plan = LogicalPlan::Sort {
            sort_keys: vec![SortKey {
                expr: PlanExpr::ColumnRef(2), // proj index 2 = age
                descending: false,
            }],
            input: Box::new(project),
        };

        let optimized = optimize(sort_plan, &db.catalog);
        assert!(
            !matches!(optimized, LogicalPlan::Sort { .. }),
            "expected Sort to be elided, got: {:?}",
            optimized
        );
    }

    #[test]
    fn optimizer_keeps_sort_when_no_index() {
        // Build Sort(age ASC, Project(Filter(age=30, Scan))) with no index → Sort must remain.
        let (db, root) = make_users_db(); // no index

        let plan = LogicalPlan::Sort {
            sort_keys: vec![SortKey {
                expr: PlanExpr::ColumnRef(2),
                descending: false,
            }],
            input: Box::new(LogicalPlan::Project {
                columns: vec![
                    PlanExpr::ColumnRef(0),
                    PlanExpr::ColumnRef(1),
                    PlanExpr::ColumnRef(2),
                ],
                input: Box::new(LogicalPlan::Filter {
                    predicate: PlanExpr::BinaryOp {
                        op: BinaryOp::Equals,
                        left: Box::new(PlanExpr::ColumnRef(2)),
                        right: Box::new(PlanExpr::Literal(Literal::Integer(30))),
                    },
                    input: Box::new(LogicalPlan::Scan {
                        rootpage: root,
                        columns: vec![0, 1, 2],
                        with_key: false,
                    }),
                }),
            }),
        };

        let optimized = optimize(plan, &db.catalog);
        assert!(
            matches!(optimized, LogicalPlan::Sort { .. }),
            "expected Sort to remain when no index, got: {:?}",
            optimized
        );
    }

    #[test]
    fn fuse_projects_collapses_double_project() {
        // Project(Project(Scan, [col0, col1, col0]), [col0, col1]) → Project(Scan, [col0, col1])
        let scan = LogicalPlan::Scan {
            rootpage: 1,
            columns: vec![0, 1, 2],
            with_key: false,
        };
        let inner = LogicalPlan::Project {
            input: Box::new(scan),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(0),
            ],
        };
        let outer = LogicalPlan::Project {
            input: Box::new(inner),
            columns: vec![PlanExpr::ColumnRef(0), PlanExpr::ColumnRef(1)],
        };
        let fused = fuse_projects(outer);
        assert!(
            matches!(&fused, LogicalPlan::Project { columns, input }
                if columns == &vec![PlanExpr::ColumnRef(0), PlanExpr::ColumnRef(1)]
                && matches!(input.as_ref(), LogicalPlan::Scan { .. })),
            "expected single Project over Scan, got: {:?}",
            fused
        );
    }

    #[test]
    fn fuse_projects_leaves_single_project_unchanged() {
        let scan = LogicalPlan::Scan {
            rootpage: 1,
            columns: vec![0, 1],
            with_key: false,
        };
        let project = LogicalPlan::Project {
            input: Box::new(scan),
            columns: vec![PlanExpr::ColumnRef(0), PlanExpr::ColumnRef(1)],
        };
        let result = fuse_projects(project.clone());
        assert_eq!(result, project);
    }

    #[test]
    fn fuse_projects_substitutes_compound_expressions() {
        // Inner: Project(Scan, [c0+1, c1, c2+2, c3+3])
        //   inner[0] = col0 + 1
        //   inner[1] = col1
        //   inner[2] = col2 + 2
        //   inner[3] = col3 + 3
        //
        // Outer: Project(Inner, [c0+1, c1, c2+c3])
        //   outer[0] = inner[0] + 1  →  (col0+1)+1
        //   outer[1] = inner[1]      →  col1
        //   outer[2] = inner[2] + inner[3]  →  (col2+2)+(col3+3)
        let lit = |n: i64| PlanExpr::Literal(Literal::Integer(n));
        let col = |i: usize| PlanExpr::ColumnRef(i);
        let add = |l: PlanExpr, r: PlanExpr| PlanExpr::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(l),
            right: Box::new(r),
        };

        let scan = LogicalPlan::Scan {
            rootpage: 1,
            columns: vec![0, 1, 2, 3],
            with_key: false,
        };

        let inner = LogicalPlan::Project {
            input: Box::new(scan),
            columns: vec![
                add(col(0), lit(1)), // inner[0] = col0 + 1
                col(1),              // inner[1] = col1
                add(col(2), lit(2)), // inner[2] = col2 + 2
                add(col(3), lit(3)), // inner[3] = col3 + 3
            ],
        };

        let outer = LogicalPlan::Project {
            input: Box::new(inner),
            columns: vec![
                add(col(0), lit(1)), // (inner[0]) + 1
                col(1),              // inner[1]
                add(col(2), col(3)), // inner[2] + inner[3]
            ],
        };

        let fused = fuse_projects(outer);

        let expected_cols = vec![
            add(add(col(0), lit(1)), lit(1)),              // (col0+1)+1
            col(1),                                        // col1
            add(add(col(2), lit(2)), add(col(3), lit(3))), // (col2+2)+(col3+3)
        ];

        assert!(
            matches!(&fused, LogicalPlan::Project { columns, input }
                if columns == &expected_cols
                && matches!(input.as_ref(), LogicalPlan::Scan { .. })),
            "unexpected fused plan: {:?}",
            fused
        );
    }

    #[test]
    fn optimizer_promotes_nested_loop_to_hash_when_no_index() {
        let (mut db, users_root) = make_users_db();
        let orders_root = db.catalog.btree_mut().create_tree();
        db.catalog.insert_entry(
            "table",
            "orders",
            "orders",
            orders_root,
            "CREATE TABLE orders (user_id INTEGER, amount INTEGER)",
        );

        // Join { NestedLoop, Scan(users), Scan(orders), on: users.id = orders.user_id }
        // No index on orders.user_id → optimizer should promote to Hash.
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2],
                with_key: false,
            }),
            right: Box::new(LogicalPlan::Scan {
                rootpage: orders_root,
                columns: vec![0, 1],
                with_key: false,
            }),
            on_condition: PlanExpr::BinaryOp {
                op: BinaryOp::Equals,
                left: Box::new(PlanExpr::ColumnRef(0)),
                right: Box::new(PlanExpr::ColumnRef(3)),
            },
            strategy: JoinStrategy::NestedLoop,
            left_column_count: 3,
        };

        let optimized = optimize(plan, &db.catalog);
        assert!(
            matches!(
                &optimized,
                LogicalPlan::Join {
                    strategy: JoinStrategy::Hash,
                    ..
                }
            ),
            "expected Hash strategy, got: {:?}",
            optimized
        );
    }

    #[test]
    fn optimizer_rewrites_nested_loop_to_index_probe_when_index_exists() {
        // users(id, name, age): 3 cols; orders(user_id, amount): 2 cols
        // Index on orders.user_id.
        // Join { NestedLoop, Scan(users), Scan(orders), on: users.id = orders.user_id }
        // = ColumnRef(0) = ColumnRef(3)  [0 is left.id; 3 = left_col_count(3) + 0 (user_id)]
        let (mut db, users_root) = make_users_db();
        let orders_root = db.catalog.btree_mut().create_tree();
        db.catalog.insert_entry(
            "table",
            "orders",
            "orders",
            orders_root,
            "CREATE TABLE orders (user_id INTEGER, amount INTEGER)",
        );
        let idx_root = db.catalog.btree_mut().create_tree();
        db.catalog.insert_entry(
            "index",
            "idx_orders_user",
            "orders",
            idx_root,
            "CREATE INDEX idx_orders_user ON orders (user_id)",
        );

        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2],
                with_key: false,
            }),
            right: Box::new(LogicalPlan::Scan {
                rootpage: orders_root,
                columns: vec![0, 1],
                with_key: false,
            }),
            on_condition: PlanExpr::BinaryOp {
                op: BinaryOp::Equals,
                left: Box::new(PlanExpr::ColumnRef(0)), // users.id
                right: Box::new(PlanExpr::ColumnRef(3)), // orders.user_id in combined space
            },
            strategy: JoinStrategy::NestedLoop,
            left_column_count: 3,
        };

        let optimized = optimize(plan, &db.catalog);

        // Should keep NestedLoop with IndexProbe as right child
        assert!(
            matches!(
                &optimized,
                LogicalPlan::Join {
                    strategy: JoinStrategy::NestedLoop,
                    right,
                    ..
                } if matches!(right.as_ref(), LogicalPlan::RowidLookup { input, .. }
                    if matches!(input.as_ref(), LogicalPlan::IndexProbe { .. }))
            ),
            "expected NestedLoop with RowidLookup(IndexProbe), got: {:?}",
            optimized
        );
    }
}
