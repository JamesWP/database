//! SELECT planning: plan_select, plan_select_with_joins, and supporting helpers.

use std::collections::{HashMap, HashSet};

use crate::frontend::ast;
use crate::storage::BTree;

use schema::resolve_table;

use super::{
    schema, AggregateExpr, Literal, LogicalPlan, PlanError, PlanExpr, SortKey,
};
use super::resolver::{
    build_column_mapping, collect_columns, collect_columns_from_column_expr, convert_aggregate,
    convert_column_expr, convert_expr, convert_having_expr, extract_limit_value, extract_table_info,
    has_aggregate, is_aggregate_function, remap_column_indices, JoinResolver, SingleTableResolver,
};

pub(super) fn output_width(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Project { columns, .. } => columns.len(),
        LogicalPlan::Aggregate {
            group_keys,
            aggregates,
            ..
        } => group_keys.len() + aggregates.len(),
        LogicalPlan::Count { .. } => 1,
        LogicalPlan::Values { rows } => rows.first().map(|r| r.len()).unwrap_or(0),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::RowidLookup { input, .. } => output_width(input),
        LogicalPlan::Scan { columns, .. } => columns.len(),
        _ => 0,
    }
}

pub(super) fn plan_select(
    select: ast::SelectStatement,
    btree: &BTree,
) -> Result<LogicalPlan, PlanError> {
    // 1. Extract table info from FROM clause
    let (table_name, table_ref) = extract_table_info(&select.from)?;

    // 2. Look up table in catalog
    let table = resolve_table(&table_name, btree)?;

    // 3. Detect if this query uses aggregation
    let is_distinct = select.distinct;
    let has_group_by = select.group_by.is_some();
    let has_aggregates = select.columns.iter().any(|col| has_aggregate(col));
    let use_aggregation = has_group_by || has_aggregates;

    // 4. Collect all column references from SELECT, WHERE, GROUP BY, and ORDER BY
    let mut columns_needed = HashSet::new();
    let has_wildcard = select
        .columns
        .iter()
        .any(|col| matches!(col, ast::ColumnExpression::Wildcard));

    if has_wildcard {
        // If SELECT *, include all columns from the table
        for col in &table.columns {
            columns_needed.insert(col.name.clone());
        }
    } else {
        for col_expr in &select.columns {
            collect_columns_from_column_expr(col_expr, &mut columns_needed);
        }
    }
    if let Some(ref filter) = select.filter {
        collect_columns(filter, &mut columns_needed);
    }
    if let Some(ref group_by) = select.group_by {
        for expr in group_by {
            collect_columns(expr, &mut columns_needed);
        }
    }
    if let Some(ref order_by) = select.order_by {
        for clause in order_by {
            collect_columns(&clause.expression, &mut columns_needed);
        }
    }

    // 5. Build column mapping
    let mapping = build_column_mapping(&columns_needed, &table, &table_ref)?;

    // 6. Build column resolver
    let resolver = SingleTableResolver {
        table_ref: &table_ref,
        columns: &mapping.column_map,
    };

    // 7. Check for COUNT(*) special case (only if no GROUP BY)
    let is_count_star = !has_group_by
        && select.columns.len() == 1
        && matches!(
            &select.columns[0],
            ast::ColumnExpression::Anonyomous(expr)
                if matches!(
                    expr.as_ref(),
                    ast::Expression::FunctionCall { name, args }
                        if name.to_uppercase() == "COUNT" && args.is_empty()
                )
        );

    // 8. Build plan bottom-up: IndexScan | (Scan → Filter?) → Count/Aggregate/Project → Sort? → Limit?
    let mut plan = if let Some(ref filter) = select.filter {
        if let Some(index_scan) = try_plan_index_scan(
            filter,
            &table_name,
            table.rootpage,
            &mapping.scan_columns,
            &table.columns,
            btree,
        ) {
            index_scan
        } else {
            // Fall back to Scan + Filter
            let scan = LogicalPlan::Scan {
                rootpage: table.rootpage,
                columns: mapping.scan_columns,
                with_key: false,
            };
            LogicalPlan::Filter {
                input: Box::new(scan),
                predicate: convert_expr(filter, &resolver)?,
            }
        }
    } else {
        LogicalPlan::Scan {
            rootpage: table.rootpage,
            columns: mapping.scan_columns,
            with_key: false,
        }
    };

    // Add aggregation, count, or projection
    // Validate: HAVING without GROUP BY or aggregates is an error
    if select.having.is_some() && !use_aggregation && !is_count_star {
        return Err(PlanError::InvalidHaving(
            "HAVING requires GROUP BY or aggregate functions".into(),
        ));
    }

    if is_count_star {
        // SELECT COUNT(*) without GROUP BY - use simple Count node
        plan = LogicalPlan::Count {
            input: Box::new(plan),
        };
    } else if use_aggregation {
        // GROUP BY or aggregates in SELECT - use Aggregate node
        let group_keys: Vec<PlanExpr> = if let Some(ref group_by) = select.group_by {
            group_by
                .iter()
                .map(|expr| convert_expr(expr, &resolver))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![] // No GROUP BY = one big group
        };

        // Process SELECT columns: track both aggregates and projection mapping
        let mut aggregates: Vec<AggregateExpr> = Vec::new();
        let mut projection_indices: Vec<usize> = Vec::new();

        for col_expr in &select.columns {
            let expr = match col_expr {
                ast::ColumnExpression::Named { expression, .. } => expression.as_ref(),
                ast::ColumnExpression::Anonyomous(expression) => expression.as_ref(),
                ast::ColumnExpression::Wildcard => continue, // Skip wildcards for now
            };

            if is_aggregate_function(expr) {
                // This SELECT column is an aggregate
                // It will appear in the Aggregate output after all group keys
                let agg_index_in_output = group_keys.len() + aggregates.len();
                projection_indices.push(agg_index_in_output);
                aggregates.push(convert_aggregate(expr, &resolver)?);
            } else {
                // This SELECT column is a non-aggregate (must be a group key)
                let group_expr = convert_expr(expr, &resolver)?;
                // Find which group key this matches
                let group_key_index = group_keys
                    .iter()
                    .position(|gk| gk == &group_expr)
                    .ok_or_else(|| PlanError::UnsupportedStatement)?; // Non-aggregate not in GROUP BY
                projection_indices.push(group_key_index);
            }
        }

        // Convert HAVING expression (after building group_keys and aggregates)
        let having = if let Some(having_expr) = select.having {
            if !use_aggregation {
                return Err(PlanError::InvalidHaving(
                    "HAVING requires GROUP BY or aggregate functions".into(),
                ));
            }
            Some(convert_having_expr(
                &having_expr,
                &group_keys,
                &mut aggregates,
                &resolver,
            )?)
        } else {
            None
        };

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys,
            aggregates,
            having,
        };

        // Add projection to select only the SELECT columns in the correct order
        // Aggregate outputs: [group_key_0, group_key_1, ..., agg_0, agg_1, ...]
        // We project the indices that correspond to SELECT columns
        let project_exprs: Vec<PlanExpr> = projection_indices
            .into_iter()
            .map(|idx| PlanExpr::ColumnRef(idx))
            .collect();

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns: project_exprs,
        };
    } else {
        // Regular SELECT - add Project
        let mut project_exprs: Vec<PlanExpr> = select
            .columns
            .iter()
            .flat_map(|col_expr| {
                match col_expr {
                    ast::ColumnExpression::Wildcard => {
                        // Expand wildcard to all columns in table order
                        table
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(idx, _col)| Ok(PlanExpr::ColumnRef(idx)))
                            .collect::<Vec<_>>()
                    }
                    _ => vec![convert_column_expr(col_expr, &resolver)],
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let select_column_count = project_exprs.len();

        // If there's ORDER BY, check if any ORDER BY columns are not in SELECT
        // If so, add them to the projection so they're available for sorting
        let mut extra_order_columns = Vec::new();
        if let Some(ref order_by) = select.order_by {
            for clause in order_by {
                // Check if this is a simple column reference
                if let ast::Expression::Value(ast::ScalarValue::Identifier(col_name)) =
                    &clause.expression
                {
                    // Check if this column is already in the SELECT list
                    let already_in_select = select.columns.iter().any(|col_expr| match col_expr {
                        ast::ColumnExpression::Anonyomous(expr) => {
                            matches!(
                                expr.as_ref(),
                                ast::Expression::Value(ast::ScalarValue::Identifier(name))
                                    if name == col_name
                            )
                        }
                        ast::ColumnExpression::Named { expression, .. } => {
                            matches!(
                                expression.as_ref(),
                                ast::Expression::Value(ast::ScalarValue::Identifier(name))
                                    if name == col_name
                            )
                        }
                        ast::ColumnExpression::Wildcard => false,
                    });

                    if !already_in_select {
                        // Add this column to the projection
                        let order_col_expr = convert_expr(&clause.expression, &resolver)?;
                        extra_order_columns.push(order_col_expr);
                    }
                }
            }
        }

        let has_extra_order_columns = !extra_order_columns.is_empty();
        project_exprs.extend(extra_order_columns);

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns: project_exprs.clone(),
        };

        // Add Sort if ORDER BY clause exists
        if let Some(ref order_by) = select.order_by {
            // Build a map from scan column index to projection index
            // This tells us where each scan column ended up in the projection
            let mut scan_idx_to_proj_idx: HashMap<usize, usize> = HashMap::new();
            for (proj_idx, expr) in project_exprs.iter().enumerate() {
                if let PlanExpr::ColumnRef(column_idx) = expr {
                    scan_idx_to_proj_idx.insert(*column_idx, proj_idx);
                }
            }

            // Convert ORDER BY expressions using the original scan context,
            // then remap the column indices to projection indices
            let sort_keys: Result<Vec<SortKey>, _> = order_by
                .iter()
                .map(|clause| {
                    // First, resolve against scan columns (this handles case-insensitive lookup)
                    let scan_expr = convert_expr(&clause.expression, &resolver)?;

                    // Remap scan column indices to projection indices
                    let proj_expr = remap_column_indices(&scan_expr, &scan_idx_to_proj_idx)?;

                    Ok(SortKey {
                        expr: proj_expr,
                        descending: clause.direction == ast::OrderDirection::Desc,
                    })
                })
                .collect();

            let sort_keys = sort_keys?;

            // Sort elision: if there is exactly one ASC sort key that resolves to a
            // table column already ordered by an IndexScan, skip the Sort node entirely.
            let elide = if sort_keys.len() == 1 && !sort_keys[0].descending {
                // Reverse-map projection index → scan column index
                let proj_to_scan: HashMap<usize, usize> = scan_idx_to_proj_idx
                    .iter()
                    .map(|(&scan, &proj)| (proj, scan))
                    .collect();

                if let PlanExpr::ColumnRef(proj_idx) = sort_keys[0].expr {
                    if let Some(&scan_col_idx) = proj_to_scan.get(&proj_idx) {
                        can_elide_sort(&plan, scan_col_idx)
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !elide {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    sort_keys,
                };
            }

            // If we added extra columns for ORDER BY, add a final projection to remove them
            if has_extra_order_columns {
                let final_project: Vec<PlanExpr> = (0..select_column_count)
                    .map(|idx| PlanExpr::ColumnRef(idx))
                    .collect();

                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns: final_project,
                };
            }
        }
    }

    // Add Distinct if SELECT DISTINCT
    if is_distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    // Add Limit if LIMIT clause exists
    if let Some(ref limit_expr) = select.limit {
        let count = extract_limit_value(limit_expr)?;
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count,
        };
    }

    Ok(plan)
}

pub(super) fn plan_select_with_joins(
    select: ast::SelectStatement,
    btree: &BTree,
) -> Result<LogicalPlan, PlanError> {
    // Support single join for now
    if select.joins.len() != 1 {
        return Err(PlanError::UnsupportedStatement);
    }

    // 1. Resolve left table (FROM clause)
    let (left_name, left_ref) = extract_table_info(&select.from)?;
    let left_table = resolve_table(&left_name, btree)?;
    let left_col_count = left_table.columns.len();

    // 2. Resolve right table (first join clause)
    let join_clause = &select.joins[0];
    let (right_name, right_ref) = extract_table_info(&join_clause.table)?;
    let right_table = resolve_table(&right_name, btree)?;
    let right_col_count = right_table.columns.len();

    // 3. Build join column resolution maps
    let mut qualified = HashMap::new();
    let mut unqualified = HashMap::new();

    // Add left table columns (positions 0..left_col_count)
    for (idx, col) in left_table.columns.iter().enumerate() {
        qualified.insert((left_ref.clone(), col.name.clone()), idx);
        unqualified
            .entry(col.name.clone())
            .and_modify(|e| *e = None) // Mark as ambiguous if already exists
            .or_insert(Some(idx));
    }

    // Add right table columns (positions left_col_count..)
    for (idx, col) in right_table.columns.iter().enumerate() {
        let combined_idx = left_col_count + idx;
        qualified.insert((right_ref.clone(), col.name.clone()), combined_idx);
        unqualified
            .entry(col.name.clone())
            .and_modify(|e| *e = None) // Mark as ambiguous if already exists
            .or_insert(Some(combined_idx));
    }

    let join_resolver = JoinResolver {
        qualified: &qualified,
        unqualified: &unqualified,
    };

    // 4. Build scan plans (read ALL columns from each table)
    let left_scan = LogicalPlan::Scan {
        rootpage: left_table.rootpage,
        columns: (0..left_col_count).collect(),
        with_key: false,
    };
    let right_scan = LogicalPlan::Scan {
        rootpage: right_table.rootpage,
        columns: (0..right_col_count).collect(),
        with_key: false,
    };

    // 5. Convert ON condition using join resolver
    let on_condition = convert_expr(&join_clause.on_condition, &join_resolver)?;

    // 6. Build Join plan
    let mut plan = LogicalPlan::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        on_condition,
        left_column_count: left_col_count,
    };

    // 7. Add WHERE filter if present (also uses join resolver)
    if let Some(ref filter) = select.filter {
        let predicate = convert_expr(filter, &join_resolver)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // 8. Project SELECT columns
    let mut project_columns: Vec<PlanExpr> = Vec::new();
    for col_expr in &select.columns {
        match col_expr {
            ast::ColumnExpression::Wildcard => {
                // Expand to all columns from both tables
                for idx in 0..(left_col_count + right_col_count) {
                    project_columns.push(PlanExpr::ColumnRef(idx));
                }
            }
            ast::ColumnExpression::Named { expression, .. } => {
                project_columns.push(convert_expr(expression, &join_resolver)?);
            }
            ast::ColumnExpression::Anonyomous(expression) => {
                project_columns.push(convert_expr(expression, &join_resolver)?);
            }
        }
    }

    let select_column_count = project_columns.len();

    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: project_columns.clone(),
    };

    // 9. ORDER BY (if present) - similar to plan_select
    if let Some(ref order_by) = select.order_by {
        // Build a map from join output index to projection index
        let mut join_idx_to_proj_idx: HashMap<usize, usize> = HashMap::new();
        for (proj_idx, expr) in project_columns.iter().enumerate() {
            if let PlanExpr::ColumnRef(column_idx) = expr {
                join_idx_to_proj_idx.insert(*column_idx, proj_idx);
            }
        }

        // Check if any ORDER BY columns are not in the SELECT list
        let mut extra_order_columns = Vec::new();
        for clause in order_by.iter() {
            let order_expr = convert_expr(&clause.expression, &join_resolver)?;

            // Check if this column is already in the projection
            let already_in_select = project_columns.iter().any(|e| e == &order_expr);

            if !already_in_select {
                extra_order_columns.push(order_expr);
            }
        }

        let has_extra_order_columns = !extra_order_columns.is_empty();
        if has_extra_order_columns {
            project_columns.extend(extra_order_columns);
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                columns: project_columns.clone(),
            };

            // Rebuild the index map with the extended projection
            join_idx_to_proj_idx.clear();
            for (proj_idx, expr) in project_columns.iter().enumerate() {
                if let PlanExpr::ColumnRef(column_idx) = expr {
                    join_idx_to_proj_idx.insert(*column_idx, proj_idx);
                }
            }
        }

        // Convert ORDER BY expressions and remap column indices
        let sort_keys: Result<Vec<SortKey>, _> = order_by
            .iter()
            .map(|clause| {
                let join_expr = convert_expr(&clause.expression, &join_resolver)?;
                let proj_expr = remap_column_indices(&join_expr, &join_idx_to_proj_idx)?;

                Ok(SortKey {
                    expr: proj_expr,
                    descending: clause.direction == ast::OrderDirection::Desc,
                })
            })
            .collect();

        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            sort_keys: sort_keys?,
        };

        // If we added extra columns for ORDER BY, add a final projection to remove them
        if has_extra_order_columns {
            let final_project: Vec<PlanExpr> = (0..select_column_count)
                .map(|idx| PlanExpr::ColumnRef(idx))
                .collect();

            plan = LogicalPlan::Project {
                input: Box::new(plan),
                columns: final_project,
            };
        }
    }

    // 10. LIMIT (if present)
    if let Some(ref limit_expr) = select.limit {
        let count = extract_limit_value(limit_expr)?;
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count,
        };
    }

    Ok(plan)
}

/// Try to use an index for the given filter (equality or range predicate).
pub(super) fn try_plan_index_scan(
    filter: &ast::Expression,
    table_name: &str,
    table_rootpage: u32,
    scan_columns: &[usize],
    table_columns: &[schema::Column],
    btree: &BTree,
) -> Option<LogicalPlan> {
    let (col_name, lower_bound, upper_bound) =
        if let Some((col, lit)) = extract_equality_filter(filter) {
            // Equality: col = X → [X, X] inclusive on both sides
            (col, Some((lit.clone(), true)), Some((lit, true)))
        } else if let Some(col) = extract_is_null_filter(filter) {
            // IS NULL: use Null literal as both bounds
            (
                col,
                Some((Literal::Null, true)),
                Some((Literal::Null, true)),
            )
        } else {
            extract_range_filter(filter)?
        };

    let indexes = btree.lookup_indexes_for_table(table_name);
    // Find an index whose first column matches col_name (prefix matching)
    let index = indexes
        .iter()
        .find(|idx| idx.column_names.first().map(|s| s.as_str()) == Some(col_name.as_str()))?;

    // Resolve the column name to a table column index
    let index_col_idx = table_columns
        .iter()
        .position(|c| c.name == col_name)
        .unwrap_or(0);

    Some(LogicalPlan::RowidLookup {
        input: Box::new(LogicalPlan::IndexScan {
            index_rootpage: index.rootpage,
            index_col_idx,
            lower_bound,
            upper_bound,
        }),
        table_rootpage,
        columns: scan_columns.to_vec(),
    })
}

/// Returns true if the plan tree rooted at `plan` contains an IndexScan on
/// `sort_col_idx` with only pass-through nodes (Project/Filter/Limit/RowidLookup)
/// in between — meaning the scan already produces rows in ascending order by that column.
pub(super) fn can_elide_sort(plan: &LogicalPlan, sort_col_idx: usize) -> bool {
    match plan {
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. } => can_elide_sort(input, sort_col_idx),

        LogicalPlan::RowidLookup { input, .. } => can_elide_sort(input, sort_col_idx),

        LogicalPlan::IndexScan { index_col_idx, .. } => *index_col_idx == sort_col_idx,

        _ => false,
    }
}

/// Extract a range filter from an expression involving a single column.
/// Returns (column_name, lower_bound, upper_bound).
/// Each bound is (Literal, inclusive: bool).
pub(super) fn extract_range_filter(
    expr: &ast::Expression,
) -> Option<(String, Option<(Literal, bool)>, Option<(Literal, bool)>)> {
    // Try single comparison: col > lit, col >= lit, col < lit, col <= lit
    if let Some((col, lower, upper)) = extract_single_range(expr) {
        return Some((col, lower, upper));
    }
    // Try AND: (col > L) AND (col < U)
    if let ast::Expression::BinaryOp {
        op: ast::BinaryOp::And,
        lhs,
        rhs,
    } = expr
    {
        let left = extract_single_range(lhs)?;
        let right = extract_single_range(rhs)?;
        if left.0 != right.0 {
            return None; // Different columns
        }
        let col = left.0;
        let lower = left.1.or(right.1);
        let upper = left.2.or(right.2);
        if lower.is_none() && upper.is_none() {
            return None;
        }
        return Some((col, lower, upper));
    }
    None
}

pub(super) fn extract_single_range(
    expr: &ast::Expression,
) -> Option<(String, Option<(Literal, bool)>, Option<(Literal, bool)>)> {
    let ast::Expression::BinaryOp { op, lhs, rhs } = expr else {
        return None;
    };
    match op {
        ast::BinaryOp::GreaterThan => {
            let col = extract_column_name(lhs)?;
            let lit = extract_literal(rhs)?;
            Some((col, Some((lit, false)), None))
        }
        ast::BinaryOp::GreaterThanOrEqual => {
            let col = extract_column_name(lhs)?;
            let lit = extract_literal(rhs)?;
            Some((col, Some((lit, true)), None))
        }
        ast::BinaryOp::LessThan => {
            let col = extract_column_name(lhs)?;
            let lit = extract_literal(rhs)?;
            Some((col, None, Some((lit, false))))
        }
        ast::BinaryOp::LessThanOrEqual => {
            let col = extract_column_name(lhs)?;
            let lit = extract_literal(rhs)?;
            Some((col, None, Some((lit, true))))
        }
        _ => None,
    }
}

pub(super) fn extract_is_null_filter(expr: &ast::Expression) -> Option<String> {
    match expr {
        ast::Expression::UnaryOp {
            op: ast::UnaryOp::IsNull,
            expression,
        } => extract_column_name(expression),
        _ => None,
    }
}

pub(super) fn extract_equality_filter(expr: &ast::Expression) -> Option<(String, Literal)> {
    match expr {
        ast::Expression::BinaryOp { op, lhs, rhs } if matches!(op, ast::BinaryOp::Equals) => {
            // Case 1: col = literal
            if let Some(col) = extract_column_name(lhs) {
                if let Some(lit) = extract_literal(rhs) {
                    return Some((col, lit));
                }
            }
            // Case 2: literal = col
            if let Some(col) = extract_column_name(rhs) {
                if let Some(lit) = extract_literal(lhs) {
                    return Some((col, lit));
                }
            }
            None
        }
        _ => None,
    }
}

pub(super) fn extract_column_name(expr: &ast::Expression) -> Option<String> {
    match expr {
        ast::Expression::Value(ast::ScalarValue::Identifier(name)) => Some(name.clone()),
        _ => None,
    }
}

pub(super) fn extract_literal(expr: &ast::Expression) -> Option<Literal> {
    match expr {
        ast::Expression::Value(s) => match s {
            ast::ScalarValue::IntegerNumber(i) => Some(Literal::Integer(*i)),
            ast::ScalarValue::StringLiteral(s) => Some(Literal::String(s.clone())),
            ast::ScalarValue::FloatingNumber(f) => Some(Literal::Float(*f)),
            _ => None,
        },
        _ => None,
    }
}
