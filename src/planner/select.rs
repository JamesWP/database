//! SELECT planning: plan_select, plan_select_with_joins, and supporting helpers.

use std::collections::{HashMap, HashSet};

use crate::frontend::ast;
use crate::storage::BTree;

use schema::resolve_table;

use super::resolver::{
    build_column_mapping, collect_columns, collect_columns_from_column_expr, convert_aggregate,
    convert_column_expr, convert_expr, convert_having_expr, extract_limit_value,
    extract_table_info, has_aggregate, is_aggregate_function, remap_column_indices, JoinResolver,
    SingleTableResolver,
};
use super::{schema, AggregateExpr, LogicalPlan, PlanError, PlanExpr, SortKey};

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

pub(crate) fn plan_select(
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

    // 8. Build naive plan bottom-up: Scan → Filter? → Count/Aggregate/Project → Sort? → Limit?
    // Index selection and sort elision are handled by the optimizer pass.
    let mut plan = if let Some(ref filter) = select.filter {
        let scan = LogicalPlan::Scan {
            rootpage: table.rootpage,
            columns: mapping.scan_columns,
            with_key: false,
        };
        LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: convert_expr(filter, &resolver)?,
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
        let has_wildcard = select
            .columns
            .iter()
            .any(|c| matches!(c, ast::ColumnExpression::Wildcard));
        let mut extra_order_columns = Vec::new();
        if let Some(ref order_by) = select.order_by {
            for clause in order_by {
                // Check if this is a simple column reference
                if let ast::Expression::Value(ast::ScalarValue::Identifier(col_name)) =
                    &clause.expression
                {
                    // Check if this column is already in the SELECT list.
                    // A wildcard expands to all table columns, so any ORDER BY column is covered.
                    let already_in_select = has_wildcard
                        || select.columns.iter().any(|col_expr| match col_expr {
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

            // Always add Sort; optimizer pass will elide it when an IndexScan provides the order.
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                sort_keys,
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use crate::frontend::{ast, parse};
    use crate::planner::{plan, BinaryOp, Literal, LogicalPlan, PlanError, PlanExpr};
    use crate::test::TestDb;

    /// Create a test database with a "users" table (id, name, age) registered in the catalog.
    fn make_users_db() -> (TestDb, u32) {
        let mut test = TestDb::default();
        let users_root = test.btree.create_tree();
        test.btree.insert_schema_entry(
            "table",
            "users",
            "users",
            users_root,
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
        );
        (test, users_root)
    }

    fn parse_sql(sql: &str) -> ast::Statement {
        parse(sql).expect("Failed to parse SQL")
    }

    /// Example 1: Simple SELECT
    /// SELECT id, name FROM users
    #[test]
    fn test_simple_select() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT id, name FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1], // id, name
                with_key: false,
            }),
            columns: vec![PlanExpr::ColumnRef(0), PlanExpr::ColumnRef(1)],
        };

        assert_eq!(plan, expected);
    }

    /// Example 2: SELECT with WHERE
    /// SELECT name FROM users WHERE age > 21
    #[test]
    fn test_select_with_where() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users WHERE age > 21");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    rootpage: users_root,
                    columns: vec![1, 2], // name, age
                    with_key: false,
                }),
                predicate: PlanExpr::BinaryOp {
                    op: BinaryOp::GreaterThan,
                    left: Box::new(PlanExpr::ColumnRef(1)), // age
                    right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
                },
            }),
            columns: vec![PlanExpr::ColumnRef(0)], // name
        };

        assert_eq!(plan, expected);
    }

    /// Example 3: SELECT with LIMIT
    /// SELECT name FROM users LIMIT 10
    #[test]
    fn test_select_with_limit() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users LIMIT 10");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    rootpage: users_root,
                    columns: vec![1], // name
                    with_key: false,
                }),
                columns: vec![PlanExpr::ColumnRef(0)],
            }),
            count: 10,
        };

        assert_eq!(plan, expected);
    }

    /// SELECT * should expand to all columns
    #[test]
    fn test_select_star() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT * FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_star_multi_column() {
        // Create table with 5 columns
        let mut test = TestDb::default();
        let root = test.btree.create_tree();
        test.btree.insert_schema_entry(
            "table",
            "data",
            "data",
            root,
            "CREATE TABLE data (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)",
        );

        let stmt = parse_sql("SELECT * FROM data");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: root,
                columns: vec![0, 1, 2, 3, 4], // all 5 columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
                PlanExpr::ColumnRef(3),
                PlanExpr::ColumnRef(4),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_star_with_literal() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT *, 999 FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
                PlanExpr::Literal(Literal::Integer(999)),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_literal_star() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT 999, * FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::Literal(Literal::Integer(999)),
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_star_with_expression() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT *, age + 10 FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
                PlanExpr::BinaryOp {
                    op: BinaryOp::Add,
                    left: Box::new(PlanExpr::ColumnRef(2)),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(10))),
                },
            ],
        };

        assert_eq!(plan, expected);
    }

    /// Error case: table not found
    #[test]
    fn test_table_not_found() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT id FROM nonexistent");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::TableNotFound("nonexistent".to_string()))
        );
    }

    /// Error case: column not found
    #[test]
    fn test_column_not_found() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT nonexistent FROM users");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }

    #[test]
    fn test_select_null_literal() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT NULL FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![], // No columns needed from scan
                with_key: false,
            }),
            columns: vec![PlanExpr::Literal(Literal::Null)],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_null_with_columns() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT id, NULL, name FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1], // id, name
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::Literal(Literal::Null),
                PlanExpr::ColumnRef(1),
            ],
        };

        assert_eq!(plan, expected);
    }

    // ========================================================================
    // HAVING Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_having_count_star() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 3");
        let result = plan(stmt, &test.btree).expect("Planning failed");
        // Walk to find the Aggregate node
        fn find_aggregate(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                p @ LogicalPlan::Aggregate { .. } => Some(p),
                LogicalPlan::Project { input, .. } => find_aggregate(input),
                LogicalPlan::Sort { input, .. } => find_aggregate(input),
                LogicalPlan::Limit { input, .. } => find_aggregate(input),
                _ => None,
            }
        }
        let agg = find_aggregate(&result).expect("Expected Aggregate node");
        match agg {
            LogicalPlan::Aggregate { having, .. } => {
                assert!(having.is_some(), "expected HAVING predicate in plan");
            }
            _ => panic!("Expected Aggregate node"),
        }
    }

    #[test]
    fn test_plan_having_without_group_by_errors() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT id FROM users HAVING COUNT(*) > 1");
        let err = plan(stmt, &test.btree).expect_err("Expected planning error");
        assert!(matches!(err, PlanError::InvalidHaving(_)));
    }

    // ========================================================================
    // ORDER BY Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_order_by_column_in_select() {
        // ORDER BY a column that's in SELECT - should work without extra projection
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name, age FROM users ORDER BY age");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Check structure: Scan -> Project -> Sort
        if let LogicalPlan::Sort { input, sort_keys } = plan {
            assert_eq!(sort_keys.len(), 1);
            // Sort key should reference projection column 1 (age in the projection)
            if let PlanExpr::ColumnRef(column_idx) = &sort_keys[0].expr {
                assert_eq!(*column_idx, 1, "age should be at projection index 1");
            } else {
                panic!("Expected simple column reference in sort key");
            }
            // Input should be Project with 2 columns (name, age)
            if let LogicalPlan::Project { columns, .. } = *input {
                assert_eq!(
                    columns.len(),
                    2,
                    "Projection should have 2 columns (name, age)"
                );
            } else {
                panic!("Expected Project node as input to Sort");
            }
        } else {
            panic!("Expected Sort node, got {:?}", plan);
        }
    }

    #[test]
    fn test_plan_order_by_column_not_in_select() {
        // ORDER BY a column NOT in SELECT - should add extended projection and final projection
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users ORDER BY age");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Check structure: Scan -> Project(name, age) -> Sort -> Project(name)
        if let LogicalPlan::Project { input, columns } = plan {
            assert_eq!(
                columns.len(),
                1,
                "Final projection should have 1 column (name)"
            );

            // Input should be Sort
            if let LogicalPlan::Sort {
                input: sort_input,
                sort_keys,
            } = *input
            {
                assert_eq!(sort_keys.len(), 1);
                // Sort key should reference age at projection index 1
                if let PlanExpr::ColumnRef(column_idx) = &sort_keys[0].expr {
                    assert_eq!(
                        *column_idx, 1,
                        "age should be at projection index 1 in extended projection"
                    );
                } else {
                    panic!("Expected simple column reference in sort key");
                }

                // Sort input should be extended Project with 2 columns (name, age)
                if let LogicalPlan::Project { columns, .. } = *sort_input {
                    assert_eq!(
                        columns.len(),
                        2,
                        "Extended projection should have 2 columns (name, age)"
                    );
                } else {
                    panic!("Expected Project node as input to Sort");
                }
            } else {
                panic!("Expected Sort node as input to final projection");
            }
        } else {
            panic!("Expected final Project node, got {:?}", plan);
        }
    }

    #[test]
    fn test_plan_order_by_multiple_columns() {
        // ORDER BY multiple columns - should handle both in and not in SELECT
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users ORDER BY age DESC, name ASC");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Should have final projection to remove age
        if let LogicalPlan::Project { input, .. } = plan {
            if let LogicalPlan::Sort { sort_keys, .. } = *input {
                assert_eq!(sort_keys.len(), 2, "Should have 2 sort keys");
                assert_eq!(
                    sort_keys[0].descending, true,
                    "First sort key (age) should be DESC"
                );
                assert_eq!(
                    sort_keys[1].descending, false,
                    "Second sort key (name) should be ASC"
                );
            } else {
                panic!("Expected Sort node");
            }
        } else {
            panic!("Expected final Project node");
        }
    }

    #[test]
    fn test_plan_order_by_with_function_in_select() {
        // ORDER BY column not in SELECT, but SELECT has function expressions
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT upper(name) FROM users ORDER BY age");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Should have structure: Scan -> Project(upper(name), age) -> Sort -> Project(upper(name))
        if let LogicalPlan::Project { input, columns } = plan {
            assert_eq!(
                columns.len(),
                1,
                "Final projection should have 1 column (upper(name))"
            );

            if let LogicalPlan::Sort { input, sort_keys } = *input {
                assert_eq!(sort_keys.len(), 1);

                // Extended projection should have 2 columns: upper(name) and age
                if let LogicalPlan::Project { columns, .. } = *input {
                    assert_eq!(
                        columns.len(),
                        2,
                        "Extended projection should have upper(name) and age"
                    );
                    // First should be function call, second should be column ref
                    assert!(
                        matches!(columns[0], PlanExpr::FunctionCall { .. }),
                        "First column should be function call"
                    );
                    assert!(
                        matches!(columns[1], PlanExpr::ColumnRef(_)),
                        "Second column should be column ref (age)"
                    );
                } else {
                    panic!("Expected Project node");
                }
            } else {
                panic!("Expected Sort node");
            }
        } else {
            panic!("Expected final Project node");
        }
    }

    // ========================================================================
    // JOIN Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_join() {
        // Create TestDb and two tables
        let test = TestDb::default();
        let mut btree = test.btree;

        // Get the catalog root
        let catalog_root = btree.schema_root_page().expect("No catalog");

        // Create departments table (id, name)
        let dept_root = btree.create_tree();
        {
            use crate::engine::scalarvalue::ScalarValue;
            let ddl = "CREATE TABLE departments (id INTEGER, name TEXT)";
            let values = vec![
                ScalarValue::String("table".to_string()),
                ScalarValue::String("departments".to_string()),
                ScalarValue::String("departments".to_string()),
                ScalarValue::Integer(dept_root as i64),
                ScalarValue::String(ddl.to_string()),
            ];
            let mut cursor = btree.open(catalog_root);
            let mut c = cursor.open_readwrite();
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&values, &mut buf).unwrap();
            c.insert_u64(1, buf); // key 1 (catalog row 0 is self-referencing db_schema)
        }

        // Create employees table (id, name, dept_id)
        let emp_root = btree.create_tree();
        {
            use crate::engine::scalarvalue::ScalarValue;
            let ddl = "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)";
            let values = vec![
                ScalarValue::String("table".to_string()),
                ScalarValue::String("employees".to_string()),
                ScalarValue::String("employees".to_string()),
                ScalarValue::Integer(emp_root as i64),
                ScalarValue::String(ddl.to_string()),
            ];
            let mut cursor = btree.open(catalog_root);
            let mut c = cursor.open_readwrite();
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&values, &mut buf).unwrap();
            c.insert_u64(2, buf); // key 2
        }

        // Plan: "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id"
        let stmt = parse_sql(
            "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id",
        );
        let plan = plan(stmt, &btree).expect("Planning should succeed");

        // Verify plan structure: Project { Join { Scan(employees), Scan(departments), ... }, ... }
        if let LogicalPlan::Project { input, columns } = plan {
            assert_eq!(columns.len(), 2, "Should project 2 columns");

            // Both should be column references
            assert!(matches!(columns[0], PlanExpr::ColumnRef(_)));
            assert!(matches!(columns[1], PlanExpr::ColumnRef(_)));

            if let LogicalPlan::Join {
                left,
                right,
                on_condition,
                left_column_count,
            } = *input
            {
                assert_eq!(left_column_count, 3, "Employees has 3 columns");

                // Left should be Scan of employees
                assert!(matches!(*left, LogicalPlan::Scan { .. }));

                // Right should be Scan of departments
                assert!(matches!(*right, LogicalPlan::Scan { .. }));

                // ON condition should be a binary operation
                assert!(matches!(on_condition, PlanExpr::BinaryOp { .. }));
            } else {
                panic!("Expected Join node");
            }
        } else {
            panic!("Expected Project node");
        }
    }

    // ========================================================================
    // Index Scan Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_index_scan() {
        let test = TestDb::default();
        let mut btree = test.btree;

        // Create table and index
        let sql_table = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);

        // Query that should use index
        let stmt = parse_sql("SELECT name FROM users WHERE age = 30");
        let plan = plan(stmt, &btree).expect("Planning failed");

        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup {
                    input,
                    table_rootpage,
                    ..
                } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                    } => {
                        assert_eq!(index_rootpage, index_root);
                        assert_eq!(lower_bound, Some((Literal::Integer(30), true)));
                        assert_eq!(upper_bound, Some((Literal::Integer(30), true)));
                        assert_eq!(table_rootpage, users_root);
                    }
                    _ => panic!("Expected IndexScan inside RowidLookup, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", plan),
        }
    }

    #[test]
    fn test_plan_index_range_scan() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE data (id INTEGER, value INTEGER)";
        let data_root = btree.create_tree();
        btree.insert_schema_entry("table", "data", "data", data_root, sql_table);

        let sql_index = "CREATE INDEX idx_value ON data(value)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_value", "data", index_root, sql_index);

        // Test greater than
        let stmt = parse_sql("SELECT id FROM data WHERE value > 20");
        let p = plan(stmt, &btree).expect("Planning failed");
        match p {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup {
                    input,
                    table_rootpage,
                    ..
                } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                    } => {
                        assert_eq!(index_rootpage, index_root);
                        assert_eq!(lower_bound, Some((Literal::Integer(20), false)));
                        assert_eq!(upper_bound, None);
                        assert_eq!(table_rootpage, data_root);
                    }
                    _ => panic!("Expected IndexScan, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", p),
        }

        // Test range with AND
        let stmt = parse_sql("SELECT id FROM data WHERE value >= 10 AND value <= 40");
        let p = plan(stmt, &btree).expect("Planning failed");
        match p {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup { input, .. } => match *input {
                    LogicalPlan::IndexScan {
                        lower_bound,
                        upper_bound,
                        ..
                    } => {
                        assert_eq!(lower_bound, Some((Literal::Integer(10), true)));
                        assert_eq!(upper_bound, Some((Literal::Integer(40), true)));
                    }
                    _ => panic!("Expected IndexScan, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", p),
        }
    }

    #[test]
    fn test_plan_multi_column_index_uses_first_column() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE events (id INTEGER, year INTEGER, month INTEGER)";
        let root = btree.create_tree();
        btree.insert_schema_entry("table", "events", "events", root, sql_table);

        // Multi-column index on (year, month)
        let sql_index = "CREATE INDEX idx_year_month ON events(year, month)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_year_month", "events", index_root, sql_index);

        // Query on first column should use the index
        let stmt = parse_sql("SELECT id FROM events WHERE year = 2024");
        let plan = plan(stmt, &btree).expect("Planning failed");

        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup { input, .. } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                    } => {
                        assert_eq!(index_rootpage, index_root);
                        assert_eq!(lower_bound, Some((Literal::Integer(2024), true)));
                        assert_eq!(upper_bound, Some((Literal::Integer(2024), true)));
                    }
                    _ => panic!("Expected IndexScan, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", plan),
        }
    }

    #[test]
    fn test_plan_multi_column_index_not_used_for_non_first_column() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE events (id INTEGER, year INTEGER, month INTEGER)";
        let root = btree.create_tree();
        btree.insert_schema_entry("table", "events", "events", root, sql_table);

        // Multi-column index on (year, month) - only second column referenced
        let sql_index = "CREATE INDEX idx_year_month ON events(year, month)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_year_month", "events", index_root, sql_index);

        // Query on second column should NOT use the index (falls back to table scan)
        let stmt = parse_sql("SELECT id FROM events WHERE month = 6");
        let plan = plan(stmt, &btree).expect("Planning failed");

        // Should NOT contain IndexScan (uses table scan + filter instead)
        fn contains_index_scan(p: &LogicalPlan) -> bool {
            matches!(p, LogicalPlan::IndexScan { .. })
                || match p {
                    LogicalPlan::Project { input, .. } => contains_index_scan(input),
                    LogicalPlan::Filter { input, .. } => contains_index_scan(input),
                    LogicalPlan::RowidLookup { input, .. } => contains_index_scan(input),
                    _ => false,
                }
        }
        assert!(
            !contains_index_scan(&plan),
            "Should not use index for non-first column: {:?}",
            plan
        );
    }

    #[test]
    fn test_plan_distinct() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE colors (id INTEGER, category TEXT)";
        let colors_root = btree.create_tree();
        btree.insert_schema_entry("table", "colors", "colors", colors_root, sql_table);

        let stmt = parse_sql("SELECT DISTINCT category FROM colors");
        let plan = plan(stmt, &btree).expect("Planning failed");

        // Plan should be: Distinct { Project { Scan } }
        match plan {
            LogicalPlan::Distinct { input } => match *input {
                LogicalPlan::Project { input, .. } => match *input {
                    LogicalPlan::Scan { rootpage, .. } => {
                        assert_eq!(rootpage, colors_root);
                    }
                    _ => panic!("Expected Scan inside Project, got {:?}", input),
                },
                _ => panic!("Expected Project inside Distinct, got {:?}", input),
            },
            _ => panic!("Expected Distinct at top, got {:?}", plan),
        }
    }

    // ========================================================================
    // Sort Elision Tests
    // ========================================================================

    fn make_btree_with_index_on_age() -> crate::storage::BTree {
        let test = TestDb::default();
        let mut btree = test.btree;
        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);
        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);
        btree
    }

    fn plan_contains_sort(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Sort { .. } => true,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Limit { input, .. } => plan_contains_sort(input),
            LogicalPlan::RowidLookup { input, .. } => plan_contains_sort(input),
            _ => false,
        }
    }

    #[test]
    fn test_sort_elided_for_index_scan() {
        let btree = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY age");
        let plan = plan(stmt, &btree).expect("Planning failed");
        assert!(
            !plan_contains_sort(&plan),
            "expected sort to be elided, got:\n{:#?}",
            plan
        );
    }

    #[test]
    fn test_sort_not_elided_for_desc() {
        let btree = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY age DESC");
        let plan = plan(stmt, &btree).expect("Planning failed");
        assert!(plan_contains_sort(&plan), "DESC should not be elided");
    }

    #[test]
    fn test_sort_not_elided_for_different_column() {
        let btree = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY id");
        let plan = plan(stmt, &btree).expect("Planning failed");
        assert!(
            plan_contains_sort(&plan),
            "non-indexed column should not be elided"
        );
    }
}
