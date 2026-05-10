//! SELECT planning: unified plan_select, context builders, and shared helpers.

use std::collections::{HashMap, HashSet};

use crate::frontend::ast;
use crate::storage::BTree;

use schema::resolve_table;

use super::resolver::{
    build_column_mapping, col_expr_uses_rowid, collect_columns, collect_columns_from_column_expr,
    convert_aggregate, convert_column_expr, convert_expr, convert_having_expr, expr_uses_rowid,
    extract_limit_value, extract_table_info, has_aggregate, is_aggregate_function,
    remap_column_indices, ColumnResolver, JoinResolver, SingleTableResolver,
};
use super::{schema, to_scan_index, AggregateExpr, LogicalPlan, PlanError, PlanExpr, SortKey};

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

/// Unified resolver that works for both single-table and join queries.
enum SelectResolver<'a> {
    Single(SingleTableResolver<'a>),
    Join(JoinResolver<'a>),
    Materialize(MaterializeResolver<'a>),
}

impl ColumnResolver for SelectResolver<'_> {
    fn resolve_identifier(&self, name: &str) -> Result<usize, PlanError> {
        match self {
            Self::Single(r) => r.resolve_identifier(name),
            Self::Join(r) => r.resolve_identifier(name),
            Self::Materialize(r) => r.resolve_identifier(name),
        }
    }

    fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize, PlanError> {
        match self {
            Self::Single(r) => r.resolve_qualified(table, column),
            Self::Join(r) => r.resolve_qualified(table, column),
            Self::Materialize(r) => r.resolve_qualified(table, column),
        }
    }

    fn resolve_rowid(&self) -> Option<usize> {
        match self {
            Self::Single(r) => r.resolve_rowid(),
            Self::Join(_) => None,
            Self::Materialize(_) => None,
        }
    }
}

/// Resolver for queries where the FROM clause is a materialized subquery.
struct MaterializeResolver<'a> {
    alias: &'a str,
    columns: &'a [String],
}

impl ColumnResolver for MaterializeResolver<'_> {
    fn resolve_identifier(&self, name: &str) -> Result<usize, PlanError> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
            .ok_or_else(|| PlanError::ColumnNotFound {
                table: self.alias.to_string(),
                column: name.to_string(),
            })
    }

    fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize, PlanError> {
        if !table.eq_ignore_ascii_case(self.alias) {
            return Err(PlanError::TableNotFound(table.to_string()));
        }
        self.resolve_identifier(column)
    }
}

pub(crate) fn plan_select(
    select: ast::SelectStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    if is_from_subquery(&select.from) {
        plan_select_from_subquery(select, catalog)
    } else if select.joins.is_empty() {
        plan_select_single(select, catalog)
    } else {
        plan_select_joined(select, catalog)
    }
}

fn is_from_subquery(from: &ast::NamedTupleSource) -> bool {
    match from {
        ast::NamedTupleSource::Named { source, .. }
        | ast::NamedTupleSource::Anonyomous(source) => {
            matches!(source, ast::TupleSource::Subquery(_))
        }
    }
}

/// Extract the alias and subquery from a FROM subquery source.
fn from_subquery_parts(
    from: ast::NamedTupleSource,
) -> (String, ast::SelectStatement) {
    match from {
        ast::NamedTupleSource::Named { alias, source } => {
            let ast::TupleSource::Subquery(inner) = source else { unreachable!() };
            (alias, *inner)
        }
        ast::NamedTupleSource::Anonyomous(source) => {
            let ast::TupleSource::Subquery(inner) = source else { unreachable!() };
            ("subquery".to_string(), *inner)
        }
    }
}

/// Derive the output column names from a SELECT statement's column list.
fn select_output_names(select: &ast::SelectStatement) -> Vec<String> {
    select
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| match col {
            ast::ColumnExpression::Named { name, .. } => name.clone(),
            ast::ColumnExpression::Anonyomous(expr) => match expr.as_ref() {
                ast::Expression::Value(ast::ScalarValue::Identifier(name)) => name.clone(),
                ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(_, name)) => {
                    name.clone()
                }
                _ => format!("col{i}"),
            },
            ast::ColumnExpression::Wildcard => format!("col{i}"),
        })
        .collect()
}

/// Plan a SELECT where the FROM clause is a subquery: `SELECT ... FROM (SELECT ...) AS alias`.
fn plan_select_from_subquery(
    select: ast::SelectStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    if !select.joins.is_empty() {
        return Err(PlanError::UnsupportedStatement);
    }

    // Destructure select so we can move `from` while borrowing the rest.
    let ast::SelectStatement {
        from,
        columns: col_exprs,
        filter,
        order_by,
        limit,
        group_by: _,
        having: _,
        distinct: _,
        joins: _,
    } = select;

    let (alias, inner_stmt) = from_subquery_parts(from);

    // Derive column names from the inner SELECT before planning it.
    let col_names = select_output_names(&inner_stmt);
    let col_count = col_names.len();

    // Plan the inner query recursively.
    let inner_plan = plan_select(inner_stmt, catalog)?;
    let mat_plan = LogicalPlan::Materialize {
        input: Box::new(inner_plan),
    };

    // Build resolver for the outer query's expressions.
    let resolver = SelectResolver::Materialize(MaterializeResolver {
        alias: &alias,
        columns: &col_names,
    });

    // Outer query: wrap Materialize in Filter if needed.
    let base_plan = apply_filter(mat_plan, filter.as_ref(), &resolver)?;

    // Build a minimal SelectStatement view for apply_project.
    let fake_select = ast::SelectStatement {
        distinct: false,
        columns: col_exprs,
        from: ast::NamedTupleSource::Anonyomous(ast::TupleSource::Table("_".into())),
        joins: vec![],
        filter: None,
        limit: None,
        order_by: None,
        group_by: None,
        having: None,
    };
    let mut plan = apply_project(base_plan, &fake_select, col_count, &resolver)?;

    if let Some(ref order_by_clauses) = order_by {
        let sort_keys: Vec<super::SortKey> = order_by_clauses
            .iter()
            .map(|clause| {
                convert_expr(&clause.expression, &resolver).map(|expr| super::SortKey {
                    expr,
                    descending: clause.direction == ast::OrderDirection::Desc,
                })
            })
            .collect::<Result<_, _>>()?;
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            sort_keys,
        };
    }

    if let Some(ref limit_expr) = limit {
        let count = extract_limit_value(limit_expr)?;
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count,
        };
    }

    Ok(plan)
}

/// Plan a SELECT with no joins (single-table path).
fn plan_select_single(
    select: ast::SelectStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    // 1. Extract table info from FROM clause
    let (table_name, table_ref) = extract_table_info(&select.from)?;

    // 2. Look up table in catalog
    let table = resolve_table(&table_name, catalog)?;

    // 3. Collect all column references from SELECT, WHERE, GROUP BY, and ORDER BY
    let mut columns_needed = HashSet::new();
    let has_wildcard = select
        .columns
        .iter()
        .any(|col| matches!(col, ast::ColumnExpression::Wildcard));

    if has_wildcard {
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

    // 4. Build column mapping and resolver
    let mapping = build_column_mapping(&columns_needed, &table, &table_ref)?;

    // 5. Build base plan: Scan → optional Filter
    let rowid_col = table.rowid_column();
    let mut scan_cols: Vec<usize> = mapping
        .scan_columns
        .iter()
        .map(|&i| to_scan_index(i, rowid_col))
        .collect();

    // Detect rowid() usage in SELECT, WHERE, and ORDER BY; ensure scan index 0 is included.
    let needs_rowid = select.columns.iter().any(col_expr_uses_rowid)
        || select.filter.as_ref().map_or(false, |f| expr_uses_rowid(f))
        || select.order_by.as_ref().map_or(false, |ob| {
            ob.iter().any(|c| expr_uses_rowid(&c.expression))
        });
    let rowid_output_pos = if needs_rowid {
        if let Some(pos) = scan_cols.iter().position(|&c| c == 0) {
            Some(pos)
        } else {
            let pos = scan_cols.len();
            scan_cols.push(0);
            Some(pos)
        }
    } else {
        None
    };

    let resolver = SelectResolver::Single(SingleTableResolver {
        table_ref: &table_ref,
        columns: &mapping.column_map,
        rowid_output_pos,
    });

    let scan = LogicalPlan::Scan {
        rootpage: table.rootpage,
        columns: scan_cols,
    };
    let base_plan = apply_filter(scan, select.filter.as_ref(), &resolver)?;

    // 6. Build the rest of the plan using the shared body.
    // COUNT(*) and aggregation are only available on single-table queries.
    let wildcard_col_count = table.columns.len();
    plan_select_body(
        select,
        base_plan,
        resolver,
        wildcard_col_count,
        true, // supports_aggregation
    )
}

/// Plan a SELECT with one or more JOINs.
fn plan_select_joined(
    select: ast::SelectStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    // Support exactly one join for now
    if select.joins.len() != 1 {
        return Err(PlanError::UnsupportedStatement);
    }

    // 1. Resolve left table (FROM clause)
    let (left_name, left_ref) = extract_table_info(&select.from)?;
    let left_table = resolve_table(&left_name, catalog)?;
    let left_col_count = left_table.columns.len();

    // 2. Resolve right table (first join clause)
    let join_clause = &select.joins[0];
    let (right_name, right_ref) = extract_table_info(&join_clause.table)?;
    let right_table = resolve_table(&right_name, catalog)?;
    let right_col_count = right_table.columns.len();

    // 3. Build join column resolution maps
    let mut qualified = HashMap::new();
    let mut unqualified = HashMap::new();

    for (idx, col) in left_table.columns.iter().enumerate() {
        qualified.insert((left_ref.clone(), col.name.clone()), idx);
        unqualified
            .entry(col.name.clone())
            .and_modify(|e| *e = None)
            .or_insert(Some(idx));
    }
    for (idx, col) in right_table.columns.iter().enumerate() {
        let combined_idx = left_col_count + idx;
        qualified.insert((right_ref.clone(), col.name.clone()), combined_idx);
        unqualified
            .entry(col.name.clone())
            .and_modify(|e| *e = None)
            .or_insert(Some(combined_idx));
    }

    let resolver = SelectResolver::Join(JoinResolver {
        qualified: &qualified,
        unqualified: &unqualified,
    });

    // 4. Build scan plans (read ALL columns from each table)
    let left_rowid_col = left_table.rowid_column();
    let right_rowid_col = right_table.rowid_column();
    let left_scan = LogicalPlan::Scan {
        rootpage: left_table.rootpage,
        columns: (0..left_col_count)
            .map(|i| to_scan_index(i, left_rowid_col))
            .collect(),
    };
    let right_scan = LogicalPlan::Scan {
        rootpage: right_table.rootpage,
        columns: (0..right_col_count)
            .map(|i| to_scan_index(i, right_rowid_col))
            .collect(),
    };

    // 5. Build Join plan, then optional WHERE filter
    let on_condition = convert_expr(&join_clause.on_condition, &resolver)?;
    let join_plan = LogicalPlan::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        on_condition,
        strategy: crate::planner::JoinStrategy::NestedLoop,
        left_column_count: left_col_count,
    };
    let base_plan = apply_filter(join_plan, select.filter.as_ref(), &resolver)?;

    // 6. Build the rest of the plan using the shared body.
    let wildcard_col_count = left_col_count + right_col_count;
    plan_select_body(
        select,
        base_plan,
        resolver,
        wildcard_col_count,
        false, // joins don't support aggregation yet
    )
}

/// Shared body: builds Project → Sort? → Distinct? → Limit? on top of `base_plan`.
///
/// `wildcard_col_count` is the total number of columns produced by `base_plan`, used to
/// expand `SELECT *`. `supports_aggregation` gates COUNT(*) and GROUP BY support.
fn plan_select_body(
    select: ast::SelectStatement,
    base_plan: LogicalPlan,
    resolver: SelectResolver<'_>,
    wildcard_col_count: usize,
    supports_aggregation: bool,
) -> Result<LogicalPlan, PlanError> {
    let is_distinct = select.distinct;
    let has_group_by = select.group_by.is_some();
    let has_aggregates = select.columns.iter().any(|col| has_aggregate(col));
    let use_aggregation = has_group_by || has_aggregates;

    // COUNT(*) fast path (single-table only)
    let is_count_star = supports_aggregation
        && !has_group_by
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

    // Validate HAVING
    if select.having.is_some() && !use_aggregation && !is_count_star {
        return Err(PlanError::InvalidHaving(
            "HAVING requires GROUP BY or aggregate functions".into(),
        ));
    }

    let mut plan = base_plan;

    if is_count_star {
        plan = LogicalPlan::Count {
            input: Box::new(plan),
        };
    } else if use_aggregation {
        plan = apply_aggregate(plan, &select, &resolver)?;
    } else {
        plan = apply_project(plan, &select, wildcard_col_count, &resolver)?;
    }

    // DISTINCT
    if is_distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    // LIMIT
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
// Shared plan-building helpers
// ============================================================================

/// Optionally wrap `plan` with a Filter node if `filter` is Some.
fn apply_filter(
    plan: LogicalPlan,
    filter: Option<&ast::Expression>,
    resolver: &impl ColumnResolver,
) -> Result<LogicalPlan, PlanError> {
    if let Some(expr) = filter {
        Ok(LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: convert_expr(expr, resolver)?,
        })
    } else {
        Ok(plan)
    }
}

/// Build the Aggregate + Project plan for GROUP BY / aggregate-function queries.
fn apply_aggregate(
    plan: LogicalPlan,
    select: &ast::SelectStatement,
    resolver: &impl ColumnResolver,
) -> Result<LogicalPlan, PlanError> {
    let group_keys: Vec<PlanExpr> = if let Some(ref group_by) = select.group_by {
        group_by
            .iter()
            .map(|expr| convert_expr(expr, resolver))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };

    let mut aggregates: Vec<AggregateExpr> = Vec::new();
    let mut projection_indices: Vec<usize> = Vec::new();

    for col_expr in &select.columns {
        let expr = match col_expr {
            ast::ColumnExpression::Named { expression, .. } => expression.as_ref(),
            ast::ColumnExpression::Anonyomous(expression) => expression.as_ref(),
            ast::ColumnExpression::Wildcard => continue,
        };

        if is_aggregate_function(expr) {
            let agg_index_in_output = group_keys.len() + aggregates.len();
            projection_indices.push(agg_index_in_output);
            aggregates.push(convert_aggregate(expr, resolver)?);
        } else {
            let group_expr = convert_expr(expr, resolver)?;
            let group_key_index = group_keys
                .iter()
                .position(|gk| gk == &group_expr)
                .ok_or(PlanError::UnsupportedStatement)?;
            projection_indices.push(group_key_index);
        }
    }

    let having = if let Some(ref having_expr) = select.having {
        Some(convert_having_expr(
            having_expr,
            &group_keys,
            &mut aggregates,
            resolver,
        )?)
    } else {
        None
    };

    let agg_plan = LogicalPlan::Aggregate {
        input: Box::new(plan),
        group_keys,
        aggregates,
        having,
    };

    let project_exprs: Vec<PlanExpr> = projection_indices
        .into_iter()
        .map(PlanExpr::ColumnRef)
        .collect();
    Ok(LogicalPlan::Project {
        input: Box::new(agg_plan),
        columns: project_exprs,
    })
}

/// Build the Project (and optional Sort) plan for regular non-aggregate SELECT.
///
/// `wildcard_col_count` is the number of columns produced by the base plan, used to expand `*`.
fn apply_project(
    plan: LogicalPlan,
    select: &ast::SelectStatement,
    wildcard_col_count: usize,
    resolver: &impl ColumnResolver,
) -> Result<LogicalPlan, PlanError> {
    let project_exprs: Vec<PlanExpr> = select
        .columns
        .iter()
        .flat_map(|col_expr| match col_expr {
            ast::ColumnExpression::Wildcard => (0..wildcard_col_count)
                .map(|idx| Ok(PlanExpr::ColumnRef(idx)))
                .collect::<Vec<_>>(),
            _ => vec![convert_column_expr(col_expr, resolver)],
        })
        .collect::<Result<Vec<_>, _>>()?;

    let select_column_count = project_exprs.len();

    if let Some(ref order_by) = select.order_by {
        apply_order_by(plan, order_by, project_exprs, select_column_count, resolver)
    } else {
        Ok(LogicalPlan::Project {
            input: Box::new(plan),
            columns: project_exprs,
        })
    }
}

// ============================================================================
// Shared ORDER BY helper
// ============================================================================

/// Wrap `plan` with Project → Sort → optional final Project.
///
/// `project_exprs` is the initial set of SELECT column expressions (no extra ORDER BY columns).
/// `select_col_count` is the number of columns in the user-visible output.
/// `resolver` is used to convert ORDER BY AST expressions to PlanExprs.
///
/// If any ORDER BY expression is not already present in `project_exprs`, it is appended to the
/// extended projection so the Sort node can reference it; a final Project then strips the extras.
fn apply_order_by(
    plan: LogicalPlan,
    order_by: &[ast::OrderByClause],
    mut project_exprs: Vec<PlanExpr>,
    select_col_count: usize,
    resolver: &impl ColumnResolver,
) -> Result<LogicalPlan, PlanError> {
    // Append any ORDER BY expression that is not already covered by project_exprs.
    for clause in order_by {
        let order_expr = convert_expr(&clause.expression, resolver)?;
        if !project_exprs.contains(&order_expr) {
            project_exprs.push(order_expr);
        }
    }

    let has_extra = project_exprs.len() > select_col_count;

    // Build the (possibly extended) Project node.
    let mut plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: project_exprs.clone(),
    };

    // Build a map from source-column index to projection index so we can remap sort keys.
    let mut idx_to_proj: HashMap<usize, usize> = HashMap::new();
    for (proj_idx, expr) in project_exprs.iter().enumerate() {
        if let PlanExpr::ColumnRef(col_idx) = expr {
            idx_to_proj.insert(*col_idx, proj_idx);
        }
    }

    // Convert ORDER BY expressions and remap column indices into projection space.
    let sort_keys: Result<Vec<SortKey>, _> = order_by
        .iter()
        .map(|clause| {
            let source_expr = convert_expr(&clause.expression, resolver)?;
            let proj_expr = remap_column_indices(&source_expr, &idx_to_proj)?;
            Ok(SortKey {
                expr: proj_expr,
                descending: clause.direction == ast::OrderDirection::Desc,
            })
        })
        .collect();

    // Always emit Sort; the optimizer removes it when an IndexScan already provides the order.
    plan = LogicalPlan::Sort {
        input: Box::new(plan),
        sort_keys: sort_keys?,
    };

    // If extra ORDER BY columns were added, strip them from the output.
    if has_extra {
        let final_cols: Vec<PlanExpr> = (0..select_col_count).map(PlanExpr::ColumnRef).collect();
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns: final_cols,
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
        test.btree.insert_entry(
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
                columns: vec![1, 2], // id, name (scan-space: no rowid alias → +1)
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
                    columns: vec![2, 3], // name, age (scan-space)
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
                    columns: vec![2], // name (scan-space)
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
                columns: vec![1, 2, 3], // all columns (scan-space)
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
        test.btree.insert_entry(
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
                columns: vec![1, 2, 3, 4, 5], // all 5 columns (scan-space, no PK)
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
                columns: vec![1, 2, 3], // all columns (scan-space)
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
                columns: vec![1, 2, 3], // all columns (scan-space)
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
                columns: vec![1, 2, 3], // all columns (scan-space)
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
                columns: vec![1, 2], // id, name (scan-space)
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
        let mut test = TestDb::default();

        // Create departments table (id, name)
        let dept_root = test.btree.create_tree();
        test.btree.insert_entry(
            "table",
            "departments",
            "departments",
            dept_root,
            "CREATE TABLE departments (id INTEGER, name TEXT)",
        );

        // Create employees table (id, name, dept_id)
        let emp_root = test.btree.create_tree();
        test.btree.insert_entry(
            "table",
            "employees",
            "employees",
            emp_root,
            "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)",
        );

        // Plan: "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id"
        let stmt = parse_sql(
            "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id",
        );
        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

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
                ..
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
        let mut test = TestDb::default();

        // Create table and index
        let sql_table = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        let users_root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_age", "users", index_root, sql_index);

        // Query that should use index
        let stmt = parse_sql("SELECT name FROM users WHERE age = 30");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

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
                        ..
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
        let mut test = TestDb::default();

        let sql_table = "CREATE TABLE data (id INTEGER, value INTEGER)";
        let data_root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "data", "data", data_root, sql_table);

        let sql_index = "CREATE INDEX idx_value ON data(value)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_value", "data", index_root, sql_index);

        // Test greater than
        let stmt = parse_sql("SELECT id FROM data WHERE value > 20");
        let p = plan(stmt, &test.btree).expect("Planning failed");
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
                        ..
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
        let p = plan(stmt, &test.btree).expect("Planning failed");
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
        let mut test = TestDb::default();

        let sql_table = "CREATE TABLE events (id INTEGER, year INTEGER, month INTEGER)";
        let root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "events", "events", root, sql_table);

        // Multi-column index on (year, month)
        let sql_index = "CREATE INDEX idx_year_month ON events(year, month)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_year_month", "events", index_root, sql_index);

        // Query on first column should use the index
        let stmt = parse_sql("SELECT id FROM events WHERE year = 2024");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup { input, .. } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                        ..
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
        let mut test = TestDb::default();

        let sql_table = "CREATE TABLE events (id INTEGER, year INTEGER, month INTEGER)";
        let root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "events", "events", root, sql_table);

        // Multi-column index on (year, month) - only second column referenced
        let sql_index = "CREATE INDEX idx_year_month ON events(year, month)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_year_month", "events", index_root, sql_index);

        // Query on second column should NOT use the index (falls back to table scan)
        let stmt = parse_sql("SELECT id FROM events WHERE month = 6");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

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
        let mut test = TestDb::default();

        let sql_table = "CREATE TABLE colors (id INTEGER, category TEXT)";
        let colors_root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "colors", "colors", colors_root, sql_table);

        let stmt = parse_sql("SELECT DISTINCT category FROM colors");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

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

    fn make_btree_with_index_on_age() -> (TestDb,) {
        let mut test = TestDb::default();
        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "users", "users", users_root, sql_table);
        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_age", "users", index_root, sql_index);
        (test,)
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
        let (test,) = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY age");
        let plan = plan(stmt, &test.btree).expect("Planning failed");
        assert!(
            !plan_contains_sort(&plan),
            "expected sort to be elided, got:\n{:#?}",
            plan
        );
    }

    #[test]
    fn test_sort_not_elided_for_desc() {
        let (test,) = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY age DESC");
        let plan = plan(stmt, &test.btree).expect("Planning failed");
        assert!(plan_contains_sort(&plan), "DESC should not be elided");
    }

    #[test]
    fn test_sort_not_elided_for_different_column() {
        let (test,) = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY id");
        let plan = plan(stmt, &test.btree).expect("Planning failed");
        assert!(
            plan_contains_sort(&plan),
            "non-indexed column should not be elided"
        );
    }

    // ========================================================================
    // Join tests: LIMIT, DISTINCT, ORDER BY (previously unsupported via the
    // now-deleted plan_select_with_joins path)
    // ========================================================================

    fn make_join_db() -> (TestDb, u32, u32) {
        let mut test = TestDb::default();
        let orders_root = test.btree.create_tree();
        test.btree.insert_entry(
            "table",
            "orders",
            "orders",
            orders_root,
            "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)",
        );
        let users_root = test.btree.create_tree();
        test.btree.insert_entry(
            "table",
            "users",
            "users",
            users_root,
            "CREATE TABLE users (id INTEGER, name TEXT)",
        );
        (test, orders_root, users_root)
    }

    #[test]
    fn join_with_limit() {
        let (test, orders_root, users_root) = make_join_db();
        let stmt = parse_sql(
            "SELECT orders.id FROM orders JOIN users ON orders.user_id = users.id LIMIT 5",
        );
        let result = plan(stmt, &test.btree).expect("Planning failed");

        // Top-level node must be Limit
        match result {
            LogicalPlan::Limit { count, input } => {
                assert_eq!(count, 5);
                // Input must be Project over Join
                match *input {
                    LogicalPlan::Project { input, .. } => {
                        match *input {
                            LogicalPlan::Join {
                                left_column_count, ..
                            } => {
                                // orders has 3 columns, users has 2 — left_column_count = 3
                                assert_eq!(left_column_count, 3);
                                let _ = (orders_root, users_root); // suppress unused warnings
                            }
                            other => panic!("Expected Join, got {other:?}"),
                        }
                    }
                    other => panic!("Expected Project, got {other:?}"),
                }
            }
            other => panic!("Expected Limit, got {other:?}"),
        }
    }

    #[test]
    fn join_with_distinct() {
        let (test, _orders_root, _users_root) = make_join_db();
        let stmt = parse_sql(
            "SELECT DISTINCT orders.user_id FROM orders JOIN users ON orders.user_id = users.id",
        );
        let result = plan(stmt, &test.btree).expect("Planning failed");

        // Top-level node must be Distinct
        assert!(
            matches!(result, LogicalPlan::Distinct { .. }),
            "Expected Distinct, got {result:?}"
        );
    }

    #[test]
    fn join_with_order_by() {
        let (test, _orders_root, _users_root) = make_join_db();
        let stmt = parse_sql(
            "SELECT orders.id, users.name FROM orders JOIN users ON orders.user_id = users.id ORDER BY orders.id",
        );
        let result = plan(stmt, &test.btree).expect("Planning failed");

        // Top-level node must be Sort
        assert!(
            matches!(result, LogicalPlan::Sort { .. }),
            "Expected Sort, got {result:?}"
        );
    }

    /// rowid() on a regular table (no PK): adds scan index 0 to scan_cols.
    #[test]
    fn test_rowid_function_no_pk_table() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT name, rowid() FROM users");
        let result = plan(stmt, &test.btree).expect("Planning failed");

        // scan_cols: name=schema 1→scan 2, rowid()→scan 0 (appended)
        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![2, 0], // name at scan 2, rowid at scan 0
            }),
            columns: vec![
                PlanExpr::ColumnRef(0), // name (output pos 0)
                PlanExpr::ColumnRef(1), // rowid() (output pos 1)
            ],
        };
        assert_eq!(result, expected);
    }

    /// rowid() on a rowid-alias table: PK column and rowid() share scan index 0.
    #[test]
    fn test_rowid_function_pk_table() {
        let mut test = TestDb::default();
        let root = test.btree.create_tree();
        test.btree.insert_entry(
            "table",
            "t",
            "t",
            root,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        );
        let stmt = parse_sql("SELECT id, rowid() FROM t");
        let result = plan(stmt, &test.btree).expect("Planning failed");

        // id is rowid alias → scan 0; rowid() also maps to scan 0 (same slot, deduped)
        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: root,
                columns: vec![0], // id = rowid alias → scan index 0 only
            }),
            columns: vec![
                PlanExpr::ColumnRef(0), // id
                PlanExpr::ColumnRef(0), // rowid() — same position as id
            ],
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn plan_from_subquery_produces_materialize_node() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT name FROM (SELECT id, name FROM users) AS u");
        let result = plan(stmt, &test.btree).expect("Planning failed");

        // Expected: Project(Materialize(...))
        let LogicalPlan::Project { input, .. } = result else {
            panic!("Expected Project at top level");
        };
        assert!(
            matches!(input.as_ref(), LogicalPlan::Materialize { .. }),
            "Expected Materialize as Project input, got: {input:?}"
        );
    }

    /// rowid() only (no other columns): scan just reads the key.
    #[test]
    fn test_rowid_function_only() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT rowid() FROM users");
        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0], // only the B-tree key
            }),
            columns: vec![PlanExpr::ColumnRef(0)],
        };
        assert_eq!(result, expected);
    }
}
