//! Query Planner - Logical Operator Tree (Option A)
//!
//! Converts AST to a tree of logical operators (LogicalPlan).
//! The compiler (future) will convert LogicalPlan to bytecode.

use crate::frontend::ast::{self, Statement};
use crate::storage::BTree;
use schema::resolve_table;

pub(crate) mod resolver;
use resolver::{
    ast_expr_name, build_column_mapping, collect_columns, collect_columns_from_column_expr,
    convert_aggregate, convert_column_expr, convert_expr, convert_having_expr, eval_constant,
    extract_limit_value, extract_table_info, has_aggregate, is_aggregate_function,
    remap_column_indices, JoinResolver, NoColumnResolver, SingleTableResolver,
};

// ============================================================================
// Operators
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Plus,
    Negate,
    #[allow(dead_code)]
    Not,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,

    // Comparison
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,

    // Logical
    And,
    Or,

    // Bitwise
    LeftShift,
    RightShift,
    BitOr,
    BitXor,
    BitAnd,
}

// ============================================================================
// Plan Types
// ============================================================================

/// Literal values in expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    #[allow(dead_code)]
    Bool(bool),
    #[allow(dead_code)]
    Null,
}

/// Sort key specification for Sort node
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: PlanExpr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexMaintenanceInfo {
    pub rootpage: u32,
    pub column_idxs: Vec<usize>,
}

/// Aggregate function types
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count, // COUNT(*) or COUNT(expr)
    Sum,   // SUM(expr)
    Avg,   // AVG(expr)
    Min,   // MIN(expr)
    Max,   // MAX(expr)
}

/// Aggregate expression specification
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub argument: Option<PlanExpr>, // None for COUNT(*)
}

/// Planner's expression type - like ast::Expression but with resolved columns
#[derive(Debug, Clone, PartialEq)]
pub enum PlanExpr {
    /// Reference to a column by index in the input node's output
    ColumnRef(usize),
    Literal(Literal),
    BinaryOp {
        op: BinaryOp,
        left: Box<PlanExpr>,
        right: Box<PlanExpr>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<PlanExpr>,
    },
    FunctionCall {
        name: String,
        args: Vec<PlanExpr>,
    },
}

/// Logical plan nodes - relational algebra operators
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Scan rows from a table (leaf node, no inputs)
    /// rootpage: the B-tree root page number for this table
    /// columns: indices of columns to read from the table schema
    Scan {
        rootpage: u32,
        columns: Vec<usize>,
        with_key: bool,
    },

    /// Scan via an index
    /// Scan via an index — handles both equality and range predicates.
    /// Equality (col = X): lower_bound = Some((X, true)), upper_bound = Some((X, true))
    /// Range (col > X): lower_bound = Some((X, false)), upper_bound = None
    /// Scan an index B-tree and yield one rowid per matching entry.
    /// Knows nothing about the table; a RowidLookup node above fetches columns.
    IndexScan {
        index_rootpage: u32,
        index_col_idx: usize, // table column index of the indexed column
        lower_bound: Option<(Literal, bool)>, // (value, inclusive)
        upper_bound: Option<(Literal, bool)>, // (value, inclusive)
    },

    /// For each rowid produced by its input, fetch the requested columns from
    /// the table B-tree and yield a full row.
    RowidLookup {
        input: Box<LogicalPlan>,
        table_rootpage: u32,
        columns: Vec<usize>,
    },

    /// Filter rows based on a predicate (1 input)
    /// Pass-through: outputs all columns from its child unchanged.
    /// Only rows where predicate evaluates to true are emitted.
    Filter {
        input: Box<LogicalPlan>,
        predicate: PlanExpr,
    },

    /// Project specific columns/expressions (1 input)
    /// Transforms output: produces exactly the columns specified.
    /// ColumnRefs in expressions refer to positions in the child's output.
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<PlanExpr>,
    },

    /// Limit output rows (1 input)
    /// Pass-through: outputs all columns from its child unchanged.
    /// Only emits up to `count` rows.
    Limit { input: Box<LogicalPlan>, count: u64 },

    /// Sort rows based on sort keys (1 input)
    /// Pass-through: outputs all columns from its child unchanged.
    /// Materializes all rows, sorts them, then yields in sorted order.
    Sort {
        input: Box<LogicalPlan>,
        sort_keys: Vec<SortKey>,
    },

    /// Count rows from input (1 input)
    /// Consumes all rows from child and outputs a single row with the count.
    /// Output: single integer column containing the row count.
    Count { input: Box<LogicalPlan> },

    /// Aggregate rows with grouping (1 input)
    /// Groups rows by group_keys, computes aggregates for each group.
    /// Output: group_keys + aggregate results (one column per aggregate)
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<PlanExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<PlanExpr>,
    },

    /// Emit fixed rows (leaf node, no inputs)
    /// Useful for testing and for VALUES clauses.
    /// Each inner Vec is a row; all rows must have the same number of columns.
    Values { rows: Vec<Vec<Literal>> },

    /// Generate a sequence of integers (leaf node, no inputs)
    /// Useful for testing. Generates rows [start], [start+1], ..., [end-1]
    /// Output: single integer column
    #[allow(dead_code)]
    Sequence { start: i64, end: i64 },

    /// Insert rows into a table (1 input, typically Values)
    /// Consumes all rows from input, writes each to the table's B-tree.
    /// Output: single integer column containing the count of rows inserted.
    Insert {
        rootpage: u32,
        table_columns: Vec<usize>,
        input: Box<LogicalPlan>,
        indexes: Vec<IndexMaintenanceInfo>,
    },

    /// Update rows in a table
    /// Scans table, applies filter, updates matching rows.
    /// Output: single integer column containing the count of rows updated.
    Update {
        rootpage: u32,
        table_columns: Vec<usize>,
        assignments: Vec<(usize, PlanExpr)>, // (column_index, new_value_expr)
        filter: Option<PlanExpr>,
        indexes: Vec<IndexMaintenanceInfo>,
    },

    /// Delete rows from a table
    /// Scans table, applies filter, deletes matching rows by key.
    /// Output: single integer column containing the count of rows deleted.
    Delete {
        rootpage: u32,
        table_columns: Vec<usize>,
        filter: Option<PlanExpr>,
        indexes: Vec<IndexMaintenanceInfo>,
    },

    /// Join two tables (2 inputs)
    /// Performs nested loop join: for each left row, iterate all right rows,
    /// emit combined rows where on_condition is true.
    /// Output: left columns followed by right columns (left_column_count + right_column_count columns)
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on_condition: PlanExpr,
        left_column_count: usize, // for register offset calculation
    },

    /// Deduplicate rows from input (1 input)
    /// Materializes all rows, removes duplicates, yields unique rows.
    /// Output: same columns as input.
    Distinct { input: Box<LogicalPlan> },

    /// Populate an index B-tree from an existing table.
    /// Scans all rows in the table, encoding each row's indexed columns and
    /// primary key as a composite index key, then writes to the index B-tree.
    /// Output: no rows (yields immediately to on_done).
    PopulateIndex {
        input: Box<LogicalPlan>,
        index_rootpage: u32,
        column_idxs: Vec<usize>,
    },
}
// ============================================================================
// Schema (for column resolution)
// ============================================================================

pub mod schema;

// ============================================================================
// Planning
// ============================================================================

/// Extract output column names from a SELECT statement's column list.
///
/// Returns the names in SELECT column order. Wildcards are expanded using the
/// btree catalog to look up the table's column names.
pub fn extract_select_column_names(select: &ast::SelectStatement, btree: &BTree) -> Vec<String> {
    let mut names = Vec::new();
    for col_expr in &select.columns {
        match col_expr {
            ast::ColumnExpression::Named { name, .. } => {
                names.push(name.clone());
            }
            ast::ColumnExpression::Anonyomous(expr) => {
                names.push(ast_expr_name(expr));
            }
            ast::ColumnExpression::Wildcard => {
                // Expand wildcard using catalog
                let table_name = match &select.from {
                    ast::NamedTupleSource::Named { source, .. } => {
                        if let ast::TupleSource::Table(name) = source {
                            name.clone()
                        } else {
                            continue;
                        }
                    }
                    ast::NamedTupleSource::Anonyomous(source) => {
                        if let ast::TupleSource::Table(name) = source {
                            name.clone()
                        } else {
                            continue;
                        }
                    }
                };
                if let Ok(table) = resolve_table(&table_name, btree) {
                    for col in &table.columns {
                        names.push(col.name.clone());
                    }
                }
            }
        }
    }
    names
}

/// Convert an AST Statement to a LogicalPlan by querying the db_schema catalog.
pub fn plan(statement: Statement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    match statement {
        Statement::Select(select) => {
            if select.joins.is_empty() {
                plan_select(select, btree)
            } else {
                plan_select_with_joins(select, btree)
            }
        }
        Statement::CreateTable(_) | Statement::CreateIndex(_) | Statement::Drop(_) => {
            Err(PlanError::UnsupportedStatement)
        }
        Statement::Insert(insert) => plan_insert(insert, btree),
        Statement::Update(update) => plan_update(update, btree),
        Statement::Delete(delete) => plan_delete(delete, btree),
        Statement::Explain(inner) => plan(*inner, btree),
    }
}

fn plan_select(select: ast::SelectStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
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

fn plan_select_with_joins(
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

fn output_width(plan: &LogicalPlan) -> usize {
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

fn plan_insert(insert: ast::InsertStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&insert.table_name, btree)?;
    let num_table_columns = table.columns.len();

    // Determine which columns we're inserting into
    let table_columns: Vec<usize> = match &insert.columns {
        Some(col_names) => col_names
            .iter()
            .map(|name| {
                table
                    .get_column_index(name)
                    .ok_or_else(|| PlanError::ColumnNotFound {
                        table: insert.table_name.clone(),
                        column: name.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        None => (0..num_table_columns).collect(),
    };

    // Build the input plan from the INSERT source
    let input_plan = match insert.source {
        ast::InsertSource::Values(value_rows) => {
            let no_resolver = NoColumnResolver;
            let mut rows = Vec::new();
            for value_row in &value_rows {
                if value_row.len() != table_columns.len() {
                    return Err(PlanError::ColumnCountMismatch {
                        expected: table_columns.len(),
                        got: value_row.len(),
                    });
                }
                let literals: Vec<Literal> = value_row
                    .iter()
                    .map(|expr| {
                        let plan_expr = convert_expr(expr, &no_resolver)?;
                        eval_constant(&plan_expr)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                rows.push(literals);
            }
            LogicalPlan::Values { rows }
        }
        ast::InsertSource::Query(select) => {
            let input = plan_select(*select, btree)?;
            let produced = output_width(&input);
            if produced != table_columns.len() {
                return Err(PlanError::ColumnCountMismatch {
                    expected: table_columns.len(),
                    got: produced,
                });
            }
            input
        }
    };

    // Look up indexes for this table
    let index_infos = btree.lookup_indexes_for_table(&insert.table_name);
    let mut indexes = Vec::new();
    for index_info in index_infos {
        // Find column indexes
        let column_idxs = index_info
            .column_names
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .position(|col| &col.name == name)
                    .expect("Index column not found in table")
            })
            .collect();

        indexes.push(IndexMaintenanceInfo {
            rootpage: index_info.rootpage,
            column_idxs,
        });
    }

    Ok(LogicalPlan::Insert {
        rootpage: table.rootpage,
        table_columns,
        input: Box::new(input_plan),
        indexes,
    })
}

/// Try to use an index for the given filter (equality or range predicate).
fn try_plan_index_scan(
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
fn can_elide_sort(plan: &LogicalPlan, sort_col_idx: usize) -> bool {
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
fn extract_range_filter(
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

fn extract_single_range(
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

fn extract_is_null_filter(expr: &ast::Expression) -> Option<String> {
    match expr {
        ast::Expression::UnaryOp {
            op: ast::UnaryOp::IsNull,
            expression,
        } => extract_column_name(expression),
        _ => None,
    }
}

fn extract_equality_filter(expr: &ast::Expression) -> Option<(String, Literal)> {
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

fn extract_column_name(expr: &ast::Expression) -> Option<String> {
    match expr {
        ast::Expression::Value(ast::ScalarValue::Identifier(name)) => Some(name.clone()),
        _ => None,
    }
}

fn extract_literal(expr: &ast::Expression) -> Option<Literal> {
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

fn plan_update(update: ast::UpdateStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&update.table_name, btree)?;

    // Build column mapping: all table columns in order
    let column_map = table.column_name_map();

    // Create column resolver
    let resolver = SingleTableResolver {
        table_ref: &update.table_name,
        columns: &column_map,
    };

    // Resolve assignment column names to indices and plan their expressions
    let mut assignments = Vec::new();
    for (col_name, expr) in update.assignments {
        let col_idx = table
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| PlanError::ColumnNotFound {
                table: update.table_name.clone(),
                column: col_name.clone(),
            })?;

        // Plan the expression (column refs refer to table schema)
        let expr_plan = convert_expr(&expr, &resolver)?;
        assignments.push((col_idx, expr_plan));
    }

    // Plan the filter expression if present
    let filter = match update.filter {
        Some(expr) => Some(convert_expr(&expr, &resolver)?),
        None => None,
    };

    // Get all table column indices
    let table_columns: Vec<usize> = (0..table.columns.len()).collect();

    // Gather secondary index maintenance info (mirrors plan_delete)
    let index_infos = btree.lookup_indexes_for_table(&update.table_name);
    let mut indexes = Vec::new();
    for index_info in index_infos {
        let column_idxs = index_info
            .column_names
            .iter()
            .map(|name| table.columns.iter().position(|c| &c.name == name).unwrap())
            .collect();
        indexes.push(IndexMaintenanceInfo {
            rootpage: index_info.rootpage,
            column_idxs,
        });
    }

    Ok(LogicalPlan::Update {
        rootpage: table.rootpage,
        table_columns,
        assignments,
        filter,
        indexes,
    })
}

fn plan_delete(delete: ast::DeleteStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&delete.table_name, btree)?;

    // Build column mapping: all table columns in order
    let column_map = table.column_name_map();

    // Create column resolver
    let resolver = SingleTableResolver {
        table_ref: &delete.table_name,
        columns: &column_map,
    };

    // Plan the filter expression if present
    let filter = match delete.filter {
        Some(expr) => Some(convert_expr(&expr, &resolver)?),
        None => None,
    };

    // Get all table column indices
    let table_columns: Vec<usize> = (0..table.columns.len()).collect();

    // Gather secondary index maintenance info (mirrors plan_insert)
    let index_infos = btree.lookup_indexes_for_table(&delete.table_name);
    let mut indexes = Vec::new();
    for index_info in index_infos {
        let column_idxs = index_info
            .column_names
            .iter()
            .map(|name| table.columns.iter().position(|c| &c.name == name).unwrap())
            .collect();
        indexes.push(IndexMaintenanceInfo {
            rootpage: index_info.rootpage,
            column_idxs,
        });
    }

    Ok(LogicalPlan::Delete {
        rootpage: table.rootpage,
        table_columns,
        filter,
        indexes,
    })
}


#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    TableNotFound(String),
    ColumnNotFound {
        table: String,
        column: String,
    },
    AmbiguousColumn(String),
    ColumnCountMismatch {
        expected: usize,
        got: usize,
    },
    UnsupportedStatement,
    UnknownFunction(String),
    InvalidFunctionArguments {
        function: String,
        expected: usize,
        got: usize,
    },
    InvalidHaving(String),
}

use std::collections::HashMap;
use std::collections::HashSet;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::parse;
    use crate::test::TestDb;

    // ========================================================================
    // Expression Converter Tests
    // ========================================================================

    fn make_column_map() -> HashMap<String, usize> {
        // Simulates: Scan { columns: [0, 1, 2] } for users(id, name, age)
        // So id → 0, name → 1, age → 2 in scan output
        let mut map = HashMap::new();
        map.insert("id".to_string(), 0);
        map.insert("name".to_string(), 1);
        map.insert("age".to_string(), 2);
        map
    }

    #[test]
    fn test_convert_integer_literal() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::IntegerNumber(42));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Integer(42)));
    }

    #[test]
    fn test_convert_float_literal() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::FloatingNumber(3.14));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Float(3.14)));
    }

    #[test]
    fn test_convert_column_ref() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(2));
    }

    #[test]
    fn test_convert_qualified_column_ref() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // users.name
        let table_expr = Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
            "users".to_string(),
        )));
        let expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            table_expr,
            "name".to_string(),
        ));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(1));
    }

    #[test]
    fn test_convert_qualified_column_wrong_table() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // other.name - should fail because "other" != "users"
        let table_expr = Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
            "other".to_string(),
        )));
        let expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            table_expr,
            "name".to_string(),
        ));
        let result = convert_expr(&expr, &resolver);

        assert_eq!(result, Err(PlanError::TableNotFound("other".to_string())));
    }

    #[test]
    fn test_convert_column_not_found() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("nonexistent".to_string()));
        let result = convert_expr(&expr, &resolver);

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }

    #[test]
    fn test_convert_binary_comparison() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // age > 21
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(
            result,
            PlanExpr::BinaryOp {
                op: BinaryOp::GreaterThan,
                left: Box::new(PlanExpr::ColumnRef(2)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
            }
        );
    }

    #[test]
    fn test_convert_unary_negate() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // -age
        let expr = ast::Expression::UnaryOp {
            op: ast::UnaryOp::Negate,
            expression: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
        };
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(
            result,
            PlanExpr::UnaryOp {
                op: UnaryOp::Negate,
                operand: Box::new(PlanExpr::ColumnRef(2)),
            }
        );
    }

    #[test]
    fn test_convert_nested_expression() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // (age + 1) > 21
        let age_plus_one = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        };
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(age_plus_one),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let result = convert_expr(&expr, &resolver).unwrap();

        let expected = PlanExpr::BinaryOp {
            op: BinaryOp::GreaterThan,
            left: Box::new(PlanExpr::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(PlanExpr::ColumnRef(2)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(1))),
            }),
            right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
        };
        assert_eq!(result, expected);
    }

    // ========================================================================
    // Column Collection Tests
    // ========================================================================

    #[test]
    fn test_collect_simple_column() {
        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()));
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_collect_qualified_column() {
        // users.name
        let table_expr = Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
            "users".to_string(),
        )));
        let expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            table_expr,
            "name".to_string(),
        ));
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["name".to_string()]));
    }

    #[test]
    fn test_collect_literal_no_columns() {
        let expr = ast::Expression::Value(ast::ScalarValue::IntegerNumber(42));
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert!(columns.is_empty());
    }

    #[test]
    fn test_collect_binary_expr_columns() {
        // age > 21
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_collect_multiple_columns() {
        // name = age (contrived but tests collecting from both sides)
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Equals,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "name".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(
            columns,
            HashSet::from(["name".to_string(), "age".to_string()])
        );
    }

    #[test]
    fn test_collect_nested_columns() {
        // (age + 1) > id
        let age_plus_one = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        };
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(age_plus_one),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "id".to_string(),
            ))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(
            columns,
            HashSet::from(["age".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn test_collect_from_column_expr_named() {
        // SELECT age AS user_age
        let col_expr = ast::ColumnExpression::Named {
            name: "user_age".to_string(),
            expression: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
        };
        let mut columns = HashSet::new();
        collect_columns_from_column_expr(&col_expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_collect_from_column_expr_anonymous() {
        // SELECT age + 1
        let col_expr = ast::ColumnExpression::Anonyomous(Box::new(ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        }));
        let mut columns = HashSet::new();
        collect_columns_from_column_expr(&col_expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    // ========================================================================
    // Column Mapping Tests
    // ========================================================================

    fn make_test_table() -> schema::Table {
        schema::Table {
            name: "users".to_string(),
            rootpage: 5,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "name".to_string(),
                },
                schema::Column {
                    name: "age".to_string(),
                },
            ],
        }
    }

    #[test]
    fn test_build_column_mapping_simple() {
        let table = make_test_table();
        let columns = HashSet::from(["id".to_string(), "name".to_string()]);

        let mapping = build_column_mapping(&columns, &table, "users").unwrap();

        // Scan should read columns [0, 1] (id, name) in table order
        assert_eq!(mapping.scan_columns, vec![0, 1]);
        // id is at scan position 0, name is at scan position 1
        assert_eq!(mapping.column_map.get("id"), Some(&0));
        assert_eq!(mapping.column_map.get("name"), Some(&1));
    }

    #[test]
    fn test_build_column_mapping_reordered() {
        let table = make_test_table();
        // Request columns in different order than table schema
        let columns = HashSet::from(["age".to_string(), "id".to_string()]);

        let mapping = build_column_mapping(&columns, &table, "users").unwrap();

        // Scan should read columns [0, 2] (id, age) in table order
        assert_eq!(mapping.scan_columns, vec![0, 2]);
        // id is at scan position 0, age is at scan position 1
        assert_eq!(mapping.column_map.get("id"), Some(&0));
        assert_eq!(mapping.column_map.get("age"), Some(&1));
    }

    #[test]
    fn test_build_column_mapping_all_columns() {
        let table = make_test_table();
        let columns = HashSet::from(["id".to_string(), "name".to_string(), "age".to_string()]);

        let mapping = build_column_mapping(&columns, &table, "users").unwrap();

        assert_eq!(mapping.scan_columns, vec![0, 1, 2]);
        assert_eq!(mapping.column_map.get("id"), Some(&0));
        assert_eq!(mapping.column_map.get("name"), Some(&1));
        assert_eq!(mapping.column_map.get("age"), Some(&2));
    }

    #[test]
    fn test_build_column_mapping_column_not_found() {
        let table = make_test_table();
        let columns = HashSet::from(["nonexistent".to_string()]);

        let result = build_column_mapping(&columns, &table, "users");

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }

    // ========================================================================
    // Plan Tests
    // ========================================================================

    /// Create a test database with a "users" table (id, name, age) registered in the catalog.
    /// Returns (TestDb, users_rootpage) - TestDb must be kept alive for the BTree.
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

    fn parse_sql(sql: &str) -> Statement {
        parse(sql).expect("Failed to parse SQL")
    }

    /// Example 1: Simple SELECT
    /// SELECT id, name FROM users
    ///
    /// Expected LogicalPlan:
    /// Project { columns: [ColumnRef(0), ColumnRef(1)] }
    ///   └─ Scan { table: "users", columns: [0, 1] }
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
    ///
    /// Expected LogicalPlan:
    /// Project { columns: [ColumnRef(0)] }   // name (position 0 in scan output)
    ///   └─ Filter { predicate: ColumnRef(1) > 21 }   // age (position 1 in scan output)
    ///        └─ Scan { table: "users", columns: [1, 2] }   // name, age
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
    ///
    /// Expected LogicalPlan:
    /// Limit { count: 10 }
    ///   └─ Project { columns: [ColumnRef(0)] }
    ///        └─ Scan { table: "users", columns: [1] }
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
    /// Scan { columns: [0, 1, 2] } reads all columns
    /// Project outputs them in order
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
    // INSERT Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_insert_basic() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users VALUES (1, 'alice', 30)");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![0, 1, 2],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Integer(1),
                    Literal::String("alice".to_string()),
                    Literal::Integer(30),
                ]],
            }),
            indexes: vec![],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_with_columns() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users (age, name) VALUES (30, 'alice')");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![2, 1], // age=2, name=1
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Integer(30),
                    Literal::String("alice".to_string()),
                ]],
            }),
            indexes: vec![],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_column_count_mismatch() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("INSERT INTO users VALUES (1, 'alice')");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::ColumnCountMismatch {
                expected: 3,
                got: 2,
            })
        );
    }

    #[test]
    fn test_plan_insert_with_expressions() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users VALUES (1+1, 'alice', 10*3)");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![0, 1, 2],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Integer(2),
                    Literal::String("alice".to_string()),
                    Literal::Integer(30),
                ]],
            }),
            indexes: vec![],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_table_not_found() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("INSERT INTO nonexistent VALUES (1)");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::TableNotFound("nonexistent".to_string()))
        );
    }

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

    #[test]
    fn test_join_resolver() {
        // Build a JoinResolver with:
        //   left: columns [id, name, dept_id], alias "e"
        //   right: columns [id, name], alias "d"
        use super::schema;
        use std::collections::HashMap;

        let left_table = schema::Table {
            name: "employees".to_string(),
            rootpage: 1,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "name".to_string(),
                },
                schema::Column {
                    name: "dept_id".to_string(),
                },
            ],
        };

        let right_table = schema::Table {
            name: "departments".to_string(),
            rootpage: 2,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "name".to_string(),
                },
            ],
        };

        // Build join column resolution maps
        let mut qualified = HashMap::new();
        let mut unqualified = HashMap::new();
        let left_col_count = left_table.columns.len();

        for (idx, col) in left_table.columns.iter().enumerate() {
            qualified.insert(("e".to_string(), col.name.clone()), idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(idx));
        }

        for (idx, col) in right_table.columns.iter().enumerate() {
            let combined_idx = left_col_count + idx;
            qualified.insert(("d".to_string(), col.name.clone()), combined_idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(combined_idx));
        }

        // Test qualified resolution
        assert_eq!(
            qualified.get(&("e".to_string(), "name".to_string())),
            Some(&1)
        );
        assert_eq!(
            qualified.get(&("d".to_string(), "name".to_string())),
            Some(&4)
        );
        assert_eq!(
            qualified.get(&("e".to_string(), "dept_id".to_string())),
            Some(&2)
        );

        // Test unqualified unique column
        assert_eq!(unqualified.get("dept_id"), Some(&Some(2)));

        // Test unqualified ambiguous columns (appear in both tables)
        assert_eq!(unqualified.get("id"), Some(&None));
        assert_eq!(unqualified.get("name"), Some(&None));

        // Test missing column
        assert_eq!(
            qualified.get(&("e".to_string(), "nonexistent".to_string())),
            None
        );
    }

    #[test]
    fn test_convert_expr_with_join_resolver() {
        use super::{convert_expr, schema, JoinResolver, PlanExpr};
        use std::collections::HashMap;

        let left_table = schema::Table {
            name: "employees".to_string(),
            rootpage: 1,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "dept_id".to_string(),
                },
            ],
        };

        let right_table = schema::Table {
            name: "departments".to_string(),
            rootpage: 2,
            columns: vec![schema::Column {
                name: "id".to_string(),
            }],
        };

        // Build join column resolution maps
        let mut qualified = HashMap::new();
        let mut unqualified = HashMap::new();
        let left_col_count = left_table.columns.len();

        for (idx, col) in left_table.columns.iter().enumerate() {
            qualified.insert(("e".to_string(), col.name.clone()), idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(idx));
        }

        for (idx, col) in right_table.columns.iter().enumerate() {
            let combined_idx = left_col_count + idx;
            qualified.insert(("d".to_string(), col.name.clone()), combined_idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(combined_idx));
        }

        let resolver = JoinResolver {
            qualified: &qualified,
            unqualified: &unqualified,
        };

        // Test qualified column: e.dept_id → ColumnRef(1)
        let ast_expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "e".to_string(),
            ))),
            "dept_id".to_string(),
        ));
        let plan_expr = convert_expr(&ast_expr, &resolver).unwrap();
        assert_eq!(plan_expr, PlanExpr::ColumnRef(1));

        // Test qualified column: d.id → ColumnRef(2)
        let ast_expr2 = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "d".to_string(),
            ))),
            "id".to_string(),
        ));
        let plan_expr2 = convert_expr(&ast_expr2, &resolver).unwrap();
        assert_eq!(plan_expr2, PlanExpr::ColumnRef(2));

        // Test unqualified unique column: dept_id → ColumnRef(1)
        let ast_expr3 = ast::Expression::Value(ast::ScalarValue::Identifier("dept_id".to_string()));
        let plan_expr3 = convert_expr(&ast_expr3, &resolver).unwrap();
        assert_eq!(plan_expr3, PlanExpr::ColumnRef(1));

        // Test ambiguous column: id → Error
        let ast_expr4 = ast::Expression::Value(ast::ScalarValue::Identifier("id".to_string()));
        let result = convert_expr(&ast_expr4, &resolver);
        assert!(matches!(result, Err(PlanError::AmbiguousColumn(_))));

        // Test binary operation: e.dept_id = d.id
        let ast_expr5 = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Equals,
            lhs: Box::new(ast::Expression::Value(
                ast::ScalarValue::MultiPartIdentifier(
                    Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                        "e".to_string(),
                    ))),
                    "dept_id".to_string(),
                ),
            )),
            rhs: Box::new(ast::Expression::Value(
                ast::ScalarValue::MultiPartIdentifier(
                    Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                        "d".to_string(),
                    ))),
                    "id".to_string(),
                ),
            )),
        };
        let plan_expr5 = convert_expr(&ast_expr5, &resolver).unwrap();
        if let PlanExpr::BinaryOp { left, right, .. } = plan_expr5 {
            assert_eq!(*left, PlanExpr::ColumnRef(1));
            assert_eq!(*right, PlanExpr::ColumnRef(2));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_plan_join() {
        use super::{plan, LogicalPlan, PlanExpr};
        use crate::test::TestDb;

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

    #[test]
    fn test_plan_index_scan() {
        use super::{plan, Literal, LogicalPlan};
        use crate::test::TestDb;

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
        use super::{plan, Literal, LogicalPlan};
        use crate::test::TestDb;

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
        use super::{plan, Literal, LogicalPlan};
        use crate::test::TestDb;

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
        use super::{plan, LogicalPlan};
        use crate::test::TestDb;

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
        use super::{plan, LogicalPlan};
        use crate::test::TestDb;

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

    #[test]
    fn test_plan_delete_gathers_indexes() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);

        let stmt = parse_sql("DELETE FROM users WHERE id = 1");
        let plan = plan(stmt, &btree).expect("Planning failed");

        if let LogicalPlan::Delete { indexes, .. } = plan {
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].column_idxs, vec![1]); // age is column index 1
            assert_eq!(indexes[0].rootpage, index_root);
        } else {
            panic!("Expected Delete plan");
        }
    }

    #[test]
    fn test_plan_update_gathers_indexes() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);

        let stmt = parse_sql("UPDATE users SET age = 30 WHERE id = 1");
        let plan = plan(stmt, &btree).expect("Planning failed");

        if let LogicalPlan::Update { indexes, .. } = plan {
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].column_idxs, vec![1]); // age is column index 1
            assert_eq!(indexes[0].rootpage, index_root);
        } else {
            panic!("Expected Update plan");
        }
    }

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
