//! Query Planner - Logical Operator Tree (Option A)
//!
//! Converts AST to a tree of logical operators (LogicalPlan).
//! The compiler (future) will convert LogicalPlan to bytecode.

use crate::frontend::ast::{self, Statement};
use crate::frontend::parse;
use crate::storage::BTree;

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
    Scan { rootpage: u32, columns: Vec<usize> },

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
    },

    /// Update rows in a table
    /// Scans table, applies filter, updates matching rows.
    /// Output: single integer column containing the count of rows updated.
    Update {
        rootpage: u32,
        table_columns: Vec<usize>,
        assignments: Vec<(usize, PlanExpr)>, // (column_index, new_value_expr)
        filter: Option<PlanExpr>,
    },

    /// Delete rows from a table
    /// Scans table, applies filter, deletes matching rows by key.
    /// Output: single integer column containing the count of rows deleted.
    Delete {
        rootpage: u32,
        table_columns: Vec<usize>,
        filter: Option<PlanExpr>,
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
}

// ============================================================================
// Schema (for column resolution)
// ============================================================================

pub mod schema {
    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    pub struct Schema {
        pub tables: Vec<Table>,
    }

    #[derive(Debug, Clone)]
    pub struct Table {
        #[allow(dead_code)]
        pub name: String,
        pub rootpage: u32,
        pub columns: Vec<Column>,
    }

    #[derive(Debug, Clone)]
    pub struct Column {
        pub name: String,
        // Future: pub data_type: DataType,
    }

    impl Schema {
        #[allow(dead_code)]
        pub fn get_table(&self, name: &str) -> Option<&Table> {
            self.tables.iter().find(|t| t.name == name)
        }
    }

    impl Table {
        pub fn get_column_index(&self, name: &str) -> Option<usize> {
            self.columns.iter().position(|c| c.name == name)
        }
    }
}

// ============================================================================
// Planning
// ============================================================================

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
        Statement::CreateTable(_) => Err(PlanError::UnsupportedStatement),
        Statement::Insert(insert) => plan_insert(insert, btree),
        Statement::Update(update) => plan_update(update, btree),
        Statement::Delete(delete) => plan_delete(delete, btree),
        Statement::Drop(_) => Err(PlanError::UnsupportedStatement),
    }
}

/// Resolve a table name to a schema::Table by querying the db_schema catalog
/// and parsing the stored DDL.
fn resolve_table(table_name: &str, btree: &BTree) -> Result<schema::Table, PlanError> {
    let (rootpage, sql) = btree
        .lookup_table(table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;

    // Parse the stored CREATE TABLE DDL to extract column definitions
    let stmt = parse(&sql).map_err(|_| PlanError::UnsupportedStatement)?;
    let create = match stmt {
        Statement::CreateTable(c) => c,
        _ => return Err(PlanError::UnsupportedStatement),
    };

    let columns = create
        .columns
        .into_iter()
        .map(|col| schema::Column { name: col.name })
        .collect();

    Ok(schema::Table {
        name: table_name.to_string(),
        rootpage,
        columns,
    })
}

fn plan_select(select: ast::SelectStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    // 1. Extract table info from FROM clause
    let (table_name, table_ref) = extract_table_info(&select.from)?;

    // 2. Look up table in catalog
    let table = resolve_table(&table_name, btree)?;

    // 3. Detect if this query uses aggregation
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

    // 6. Build expression context
    let ctx = ExprContext {
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

    // 8. Build plan bottom-up: Scan → Filter? → Count/Aggregate/Project → Sort? → Limit?
    let mut plan = LogicalPlan::Scan {
        rootpage: table.rootpage,
        columns: mapping.scan_columns,
    };

    // Add Filter if WHERE clause exists
    if let Some(ref filter) = select.filter {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: convert_expr(filter, &ctx)?,
        };
    }

    // Add aggregation, count, or projection
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
                .map(|expr| convert_expr(expr, &ctx))
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
                aggregates.push(convert_aggregate(expr, &ctx)?);
            } else {
                // This SELECT column is a non-aggregate (must be a group key)
                let group_expr = convert_expr(expr, &ctx)?;
                // Find which group key this matches
                let group_key_index = group_keys
                    .iter()
                    .position(|gk| gk == &group_expr)
                    .ok_or_else(|| PlanError::UnsupportedStatement)?; // Non-aggregate not in GROUP BY
                projection_indices.push(group_key_index);
            }
        }

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys,
            aggregates,
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
                    _ => vec![convert_column_expr(col_expr, &ctx)],
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
                        let order_col_expr = convert_expr(&clause.expression, &ctx)?;
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
                    let scan_expr = convert_expr(&clause.expression, &ctx)?;

                    // Remap scan column indices to projection indices
                    let proj_expr = remap_column_indices(&scan_expr, &scan_idx_to_proj_idx)?;

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

    // 3. Build JoinExprContext
    let join_ctx = build_join_expr_context(&left_table, &left_ref, &right_table, &right_ref);

    // 4. Build scan plans (read ALL columns from each table)
    let left_scan = LogicalPlan::Scan {
        rootpage: left_table.rootpage,
        columns: (0..left_col_count).collect(),
    };
    let right_scan = LogicalPlan::Scan {
        rootpage: right_table.rootpage,
        columns: (0..right_col_count).collect(),
    };

    // 5. Convert ON condition using join context
    let on_condition = convert_expr_join(&join_clause.on_condition, &join_ctx)?;

    // 6. Build Join plan
    let mut plan = LogicalPlan::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        on_condition,
        left_column_count: left_col_count,
    };

    // 7. Add WHERE filter if present (also uses join context)
    if let Some(ref filter) = select.filter {
        let predicate = convert_expr_join(filter, &join_ctx)?;
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
                project_columns.push(convert_expr_join(expression, &join_ctx)?);
            }
            ast::ColumnExpression::Anonyomous(expression) => {
                project_columns.push(convert_expr_join(expression, &join_ctx)?);
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
            let order_expr = convert_expr_join(&clause.expression, &join_ctx)?;

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
                let join_expr = convert_expr_join(&clause.expression, &join_ctx)?;
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

    // Convert each value row to Literals, validating column count
    let mut rows = Vec::new();
    for value_row in &insert.values {
        if value_row.len() != table_columns.len() {
            return Err(PlanError::ColumnCountMismatch {
                expected: table_columns.len(),
                got: value_row.len(),
            });
        }
        let literals: Vec<Literal> = value_row
            .iter()
            .map(|expr| {
                let plan_expr = convert_expr_no_context(expr)?;
                eval_constant(&plan_expr)
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(literals);
    }

    Ok(LogicalPlan::Insert {
        rootpage: table.rootpage,
        table_columns,
        input: Box::new(LogicalPlan::Values { rows }),
    })
}

fn plan_update(update: ast::UpdateStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&update.table_name, btree)?;

    // Build column mapping: all table columns in order
    let mut column_map = HashMap::new();
    for (i, col) in table.columns.iter().enumerate() {
        column_map.insert(col.name.clone(), i);
    }

    // Create expression context
    let ctx = ExprContext {
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
        let expr_plan = convert_expr(&expr, &ctx)?;
        assignments.push((col_idx, expr_plan));
    }

    // Plan the filter expression if present
    let filter = match update.filter {
        Some(expr) => Some(convert_expr(&expr, &ctx)?),
        None => None,
    };

    // Get all table column indices
    let table_columns: Vec<usize> = (0..table.columns.len()).collect();

    Ok(LogicalPlan::Update {
        rootpage: table.rootpage,
        table_columns,
        assignments,
        filter,
    })
}

fn plan_delete(delete: ast::DeleteStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&delete.table_name, btree)?;

    // Build column mapping: all table columns in order
    let mut column_map = HashMap::new();
    for (i, col) in table.columns.iter().enumerate() {
        column_map.insert(col.name.clone(), i);
    }

    // Create expression context
    let ctx = ExprContext {
        table_ref: &delete.table_name,
        columns: &column_map,
    };

    // Plan the filter expression if present
    let filter = match delete.filter {
        Some(expr) => Some(convert_expr(&expr, &ctx)?),
        None => None,
    };

    // Get all table column indices
    let table_columns: Vec<usize> = (0..table.columns.len()).collect();

    Ok(LogicalPlan::Delete {
        rootpage: table.rootpage,
        table_columns,
        filter,
    })
}

/// Convert an AST expression to a PlanExpr without column resolution context.
/// Used for INSERT VALUES where expressions are constants (no column references).
fn convert_expr_no_context(expr: &ast::Expression) -> Result<PlanExpr, PlanError> {
    match expr {
        ast::Expression::Value(scalar) => convert_scalar_no_context(scalar),
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_expr_no_context(lhs)?),
            right: Box::new(convert_expr_no_context(rhs)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_expr_no_context(expression)?),
        }),
        ast::Expression::FunctionCall { name, args } => {
            // Validate function name
            let name_upper = name.to_uppercase();
            let supported_functions = ["LENGTH", "UPPER", "LOWER", "ABS"];

            if !supported_functions.contains(&name_upper.as_str()) {
                return Err(PlanError::UnknownFunction(name.clone()));
            }

            // For v1, all functions take exactly 1 argument
            if args.len() != 1 {
                return Err(PlanError::InvalidFunctionArguments {
                    function: name.clone(),
                    expected: 1,
                    got: args.len(),
                });
            }

            // Convert arguments
            let plan_args: Result<Vec<_>, _> = args.iter().map(convert_expr_no_context).collect();

            Ok(PlanExpr::FunctionCall {
                name: name_upper,
                args: plan_args?,
            })
        }
    }
}

fn convert_scalar_no_context(scalar: &ast::ScalarValue) -> Result<PlanExpr, PlanError> {
    match scalar {
        ast::ScalarValue::IntegerNumber(n) => Ok(PlanExpr::Literal(Literal::Integer(*n))),
        ast::ScalarValue::FloatingNumber(n) => Ok(PlanExpr::Literal(Literal::Float(*n))),
        ast::ScalarValue::StringLiteral(s) => Ok(PlanExpr::Literal(Literal::String(s.clone()))),
        ast::ScalarValue::Null => Ok(PlanExpr::Literal(Literal::Null)),
        ast::ScalarValue::Identifier(_) | ast::ScalarValue::MultiPartIdentifier(_, _) => {
            Err(PlanError::UnsupportedStatement)
        }
    }
}

/// Try to evaluate a PlanExpr down to a Literal at plan time.
fn eval_constant(expr: &PlanExpr) -> Result<Literal, PlanError> {
    match expr {
        PlanExpr::Literal(lit) => Ok(lit.clone()),
        PlanExpr::UnaryOp { op, operand } => {
            let val = eval_constant(operand)?;
            match (op, val) {
                (UnaryOp::Negate, Literal::Integer(n)) => Ok(Literal::Integer(-n)),
                (UnaryOp::Negate, Literal::Float(n)) => Ok(Literal::Float(-n)),
                (UnaryOp::Plus, v) => Ok(v),
                _ => Err(PlanError::UnsupportedStatement),
            }
        }
        PlanExpr::BinaryOp { op, left, right } => {
            let l = eval_constant(left)?;
            let r = eval_constant(right)?;
            eval_binary_constant(op, &l, &r)
        }
        PlanExpr::ColumnRef(_) => Err(PlanError::UnsupportedStatement),
        PlanExpr::FunctionCall { .. } => Err(PlanError::UnsupportedStatement),
    }
}

fn eval_binary_constant(op: &BinaryOp, l: &Literal, r: &Literal) -> Result<Literal, PlanError> {
    match (op, l, r) {
        (BinaryOp::Add, Literal::Integer(a), Literal::Integer(b)) => Ok(Literal::Integer(a + b)),
        (BinaryOp::Subtract, Literal::Integer(a), Literal::Integer(b)) => {
            Ok(Literal::Integer(a - b))
        }
        (BinaryOp::Multiply, Literal::Integer(a), Literal::Integer(b)) => {
            Ok(Literal::Integer(a * b))
        }
        (BinaryOp::Divide, Literal::Integer(a), Literal::Integer(b)) => Ok(Literal::Integer(a / b)),
        (BinaryOp::Remainder, Literal::Integer(a), Literal::Integer(b)) => {
            Ok(Literal::Integer(a % b))
        }
        (BinaryOp::Add, Literal::Float(a), Literal::Float(b)) => Ok(Literal::Float(a + b)),
        (BinaryOp::Subtract, Literal::Float(a), Literal::Float(b)) => Ok(Literal::Float(a - b)),
        (BinaryOp::Multiply, Literal::Float(a), Literal::Float(b)) => Ok(Literal::Float(a * b)),
        (BinaryOp::Divide, Literal::Float(a), Literal::Float(b)) => Ok(Literal::Float(a / b)),
        (BinaryOp::Add, Literal::String(a), Literal::String(b)) => {
            Ok(Literal::String(format!("{}{}", a, b)))
        }
        _ => Err(PlanError::UnsupportedStatement),
    }
}

/// Extract table name and reference (alias or table name) from FROM clause
fn extract_table_info(from: &ast::NamedTupleSource) -> Result<(String, String), PlanError> {
    match from {
        ast::NamedTupleSource::Named { alias, source } => {
            let table_name = extract_table_name(source)?;
            // The alias is what we use for column references
            Ok((table_name, alias.clone()))
        }
        ast::NamedTupleSource::Anonyomous(source) => {
            let table_name = extract_table_name(source)?;
            // No alias, use table name for references
            Ok((table_name.clone(), table_name))
        }
    }
}

fn extract_table_name(source: &ast::TupleSource) -> Result<String, PlanError> {
    match source {
        ast::TupleSource::Table(name) => Ok(name.clone()),
        ast::TupleSource::Subquery(_) => Err(PlanError::UnsupportedStatement),
    }
}

/// Convert a ColumnExpression to a PlanExpr
fn convert_column_expr(
    col_expr: &ast::ColumnExpression,
    ctx: &ExprContext,
) -> Result<PlanExpr, PlanError> {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => convert_expr(expression, ctx),
        ast::ColumnExpression::Anonyomous(expression) => convert_expr(expression, ctx),
        ast::ColumnExpression::Wildcard => {
            // Wildcard should be expanded before calling this function
            panic!("Wildcard should be expanded earlier in planning")
        }
    }
}

/// Extract limit count from a limit expression (must be an integer literal)
fn extract_limit_value(expr: &ast::Expression) -> Result<u64, PlanError> {
    match expr {
        ast::Expression::Value(ast::ScalarValue::IntegerNumber(n)) => {
            if *n < 0 {
                Err(PlanError::UnsupportedStatement)
            } else {
                Ok(*n as u64)
            }
        }
        _ => Err(PlanError::UnsupportedStatement),
    }
}

/// Check if an expression is an aggregate function
fn is_aggregate_function(expr: &ast::Expression) -> bool {
    match expr {
        ast::Expression::FunctionCall { name, .. } => {
            matches!(
                name.to_uppercase().as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            )
        }
        _ => false,
    }
}

/// Check if a column expression contains aggregates
fn has_aggregate(col_expr: &ast::ColumnExpression) -> bool {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => is_aggregate_function(expression),
        ast::ColumnExpression::Anonyomous(expression) => is_aggregate_function(expression),
        ast::ColumnExpression::Wildcard => false,
    }
}

/// Convert aggregate function to AggregateExpr
fn convert_aggregate(
    expr: &ast::Expression,
    ctx: &ExprContext,
) -> Result<AggregateExpr, PlanError> {
    match expr {
        ast::Expression::FunctionCall { name, args } => {
            let function = match name.to_uppercase().as_str() {
                "COUNT" => AggregateFunction::Count,
                "SUM" => AggregateFunction::Sum,
                "AVG" => AggregateFunction::Avg,
                "MIN" => AggregateFunction::Min,
                "MAX" => AggregateFunction::Max,
                _ => return Err(PlanError::UnknownFunction(name.clone())),
            };

            let argument = if args.is_empty() {
                // COUNT(*) has no argument
                None
            } else if args.len() == 1 {
                Some(convert_expr(&args[0], ctx)?)
            } else {
                return Err(PlanError::InvalidFunctionArguments {
                    function: name.clone(),
                    expected: 1,
                    got: args.len(),
                });
            };

            Ok(AggregateExpr { function, argument })
        }
        _ => Err(PlanError::UnsupportedStatement),
    }
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
}

// ============================================================================
// Expression Conversion
// ============================================================================

use std::collections::HashMap;

// TODO: For JOIN support, replace ExprContext with a ColumnResolver that handles:
//
// 1. Qualified refs (table.column): lookup in specific table
// 2. Unqualified refs (column): lookup across all tables, error if ambiguous
//
// Example: SELECT age, user.name FROM user JOIN relative ON relative.name = user.name
//   - "age" is allowed if only one table has it (otherwise ambiguous error)
//   - "user.name" must resolve to the "user" table specifically
//
// Data structure:
//   struct ColumnResolver {
//       // (table_alias, column_name) → scan output position
//       qualified: HashMap<(String, String), usize>,
//       // column_name → Some(position) if unique, None if ambiguous
//       unqualified: HashMap<String, Option<usize>>,
//   }
//
// Build by iterating all tables: add to qualified map, track ambiguity in unqualified map.

/// Context for expression conversion (single-table queries)
struct ExprContext<'a> {
    /// Valid table name or alias for qualified refs (e.g., "u" for "FROM users AS u")
    table_ref: &'a str,
    /// Maps column name → position in scan output
    columns: &'a HashMap<String, usize>,
}

/// Convert an AST Expression to a PlanExpr
fn convert_expr(expr: &ast::Expression, ctx: &ExprContext) -> Result<PlanExpr, PlanError> {
    match expr {
        ast::Expression::Value(scalar) => convert_scalar(scalar, ctx),
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_expr(lhs, ctx)?),
            right: Box::new(convert_expr(rhs, ctx)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_expr(expression, ctx)?),
        }),
        ast::Expression::FunctionCall { name, args } => {
            // Validate function name (case-insensitive)
            let name_upper = name.to_uppercase();
            let supported_functions = ["LENGTH", "UPPER", "LOWER", "ABS"];

            if !supported_functions.contains(&name_upper.as_str()) {
                return Err(PlanError::UnknownFunction(name.clone()));
            }

            // For v1, all functions take exactly 1 argument
            if args.len() != 1 {
                return Err(PlanError::InvalidFunctionArguments {
                    function: name.clone(),
                    expected: 1,
                    got: args.len(),
                });
            }

            // Convert arguments
            let plan_args: Result<Vec<_>, _> =
                args.iter().map(|arg| convert_expr(arg, ctx)).collect();

            Ok(PlanExpr::FunctionCall {
                name: name_upper,
                args: plan_args?,
            })
        }
    }
}

fn convert_scalar(scalar: &ast::ScalarValue, ctx: &ExprContext) -> Result<PlanExpr, PlanError> {
    match scalar {
        ast::ScalarValue::IntegerNumber(n) => Ok(PlanExpr::Literal(Literal::Integer(*n))),
        ast::ScalarValue::FloatingNumber(n) => Ok(PlanExpr::Literal(Literal::Float(*n))),
        ast::ScalarValue::StringLiteral(s) => Ok(PlanExpr::Literal(Literal::String(s.clone()))),
        ast::ScalarValue::Null => Ok(PlanExpr::Literal(Literal::Null)),
        ast::ScalarValue::Identifier(name) => {
            let pos = ctx
                .columns
                .get(name)
                .ok_or_else(|| PlanError::ColumnNotFound {
                    table: ctx.table_ref.to_string(),
                    column: name.clone(),
                })?;
            Ok(PlanExpr::ColumnRef(*pos))
        }
        ast::ScalarValue::MultiPartIdentifier(table_expr, column_name) => {
            // Extract table name from expression (e.g., "u" from "u.name")
            let ref_table = extract_identifier(table_expr)?;

            // Validate table reference matches our context
            if ref_table != ctx.table_ref {
                return Err(PlanError::TableNotFound(ref_table));
            }

            let pos = ctx
                .columns
                .get(column_name)
                .ok_or_else(|| PlanError::ColumnNotFound {
                    table: ctx.table_ref.to_string(),
                    column: column_name.clone(),
                })?;
            Ok(PlanExpr::ColumnRef(*pos))
        }
    }
}

/// Extract a simple identifier string from an expression
fn extract_identifier(expr: &ast::Expression) -> Result<String, PlanError> {
    match expr {
        ast::Expression::Value(ast::ScalarValue::Identifier(name)) => Ok(name.clone()),
        _ => Err(PlanError::UnsupportedStatement),
    }
}

// ============================================================================
// JOIN Expression Context
// ============================================================================

use std::collections::HashMap as JoinHashMap;

/// Context for resolving column references in JOIN expressions
struct JoinExprContext {
    /// Maps (table_name_or_alias, column_name) → position in combined output
    qualified: JoinHashMap<(String, String), usize>,
    /// Maps column_name → Some(position) if unambiguous, None if ambiguous
    unqualified: JoinHashMap<String, Option<usize>>,
}

/// Build JoinExprContext from two tables and their aliases
fn build_join_expr_context(
    left_table: &schema::Table,
    left_alias: &str,
    right_table: &schema::Table,
    right_alias: &str,
) -> JoinExprContext {
    let mut qualified = JoinHashMap::new();
    let mut unqualified = JoinHashMap::new();

    let left_col_count = left_table.columns.len();

    // Add left table columns (positions 0..left_col_count)
    for (idx, col) in left_table.columns.iter().enumerate() {
        qualified.insert((left_alias.to_string(), col.name.clone()), idx);

        // Track for unqualified resolution
        unqualified
            .entry(col.name.clone())
            .and_modify(|e| *e = None) // Mark as ambiguous if already exists
            .or_insert(Some(idx));
    }

    // Add right table columns (positions left_col_count..)
    for (idx, col) in right_table.columns.iter().enumerate() {
        let combined_idx = left_col_count + idx;
        qualified.insert((right_alias.to_string(), col.name.clone()), combined_idx);

        // Track for unqualified resolution
        unqualified
            .entry(col.name.clone())
            .and_modify(|e| *e = None) // Mark as ambiguous if already exists
            .or_insert(Some(combined_idx));
    }

    JoinExprContext {
        qualified,
        unqualified,
    }
}

/// Convert an AST expression to a plan expression using JOIN context
fn convert_expr_join(expr: &ast::Expression, ctx: &JoinExprContext) -> Result<PlanExpr, PlanError> {
    match expr {
        ast::Expression::Value(scalar) => convert_scalar_join(scalar, ctx),
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_expr_join(lhs, ctx)?),
            right: Box::new(convert_expr_join(rhs, ctx)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_expr_join(expression, ctx)?),
        }),
        ast::Expression::FunctionCall { name, args } => {
            let plan_args: Result<Vec<_>, _> =
                args.iter().map(|arg| convert_expr_join(arg, ctx)).collect();
            Ok(PlanExpr::FunctionCall {
                name: name.to_uppercase(),
                args: plan_args?,
            })
        }
    }
}

/// Convert an AST scalar value to a plan expression using JOIN context
fn convert_scalar_join(
    scalar: &ast::ScalarValue,
    ctx: &JoinExprContext,
) -> Result<PlanExpr, PlanError> {
    match scalar {
        ast::ScalarValue::IntegerNumber(n) => Ok(PlanExpr::Literal(Literal::Integer(*n))),
        ast::ScalarValue::FloatingNumber(n) => Ok(PlanExpr::Literal(Literal::Float(*n))),
        ast::ScalarValue::StringLiteral(s) => Ok(PlanExpr::Literal(Literal::String(s.clone()))),
        ast::ScalarValue::Null => Ok(PlanExpr::Literal(Literal::Null)),
        ast::ScalarValue::Identifier(name) => {
            // Unqualified column reference
            match ctx.unqualified.get(name) {
                Some(Some(pos)) => Ok(PlanExpr::ColumnRef(*pos)),
                Some(None) => Err(PlanError::AmbiguousColumn(name.clone())),
                None => Err(PlanError::ColumnNotFound {
                    table: "join".to_string(),
                    column: name.clone(),
                }),
            }
        }
        ast::ScalarValue::MultiPartIdentifier(table_expr, column_name) => {
            // Qualified column reference (e.g., e.name)
            let ref_table = extract_identifier(table_expr)?;
            match ctx.qualified.get(&(ref_table.clone(), column_name.clone())) {
                Some(pos) => Ok(PlanExpr::ColumnRef(*pos)),
                None => Err(PlanError::ColumnNotFound {
                    table: ref_table,
                    column: column_name.clone(),
                }),
            }
        }
    }
}

// ============================================================================
// Column Collection
// ============================================================================

use std::collections::HashSet;

/// Collect all column names referenced in an expression
fn collect_columns(expr: &ast::Expression, columns: &mut HashSet<String>) {
    match expr {
        ast::Expression::Value(scalar) => collect_columns_scalar(scalar, columns),
        ast::Expression::BinaryOp { lhs, rhs, .. } => {
            collect_columns(lhs, columns);
            collect_columns(rhs, columns);
        }
        ast::Expression::UnaryOp { expression, .. } => {
            collect_columns(expression, columns);
        }
        ast::Expression::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, columns);
            }
        }
    }
}

fn collect_columns_scalar(scalar: &ast::ScalarValue, columns: &mut HashSet<String>) {
    match scalar {
        ast::ScalarValue::Identifier(name) => {
            columns.insert(name.clone());
        }
        ast::ScalarValue::MultiPartIdentifier(_, column_name) => {
            // For table.column, we only need the column name
            columns.insert(column_name.clone());
        }
        ast::ScalarValue::IntegerNumber(_)
        | ast::ScalarValue::FloatingNumber(_)
        | ast::ScalarValue::StringLiteral(_)
        | ast::ScalarValue::Null => {
            // Literals don't reference columns
        }
    }
}

/// Collect columns from a ColumnExpression (SELECT list item)
fn collect_columns_from_column_expr(
    col_expr: &ast::ColumnExpression,
    columns: &mut HashSet<String>,
) {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => {
            collect_columns(expression, columns);
        }
        ast::ColumnExpression::Anonyomous(expression) => {
            collect_columns(expression, columns);
        }
        ast::ColumnExpression::Wildcard => {
            // Wildcard is handled specially in plan_select
        }
    }
}

/// Remap column indices in a PlanExpr from one space to another
fn remap_column_indices(
    expr: &PlanExpr,
    index_map: &HashMap<usize, usize>,
) -> Result<PlanExpr, PlanError> {
    match expr {
        PlanExpr::ColumnRef(column_idx) => {
            let new_idx = *index_map
                .get(column_idx)
                .expect("Column from scan should be in projection");
            Ok(PlanExpr::ColumnRef(new_idx))
        }
        PlanExpr::Literal(lit) => Ok(PlanExpr::Literal(lit.clone())),
        PlanExpr::BinaryOp { op, left, right } => Ok(PlanExpr::BinaryOp {
            op: op.clone(),
            left: Box::new(remap_column_indices(left, index_map)?),
            right: Box::new(remap_column_indices(right, index_map)?),
        }),
        PlanExpr::UnaryOp { op, operand } => Ok(PlanExpr::UnaryOp {
            op: op.clone(),
            operand: Box::new(remap_column_indices(operand, index_map)?),
        }),
        PlanExpr::FunctionCall { name, args } => {
            let remapped_args = args
                .iter()
                .map(|arg| remap_column_indices(arg, index_map))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanExpr::FunctionCall {
                name: name.clone(),
                args: remapped_args,
            })
        }
    }
}

// ============================================================================
// Column Map Building
// ============================================================================

/// Result of building the column map
#[derive(Debug, PartialEq)]
struct ColumnMapping {
    /// Indices of table columns to read (sorted)
    scan_columns: Vec<usize>,
    /// Maps column name → position in scan output
    column_map: HashMap<String, usize>,
}

/// Build the column mapping from a set of column names and table schema
///
/// Returns the scan columns list and a map from column name to scan output position.
fn build_column_mapping(
    columns: &HashSet<String>,
    table: &schema::Table,
    table_name: &str,
) -> Result<ColumnMapping, PlanError> {
    // Resolve each column name to its table index
    let mut table_indices: Vec<(String, usize)> = Vec::new();
    for col_name in columns {
        let idx = table
            .get_column_index(col_name)
            .ok_or_else(|| PlanError::ColumnNotFound {
                table: table_name.to_string(),
                column: col_name.clone(),
            })?;
        table_indices.push((col_name.clone(), idx));
    }

    // Sort by table index to get consistent scan order
    table_indices.sort_by_key(|(_, idx)| *idx);

    // Build scan_columns and column_map
    let mut scan_columns = Vec::new();
    let mut column_map = HashMap::new();
    for (scan_pos, (col_name, table_idx)) in table_indices.into_iter().enumerate() {
        scan_columns.push(table_idx);
        column_map.insert(col_name, scan_pos);
    }

    Ok(ColumnMapping {
        scan_columns,
        column_map,
    })
}

fn convert_binary_op(op: &ast::BinaryOp) -> BinaryOp {
    match op {
        ast::BinaryOp::Sum => BinaryOp::Add,
        ast::BinaryOp::Difference => BinaryOp::Subtract,
        ast::BinaryOp::Product => BinaryOp::Multiply,
        ast::BinaryOp::Quotient => BinaryOp::Divide,
        ast::BinaryOp::Remainder => BinaryOp::Remainder,
        ast::BinaryOp::Equals => BinaryOp::Equals,
        ast::BinaryOp::NotEquals => BinaryOp::NotEquals,
        ast::BinaryOp::GreaterThan => BinaryOp::GreaterThan,
        ast::BinaryOp::GreaterThanOrEqual => BinaryOp::GreaterThanOrEqual,
        ast::BinaryOp::LessThan => BinaryOp::LessThan,
        ast::BinaryOp::LessThanOrEqual => BinaryOp::LessThanOrEqual,
        ast::BinaryOp::And => BinaryOp::And,
        ast::BinaryOp::Or => BinaryOp::Or,
        ast::BinaryOp::LeftBitShift => BinaryOp::LeftShift,
        ast::BinaryOp::RightBitShift => BinaryOp::RightShift,
        ast::BinaryOp::BinaryOr => BinaryOp::BitOr,
        ast::BinaryOp::BinaryExclusiveOr => BinaryOp::BitXor,
        ast::BinaryOp::BinaryAnd => BinaryOp::BitAnd,
        ast::BinaryOp::Like => BinaryOp::Like,
    }
}

fn convert_unary_op(op: &ast::UnaryOp) -> UnaryOp {
    match op {
        ast::UnaryOp::Plus => UnaryOp::Plus,
        ast::UnaryOp::Negate => UnaryOp::Negate,
        ast::UnaryOp::IsNull => UnaryOp::IsNull,
        ast::UnaryOp::IsNotNull => UnaryOp::IsNotNull,
    }
}

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
        let ctx = ExprContext {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::IntegerNumber(42));
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Integer(42)));
    }

    #[test]
    fn test_convert_float_literal() {
        let columns = make_column_map();
        let ctx = ExprContext {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::FloatingNumber(3.14));
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Float(3.14)));
    }

    #[test]
    fn test_convert_column_ref() {
        let columns = make_column_map();
        let ctx = ExprContext {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()));
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(2));
    }

    #[test]
    fn test_convert_qualified_column_ref() {
        let columns = make_column_map();
        let ctx = ExprContext {
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
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(1));
    }

    #[test]
    fn test_convert_qualified_column_wrong_table() {
        let columns = make_column_map();
        let ctx = ExprContext {
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
        let result = convert_expr(&expr, &ctx);

        assert_eq!(result, Err(PlanError::TableNotFound("other".to_string())));
    }

    #[test]
    fn test_convert_column_not_found() {
        let columns = make_column_map();
        let ctx = ExprContext {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("nonexistent".to_string()));
        let result = convert_expr(&expr, &ctx);

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
        let ctx = ExprContext {
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
        let result = convert_expr(&expr, &ctx).unwrap();

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
        let ctx = ExprContext {
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
        let result = convert_expr(&expr, &ctx).unwrap();

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
        let ctx = ExprContext {
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
        let result = convert_expr(&expr, &ctx).unwrap();

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
                columns: vec![0, 1], // id, name
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
    fn test_join_expr_context() {
        // Build a JoinExprContext with:
        //   left: columns [id, name, dept_id], alias "e"
        //   right: columns [id, name], alias "d"
        use super::{build_join_expr_context, schema};

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

        let ctx = build_join_expr_context(&left_table, "e", &right_table, "d");

        // Test qualified resolution
        assert_eq!(
            ctx.qualified.get(&("e".to_string(), "name".to_string())),
            Some(&1)
        );
        assert_eq!(
            ctx.qualified.get(&("d".to_string(), "name".to_string())),
            Some(&4)
        );
        assert_eq!(
            ctx.qualified.get(&("e".to_string(), "dept_id".to_string())),
            Some(&2)
        );

        // Test unqualified unique column
        assert_eq!(ctx.unqualified.get("dept_id"), Some(&Some(2)));

        // Test unqualified ambiguous columns (appear in both tables)
        assert_eq!(ctx.unqualified.get("id"), Some(&None));
        assert_eq!(ctx.unqualified.get("name"), Some(&None));

        // Test missing column
        assert_eq!(
            ctx.qualified
                .get(&("e".to_string(), "nonexistent".to_string())),
            None
        );
    }

    #[test]
    fn test_convert_expr_join() {
        use super::{build_join_expr_context, convert_expr_join, schema, PlanExpr};

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

        let ctx = build_join_expr_context(&left_table, "e", &right_table, "d");

        // Test qualified column: e.dept_id → ColumnRef(1)
        let ast_expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "e".to_string(),
            ))),
            "dept_id".to_string(),
        ));
        let plan_expr = convert_expr_join(&ast_expr, &ctx).unwrap();
        assert_eq!(plan_expr, PlanExpr::ColumnRef(1));

        // Test qualified column: d.id → ColumnRef(2)
        let ast_expr2 = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "d".to_string(),
            ))),
            "id".to_string(),
        ));
        let plan_expr2 = convert_expr_join(&ast_expr2, &ctx).unwrap();
        assert_eq!(plan_expr2, PlanExpr::ColumnRef(2));

        // Test unqualified unique column: dept_id → ColumnRef(1)
        let ast_expr3 = ast::Expression::Value(ast::ScalarValue::Identifier("dept_id".to_string()));
        let plan_expr3 = convert_expr_join(&ast_expr3, &ctx).unwrap();
        assert_eq!(plan_expr3, PlanExpr::ColumnRef(1));

        // Test ambiguous column: id → Error
        let ast_expr4 = ast::Expression::Value(ast::ScalarValue::Identifier("id".to_string()));
        let result = convert_expr_join(&ast_expr4, &ctx);
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
        let plan_expr5 = convert_expr_join(&ast_expr5, &ctx).unwrap();
        if let PlanExpr::BinaryOp { left, right, .. } = plan_expr5 {
            assert_eq!(*left, PlanExpr::ColumnRef(1));
            assert_eq!(*right, PlanExpr::ColumnRef(2));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_plan_join() {
        use super::{plan, schema, LogicalPlan, PlanExpr};
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
            c.insert(1, buf); // key 1 (catalog row 0 is self-referencing db_schema)
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
            c.insert(2, buf); // key 2
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
}
