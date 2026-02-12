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

/// Reference to a column from an input node
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnRef {
    /// Column from a single-input node (Filter, Project, etc.)
    /// column_idx is the index into the input node's output columns
    Single { column_idx: usize },
    // Future: Multi { node_idx: usize, column_idx: usize } for JOINs
}

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

/// Planner's expression type - like ast::Expression but with resolved columns
#[derive(Debug, Clone, PartialEq)]
pub enum PlanExpr {
    ColumnRef(ColumnRef),
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

    /// Count rows from input (1 input)
    /// Consumes all rows from child and outputs a single row with the count.
    /// Output: single integer column containing the row count.
    #[allow(dead_code)]
    Count { input: Box<LogicalPlan> },

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
    // Future: Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, ... }
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
        Statement::Select(select) => plan_select(select, btree),
        Statement::CreateTable(_) => Err(PlanError::UnsupportedStatement),
        Statement::Insert(insert) => plan_insert(insert, btree),
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

    // 3. Collect all column references from SELECT and WHERE
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

    // 4. Build column mapping
    let mapping = build_column_mapping(&columns_needed, &table, &table_ref)?;

    // 5. Build expression context
    let ctx = ExprContext {
        table_ref: &table_ref,
        columns: &mapping.column_map,
    };

    // 6. Convert SELECT expressions
    let project_exprs: Vec<PlanExpr> = select
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
                        .map(|(idx, _col)| {
                            Ok(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: idx }))
                        })
                        .collect::<Vec<_>>()
                }
                _ => vec![convert_column_expr(col_expr, &ctx)],
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // 7. Build plan bottom-up: Scan → Filter? → Project → Limit?
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

    // Add Project
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: project_exprs,
    };

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

#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    TableNotFound(String),
    ColumnNotFound { table: String, column: String },
    ColumnCountMismatch { expected: usize, got: usize },
    UnsupportedStatement,
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
            Ok(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: *pos }))
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
            Ok(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: *pos }))
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
    }
}

fn convert_unary_op(op: &ast::UnaryOp) -> UnaryOp {
    match op {
        ast::UnaryOp::Plus => UnaryOp::Plus,
        ast::UnaryOp::Negate => UnaryOp::Negate,
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

        assert_eq!(
            result,
            PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })
        );
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

        assert_eq!(
            result,
            PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 })
        );
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
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })),
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
                operand: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })),
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
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })),
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
            1,
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
            columns: vec![
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
            ],
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
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 })), // age
                    right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
                },
            }),
            columns: vec![PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })], // name
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
                columns: vec![PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })],
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
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }),
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
            1,
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
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 3 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 4 }),
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
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }),
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
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }),
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
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }),
                PlanExpr::BinaryOp {
                    op: BinaryOp::Add,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })),
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
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::Literal(Literal::Null),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
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
}
