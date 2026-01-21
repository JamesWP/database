//! Query Planner - Logical Operator Tree (Option A)
//!
//! Converts AST to a tree of logical operators (LogicalPlan).
//! The compiler (future) will convert LogicalPlan to bytecode.

use crate::frontend::ast::Statement;

// ============================================================================
// Operators
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Plus,
    Negate,
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
    Bool(bool),
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
    /// columns: indices of columns to read from the table schema
    Scan { table: String, columns: Vec<usize> },

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
    Limit {
        input: Box<LogicalPlan>,
        count: u64,
    },

    /// Count rows from input (1 input)
    /// Consumes all rows from child and outputs a single row with the count.
    /// Output: single integer column containing the row count.
    Count { input: Box<LogicalPlan> },

    /// Emit fixed rows (leaf node, no inputs)
    /// Useful for testing and for VALUES clauses.
    /// Each inner Vec is a row; all rows must have the same number of columns.
    Values { rows: Vec<Vec<Literal>> },

    /// Generate a sequence of integers (leaf node, no inputs)
    /// Useful for testing. Generates rows [start], [start+1], ..., [end-1]
    /// Output: single integer column
    Sequence { start: i64, end: i64 },

    // Future: Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, ... }
}

// ============================================================================
// Schema (for column resolution)
// ============================================================================

pub mod schema {
    #[derive(Debug, Clone)]
    pub struct Schema {
        pub tables: Vec<Table>,
    }

    #[derive(Debug, Clone)]
    pub struct Table {
        pub name: String,
        pub columns: Vec<Column>,
    }

    #[derive(Debug, Clone)]
    pub struct Column {
        pub name: String,
        // Future: pub data_type: DataType,
    }

    impl Schema {
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

/// Convert an AST Statement to a LogicalPlan
pub fn plan(statement: Statement, schema: &schema::Schema) -> Result<LogicalPlan, PlanError> {
    match statement {
        Statement::Select(select) => plan_select(select, schema),
    }
}

fn plan_select(
    select: ast::SelectStatement,
    schema: &schema::Schema,
) -> Result<LogicalPlan, PlanError> {
    // 1. Extract table info from FROM clause
    let (table_name, table_ref) = extract_table_info(&select.from)?;

    // 2. Look up table in schema
    let table = schema
        .get_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;

    // 3. Collect all column references from SELECT and WHERE
    let mut columns_needed = HashSet::new();
    for col_expr in &select.columns {
        collect_columns_from_column_expr(col_expr, &mut columns_needed);
    }
    if let Some(ref filter) = select.filter {
        collect_columns(filter, &mut columns_needed);
    }

    // 4. Build column mapping
    let mapping = build_column_mapping(&columns_needed, table, &table_ref)?;

    // 5. Build expression context
    let ctx = ExprContext {
        table_ref: &table_ref,
        columns: &mapping.column_map,
    };

    // 6. Convert SELECT expressions
    let project_exprs: Vec<PlanExpr> = select
        .columns
        .iter()
        .map(|col_expr| convert_column_expr(col_expr, &ctx))
        .collect::<Result<Vec<_>, _>>()?;

    // 7. Build plan bottom-up: Scan → Filter? → Project → Limit?
    let mut plan = LogicalPlan::Scan {
        table: table_name,
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
    UnsupportedStatement,
}

// ============================================================================
// Expression Conversion
// ============================================================================

use std::collections::HashMap;
use crate::frontend::ast;

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
        ast::ScalarValue::Identifier(name) => {
            let pos = ctx.columns.get(name).ok_or_else(|| PlanError::ColumnNotFound {
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

            let pos = ctx.columns.get(column_name).ok_or_else(|| PlanError::ColumnNotFound {
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
        ast::ScalarValue::IntegerNumber(_) | ast::ScalarValue::FloatingNumber(_) => {
            // Literals don't reference columns
        }
    }
}

/// Collect columns from a ColumnExpression (SELECT list item)
fn collect_columns_from_column_expr(col_expr: &ast::ColumnExpression, columns: &mut HashSet<String>) {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => {
            collect_columns(expression, columns);
        }
        ast::ColumnExpression::Anonyomous(expression) => {
            collect_columns(expression, columns);
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
        let idx = table.get_column_index(col_name).ok_or_else(|| {
            PlanError::ColumnNotFound {
                table: table_name.to_string(),
                column: col_name.clone(),
            }
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
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        let expr = ast::Expression::Value(ast::ScalarValue::IntegerNumber(42));
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Integer(42)));
    }

    #[test]
    fn test_convert_float_literal() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        let expr = ast::Expression::Value(ast::ScalarValue::FloatingNumber(3.14));
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Float(3.14)));
    }

    #[test]
    fn test_convert_column_ref() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()));
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }));
    }

    #[test]
    fn test_convert_qualified_column_ref() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        // users.name
        let table_expr = Box::new(ast::Expression::Value(
            ast::ScalarValue::Identifier("users".to_string())
        ));
        let expr = ast::Expression::Value(
            ast::ScalarValue::MultiPartIdentifier(table_expr, "name".to_string())
        );
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }));
    }

    #[test]
    fn test_convert_qualified_column_wrong_table() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        // other.name - should fail because "other" != "users"
        let table_expr = Box::new(ast::Expression::Value(
            ast::ScalarValue::Identifier("other".to_string())
        ));
        let expr = ast::Expression::Value(
            ast::ScalarValue::MultiPartIdentifier(table_expr, "name".to_string())
        );
        let result = convert_expr(&expr, &ctx);

        assert_eq!(result, Err(PlanError::TableNotFound("other".to_string())));
    }

    #[test]
    fn test_convert_column_not_found() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("nonexistent".to_string()));
        let result = convert_expr(&expr, &ctx);

        assert_eq!(result, Err(PlanError::ColumnNotFound {
            table: "users".to_string(),
            column: "nonexistent".to_string(),
        }));
    }

    #[test]
    fn test_convert_binary_comparison() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        // age > 21
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::BinaryOp {
            op: BinaryOp::GreaterThan,
            left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })),
            right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
        });
    }

    #[test]
    fn test_convert_unary_negate() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        // -age
        let expr = ast::Expression::UnaryOp {
            op: ast::UnaryOp::Negate,
            expression: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
        };
        let result = convert_expr(&expr, &ctx).unwrap();

        assert_eq!(result, PlanExpr::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 })),
        });
    }

    #[test]
    fn test_convert_nested_expression() {
        let columns = make_column_map();
        let ctx = ExprContext { table_ref: "users", columns: &columns };

        // (age + 1) > 21
        let age_plus_one = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
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
        let table_expr = Box::new(ast::Expression::Value(
            ast::ScalarValue::Identifier("users".to_string())
        ));
        let expr = ast::Expression::Value(
            ast::ScalarValue::MultiPartIdentifier(table_expr, "name".to_string())
        );
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
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
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
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("name".to_string()))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["name".to_string(), "age".to_string()]));
    }

    #[test]
    fn test_collect_nested_columns() {
        // (age + 1) > id
        let age_plus_one = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        };
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(age_plus_one),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("id".to_string()))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string(), "id".to_string()]));
    }

    #[test]
    fn test_collect_from_column_expr_named() {
        // SELECT age AS user_age
        let col_expr = ast::ColumnExpression::Named {
            name: "user_age".to_string(),
            expression: Box::new(ast::Expression::Value(
                ast::ScalarValue::Identifier("age".to_string())
            )),
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
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()))),
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
            columns: vec![
                schema::Column { name: "id".to_string() },
                schema::Column { name: "name".to_string() },
                schema::Column { name: "age".to_string() },
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
        let columns = HashSet::from([
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
        ]);

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

        assert_eq!(result, Err(PlanError::ColumnNotFound {
            table: "users".to_string(),
            column: "nonexistent".to_string(),
        }));
    }

    // ========================================================================
    // Plan Tests
    // ========================================================================

    fn make_users_schema() -> schema::Schema {
        schema::Schema {
            tables: vec![schema::Table {
                name: "users".to_string(),
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
            }],
        }
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
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT id, name FROM users");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
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
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT name FROM users WHERE age > 21");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
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
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT name FROM users LIMIT 10");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
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
    #[ignore = "parser does not yet support SELECT *"]
    fn test_select_star() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT * FROM users");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
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

    /// Error case: table not found
    #[test]
    fn test_table_not_found() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT id FROM nonexistent");

        let result = plan(stmt, &schema);

        assert_eq!(
            result,
            Err(PlanError::TableNotFound("nonexistent".to_string()))
        );
    }

    /// Error case: column not found
    #[test]
    fn test_column_not_found() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT nonexistent FROM users");

        let result = plan(stmt, &schema);

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }
}
