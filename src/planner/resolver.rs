//! Column resolution, expression conversion, and related helpers.

use std::collections::{HashMap, HashSet};

use crate::frontend::ast;
use crate::storage::BTree;

use super::{
    schema, select::output_width, select::plan_select, AggregateExpr, AggregateFunction, BinaryOp,
    Literal, PlanError, PlanExpr, UnaryOp,
};

// ============================================================================
// Column Resolution Strategy (Trait-Based)
// ============================================================================

/// Strategy for resolving column references during expression conversion
pub(super) trait ColumnResolver {
    fn resolve_identifier(&self, name: &str) -> Result<usize, PlanError>;
    fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize, PlanError>;
    /// Output position of the B-tree key (scan index 0) in the scan output, if available.
    /// Returns None when rowid() is unsupported in this context (e.g. joins).
    fn resolve_rowid(&self) -> Option<usize> {
        None
    }
}

/// Single-table resolver (for SELECT, WHERE, etc.)
pub(super) struct SingleTableResolver<'a> {
    pub(super) table_ref: &'a str,
    pub(super) columns: &'a HashMap<String, usize>,
    /// Output position of scan index 0 (B-tree key / rowid), if rowid() is requested.
    pub(super) rowid_output_pos: Option<usize>,
}

impl ColumnResolver for SingleTableResolver<'_> {
    fn resolve_identifier(&self, name: &str) -> Result<usize, PlanError> {
        self.columns
            .get(name)
            .copied()
            .ok_or_else(|| PlanError::ColumnNotFound {
                table: self.table_ref.to_string(),
                column: name.to_string(),
            })
    }

    fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize, PlanError> {
        if table != self.table_ref {
            return Err(PlanError::TableNotFound(table.to_string()));
        }
        self.resolve_identifier(column)
    }

    fn resolve_rowid(&self) -> Option<usize> {
        self.rowid_output_pos
    }
}

/// JOIN resolver (handles qualified and ambiguous columns)
pub(super) struct JoinResolver<'a> {
    pub(super) qualified: &'a HashMap<(String, String), usize>,
    pub(super) unqualified: &'a HashMap<String, Option<usize>>,
}

impl ColumnResolver for JoinResolver<'_> {
    fn resolve_identifier(&self, name: &str) -> Result<usize, PlanError> {
        match self.unqualified.get(name) {
            Some(Some(pos)) => Ok(*pos),
            Some(None) => Err(PlanError::AmbiguousColumn(name.to_string())),
            None => Err(PlanError::ColumnNotFound {
                table: "join".to_string(),
                column: name.to_string(),
            }),
        }
    }

    fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize, PlanError> {
        self.qualified
            .get(&(table.to_string(), column.to_string()))
            .copied()
            .ok_or_else(|| PlanError::ColumnNotFound {
                table: table.to_string(),
                column: column.to_string(),
            })
    }
}

/// No-context resolver (for INSERT VALUES - disallows column refs)
pub(super) struct NoColumnResolver;

impl ColumnResolver for NoColumnResolver {
    fn resolve_identifier(&self, name: &str) -> Result<usize, PlanError> {
        Err(PlanError::ColumnNotFound {
            table: "VALUES".to_string(),
            column: name.to_string(),
        })
    }

    fn resolve_qualified(&self, _table: &str, _column: &str) -> Result<usize, PlanError> {
        Err(PlanError::ColumnNotFound {
            table: "VALUES".to_string(),
            column: "qualified reference".to_string(),
        })
    }
}

// ============================================================================
// Expression Conversion
// ============================================================================

/// Convert an AST Expression to a PlanExpr using a column resolution strategy
pub(super) fn convert_expr(
    expr: &ast::Expression,
    resolver: &impl ColumnResolver,
) -> Result<PlanExpr, PlanError> {
    match expr {
        ast::Expression::Value(scalar) => convert_scalar(scalar, resolver),
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_expr(lhs, resolver)?),
            right: Box::new(convert_expr(rhs, resolver)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_expr(expression, resolver)?),
        }),
        ast::Expression::FunctionCall { name, args } => {
            // Validate function name (case-insensitive)
            let name_upper = name.to_uppercase();

            // RANDOM() takes zero arguments
            if name_upper == "RANDOM" {
                if !args.is_empty() {
                    return Err(PlanError::InvalidFunctionArguments {
                        function: name.clone(),
                        expected: 0,
                        got: args.len(),
                    });
                }
                return Ok(PlanExpr::FunctionCall {
                    name: name_upper,
                    args: vec![],
                });
            }

            // ROWID() resolves to the output position of scan index 0 (B-tree key).
            if name_upper == "ROWID" {
                if !args.is_empty() {
                    return Err(PlanError::InvalidFunctionArguments {
                        function: name.clone(),
                        expected: 0,
                        got: args.len(),
                    });
                }
                return resolver
                    .resolve_rowid()
                    .map(PlanExpr::ColumnRef)
                    .ok_or(PlanError::UnsupportedStatement);
            }

            let supported_functions = ["LENGTH", "UPPER", "LOWER", "ABS"];

            if !supported_functions.contains(&name_upper.as_str()) {
                return Err(PlanError::UnknownFunction(name.clone()));
            }

            // For v1, all other functions take exactly 1 argument
            if args.len() != 1 {
                return Err(PlanError::InvalidFunctionArguments {
                    function: name.clone(),
                    expected: 1,
                    got: args.len(),
                });
            }

            // Convert arguments
            let plan_args: Result<Vec<_>, _> =
                args.iter().map(|arg| convert_expr(arg, resolver)).collect();

            Ok(PlanExpr::FunctionCall {
                name: name_upper,
                args: plan_args?,
            })
        }
        ast::Expression::In {
            expr,
            source: ast::InSource::Values(vals),
            negated,
        } => {
            let plan_expr = convert_expr(expr, resolver)?;
            let plan_values: Result<Vec<_>, _> =
                vals.iter().map(|v| convert_expr(v, resolver)).collect();
            Ok(PlanExpr::In {
                expr: Box::new(plan_expr),
                values: plan_values?,
                negated: *negated,
            })
        }
        ast::Expression::In { .. } => Err(PlanError::UnsupportedStatement),
        ast::Expression::ScalarSubquery(_) => Err(PlanError::UnsupportedStatement),
    }
}

/// Convert an AST Expression with catalog access for scalar subquery resolution.
/// Fully recursive: handles `Expression::ScalarSubquery` at any depth.
pub(super) fn convert_expr_with_catalog(
    expr: &ast::Expression,
    resolver: &impl ColumnResolver,
    catalog: &BTree,
) -> Result<PlanExpr, PlanError> {
    match expr {
        ast::Expression::ScalarSubquery(stmt) => {
            let inner_plan = plan_select(*stmt.clone(), catalog)?;
            if output_width(&inner_plan) != 1 {
                return Err(PlanError::ScalarSubqueryMustReturnOneColumn);
            }
            Ok(PlanExpr::ScalarSubquery {
                plan: Box::new(inner_plan),
            })
        }
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_expr_with_catalog(lhs, resolver, catalog)?),
            right: Box::new(convert_expr_with_catalog(rhs, resolver, catalog)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_expr_with_catalog(expression, resolver, catalog)?),
        }),
        // All other variants don't contain subexpressions that could be ScalarSubquery.
        other => convert_expr(other, resolver),
    }
}

pub(super) fn convert_scalar(
    scalar: &ast::ScalarValue,
    resolver: &impl ColumnResolver,
) -> Result<PlanExpr, PlanError> {
    match scalar {
        ast::ScalarValue::IntegerNumber(n) => Ok(PlanExpr::Literal(Literal::Integer(*n))),
        ast::ScalarValue::FloatingNumber(n) => Ok(PlanExpr::Literal(Literal::Float(*n))),
        ast::ScalarValue::StringLiteral(s) => Ok(PlanExpr::Literal(Literal::String(s.clone()))),
        ast::ScalarValue::Null => Ok(PlanExpr::Literal(Literal::Null)),
        ast::ScalarValue::Identifier(name) => {
            let idx = resolver.resolve_identifier(name)?;
            Ok(PlanExpr::ColumnRef(idx))
        }
        ast::ScalarValue::MultiPartIdentifier(table_expr, column_name) => {
            let ref_table = extract_identifier(table_expr)?;
            let idx = resolver.resolve_qualified(&ref_table, column_name)?;
            Ok(PlanExpr::ColumnRef(idx))
        }
    }
}

/// Extract a simple identifier string from an expression
pub(super) fn extract_identifier(expr: &ast::Expression) -> Result<String, PlanError> {
    match expr {
        ast::Expression::Value(ast::ScalarValue::Identifier(name)) => Ok(name.clone()),
        _ => Err(PlanError::UnsupportedStatement),
    }
}

pub(super) fn convert_column_expr_with_catalog(
    col_expr: &ast::ColumnExpression,
    resolver: &impl ColumnResolver,
    catalog: &BTree,
) -> Result<PlanExpr, PlanError> {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => {
            convert_expr_with_catalog(expression, resolver, catalog)
        }
        ast::ColumnExpression::Anonyomous(expression) => {
            convert_expr_with_catalog(expression, resolver, catalog)
        }
        ast::ColumnExpression::Wildcard => {
            panic!("Wildcard should be expanded earlier in planning")
        }
    }
}

pub(super) fn convert_binary_op(op: &ast::BinaryOp) -> BinaryOp {
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

pub(super) fn convert_unary_op(op: &ast::UnaryOp) -> UnaryOp {
    match op {
        ast::UnaryOp::Plus => UnaryOp::Plus,
        ast::UnaryOp::Negate => UnaryOp::Negate,
        ast::UnaryOp::IsNull => UnaryOp::IsNull,
        ast::UnaryOp::IsNotNull => UnaryOp::IsNotNull,
        ast::UnaryOp::Not => UnaryOp::Not,
    }
}

// ============================================================================
// Aggregate helpers
// ============================================================================

/// Check if an expression is an aggregate function
pub(super) fn is_aggregate_function(expr: &ast::Expression) -> bool {
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
pub(super) fn has_aggregate(col_expr: &ast::ColumnExpression) -> bool {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => is_aggregate_function(expression),
        ast::ColumnExpression::Anonyomous(expression) => is_aggregate_function(expression),
        ast::ColumnExpression::Wildcard => false,
    }
}

/// Convert aggregate function to AggregateExpr
pub(super) fn convert_aggregate(
    expr: &ast::Expression,
    resolver: &impl ColumnResolver,
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
                Some(convert_expr(&args[0], resolver)?)
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

/// Convert a HAVING expression to a PlanExpr in aggregate-output space.
///
/// Column references are resolved to their position in the aggregate output:
///   [group_key_0, group_key_1, ..., agg_0, agg_1, ...]
///
/// Aggregate function calls are mapped to their index in `aggregates`
/// (appending if not already present).
pub(super) fn convert_having_expr(
    expr: &ast::Expression,
    group_keys: &[PlanExpr],
    aggregates: &mut Vec<AggregateExpr>,
    resolver: &impl ColumnResolver,
) -> Result<PlanExpr, PlanError> {
    if is_aggregate_function(expr) {
        let agg = convert_aggregate(expr, resolver)?;
        let agg_idx = if let Some(idx) = aggregates.iter().position(|a| a == &agg) {
            idx
        } else {
            let idx = aggregates.len();
            aggregates.push(agg);
            idx
        };
        return Ok(PlanExpr::ColumnRef(group_keys.len() + agg_idx));
    }

    match expr {
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_having_expr(lhs, group_keys, aggregates, resolver)?),
            right: Box::new(convert_having_expr(rhs, group_keys, aggregates, resolver)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_having_expr(
                expression, group_keys, aggregates, resolver,
            )?),
        }),
        ast::Expression::Value(scalar) => match scalar {
            ast::ScalarValue::Identifier(name) => {
                // Resolve to scan position, then find which group key it maps to
                let scan_idx = resolver.resolve_identifier(name)?;
                let col_ref = PlanExpr::ColumnRef(scan_idx);
                let group_idx =
                    group_keys
                        .iter()
                        .position(|gk| gk == &col_ref)
                        .ok_or_else(|| {
                            PlanError::InvalidHaving(format!(
                                "Column '{}' must appear in GROUP BY or be used in an aggregate",
                                name
                            ))
                        })?;
                Ok(PlanExpr::ColumnRef(group_idx))
            }
            ast::ScalarValue::MultiPartIdentifier(table_expr, column_name) => {
                let ref_table = extract_identifier(table_expr)?;
                let scan_idx = resolver.resolve_qualified(&ref_table, column_name)?;
                let col_ref = PlanExpr::ColumnRef(scan_idx);
                let group_idx =
                    group_keys
                        .iter()
                        .position(|gk| gk == &col_ref)
                        .ok_or_else(|| {
                            PlanError::InvalidHaving(format!(
                                "Column '{}.{}' must appear in GROUP BY or be used in an aggregate",
                                ref_table, column_name
                            ))
                        })?;
                Ok(PlanExpr::ColumnRef(group_idx))
            }
            // Literals pass through
            _ => convert_scalar(scalar, resolver),
        },
        ast::Expression::FunctionCall { name, .. } => Err(PlanError::UnknownFunction(name.clone())),
        ast::Expression::In { .. } | ast::Expression::ScalarSubquery(_) => {
            Err(PlanError::UnsupportedStatement)
        }
    }
}

// ============================================================================
// Column Collection
// ============================================================================

/// Collect all column names referenced in an expression
pub(super) fn collect_columns(expr: &ast::Expression, columns: &mut HashSet<String>) {
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
        ast::Expression::In { expr, .. } => collect_columns(expr, columns),
        ast::Expression::ScalarSubquery(_) => {}
    }
}

/// Return true if the expression tree contains a zero-argument `rowid()` call.
pub(super) fn expr_uses_rowid(expr: &ast::Expression) -> bool {
    match expr {
        ast::Expression::FunctionCall { name, args } => {
            (name.to_uppercase() == "ROWID" && args.is_empty()) || args.iter().any(expr_uses_rowid)
        }
        ast::Expression::BinaryOp { lhs, rhs, .. } => expr_uses_rowid(lhs) || expr_uses_rowid(rhs),
        ast::Expression::UnaryOp { expression, .. } => expr_uses_rowid(expression),
        ast::Expression::Value(_) => false,
        ast::Expression::In { expr, .. } => expr_uses_rowid(expr),
        ast::Expression::ScalarSubquery(_) => false,
    }
}

/// Return true if any expression in the SELECT column list uses rowid().
pub(super) fn col_expr_uses_rowid(col_expr: &ast::ColumnExpression) -> bool {
    match col_expr {
        ast::ColumnExpression::Named { expression, .. } => expr_uses_rowid(expression),
        ast::ColumnExpression::Anonyomous(expression) => expr_uses_rowid(expression),
        ast::ColumnExpression::Wildcard => false,
    }
}

pub(super) fn collect_columns_scalar(scalar: &ast::ScalarValue, columns: &mut HashSet<String>) {
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
pub(super) fn collect_columns_from_column_expr(
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
pub(super) struct ColumnMapping {
    /// Indices of table columns to read (sorted)
    pub(super) scan_columns: Vec<usize>,
    /// Maps column name → position in scan output
    pub(super) column_map: HashMap<String, usize>,
}

/// Build the column mapping from a set of column names and table schema
///
/// Returns the scan columns list and a map from column name to scan output position.
pub(super) fn build_column_mapping(
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

// ============================================================================
// Remap Column Indices
// ============================================================================

/// Remap column indices in a PlanExpr from one space to another
pub(super) fn remap_column_indices(
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
        PlanExpr::In {
            expr,
            values,
            negated,
        } => {
            let remapped_values = values
                .iter()
                .map(|v| remap_column_indices(v, index_map))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanExpr::In {
                expr: Box::new(remap_column_indices(expr, index_map)?),
                values: remapped_values,
                negated: *negated,
            })
        }
        PlanExpr::ScalarSubquery { plan } => Ok(PlanExpr::ScalarSubquery { plan: plan.clone() }),
    }
}

// ============================================================================
// Table Info Extraction
// ============================================================================

/// Extract table name and reference (alias or table name) from FROM clause
pub(super) fn extract_table_info(
    from: &ast::NamedTupleSource,
) -> Result<(String, String), PlanError> {
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

pub(super) fn extract_table_name(source: &ast::TupleSource) -> Result<String, PlanError> {
    match source {
        ast::TupleSource::Table(name) => Ok(name.clone()),
        ast::TupleSource::Subquery(_) => Err(PlanError::UnsupportedStatement),
    }
}

// ============================================================================
// Limit Value Extraction
// ============================================================================

/// Extract limit count from a limit expression (must be an integer literal)
pub(super) fn extract_limit_value(expr: &ast::Expression) -> Result<u64, PlanError> {
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

// ============================================================================
// Constant Evaluation
// ============================================================================

/// Try to evaluate a PlanExpr down to a Literal at plan time.
pub(super) fn eval_constant(expr: &PlanExpr) -> Result<Literal, PlanError> {
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
        PlanExpr::In { .. } => Err(PlanError::UnsupportedStatement),
        PlanExpr::ScalarSubquery { .. } => Err(PlanError::UnsupportedStatement),
    }
}

pub(super) fn eval_binary_constant(
    op: &BinaryOp,
    l: &Literal,
    r: &Literal,
) -> Result<Literal, PlanError> {
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

// ============================================================================
// AST Expression Name
// ============================================================================

/// Derive a display name from an AST expression for use as a column header.
pub(super) fn ast_expr_name(expr: &ast::Expression) -> String {
    match expr {
        ast::Expression::Value(ast::ScalarValue::Identifier(name)) => name.clone(),
        ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(_, name)) => name.clone(),
        ast::Expression::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}(*)", name.to_uppercase())
            } else {
                let arg_str = args
                    .iter()
                    .map(ast_expr_name)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name.to_uppercase(), arg_str)
            }
        }
        _ => "?".to_string(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::ast;
    use crate::planner::{BinaryOp, Literal, PlanError, PlanExpr, UnaryOp};
    use std::collections::{HashMap, HashSet};

    fn make_column_map() -> HashMap<String, usize> {
        // Simulates: Scan { columns: [0, 1, 2] } for users(id, name, age)
        // So id → 0, name → 1, age → 2 in scan output
        let mut map = HashMap::new();
        map.insert("id".to_string(), 0);
        map.insert("name".to_string(), 1);
        map.insert("age".to_string(), 2);
        map
    }

    // ========================================================================
    // Expression Converter Tests
    // ========================================================================

    #[test]
    fn test_convert_integer_literal() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
            rowid_output_pos: None,
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
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "id".to_string(),
                },
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "name".to_string(),
                },
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
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
    // Join Resolver Tests
    // ========================================================================

    #[test]
    fn test_join_resolver() {
        // Build a JoinResolver with:
        //   left: columns [id, name, dept_id], alias "e"
        //   right: columns [id, name], alias "d"
        use std::collections::HashMap;

        let left_table = schema::Table {
            name: "employees".to_string(),
            rootpage: 1,
            columns: vec![
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "id".to_string(),
                },
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "name".to_string(),
                },
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "dept_id".to_string(),
                },
            ],
        };

        let right_table = schema::Table {
            name: "departments".to_string(),
            rootpage: 2,
            columns: vec![
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "id".to_string(),
                },
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
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
        use std::collections::HashMap;

        let left_table = schema::Table {
            name: "employees".to_string(),
            rootpage: 1,
            columns: vec![
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "id".to_string(),
                },
                schema::Column {
                    data_type: None,
                    default: None,
                    primary_key: false,
                    unique: false,
                    name: "dept_id".to_string(),
                },
            ],
        };

        let right_table = schema::Table {
            name: "departments".to_string(),
            rootpage: 2,
            columns: vec![schema::Column {
                data_type: None,
                default: None,
                primary_key: false,
                unique: false,
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
}
