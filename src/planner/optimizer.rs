//! Query optimizer: rewrites a naive LogicalPlan tree to use indexes and elide redundant sorts.

use crate::frontend::ast;
use crate::storage::BTree;

use super::{schema, Literal, LogicalPlan};

/// Apply optimization rules to a naive LogicalPlan.
///
/// Currently a pass-through; rules are implemented in item 93.2.
pub(super) fn optimize(plan: LogicalPlan, _btree: &BTree) -> LogicalPlan {
    plan
}

/// Try to replace a Scan+Filter with an IndexScan+RowidLookup when a matching index exists.
pub(super) fn try_index_scan(
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
/// `sort_col_idx` with only pass-through nodes in between — meaning the scan
/// already produces rows in ascending order by that column.
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
