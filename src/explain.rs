//! EXPLAIN query plan formatter.
//!
//! Walks a `LogicalPlan` tree and produces a list of `(id, indented_text)` rows
//! suitable for returning as a two-column query result.
//!
//! Column references in expressions are rendered as `name:index` (e.g. `price:2`)
//! when the column name can be resolved from the schema context flowing up from
//! leaf Scan/RowidLookup nodes.

use std::collections::HashMap;

use crate::planner::{from_scan_index, BinaryOp, Literal, LogicalPlan, PlanExpr, SortKey, UnaryOp};

// ============================================================================
// Schema metadata (for name resolution)
// ============================================================================

pub struct TableMeta {
    pub name: String,
    pub columns: Vec<String>,     // ordered by schema column index
    pub rowid_col: Option<usize>, // schema index of INTEGER PRIMARY KEY column, if any
}

pub struct IndexMeta {
    pub name: String,
    #[allow(dead_code)]
    pub table_name: String,
    pub column_names: Vec<String>,
}

#[derive(Default)]
pub struct ExplainSchema {
    pub tables: HashMap<u32, TableMeta>,  // rootpage → meta
    pub indexes: HashMap<u32, IndexMeta>, // rootpage → meta
}

impl ExplainSchema {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn table_name(&self, rootpage: u32) -> String {
        self.tables
            .get(&rootpage)
            .map(|m| m.name.clone())
            .unwrap_or_else(|| format!("table@{rootpage}"))
    }

    /// Return the column name for a scan-space index at the given table rootpage.
    pub fn column_name(&self, rootpage: u32, scan_idx: usize) -> String {
        self.tables
            .get(&rootpage)
            .and_then(|m| {
                let schema_idx = from_scan_index(scan_idx, m.rowid_col)?;
                m.columns.get(schema_idx)
            })
            .cloned()
            .unwrap_or_else(|| {
                if scan_idx == 0 {
                    "rowid".to_string()
                } else {
                    format!("col:{scan_idx}")
                }
            })
    }

    pub fn index_name(&self, rootpage: u32) -> String {
        self.indexes
            .get(&rootpage)
            .map(|m| m.name.clone())
            .unwrap_or_else(|| format!("index@{rootpage}"))
    }

    /// Return the first indexed column name for this index rootpage.
    pub fn index_col_name(&self, rootpage: u32) -> String {
        self.indexes
            .get(&rootpage)
            .and_then(|m| m.column_names.first())
            .cloned()
            .unwrap_or_else(|| format!("col@{rootpage}"))
    }

    /// Return all indexed column names for this index rootpage.
    pub fn index_col_names(&self, rootpage: u32) -> Vec<String> {
        self.indexes
            .get(&rootpage)
            .map(|m| m.column_names.clone())
            .unwrap_or_default()
    }
}

// ============================================================================
// Column context propagation
// ============================================================================

/// Compute the ordered list of column names produced by `plan`.
/// Used to resolve `ColumnRef` indices in parent nodes' expressions.
fn node_output_cols(plan: &LogicalPlan, schema: &ExplainSchema) -> Vec<String> {
    match plan {
        LogicalPlan::Scan {
            rootpage, columns, ..
        } => columns
            .iter()
            .map(|&i| schema.column_name(*rootpage, i))
            .collect(),
        LogicalPlan::RowidLookup {
            table_rootpage,
            columns,
            ..
        } => columns
            .iter()
            .map(|&i| schema.column_name(*table_rootpage, i))
            .collect(),
        // Pass-through nodes: same columns as input
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Count { input }
        | LogicalPlan::Distinct { input } => node_output_cols(input, schema),
        LogicalPlan::Project { input, columns } => {
            let input_cols = node_output_cols(input, schema);
            columns
                .iter()
                .map(|expr| format_expr_with_names(expr, &input_cols))
                .collect()
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            ..
        } => {
            let input_cols = node_output_cols(input, schema);
            let mut out: Vec<String> = group_keys
                .iter()
                .map(|e| format_expr_with_names(e, &input_cols))
                .collect();
            for _ in aggregates {
                out.push("agg".to_string());
            }
            out
        }
        LogicalPlan::Join { left, right, .. } => {
            let mut cols = node_output_cols(left, schema);
            cols.extend(node_output_cols(right, schema));
            cols
        }
        LogicalPlan::IndexScan {
            index_rootpage,
            output_columns,
            ..
        } => match output_columns {
            None => vec!["rowid".to_string()],
            Some(cols) => {
                let all_names = schema.index_col_names(*index_rootpage);
                cols.iter()
                    .map(|&j| {
                        all_names
                            .get(j)
                            .cloned()
                            .unwrap_or_else(|| format!("col:{j}"))
                    })
                    .collect()
            }
        },
        LogicalPlan::IndexProbe { .. } => {
            vec!["rowid".to_string()]
        }
        LogicalPlan::Values { rows } => rows
            .first()
            .map(|r| (0..r.len()).map(|i| format!("col:{i}")).collect())
            .unwrap_or_default(),
        LogicalPlan::Sequence { .. } => vec!["n".to_string()],
        LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::PopulateIndex { .. } => vec!["count".to_string()],
        LogicalPlan::Materialize { input } => node_output_cols(input, schema),
    }
}

// ============================================================================
// Plan formatting
// ============================================================================

/// Walk `plan` in DFS pre-order and return `(id, indented_text)` rows.
pub fn format_plan(plan: &LogicalPlan, schema: &ExplainSchema) -> Vec<(u32, String)> {
    let mut rows = Vec::new();
    let mut counter = 0u32;
    collect_rows(plan, schema, 0, &mut counter, &mut rows);
    rows
}

fn collect_rows(
    plan: &LogicalPlan,
    schema: &ExplainSchema,
    depth: usize,
    counter: &mut u32,
    rows: &mut Vec<(u32, String)>,
) {
    let id = *counter;
    *counter += 1;
    let indent = "  ".repeat(depth);

    // Compute the input column names (what the child/children produce).
    // These are used to resolve ColumnRef indices in this node's expressions.
    let input_cols: Vec<String> = match plan {
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Count { input }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Insert { input, .. }
        | LogicalPlan::RowidLookup { input, .. }
        | LogicalPlan::PopulateIndex { input, .. }
        | LogicalPlan::Materialize { input } => node_output_cols(input, schema),
        LogicalPlan::Join { left, right, .. } => {
            let mut cols = node_output_cols(left, schema);
            cols.extend(node_output_cols(right, schema));
            cols
        }
        _ => vec![],
    };

    let summary = match plan {
        LogicalPlan::Scan {
            rootpage, columns, ..
        } => {
            let table = schema.table_name(*rootpage);
            let cols = resolve_cols(schema, *rootpage, columns);
            format!("{indent}Scan {table} [cols: {cols}]")
        }
        LogicalPlan::IndexScan {
            index_rootpage,
            index_col_idx: _,
            lower_bound,
            upper_bound,
            output_columns,
        } => {
            let index = schema.index_name(*index_rootpage);
            let pred = format_index_predicate(lower_bound, upper_bound);
            match output_columns {
                None => format!("{indent}IndexScan via {index} [{pred}]"),
                Some(cols) => {
                    let names: Vec<String> = cols
                        .iter()
                        .map(|&j| {
                            schema
                                .index_col_names(*index_rootpage)
                                .get(j)
                                .cloned()
                                .unwrap_or_else(|| format!("col:{j}"))
                        })
                        .collect();
                    format!(
                        "{indent}IndexScan via {index} [{pred}] [{}]",
                        names.join(", ")
                    )
                }
            }
        }
        LogicalPlan::RowidLookup {
            table_rootpage,
            columns,
            ..
        } => {
            let table = schema.table_name(*table_rootpage);
            let cols = resolve_cols(schema, *table_rootpage, columns);
            format!("{indent}RowidLookup {table} [cols: {cols}]")
        }
        LogicalPlan::IndexProbe {
            index_rootpage,
            key_expr,
            index_col_idx: _,
        } => {
            let index_col = schema.index_col_name(*index_rootpage);
            format!(
                "{indent}IndexProbe [index_col={index_col}, key={}]",
                format_expr_with_names(key_expr, &input_cols)
            )
        }
        LogicalPlan::Filter { predicate, .. } => {
            format!(
                "{indent}Filter [{}]",
                format_expr_with_names(predicate, &input_cols)
            )
        }
        LogicalPlan::Project { columns, .. } => {
            let exprs: Vec<_> = columns
                .iter()
                .map(|e| format_expr_with_names(e, &input_cols))
                .collect();
            format!("{indent}Project [{}]", exprs.join(", "))
        }
        LogicalPlan::Limit { count, .. } => format!("{indent}Limit [{count}]"),
        LogicalPlan::Sort { sort_keys, .. } => {
            let keys: Vec<_> = sort_keys
                .iter()
                .map(|k| format_sort_key_with_names(k, &input_cols))
                .collect();
            format!("{indent}Sort [{}]", keys.join(", "))
        }
        LogicalPlan::Count { .. } => format!("{indent}Count"),
        LogicalPlan::Aggregate {
            group_keys,
            aggregates,
            ..
        } => {
            format!(
                "{indent}Aggregate [groups: {}, aggs: {}]",
                group_keys.len(),
                aggregates.len()
            )
        }
        LogicalPlan::Join {
            on_condition,
            strategy,
            ..
        } => {
            let strategy_label = match strategy {
                crate::planner::JoinStrategy::Hash => "Hash",
                crate::planner::JoinStrategy::NestedLoop => "NestedLoop",
            };
            format!(
                "{indent}Join [{} | {}]",
                strategy_label,
                format_expr_with_names(on_condition, &input_cols)
            )
        }
        LogicalPlan::Distinct { .. } => format!("{indent}Distinct"),
        LogicalPlan::Insert { rootpage, .. } => {
            format!("{indent}Insert [{}]", schema.table_name(*rootpage))
        }
        LogicalPlan::Update { rootpage, .. } => {
            format!("{indent}Update [{}]", schema.table_name(*rootpage))
        }
        LogicalPlan::Delete { rootpage, .. } => {
            format!("{indent}Delete [{}]", schema.table_name(*rootpage))
        }
        LogicalPlan::Values { rows: r } => format!("{indent}Values [{} rows]", r.len()),
        LogicalPlan::PopulateIndex { index_rootpage, .. } => {
            format!(
                "{indent}PopulateIndex [{}]",
                schema.index_name(*index_rootpage)
            )
        }
        LogicalPlan::Sequence { start, end } => {
            format!("{indent}Sequence [{start}..{end})")
        }
        LogicalPlan::Materialize { .. } => format!("{indent}Materialize"),
    };

    rows.push((id, summary));

    for child in plan_children(plan) {
        collect_rows(child, schema, depth + 1, counter, rows);
    }
}

fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match plan {
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Count { input }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Insert { input, .. }
        | LogicalPlan::RowidLookup { input, .. }
        | LogicalPlan::PopulateIndex { input, .. }
        | LogicalPlan::Materialize { input } => vec![input],
        LogicalPlan::Join { left, right, .. } => vec![left, right],
        _ => vec![],
    }
}

// ============================================================================
// Expression and predicate formatting
// ============================================================================

fn resolve_cols(schema: &ExplainSchema, rootpage: u32, columns: &[usize]) -> String {
    columns
        .iter()
        .map(|&i| schema.column_name(rootpage, i))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format an expression, resolving column indices to `name:index` using `col_names`.
/// Falls back to `col:index` when the name isn't available.
pub fn format_expr_with_names(expr: &PlanExpr, col_names: &[String]) -> String {
    match expr {
        PlanExpr::ColumnRef(idx) => {
            if let Some(name) = col_names.get(*idx) {
                format!("{name}:{idx}")
            } else {
                format!("col:{idx}")
            }
        }
        PlanExpr::Literal(lit) => format_literal(lit),
        PlanExpr::BinaryOp { op, left, right } => {
            format!(
                "{} {} {}",
                format_expr_with_names(left, col_names),
                format_binary_op(op),
                format_expr_with_names(right, col_names)
            )
        }
        PlanExpr::UnaryOp { op, operand } => {
            format!(
                "{}{}",
                format_unary_op(op),
                format_expr_with_names(operand, col_names)
            )
        }
        PlanExpr::FunctionCall { name, args } => {
            let a: Vec<_> = args
                .iter()
                .map(|e| format_expr_with_names(e, col_names))
                .collect();
            format!("{name}({})", a.join(", "))
        }
    }
}

/// Format an expression without column name context (falls back to `col:index`).
pub fn format_expr(expr: &PlanExpr) -> String {
    format_expr_with_names(expr, &[])
}

fn format_literal(lit: &Literal) -> String {
    match lit {
        Literal::Integer(n) => n.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::String(s) => format!("'{s}'"),
        Literal::Bool(b) => b.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn format_binary_op(op: &BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Subtract => "-",
        BinaryOp::Multiply => "*",
        BinaryOp::Divide => "/",
        BinaryOp::Remainder => "%",
        BinaryOp::Equals => "=",
        BinaryOp::NotEquals => "!=",
        BinaryOp::GreaterThan => ">",
        BinaryOp::GreaterThanOrEqual => ">=",
        BinaryOp::LessThan => "<",
        BinaryOp::LessThanOrEqual => "<=",
        BinaryOp::Like => "LIKE",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::LeftShift => "<<",
        BinaryOp::RightShift => ">>",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "^",
        BinaryOp::BitAnd => "&",
    }
}

fn format_unary_op(op: &UnaryOp) -> &'static str {
    match op {
        UnaryOp::Plus => "+",
        UnaryOp::Negate => "-",
        UnaryOp::Not => "NOT ",
        UnaryOp::IsNull => "IS NULL ",
        UnaryOp::IsNotNull => "IS NOT NULL ",
    }
}

fn format_sort_key_with_names(key: &SortKey, col_names: &[String]) -> String {
    let dir = if key.descending { "DESC" } else { "ASC" };
    format!("{} {dir}", format_expr_with_names(&key.expr, col_names))
}

fn format_index_predicate(
    lower: &Option<(Literal, bool)>,
    upper: &Option<(Literal, bool)>,
) -> String {
    match (lower, upper) {
        (Some((lo, true)), Some((hi, true))) if lo == hi => {
            format!("= {}", format_literal(lo))
        }
        (Some((lo, lo_inc)), Some((hi, hi_inc))) => {
            let lo_op = if *lo_inc { ">=" } else { ">" };
            let hi_op = if *hi_inc { "<=" } else { "<" };
            format!(
                "{lo_op} {} AND {hi_op} {}",
                format_literal(lo),
                format_literal(hi)
            )
        }
        (Some((lo, inc)), None) => {
            let op = if *inc { ">=" } else { ">" };
            format!("{op} {}", format_literal(lo))
        }
        (None, Some((hi, inc))) => {
            let op = if *inc { "<=" } else { "<" };
            format!("{op} {}", format_literal(hi))
        }
        (None, None) => "full scan".to_string(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{BinaryOp, Literal, LogicalPlan, PlanExpr};

    #[test]
    fn test_explain_scan_only() {
        let plan = LogicalPlan::Scan {
            rootpage: 1,
            columns: vec![0, 1],
        };
        let rows = format_plan(&plan, &ExplainSchema::empty());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 0);
        assert!(rows[0].1.contains("Scan"), "got: {}", rows[0].1);
    }

    #[test]
    fn test_explain_filter_scan_depth() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                rootpage: 1,
                columns: vec![0],
            }),
            predicate: PlanExpr::Literal(Literal::Integer(1)),
        };
        let rows = format_plan(&plan, &ExplainSchema::empty());
        assert_eq!(rows.len(), 2);
        assert!(rows[0].1.starts_with("Filter"), "got: {}", rows[0].1);
        assert!(rows[1].1.starts_with("  Scan"), "got: {}", rows[1].1);
    }

    #[test]
    fn test_explain_join_two_children() {
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                rootpage: 1,
                columns: vec![0],
            }),
            right: Box::new(LogicalPlan::Scan {
                rootpage: 2,
                columns: vec![0],
            }),
            on_condition: PlanExpr::Literal(Literal::Integer(1)),
            strategy: crate::planner::JoinStrategy::Hash,
            left_column_count: 1,
        };
        let rows = format_plan(&plan, &ExplainSchema::empty());
        assert_eq!(rows.len(), 3);
        assert!(rows[0].1.starts_with("Join"), "got: {}", rows[0].1);
        assert!(rows[1].1.starts_with("  Scan"), "got: {}", rows[1].1);
        assert!(rows[2].1.starts_with("  Scan"), "got: {}", rows[2].1);
    }

    #[test]
    fn test_explain_index_scan_equality() {
        let plan = LogicalPlan::IndexScan {
            index_rootpage: 5,
            index_col_idx: 0,
            lower_bound: Some((Literal::Integer(30), true)),
            upper_bound: Some((Literal::Integer(30), true)),
            output_columns: None,
        };
        let rows = format_plan(&plan, &ExplainSchema::empty());
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.contains("IndexScan"), "got: {}", rows[0].1);
        assert!(rows[0].1.contains("= 30"), "got: {}", rows[0].1);
    }

    #[test]
    fn test_explain_schema_name_resolution() {
        let mut schema = ExplainSchema::empty();
        schema.tables.insert(
            1,
            TableMeta {
                name: "users".to_string(),
                rowid_col: None,
                columns: vec!["id".to_string(), "age".to_string()],
            },
        );
        // No PK: scan-space 1→id, 2→age (scan-space 0 is hidden rowid)
        let plan = LogicalPlan::Scan {
            rootpage: 1,
            columns: vec![1, 2],
        };
        let rows = format_plan(&plan, &schema);
        assert!(rows[0].1.contains("Scan users"), "got: {}", rows[0].1);
        assert!(rows[0].1.contains("id, age"), "got: {}", rows[0].1);
    }

    #[test]
    fn test_column_names_in_filter_expr() {
        // Filter on a scan: col refs in predicate should resolve to names
        let mut schema = ExplainSchema::empty();
        schema.tables.insert(
            1,
            TableMeta {
                name: "users".to_string(),
                rowid_col: None,
                columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            },
        );
        // No PK: scan-space 1→id, 2→name, 3→age (output positions 0,1,2)
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                rootpage: 1,
                columns: vec![1, 2, 3],
            }),
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::Equals,
                left: Box::new(PlanExpr::ColumnRef(2)), // output pos 2 = "age"
                right: Box::new(PlanExpr::Literal(Literal::Integer(30))),
            },
        };
        let rows = format_plan(&plan, &schema);
        // Filter row should show "age:2 = 30", not "col:2 = 30"
        assert!(rows[0].1.contains("age:2 = 30"), "got: {}", rows[0].1);
    }

    #[test]
    fn test_column_names_in_project_expr() {
        let mut schema = ExplainSchema::empty();
        schema.tables.insert(
            1,
            TableMeta {
                name: "users".to_string(),
                rowid_col: None,
                columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            },
        );
        // No PK: scan-space 1→id, 2→name, 3→age (output positions 0,1,2)
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: 1,
                columns: vec![1, 2, 3],
            }),
            columns: vec![PlanExpr::ColumnRef(0), PlanExpr::ColumnRef(1)],
        };
        let rows = format_plan(&plan, &schema);
        // Project row should show "id:0, name:1"
        assert!(rows[0].1.contains("id:0"), "got: {}", rows[0].1);
        assert!(rows[0].1.contains("name:1"), "got: {}", rows[0].1);
    }

    #[test]
    fn test_format_expr_no_context_falls_back() {
        let expr = PlanExpr::BinaryOp {
            op: BinaryOp::Equals,
            left: Box::new(PlanExpr::ColumnRef(2)),
            right: Box::new(PlanExpr::Literal(Literal::Integer(30))),
        };
        // Without context, falls back to col:N
        assert_eq!(format_expr(&expr), "col:2 = 30");
    }

    #[test]
    fn test_explain_covering_index_scan() {
        let mut schema = ExplainSchema::empty();
        schema.indexes.insert(
            5,
            IndexMeta {
                name: "idx_age".to_string(),
                table_name: "users".to_string(),
                column_names: vec!["age".to_string()],
            },
        );
        let plan = LogicalPlan::IndexScan {
            index_rootpage: 5,
            index_col_idx: 2,
            lower_bound: Some((Literal::Integer(30), true)),
            upper_bound: Some((Literal::Integer(30), true)),
            output_columns: Some(vec![0]), // covering: output index col 0 (age)
        };
        let rows = format_plan(&plan, &schema);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.contains("idx_age"), "got: {}", rows[0].1);
        assert!(rows[0].1.contains("= 30"), "got: {}", rows[0].1);
        assert!(rows[0].1.contains("[age]"), "got: {}", rows[0].1);
    }

    #[test]
    fn test_explain_non_covering_index_scan_unchanged() {
        let plan = LogicalPlan::IndexScan {
            index_rootpage: 5,
            index_col_idx: 0,
            lower_bound: Some((Literal::Integer(30), true)),
            upper_bound: Some((Literal::Integer(30), true)),
            output_columns: None,
        };
        let rows = format_plan(&plan, &ExplainSchema::empty());
        assert!(rows[0].1.contains("IndexScan"), "got: {}", rows[0].1);
        // No column list appended for non-covering scan
        assert!(
            !rows[0].1.contains("[age]") && !rows[0].1.contains("[col:"),
            "should have no column list: {}",
            rows[0].1
        );
    }
}
