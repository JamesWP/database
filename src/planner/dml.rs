//! DML planning: INSERT, UPDATE, DELETE.

use crate::frontend::ast;
use crate::storage::BTree;

use schema::resolve_table;

use super::{
    schema, IndexMaintenanceInfo, Literal, LogicalPlan, PlanError, PlanExpr,
};
use super::resolver::{convert_expr, eval_constant, NoColumnResolver, SingleTableResolver};

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

pub(super) fn plan_insert(
    insert: ast::InsertStatement,
    btree: &BTree,
) -> Result<LogicalPlan, PlanError> {
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
            let input = super::plan_select(*select, btree)?;
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

pub(super) fn plan_update(
    update: ast::UpdateStatement,
    btree: &BTree,
) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&update.table_name, btree)?;

    // Build column mapping: all table columns in order
    let column_map = table.column_name_map();

    // Create column resolver
    let resolver = SingleTableResolver {
        table_ref: &update.table_name,
        columns: &column_map,
    };

    // Resolve assignment column names to indices and plan their expressions
    let mut assignments: Vec<(usize, PlanExpr)> = Vec::new();
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

pub(super) fn plan_delete(
    delete: ast::DeleteStatement,
    btree: &BTree,
) -> Result<LogicalPlan, PlanError> {
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
