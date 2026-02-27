//! DML planning: INSERT, UPDATE, DELETE.

use crate::frontend::ast;
use crate::storage::BTree;

use schema::resolve_table;

use super::resolver::{convert_expr, eval_constant, NoColumnResolver, SingleTableResolver};
use super::select::output_width;
use super::{schema, IndexMaintenanceInfo, Literal, LogicalPlan, PlanError, PlanExpr};

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use crate::frontend::{ast, parse};
    use crate::planner::{plan, Literal, LogicalPlan, PlanError};
    use crate::test::TestDb;

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

    // ========================================================================
    // DELETE Plan Tests
    // ========================================================================

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

    // ========================================================================
    // UPDATE Plan Tests
    // ========================================================================

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
}
