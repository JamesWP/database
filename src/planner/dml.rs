//! DML planning: INSERT, UPDATE, DELETE.

use crate::frontend::ast::{self, DataType};
use crate::storage::BTree;

use schema::resolve_table;

use super::resolver::{convert_expr, eval_constant, NoColumnResolver, SingleTableResolver};
use super::select::output_width;
use super::{schema, IndexMaintenanceInfo, Literal, LogicalPlan, PlanError, PlanExpr};

pub(super) fn plan_insert(
    insert: ast::InsertStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&insert.table_name, catalog)?;
    let num_table_columns = table.columns.len();

    // Find the autoincrement column index, if any
    let autoincrement_col_idx = table
        .columns
        .iter()
        .position(|c| c.autoincrement && c.primary_key);

    // Determine which columns we're inserting into
    let (table_columns, fill_autoincrement_at): (Vec<usize>, Option<usize>) = match &insert.columns
    {
        Some(col_names) => {
            let cols = col_names
                .iter()
                .map(|name| {
                    table
                        .get_column_index(name)
                        .ok_or_else(|| PlanError::ColumnNotFound {
                            table: insert.table_name.clone(),
                            column: name.clone(),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let fill = autoincrement_col_idx.and_then(|pk_idx| {
                if !cols.contains(&pk_idx) {
                    Some(pk_idx)
                } else {
                    None
                }
            });
            (cols, fill)
        }
        None => {
            // No column list: if VALUES count will be one short and there's an
            // autoincrement column, treat the positional values as non-PK columns.
            // The actual count check is deferred to the Values loop below; here we
            // just set up the column list to omit the PK column when appropriate.
            // We detect the "one short" case after seeing the first value row, but
            // we must build table_columns now. We peek at the first row length.
            let first_row_len = match &insert.source {
                ast::InsertSource::Values(rows) => rows.first().map(|r| r.len()),
                ast::InsertSource::Query(_) => None,
            };
            if let (Some(pk_idx), Some(len)) = (autoincrement_col_idx, first_row_len) {
                if len == num_table_columns - 1 {
                    // User provided all non-PK columns in order; synthesise the
                    // column list as all columns except the PK column.
                    let cols: Vec<usize> =
                        (0..num_table_columns).filter(|&i| i != pk_idx).collect();
                    (cols, Some(pk_idx))
                } else {
                    ((0..num_table_columns).collect(), None)
                }
            } else {
                ((0..num_table_columns).collect(), None)
            }
        }
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
                let provided: Vec<(usize, Literal)> = value_row
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| {
                        let plan_expr = convert_expr(expr, &no_resolver)?;
                        let lit = eval_constant(&plan_expr)?;
                        let target_type = table.columns[table_columns[i]].data_type.as_ref();
                        let lit = coerce_literal(lit, target_type)?;
                        Ok((table_columns[i], lit))
                    })
                    .collect::<Result<Vec<_>, PlanError>>()?;
                let full_row = make_full_row(&provided, &table.columns);
                rows.push(full_row);
            }
            LogicalPlan::Values { rows }
        }
        ast::InsertSource::Query(select) => {
            let input = super::plan_select(*select, catalog)?;
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
    let index_infos = catalog
        .catalog()
        .lookup_indexes_for_table(&insert.table_name);
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
            unique: index_sql_is_unique(&index_info.sql),
        });
    }

    Ok(LogicalPlan::Insert {
        rootpage: table.rootpage,
        table_columns: (0..num_table_columns).collect(),
        input: Box::new(input_plan),
        indexes,
        fill_autoincrement_at,
    })
}

pub(super) fn plan_update(
    update: ast::UpdateStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&update.table_name, catalog)?;

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
    let index_infos = catalog
        .catalog()
        .lookup_indexes_for_table(&update.table_name);
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
            unique: index_sql_is_unique(&index_info.sql),
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
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&delete.table_name, catalog)?;

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
    let index_infos = catalog
        .catalog()
        .lookup_indexes_for_table(&delete.table_name);
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
            unique: index_sql_is_unique(&index_info.sql),
        });
    }

    Ok(LogicalPlan::Delete {
        rootpage: table.rootpage,
        table_columns,
        filter,
        indexes,
    })
}

/// Convert a `DefaultValue` from the AST to a `Literal` for use in INSERT.
fn default_value_to_literal(dv: &ast::DefaultValue) -> Literal {
    match dv {
        ast::DefaultValue::Null => Literal::Null,
        ast::DefaultValue::Integer(n) => Literal::Integer(*n),
        ast::DefaultValue::Float(f) => Literal::Float(*f),
        ast::DefaultValue::Text(s) => Literal::String(s.clone()),
    }
}

/// Expand a partial set of `(column_index, value)` pairs into a full row, filling omitted
/// columns with their DEFAULT value or NULL.
fn make_full_row(provided: &[(usize, Literal)], all_columns: &[schema::Column]) -> Vec<Literal> {
    let mut row: Vec<Literal> = all_columns
        .iter()
        .map(|col| {
            col.default
                .as_ref()
                .map(default_value_to_literal)
                // TODO(phase-aj): reject NOT NULL columns with no default
                .unwrap_or(Literal::Null)
        })
        .collect();
    for (col_idx, lit) in provided {
        row[*col_idx] = lit.clone();
    }
    row
}

/// Coerce a literal value to the target column type for INSERT.
fn coerce_literal(lit: Literal, target_type: Option<&DataType>) -> Result<Literal, PlanError> {
    match (lit, target_type) {
        (Literal::String(s), Some(DataType::Integer)) => s
            .parse::<i64>()
            .map(Literal::Integer)
            .map_err(|_| PlanError::TypeMismatch {
                expected: "INTEGER".into(),
                got: s,
            }),
        (Literal::String(s), Some(DataType::Real)) => {
            s.parse::<f64>()
                .map(Literal::Float)
                .map_err(|_| PlanError::TypeMismatch {
                    expected: "REAL".into(),
                    got: s,
                })
        }
        (Literal::Integer(n), Some(DataType::Real)) => Ok(Literal::Float(n as f64)),
        (other, _) => Ok(other),
    }
}

/// Returns true if the index DDL SQL represents a unique index.
/// The DDL is `CREATE UNIQUE INDEX ...` for unique indexes and `CREATE INDEX ...` otherwise.
fn index_sql_is_unique(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    // Match "CREATE UNIQUE INDEX" (with any whitespace between tokens)
    let after_create = upper
        .trim_start()
        .strip_prefix("CREATE")
        .unwrap_or("")
        .trim_start();
    after_create.starts_with("UNIQUE")
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
            fill_autoincrement_at: None,
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_with_columns() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users (age, name) VALUES (30, 'alice')");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        // After item 114: omitted columns are filled with NULL; table_columns is always full-width.
        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![0, 1, 2], // all columns
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Null,                        // id (omitted, no default)
                    Literal::String("alice".to_string()), // name
                    Literal::Integer(30),                 // age
                ]],
            }),
            indexes: vec![],
            fill_autoincrement_at: None,
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
            fill_autoincrement_at: None,
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
        let mut test = TestDb::default();

        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_age", "users", index_root, sql_index);

        let stmt = parse_sql("DELETE FROM users WHERE id = 1");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

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
        let mut test = TestDb::default();

        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = test.btree.create_tree();
        test.btree
            .insert_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = test.btree.create_tree();
        test.btree
            .insert_entry("index", "idx_age", "users", index_root, sql_index);

        let stmt = parse_sql("UPDATE users SET age = 30 WHERE id = 1");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

        if let LogicalPlan::Update { indexes, .. } = plan {
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].column_idxs, vec![1]); // age is column index 1
            assert_eq!(indexes[0].rootpage, index_root);
        } else {
            panic!("Expected Update plan");
        }
    }
}
