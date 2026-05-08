//! DML planning: INSERT, UPDATE, DELETE.

use crate::frontend::ast::{self, DataType};
use crate::storage::BTree;

use schema::resolve_table;

use super::resolver::{convert_expr, eval_constant, NoColumnResolver, SingleTableResolver};
use super::select::output_width;
use super::{
    schema, to_scan_index, IndexMaintenanceInfo, Literal, LogicalPlan, PlanError, PlanExpr,
};

pub(super) fn plan_insert(
    insert: ast::InsertStatement,
    catalog: &BTree,
) -> Result<LogicalPlan, PlanError> {
    let table = resolve_table(&insert.table_name, catalog)?;
    let num_table_columns = table.columns.len();
    let rowid_col = table.rowid_column();

    // user_to_schema: user-column-position → schema-column-index
    let user_to_schema: Vec<usize> = match &insert.columns {
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

    // For rowid-alias tables: is the PK column in the user-provided column list?
    // If not, it should be auto-assigned and excluded from insert-space index 0.
    let pk_in_user_cols = rowid_col.map_or(false, |pk| user_to_schema.contains(&pk));

    // Build the input plan from the INSERT source
    let (input_plan, insert_table_cols) = match insert.source {
        ast::InsertSource::Values(value_rows) => {
            let no_resolver = NoColumnResolver;
            let mut rows = Vec::new();
            for value_row in &value_rows {
                if value_row.len() != user_to_schema.len() {
                    return Err(PlanError::ColumnCountMismatch {
                        expected: user_to_schema.len(),
                        got: value_row.len(),
                    });
                }
                let provided: Vec<(usize, Literal)> = value_row
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| {
                        let plan_expr = convert_expr(expr, &no_resolver)?;
                        let lit = eval_constant(&plan_expr)?;
                        let target_type = table.columns[user_to_schema[i]].data_type.as_ref();
                        let lit = coerce_literal(lit, target_type)?;
                        Ok((user_to_schema[i], lit))
                    })
                    .collect::<Result<Vec<_>, PlanError>>()?;

                // make_full_row produces values in schema order (0..N).
                let full_schema_row = make_full_row(&provided, &table.columns);

                // Build the Values row in schema order, excluding the rowid-alias PK column
                // when it was not explicitly provided (auto-assign case).
                let values_row: Vec<Literal> = (0..num_table_columns)
                    .filter(|&sc| should_include_schema_col(sc, rowid_col, pk_in_user_cols))
                    .map(|sc| full_schema_row[sc].clone())
                    .collect();
                rows.push(values_row);
            }

            // table_cols for Values rows: schema-col position in the row → insert-space index.
            // Values rows are in schema order with the PK column omitted when not provided.
            let cols: Vec<usize> = (0..num_table_columns)
                .filter(|&sc| should_include_schema_col(sc, rowid_col, pk_in_user_cols))
                .map(|sc| to_scan_index(sc, rowid_col))
                .collect();

            (LogicalPlan::Values { rows }, cols)
        }
        ast::InsertSource::Query(select) => {
            let input = super::plan_select(*select, catalog)?;
            let produced = output_width(&input);
            let expected = user_to_schema
                .iter()
                .filter(|&&sc| should_include_schema_col(sc, rowid_col, pk_in_user_cols))
                .count();
            if produced != expected {
                return Err(PlanError::ColumnCountMismatch {
                    expected,
                    got: produced,
                });
            }
            // Query output is in user-specified order; map each user col to insert-space.
            let cols: Vec<usize> = user_to_schema
                .iter()
                .filter(|&&sc| should_include_schema_col(sc, rowid_col, pk_in_user_cols))
                .map(|&sc| to_scan_index(sc, rowid_col))
                .collect();
            (input, cols)
        }
    };

    // Look up indexes for this table.
    // column_idxs are stored in insert-space (0=key, k>0=CBOR[k-1]) so that
    // codegen_insert can index directly into the reordered insert-space register array.
    let index_infos = catalog
        .catalog()
        .lookup_indexes_for_table(&insert.table_name);
    let mut indexes = Vec::new();
    for index_info in index_infos {
        let column_idxs = index_info
            .column_names
            .iter()
            .map(|name| {
                let schema_col = table
                    .columns
                    .iter()
                    .position(|col| &col.name == name)
                    .expect("Index column not found in table");
                to_scan_index(schema_col, rowid_col)
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
        table_columns: insert_table_cols,
        input: Box::new(input_plan),
        indexes,
    })
}

/// Whether to include a schema column in the insert-space Values row.
/// For rowid-alias tables, the PK column is excluded when not explicitly provided
/// (it will be auto-assigned from the rowid cache).
fn should_include_schema_col(
    schema_col: usize,
    rowid_col: Option<usize>,
    pk_in_user_cols: bool,
) -> bool {
    match rowid_col {
        Some(pk) if schema_col == pk => pk_in_user_cols,
        _ => true,
    }
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
        rowid_output_pos: None,
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
        rowid_output_pos: None,
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
            table_columns: vec![1, 2, 3],
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

        // After item 114: omitted columns are filled with NULL; table_columns is always full-width.
        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![1, 2, 3], // all columns
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Null,                        // id (omitted, no default)
                    Literal::String("alice".to_string()), // name
                    Literal::Integer(30),                 // age
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
            table_columns: vec![1, 2, 3],
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
