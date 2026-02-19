use crate::compiler;
use crate::engine::scalarvalue::ScalarValue;
use crate::engine::Engine;
use crate::frontend::ast::{DataType, Statement};
use crate::frontend::{parse, ParseError};
use crate::planner::{self, PlanError};
use crate::storage;
use crate::storage::{decode_u64_key, encode_integer_key, BTree};

#[derive(Debug)]
pub enum ExecuteResult {
    CreateTable { table_name: String },
    CreateIndex { index_name: String },
    DropTable { table_name: String },
    Query(QueryExecution),
}

#[derive(Debug)]
pub struct QueryExecution {
    rows: Vec<Vec<ScalarValue>>,
    pos: usize,
}

impl QueryExecution {
    fn new(mut engine: Engine) -> Self {
        let rows = engine.run();
        QueryExecution { rows, pos: 0 }
    }

    pub fn next(&mut self) -> Option<Vec<ScalarValue>> {
        if self.pos < self.rows.len() {
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum ExecuteError {
    Parse(ParseError),
    Plan(PlanError),
    TableAlreadyExists(String),
    TableNotFound(String),
    IndexAlreadyExists(String),
    ColumnNotFound { table: String, column: String },
    ColumnNotInteger { table: String, column: String },
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::Parse(e) => write!(f, "Parse error: {:?}", e),
            ExecuteError::Plan(e) => write!(f, "Planning error: {:?}", e),
            ExecuteError::TableAlreadyExists(name) => {
                write!(f, "Table '{}' already exists", name)
            }
            ExecuteError::TableNotFound(name) => {
                write!(f, "Table '{}' not found", name)
            }
            ExecuteError::IndexAlreadyExists(name) => {
                write!(f, "Index '{}' already exists", name)
            }
            ExecuteError::ColumnNotFound { table, column } => {
                write!(f, "Column '{}' not found in table '{}'", column, table)
            }
            ExecuteError::ColumnNotInteger { table, column } => {
                write!(
                    f,
                    "Column '{}' in table '{}' is not INTEGER (V1: only INTEGER columns supported)",
                    column, table
                )
            }
        }
    }
}

pub fn execute(sql: &str, btree: &mut BTree) -> Result<ExecuteResult, ExecuteError> {
    let stmt = parse(sql).map_err(ExecuteError::Parse)?;

    match &stmt {
        Statement::CreateTable(_) => {
            let ct = match stmt {
                Statement::CreateTable(ct) => ct,
                _ => unreachable!(),
            };
            let name = &ct.table_name;

            // Check if table already exists
            if btree.lookup_table(name).is_some() {
                return Err(ExecuteError::TableAlreadyExists(name.clone()));
            }

            let root_page = btree.create_tree();
            let ddl = sql.to_string();
            btree.insert_schema_entry("table", name, name, root_page, &ddl);
            Ok(ExecuteResult::CreateTable {
                table_name: name.clone(),
            })
        }
        Statement::Drop(_) => {
            let drop = match stmt {
                Statement::Drop(d) => d,
                _ => unreachable!(),
            };
            let name = &drop.table_name;

            // Check if table exists
            if btree.lookup_table(name).is_none() {
                return Err(ExecuteError::TableNotFound(name.clone()));
            }

            // Delete the catalog entry (and associated indexes)
            btree.delete_schema_entries_for_table(name);

            Ok(ExecuteResult::DropTable {
                table_name: name.clone(),
            })
        }
        Statement::CreateIndex(_) => {
            let ci = match stmt {
                Statement::CreateIndex(ci) => ci,
                _ => unreachable!(),
            };

            // 1. Resolve table and validate it exists
            let (table_rootpage, ddl) = btree
                .lookup_table(&ci.table_name)
                .ok_or_else(|| ExecuteError::TableNotFound(ci.table_name.clone()))?;

            // 2. Check if index already exists
            let existing = btree.lookup_indexes_for_table(&ci.table_name);
            for index in &existing {
                if index.index_name == ci.index_name {
                    return Err(ExecuteError::IndexAlreadyExists(ci.index_name.clone()));
                }
            }

            // 3. Parse DDL to get column info
            let parsed_ddl = parse(&ddl).map_err(ExecuteError::Parse)?;
            let create_table = match parsed_ddl {
                Statement::CreateTable(ct) => ct,
                _ => {
                    return Err(ExecuteError::Parse(
                        crate::frontend::ParseError::UnexpectedToken(
                            crate::frontend::parser::Expect::Table,
                            crate::frontend::lexer::Type::Eof,
                        ),
                    ))
                }
            };

            // 4. Find column and verify it's INTEGER (V1 restriction)
            let column_def = create_table
                .columns
                .iter()
                .find(|col| col.name == ci.column_name)
                .ok_or_else(|| ExecuteError::ColumnNotFound {
                    table: ci.table_name.clone(),
                    column: ci.column_name.clone(),
                })?;

            if !matches!(column_def.type_name, Some(DataType::Integer)) {
                return Err(ExecuteError::ColumnNotInteger {
                    table: ci.table_name.clone(),
                    column: ci.column_name.clone(),
                });
            }

            let column_idx = create_table
                .columns
                .iter()
                .position(|col| col.name == ci.column_name)
                .unwrap();

            // 5. Create the index B-tree
            let index_rootpage = btree.create_tree();

            // 6. Scan table and collect (index_key, primary_key) pairs, then write to index.
            // We collect first to avoid holding readonly borrow while inserting (readwrite borrow).
            let entries_to_index: Vec<(Vec<u8>, i64)> = {
                let mut table_cursor = btree.open(table_rootpage);
                let mut tc = table_cursor.open_readonly();
                tc.first();
                let mut entries = Vec::new();
                loop {
                    match tc.get_entry() {
                        None => break,
                        Some(mut reader) => {
                            let table_key = decode_u64_key(reader.key());
                            let values = reader.decode_as_array();
                            if column_idx < values.len() {
                                if let ScalarValue::Integer(col_int) = values[column_idx] {
                                    entries.push((encode_integer_key(col_int), table_key as i64));
                                }
                            }
                        }
                    }
                    tc.next();
                }
                entries
            };

            for (index_key, pk) in entries_to_index {
                // Compose index key: [encoded column value] + [encoded rowid]
                let mut full_key = index_key;
                full_key.extend_from_slice(&storage::encode_u64_key(pk as u64));

                let index_value = vec![ScalarValue::Integer(pk)];
                let mut encoded = Vec::new();
                ciborium::ser::into_writer(&index_value, &mut encoded).unwrap();
                let mut index_cursor = btree.open(index_rootpage);
                index_cursor.open_readwrite().insert(&full_key, encoded);
            }

            // 7. Add catalog entry
            btree.insert_schema_entry("index", &ci.index_name, &ci.table_name, index_rootpage, sql);

            Ok(ExecuteResult::CreateIndex {
                index_name: ci.index_name.clone(),
            })
        }
        Statement::Select(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_) => {
            let plan = planner::plan(stmt, btree).map_err(ExecuteError::Plan)?;
            let compiled = compiler::compile(&plan);
            let engine = Engine::with_program(
                compiled.operations(),
                compiled.num_registers(),
                btree.clone(),
            );
            Ok(ExecuteResult::Query(QueryExecution::new(engine)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TestDb;

    #[test]
    fn test_execute_create_table() {
        let mut test = TestDb::default();
        let result = execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();
        match result {
            ExecuteResult::CreateTable { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected CreateTable result"),
        }
    }

    #[test]
    fn test_execute_create_duplicate_table() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Try to create the same table again
        let result = execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        );
        match result {
            Err(ExecuteError::TableAlreadyExists(name)) => {
                assert_eq!(name, "users");
            }
            _ => panic!("Expected TableAlreadyExists error"),
        }
    }

    #[test]
    fn test_execute_insert() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        let result = execute("INSERT INTO users VALUES (1, 'alice', 30)", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                // INSERT yields a count row
                let row = q.next().expect("Expected a result row");
                assert_eq!(row[0], ScalarValue::Integer(1));
                assert!(q.next().is_none());
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_execute_select() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Insert two rows
        let mut q =
            match execute("INSERT INTO users VALUES (1, 'alice', 30)", &mut test.btree).unwrap() {
                ExecuteResult::Query(q) => q,
                _ => panic!(),
            };
        while q.next().is_some() {}

        let mut q =
            match execute("INSERT INTO users VALUES (2, 'bob', 25)", &mut test.btree).unwrap() {
                ExecuteResult::Query(q) => q,
                _ => panic!(),
            };
        while q.next().is_some() {}

        // Select column subset (name, age) from 3-column table
        let result = execute("SELECT name, age FROM users", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                let row1 = q.next().expect("Expected row 1");
                assert_eq!(row1[0], ScalarValue::String("alice".to_string()));
                assert_eq!(row1[1], ScalarValue::Integer(30));

                let row2 = q.next().expect("Expected row 2");
                assert_eq!(row2[0], ScalarValue::String("bob".to_string()));
                assert_eq!(row2[1], ScalarValue::Integer(25));

                assert!(q.next().is_none());
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_execute_select_with_filter() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Insert rows
        for sql in [
            "INSERT INTO users VALUES (1, 'alice', 30)",
            "INSERT INTO users VALUES (2, 'bob', 25)",
            "INSERT INTO users VALUES (3, 'charlie', 35)",
        ] {
            let mut q = match execute(sql, &mut test.btree).unwrap() {
                ExecuteResult::Query(q) => q,
                _ => panic!(),
            };
            while q.next().is_some() {}
        }

        // Select with filter (include id so scan covers columns from 0)
        let result = execute("SELECT id, name FROM users WHERE age > 26", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                let row1 = q.next().expect("Expected row 1");
                assert_eq!(row1[0], ScalarValue::Integer(1));
                assert_eq!(row1[1], ScalarValue::String("alice".to_string()));

                let row2 = q.next().expect("Expected row 2");
                assert_eq!(row2[0], ScalarValue::Integer(3));
                assert_eq!(row2[1], ScalarValue::String("charlie".to_string()));

                assert!(q.next().is_none());
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_execute_insert_with_column_names() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Insert with columns in different order: age, id, name
        // Should reorder values to match table's actual column order: id, name, age
        let mut q = match execute(
            "INSERT INTO users (age, id, name) VALUES (70, 1, 'oldie')",
            &mut test.btree,
        )
        .unwrap()
        {
            ExecuteResult::Query(q) => q,
            _ => panic!(),
        };
        while q.next().is_some() {}

        // Verify the values were inserted in correct positions
        let result = execute("SELECT id, name, age FROM users", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                let row = q.next().expect("Expected row");
                assert_eq!(row[0], ScalarValue::Integer(1), "id should be 1");
                assert_eq!(
                    row[1],
                    ScalarValue::String("oldie".to_string()),
                    "name should be 'oldie'"
                );
                assert_eq!(row[2], ScalarValue::Integer(70), "age should be 70");
                assert!(q.next().is_none());
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_persistence() {
        use tempfile::NamedTempFile;

        // Create a persistent temp file
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();

        // First session: create table and insert data
        {
            let mut btree = BTree::new(&path);
            execute(
                "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
                &mut btree,
            )
            .unwrap();

            execute("INSERT INTO users VALUES (1, 'alice', 30)", &mut btree).unwrap();
            execute("INSERT INTO users VALUES (2, 'bob', 25)", &mut btree).unwrap();
            // btree dropped here, flushes to disk
        }

        // Second session: reopen database and verify data
        {
            let mut btree = BTree::new(&path);
            let result = execute("SELECT id, name, age FROM users", &mut btree).unwrap();
            match result {
                ExecuteResult::Query(mut q) => {
                    let row1 = q.next().expect("Expected row 1");
                    assert_eq!(row1[0], ScalarValue::Integer(1));
                    assert_eq!(row1[1], ScalarValue::String("alice".to_string()));
                    assert_eq!(row1[2], ScalarValue::Integer(30));

                    let row2 = q.next().expect("Expected row 2");
                    assert_eq!(row2[0], ScalarValue::Integer(2));
                    assert_eq!(row2[1], ScalarValue::String("bob".to_string()));
                    assert_eq!(row2[2], ScalarValue::Integer(25));

                    assert!(q.next().is_none());
                }
                _ => panic!("Expected Query result"),
            }
        }
    }

    #[test]
    fn test_multi_table() {
        let mut test = TestDb::default();

        // Create two tables
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT)",
            &mut test.btree,
        )
        .unwrap();
        execute(
            "CREATE TABLE products (id INTEGER, title TEXT, price INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Insert into first table
        execute("INSERT INTO users VALUES (1, 'alice')", &mut test.btree).unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')", &mut test.btree).unwrap();

        // Insert into second table
        execute(
            "INSERT INTO products VALUES (10, 'laptop', 999)",
            &mut test.btree,
        )
        .unwrap();
        execute(
            "INSERT INTO products VALUES (11, 'mouse', 25)",
            &mut test.btree,
        )
        .unwrap();

        // Query first table
        let result = execute("SELECT id, name FROM users", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                let row1 = q.next().expect("Expected row 1");
                assert_eq!(row1[0], ScalarValue::Integer(1));
                assert_eq!(row1[1], ScalarValue::String("alice".to_string()));

                let row2 = q.next().expect("Expected row 2");
                assert_eq!(row2[0], ScalarValue::Integer(2));
                assert_eq!(row2[1], ScalarValue::String("bob".to_string()));

                assert!(q.next().is_none());
            }
            _ => panic!("Expected Query result"),
        }

        // Query second table
        let result = execute("SELECT title, price FROM products", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                let row1 = q.next().expect("Expected row 1");
                assert_eq!(row1[0], ScalarValue::String("laptop".to_string()));
                assert_eq!(row1[1], ScalarValue::Integer(999));

                let row2 = q.next().expect("Expected row 2");
                assert_eq!(row2[0], ScalarValue::String("mouse".to_string()));
                assert_eq!(row2[1], ScalarValue::Integer(25));

                assert!(q.next().is_none());
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_large_insert() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE numbers (id INTEGER, value INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Insert 150 rows
        for i in 0..150 {
            let sql = format!("INSERT INTO numbers VALUES ({}, {})", i, i * 10);
            let mut q = match execute(&sql, &mut test.btree).unwrap() {
                ExecuteResult::Query(q) => q,
                _ => panic!(),
            };
            while q.next().is_some() {}
        }

        // Select with WHERE filter: value > 500 should give us rows with id >= 51
        let result = execute(
            "SELECT id, value FROM numbers WHERE value > 500",
            &mut test.btree,
        )
        .unwrap();

        match result {
            ExecuteResult::Query(mut q) => {
                let mut count = 0;
                let mut last_id = 50;
                while let Some(row) = q.next() {
                    count += 1;
                    let id = match row[0] {
                        ScalarValue::Integer(i) => i,
                        _ => panic!("Expected integer id"),
                    };
                    let value = match row[1] {
                        ScalarValue::Integer(v) => v,
                        _ => panic!("Expected integer value"),
                    };
                    assert!(id > last_id, "Rows should be ordered by id");
                    assert_eq!(value, id * 10);
                    assert!(value > 500);
                    last_id = id;
                }
                // value > 500 means id > 50, so we should have 99 rows (51..149)
                assert_eq!(count, 99);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_select_nonexistent_table() {
        let mut test = TestDb::default();
        let result = execute("SELECT id FROM nonexistent", &mut test.btree);

        match result {
            Err(ExecuteError::Plan(PlanError::TableNotFound(name))) => {
                assert_eq!(name, "nonexistent");
            }
            _ => panic!("Expected TableNotFound error, got: {:?}", result),
        }
    }

    #[test]
    fn test_insert_wrong_column_count() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();

        // Too few values
        let result = execute("INSERT INTO users VALUES (1, 'alice')", &mut test.btree);
        match result {
            Err(ExecuteError::Plan(PlanError::ColumnCountMismatch { .. })) => {
                // Expected error
            }
            _ => panic!(
                "Expected ColumnCountMismatch error for too few values, got: {:?}",
                result
            ),
        }

        // Too many values
        let result = execute(
            "INSERT INTO users VALUES (1, 'alice', 30, 'extra')",
            &mut test.btree,
        );
        match result {
            Err(ExecuteError::Plan(PlanError::ColumnCountMismatch { .. })) => {
                // Expected error
            }
            _ => panic!(
                "Expected ColumnCountMismatch error for too many values, got: {:?}",
                result
            ),
        }
    }
}
