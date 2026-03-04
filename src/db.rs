use crate::compiler;
use crate::engine::scalarvalue::ScalarValue;
use crate::engine::Engine;
use crate::explain::{ExplainSchema, IndexMeta, TableMeta};
use crate::frontend::ast::{ColumnConstraint, DataType, Statement};
use crate::frontend::{parse, ParseError};
use crate::planner::{self, LogicalPlan, PlanError};
use crate::storage::BTree;

#[derive(Debug)]
pub enum ExecuteResult {
    CreateTable { table_name: String },
    CreateIndex { index_name: String },
    DropTable { table_name: String },
    Query(QueryExecution),
    Explain(QueryExecution),
}

#[derive(Debug)]
pub struct QueryExecution {
    pub column_names: Vec<String>,
    rows: Vec<Vec<ScalarValue>>,
    pos: usize,
}

impl QueryExecution {
    fn new(mut engine: Engine, column_names: Vec<String>) -> Result<Self, ExecuteError> {
        let rows = engine.run_result().map_err(|e| match e {
            crate::engine::EngineError::ConstraintViolation(msg) => {
                ExecuteError::ConstraintViolation(msg)
            }
            other => panic!("Engine error: {:?}", other),
        })?;
        Ok(QueryExecution {
            column_names,
            rows,
            pos: 0,
        })
    }

    fn from_rows(rows: Vec<Vec<ScalarValue>>) -> Self {
        QueryExecution {
            column_names: vec![],
            rows,
            pos: 0,
        }
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
    ConstraintViolation(String),
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
                    "Column '{}' in table '{}' is not INTEGER (only INTEGER columns are supported for indexes)",
                    column, table
                )
            }
            ExecuteError::ConstraintViolation(msg) => {
                write!(f, "constraint violation: {}", msg)
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

            // Create implicit unique indexes for PRIMARY KEY and UNIQUE columns
            for col in &ct.columns {
                let is_pk = col.constraints.contains(&ColumnConstraint::PrimaryKey);
                let is_uq = col.constraints.contains(&ColumnConstraint::Unique);
                if is_pk || is_uq {
                    let prefix = if is_pk { "_pk_" } else { "_uq_" };
                    let index_name = format!("{}{}_{}", prefix, name, col.name);
                    let index_rootpage = btree.create_tree();
                    let index_sql =
                        format!("CREATE INDEX {} ON {}({})", index_name, name, col.name);
                    btree.insert_schema_entry(
                        "index",
                        &index_name,
                        name,
                        index_rootpage,
                        &index_sql,
                    );
                }
            }

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

            // 4. Find columns and verify each is INTEGER or TEXT
            let mut column_idxs = Vec::new();
            for col_name in &ci.column_names {
                let column_def = create_table
                    .columns
                    .iter()
                    .find(|col| &col.name == col_name)
                    .ok_or_else(|| ExecuteError::ColumnNotFound {
                        table: ci.table_name.clone(),
                        column: col_name.clone(),
                    })?;

                if !matches!(
                    column_def.type_name,
                    Some(DataType::Integer) | Some(DataType::Text)
                ) {
                    return Err(ExecuteError::ColumnNotInteger {
                        table: ci.table_name.clone(),
                        column: col_name.clone(),
                    });
                }

                let col_idx = create_table
                    .columns
                    .iter()
                    .position(|col| &col.name == col_name)
                    .unwrap();
                column_idxs.push(col_idx);
            }

            // 5. Create the index B-tree
            let index_rootpage = btree.create_tree();

            // 6. Populate index by running a PopulateIndex plan via the engine
            let plan = LogicalPlan::PopulateIndex {
                input: Box::new(LogicalPlan::Scan {
                    rootpage: table_rootpage,
                    columns: (0..create_table.columns.len()).collect(),
                    with_key: true,
                }),
                index_rootpage,
                column_idxs,
            };
            let compiled = compiler::compile(&plan);
            Engine::with_program(
                compiled.operations(),
                compiled.num_registers(),
                btree.clone(),
            )
            .run();

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
            let column_names = if let Statement::Select(ref select) = stmt {
                planner::extract_select_column_names(select, btree)
            } else {
                vec![]
            };
            let plan = planner::plan(stmt, btree).map_err(ExecuteError::Plan)?;
            let mut compiled = compiler::compile(&plan);
            compiled.column_names = column_names;
            let engine = Engine::with_program(
                compiled.operations(),
                compiled.num_registers(),
                btree.clone(),
            );
            Ok(ExecuteResult::Query(QueryExecution::new(
                engine,
                compiled.column_names,
            )?))
        }
        Statement::Explain(_) => {
            let inner = match stmt {
                Statement::Explain(inner) => *inner,
                _ => unreachable!(),
            };
            let plan = planner::plan(inner, btree).map_err(ExecuteError::Plan)?;
            let schema = build_explain_schema(btree);
            let rows = crate::explain::format_plan(&plan, &schema)
                .into_iter()
                .map(|(id, text)| vec![ScalarValue::Integer(id as i64), ScalarValue::String(text)])
                .collect();
            Ok(ExecuteResult::Explain(QueryExecution::from_rows(rows)))
        }
    }
}

fn build_explain_schema(btree: &BTree) -> ExplainSchema {
    let mut schema = ExplainSchema::default();
    for (obj_type, name, tbl_name, rootpage, sql) in btree.scan_schema_entries() {
        match obj_type.as_str() {
            "table" => {
                if let Ok(Statement::CreateTable(ct)) = parse(&sql) {
                    let columns = ct.columns.iter().map(|c| c.name.clone()).collect();
                    schema.tables.insert(rootpage, TableMeta { name, columns });
                }
            }
            "index" => {
                let column_names = if let Ok(Statement::CreateIndex(ci)) = parse(&sql) {
                    ci.column_names
                } else {
                    vec![]
                };
                schema.indexes.insert(
                    rootpage,
                    IndexMeta {
                        name,
                        table_name: tbl_name,
                        column_names,
                    },
                );
            }
            _ => {}
        }
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TestDb;

    /// Execute a SQL query and collect all rows.
    fn collect_rows(sql: &str, btree: &mut BTree) -> Vec<Vec<ScalarValue>> {
        match execute(sql, btree).expect("execute failed") {
            ExecuteResult::Query(mut q) => {
                let mut rows = Vec::new();
                while let Some(row) = q.next() {
                    rows.push(row.to_vec());
                }
                rows
            }
            _ => panic!("Expected query result"),
        }
    }

    fn explain_rows(result: ExecuteResult) -> Vec<String> {
        match result {
            ExecuteResult::Explain(mut q) => {
                let mut out = Vec::new();
                while let Some(row) = q.next() {
                    // row[1] is the plan text column
                    out.push(row[1].plain_string());
                }
                out
            }
            other => panic!("Expected Explain result, got {:?}", other),
        }
    }

    #[test]
    fn test_explain_produces_table_scan() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();
        let rows = explain_rows(
            execute(
                "EXPLAIN SELECT id, name FROM users WHERE age = 30",
                &mut test.btree,
            )
            .unwrap(),
        );
        let joined = rows.join("\n");
        assert!(
            joined.contains("Scan users"),
            "expected table scan, got:\n{joined}"
        );
        assert!(
            !joined.contains("IndexScan"),
            "expected no index scan, got:\n{joined}"
        );
    }

    #[test]
    fn test_explain_produces_index_scan() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();
        execute("CREATE INDEX idx_age ON users(age)", &mut test.btree).unwrap();
        let rows = explain_rows(
            execute(
                "EXPLAIN SELECT id FROM users WHERE age = 30",
                &mut test.btree,
            )
            .unwrap(),
        );
        let joined = rows.join("\n");
        assert!(
            joined.contains("IndexScan via idx_age"),
            "expected index scan, got:\n{joined}"
        );
        assert!(
            joined.contains("= 30"),
            "expected equality predicate, got:\n{joined}"
        );
    }

    #[test]
    fn test_explain_does_not_execute() {
        let mut test = TestDb::default();
        execute("CREATE TABLE t (id INTEGER)", &mut test.btree).unwrap();
        execute("EXPLAIN INSERT INTO t VALUES (1)", &mut test.btree).unwrap();
        let mut q = match execute("SELECT id FROM t", &mut test.btree).unwrap() {
            ExecuteResult::Query(q) => q,
            other => panic!("Expected Query, got {:?}", other),
        };
        assert!(q.next().is_none(), "EXPLAIN should not have inserted rows");
    }

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
    fn test_having_filters_groups() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE sales (dept TEXT, amount INTEGER)",
            &mut test.btree,
        )
        .unwrap();
        for (dept, amount) in &[("eng", 100), ("eng", 200), ("hr", 50)] {
            let mut q = match execute(
                &format!("INSERT INTO sales VALUES ('{}', {})", dept, amount),
                &mut test.btree,
            )
            .unwrap()
            {
                ExecuteResult::Query(q) => q,
                _ => panic!(),
            };
            while q.next().is_some() {}
        }

        let rows = collect_rows(
            "SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 100",
            &mut test.btree,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], ScalarValue::String("eng".to_string()));
        assert_eq!(rows[0][1], ScalarValue::Integer(300));
    }

    #[test]
    fn test_having_count_star() {
        let mut test = TestDb::default();
        execute("CREATE TABLE t (cat TEXT)", &mut test.btree).unwrap();
        for cat in &["a", "a", "a", "b"] {
            let mut q = match execute(
                &format!("INSERT INTO t VALUES ('{}')", cat),
                &mut test.btree,
            )
            .unwrap()
            {
                ExecuteResult::Query(q) => q,
                _ => panic!(),
            };
            while q.next().is_some() {}
        }

        let rows = collect_rows(
            "SELECT cat, COUNT(*) FROM t GROUP BY cat HAVING COUNT(*) >= 3",
            &mut test.btree,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], ScalarValue::String("a".to_string()));
        assert_eq!(rows[0][1], ScalarValue::Integer(3));
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

    #[test]
    fn test_query_execution_exposes_column_names() {
        let mut test = TestDb::default();
        execute("CREATE TABLE t (id INTEGER, val TEXT)", &mut test.btree).unwrap();
        let result = execute("SELECT id, val FROM t", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.column_names, vec!["id", "val"]);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_insert_has_no_column_names() {
        let mut test = TestDb::default();
        execute("CREATE TABLE t (id INTEGER, val TEXT)", &mut test.btree).unwrap();
        let result = execute("INSERT INTO t VALUES (1, 'a')", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert!(q.column_names.is_empty());
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_delete_keeps_index_in_sync() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();
        execute("CREATE INDEX idx_age ON users(age)", &mut test.btree).unwrap();
        execute("INSERT INTO users VALUES (1, 25)", &mut test.btree).unwrap();
        execute("INSERT INTO users VALUES (2, 30)", &mut test.btree).unwrap();
        execute("INSERT INTO users VALUES (3, 25)", &mut test.btree).unwrap();

        // Delete one of the age=25 rows
        execute("DELETE FROM users WHERE id = 1", &mut test.btree).unwrap();

        // Query via index — must not see the deleted row
        let result = execute("SELECT id FROM users WHERE age = 25", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.rows.len(), 1);
                assert_eq!(q.rows[0][0], ScalarValue::Integer(3));
            }
            _ => panic!("Expected Query result"),
        }

        // Full scan must also agree
        let result = execute("SELECT id FROM users", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.rows.len(), 2);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_update_keeps_index_in_sync() {
        let mut test = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER, age INTEGER)",
            &mut test.btree,
        )
        .unwrap();
        execute("CREATE INDEX idx_age ON users(age)", &mut test.btree).unwrap();
        execute("INSERT INTO users VALUES (1, 25)", &mut test.btree).unwrap();
        execute("INSERT INTO users VALUES (2, 30)", &mut test.btree).unwrap();

        // Change age of row 1 from 25 to 40
        execute("UPDATE users SET age = 40 WHERE id = 1", &mut test.btree).unwrap();

        // Old value must not be findable via index
        let result = execute("SELECT id FROM users WHERE age = 25", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert!(
                    q.rows.is_empty(),
                    "stale index entry for age=25 must be gone"
                );
            }
            _ => panic!("Expected Query result"),
        }

        // New value must be findable via index
        let result = execute("SELECT id FROM users WHERE age = 40", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.rows.len(), 1);
                assert_eq!(q.rows[0][0], ScalarValue::Integer(1));
            }
            _ => panic!("Expected Query result"),
        }

        // Full scan must agree
        let result = execute("SELECT id FROM users", &mut test.btree).unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.rows.len(), 2);
            }
            _ => panic!("Expected Query result"),
        }
    }
}
