use crate::compiler;
use crate::engine::scalarvalue::ScalarValue;
use crate::engine::Engine;
use crate::frontend::ast::Statement;
use crate::frontend::{parse, ParseError};
use crate::planner::{self, PlanError};
use crate::storage::BTree;

pub enum ExecuteResult {
    CreateTable { table_name: String },
    Query(QueryExecution),
}

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
            let root_page = btree.create_tree();
            let ddl = sql.to_string();
            let key = name
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
            btree.insert_schema_entry(key, "table", name, name, root_page, &ddl);
            Ok(ExecuteResult::CreateTable {
                table_name: name.clone(),
            })
        }
        Statement::Select(_) | Statement::Insert(_) => {
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

        // Select all columns (scan reads from position 0 sequentially)
        let result = execute("SELECT id, name, age FROM users", &mut test.btree).unwrap();
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
}
