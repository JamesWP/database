use crate::db::{self, ExecuteResult};
use crate::repl::{CommandResult, Mode, ModeId, SharedState};

#[derive(Debug)]
pub struct SqlMode;

impl SqlMode {
    pub fn new() -> Self {
        SqlMode
    }
}

impl Mode for SqlMode {
    fn id(&self) -> ModeId {
        ModeId::Sql
    }

    fn execute(&mut self, tokens: &[&str], shared: &mut SharedState) -> CommandResult {
        let sql = tokens.join(" ");
        if sql.is_empty() {
            return CommandResult::Error("Enter a SQL statement".to_string());
        }

        match db::execute(&sql, &mut shared.btree) {
            Ok(ExecuteResult::CreateTable { table_name }) => {
                CommandResult::Message(format!("Created table '{}'", table_name))
            }
            Ok(ExecuteResult::Query(mut query)) => {
                let mut output = String::new();
                let mut count = 0;
                while let Some(row) = query.next() {
                    let formatted: Vec<String> = row.iter().map(|v| format!("{}", v)).collect();
                    output += &formatted.join(" | ");
                    output += "\n";
                    count += 1;
                }
                output += &format!("({} rows)", count);
                CommandResult::Message(output)
            }
            Err(e) => CommandResult::Error(format!("{:?}", e)),
        }
    }

    fn help(&self) -> String {
        r#"SQL mode - execute SQL statements directly:
  CREATE TABLE <name> (<columns>)   Create a new table
  INSERT INTO <table> VALUES (...)  Insert rows
  SELECT <columns> FROM <table>     Query rows

Example:
  CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
  INSERT INTO users VALUES (1, 'alice', 30)
  SELECT id, name, age FROM users"#
            .to_string()
    }
}
