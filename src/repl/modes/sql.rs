use crate::repl::{CommandResult, Mode, ModeId, SharedState};
use database::db::{self, ExecuteResult};
use database::engine::scalarvalue::ScalarValue;

/// Format a ScalarValue without ANSI color codes (for width calculation)
fn plain_value(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Integer(i) => i.to_string(),
        ScalarValue::Floating(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::String(s) => format!("\"{}\"", s),
        ScalarValue::Blob(b) => format!("Blob({})", b.len()),
        ScalarValue::Null => "NULL".to_string(),
    }
}

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
                CommandResult::Message(format!("Table '{}' created", table_name))
            }
            Ok(ExecuteResult::CreateIndex { index_name }) => {
                CommandResult::Message(format!("Index '{}' created", index_name))
            }
            Ok(ExecuteResult::DropTable { table_name }) => {
                CommandResult::Message(format!("Table '{}' dropped", table_name))
            }
            Ok(ExecuteResult::Query(mut query)) => {
                use colored::Colorize;

                // Collect all rows first
                let mut rows = Vec::new();
                while let Some(row) = query.next() {
                    rows.push(row);
                }

                if rows.is_empty() {
                    return CommandResult::Message("(0 rows)".to_string());
                }

                // Convert to plain strings and compute max width per column
                let num_cols = rows[0].len();
                let plain_rows: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| row.iter().map(plain_value).collect())
                    .collect();

                let mut col_widths = vec![0; num_cols];
                for row in &plain_rows {
                    for (i, cell) in row.iter().enumerate() {
                        col_widths[i] = col_widths[i].max(cell.len());
                    }
                }

                // Build output with aligned and colored cells
                let mut output = String::new();
                for plain_row in &plain_rows {
                    let padded_cells: Vec<String> = plain_row
                        .iter()
                        .enumerate()
                        .map(|(i, cell)| format!("{:width$}", cell.green(), width = col_widths[i]))
                        .collect();
                    output += &padded_cells.join(" | ");
                    output += "\n";
                }
                output += &format!("({} rows)", rows.len());
                CommandResult::Message(output)
            }
            Err(e) => CommandResult::Error(format!("{}", e)),
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
