use crate::repl::{CommandResult, Mode, ModeId, SharedState};
use database::db::{self, ExecuteResult};

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
            Ok(ExecuteResult::Query(mut query) | ExecuteResult::Explain(mut query)) => {
                use colored::Colorize;

                let column_names = query.column_names.clone();

                // Collect all rows first
                let mut rows = Vec::new();
                while let Some(row) = query.next() {
                    rows.push(row);
                }

                if rows.is_empty() && column_names.is_empty() {
                    return CommandResult::Message("(0 rows)".to_string());
                }

                // Determine number of columns
                let num_cols = if !rows.is_empty() {
                    rows[0].len()
                } else {
                    column_names.len()
                };

                // Convert to plain strings and compute max width per column
                let plain_rows: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| row.iter().map(|v| v.plain_string()).collect())
                    .collect();

                let mut col_widths = vec![0usize; num_cols];
                // Include header name widths
                for (i, name) in column_names.iter().enumerate() {
                    if i < num_cols {
                        col_widths[i] = col_widths[i].max(name.len());
                    }
                }
                for row in &plain_rows {
                    for (i, cell) in row.iter().enumerate() {
                        col_widths[i] = col_widths[i].max(cell.len());
                    }
                }

                let mut output = String::new();

                // Print header row if column names are available
                if !column_names.is_empty() {
                    let header: Vec<String> = column_names
                        .iter()
                        .enumerate()
                        .map(|(i, name)| {
                            format!("{:<width$}", name.bold().white(), width = col_widths[i])
                        })
                        .collect();
                    output += &header.join(&" │ ".truecolor(90, 90, 90).to_string());
                    output += "\n";
                    let sep: Vec<String> = col_widths
                        .iter()
                        .map(|w| "─".repeat(*w).truecolor(90, 90, 90).to_string())
                        .collect();
                    output += &sep.join(&"─┼─".truecolor(90, 90, 90).to_string());
                    output += "\n";
                }

                if rows.is_empty() {
                    output += "(0 rows)";
                    return CommandResult::Message(output);
                }

                // Build output with aligned and colored cells
                for plain_row in &plain_rows {
                    let padded_cells: Vec<String> = plain_row
                        .iter()
                        .enumerate()
                        .map(|(i, cell)| format!("{:width$}", cell.green(), width = col_widths[i]))
                        .collect();
                    output += &padded_cells.join(&" │ ".truecolor(90, 90, 90).to_string());
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
