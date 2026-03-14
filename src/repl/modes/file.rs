use crate::repl::{CommandResult, Mode, ModeId, SharedState};
use database::db::{execute, ExecuteResult};
use database::testing::sql_runner::parse_sql_test_file;

#[derive(Debug)]
pub struct FileMode;

impl FileMode {
    pub fn new() -> Self {
        FileMode
    }
}

impl Mode for FileMode {
    fn id(&self) -> ModeId {
        ModeId::File
    }

    fn execute(&mut self, tokens: &[&str], shared: &mut SharedState) -> CommandResult {
        let path = tokens.join(" ");
        if path.is_empty() {
            return CommandResult::Error("Usage: file <path>".to_string());
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => return CommandResult::Error(format!("Cannot read {}: {}", path, e)),
        };

        let statements = parse_sql_test_file(&content);
        let mut output = String::new();

        for stmt in &statements {
            match execute(&stmt.sql, &mut shared.btree) {
                Ok(ExecuteResult::CreateTable { table_name }) => {
                    output += &format!("Table '{}' created\n", table_name);
                }
                Ok(ExecuteResult::CreateIndex { index_name }) => {
                    output += &format!("Index '{}' created\n", index_name);
                }
                Ok(ExecuteResult::DropTable { table_name }) => {
                    output += &format!("Table '{}' dropped\n", table_name);
                }
                Ok(ExecuteResult::Query(mut q) | ExecuteResult::Explain(mut q)) => {
                    let mut rows = Vec::new();
                    while let Some(row) = q.next() {
                        rows.push(
                            row.iter()
                                .map(|v| v.plain_string())
                                .collect::<Vec<_>>()
                                .join(", "),
                        );
                    }
                    if rows.is_empty() {
                        output += "OK\n";
                    } else {
                        for line in rows {
                            output += &format!("{}\n", line);
                        }
                    }
                }
                Err(e) => {
                    output += &format!("ERROR: {}\n", e);
                }
            }
        }

        CommandResult::Message(output.trim_end().to_string())
    }

    fn help(&self) -> String {
        "file mode - execute SQL statements from a file:\n  <path>   Run all SQL statements in the file".to_string()
    }
}
