use crate::repl::{CommandResult, Mode, ModeId, SharedState};
use database::db::{execute, ExecuteResult};

/// Split SQL source into individual statements by splitting on `;`.
/// Strips `--` line comments and `/* */` block comments before checking
/// whether a chunk is non-empty, so comment-only chunks are skipped.
fn split_sql_statements(content: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = content.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '-' && chars.get(i + 1) == Some(&'-') {
            // Line comment: skip to end of line
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if ch == '/' && chars.get(i + 1) == Some(&'*') {
            // Block comment: skip to */
            i += 2;
            while i < chars.len() {
                if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    i += 2;
                    break;
                }
                i += 1;
            }
        } else if ch == ';' {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                statements.push(trimmed);
            }
            current.clear();
            i += 1;
        } else {
            current.push(ch);
            i += 1;
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        statements.push(trimmed);
    }
    statements
}

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

        let statements = split_sql_statements(&content);
        let mut output = String::new();

        for sql in &statements {
            match execute(sql, &mut shared.btree) {
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
