// SQL test runner helpers - used by tests and update-sql-tests binary
// This module is public but intended for internal testing use only

use crate::catalog::Catalog;
use crate::db::{execute, ExecuteResult};
use crate::engine::scalarvalue::ScalarValue;
use std::fs;
use std::path::PathBuf;

fn format_row(row: &[ScalarValue]) -> String {
    row.iter()
        .map(|v| v.plain_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// A single SQL statement and its expected output lines from the inline format
pub struct SqlStatement {
    pub sql: String,
    pub expected: Vec<String>,
}

/// Parse a .sql file with inline expected output (-- > lines after each statement)
pub fn parse_sql_test_file(content: &str) -> Vec<SqlStatement> {
    let mut statements: Vec<SqlStatement> = Vec::new();

    for line in content.lines() {
        let ltrimmed = line.trim_start();

        if ltrimmed.is_empty() {
            continue;
        }

        if let Some(rest) = ltrimmed.strip_prefix("-- >") {
            // Expected output line — attach to last statement if present
            // Use trim_start only (preserve trailing whitespace in values like "3\t")
            if let Some(last) = statements.last_mut() {
                last.expected.push(rest.trim_start().to_string());
            }
        } else if ltrimmed.starts_with("--") {
            // Regular comment — ignore
        } else {
            // SQL statement
            statements.push(SqlStatement {
                sql: ltrimmed.trim_end().to_string(),
                expected: Vec::new(),
            });
        }
    }

    statements
}

/// Execute a single SQL statement against catalog, return output lines
fn execute_statement(sql: &str, catalog: &mut Catalog) -> Vec<String> {
    match execute(sql, catalog) {
        Ok(result) => match result {
            ExecuteResult::CreateTable { table_name } => {
                vec![format!("Table '{}' created", table_name)]
            }
            ExecuteResult::CreateIndex { index_name } => {
                vec![format!("Index '{}' created", index_name)]
            }
            ExecuteResult::DropTable { table_name } => {
                vec![format!("Table '{}' dropped", table_name)]
            }
            ExecuteResult::Query(mut query) | ExecuteResult::Explain(mut query) => {
                let mut rows = Vec::new();
                while let Some(row) = query.next() {
                    rows.push(format_row(&row));
                }
                if rows.is_empty() {
                    vec!["OK".to_string()]
                } else {
                    rows
                }
            }
        },
        Err(e) => vec![format!("ERROR: {}", e)],
    }
}

/// Execute SQL script and return per-statement output
fn execute_sql_script_parsed(statements: &[SqlStatement]) -> Vec<Vec<String>> {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap();
    let mut catalog = Catalog::create(temp_path);

    statements
        .iter()
        .map(|s| execute_statement(&s.sql, &mut catalog))
        .collect()
}

/// Compare actual output with expected output, return error message if mismatch
fn compare_output(
    actual_output: &[String],
    expected_lines: &[&str],
    test_name: &str,
) -> Result<(), String> {
    if actual_output.len() != expected_lines.len() {
        return Err(format!(
            "Output line count mismatch in {:?}:\nExpected {} lines, got {} lines\n\nExpected:\n{}\n\nActual:\n{}",
            test_name,
            expected_lines.len(),
            actual_output.len(),
            expected_lines.join("\n"),
            actual_output.join("\n")
        ));
    }

    for (i, (actual, expected)) in actual_output.iter().zip(expected_lines.iter()).enumerate() {
        if expected.starts_with("ERROR:") {
            if !actual.starts_with("ERROR:") {
                return Err(format!(
                    "Output mismatch at line {} in {:?}:\nExpected error but got: {}",
                    i + 1,
                    test_name,
                    actual
                ));
            }
            let pattern = expected.strip_prefix("ERROR:").unwrap().trim();
            let actual_error = actual.strip_prefix("ERROR:").unwrap().trim();
            if !actual_error
                .to_lowercase()
                .contains(&pattern.to_lowercase())
            {
                return Err(format!(
                    "Error pattern mismatch at line {} in {:?}:\nExpected pattern: {}\nActual error:    {}",
                    i + 1, test_name, pattern, actual_error
                ));
            }
        } else if actual != expected {
            return Err(format!(
                "Output mismatch at line {} in {:?}:\nExpected: {}\nActual:   {}",
                i + 1,
                test_name,
                expected,
                actual
            ));
        }
    }

    Ok(())
}

/// Compare per-statement actual vs inline expected output
fn compare_inline_output(
    statements: &[SqlStatement],
    actual_per_statement: &[Vec<String>],
    test_name: &str,
) -> Result<(), String> {
    for (stmt, actual_lines) in statements.iter().zip(actual_per_statement.iter()) {
        if stmt.expected.is_empty() {
            continue;
        }
        let expected_refs: Vec<&str> = stmt.expected.iter().map(|s| s.as_str()).collect();
        // Build a scoped name for better error messages
        let scoped = format!("{} (statement: {})", test_name, stmt.sql);
        compare_output(actual_lines, &expected_refs, &scoped)?;
    }
    Ok(())
}

/// Rewrite a .sql file with updated inline expected output
pub fn update_sql_file_inline(
    sql_path: &PathBuf,
    statements: &[SqlStatement],
    actual_per_statement: &[Vec<String>],
) {
    let original = fs::read_to_string(sql_path).unwrap();
    let mut output_lines: Vec<String> = Vec::new();
    let mut stmt_iter = statements
        .iter()
        .zip(actual_per_statement.iter())
        .peekable();

    for line in original.lines() {
        let trimmed = line.trim();

        // Drop old -- > lines (they'll be rewritten)
        if trimmed.starts_with("-- >") {
            continue;
        }

        output_lines.push(line.to_string());

        // After a SQL statement line, emit fresh expected output
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            if let Some((_, actual)) = stmt_iter.next() {
                for out_line in actual {
                    output_lines.push(format!("-- > {}", out_line));
                }
            }
        }
    }

    fs::write(sql_path, output_lines.join("\n") + "\n").unwrap();
}

/// Run a single SQL test by name (e.g., "where_clauses")
/// If update_mode is true, rewrites inline expected output instead of comparing
pub fn run_sql_test(test_name: &str, update_mode: bool) {
    let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let sql_path = sql_dir.join(format!("{}.sql", test_name));

    let sql_content = fs::read_to_string(&sql_path).unwrap();
    let statements = parse_sql_test_file(&sql_content);
    let actual_per_statement = execute_sql_script_parsed(&statements);

    if update_mode {
        update_sql_file_inline(&sql_path, &statements, &actual_per_statement);
        println!("Updated: {}.sql", test_name);
    } else {
        compare_inline_output(&statements, &actual_per_statement, test_name).unwrap();
    }
}

/// Migrate a test from legacy .expected file to inline format
pub fn migrate_sql_test(test_name: &str) {
    let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let sql_path = sql_dir.join(format!("{}.sql", test_name));

    let sql_content = fs::read_to_string(&sql_path).unwrap();
    let statements = parse_sql_test_file(&sql_content);
    let actual_per_statement = execute_sql_script_parsed(&statements);

    update_sql_file_inline(&sql_path, &statements, &actual_per_statement);
    println!("Migrated: {}.sql", test_name);
}

/// Get all SQL test names from the tests/sql directory
pub fn get_all_sql_tests() -> Vec<String> {
    let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let mut test_names = Vec::new();

    if let Ok(entries) = fs::read_dir(&sql_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        test_names.push(stem.to_string());
                    }
                }
            }
        }
    }

    test_names.sort();
    test_names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_inline_expected() {
        let input = "SELECT 1\n-- > 1\nSELECT 2\n-- > 2\n-- > extra\n";
        let parsed = parse_sql_test_file(input);
        assert_eq!(parsed[0].sql, "SELECT 1");
        assert_eq!(parsed[0].expected, vec!["1"]);
        assert_eq!(parsed[1].sql, "SELECT 2");
        assert_eq!(parsed[1].expected, vec!["2", "extra"]);
    }

    #[test]
    fn test_parse_inline_ignores_plain_comments() {
        let input = "-- This is a regular comment\nSELECT 1\n-- > 1\n";
        let parsed = parse_sql_test_file(input);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].sql, "SELECT 1");
        assert_eq!(parsed[0].expected, vec!["1"]);
    }

    #[test]
    fn test_parse_inline_no_expected() {
        let input = "SELECT 1\nSELECT 2\n";
        let parsed = parse_sql_test_file(input);
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].expected.is_empty());
        assert!(parsed[1].expected.is_empty());
    }

    #[test]
    fn test_parse_inline_blank_lines_ignored() {
        let input = "\nSELECT 1\n\n-- > 1\n\nSELECT 2\n-- > 2\n";
        let parsed = parse_sql_test_file(input);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].expected, vec!["1"]);
        assert_eq!(parsed[1].expected, vec!["2"]);
    }
}
