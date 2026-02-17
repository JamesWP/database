// SQL test runner helpers - used by tests and update-sql-tests binary
// This module is public but intended for internal testing use only

use crate::db::{execute, ExecuteResult};
use crate::engine::scalarvalue::ScalarValue;
use crate::storage::BTree;
use std::fs;
use std::path::PathBuf;

fn format_scalar(val: &ScalarValue) -> String {
    match val {
        ScalarValue::Integer(i) => i.to_string(),
        ScalarValue::Floating(f) => f.to_string(),
        ScalarValue::String(s) => s.clone(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Null => "NULL".to_string(),
    }
}

fn format_row(row: &[ScalarValue]) -> String {
    row.iter().map(format_scalar).collect::<Vec<_>>().join("\t")
}

/// Execute SQL script and return output lines
fn execute_sql_script(sql_path: &PathBuf) -> Vec<String> {
    // Create a temporary database
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap();
    let mut btree = BTree::new(temp_path);

    // Read SQL script
    let sql_content = fs::read_to_string(sql_path).unwrap();

    // Execute each SQL statement and collect output
    let mut actual_output = Vec::new();

    for (_line_num, line) in sql_content.lines().enumerate() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with("--") {
            continue;
        }

        match execute(line, &mut btree) {
            Ok(result) => match result {
                ExecuteResult::CreateTable { table_name } => {
                    actual_output.push(format!("Table '{}' created", table_name));
                }
                ExecuteResult::CreateIndex { index_name } => {
                    actual_output.push(format!("Index '{}' created", index_name));
                }
                ExecuteResult::DropTable { table_name } => {
                    actual_output.push(format!("Table '{}' dropped", table_name));
                }
                ExecuteResult::Query(mut query) => {
                    // Collect all rows
                    let mut row_count = 0;
                    while let Some(row) = query.next() {
                        actual_output.push(format_row(&row));
                        row_count += 1;
                    }
                    // For queries that produce no output rows, just acknowledge
                    if row_count == 0 {
                        actual_output.push("OK".to_string());
                    }
                }
            },
            Err(e) => {
                // Collect error for later validation
                actual_output.push(format!("ERROR: {}", e));
            }
        }
    }

    actual_output
}

/// Compare actual output with expected output, return error message if mismatch
fn compare_output(
    actual_output: &[String],
    expected_lines: &[&str],
    test_name: &str,
) -> Result<(), String> {
    // Check line count
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

    // Check each line
    for (i, (actual, expected)) in actual_output.iter().zip(expected_lines.iter()).enumerate() {
        if expected.starts_with("ERROR:") {
            // Error line - check that actual is also an error
            if !actual.starts_with("ERROR:") {
                return Err(format!(
                    "Output mismatch at line {} in {:?}:\nExpected error but got: {}",
                    i + 1,
                    test_name,
                    actual
                ));
            }
            // Extract pattern and actual error message
            let pattern = expected.strip_prefix("ERROR:").unwrap().trim();
            let actual_error = actual.strip_prefix("ERROR:").unwrap().trim();

            // Check if actual error contains pattern (case-insensitive substring match)
            if !actual_error
                .to_lowercase()
                .contains(&pattern.to_lowercase())
            {
                return Err(format!(
                    "Error pattern mismatch at line {} in {:?}:\nExpected pattern: {}\nActual error:    {}",
                    i + 1, test_name, pattern, actual_error
                ));
            }
        } else {
            // Normal line - require exact match
            if actual != expected {
                return Err(format!(
                    "Output mismatch at line {} in {:?}:\nExpected: {}\nActual:   {}",
                    i + 1,
                    test_name,
                    expected,
                    actual
                ));
            }
        }
    }

    Ok(())
}

/// Update expected file with actual output
fn update_expected_file(expected_path: &PathBuf, actual_output: &[String]) {
    fs::write(expected_path, actual_output.join("\n") + "\n").unwrap();
}

/// Run a single SQL test by name (e.g., "where_clauses")
/// If update_mode is true, updates the .expected file instead of comparing
pub fn run_sql_test(test_name: &str, update_mode: bool) {
    let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let sql_path = sql_dir.join(format!("{}.sql", test_name));
    let expected_path = sql_dir.join(format!("{}.expected", test_name));

    // Execute SQL script
    let actual_output = execute_sql_script(&sql_path);

    if update_mode {
        // Update mode: write actual output to expected file (create if missing)
        update_expected_file(&expected_path, &actual_output);
        println!("Updated: {}.expected", test_name);
    } else {
        // Normal mode: compare and panic if mismatch
        if !expected_path.exists() {
            panic!(
                "Missing .expected file for {}.sql\nRun: cargo run --bin update-sql-tests {}",
                test_name, test_name
            );
        }
        let expected_content = fs::read_to_string(&expected_path).unwrap();
        let expected_lines: Vec<&str> = expected_content.lines().collect();

        compare_output(&actual_output, &expected_lines, test_name).unwrap();
    }
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
