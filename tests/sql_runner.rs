use database::db::{execute, ExecuteResult};
use database::engine::scalarvalue::ScalarValue;
use database::storage::BTree;
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

fn run_sql_test(sql_path: PathBuf, expected_path: PathBuf) {
    // Create a temporary database
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap();
    let mut btree = BTree::new(temp_path);

    // Read SQL script and expected output
    let sql_content = fs::read_to_string(&sql_path).unwrap();
    let expected_content = fs::read_to_string(&expected_path).unwrap();
    let expected_lines: Vec<&str> = expected_content.lines().collect();

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

    // Compare
    if actual_output.len() != expected_lines.len() {
        panic!(
            "Output line count mismatch in {:?}:\nExpected {} lines, got {} lines\n\nExpected:\n{}\n\nActual:\n{}",
            sql_path.file_name().unwrap(),
            expected_lines.len(),
            actual_output.len(),
            expected_content,
            actual_output.join("\n")
        );
    }

    for (i, (actual, expected)) in actual_output.iter().zip(expected_lines.iter()).enumerate() {
        if expected.starts_with("ERROR:") {
            // Error line - check that actual is also an error
            if !actual.starts_with("ERROR:") {
                panic!(
                    "Output mismatch at line {} in {:?}:\nExpected error but got: {}",
                    i + 1,
                    sql_path.file_name().unwrap(),
                    actual
                );
            }
            // Extract pattern and actual error message
            let pattern = expected.strip_prefix("ERROR:").unwrap().trim();
            let actual_error = actual.strip_prefix("ERROR:").unwrap().trim();

            // Check if actual error contains pattern (case-insensitive substring match)
            if !actual_error
                .to_lowercase()
                .contains(&pattern.to_lowercase())
            {
                panic!(
                    "Error pattern mismatch at line {} in {:?}:\nExpected pattern: {}\nActual error:    {}",
                    i + 1,
                    sql_path.file_name().unwrap(),
                    pattern,
                    actual_error
                );
            }
        } else {
            // Normal line - require exact match
            if actual != expected {
                panic!(
                    "Output mismatch at line {} in {:?}:\nExpected: {}\nActual:   {}",
                    i + 1,
                    sql_path.file_name().unwrap(),
                    expected,
                    actual
                );
            }
        }
    }
}

#[test]
fn test_sql_scripts() {
    let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");

    // Discover all .sql files
    let entries = fs::read_dir(&sql_dir).unwrap();
    let mut test_files = Vec::new();

    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("sql") {
            let expected_path = path.with_extension("expected");
            if expected_path.exists() {
                test_files.push((path, expected_path));
            }
        }
    }

    // Sort for deterministic test order
    test_files.sort_by(|a, b| a.0.cmp(&b.0));

    if test_files.is_empty() {
        panic!("No SQL test files found in {:?}", sql_dir);
    }

    // Run each test
    for (sql_path, expected_path) in test_files {
        println!("Running SQL test: {:?}", sql_path.file_name().unwrap());
        run_sql_test(sql_path, expected_path);
    }
}
