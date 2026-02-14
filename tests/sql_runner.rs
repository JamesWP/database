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

/// Run a single SQL test: execute, compare, and optionally update
fn run_sql_test(sql_path: PathBuf, expected_path: PathBuf) -> Option<String> {
    let update_expected = std::env::var("UPDATE_EXPECTED").is_ok();

    // Execute SQL script
    let actual_output = execute_sql_script(&sql_path);

    if update_expected {
        // Update mode: write actual output to expected file
        update_expected_file(&expected_path, &actual_output);
        let filename = expected_path.file_name().unwrap().to_str().unwrap();
        println!("Updated: {}", filename);
        Some(filename.to_string())
    } else {
        // Normal mode: compare and panic if mismatch
        let expected_content = fs::read_to_string(&expected_path).unwrap();
        let expected_lines: Vec<&str> = expected_content.lines().collect();
        let test_name = sql_path.file_name().unwrap().to_str().unwrap();

        compare_output(&actual_output, &expected_lines, test_name).unwrap();
        None
    }
}

#[test]
fn test_sql_scripts() {
    let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");

    // Check if we should run a single test
    let single_test = std::env::var("SQL_TEST_FILE").ok();

    // Discover all .sql files
    let entries = fs::read_dir(&sql_dir).unwrap();
    let mut test_files = Vec::new();

    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("sql") {
            // Filter by filename if SQL_TEST_FILE is set
            if let Some(ref filter) = single_test {
                let file_stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                if file_stem != filter {
                    continue;
                }
            }

            let expected_path = path.with_extension("expected");
            if expected_path.exists() {
                test_files.push((path, expected_path));
            }
        }
    }

    // Sort for deterministic test order
    test_files.sort_by(|a, b| a.0.cmp(&b.0));

    if test_files.is_empty() {
        if let Some(filter) = single_test {
            panic!("No SQL test file found matching '{}'", filter);
        } else {
            panic!("No SQL test files found in {:?}", sql_dir);
        }
    }

    // Run each test
    let mut updated_files = Vec::new();
    for (sql_path, expected_path) in test_files {
        println!("Running SQL test: {:?}", sql_path.file_name().unwrap());
        if let Some(updated_file) = run_sql_test(sql_path, expected_path) {
            updated_files.push(updated_file);
        }
    }

    // Print summary of updated files
    if !updated_files.is_empty() {
        println!("\nUpdated {} .expected file(s):", updated_files.len());
        for file in &updated_files {
            println!("  - {}", file);
        }
        println!("\nReview changes with: git diff tests/sql/");
    }
}
