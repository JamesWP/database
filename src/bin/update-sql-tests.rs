// Binary for updating SQL test .expected files
// Usage:
//   cargo run --bin update-sql-tests              # Update all .expected files
//   cargo run --bin update-sql-tests where_clauses delete  # Update specific tests

use database::testing::sql_runner::{get_all_sql_tests, run_sql_test};
use std::env;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    let test_names = if args.is_empty() {
        // No args - update all tests
        get_all_sql_tests()
    } else {
        // Specific tests provided
        args
    };

    if test_names.is_empty() {
        eprintln!("No SQL test files found in tests/sql/");
        std::process::exit(1);
    }

    println!("Updating {} SQL test(s)...\n", test_names.len());

    for test_name in &test_names {
        run_sql_test(test_name, true); // true = update mode
    }

    println!("\nUpdated {} .expected file(s)", test_names.len());
    println!("Review changes with: git diff tests/sql/");
}
