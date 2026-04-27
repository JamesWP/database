// Binary for updating SQL test expected output
// Usage:
//   cargo run -p database --bin update-sql-tests              # Update all tests
//   cargo run -p database --bin update-sql-tests where_clauses delete  # Update specific tests
//   cargo run -p database --bin update-sql-tests --migrate    # Migrate all tests to inline format
//   cargo run -p database --bin update-sql-tests --migrate where_clauses  # Migrate specific tests

use database::testing::sql_runner::{get_all_sql_tests, migrate_sql_test, run_sql_test};
use std::env;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    let migrate = args.first().map(|a| a == "--migrate").unwrap_or(false);
    let rest: Vec<String> = if migrate {
        args.into_iter().skip(1).collect()
    } else {
        args
    };

    let test_names = if rest.is_empty() {
        get_all_sql_tests()
    } else {
        rest
    };

    if test_names.is_empty() {
        eprintln!("No SQL test files found in tests/sql/");
        std::process::exit(1);
    }

    if migrate {
        println!(
            "Migrating {} SQL test(s) to inline format...\n",
            test_names.len()
        );
        for test_name in &test_names {
            migrate_sql_test(test_name);
        }
        println!("\nMigrated {} file(s)", test_names.len());
        println!("Review changes with: git diff tests/sql/");
    } else {
        println!("Updating {} SQL test(s)...\n", test_names.len());
        for test_name in &test_names {
            run_sql_test(test_name, true);
        }
        println!("\nUpdated {} test(s)", test_names.len());
        println!("Review changes with: git diff tests/sql/");
    }
}
