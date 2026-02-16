mod repl;

use database::storage;
use repl::{Repl, SharedState};
use storage::BTree;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        eprintln!(
            "Usage: {} <database_file> [command...]",
            std::env::args().next().unwrap()
        );
        eprintln!("Examples:");
        eprintln!(
            "  {} test.db                       # Interactive mode",
            std::env::args().next().unwrap()
        );
        eprintln!(
            "  {} test.db btree tables          # List all tables",
            std::env::args().next().unwrap()
        );
        eprintln!(
            "  {} test.db btree inspect page 0  # Inspect page 0",
            std::env::args().next().unwrap()
        );
        std::process::exit(1);
    }

    let db_name = &args[0];
    let db_path = std::path::Path::new(db_name);

    if db_path.exists() {
        if !args.get(1).is_some() {
            println!("Path {db_path:?} exists. opening");
        }
        assert!(
            db_path.is_file(),
            "Path {db_path:?} is not a file directory"
        );
    } else {
        if !args.get(1).is_some() {
            println!("Path {db_path:?} does not exist. creating");
        }
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&db_path)
            .expect("can create database file");
    }

    let db_path = db_path.canonicalize().unwrap();

    let btree = BTree::new(db_path.to_str().unwrap());
    let shared = SharedState::new(db_path.clone(), btree);

    let mut repl = Repl::new(shared);

    // If additional arguments provided, run as single command
    if args.len() > 1 {
        let command = args[1..].join(" ");
        repl.run_command(&command);
    } else {
        // Otherwise, run interactive REPL
        repl.run();
    }
}
