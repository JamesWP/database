mod compiler;
mod engine;
mod frontend;
mod planner;
mod repl;
mod storage;

#[cfg(test)]
mod test;

use repl::{Repl, SharedState};
use storage::BTree;

pub(crate) fn main() {
    let mut args = std::env::args().skip(1);

    let db_name = args.next().expect("first arg should be database name");

    let db_path = std::path::Path::new(&db_name);

    if db_path.exists() {
        println!("Path {db_path:?} exists. opening");
        assert!(
            db_path.is_file(),
            "Path {db_path:?} is not a file directory"
        );
    } else {
        println!("Path {db_path:?} does not exist. creating");
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
    repl.run();
}
