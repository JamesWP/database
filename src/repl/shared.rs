use std::path::PathBuf;

use crate::storage::BTree;

/// State shared across all modes
pub struct SharedState {
    /// The BTree storage
    pub btree: Box<BTree>,

    /// Database file path
    pub db_path: PathBuf,
}

impl SharedState {
    pub fn new(db_path: PathBuf, btree: BTree) -> Self {
        SharedState {
            btree: Box::new(btree),
            db_path,
        }
    }
}
