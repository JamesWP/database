use std::path::PathBuf;

use crate::planner::schema::Schema;
use crate::storage::BTree;

/// State shared across all modes
pub struct SharedState {
    /// The BTree storage
    pub btree: Box<BTree>,

    /// Database file path
    pub db_path: PathBuf,

    /// Schema for planner mode (can be loaded or mocked)
    pub schema: Option<Schema>,
}

impl SharedState {
    pub fn new(db_path: PathBuf, btree: BTree) -> Self {
        SharedState {
            btree: Box::new(btree),
            db_path,
            schema: None,
        }
    }
}
