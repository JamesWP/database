use std::path::PathBuf;

use database::catalog::Catalog;

/// State shared across all modes
pub struct SharedState {
    /// The Catalog (schema + BTree storage)
    pub btree: Box<Catalog>,

    /// Database file path
    #[allow(dead_code)]
    pub db_path: PathBuf,
}

impl SharedState {
    pub fn new(db_path: PathBuf, catalog: Catalog) -> Self {
        SharedState {
            btree: Box::new(catalog),
            db_path,
        }
    }
}
