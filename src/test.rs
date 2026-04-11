use std::ops::{Deref, DerefMut};

use tempfile::NamedTempFile;

use crate::storage::BTree;

pub struct TestDb {
    pub btree: BTree,
    _file: NamedTempFile,
}

impl Default for TestDb {
    fn default() -> Self {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        Self {
            btree: BTree::new(path),
            _file: file,
        }
    }
}

/// A temporary BTree backed by a temp file that is deleted on drop.
pub struct TempCatalog {
    pub btree: BTree,
    _file: NamedTempFile,
}

impl TempCatalog {
    pub fn new() -> Self {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        Self {
            btree: BTree::new(path),
            _file: file,
        }
    }
}

impl Deref for TempCatalog {
    type Target = BTree;
    fn deref(&self) -> &Self::Target {
        &self.btree
    }
}

impl DerefMut for TempCatalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.btree
    }
}

/// Return a path string for a new temporary database file.
/// The file is created but left empty; it persists until the caller drops it
/// (or until the OS cleans it up). Use this when you need to test
/// `BTree::new` after creation in the same test.
pub fn temp_db_path() -> String {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_str().unwrap().to_string();
    // Keep the file alive so the path is valid; leak the guard intentionally
    // since this is test-only code and the OS will clean up on process exit.
    std::mem::forget(file);
    path
}
