use std::ops::{Deref, DerefMut};

use tempfile::NamedTempFile;

use crate::catalog::Catalog;

pub struct TestDb {
    pub catalog: Catalog,
    _file: NamedTempFile,
}

impl Default for TestDb {
    fn default() -> Self {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        Self {
            catalog: Catalog::create(path),
            _file: file,
        }
    }
}

/// A temporary Catalog backed by a temp file that is deleted on drop.
pub struct TempCatalog {
    pub catalog: Catalog,
    _file: NamedTempFile,
}

impl TempCatalog {
    pub fn new() -> Self {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        Self {
            catalog: Catalog::create(path),
            _file: file,
        }
    }
}

impl Deref for TempCatalog {
    type Target = Catalog;
    fn deref(&self) -> &Self::Target {
        &self.catalog
    }
}

impl DerefMut for TempCatalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.catalog
    }
}

/// Return a path string for a new temporary database file.
/// The file is created but left empty; it persists until the caller drops it
/// (or until the OS cleans it up). Use this when you need to test
/// `Catalog::open` after a `Catalog::create` in the same test.
pub fn temp_db_path() -> String {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_str().unwrap().to_string();
    // Keep the file alive so the path is valid; leak the guard intentionally
    // since this is test-only code and the OS will clean up on process exit.
    std::mem::forget(file);
    path
}
