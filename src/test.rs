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
