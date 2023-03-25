use std::ops::{Deref, DerefMut};

use crate::pager::{self, Pager};

pub struct Cursor<PagerRef> {
    pager: PagerRef,
    root_page: u32,

    /// key for the item pointed to by the cursor
    key: Option<u64>,
}

/// Mutable cursor implementation
impl<PagerRef> Cursor<PagerRef>
where
    PagerRef: DerefMut<Target = Pager>,
{
    fn insert(&mut self, key: u64, value: Vec<serde_json::Value>) {
        todo!()
    }
}

/// Imutable cursor implementation
impl<PagerRef> Cursor<PagerRef>
where
    PagerRef: Deref<Target = Pager>,
{
    /// Move the cursor to point at the first row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// first row to point to
    fn first(&mut self) {
        todo!()
    }

    /// Move the cursor to point at the last row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// last row to point to
    fn last(&mut self) {
        todo!()
    }

    /// Move the cursor to point at the row in the btree identified by the given key
    /// This may result in the cursor not pointing to a row if there is no
    /// row found with that key to point to
    fn find(&mut self, key: u64) {
        todo!()
    }

    /// get the value at the specified column index from the row pointed to by the cursor,
    /// or None if the cursor is not pointing to a row
    fn column(&self, col_idx: u32) -> Option<serde_json::Value> {
        todo!()
    }

    /// Move the cursor to point at the next item in the btree
    fn next(&mut self) {
        todo!()
    }

    /// Move the cursor to point at the next item in the btree
    fn prev(&mut self) {
        todo!()
    }
}

pub struct BTree {
    pager: pager::Pager,
}

impl BTree {
    fn new(path: &str) -> BTree {
        BTree {
            pager: Pager::new(path),
        }
    }

    fn open_readonly<'a>(&'a self, tree_name: &str) -> Option<Cursor<&'a Pager>> {
        let idx = self.pager.get_root_page(tree_name)?;

        Some(Cursor {
            pager: &self.pager,
            key: Default::default(),
            root_page: idx,
        })
    }

    fn open_readwrite<'a>(&'a mut self, tree_name: &str) -> Option<Cursor<&'a mut Pager>> {
        let idx = self.pager.get_root_page(tree_name)?;

        Some(Cursor {
            pager: &mut self.pager,
            key: Default::default(),
            root_page: idx,
        })
    }

    /// Create a new tree with the given name, tree must not already exist
    fn create_tree(&mut self, tree_name: &str) {
        assert!(self.pager.get_root_page(tree_name).is_none());
        let idx = self.pager.allocate();
        self.pager.set_root_page(tree_name, idx);
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tempfile::NamedTempFile;

    use super::BTree;

    struct TestDb {
        btree: BTree,
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

    #[test]
    fn test_create_blank() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open_readonly("testing").is_none());

        btree.create_tree("testing");

        // Test we can take two readonly cursors at the same time
        {
            let mut _cursor1 = btree.open_readonly("testing").unwrap();
            let mut _cursor2 = btree.open_readonly("testing").unwrap();
        }

        // Test the new table is empty, when using a readonly cursor
        {
            let mut cursor = btree.open_readonly("testing").unwrap();
            cursor.first();

            assert!(cursor.column(0).is_none());
        }

        // Test the new table is empty, when using a readwrite cursor
        {
            let mut cursor = btree.open_readwrite("testing").unwrap();

            cursor.first();
            assert!(cursor.column(0).is_none());
        }
    }


    #[test]
    fn test_create_and_insert() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open_readonly("testing").is_none());

        btree.create_tree("testing");

        // Test we can insert a value
        {
            let mut cursor = btree.open_readwrite("testing").unwrap();

            cursor.insert(42, vec![json!(1337), json!(42), json!(386), json!(64)]);
        }

        // Test we can read out the new value
        {
            let mut cursor = btree.open_readonly("testing").unwrap();

            cursor.first();
            assert_eq!(cursor.column(0), Some(json!(1337)));
        }
    }
}
