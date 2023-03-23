use crate::pager::{self, Pager};

struct CursorState {
    root_page: u32,

    /// key for the item pointed to by the cursor
    key: Option<u64>,
}

impl CursorState {
    fn new(root_page: u32) -> Self {
        Self {
            root_page,
            key: Default::default(),
        }
    }
}

pub struct ReadonlyCursor<'a> {
    btree: &'a BTree,

    state: CursorState,
}

impl<'a> ReadonlyCursor<'a> {
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

pub struct ReadwriteCursor<'a> {
    btree: &'a mut BTree,

    state: CursorState,
}

impl<'a> ReadwriteCursor<'a> {
    fn insert(&mut self, key: u64, value: Vec<serde_json::Value>) {
        todo!()
    }

    fn reader<'b>(&'b self) -> ReadonlyCursor<'b> {
        ReadonlyCursor {
            btree: self.btree,
            state: CursorState::new(self.state.root_page),
        }
    }
}

pub struct BTree {
    pager: pager::Pager,
}

impl BTree {
    fn new(path: &str) -> BTree {
        BTree { pager: Pager::new(path) }
    }

    fn open_readonly<'a>(&'a self, tree_name: &str) -> Option<ReadonlyCursor<'a>> {
        let idx = self.pager.get_root_page(tree_name)?;

        Some(ReadonlyCursor {
            btree: self,
            state: CursorState::new(idx),
        })
    }

    fn open_readwrite<'a>(&'a mut self, tree_name: &str) -> Option<ReadwriteCursor<'a>> {
        let idx = self.pager.get_root_page(tree_name)?;

        Some(ReadwriteCursor {
            btree: self,
            state: CursorState::new(idx),
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

    #[test]
    fn test_create_add_read() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let mut btree = BTree::new(path);

        assert!(btree.open_readonly("testing").is_none());

        btree.create_tree("testing");

        let mut cursor1 = btree.open_readonly("testing").unwrap();
        let _cursor2 = btree.open_readonly("testing").unwrap();

        cursor1.first();

        assert!(cursor1.column(0).is_none());

        let mut cursor3 = btree.open_readwrite("testing").unwrap();

        let mut cursor4 = cursor3.reader();

        cursor4.first();
        assert!(cursor4.column(0).is_none());

        cursor3.insert(42, vec![json!(1337), json!(42), json!(386), json!(64)]);

        let mut cursor5 = cursor3.reader();

        cursor5.first();
        assert_eq!(cursor5.column(0), Some(json!(1337)));
    }
}