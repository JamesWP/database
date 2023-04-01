use std::ops::{Deref, DerefMut};

use crate::{
    node::{self, SearchResult},
    pager::{self, Pager},
};

type Tuple = std::vec::Vec<serde_json::Value>;
type NodePage = node::NodePage<u64, Tuple>;
type LeafNodePage = node::LeafNodePage<u64, Tuple>;

pub struct Cursor<PagerRef> {
    pager: PagerRef,
    root_page: u32,

    /// key for the item pointed to by the cursor
    stack: Vec<InteriorNodeIterator>,
    leaf_iterator: Option<LeafNodeIterator>,
}

/// identifies the page index of the interior node and the index of the child curently selected
type InteriorNodeIterator = (u32, usize);

/// identifies the page index of the leaf node and the index of the entry curently selected
type LeafNodeIterator = (u32, usize);

const NULL: serde_json::Value = serde_json::Value::Null;

/// Mutable cursor implementation
impl<PagerRef> Cursor<PagerRef>
where
    PagerRef: DerefMut<Target = Pager>,
{
    fn insert(&mut self, key: u64, value: Tuple) {
        // we maintain a stack of the nodes we decended through in case of needing to split them.
        // Starting at the root, we search to find:
        //   an empty place to put the new value
        //   en existing value to replace
        let mut stack = Vec::new();

        stack.push(self.root_page);

        loop {
            let top_page_idx = stack.last().unwrap();
            let mut top_page: NodePage = self.pager.get_and_decode(top_page_idx);
            match top_page.search(&key) {
                SearchResult::Found(insertion_index) => {
                    // We found the index in the node where an existing value for this key exists
                    // we need to replace it with our value

                    top_page.set_item_at_index(insertion_index, value);

                    // TODO: there is going to be a panic if the new value does not fit on this page...
                    self.pager.encode_and_set(top_page_idx, top_page);

                    return;
                }
                SearchResult::NotPresent(item_idx) => {

                    top_page.insert_item_at_index(item_idx, key, value);

                    self.pager.encode_and_set(top_page_idx, top_page);

                    return;
                }
                SearchResult::GoDown(_child_index) => {
                    // The node does not contain the value, instead we found the index of a child of this node where the value should be inserted instead
                    // we need to go deeper.

                    todo!()
                }
            }
        }
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
        // Take the tree identified by the root page number, and find its left most node and
        // find its smallest entry
        let root_page: NodePage = self.pager.get_and_decode(self.root_page);

        let mut page = root_page;
        let mut page_idx = self.root_page;
        loop {
            match page {
                node::NodePage::Leaf(l) => {
                    // We found the first leaf in the tree.
                    // TODO: Maybe store a readonly copy of this leaf node instead of this `leaf_iterator`
                    self.leaf_iterator = Some((page_idx, 0));
                    return;
                },
                node::NodePage::Interior(i) => todo!(),
            }
        }
    }

    /// Move the cursor to point at the last row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// last row to point to
    fn last(&mut self) {
        // Take the tree identified by the root page number, and find its right most node and
        // find its largest entry
        let root_page: NodePage = self.pager.get_and_decode(self.root_page);

        let mut page = root_page;
        let mut page_idx = self.root_page;
        loop {
            match page {
                node::NodePage::Leaf(l) => {
                    // We found the first leaf in the tree.
                    // TODO: Maybe store a readonly copy of this leaf node instead of this `leaf_iterator`
                    self.leaf_iterator = Some((page_idx, l.num_items()-1));
                    return;
                },
                node::NodePage::Interior(_i) => todo!(),
            }
        }
    }

    /// Move the cursor to point at the row in the btree identified by the given key
    /// This may result in the cursor not pointing to a row if there is no
    /// row found with that key to point to
    fn find(&mut self, key: u64) {
        let root_page: NodePage = self.pager.get_and_decode(self.root_page);

        let mut page = root_page;
        let page_idx = self.root_page;

        loop {
            match page.search(&key) {
                SearchResult::Found(index) => {
                    self.leaf_iterator = Some((page_idx, index));
                    return;
                },
                SearchResult::NotPresent(_) => self.leaf_iterator = None,
                SearchResult::GoDown(_) => todo!(),
            }
        }
    }

    /// get the value at the specified column index from the row pointed to by the cursor,
    /// or None if the cursor is not pointing to a row
    fn column(&self, col_idx: usize) -> Option<serde_json::Value> {
        let (leaf_page_number, entry_index) = self.leaf_iterator?;

        let page: NodePage = self.pager.get_and_decode(leaf_page_number);

        match page {
            node::NodePage::Leaf(l) => {
                let (_key, value) = l.get_item_at_index(entry_index)?;
                let value = value.get(col_idx).unwrap_or(&NULL);
                Some(value.to_owned())
            },
            node::NodePage::Interior(_) => panic!("Values are always supposed to be in leaf pages"),
        }
    }

    /// Move the cursor to point at the next item in the btree
    fn next(&mut self) {
        if self.leaf_iterator.is_none() {
            return;
        }

        let (leaf_page_number, entry_index) = self.leaf_iterator.unwrap();
        
        let page: NodePage = self.pager.get_and_decode(leaf_page_number);

        match page {
            node::NodePage::Leaf(l) => {
                if entry_index +1 < l.num_items() {
                    self.leaf_iterator = Some((leaf_page_number, entry_index+1));
                } else {
                    // We ran out of items on this page, find the next leaf page
                }
            },
            node::NodePage::Interior(_) => panic!("Values are always supposed to be in leaf pages"),
        }
    }

    /// Move the cursor to point at the next item in the btree
    fn prev(&mut self) {
        if self.leaf_iterator.is_none() {
            return;
        }

        let (leaf_page_number, entry_index) = self.leaf_iterator.unwrap();
        
        let page: NodePage = self.pager.get_and_decode(leaf_page_number);

        match page {
            node::NodePage::Leaf(l) => {
                if entry_index > 0 {
                    self.leaf_iterator = Some((leaf_page_number, entry_index-1));
                } else {
                    // We ran out of items on this page, find the previous leaf page
                }
            },
            node::NodePage::Interior(_) => panic!("Values are always supposed to be in leaf pages"),
        }
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
            root_page: idx,
            stack: vec![],
            leaf_iterator: None,
        })
    }

    fn open_readwrite<'a>(&'a mut self, tree_name: &str) -> Option<Cursor<&'a mut Pager>> {
        let idx = self.pager.get_root_page(tree_name)?;

        Some(Cursor {
            pager: &mut self.pager,
            root_page: idx,
            stack: vec![],
            leaf_iterator: None,
        })
    }

    /// Create a new tree with the given name, tree must not already exist
    fn create_tree(&mut self, tree_name: &str) {
        assert!(self.pager.get_root_page(tree_name).is_none());
        let idx = self.pager.allocate();
        self.pager.set_root_page(tree_name, idx);
        let empty_leaf_node = node::LeafNodePage::<u64, Tuple>::default();
        let empty_root_node = node::NodePage::Leaf(empty_leaf_node);
        // Encode and set the empty_root_node in the pager
        self.pager.encode_and_set(idx, empty_root_node);
    }

    fn debug(&self) {
        self.pager.debug()
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

        btree.debug();
    }

    #[test]
    fn test_insert_many() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open_readonly("testing").is_none());

        btree.create_tree("testing");

        // Test we can insert a value
        {
            let mut cursor = btree.open_readwrite("testing").unwrap();

            for i in 1..10 {
                cursor.insert(i, vec![json!(i)]);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor = btree.open_readonly("testing").unwrap();

            cursor.first();
            for i in 1..10 {
                assert_eq!(cursor.column(0), Some(json!(i)));
                cursor.next();
            }
        }

        btree.debug();
    }

    #[test]
    fn test_search_many() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open_readonly("testing").is_none());

        btree.create_tree("testing");

        // Test we can insert a value
        {
            let mut cursor = btree.open_readwrite("testing").unwrap();

            for i in 1..10 {
                cursor.insert(i, vec![json!(i)]);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor = btree.open_readonly("testing").unwrap();

            cursor.find(7);

            for i in 7..10 {
                assert_eq!(cursor.column(0), Some(json!(i)));
                cursor.next();
            }
        }

        btree.debug();
    }
}
