use std::ops::{Deref, DerefMut};

use crate::{
    node::{self, SearchResult},
    pager::{self, Pager},
};

use self::stack::PushResult::{Done, Grew};

type Tuple = std::vec::Vec<serde_json::Value>;

mod stack {
    use crate::{node::{self}, pager};

    use super::Tuple;

    type NodePage = node::NodePage<u64, Tuple>;
    type LeafNodePage = node::LeafNodePage<u64, Tuple>;

    /// a pair of the page number and the index in that page 
    type StackItem = (u32, usize); 
    type Stack = Vec<StackItem>;

    pub struct PartialSearchStack<'a> {
        pager: &'a mut pager::Pager,
        stack: Stack,

        // the "top" of the stack
        next: u32
    }

    pub struct SearchStack<'a> {
        pager: &'a mut pager::Pager,
        stack: Stack,

        /// The location in the node pointed to by the stack which we were looking for
        top: StackItem,
    }

    pub enum PushResult<'a> {
        /// The push resulted in finding a new child node and is now also pointing to that
        Grew(PartialSearchStack<'a>),

        /// The push resulted in finding the location we were searching for, we now have the entire
        /// path to the node we were looking for
        Done(SearchStack<'a>)
    }

    impl<'a> PartialSearchStack<'a> {
        pub fn new(pager: &'a mut pager::Pager, root_page_number: u32) -> PartialSearchStack {
            PartialSearchStack {
                pager,
                stack: Default::default(),
                next: root_page_number,
            }
        }

        pub fn top(&self) -> NodePage {
            self.pager.get_and_decode(self.top_page_idx())
        }

        pub fn top_page_idx(&self) -> u32 {
            self.next
        }

        pub fn push(self, idx: u32) -> PushResult<'a> {
            todo!()
        }
    }

    impl SearchStack<'_> {
        pub fn insert(self, key: u64, value: Tuple) {
            // Insert value into the leaf(node) at the top of the stack

            let (page_idx, item_idx) = self.top;
            let mut page: LeafNodePage = self.pager.get_and_decode(page_idx);

            page.insert_item_at_index(item_idx, key, value);

            // TODO: handle encode faling due to lack of space
            self.pager.encode_and_set(page_idx, page);

            // loop until all splits are resolved
            // if we have no split -> return
            // if we have a split (key, value, )
        }
    }
}

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
    fn insert(&mut self, key: u64, value: Tuple) {
        // we maintain a stack of the nodes we decended through in case of needing to split them.
        // Starting at the root, we search to find:
        //   an empty place to put the new value
        //   en existing value to replace
        let mut stack = stack::PartialSearchStack::new(&mut self.pager, self.root_page);

        loop {
            match stack.top().search(&key) {
                SearchResult::Found(mut leaf, insertion_index) => {
                    // We found the index in the node where an existing value for this key exists
                    // we need to replace it with our value
                    let page_idx = stack.top_page_idx();

                    leaf.set_item_at_index(insertion_index, value);

                    // TODO: there is going to be a panic if the new value does not fit on this page...
                    self.pager.encode_and_set(page_idx, leaf);

                    return;
                }
                SearchResult::GoDown(child_index) => {
                    // The node does not contain the value, instead we found the index of a child of this node where the value should be inserted instead
                    // we need to go deeper.

                    stack = match stack.push(child_index) {
                        Done(stack) => { 
                            // We reached a leaf node where we need to insert this as a new value
                            stack.insert(key, value);
                            return;
                        },
                        Grew(stack) => { 
                            // We found an existing child at this location, continue the search there
                            stack
                        },
                    };
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
        let _empty_root_node = node::LeafNodePage::<u64, Tuple>::default();
        // Encode and set the empty_root_node in the pager
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
