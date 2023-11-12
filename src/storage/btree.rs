use std::cell::{Ref, RefCell, RefMut};
use std::io::Write;
use std::sync::Arc;
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

use crate::storage::cell::Cell;
use crate::storage::node::{NodePage, OverflowPage, SearchResult};

use super::btree_verify::VerifyError;
use super::cell::Value;
use super::node::{self, InteriorNodePage};
use super::pager::{self, Pager};
use super::{btree_graph, btree_verify, CellReader};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorState {
    tree_name: String,

    /// key for the item pointed to by the cursor
    stack: Vec<InteriorNodeIterator>,
    leaf_iterator: Option<LeafNodeIterator>,
}

#[derive(Debug, Clone)]
pub struct CursorHandle {
    pager: Arc<RefCell<Pager>>,
    state: CursorState,
}

impl CursorHandle {
    pub fn open_readonly<'a>(&'a mut self) -> Cursor<'a, Ref<'a, Pager>> {
        let pager = RefCell::borrow(&self.pager);
        Cursor {
            pager,
            cursor_state: &mut self.state,
        }
    }

    pub fn open_readwrite<'a>(&'a mut self) -> Cursor<'a, RefMut<'a, Pager>> {
        let pager = RefCell::borrow_mut(&self.pager);
        Cursor {
            pager,
            cursor_state: &mut self.state,
        }
    }
}

pub struct Cursor<'a, PagerRef> {
    pager: PagerRef,
    cursor_state: &'a mut CursorState,
}

/// identifies the page index of the interior node and the index of the child curently selected
type InteriorNodeIterator = (u32, usize);

/// identifies the page index of the leaf node and the index of the entry curently selected
type LeafNodeIterator = (u32, usize);

const NULL: serde_json::Value = serde_json::Value::Null;
const CHUNK_THRESHOLD: usize = 55;

/// Mutable cursor implementation
impl<'a, PagerRef> Cursor<'a, PagerRef>
where
    PagerRef: DerefMut<Target = Pager>,
{
    pub fn insert(&mut self, key: u64, value: Value) {
        assert!(value.len() > 0);

        // values must be small enough so that a few can fit on each page
        // this is to ensure when splitting nodes we always end up with at least 50% free space
        let (first_part, continuation) = if value.len() > CHUNK_THRESHOLD {
            let (first_part, rest) = value.split_at(CHUNK_THRESHOLD);
            let second_part = split_and_store(&mut self.pager, rest);
            (first_part.to_owned(), Some(second_part))
        } else {
            (value, None)
        };

        let cell = Cell::new(key, first_part, continuation);

        // we maintain a stack of the nodes we decended through in case of needing to split them.
        // Starting at the root, we search to find:
        //   an empty place to put the new value
        //   en existing value to replace
        let mut stack = Vec::new();

        let root_page = self
            .pager
            .get_root_page(&self.cursor_state.tree_name)
            .unwrap();
        stack.push(root_page);

        loop {
            let top_page_idx = *stack.last().unwrap();
            let mut top_page: NodePage = self.pager.get_and_decode(top_page_idx);
            match top_page.search(&key) {
                SearchResult::Found(insertion_index) => {
                    // We found the index in the node where an existing value for this key exists
                    // we need to replace it with our value

                    top_page.set_item_at_index(insertion_index, cell);

                    self.update_page(top_page, stack);

                    break;
                }
                SearchResult::NotPresent(item_idx) => {
                    top_page.insert_item_at_index(item_idx, cell);

                    self.update_page(top_page, stack);

                    break;
                }
                SearchResult::GoDown(_child_index, child_page_idx) => {
                    // The node does not contain the value, instead we found the index of a child of this node where the value should be inserted instead
                    // we need to go deeper.

                    stack.push(child_page_idx);
                }
            }
        }
    }

    /// Updates a page with new content
    ///
    /// # Args
    /// * `stack` the path of pages to the modified page, last entry in the stack is the one which needs updating
    /// * `modified_page` the updated content to be saved to the page identified by the stack
    fn update_page(&mut self, modified_page: NodePage, stack: Vec<u32>) {
        let modified_page_idx = stack.last().unwrap();
        let result = self.pager.encode_and_set(modified_page_idx, &modified_page);

        if result.is_ok() {
            return;
        }

        let result = result.unwrap_err();

        match result {
            pager::EncodingError::NotEnoughSpaceInPage => {
                self.split_page(modified_page, stack);
            }
        }
    }

    fn split_page(&mut self, page_to_be_split: NodePage, mut stack: Vec<u32>) {
        let top_page_idx = stack.pop().unwrap();
        let (top_page, extra_page) = page_to_be_split.split();
        let extra_page_idx = self.pager.allocate();

        let extra_page_first_key = extra_page.smallest_key();

        self.pager
            .encode_and_set(top_page_idx, top_page)
            .expect("After split, parts are smaller");
        self.pager
            .encode_and_set(extra_page_idx, extra_page)
            .expect("After split, parts are smaller");

        // We now must put our new page into the tree.
        // The new page is at index: extra_page_idx, and the first key on that new page is extra_page_first_key
        if stack.len() != 0 {
            // We must update the parent node
            // A reference to the new extra_page must be inserted into the parent node
            // Our reference in our parent might need updating???

            let parent_node_idx = stack.pop().unwrap();

            let parent_node: NodePage = self.pager.get_and_decode(parent_node_idx);

            let mut parent_interior_node = parent_node.interior().unwrap();

            parent_interior_node.insert_child_page(extra_page_first_key, extra_page_idx);

            let parent_interior_node = parent_interior_node.node();

            let result = self
                .pager
                .encode_and_set(parent_node_idx, parent_interior_node.clone());

            match result {
                Err(pager::EncodingError::NotEnoughSpaceInPage) => {
                    stack.push(parent_node_idx);
                    self.split_page(parent_interior_node, stack);
                }
                Ok(_) => {}
            }
        } else {
            // We have just split the root node...
            // We must now create the first interior node and insert two new child pages
            let interior_node =
                InteriorNodePage::new(top_page_idx, extra_page_first_key, extra_page_idx);

            let root_node = NodePage::Interior(interior_node);

            let root_node_idx = self.pager.allocate();
            self.pager.encode_and_set(root_node_idx, root_node).unwrap();
            self.pager
                .set_root_page(&self.cursor_state.tree_name, root_node_idx);
        }
    }
}

/// Imutable cursor implementation
impl<'a, PagerRef> Cursor<'a, PagerRef>
where
    PagerRef: Deref<Target = Pager>,
{
    /// Move the cursor to point at the first row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// first row to point to
    pub fn first(&mut self) {
        // Take the tree identified by the root page number, and find its left most node and
        // find its smallest entry

        let root_page = self
            .pager
            .get_root_page(&self.cursor_state.tree_name)
            .unwrap();
        self.select_leftmost_of_idx(root_page)
    }

    fn select_leftmost_of_idx(&mut self, page_idx: u32) {
        let mut page_idx = page_idx;

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);
            match page {
                node::NodePage::Leaf(l) => {
                    // We found the first leaf in the tree.
                    // TODO: Maybe store a readonly copy of this leaf node instead of this `leaf_iterator`
                    self.cursor_state.leaf_iterator = Some((page_idx, 0));
                    return;
                }
                node::NodePage::Interior(i) => {
                    self.cursor_state.stack.push((page_idx, 0));
                    page_idx = i.get_child_page_by_index(0);
                }
                NodePage::OverflowPage(_) => panic!(),
            }
        }
    }

    fn select_rightmost_of_idx(&mut self, page_idx: u32) {
        let mut page_idx = page_idx;

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);
            match page {
                node::NodePage::Leaf(l) => {
                    // We found the first leaf in the tree.
                    // TODO: Maybe store a readonly copy of this leaf node instead of this `leaf_iterator`
                    self.cursor_state.leaf_iterator = Some((page_idx, l.num_items() - 1));
                    return;
                }
                node::NodePage::Interior(i) => {
                    self.cursor_state.stack.push((page_idx, i.num_edges() - 1));
                    page_idx = i.get_child_page_by_index(i.num_edges() - 1);
                }
                NodePage::OverflowPage(_) => panic!(),
            }
        }
    }

    /// Move the cursor to point at the last row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// last row to point to
    pub fn last(&mut self) {
        // Take the tree identified by the root page number, and find its right most node and
        // find its largest entry.
        let root_page_idx = self
            .pager
            .get_root_page(&self.cursor_state.tree_name)
            .unwrap();
        let root_page: NodePage = self.pager.get_and_decode(root_page_idx);

        let mut page = root_page;
        let mut page_idx = root_page_idx;
        loop {
            match page {
                node::NodePage::Leaf(l) => {
                    // We found the first leaf in the tree.
                    // TODO: Maybe store a readonly copy of this leaf node instead of this `leaf_iterator`
                    self.cursor_state.leaf_iterator = Some((page_idx, l.num_items() - 1));
                    return;
                }
                node::NodePage::Interior(_i) => todo!(),
                node::NodePage::OverflowPage(_) => panic!(),
            }
        }
    }

    /// Move the cursor to point at the row in the btree identified by the given key
    /// This may result in the cursor not pointing to a row if there is no
    /// row found with that key to point to
    pub fn find(&mut self, key: u64) {
        let root_page_idx = self
            .pager
            .get_root_page(&self.cursor_state.tree_name)
            .unwrap();
        let mut page_idx = root_page_idx;

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);

            match page.search(&key) {
                SearchResult::Found(index) => {
                    self.cursor_state.leaf_iterator = Some((page_idx, index));
                    return;
                }
                SearchResult::NotPresent(index) => {
                    self.cursor_state.leaf_iterator = Some((page_idx, index));
                    // TODO: does the caller need to know this isnt what they were looking for?
                    return;
                }
                SearchResult::GoDown(c_idx, c) => {
                    self.cursor_state.stack.push((page_idx, c_idx));
                    // we should continue searching at the child page below
                    page_idx = c;
                }
            }
        }
    }

    fn row_key(&self) -> Option<u64> {
        let cell = self.get_entry()?;

        Some(cell.key())
    }

    pub fn get_entry<'b>(&'b self) -> Option<CellReader<'b>> {
        let (leaf_page_number, entry_index) = self.cursor_state.leaf_iterator?;

        CellReader::new(&self.pager, leaf_page_number, entry_index)
    }

    /// Move the cursor to point at the next item in the btree
    pub fn next(&mut self) {
        // function takes a curent index and the number of indexes, and returns Some(idx) where idx is the next index to consider
        // or none if there are no more on this page
        let next_idx = |curent: usize, count| {
            if curent + 1 < count {
                Some(curent + 1)
            } else {
                None
            }
        };

        // function to move the cursor to the next item to consider in subtree identified by page_idx in the given direction
        let select_first_in_direction = Self::select_leftmost_of_idx;

        self.move_in_direction(next_idx, select_first_in_direction);
    }

    /// Move the cursor to point at the next item in the btree
    pub fn prev(&mut self) {
        // function takes a curent index and the number of indexes, and returns Some(idx) where idx is the next index to consider
        // or none if there are no more on this page
        let next_idx = |curent: usize, _count| {
            if curent != 0 {
                Some(curent - 1)
            } else {
                None
            }
        };

        // function to move the cursor to the next item to consider in subtree identified by page_idx in the given direction
        let select_first_in_direction = Self::select_rightmost_of_idx;

        self.move_in_direction(next_idx, select_first_in_direction);
    }

    fn move_in_direction(
        &mut self,
        next_idx: impl Fn(usize, usize) -> Option<usize>,
        select_first_in_direction: impl Fn(&mut Self, u32),
    ) {
        if self.cursor_state.leaf_iterator.is_none() {
            return;
        }
        let (page_number, entry_index) = self.cursor_state.leaf_iterator.unwrap();
        let page: NodePage = self.pager.get_and_decode(page_number);
        let page = page
            .leaf()
            .expect("Values are always supposed to be in leaf pages");
        let num_items_in_leaf = page.num_items();
        if let Some(entry_index) = next_idx(entry_index, num_items_in_leaf) {
            self.cursor_state.leaf_iterator = Some((page_number, entry_index));
            return;
        }
        loop {
            // if the stack is empty then we have no more places to go
            if self.cursor_state.stack.is_empty() {
                self.cursor_state.leaf_iterator = None;
                return;
            }

            let (curent_interior_idx, curent_edge) = self.cursor_state.stack.pop().unwrap();

            let curent_interior: NodePage = self.pager.get_and_decode(curent_interior_idx);

            let curent_interior = curent_interior
                .interior()
                .expect("The stack should only contain interior pages");
            let edge_count = curent_interior.num_edges();

            // if we there are more edges to the right:
            if let Some(next_edge) = next_idx(curent_edge, edge_count) {
                // select the next edge in the curent page
                self.cursor_state
                    .stack
                    .push((curent_interior_idx, next_edge));

                // find the page_idx for the new edge
                let curent_edge_idx = curent_interior.get_child_page_by_index(next_edge);

                // then select the first item in the leftmost leaf of that subtree
                select_first_in_direction(self, curent_edge_idx);
                return;
            }

            // if there are no more edges in this node:
            //    pop this item off the stack and repeat
            // pop already happened
        }
    }

    pub fn debug(&self, message: &str) {
        self.pager.debug(message);
    }

    pub fn verify(&self) -> Result<(), VerifyError> {
        btree_verify::verify(&self.pager, &self.cursor_state.tree_name)
    }
}

fn split_and_store(pager: &mut Pager, mut rest: &[u8]) -> u32 {
    // [first] [next] [next+1] ...
    //  ^ page_idx
    //          ^ next_page_idx

    // [next] [last]
    //  ^ page_idx
    //         ^ next_page_idx

    // after loop exits:
    // [last]
    //  ^ page_idx

    assert!(rest.len() > 0);

    const OVERFLOW_LIMIT: usize = 100;

    let mut page_idx = pager.allocate();
    let first_page_idx = page_idx;

    while rest.len() > OVERFLOW_LIMIT {
        // We know there will be at least one more page following this...
        let next_page_idx = pager.allocate();
        let (first, the_rest) = rest.split_at(OVERFLOW_LIMIT);
        let overflow_page =
            NodePage::OverflowPage(OverflowPage::new(first.to_owned(), Some(next_page_idx)));
        pager
            .encode_and_set(page_idx, overflow_page)
            .expect("to be able to store overflow pages");
        rest = the_rest;
        page_idx = next_page_idx;
    }

    let overflow_page = NodePage::OverflowPage(OverflowPage::new(rest.to_owned(), None));
    pager
        .encode_and_set(page_idx, overflow_page)
        .expect("to be able to store overflow pages");

    first_page_idx
}

pub struct BTree {
    pager: Arc<RefCell<pager::Pager>>,
}

impl BTree {
    pub fn new(path: &str) -> BTree {
        BTree {
            pager: Arc::new(RefCell::new(Pager::new(path))),
        }
    }

    pub fn open(&self, tree_name: &str) -> Option<CursorHandle> {
        // Check if the root page actually exists, or return None
        self.pager.borrow().get_root_page(tree_name)?;

        let state = CursorState {
            stack: vec![],
            leaf_iterator: None,
            tree_name: tree_name.to_owned(),
        };

        Some(CursorHandle {
            pager: self.pager.clone(),
            state,
        })
    }

    /// Create a new tree with the given name, tree must not already exist
    pub fn create_tree(&mut self, tree_name: &str) {
        let mut pager = self.pager.borrow_mut();

        assert!(pager.get_root_page(tree_name).is_none());
        let idx = pager.allocate();
        pager.set_root_page(tree_name, idx);
        let empty_leaf_node = node::LeafNodePage::default();
        let empty_root_node = node::NodePage::Leaf(empty_leaf_node);
        // Encode and set the empty_root_node in the pager
        pager.encode_and_set(idx, empty_root_node).unwrap();
    }

    pub fn debug(&self, message: &str) {
        self.pager.borrow().debug(message)
    }

    pub fn dump_to_file(&self, output_path: &std::path::Path) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .open(output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        write!(writer, "{}", self)?;
        Ok(())
    }

    pub fn verify(&self) -> Result<(), VerifyError> {
        btree_verify::verify_all_trees(&self.pager.borrow())
    }
}

impl Display for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        btree_graph::dump(f, &self.pager.borrow())?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::CellReader;
    use crate::test::TestDb;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::io::Read;

    use super::BTree;

    #[test]
    fn test_create_blank() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open("testing").is_none());

        btree.create_tree("testing");

        // Test we can take two readonly cursors at the same time
        {
            let mut _cursor1 = btree.open("testing").unwrap();
            let mut _cursor2 = btree.open("testing").unwrap();
        }

        // Test the new table is empty, when using a readonly cursor
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            assert!(cursor.get_entry().is_none());
        }

        // Test the new table is empty, when using a readwrite cursor
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readwrite();

            cursor.first();
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_create_and_insert() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open("testing").is_none());

        btree.create_tree("testing");

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readwrite();

            cursor.insert(42, vec![42, 255, 64]);
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();
            let mut buf = [0; 3];
            cursor.get_entry().unwrap().read(&mut buf).unwrap();
            assert_eq!(&buf, &[42, 255, 64]);
        }

        btree.debug("");
    }

    #[test]
    fn test_insert_many() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open("testing").is_none());

        btree.create_tree("testing");

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readwrite();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readonly();

            cursor.first();
            for i in 1..10u64 {
                let mut buf = [0; 8];
                cursor.get_entry().unwrap().read(&mut buf).unwrap();
                assert_eq!(buf, i.to_be_bytes());
                cursor.next();
            }
        }

        btree.debug("");
        println!("{}", btree);
    }

    #[test]
    fn test_search_many() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open("testing").is_none());

        btree.create_tree("testing");

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readwrite();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open("testing").unwrap();
            let mut cursor = cursor_handle.open_readonly();

            cursor.find(7);

            for i in 7..10u64 {
                let mut buf = [0; 8];
                cursor.get_entry().unwrap().read(&mut buf).unwrap();
                assert_eq!(buf, i.to_be_bytes());
                cursor.next();
            }
        }

        btree.debug("");
    }

    #[test]
    fn multi_level_insertion() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open("testing").is_none());

        btree.create_tree("testing");

        let mut cursor_handle = btree.open("testing").unwrap();
        let mut cursor = cursor_handle.open_readwrite();

        let long_string = |s: &str, num| s.repeat(num).into_bytes();

        cursor.insert(1, long_string("AA", 263));
        cursor.insert(10, long_string("BBBB", 900));
        cursor.debug("");
        cursor.insert(11, long_string("C", 1));

        cursor.first();
        cursor.debug("");
        cursor.verify().unwrap();

        assert_eq!(1, cursor.row_key().unwrap());
        cursor.next();
        assert_eq!(10, cursor.row_key().unwrap());
        cursor.next();
        assert_eq!(11, cursor.row_key().unwrap());
        cursor.next();
        assert!(cursor.row_key().is_none());

        // Must close cursor or we cant print the btree below
        drop(cursor);
        drop(cursor_handle);

        println!("{btree}");
    }

    fn do_test_ordering(
        elements: &[(u64, (char, usize))],
        my_btree: &mut BTree,
        ordering_forwards: bool,
    ) {
        println!("Test: {elements:?}");

        let mut rust_btree = BTreeMap::new();

        my_btree.create_tree("testing");

        let mut cursor_handle = my_btree.open("testing").unwrap();
        let mut cursor = cursor_handle.open_readwrite();

        for (k, (v, len)) in elements.to_owned() {
            cursor.verify().unwrap();
            let value = v.to_string().repeat(len).as_bytes().to_vec();

            rust_btree.insert(k, value.clone());
            cursor.insert(k, value);
        }

        cursor.verify().unwrap();
        // cursor.debug("Before order check");

        if ordering_forwards {
            cursor.first();
        } else {
            cursor.last();
        }

        let rust_btree_iter: Box<dyn Iterator<Item = _>> = if ordering_forwards {
            Box::new(rust_btree.iter())
        } else {
            Box::new(rust_btree.iter().rev())
        };

        for (_key, actual_value) in rust_btree_iter {
            // println!("Key: {key} {my_value}");
            let mut buf = vec![];
            cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
            assert_eq!(actual_value, &buf);

            if ordering_forwards {
                cursor.next();
            } else {
                cursor.prev();
            }
        }

        cursor.verify().unwrap();
    }

    #[test]
    fn large_test_case() {
        let large_test_case = [(28, ('A', 976))];

        let test = TestDb::default();
        let mut btree = test.btree;
        do_test_ordering(&large_test_case, &mut btree, true);

        println!("{btree}");
    }

    proptest! {
        #[test]
        fn test_ordering(ordering: bool, elements in prop::collection::vec(&(50..60u64, &(prop::char::range('A', 'Z'), 500..600usize)), 10..20usize)) {
            let test = TestDb::default();
            let mut btree = test.btree;
            do_test_ordering(elements.as_slice(), &mut btree, ordering);
        }
    }
}
