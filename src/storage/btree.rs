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
    root_page: u32,

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
    #[allow(dead_code)]
    pub fn root_page(&self) -> u32 {
        self.state.root_page
    }

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

#[allow(dead_code)]
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

        stack.push(self.cursor_state.root_page);

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
            pager::EncodingError::SerializationError(e) => {
                panic!("Serialization error: {}", e);
            }
        }
    }

    /// Split an overfull page into two halves and link them into the tree.
    ///
    /// `stack` is the path from root to the overfull page (last element = overfull page).
    ///
    /// Non-root split — insert a new child pointer into the parent:
    ///
    /// ```text
    ///        [parent]              [parent]
    ///           |            =>    /      \
    ///       [overfull]         [left]  [right]
    /// ```
    ///
    /// Root split — the root page index stays stable (the SQLite approach).
    /// The two halves move to new pages; the root is rewritten as an interior node:
    ///
    /// ```text
    ///       [root:overfull]       [root:interior]
    ///                        =>    /          \
    ///                          [left]      [right]
    /// ```
    ///
    fn split_page(&mut self, overfull_page: NodePage, mut stack: Vec<u32>) {
        let overfull_idx = stack.pop().unwrap();

        // 1. Split the overfull page into left and right halves
        let (left_half, right_half) = overfull_page.split();
        let right_idx = self.pager.allocate();
        let right_first_key = right_half.smallest_key();

        // 2. Write both halves to disk
        self.pager
            .encode_and_set(overfull_idx, left_half)
            .expect("After split, parts are smaller");
        self.pager
            .encode_and_set(right_idx, right_half)
            .expect("After split, parts are smaller");

        // 3. Link the new right page into the tree
        if stack.len() != 0 {
            // Non-root: add a child pointer for the right page to the parent
            let parent_idx = stack.pop().unwrap();
            let parent_page: NodePage = self.pager.get_and_decode(parent_idx);
            let mut parent_interior = parent_page.interior().unwrap();
            parent_interior.insert_child_page(right_first_key, right_idx);
            let parent_node = parent_interior.node();

            let result = self.pager.encode_and_set(parent_idx, parent_node.clone());

            // If the parent is now overfull, recursively split it
            match result {
                Err(pager::EncodingError::NotEnoughSpaceInPage) => {
                    stack.push(parent_idx);
                    self.split_page(parent_node, stack);
                }
                Err(pager::EncodingError::SerializationError(e)) => {
                    panic!("Serialization error: {}", e);
                }
                Ok(_) => {}
            }
        } else {
            // Root: keep the root at the same page index.
            // Move the left half (currently at overfull_idx) to a fresh page,
            // then overwrite the root with a new interior node.
            let left_page: NodePage = self.pager.get_and_decode(overfull_idx);
            let left_idx = self.pager.allocate();
            self.pager.encode_and_set(left_idx, left_page).unwrap();

            let interior = InteriorNodePage::new(left_idx, right_first_key, right_idx);
            self.pager
                .encode_and_set(overfull_idx, NodePage::Interior(interior))
                .unwrap();
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
        self.select_leftmost_of_idx(self.cursor_state.root_page)
    }

    fn select_leftmost_of_idx(&mut self, page_idx: u32) {
        let mut page_idx = page_idx;

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);
            match page {
                node::NodePage::Leaf(_l) => {
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
        let root_page_idx = self.cursor_state.root_page;
        let root_page: NodePage = self.pager.get_and_decode(root_page_idx);

        let page = root_page;
        let page_idx = root_page_idx;
        loop {
            match page {
                node::NodePage::Leaf(l) => {
                    if l.num_items() == 0 {
                        self.cursor_state.leaf_iterator = None;
                    } else {
                        self.cursor_state.leaf_iterator = Some((page_idx, l.num_items() - 1));
                    }
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
        let mut page_idx = self.cursor_state.root_page;

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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn debug(&self, message: &str) {
        self.pager.debug(message);
    }

    pub fn verify(&self) -> Result<(), VerifyError> {
        btree_verify::verify(&self.pager, self.cursor_state.root_page)
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

#[derive(Clone)]
pub struct BTree {
    pager: Arc<RefCell<pager::Pager>>,
}

impl BTree {
    pub fn new(path: &str) -> BTree {
        let btree = BTree {
            pager: Arc::new(RefCell::new(Pager::new(path))),
        };

        // Bootstrap db_schema table if this is a new (empty) database
        if btree.pager.borrow().get_file_size_pages() == 0 {
            btree.bootstrap_schema();
        }

        btree
    }

    /// Bootstrap a new database by creating the db_schema catalog table.
    /// Inserts a self-referencing row so the catalog describes itself.
    fn bootstrap_schema(&self) {
        let schema_root = {
            let mut pager = self.pager.borrow_mut();
            let idx = pager.allocate();
            let empty_leaf = node::LeafNodePage::default();
            pager
                .encode_and_set(idx, node::NodePage::Leaf(empty_leaf))
                .unwrap();
            pager.set_schema_root_page(idx);
            idx
        };

        self.insert_schema_entry(
            0,
            "table",
            "db_schema",
            "db_schema",
            schema_root,
            "CREATE TABLE db_schema (type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT)",
        );
    }

    /// Returns the root page of the db_schema catalog table, if the database
    /// has been bootstrapped.
    pub fn schema_root_page(&self) -> Option<u32> {
        self.pager.borrow().get_schema_root_page()
    }

    pub fn open(&self, root_page: u32) -> CursorHandle {
        let state = CursorState {
            stack: vec![],
            leaf_iterator: None,
            root_page,
        };

        CursorHandle {
            pager: self.pager.clone(),
            state,
        }
    }

    /// Create a new tree, returning its root page number.
    pub fn create_tree(&mut self) -> u32 {
        let mut pager = self.pager.borrow_mut();
        let idx = pager.allocate();
        let empty_leaf_node = node::LeafNodePage::default();
        let empty_root_node = node::NodePage::Leaf(empty_leaf_node);
        pager.encode_and_set(idx, empty_root_node).unwrap();
        idx
    }

    /// Insert a row into the db_schema catalog table.
    /// `key` is the B-tree key for the catalog row (caller manages key allocation).
    /// The row is stored as a JSON array: [type, name, tbl_name, rootpage, sql].
    ///
    /// If the insert causes the catalog's root page to split, the new root
    /// is automatically persisted to ZeroPage.
    pub fn insert_schema_entry(
        &self,
        key: u64,
        obj_type: &str,
        name: &str,
        tbl_name: &str,
        rootpage: u32,
        sql: &str,
    ) {
        let schema_root = self.schema_root_page().expect("db_schema not bootstrapped");
        let row = serde_json::to_vec(&serde_json::json!([
            obj_type, name, tbl_name, rootpage, sql
        ]))
        .unwrap();
        let mut cursor = self.open(schema_root);
        cursor.open_readwrite().insert(key, row);
    }

    /// Look up a table's root page and DDL by scanning db_schema for a matching name.
    /// Returns (rootpage, sql) if found.
    pub fn lookup_table(&self, table_name: &str) -> Option<(u32, String)> {
        let schema_root = self.schema_root_page()?;
        let mut cursor = self.open(schema_root);
        let mut c = cursor.open_readonly();
        c.first();
        loop {
            let entry = c.get_entry();
            match entry {
                None => return None,
                Some(mut reader) => {
                    let values = reader.decode_as_json_array();
                    // Row format: [type, name, tbl_name, rootpage, sql]
                    if values.len() >= 5 {
                        let obj_type = values[0].as_str().unwrap_or("");
                        let name = values[1].as_str().unwrap_or("");
                        if obj_type == "table" && name == table_name {
                            let rootpage = values[3].as_u64().unwrap() as u32;
                            let sql = values[4].as_str().unwrap_or("").to_string();
                            return Some((rootpage, sql));
                        }
                    }
                }
            }
            c.next();
        }
    }

    #[allow(dead_code)]
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
}

impl Display for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        btree_graph::dump(f, &self.pager.borrow())?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use crate::test::TestDb;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::io::Read;

    use super::BTree;

    #[test]
    fn test_create_blank() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        // Test we can take two readonly cursors at the same time
        {
            let mut _cursor1 = btree.open(root);
            let mut _cursor2 = btree.open(root);
        }

        // Test the new table is empty, when using a readonly cursor
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            assert!(cursor.get_entry().is_none());
        }

        // Test the new table is empty, when using a readwrite cursor
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();

            cursor.first();
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_create_and_insert() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();

            cursor.insert(42, vec![42, 255, 64]);
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
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

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
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

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
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

        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
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

        let root = my_btree.create_tree();

        let mut cursor_handle = my_btree.open(root);
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

    // ========================================================================
    // Schema catalog tests
    // ========================================================================

    #[test]
    fn test_bootstrap_creates_schema() {
        let test = TestDb::default();
        let btree = test.btree;

        // db_schema root page should exist
        let schema_root = btree.schema_root_page();
        assert!(schema_root.is_some());
        let schema_root = schema_root.unwrap();

        // Read the self-referencing row from db_schema
        let mut cursor = btree.open(schema_root);
        let mut c = cursor.open_readonly();
        c.first();
        let entry = c.get_entry();
        assert!(entry.is_some());

        let values = entry.unwrap().decode_as_json_array();
        assert_eq!(values[0], "table");
        assert_eq!(values[1], "db_schema");
        assert_eq!(values[2], "db_schema");
        assert_eq!(values[3], schema_root as u64);
        assert!(values[4]
            .as_str()
            .unwrap()
            .starts_with("CREATE TABLE db_schema"));
    }

    #[test]
    fn test_lookup_table_self() {
        let test = TestDb::default();
        let btree = test.btree;

        // Should be able to look up db_schema itself
        let result = btree.lookup_table("db_schema");
        assert!(result.is_some());

        let (rootpage, sql) = result.unwrap();
        assert_eq!(rootpage, btree.schema_root_page().unwrap());
        assert!(sql.starts_with("CREATE TABLE db_schema"));
    }

    #[test]
    fn test_lookup_table_not_found() {
        let test = TestDb::default();
        let btree = test.btree;

        let result = btree.lookup_table("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_insert_and_lookup_schema_entry() {
        let test = TestDb::default();
        let mut btree = test.btree;

        // Create a new table
        let root = btree.create_tree();
        btree.insert_schema_entry(
            1,
            "table",
            "users",
            "users",
            root,
            "CREATE TABLE users (id INTEGER, name TEXT)",
        );

        // Look it up
        let result = btree.lookup_table("users");
        assert!(result.is_some());

        let (rootpage, sql) = result.unwrap();
        assert_eq!(rootpage, root);
        assert_eq!(sql, "CREATE TABLE users (id INTEGER, name TEXT)");
    }

    #[test]
    fn test_insert_multiple_schema_entries() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let users_root = btree.create_tree();
        btree.insert_schema_entry(
            1,
            "table",
            "users",
            "users",
            users_root,
            "CREATE TABLE users (id INTEGER, name TEXT)",
        );

        let orders_root = btree.create_tree();
        btree.insert_schema_entry(
            2,
            "table",
            "orders",
            "orders",
            orders_root,
            "CREATE TABLE orders (id INTEGER, user_id INTEGER, total REAL)",
        );

        // Both should be findable
        let (rp, _) = btree.lookup_table("users").unwrap();
        assert_eq!(rp, users_root);

        let (rp, _) = btree.lookup_table("orders").unwrap();
        assert_eq!(rp, orders_root);

        // db_schema still works
        let (rp, _) = btree.lookup_table("db_schema").unwrap();
        assert_eq!(rp, btree.schema_root_page().unwrap());
    }

    #[test]
    fn test_scan_schema_via_cursor() {
        // db_schema can be scanned with the regular cursor API, just like any other table.
        let test = TestDb::default();
        let mut btree = test.btree;

        let users_root = btree.create_tree();
        btree.insert_schema_entry(
            1,
            "table",
            "users",
            "users",
            users_root,
            "CREATE TABLE users (id INTEGER)",
        );
        let orders_root = btree.create_tree();
        btree.insert_schema_entry(
            2,
            "table",
            "orders",
            "orders",
            orders_root,
            "CREATE TABLE orders (id INTEGER)",
        );

        // Look up db_schema's rootpage via lookup_table, then scan it with a cursor
        let (schema_root, _) = btree.lookup_table("db_schema").unwrap();
        let mut cursor = btree.open(schema_root);
        let mut c = cursor.open_readonly();
        c.first();
        let mut names = Vec::new();
        loop {
            match c.get_entry() {
                None => break,
                Some(mut reader) => {
                    let row = reader.decode_as_json_array();
                    if row.len() >= 5 && row[0].as_str() == Some("table") {
                        names.push(row[1].as_str().unwrap().to_string());
                    }
                }
            }
            c.next();
        }
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"db_schema".to_string()));
        assert!(names.contains(&"users".to_string()));
        assert!(names.contains(&"orders".to_string()));
    }

    #[test]
    fn test_catalog_root_stable_after_splits() {
        // Insert enough schema entries to force the db_schema B-tree root to split.
        // With stable root pages, schema_root_page() must stay the same,
        // and all entries must still be accessible via lookup_table().
        let test = TestDb::default();
        let mut btree = test.btree;

        let initial_root = btree.schema_root_page().unwrap();

        let mut roots = Vec::new();
        for i in 0..40 {
            let name = format!("table_{:03}", i);
            let ddl = format!("CREATE TABLE {} (id INTEGER, data TEXT)", name);
            let root = btree.create_tree();
            roots.push((name.clone(), root));
            btree.insert_schema_entry(i + 100, "table", &name, &name, root, &ddl);
        }

        // The root page should remain stable
        let final_root = btree.schema_root_page().unwrap();
        assert_eq!(
            initial_root, final_root,
            "Schema root page should stay stable after splits"
        );

        // All entries should still be accessible
        for (name, root) in &roots {
            let result = btree.lookup_table(name);
            assert!(result.is_some(), "Failed to find table '{}'", name);
            let (rp, _) = result.unwrap();
            assert_eq!(rp, *root, "Wrong rootpage for table '{}'", name);
        }

        // db_schema self-reference should still work
        let (rp, _) = btree.lookup_table("db_schema").unwrap();
        assert_eq!(rp, initial_root);
    }

    #[test]
    fn test_user_table_root_stable_after_splits() {
        // User table root pages should remain stable after many inserts
        // that cause multiple levels of splits.
        let test = TestDb::default();
        let mut btree = test.btree;

        let initial_root = btree.create_tree();
        btree.insert_schema_entry(
            1,
            "table",
            "big_table",
            "big_table",
            initial_root,
            "CREATE TABLE big_table (id INTEGER, data TEXT)",
        );

        // Insert enough rows to force multiple root splits
        {
            let mut cursor = btree.open(initial_root);
            let mut c = cursor.open_readwrite();
            for i in 0..100u64 {
                let value = format!("[{}, \"row_{}\"]", i, i);
                c.insert(i, value.into_bytes());
            }
        }

        // The catalog entry should still have the original rootpage
        let (catalog_root, _) = btree.lookup_table("big_table").unwrap();
        assert_eq!(catalog_root, initial_root, "Root page should stay stable");

        // Data should be accessible through the original rootpage
        let mut read_cursor = btree.open(initial_root);
        let mut c = read_cursor.open_readonly();
        c.first();
        let mut count = 0;
        while c.get_entry().is_some() {
            count += 1;
            c.next();
        }
        assert_eq!(count, 100, "All 100 rows should be accessible");
    }

    #[test]
    fn test_empty_table_scan() {
        // Verify first() on empty tree returns None
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_readonly();
        cursor.first();

        assert!(
            cursor.get_entry().is_none(),
            "Empty table should return None"
        );
    }

    #[test]
    fn test_duplicate_key_insert() {
        // Insert same key twice with different values, verify overwrite
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert key=1 with value "first"
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(1, b"first".to_vec());
        }

        // Insert key=1 again with value "second"
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(1, b"second".to_vec());
        }

        // Read back and verify it's "second"
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.find(1);
            let mut buf = vec![];
            cursor
                .get_entry()
                .expect("Key should exist")
                .read_to_end(&mut buf)
                .unwrap();
            assert_eq!(buf, b"second", "Value should be overwritten");
        }
    }

    #[test]
    fn test_find_nonexistent_key() {
        // find() for key not in tree
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert keys 1, 3, 5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(1, b"one".to_vec());
            cursor.insert(3, b"three".to_vec());
            cursor.insert(5, b"five".to_vec());
        }

        // Try to find key 2 (doesn't exist)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.find(2);
            // After find() for non-existent key, cursor should be positioned
            // but get_entry() should return None (or positioned at next key)
            // Current implementation positions at the spot where it would be inserted
            // For now, just verify it doesn't panic
        }
    }

    #[test]
    fn test_cursor_prev_from_middle() {
        // Insert 10 keys, navigate to middle, call prev(), verify correct key
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert keys 0-9
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 0..10u64 {
                cursor.insert(i, i.to_be_bytes().to_vec());
            }
        }

        // Navigate to middle and go backwards
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();

            // Find key 5
            cursor.find(5);
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 5 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 5);

            // Move to prev (key 4)
            cursor.prev();
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 4 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 4);

            // Move to prev again (key 3)
            cursor.prev();
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 3 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 3);
        }
    }

    #[test]
    fn test_large_tree_ordering() {
        // Insert 1000+ keys in random order, scan and verify sorted
        use rand::seq::SliceRandom;
        use rand::SeedableRng;

        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Create shuffled keys
        let mut keys: Vec<u64> = (0..1000).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
        keys.shuffle(&mut rng);

        // Insert in random order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for &key in &keys {
                cursor.insert(key, key.to_be_bytes().to_vec());
            }
        }

        // Scan and verify sorted order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            for expected_key in 0..1000u64 {
                let mut buf = [0u8; 8];
                cursor
                    .get_entry()
                    .unwrap_or_else(|| panic!("Key {} should exist", expected_key))
                    .read(&mut buf)
                    .unwrap();
                let actual_key = u64::from_be_bytes(buf);
                assert_eq!(actual_key, expected_key, "Keys should be in sorted order");
                cursor.next();
            }

            // Should be at end
            assert!(cursor.get_entry().is_none(), "Should be at end of tree");
        }
    }
}
