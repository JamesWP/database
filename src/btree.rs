use std::ops::{Deref, DerefMut};

use proptest::result;

use crate::{
    node::{self, SearchResult},
    pager::{self, Pager},
};

type Tuple = std::vec::Vec<serde_json::Value>;
type NodePage = node::NodePage<u64, Tuple>;
type LeafNodePage = node::LeafNodePage<u64, Tuple>;
type InteriorNodePage = node::InteriorNodePage<u64>;

pub struct Cursor<PagerRef> {
    pager: PagerRef,
    tree_name: String,

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

        let root_page = self.pager.get_root_page(&self.tree_name).unwrap();
        stack.push(root_page);

        loop {
            let top_page_idx = *stack.last().unwrap();
            let mut top_page: NodePage = self.pager.get_and_decode(top_page_idx);
            match top_page.search(&key) {
                SearchResult::Found(insertion_index) => {
                    // We found the index in the node where an existing value for this key exists
                    // we need to replace it with our value

                    top_page.set_item_at_index(insertion_index, key, value);

                    self.update_page(top_page, stack);

                    break;
                }
                SearchResult::NotPresent(item_idx) => {
                    top_page.insert_item_at_index(item_idx, key, value);

                    self.update_page(top_page, stack);

                    break;
                }
                SearchResult::GoDown(child_index) => {
                    // The node does not contain the value, instead we found the index of a child of this node where the value should be inserted instead
                    // we need to go deeper.

                    stack.push(child_index);
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

        self.debug("Before split");
        if stack.len() != 0 {
            // We must update the parent node
            // A reference to the new extra_page must be inserted into the parent node
            // Our reference in our parent might need updating???

            let parent_node_idx = stack.pop().unwrap();

            let parent_node: NodePage = self.pager.get_and_decode(parent_node_idx);

            let mut parent_interior_node = parent_node.interior().unwrap(); 

            parent_interior_node.insert_child_page(extra_page_first_key, extra_page_idx);

            // TODO: this will eventuallly overflow when an interior node needs splitting
            self.pager.encode_and_set(parent_node_idx, parent_interior_node.node::<Tuple>()).unwrap();


            // TODO: This logic needs to repeat to arbitrary tree depths
            assert!(stack.len() == 0);
        } else {
            // We have just split the root node...
            // We must now create the first interior node and insert two new child pages
            let interior_node =
                InteriorNodePage::new(top_page_idx, extra_page_first_key, extra_page_idx);

            let root_node = NodePage::Interior(interior_node);

            let root_node_idx = self.pager.allocate();
            self.pager.encode_and_set(root_node_idx, root_node).unwrap();
            self.pager.set_root_page(&self.tree_name, root_node_idx);

            // TODO: remove this
            self.verify().unwrap();
        }
        
        self.debug("After split");
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

        let root_page = self.pager.get_root_page(&self.tree_name).unwrap();
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
                    self.leaf_iterator = Some((page_idx, 0));
                    return;
                }
                node::NodePage::Interior(i) => {
                    self.stack.push((page_idx, 0));
                    page_idx = i.get_child_page_by_index(0);
                }
            }
        }
    }

    /// Move the cursor to point at the last row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// last row to point to
    fn last(&mut self) {
        // Take the tree identified by the root page number, and find its right most node and
        // find its largest entry.
        let root_page_idx = self.pager.get_root_page(&self.tree_name).unwrap();
        let root_page: NodePage = self.pager.get_and_decode(root_page_idx);

        let mut page = root_page;
        let mut page_idx = root_page_idx;
        loop {
            match page {
                node::NodePage::Leaf(l) => {
                    // We found the first leaf in the tree.
                    // TODO: Maybe store a readonly copy of this leaf node instead of this `leaf_iterator`
                    self.leaf_iterator = Some((page_idx, l.num_items() - 1));
                    return;
                }
                node::NodePage::Interior(_i) => todo!(),
            }
        }
    }

    /// Move the cursor to point at the row in the btree identified by the given key
    /// This may result in the cursor not pointing to a row if there is no
    /// row found with that key to point to
    fn find(&mut self, key: u64) {
        let root_page_idx = self.pager.get_root_page(&self.tree_name).unwrap();
        let root_page: NodePage = self.pager.get_and_decode(root_page_idx);

        let mut page = root_page;
        let page_idx = root_page_idx;

        loop {
            match page.search(&key) {
                SearchResult::Found(index) => {
                    self.leaf_iterator = Some((page_idx, index));
                    return;
                }
                SearchResult::NotPresent(_) => self.leaf_iterator = None,
                SearchResult::GoDown(_) => todo!(),
            }
        }
    }

    /// get the value at the specified column index from the row pointed to by the cursor,
    /// or None if the cursor is not pointing to a row
    fn column(&self, col_idx: usize) -> Option<serde_json::Value> {
        let (_key, value) = self.get_entry()?;

        let value = &value[col_idx];

        Some(value.clone())
    }

    fn row_key(&self) -> Option<u64> {
       let (key, _value) = self.get_entry()?; 

       Some(key)
    }

    fn get_entry(&self) -> Option<(u64, Tuple)> {
        // TODO: This returns a copy of the entry even if we dont need a copy
        let (leaf_page_number, entry_index) = self.leaf_iterator?;

        let page: NodePage = self.pager.get_and_decode(leaf_page_number);

        let page = page.leaf().expect("Values are always supposed to be in leaf pages");

        return page.get_item_at_index(entry_index).cloned();
    }

    /// Move the cursor to point at the next item in the btree
    fn next(&mut self) {
        if self.leaf_iterator.is_none() {
            return;
        }

        let (page_number, entry_index) = self.leaf_iterator.unwrap();

        let page: NodePage = self.pager.get_and_decode(page_number);

        let page = page.leaf().expect("Values are always supposed to be in leaf pages");
        let num_items_in_leaf = page.num_items();

        // Check if there are more items left in the curent leaf
        if entry_index + 1 < num_items_in_leaf {
            self.leaf_iterator = Some((page_number, entry_index + 1));
            return;
        }

        // We ran out of items on this leaf page, find the next leaf page
        loop {
            // if the stack is empty then we have no more places to go
            if self.stack.is_empty() {
                self.leaf_iterator = None;
                return;
            }

            let (curent_interior_idx, curent_edge) = self.stack.pop().unwrap();

            let curent_interior: NodePage = self.pager.get_and_decode(curent_interior_idx);

            let curent_interior = curent_interior.interior().expect("The stack should only contain interior pages");
            let edge_count = curent_interior.num_edges();

            // if we there are more edges to the right:
            if curent_edge + 1 < edge_count {
                // select the next edge in the curent page
                self.stack.push((curent_interior_idx, curent_edge + 1));

                // find the page_idx for the new edge
                let curent_edge_idx = curent_interior.get_child_page_by_index(curent_edge + 1);

                // then select the first item in the leftmost leaf of that subtree
                self.select_leftmost_of_idx(curent_edge_idx);
                return;
            }

            // if there are no more edges in this node:
            //    pop this item off the stack and repeat
            // pop already happened
        }
    }

    /// Move the cursor to point at the next item in the btree
    fn prev(&mut self) {
        if self.leaf_iterator.is_none() {
            return;
        }

        let (leaf_page_number, entry_index) = self.leaf_iterator.unwrap();

        let page: NodePage = self.pager.get_and_decode(leaf_page_number);

        let _leaf_page = page.leaf().expect("Values are always supposed to be in leaf pages");

        if entry_index > 0 {
            self.leaf_iterator = Some((leaf_page_number, entry_index - 1));
        } else {
            // We ran out of items on this page, find the previous leaf page
            todo!()
        }
    }

    fn debug(&self, message: &str) {
        self.pager.debug(message);
    }

    fn verify_leaf(&self, leaf: LeafNodePage) -> Result<usize, VerifyError> {
        // Check each leaf page has keys (unless its a root node)
        assert!(leaf.num_items() > 0);

        // Check the keys in each leaf page are in order
        leaf.verify_key_ordering()?;

        Ok(0)
    }

    fn verify_interior(&self, interior: InteriorNodePage) -> Result<usize, VerifyError> {
        // if interior page contains edges to leaves, all edges must be leaves
        // if interior page contains edges to interior nodes, each interior node must have leaves at the same level
        // Check all interior node's keys are in order
        interior.verify_key_ordering()?;

        // Check all interior nodes are half full of entries ???
        // They should have at least two edges
        assert!(interior.num_edges() > 1);

        // Check all interior node's child page's keys are within bounds
        for edge in 0..interior.num_edges() - 1 {
            let child_page_idx = interior.get_child_page_by_index(edge);
            let child_page: NodePage = self.pager.get_and_decode(child_page_idx);

            let edge_key = interior.get_key_by_index(edge);
            let smallest_key = child_page.smallest_key();
            let largest_key = child_page.largest_key();

            assert!(smallest_key <= largest_key);
            assert!(largest_key <= edge_key);
        }

        let mut edge_levels = vec![];

        for edge in 0..interior.num_edges() {
            let edge_idx = interior.get_child_page_by_index(edge);
            let edge: NodePage = self.pager.get_and_decode(edge_idx);
            let level = self.verify_node(edge)?;
            edge_levels.push(level);
        }

        let first_level = edge_levels.first().unwrap().clone();

        if edge_levels
            .into_iter()
            .skip(1)
            .filter(|l| *l != first_level)
            .next()
            .is_some()
        {
            // found at least one edge with a different level to the first edge
            return Err(VerifyError::Imbalance);
        }

        Ok(first_level)
    }

    fn verify_node(&self, node: NodePage) -> Result<usize, VerifyError> {
        match node {
            node::NodePage::Leaf(l) => self.verify_leaf(l),
            node::NodePage::Interior(i) => self.verify_interior(i),
        }
    }

    fn verify(&mut self) -> Result<(), VerifyError> {
        let root_page_idx = self.pager.get_root_page(&self.tree_name).unwrap();
        let root_page: NodePage = self.pager.get_and_decode(root_page_idx);

        match root_page {
            node::NodePage::Leaf(l) => {
                // we dont need to do the other validation if the leaf is the root node
                l.verify_key_ordering()?;
            }
            node::NodePage::Interior(i) => {
                self.verify_interior(i)?;
            }
        };

        Ok(())
    }
}

#[derive(Debug)]
pub enum VerifyError {
    KeyOutOfOrder,
    Imbalance,
}

impl From<node::VerifyError> for VerifyError {
    fn from(value: node::VerifyError) -> Self {
        match value {
            node::VerifyError::KeyOutOfOrder => Self::KeyOutOfOrder,
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
        // Check if the root page actually exists, or return None
        self.pager.get_root_page(tree_name)?;

        Some(Cursor {
            pager: &self.pager,
            stack: vec![],
            leaf_iterator: None,
            tree_name: tree_name.to_owned(),
        })
    }

    fn open_readwrite<'a>(&'a mut self, tree_name: &str) -> Option<Cursor<&'a mut Pager>> {
        // Check if the root page actually exists, or return None
        self.pager.get_root_page(tree_name)?;

        Some(Cursor {
            pager: &mut self.pager,
            stack: vec![],
            leaf_iterator: None,
            tree_name: tree_name.to_owned(),
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
        self.pager.encode_and_set(idx, empty_root_node).unwrap();
    }

    fn debug(&self, message: &str) {
        self.pager.debug(message)
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

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

        btree.debug("");
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

        btree.debug("");
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

        btree.debug("");
    }

    use proptest::prelude::*;

    #[test]
    fn multi_level_insertion() {
        let test = TestDb::default();
        let mut btree = test.btree;

        assert!(btree.open_readonly("testing").is_none());

        btree.create_tree("testing");

        let mut cursor = btree.open_readwrite("testing").unwrap();

        let long_string = |s: &str, num| vec![serde_json::Value::String(s.repeat(num))];

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
    }

    proptest! {
        #[test]
        fn test_ordering(elements in prop::collection::vec(&(1..100u64, &(prop::char::range('A', 'z'), 1..1000usize)), 1..200usize)) {
            println!("Test: {elements:?}");

            let mut rust_btree = BTreeMap::new();

            let test = TestDb::default();
            let mut my_btree = test.btree;

            my_btree.create_tree("testing");

            let mut cursor = my_btree.open_readwrite("testing").unwrap();

            for (k, (v, len)) in elements {
                cursor.verify().unwrap();
                let value = v.to_string().repeat(len);

                rust_btree.insert(k, value.clone());
                cursor.insert(k, vec![serde_json::Value::String(value.clone())]);
            }

            cursor.verify().unwrap();
            cursor.debug("Before order check");

            cursor.first();

            for (key, actual_value) in rust_btree.iter() {
                let my_value = cursor.column(0).unwrap();
                println!("Key: {key} {my_value}");
                assert_eq!(json![actual_value], my_value);
                cursor.next();
            }

            cursor.verify().unwrap();
        }
    }
}
