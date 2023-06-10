use std::{cmp::Ordering::{Equal, Greater, Less}};

use serde::{Deserialize, Serialize};

use crate::cell::{Key, Value, ValueRef, Cell};

#[derive(Serialize, Deserialize)]
pub enum NodePage {
    Leaf(LeafNodePage),
    Interior(InteriorNodePage),
    OverflowPage(OverflowPage),
}

impl NodePage {
    pub fn search(&self, k: &Key) -> SearchResult {
        match self {
            NodePage::Leaf(l) => l.search(k),
            NodePage::Interior(i) => i.search(k),
            _ => panic!()
        }
    }

    // TODO: inserting an item into an interior page doesn't make sense, interior pages dont store values!
    pub fn insert_item_at_index(&mut self, item_idx: usize, cell: Cell) {
        match self {
            NodePage::Leaf(l) => {
                l.insert_item_at_index(item_idx, cell);
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!()
        };
    }

    // TODO: setting an item into an interior page doesn't make sense, interior pages dont store values!
    pub fn set_item_at_index(&mut self, item_idx: usize, cell: Cell) {
        match self {
            NodePage::Leaf(l) => {
                l.set_item_at_index(item_idx, cell);
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!()
        };
    }

    pub fn split(self) -> (Self, Self) {
        match self {
            NodePage::Leaf(l) => {
                let (left, right) = l.split();
                (Self::Leaf(left), Self::Leaf(right))
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!()
        }
    }

    pub fn smallest_key(&self) -> Key {
        match self {
            NodePage::Leaf(l) => l.cells.first().unwrap().key().clone(),
            NodePage::Interior(i) => i.keys.first().unwrap().clone(),
            _ => panic!()
        }
    }

    pub fn largest_key(&self) -> Key {
        match self {
            NodePage::Leaf(l) => l.cells.last().unwrap().key().clone(),
            NodePage::Interior(i) => i.keys.last().unwrap().clone(),
            _ => panic!()
        }
    }

    pub fn interior(self) -> Option<InteriorNodePage> {
        match self {
            NodePage::Interior(i) => Some(i),
            _ => None,
        }
    }

    pub fn leaf(&self) -> Option<&LeafNodePage> {
        match self {
            NodePage::Leaf(l) => Some(l),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeafNodePage {
    cells: Vec<Cell>,
}

impl Default for LeafNodePage {
    fn default() -> Self {
        Self {
            cells: Default::default(),
        }
    }
}

pub enum SearchResult {
    /// The value was found at the given index of the given leaf node
    Found(usize),
    /// The value is not present in the leaf node, but if it were it should be at this index
    NotPresent(usize),
    /// The element wasn't found, but if it is anywhere
    /// then it must be in the child node identified by the given page number
    GoDown(u32),
}

impl LeafNodePage {
    pub fn search(&self, search_key: &Key) -> SearchResult {
        // Simple linear search through the page.
        for (index, cell) in self.cells.iter().enumerate() {
            let cell_key = cell.key();
            match search_key.cmp(&cell_key) {
                Less => return SearchResult::NotPresent(index),
                Equal => return SearchResult::Found(index),
                Greater => {} // Continue the search
            }
        }

        SearchResult::NotPresent(self.cells.len())
    }

    pub fn set_item_at_index(&mut self, index: usize, cell: Cell) {
        self.cells[index] = cell;
    }

    pub fn insert_item_at_index(&mut self, index: usize, cell: Cell) {
        self.cells.insert(index, cell);
    }

    pub fn get_item_at_index<'a>(&'a self, entry_index: usize) -> Option<&Cell> {
        self.cells.get(entry_index)
    }

    pub fn num_items(&self) -> usize {
        self.cells.len()
    }

    pub fn verify_key_ordering(&self) -> Result<(), VerifyError> {
        let keys = || self.cells.iter().map(Cell::key);

        for (left, right) in keys().zip(keys()) {
            match left.cmp(&right) {
                Less | Equal => { /* GOOD! */ }
                Greater => {
                    return Err(VerifyError::KeyOutOfOrder);
                }
            }
        }

        Ok(())
    }

    fn split(&self) -> (LeafNodePage, LeafNodePage)
    {
        let midpoint = self.cells.len() / 2;
        let (left, right) = self.cells.split_at(midpoint);

        let left = Self {
            cells: left.to_vec(),
        };

        let right = Self {
            cells: right.to_vec(),
        };

        (left, right)
    }
}

#[derive(Debug)]
pub enum VerifyError {
    KeyOutOfOrder,
}

// [edge 0] [key 0] [edge 1] [key 1] ... [key N-1] [edge N]
// items in [edge i] are LESS than or EQUAL to [key i]
// (if there is no [key i], i.e. at the end, items in [edge i] must be GREATER than [key i-1])
#[derive(Serialize, Deserialize)]
pub struct InteriorNodePage {
    keys: Vec<Key>,
    edges: Vec<u32>,
}

impl InteriorNodePage {
    pub fn new(
        left_page_idx: u32,
        right_page_smallest_key: Key,
        right_page_idx: u32,
    ) -> InteriorNodePage {
        InteriorNodePage {
            keys: vec![right_page_smallest_key],
            edges: vec![left_page_idx, right_page_idx],
        }
    }

    pub fn get_child_page_by_index(&self, arg: usize) -> u32 {
        self.edges[arg].clone()
    }

    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }

    pub fn verify_key_ordering(&self) -> Result<(), VerifyError> {
        let keys = || self.keys.iter();

        for (left, right) in keys().zip(keys()) {
            match left.cmp(right) {
                Less | Equal => { /* GOOD! */ }
                Greater => {
                    return Err(VerifyError::KeyOutOfOrder);
                }
            }
        }

        Ok(())
    }

    pub fn get_key_by_index(&self, edge: usize) -> Key {
        self.keys[edge].clone()
    }

    fn search(&self, k: &Key) -> SearchResult {
        for (idx, key) in self.keys.iter().enumerate() {
            match k.cmp(key) {
                Less => {
                    return SearchResult::GoDown(self.edges[idx]);
                }
                Equal => return SearchResult::GoDown(self.edges[idx + 1]),
                Greater => {
                    continue;
                }
            };
        }

        SearchResult::GoDown(self.edges.last().unwrap().clone())
    }

    pub fn node(self) -> NodePage {
        NodePage::Interior(self)
    }

    pub fn insert_child_page(&mut self, edge_page_smallest_key: Key, edge_page_idx: u32) {
        for (idx, key) in self.keys.iter().enumerate() {
            match edge_page_smallest_key.cmp(key) {
                Less => {
                    self.edges.insert(idx + 1, edge_page_idx);
                    self.keys.insert(idx, edge_page_smallest_key);
                    return;
                }
                Equal => panic!("Don't think this is possible"),
                Greater => {
                    continue;
                }
            }
        }

        self.edges.push(edge_page_idx);
        self.keys.push(edge_page_smallest_key);
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct OverflowPage {
    content: Vec<u8>,
    continuation: Option<u32>
}

impl OverflowPage {
    pub fn new(content: Vec<u8>, continuation: Option<u32>) -> OverflowPage {
        OverflowPage { content, continuation }
    }

    pub fn continuation(&self) -> Option<u32> {
        self.continuation
    }

    pub fn value(&self) -> &[u8] {
        &self.content
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::node::Cell;

    use super::{LeafNodePage, SearchResult};

    #[test]
    fn test_insertion_ordering() {
        let mut page = LeafNodePage::default();

        // []
        page.insert_item_at_index(0, Cell::new(2, vec![0], None));
        // [2]
        page.insert_item_at_index(0, Cell::new(1, vec![0], None));
        // [1, 2]
        page.insert_item_at_index(2, Cell::new(3, vec![0], None));
        // [1, 2, 3]

        assert_eq!(page.cells[0].key(), 1);
        assert_eq!(page.cells[1].key(), 2);
        assert_eq!(page.cells[2].key(), 3);
    }

    fn found_index(r: SearchResult) -> usize {
        match r {
            super::SearchResult::Found(i) => i,
            super::SearchResult::NotPresent(_) => panic!(),
            super::SearchResult::GoDown(_) => panic!(),
        }
    }

    #[test]
    fn test_search() {
        let mut page = LeafNodePage::default();

        page.insert_item_at_index(0, Cell::new(1, vec![0], None));
        page.insert_item_at_index(1, Cell::new(2, vec![0], None));
        page.insert_item_at_index(2, Cell::new(3, vec![0], None));

        println!("Page: {:?}", page);
        assert_eq!(0, found_index(page.search(&1)));
        assert_eq!(1, found_index(page.search(&2)));
        assert_eq!(2, found_index(page.search(&3)));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_split(insertions in prop::collection::vec(&(0..100u64, 0..1000u64),0..100usize)) {
            let mut page = LeafNodePage::default();

            // Count num unique keys
            let n = insertions.iter().map(|(k,_)| k).collect::<HashSet<_>>().len();

            for (key, value) in insertions {
                let value = value.to_be_bytes().to_vec();
                let cell = Cell::new(key, value, None);
                let result = page.search(&key);
                match result {
                    SearchResult::Found(idx) => page.set_item_at_index(idx, cell),
                    SearchResult::NotPresent(idx) => page.insert_item_at_index(idx, cell),
                    SearchResult::GoDown(_) => panic!(),
                };

                page.verify_key_ordering().unwrap();
            }

            // Page has N elements, one for each unique key
            assert_eq!(n, page.num_items());

            // println!("both {page:?}");

            let (left, right) = page.split();

            // println!("left {left:?} <-> right {right:?}");

            // No items were lost in the making of these parts
            assert_eq!(left.num_items() + right.num_items(), n);

            let delta = left.num_items().abs_diff(right.num_items());

            assert!(delta == 0 || delta == 1);

            // If we have items in both parts, they should be in order
            if left.num_items()>0 && right.num_items()>0 {
                assert!(left.get_item_at_index(left.num_items()-1).unwrap().key() < right.get_item_at_index(0).unwrap().key());
            }
        }
    }
}
