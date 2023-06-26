use std::cmp::Ordering::{Equal, Greater, Less};

use serde::{Deserialize, Serialize};

use crate::cell::{Cell, Key, Value, ValueRef};

#[derive(Serialize, Deserialize, Clone)]
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
            _ => panic!(),
        }
    }

    // TODO: inserting an item into an interior page doesn't make sense, interior pages dont store values!
    pub fn insert_item_at_index(&mut self, item_idx: usize, cell: Cell) {
        match self {
            NodePage::Leaf(l) => {
                l.insert_item_at_index(item_idx, cell);
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!(),
        };
    }

    // TODO: setting an item into an interior page doesn't make sense, interior pages dont store values!
    pub fn set_item_at_index(&mut self, item_idx: usize, cell: Cell) {
        match self {
            NodePage::Leaf(l) => {
                l.set_item_at_index(item_idx, cell);
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!(),
        };
    }

    pub fn split(self) -> (Self, Self) {
        match self {
            NodePage::Leaf(l) => {
                let (left, right) = l.split();
                (Self::Leaf(left), Self::Leaf(right))
            }
            NodePage::Interior(i) => {
                let (left, right) = i.split();
                (Self::Interior(left), Self::Interior(right))
            }
            _ => panic!(),
        }
    }

    pub fn smallest_key(&self) -> Key {
        match self {
            NodePage::Leaf(l) => l.cells.first().unwrap().key().clone(),
            NodePage::Interior(i) => i.keys.first().unwrap().clone(),
            _ => panic!(),
        }
    }

    pub fn largest_key(&self) -> Key {
        match self {
            NodePage::Leaf(l) => l.cells.last().unwrap().key().clone(),
            NodePage::Interior(i) => i.keys.last().unwrap().clone(),
            _ => panic!(),
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    /// then it must be in the child node identified by the given index and page number
    GoDown(usize, u32),
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

    fn split(&self) -> (LeafNodePage, LeafNodePage) {
        //TODO: can this take self by value?

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
#[derive(Serialize, Deserialize, Clone, Debug)]
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

    pub fn num_keys(&self) -> usize {
        self.keys.len()
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
                    return SearchResult::GoDown(idx, self.edges[idx]);
                }
                Equal => return SearchResult::GoDown(idx + 1, self.edges[idx + 1]),
                Greater => {
                    continue;
                }
            };
        }

        SearchResult::GoDown(self.edges.len() - 1, self.edges.last().unwrap().clone())
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

    fn split(&self) -> (InteriorNodePage, InteriorNodePage) {
        /*
            W  E  R
          [A][S][D][F]

          left:     right:
            W          R
          [A][S]     [D][F]

          E is no longer required
        */

        // InteriorNodePage {
        //   keys:    [1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14], // len: 14, len/2: 7,
        //   edges: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] // len: 15, len/2: 7

        //   left_keys:    [1, 2, 3, 4, 5, 6, 7]
        //   left_edges: [1, 1, 1, 1, 1, 1, 1, 1]

        //   right_keys:    [9,10,11,12,13,14], // len: 14, len/2: 7,
        //   right_edges: [1, 1, 1, 1, 1, 1, 1] // len: 15, len/2: 7
        // }

        // invariant each of the two interior pages produced must have at least two child pages and one key
        assert!(self.keys.len() >= 3); // One key is removed in the split
        assert!(self.edges.len() >= 4);

        let (left_keys, right_keys) = self.keys.split_at(self.keys.len() / 2);

        // we must take the extra key in the right side and remove it.
        let right_keys = &right_keys[1..];

        let (left_edges, right_edges) = self.edges.split_at((self.edges.len() + 1) / 2);

        assert_eq!(left_keys.len() + 1, left_edges.len());
        assert_eq!(right_keys.len() + 1, right_edges.len());

        let left = Self {
            edges: left_edges.to_vec(),
            keys: left_keys.to_vec(),
        };
        let right = Self {
            edges: right_edges.to_vec(),
            keys: right_keys.to_vec(),
        };
        (left, right)
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OverflowPage {
    content: Vec<u8>,
    continuation: Option<u32>,
}

impl OverflowPage {
    pub fn new(content: Vec<u8>, continuation: Option<u32>) -> OverflowPage {
        OverflowPage {
            content,
            continuation,
        }
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

    use super::{InteriorNodePage, LeafNodePage, SearchResult};

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
            super::SearchResult::GoDown(_, _) => panic!(),
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
                    SearchResult::GoDown(_, _) => panic!(),
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

    #[test]
    fn test_interior_split() {
        /*
            W  E  R
          [A][S][D][F]

          left:     right:
            W          R
          [A][S]     [D][F]

          E is no longer required
        */
        let (W, E, R) = (1, 2, 3);
        let (A, S, D, F) = (10, 20, 30, 40);

        let mut interior_node = InteriorNodePage::new(A, W, S);
        interior_node.insert_child_page(E, D);
        interior_node.insert_child_page(R, F);

        assert_eq!(interior_node.edges, &[A, S, D, F]);
        assert_eq!(interior_node.keys, &[W, E, R]);

        let (left, right) = interior_node.split();

        assert_eq!(left.edges, &[A, S]);
        assert_eq!(left.keys, &[W]);

        assert_eq!(right.edges, &[D, F]);
        assert_eq!(right.keys, &[R]);
    }

    proptest! {
        #[test]
        fn test_interior_page_split(interior_num_edges in 4u64..150) {
            let num_inserts = interior_num_edges-2; // there are already two edges in the interior page
            let mut interior_node = InteriorNodePage::new(1, 1, 1);
            for page in 0..num_inserts {
                interior_node.insert_child_page(page+2,1);
            }
            // println!("{interior_node:?}");
            let (_left, _right) = interior_node.split();
        }
    }
}
