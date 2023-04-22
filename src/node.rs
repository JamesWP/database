use std::cmp::Ordering::{Equal, Greater, Less};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum NodePage<K, V> {
    Leaf(LeafNodePage<K, V>),
    Interior(InteriorNodePage<K>),
}

impl<K, V> NodePage<K, V>
where
    K: Ord,
    K: Clone,
    V: Clone,
{
    pub fn search(&self, k: &K) -> SearchResult {
        match self {
            NodePage::Leaf(l) => l.search(k),
            NodePage::Interior(i) => todo!(),
        }
    }

    pub fn insert_item_at_index(&mut self, item_idx: usize, key: K, value: V) {
        match self {
            NodePage::Leaf(l) => {
                l.insert_item_at_index(item_idx, key, value);
            }
            NodePage::Interior(_) => todo!(),
        };
    }

    pub fn set_item_at_index(&mut self, item_idx: usize, key: K, value: V) {
        match self {
            NodePage::Leaf(l) => {
                l.set_item_at_index(item_idx, key, value);
            }
            NodePage::Interior(_) => todo!(),
        };
    }

    pub fn split(self) -> (Self, Self) {
        match self {
            NodePage::Leaf(l) => {
                let (left, right) = l.split();
                (Self::Leaf(left), Self::Leaf(right))
            }
            NodePage::Interior(_) => todo!(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeafNodePage<K, V> {
    cells: Vec<(K, V)>,
}

impl<K, V> Default for LeafNodePage<K, V> {
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

impl<K, V> LeafNodePage<K, V>
where
    K: Ord,
{
    pub fn search(&self, search_key: &K) -> SearchResult {
        // Simple linear search through the page.
        for (index, (key, _value)) in self.cells.iter().enumerate() {
            match search_key.cmp(key) {
                Less => return SearchResult::NotPresent(index),
                Equal => return SearchResult::Found(index),
                Greater => {} // Continue the search
            }
        }

        SearchResult::NotPresent(self.cells.len())
    }

    pub fn set_item_at_index(&mut self, index: usize, key: K, value: V) {
        self.cells[index] = (key, value);
    }

    pub fn insert_item_at_index(&mut self, index: usize, key: K, value: V) {
        // put item into leaf at given index.

        self.cells.insert(index, (key, value));
    }

    pub fn get_item_at_index(&self, entry_index: usize) -> Option<&(K, V)> {
        self.cells.get(entry_index)
    }

    pub fn num_items(&self) -> usize {
        self.cells.len()
    }

    pub fn verify_key_ordering(&self) -> Result<(), VerifyError> {
        let keys = || self.cells.iter().map(|(k, _v)| k);

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

    fn split(&self) -> (LeafNodePage<K, V>, LeafNodePage<K, V>)
    where
        K: Clone,
        V: Clone,
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
#[derive(Serialize, Deserialize)]
pub struct InteriorNodePage<K> {
    keys: Vec<K>,
    edges: Vec<u32>,
    // TODO: create interior node
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::{LeafNodePage, SearchResult};

    #[test]
    fn test_insertion_ordering() {
        let mut page = LeafNodePage::default();

        // []
        page.insert_item_at_index(0, 2, 0);
        // [2]
        page.insert_item_at_index(0, 1, 0);
        // [1, 2]
        page.insert_item_at_index(2, 3, 0);
        // [1, 2, 3]

        assert_eq!(page.cells[0].0, 1);
        assert_eq!(page.cells[1].0, 2);
        assert_eq!(page.cells[2].0, 3);
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

        page.insert_item_at_index(0, 1, 0);
        page.insert_item_at_index(1, 2, 0);
        page.insert_item_at_index(2, 3, 0);

        println!("Page: {:?}", page);
        assert_eq!(0, found_index(page.search(&1)));
        assert_eq!(1, found_index(page.search(&2)));
        assert_eq!(2, found_index(page.search(&3)));
    }

    use proptest::{prelude::*};

    proptest! {
        #[test]
        fn test_split(insertions in prop::collection::vec(&(0..100, 0..1000),0..100usize)) {
            let mut page = LeafNodePage::default();

            // Count num unique keys
            let n = insertions.iter().map(|(k,_v)|k).collect::<HashSet<_>>().len();

            for (key, value) in insertions {
                let result = page.search(&key);
                match result {
                    SearchResult::Found(idx) => {
                        page.set_item_at_index(idx, key, value);
                    }
                    SearchResult::NotPresent(idx) => {
                        page.insert_item_at_index(idx, key, value);
                    }
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
                assert!(left.get_item_at_index(left.num_items()-1).unwrap().0 < right.get_item_at_index(0).unwrap().0);
            }
        }
    }
}
