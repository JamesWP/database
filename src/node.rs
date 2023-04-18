use std::cmp::Ordering::{Equal, Greater, Less};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum NodePage<K, V> {
    Leaf(LeafNodePage<K, V>),
    Interior(InteriorNodePage<K>),
}

impl<K, V> NodePage<K, V> 
where K: Ord
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
                Less => { return SearchResult::NotPresent(index)},
                Equal => { return SearchResult::Found(index) },
                Greater => {}, // Continue the search
            }
        }
        
        SearchResult::NotPresent(self.cells.len())
    }

    pub fn set_item_at_index(&mut self, index: usize, key: K, value: V) {
        self.cells[index] = (key, value);
    }

    pub fn insert_item_at_index(
        &mut self,
        index: usize,
        key: K,
        value: V,
    ) {
        // put item into leaf at given index.

        self.cells.insert(index, (key, value));
    }

    pub fn get_item_at_index(&self, entry_index: usize) -> Option<&(K, V)> {
        self.cells.get(entry_index)
    }

    pub fn num_items(&self) -> usize{
        self.cells.len()
    }
}

#[derive(Serialize, Deserialize)]
pub struct InteriorNodePage<K> {
    keys: Vec<K>,
    edges: Vec<u32>,
    // TODO: create interior node
}


#[cfg(test)]
mod test {
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
}